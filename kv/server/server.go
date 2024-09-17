package server

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	c "github.com/zjregee/shardkv/common"
	pb "github.com/zjregee/shardkv/proto"
	"github.com/zjregee/shardkv/raft"
	"google.golang.org/grpc"
)

const METRICS_TIMEOUT = time.Millisecond * 20

const (
	OK             = "OK"
	Duplicate      = "Duplicate"
	ErrNoKey       = "ErrNoKey"
	ErrClosed      = "ErrClosed"
	ErrWrongLeader = "ErrWrongLeader"
)

type Op struct {
	Id    int64
	Kind  string
	Key   string
	Value string
}

type Result struct {
	Err   pb.Err
	Value string
}

type Server struct {
	mu         sync.Mutex
	dataIndex  int32
	data       map[string]string
	notifyMap  map[int64]chan Result
	executeMap map[int64]struct{}
	applyCh    chan raft.ApplyMsg
	rf         *raft.Raft
	me         int32
	port       string
	peers      []string
	dead       int32
	deadCh     chan struct{}
	server     *grpc.Server
	pb.UnimplementedKvServiceServer
}

func MakeServer(kv_peers, raft_peers []string, me int32) *Server {
	kv := &Server{}
	kv.dataIndex = 0
	kv.data = make(map[string]string)
	kv.notifyMap = make(map[int64]chan Result)
	kv.executeMap = make(map[int64]struct{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.MakeRaft(raft_peers, me, kv.applyCh)
	kv.me = me
	_, port, err := net.SplitHostPort(kv_peers[me])
	if err != nil {
		panic(err)
	}
	kv.port = port
	kv.peers = kv_peers
	kv.dead = 0
	kv.deadCh = make(chan struct{})
	kv.server = nil
	return kv
}

func (kv *Server) Serve() {
	kv.rf.Serve()
	listener, err := net.Listen("tcp", ":"+kv.port)
	if err != nil {
		panic(err)
	}
	kv.server = grpc.NewServer()
	pb.RegisterKvServiceServer(kv.server, kv)
	go func() {
		if err := kv.server.Serve(listener); err != nil {
			panic(err)
		}
	}()
	go kv.run()
	go kv.leaderChangedLoop()
}

func (kv *Server) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	close(kv.deadCh)
	if kv.server != nil {
		kv.server.GracefulStop()
		kv.server = nil
	}
	kv.rf.Kill()
}

func (kv *Server) state() (bool, int32) {
	return kv.rf.State()
}

func (kv *Server) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *Server) run() {
	var msg raft.ApplyMsg
	for !kv.killed() {
		select {
		case msg = <-kv.applyCh:
		case <-kv.deadCh:
			return
		}
		kv.mu.Lock()
		c.Assert(msg.CommandValid, "commnadValid should be true")
		c.Assert(msg.CommandIndex == kv.dataIndex+1, "commandIndex should be equal to dataIndex+1")
		op := deserializeOp(msg.Command)
		kv.recordOp(op)
		kv.applyOperation(op)
		kv.dataIndex = msg.CommandIndex
		kv.mu.Unlock()
	}
}

func (kv *Server) leaderChangedLoop() {
	for !kv.killed() {
		isLeader, _ := kv.state()
		if !isLeader {
			result := Result{}
			result.Err = pb.Err_ErrWrongLeader
			kv.mu.Lock()
			for _, ch := range kv.notifyMap {
				select {
				case ch <- result:
				default:
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(METRICS_TIMEOUT)
	}
}

func (kv *Server) applyOperation(op Op) {
	_, needNotify := kv.notifyMap[op.Id]
	if needNotify {
		result := Result{}
		result.Err = pb.Err_OK
		result.Value = ""
		if op.Kind == "GET" {
			_, exists := kv.data[op.Key]
			if !exists {
				result.Err = pb.Err_ErrNoKey
			} else {
				result.Value = kv.data[op.Key]
			}
		} else {
			_, isInExecute := kv.executeMap[op.Id]
			if !isInExecute {
				if op.Kind == "DEL" {
					delete(kv.data, op.Key)
				} else if op.Kind == "SET" {
					kv.data[op.Key] = op.Value
				} else {
					_, exists := kv.data[op.Key]
					if !exists {
						kv.data[op.Key] = op.Value
					} else {
						kv.data[op.Key] += op.Value
					}
				}
				kv.executeMap[op.Id] = struct{}{}
			} else {
				result.Err = pb.Err_Duplicate
			}
		}
		ch := kv.notifyMap[op.Id]
		select {
		case ch <- result:
		default:
		}
	} else if op.Kind != "Get" {
		_, isInExecute := kv.executeMap[op.Id]
		if !isInExecute {
			if op.Kind == "DEL" {
				delete(kv.data, op.Key)
			} else if op.Kind == "SET" {
				kv.data[op.Key] = op.Value
			} else {
				_, exists := kv.data[op.Key]
				if !exists {
					kv.data[op.Key] = op.Value
				} else {
					kv.data[op.Key] += op.Value
				}
			}
			kv.executeMap[op.Id] = struct{}{}
		}
	}
}

func (kv *Server) submitOp(op Op) (pb.Err, string) {
	entry := serializeOp(op)
	kv.rf.Start(entry)
	ch := make(chan Result)
	kv.notifyMap[op.Id] = ch
	kv.mu.Unlock()
	var result Result
	dead := false
	select {
	case result = <-ch:
	case <-kv.deadCh:
		dead = true
	}
	kv.mu.Lock()
	close(ch)
	delete(kv.notifyMap, op.Id)
	if dead {
		return pb.Err_ErrClosed, ""
	}
	return result.Err, result.Value
}

func (kv *Server) recordOp(op Op) {
	var record string
	if op.Kind == "GET" || op.Kind == "DEL" {
		record = fmt.Sprintf("%s(id: %d, key: %s)", op.Kind, op.Id, op.Key)
	} else {
		record = fmt.Sprintf("%s(id: %d, key: %s, value: %s)", op.Kind, op.Id, op.Key, op.Value)
	}
	c.Log.Infof("[node %d] apply operation: %s", kv.me, record)
}

func serializeOp(op Op) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(op)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func deserializeOp(data []byte) Op {
	var op Op
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&op)
	if err != nil {
		panic(err)
	}
	return op
}
