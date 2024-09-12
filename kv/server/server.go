package kv

import (
	"bytes"
	"encoding/gob"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	c "github.com/zjregee/shardkv/common"
	pb "github.com/zjregee/shardkv/proto"
	"github.com/zjregee/shardkv/raft"
	"google.golang.org/grpc"
)

const (
	ENABLED_EXPIRED       = true
	DEFAULT_KV_SERVE_PORT = 4520
)

const (
	EXPIRED_TIMEOUT = time.Second * 1
	METRICS_TIMEOUT = time.Millisecond * 20
)

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

type Server struct {
	mu         sync.Mutex
	dataIndex  int32
	data       map[string]string
	notifyMap  map[int64]chan string
	executeMap map[int64]struct{}
	applyCh    chan raft.ApplyMsg
	rf         *raft.Raft
	me         int32
	peers      []string
	dead       int32
	deadCh     chan struct{}
	pb.UnimplementedKvServiceServer
}

func MakeServer(peers []string, me int32) *Server {
	kv := &Server{}
	kv.dataIndex = 0
	kv.data = make(map[string]string)
	kv.notifyMap = make(map[int64]chan string)
	kv.executeMap = make(map[int64]struct{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.MakeRaft(peers, me, kv.applyCh)
	kv.me = me
	kv.peers = peers
	kv.dead = 0
	kv.deadCh = make(chan struct{})
	return kv
}

func (kv *Server) Serve() {
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(DEFAULT_KV_SERVE_PORT))
	if err != nil {
		panic(err)
	}
	s := grpc.NewServer()
	pb.RegisterKvServiceServer(s, kv)
	err = s.Serve(listener)
	if err != nil {
		panic(err)
	}
	go kv.run()
	go kv.leaderChangedLoop()
}

func (kv *Server) killed() bool {
	return atomic.LoadInt32(&kv.dead) == 1
}

func (kv *Server) state() (bool, int32) {
	return kv.rf.GetState()
}

func (kv *Server) run() {
	for !kv.killed() {
		var msg raft.ApplyMsg
		select {
		case msg = <-kv.applyCh:
		case <-kv.deadCh:
			return
		}

		kv.mu.Lock()
		c.Assert(msg.CommandValid, "command should be valid")
		c.Assert(msg.CommandIndex == kv.dataIndex+1, "command index should be valid")
		op := deserializeOp(msg.Command)
		kv.applyOperation(op)
		kv.dataIndex = msg.CommandIndex
		kv.mu.Unlock()
	}
}

func (kv *Server) leaderChangedLoop() {
	for !kv.killed() {
		isLeader, _ := kv.state()
		if !isLeader {
			kv.mu.Lock()
			for _, ch := range kv.notifyMap {
				select {
				case ch <- ErrWrongLeader:
				default:
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(METRICS_TIMEOUT)
	}
}

func (kv *Server) submitOp(op Op) string {
	entry := serializeOp(op)
	kv.rf.Start(entry)
	ch := make(chan string)
	kv.notifyMap[op.Id] = ch
	kv.mu.Unlock()
	var value string
	dead := false
	select {
	case value = <-ch:
	case <-kv.deadCh:
		dead = true
	}
	kv.mu.Lock()
	close(ch)
	delete(kv.notifyMap, op.Id)
	if dead {
		return ErrClosed
	}
	return value
}

func (kv *Server) applyOperation(op Op) {
	_, needNotify := kv.notifyMap[op.Id]
	if needNotify {
		value := OK
		if op.Kind == "Get" {
			_, exists := kv.data[op.Key]
			if !exists {
				value = ErrNoKey
			} else {
				value = kv.data[op.Key]
			}
		} else {
			_, isInExecute := kv.executeMap[op.Id]
			if !isInExecute {
				if op.Value == "" {
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
				value = Duplicate
			}
		}
		ch := kv.notifyMap[op.Id]
		select {
		case ch <- value:
		default:
		}
	} else if op.Kind != "Get" {
		_, isInExecute := kv.executeMap[op.Id]
		if !isInExecute {
			if op.Value == "" {
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
