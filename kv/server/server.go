package server

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zjregee/shardkv/common/storage"
	"github.com/zjregee/shardkv/common/utils"
	mvcc "github.com/zjregee/shardkv/kv/transaction"
	pb "github.com/zjregee/shardkv/proto"
	"github.com/zjregee/shardkv/raft"
	"google.golang.org/grpc"
)

const METRICS_TIMEOUT = time.Millisecond * 20

type Server struct {
	mu         sync.Mutex
	dataIndex  int32
	data       map[string]string
	notifyMap  map[int64]chan OperationResult
	executeMap map[int64]struct{}
	applyCh    chan raft.ApplyMsg
	rf         *raft.Raft
	me         int32
	port       string
	peers      []string
	dead       int32
	deadCh     chan struct{}
	server     *grpc.Server
	latches    *mvcc.Latches
	storage    storage.Storage
	pb.UnimplementedKvServiceServer
}

func MakeServer(kv_peers, raft_peers []string, me int32) *Server {
	kv := &Server{}
	kv.dataIndex = 0
	kv.data = make(map[string]string)
	kv.notifyMap = make(map[int64]chan OperationResult)
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
		utils.Assert(msg.CommandValid, "commnadValid should be true")
		utils.Assert(msg.CommandIndex == kv.dataIndex+1, "commandIndex should be equal to dataIndex+1")
		op := deserializeOperation(msg.Command)
		kv.recordOperation(op)
		kv.applyOperation(op)
		kv.dataIndex = msg.CommandIndex
		kv.mu.Unlock()
	}
}

func (kv *Server) leaderChangedLoop() {
	for !kv.killed() {
		isLeader, _ := kv.state()
		if !isLeader {
			result := OperationResult{}
			result.err = pb.Err_ErrWrongLeader
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
