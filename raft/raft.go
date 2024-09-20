package raft

import (
	"net"
	"sync"
	"sync/atomic"
	"time"

	l "github.com/zjregee/shardkv/common/logger"
	"github.com/zjregee/shardkv/common/utils"
	pb "github.com/zjregee/shardkv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	RPC_TIMEOUT        = time.Millisecond * 20
	METRICS_TIMEOUT    = time.Millisecond * 20
	BROADCAST_TIMEOUT  = time.Millisecond * 100
	CANDIDATES_TIMEOUT = time.Millisecond * 300
	HEARTBEAT_TIMEOUT  = time.Millisecond * 400
)

const (
	LEADER     = "LEADER"
	FOLLOWER   = "FOLLOWER"
	CANDIDATES = "CANDIDATES"
)

type ApplyMsg struct {
	CommandValid  bool
	Command       []byte
	CommandIndex  int32
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int32
	SnapshotIndex int32
}

type Raft struct {
	mu                sync.Mutex
	r                 string
	term              int32
	votedTerm         int32
	leaderIndex       int32
	lastActiveTime    time.Time
	lastBroadcastTime time.Time
	log               []*pb.LogEntry
	hasNewLog         bool
	snapshot          []byte
	snapshotTerm      int32
	snapshotIndex     int32
	applySnapshotOnce bool
	commitIndex       int32
	appliedIndex      int32
	nextIndex         []int32
	matchIndex        []int32
	snapshotApplied   []bool
	me                int32
	port              string
	peers             []*grpc.ClientConn
	dead              int32
	applyCh           chan ApplyMsg
	server            *grpc.Server
	pb.UnimplementedRaftServiceServer
}

func MakeRaft(peers []string, me int32, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.r = FOLLOWER
	rf.term = 0
	rf.votedTerm = 0
	rf.leaderIndex = -1
	rf.lastActiveTime = time.Now()
	rf.lastBroadcastTime = time.Now()
	rf.log = make([]*pb.LogEntry, 0)
	rf.hasNewLog = false
	rf.snapshot = nil
	rf.snapshotTerm = 0
	rf.snapshotIndex = 0
	rf.applySnapshotOnce = false
	rf.commitIndex = 0
	rf.appliedIndex = 0
	rf.nextIndex = make([]int32, len(peers))
	rf.matchIndex = make([]int32, len(peers))
	rf.snapshotApplied = make([]bool, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
		rf.snapshotApplied[i] = true
	}
	rf.me = me
	_, port, err := net.SplitHostPort(peers[me])
	if err != nil {
		panic(err)
	}
	rf.port = port
	rf.peers = make([]*grpc.ClientConn, len(peers))
	for i, peer := range peers {
		if i == int(me) {
			rf.peers[i] = nil
		}
		conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			panic(err)
		}
		rf.peers[i] = conn
	}
	rf.dead = 0
	rf.applyCh = applyCh
	rf.server = nil
	return rf
}

func (rf *Raft) Serve() {
	listener, err := net.Listen("tcp", ":"+rf.port)
	if err != nil {
		panic(err)
	}
	rf.server = grpc.NewServer()
	pb.RegisterRaftServiceServer(rf.server, rf)
	go func() {
		if err := rf.server.Serve(listener); err != nil {
			panic(err)
		}
	}()
	go rf.run()
}

func (rf *Raft) State() (bool, int32) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.r == LEADER, rf.leaderIndex
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	if rf.server != nil {
		rf.server.GracefulStop()
		rf.server = nil
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

func (rf *Raft) run() {
	for !rf.killed() {
		rf.mu.Lock()
		r := rf.r
		lastActiveTime := rf.lastActiveTime
		lastBroadcastTime := rf.lastBroadcastTime
		hasNewLog := rf.hasNewLog
		rf.mu.Unlock()
		switch r {
		case FOLLOWER:
			if time.Since(lastActiveTime) > HEARTBEAT_TIMEOUT {
				rf.requestVote()
			}
		case LEADER:
			if time.Since(lastBroadcastTime) > BROADCAST_TIMEOUT || hasNewLog {
				rf.installSnapshot()
				rf.appendEntries()
			}
		}
		rf.applyEntries()
		time.Sleep(METRICS_TIMEOUT)
	}
}

func (rf *Raft) checkLogValid(oldLastLogIndex, oldLastLogTerm int32) bool {
	lastLogIndex, lastLogTerm := rf.getLogState()
	if lastLogIndex == 0 {
		return true
	}
	if oldLastLogIndex == 0 {
		return false
	}
	if lastLogTerm > oldLastLogTerm {
		return false
	}
	if lastLogTerm == oldLastLogTerm && lastLogIndex > oldLastLogIndex {
		return false
	}
	return true
}

func (rf *Raft) updateCommitIndex() {
	lastLogIndex, _ := rf.getLogState()
	for i := lastLogIndex; i > rf.commitIndex; i-- {
		term := rf.getLogTerm(i)
		if term != rf.term {
			// NOTES: can't commit older term log.
			return
		}
		count := 1
		for j := range rf.peers {
			if j == int(rf.me) {
				continue
			}
			if rf.matchIndex[j] >= i {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			l.Log.Tracef("[raft %d] commit index %d -> %d", rf.me, rf.commitIndex, i)
			rf.commitIndex = i
			return
		}
	}
}

func (rf *Raft) getLogState() (int32, int32) {
	lastLogIndex := int32(len(rf.log)) + rf.snapshotIndex
	var lastLogTerm int32
	if len(rf.log) == 0 {
		lastLogTerm = rf.snapshotTerm
	} else {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) getLogOverview() ([]int32, []int32, []int32) {
	terms := make([]int32, 0)
	firstIndexes := make([]int32, 0)
	lastIndexes := make([]int32, 0)
	if rf.snapshotIndex != 0 {
		terms = append(terms, rf.snapshotTerm)
		firstIndexes = append(firstIndexes, rf.snapshotIndex)
		lastIndexes = append(lastIndexes, rf.snapshotIndex)
	}
	for i, entry := range rf.log {
		index := rf.snapshotIndex + int32(i) + 1
		if len(terms) == 0 || terms[len(terms)-1] != entry.Term {
			terms = append(terms, entry.Term)
			firstIndexes = append(firstIndexes, index)
			lastIndexes = append(lastIndexes, index)
		} else {
			lastIndexes[len(lastIndexes)-1] = index
		}
	}
	utils.Assert(len(terms) == len(firstIndexes), "len(terms) should be equal to len(firstIndexes)")
	utils.Assert(len(terms) == len(lastIndexes), "len(terms) should be equal to len(lastIndexes)")
	return terms, firstIndexes, lastIndexes
}

func (rf *Raft) getLogConflictIndex(peer int32, oldTerms, oldLastIndexes []int32) int32 {
	if len(oldTerms) == 0 {
		utils.Assert(rf.snapshotIndex == 0 || !rf.snapshotApplied[peer], "snapshotIndex should be 0 or snapshotApplied should be false")
		return rf.snapshotIndex + 1
	}
	terms, firstIndexes, lastIndexes := rf.getLogOverview()
	if len(terms) == 0 {
		return 1
	}
	if len(oldTerms) > len(terms) {
		oldTerms = oldTerms[:len(terms)]
		oldLastIndexes = oldLastIndexes[:len(terms)]
	}
	conflictIndex := int32(0)
	for i := range oldTerms {
		if oldTerms[i] != terms[i] {
			conflictIndex = firstIndexes[i]
			break
		}
		if oldLastIndexes[i] != lastIndexes[i] {
			if oldLastIndexes[i] < lastIndexes[i] {
				conflictIndex = oldLastIndexes[i] + 1
			} else {
				conflictIndex = lastIndexes[i] + 1
			}
			break
		}
	}
	if conflictIndex == 0 {
		conflictIndex = lastIndexes[len(oldTerms)-1] + 1
	}
	utils.Assert(conflictIndex > rf.snapshotIndex || !rf.snapshotApplied[peer], "conflictIndex should be greater than snapshotIndex or snapshotApplied should be false")
	if conflictIndex <= rf.snapshotIndex {
		conflictIndex = rf.snapshotIndex + 1
	}
	return conflictIndex
}

func (rf *Raft) getAppendEntries(peer int32) (int32, int32, []*pb.LogEntry) {
	lastLogIndex, lastLogTerm := rf.getLogState()
	if lastLogIndex == 0 {
		return 0, 0, nil
	}
	nextIndex := rf.nextIndex[peer]
	utils.Assert(nextIndex > rf.snapshotIndex, "nextIndex should be greater than snapshotIndex")
	utils.Assert(nextIndex <= lastLogIndex+1, "nextIndex should be less than or equal to lastLogIndex+1")
	if nextIndex == lastLogIndex+1 {
		return lastLogIndex, lastLogTerm, nil
	}
	entries := make([]*pb.LogEntry, 0)
	for i := nextIndex; i <= lastLogIndex; i++ {
		entry := rf.getLog(i)
		entries = append(entries, entry)
	}
	if rf.snapshotIndex != 0 && nextIndex == rf.snapshotIndex+1 {
		return rf.snapshotIndex, rf.snapshotTerm, entries
	} else {
		if nextIndex == 1 {
			return 0, 0, entries
		} else {
			term := rf.getLogTerm(nextIndex - 1)
			return nextIndex - 1, term, entries
		}
	}
}

func (rf *Raft) getLog(index int32) *pb.LogEntry {
	utils.Assert(index > rf.snapshotIndex, "index should be greater than snapshotIndex")
	utils.Assert(index <= rf.snapshotIndex+int32(len(rf.log)), "index should be less than or equal to snapshotIndex+len(log)")
	entry := rf.log[index-rf.snapshotIndex-1]
	return entry
}

func (rf *Raft) getLogTerm(index int32) int32 {
	utils.Assert(index >= rf.snapshotIndex, "index should be greater than or equal to snapshotIndex")
	utils.Assert(index <= rf.snapshotIndex+int32(len(rf.log)), "index should be less than or equal to snapshotIndex+len(log)")
	if index == rf.snapshotIndex {
		return rf.snapshotTerm
	} else {
		entry := rf.log[index-rf.snapshotIndex-1]
		return entry.Term
	}
}
