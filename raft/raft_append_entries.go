package raft

import (
	"context"
	"sync"
	"time"

	c "github.com/zjregee/shardkv/common"
	pb "github.com/zjregee/shardkv/proto"
)

func (rf *Raft) HandleAppendEntries(_ context.Context, args *pb.AppendEntriesArgs) (reply *pb.AppendEntriesReply, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		c.Log.Tracef(
			"[raft %d] reply append entries to %d, leader_term=%d leader_commit=%d prev_log_index=%d prev_log_term=%d entries_num=%d success=%t",
			rf.me, args.LeaderIndex, args.LeaderTerm, args.LeaderCommit, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries), reply.Success,
		)
	}()
	if (rf.r == CANDIDATES && args.LeaderTerm < rf.term) || (rf.r != CANDIDATES && args.LeaderTerm < rf.votedTerm) {
		reply.Term = rf.votedTerm
		reply.Success = false
		return
	}
	if rf.r != FOLLOWER {
		c.Log.Infof("[raft %d] role %s -> FOLLOWER", rf.me, rf.r)
		rf.r = FOLLOWER
	}
	if rf.term != args.LeaderTerm {
		c.Log.Infof("[raft %d] term %d -> %d", rf.me, rf.term, args.LeaderTerm)
		rf.term = args.LeaderTerm
	}
	rf.votedTerm = args.LeaderTerm
	rf.leaderIndex = args.LeaderIndex
	rf.lastActiveTime = time.Now()
	c.Assert(args.PrevLogIndex >= rf.snapshotIndex, "prevLogIndex should be greater than or equal to snapshotIndex")
	lastLogIndex, _ := rf.getLogState()
	if lastLogIndex < args.PrevLogIndex || (args.PrevLogIndex > 0 && rf.getLogTerm(args.PrevLogIndex) != args.PrevLogTerm) {
		terms, _, lastIndexes := rf.getLogOverview()
		reply.Term = rf.term
		reply.Success = false
		reply.Terms = terms
		reply.LastIndexes = lastIndexes
		return
	}
	reply.Term = rf.term
	reply.Success = true
	if lastLogIndex > args.PrevLogIndex+int32(len(args.Entries)) {
		rf.log = rf.log[:args.PrevLogIndex+int32(len(args.Entries))-rf.snapshotIndex]
	}
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + int32(i) + 1
		if index > lastLogIndex {
			rf.log = append(rf.log, entry)
		} else {
			rf.log[index-rf.snapshotIndex-1] = entry
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		c.Log.Infof("[raft %d] commit index %d -> %d", rf.me, rf.commitIndex, args.LeaderCommit)
		rf.commitIndex = args.LeaderCommit
	}
	return
}

type appendEntriesReplyWithIndex struct {
	Index      int
	EntriesNum int32
	Reply      *pb.AppendEntriesReply
}

func (rf *Raft) callAppendEntriesWithTimeout(peer int, args *pb.AppendEntriesArgs, timeout time.Duration) (*pb.AppendEntriesReply, error) {
	client := pb.NewRaftServiceClient(rf.peers[peer])
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.HandleAppendEntries(ctx, args)
}

func (rf *Raft) appendEntries() {
	rf.mu.Lock()
	if rf.r != LEADER || rf.killed() {
		rf.mu.Unlock()
		return
	}
	shouldBroadcast := false
	for i := range rf.peers {
		if i == int(rf.me) || !rf.snapshotApplied[i] {
			continue
		}
		shouldBroadcast = true
		break
	}
	rf.mu.Unlock()
	if !shouldBroadcast {
		return
	}
	resultChan := make(chan *appendEntriesReplyWithIndex, len(rf.peers)-1)
	var wg sync.WaitGroup
	for i := range rf.peers {
		rf.mu.Lock()
		if i == int(rf.me) || !rf.snapshotApplied[i] {
			rf.mu.Unlock()
			continue
		}
		prevLogIndex, prevLogTerm, entries := rf.getAppendEntries(i)
		args := &pb.AppendEntriesArgs{
			LeaderTerm:   rf.term,
			LeaderIndex:  rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
			Entries:      entries,
		}
		rf.mu.Unlock()
		wg.Add(1)
		go func(peer int, args *pb.AppendEntriesArgs) {
			defer wg.Done()
			reply, err := rf.callAppendEntriesWithTimeout(peer, args, RPC_TIMEOUT)
			if err != nil {
				c.Log.Errorf("[raft %d] append entries to %d failed: %v", rf.me, peer, err)
				return
			}
			resultChan <- &appendEntriesReplyWithIndex{peer, int32(len(args.Entries)), reply}
		}(i, args)
	}
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	for result := range resultChan {
		rf.mu.Lock()
		if rf.r != LEADER || rf.killed() {
			rf.mu.Unlock()
			return
		}
		if !result.Reply.Success && result.Reply.Term > rf.term {
			c.Log.Infof("[raft %d] role LEADER -> FOLLOWER", rf.me)
			rf.r = FOLLOWER
			rf.votedTerm = result.Reply.Term
			rf.leaderIndex = -1
			rf.lastActiveTime = time.Now()
			rf.mu.Unlock()
			return
		}
		if result.Reply.Success {
			rf.nextIndex[result.Index] += result.EntriesNum
			rf.matchIndex[result.Index] = rf.nextIndex[result.Index] - 1
		} else {
			rf.nextIndex[result.Index] = rf.getLogConflictIndex(result.Reply.Terms, result.Reply.LastIndexes)
		}
		rf.mu.Unlock()
	}
	rf.mu.Lock()
	if rf.r != LEADER || rf.killed() {
		rf.mu.Unlock()
		return
	}
	rf.updateCommitIndex()
	rf.mu.Unlock()
}
