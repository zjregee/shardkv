package raft

import (
	"context"
	"sync"
	"time"

	c "github.com/zjregee/shardkv/common"
	pb "github.com/zjregee/shardkv/proto"
)

func (rf *Raft) HandleInstallSnapshot(_ context.Context, args *pb.InstallSnapshotArgs) (reply *pb.InstallSnapshotReply, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		c.Log.Tracef(
			"[raft %d] reply install snapshot to %d leader_term=%d snapshot_index=%d snapshot_term=%d success=%t",
			rf.me, args.LeaderIndex, args.LeaderTerm, args.SnapshotIndex, args.SnapshotTerm, reply.Success,
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
	reply.Term = rf.votedTerm
	reply.Success = true
	if args.SnapshotIndex <= rf.appliedIndex {
		return
	}
	c.Log.Infof("[raft %d] snapshot index %d -> %d", rf.me, rf.appliedIndex, args.SnapshotIndex)
	rf.snapshot = args.Snapshot
	rf.snapshotTerm = args.SnapshotTerm
	rf.snapshotIndex = args.SnapshotIndex
	rf.applySnapshotOnce = true
	discard := args.SnapshotIndex - rf.snapshotIndex
	if discard >= int32(len(rf.log)) {
		rf.log = make([]*pb.LogEntry, 0)
	} else {
		rf.log = rf.log[discard:]
	}
	if args.SnapshotIndex > rf.commitIndex {
		c.Log.Infof("[raft %d] commit index %d -> %d", rf.me, rf.commitIndex, args.SnapshotIndex)
		rf.commitIndex = args.SnapshotIndex
	}
	return
}

type installSnapshotReplyWithIndex struct {
	Index         int
	SnapshotIndex int32
	Reply         *pb.InstallSnapshotReply
}

func (rf *Raft) callInstallSnapshotWithTimeout(peer int, args *pb.InstallSnapshotArgs, timeout time.Duration) (*pb.InstallSnapshotReply, error) {
	client := pb.NewRaftServiceClient(rf.peers[peer])
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.HandleInstallSnapshot(ctx, args)
}

func (rf *Raft) installSnapshot() {
	rf.mu.Lock()
	if rf.r != LEADER || rf.killed() {
		rf.mu.Unlock()
		return
	}
	shouldInstall := false
	for i := range rf.peers {
		if i == int(rf.me) || rf.snapshotApplied[i] {
			continue
		}
		shouldInstall = true
		break
	}
	args := &pb.InstallSnapshotArgs{
		LeaderTerm:    rf.term,
		LeaderIndex:   rf.me,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.snapshotTerm,
		SnapshotIndex: rf.snapshotIndex,
	}
	rf.mu.Unlock()
	if !shouldInstall {
		return
	}
	resultChan := make(chan *installSnapshotReplyWithIndex, len(rf.peers)-1)
	var wg sync.WaitGroup
	for i := range rf.peers {
		rf.mu.Lock()
		if i == int(rf.me) || rf.snapshotApplied[i] {
			rf.mu.Unlock()
			continue
		}
		wg.Add(1)
		go func(peer int) {
			defer wg.Done()
			reply, err := rf.callInstallSnapshotWithTimeout(peer, args, RPC_TIMEOUT)
			if err != nil {
				c.Log.Warnf("[raft %d] install snapshot to %d failed: %v", rf.me, peer, err)
				return
			}
			resultChan <- &installSnapshotReplyWithIndex{peer, args.SnapshotIndex, reply}
		}(i)
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
		if !result.Reply.Success {
			c.Log.Infof("[raft %d] role LEADER -> FOLLOWER", rf.me)
			rf.r = FOLLOWER
			rf.votedTerm = result.Reply.Term
			rf.leaderIndex = -1
			rf.lastActiveTime = time.Now()
			rf.mu.Unlock()
			return
		}
		if rf.snapshotIndex == result.SnapshotIndex {
			rf.snapshotApplied[result.Index] = true
			rf.matchIndex[result.Index] = rf.snapshotIndex
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
