package raft

import (
	"context"
	"math/rand"
	"sync"
	"time"

	c "github.com/zjregee/shardkv/common"
	pb "github.com/zjregee/shardkv/proto"
)

func (rf *Raft) HandleRequestVote(_ context.Context, args *pb.RequestVoteArgs) (reply *pb.RequestVoteReply, err error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		c.Log.Tracef(
			"[raft %d] reply request vote to %d, candidate_term=%d last_log_index=%d last_log_term=%d success=%t",
			rf.me, args.CandidateIndex, args.CandidateTerm, args.LastLogIndex, args.LastLogTerm, reply.Success,
		)
	}()
	if args.CandidateTerm <= rf.votedTerm || !rf.checkLogValid(args.LastLogTerm, args.LastLogIndex) {
		reply.Term = rf.votedTerm
		reply.Success = false
		return
	}
	if rf.r != FOLLOWER {
		c.Log.Infof("[raft %d] role %s -> FOLLOWER", rf.me, rf.r)
		rf.r = FOLLOWER
	}
	rf.votedTerm = args.CandidateTerm
	rf.leaderIndex = -1
	rf.lastActiveTime = time.Now()
	reply.Term = rf.votedTerm
	reply.Success = true
	return
}

func (rf *Raft) callRequestVoteWithTimeout(peer int, args *pb.RequestVoteArgs, timeout time.Duration) (*pb.RequestVoteReply, error) {
	client := pb.NewRaftServiceClient(rf.peers[peer])
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return client.HandleRequestVote(ctx, args)
}

func (rf *Raft) requestVote() {
	rf.mu.Lock()
	if rf.r != FOLLOWER || rf.killed() || time.Since(rf.lastActiveTime) <= HEARTBEAT_TIMEOUT {
		rf.mu.Unlock()
		return
	}
	c.Log.Infof("[raft %d] role FOLLOWER -> CANDIDATES", rf.me)
	rf.r = CANDIDATES
	term := rf.term
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		if rf.r != CANDIDATES || rf.killed() {
			rf.mu.Unlock()
			return
		}
		c.Assert(term == rf.term, "term should be equal to rf.term")
		rf.votedTerm += 1
		c.Log.Infof("[raft %d] start election, term %d", rf.me, rf.votedTerm)
		lastLogTerm, lastLogIndex := rf.getLogState()
		args := &pb.RequestVoteArgs{
			CandidateTerm:  rf.votedTerm,
			CandidateIndex: rf.me,
			LastLogTerm:    lastLogTerm,
			LastLogIndex:   lastLogIndex,
		}
		rf.mu.Unlock()
		resultChan := make(chan *pb.RequestVoteReply, len(rf.peers)-1)
		var wg sync.WaitGroup
		for i := range rf.peers {
			if i == int(rf.me) {
				continue
			}
			wg.Add(1)
			go func(peer int) {
				defer wg.Done()
				reply, err := rf.callRequestVoteWithTimeout(peer, args, RPC_TIMEOUT)
				if err != nil {
					c.Log.Warnf("[raft %d] request vote from %d error: %v", rf.me, peer, err)
					return
				}
				resultChan <- reply
			}(i)
		}
		go func() {
			wg.Wait()
			close(resultChan)
		}()
		maxTerm := int32(0)
		successCount := 1
		for reply := range resultChan {
			if reply.Term > maxTerm {
				maxTerm = reply.Term
			}
			if reply.Success {
				successCount += 1
			}
		}
		rf.mu.Lock()
		if rf.r != CANDIDATES || rf.killed() {
			rf.mu.Unlock()
			return
		}
		c.Assert(term == rf.term, "term should be equal to rf.term")
		if successCount > len(rf.peers)/2 {
			c.Log.Infof("[raft %d] win the election, term %d", rf.me, rf.votedTerm)
			c.Log.Infof("[raft %d] role CANDIDATES -> LEADER", rf.me)
			rf.r = LEADER
			rf.term = rf.votedTerm
			rf.leaderIndex = rf.me
			rf.lastBroadcastTime = time.Now()
			_, lastLogIndex = rf.getLogState()
			for i := range rf.peers {
				rf.nextIndex[i] = lastLogIndex + 1
				rf.matchIndex[i] = 0
				rf.snapshotApplied[i] = rf.snapshotIndex == 0
			}
			rf.mu.Unlock()
			return
		}
		if maxTerm > rf.votedTerm {
			rf.votedTerm = maxTerm
		}
		rf.mu.Unlock()
		// NOTES: if becomes follower during sleep, will not be able to wake up
		// quickly. But it only affects the speed of apply log, so it's ok for now.
		time.Sleep(time.Duration(rand.Int63n(int64(CANDIDATES_TIMEOUT))))
	}
}
