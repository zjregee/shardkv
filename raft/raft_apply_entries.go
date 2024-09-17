package raft

import (
	c "github.com/zjregee/shardkv/common"
	pb "github.com/zjregee/shardkv/proto"
)

func (rf *Raft) Start(commnad []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.r != LEADER {
		return false
	}
	entry := &pb.LogEntry{
		Term:    rf.term,
		Command: commnad,
	}
	rf.log = append(rf.log, entry)
	rf.hasNewLog = true
	return true
}

func (rf *Raft) Snapshot(index int32, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.r != LEADER {
		return false
	}
	c.Assert(index > rf.snapshotIndex, "index should be greater than snapshotIndex")
	c.Assert(index <= rf.appliedIndex, "index should be greater than or equal to appliedIndex")
	c.Assert(index <= rf.commitIndex, "index should be less than or equal to commitIndex")
	term := rf.getLogTerm(index)
	discard := index - rf.snapshotIndex
	rf.log = rf.log[discard:]
	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = term
	for i := range rf.peers {
		if i == int(rf.me) {
			continue
		}
		if rf.snapshotIndex > rf.matchIndex[i] {
			rf.snapshotApplied[i] = false
			rf.nextIndex[i] = rf.snapshotIndex + 1
		}
	}
	return true
}

func (rf *Raft) applyEntries() {
	rf.mu.Lock()
	if rf.applySnapshotOnce {
		c.Assert(rf.snapshotIndex > rf.appliedIndex, "snapshotIndex should be greater than appliedIndex")
		c.Assert(rf.snapshotIndex <= rf.commitIndex, "snapshotIndex should be less than or equal to commitIndex")
		rf.applySnapshotOnce = false
		msg := ApplyMsg{
			CommandValid:  false,
			SnapshotValid: true,
			Snapshot:      rf.snapshot,
			SnapshotIndex: rf.snapshotIndex,
			SnapshotTerm:  rf.snapshotTerm,
		}
		c.Log.Tracef("[raft %d] applied index %d -> %d", rf.me, rf.appliedIndex, rf.snapshotIndex)
		rf.appliedIndex = rf.snapshotIndex
		rf.mu.Unlock()
		rf.applyCh <- msg
	} else {
		c.Assert(rf.appliedIndex <= rf.commitIndex, "appliedIndex should be less than or equal to commitIndex")
		if rf.appliedIndex == rf.commitIndex {
			rf.mu.Unlock()
			return
		}
		result := make([]ApplyMsg, 0)
		for i := rf.appliedIndex + 1; i <= rf.commitIndex; i++ {
			entry := rf.getLog(i)
			msg := ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false,
				Command:       entry.Command,
				CommandIndex:  i,
			}
			result = append(result, msg)
		}
		c.Log.Tracef("[raft %d] applied index %d -> %d", rf.me, rf.appliedIndex, rf.commitIndex)
		rf.appliedIndex = rf.commitIndex
		rf.mu.Unlock()
		for _, msg := range result {
			rf.applyCh <- msg
		}
	}
}
