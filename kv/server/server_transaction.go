package server

import (
	"context"

	"github.com/zjregee/shardkv/common/utils"
	mvcc "github.com/zjregee/shardkv/kv/transaction"
	pb "github.com/zjregee/shardkv/proto"
)

func (kv *Server) HandleTxnGet(_ context.Context, args *pb.TxnGetArgs) (reply *pb.TxnGetReply, nullErr error) {
	reply = &pb.TxnGetReply{}
	if kv.killed() {
		reply.Err = pb.Err_ErrClosed
		return
	}
	isLeader, leaderIndex := kv.state()
	if !isLeader {
		reply.Err = pb.Err_ErrWrongLeader
		reply.LeaderIndex = leaderIndex
		return
	}
	reader := kv.storage.Reader()
	txn := mvcc.NewMvccTxn(reader, args.StartTs)
	value := txn.GetValue(args.Key)
	reply.Value = value
	return
}

func (kv *Server) HandleTxnPrewrite(_ context.Context, args *pb.TxnPrewriteArgs) (reply *pb.TxnPrewriteReply, nullErr error) {
	reply = &pb.TxnPrewriteReply{}
	if kv.killed() {
		reply.Err = pb.Err_ErrClosed
		return
	}
	isLeader, leaderIndex := kv.state()
	if !isLeader {
		reply.Err = pb.Err_ErrWrongLeader
		reply.LeaderIndex = leaderIndex
		return
	}
	reader := kv.storage.Reader()
	txn := mvcc.NewMvccTxn(reader, args.StartTs)
	for _, m := range args.Mutations {
		w, ts := txn.MostRecentWrite(m.Key)
		if w != nil && ts > args.StartTs {
			reply.Err = pb.Err_ErrConflict
			return
		}
		kv.latches.WaitForLatches([][]byte{m.Key})
		defer kv.latches.ReleaseLatches([][]byte{m.Key})
		locked, err := txn.GetLock(m.Key)
		utils.Assert(err == nil, "err should be nil")
		if locked != nil {
			reply.Err = pb.Err_ErrConflict
			return
		}
		lock := &mvcc.Lock{}
		lock.Primary = m.Key
		lock.StartTs = args.StartTs
		switch m.Kind {
		case pb.MutationKind_Put:
			txn.PutValue(m.Key, m.Value)
			lock.Kind = mvcc.WriteKindPut
		case pb.MutationKind_Append:
			txn.AppendValue(m.Key, m.Value)
			lock.Kind = mvcc.WriteKindAppend
		case pb.MutationKind_Delete:
			txn.DeleteValue(m.Key)
			lock.Kind = mvcc.WriteKindDelete
		}
		txn.PutLock(m.Key, lock)
	}
	kv.storage.Write(txn.Writes)
	return
}

func (kv *Server) HandleTxnCommit(_ context.Context, args *pb.TxnCommitArgs) (reply *pb.TxnCommitReply, nullErr error) {
	reply = &pb.TxnCommitReply{}
	if kv.killed() {
		reply.Err = pb.Err_ErrClosed
		return
	}
	isLeader, leaderIndex := kv.state()
	if !isLeader {
		reply.Err = pb.Err_ErrWrongLeader
		reply.LeaderIndex = leaderIndex
		return
	}
	reader := kv.storage.Reader()
	txn := mvcc.NewMvccTxn(reader, args.StartTs)
	kv.latches.WaitForLatches(args.Keys)
	defer kv.latches.ReleaseLatches(args.Keys)
	for _, key := range args.Keys {
		lock, err := txn.GetLock(key)
		utils.Assert(err == nil, "err should be nil")
		utils.Assert(lock != nil, "lock should not be nil")
		utils.Assert(lock.StartTs == args.StartTs, "lock.StartTs should be equal to args.StartTs")
		write := &mvcc.Write{}
		write.StartTs = args.StartTs
		write.Kind = lock.Kind
		txn.DeleteLock(key)
		txn.PutWrite(key, args.CommitTs, write)
	}
	kv.storage.Write(txn.Writes)
	return
}

func (kv *Server) HandleTxnRollback(_ context.Context, args *pb.TxnRollbackArgs) (reply *pb.TxnRollbackReply, nullErr error) {
	reply = &pb.TxnRollbackReply{}
	if kv.killed() {
		reply.Err = pb.Err_ErrClosed
		return
	}
	isLeader, leaderIndex := kv.state()
	if !isLeader {
		reply.Err = pb.Err_ErrWrongLeader
		reply.LeaderIndex = leaderIndex
		return
	}
	reader := kv.storage.Reader()
	txn := mvcc.NewMvccTxn(reader, args.StartTs)
	kv.latches.WaitForLatches(args.Keys)
	defer kv.latches.ReleaseLatches(args.Keys)
	for _, key := range args.Keys {
		write, _ := txn.CurrentWrite(key)
		if write != nil && write.Kind == mvcc.WriteKindRollback {
			continue
		}
		lock, err := txn.GetLock(key)
		utils.Assert(err == nil, "err should be nil")
		utils.Assert(lock != nil, "lock should not be nil")
		utils.Assert(lock.StartTs == args.StartTs, "lock.StartTs should be equal to args.StartTs")
		txn.DeleteLock(key)
		txn.DeleteValue(key)
	}
	kv.storage.Write(txn.Writes)
	return
}

func (kv *Server) HandleTxnCheckStatus(_ context.Context, args *pb.TxnCheckStatusArgs) (reply *pb.TxnCheckStatusReply, nullErr error) {
	reply = &pb.TxnCheckStatusReply{}
	if kv.killed() {
		reply.Err = pb.Err_ErrClosed
		return
	}
	isLeader, leaderIndex := kv.state()
	if !isLeader {
		reply.Err = pb.Err_ErrWrongLeader
		reply.LeaderIndex = leaderIndex
		return
	}
	reader := kv.storage.Reader()
	txn := mvcc.NewMvccTxn(reader, args.StartTs)
	write, _ := txn.CurrentWrite(args.PrimaryKey)
	if write != nil {
		if write.Kind != mvcc.WriteKindRollback {
			reply.LockStatus = pb.LockStatus_Commited
		} else {
			reply.LockStatus = pb.LockStatus_Rollbacked
		}
		return
	}
	lock, err := txn.GetLock(args.PrimaryKey)
	utils.Assert(err == nil, "err should be nil")
	if lock != nil {
		reply.LockStatus = pb.LockStatus_Locked
	} else {
		reply.LockStatus = pb.LockStatus_Rollbacked
	}
	return
}
