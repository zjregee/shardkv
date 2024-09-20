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
	reader, err := kv.storage.Reader()
	utils.Assert(err == nil, "err should be nil")
	txn := mvcc.NewMvccTxn(reader, args.StartVersion)
	value, err := txn.GetValue(args.Key)
	utils.Assert(err == nil, "err should be nil")
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
	reader, err := kv.storage.Reader()
	utils.Assert(err == nil, "err should be nil")
	txn := mvcc.NewMvccTxn(reader, args.StartVersion)
	for _, m := range args.Mutations {
		w, ts, err := txn.MostRecentWrite(m.Key)
		utils.Assert(err == nil, "err should be nil")
		if w != nil && ts > args.StartVersion {
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
		lock.StartTs = args.StartVersion
		switch m.Op {
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
	err = kv.storage.Write(txn.Writes)
	utils.Assert(err == nil, "err should be nil")
	return
}

func (kv *Server) HandleTxnCommit(_ context.Context, args *pb.TxnCommitArgs) (reply *pb.TxnCommitReply, nullErr error) {
	reply = &pb.TxnCommitReply{}
	return
}
