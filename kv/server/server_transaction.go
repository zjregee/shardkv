package server

import (
	"context"

	pb "github.com/zjregee/shardkv/proto"
)

func (kv *Server) HandleTxnGet(_ context.Context, args *pb.TxnGetArgs) (reply *pb.TxnGetReply, nullErr error) {
	reply = &pb.TxnGetReply{}
	return
}

func (kv *Server) HandleTxnPrewrite(_ context.Context, args *pb.TxnPrewriteArgs) (reply *pb.TxnPrewriteReply, nullErr error) {
	reply = &pb.TxnPrewriteReply{}
	return
}

func (kv *Server) HandleTxnCommit(_ context.Context, args *pb.TxnCommitArgs) (reply *pb.TxnCommitReply, nullErr error) {
	reply = &pb.TxnCommitReply{}
	return
}
