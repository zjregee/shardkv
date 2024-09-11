package kv

import (
	"context"

	pb "github.com/zjregee/shardkv/proto"
)

func (kv *Server) HandleGet(_ context.Context, args *pb.GetArgs) (reply *pb.GetReply, err error) {
	reply = &pb.GetReply{}
	if kv.killed() {
		reply.Err = ErrClosed
		return
	}
	isLeader, leaderIndex := kv.state()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.LeaderIndex = leaderIndex
		return
	}
	kv.mu.Lock()
	op := Op{
		Id:   args.Id,
		Kind: "Get",
		Key:  args.Key,
	}
	result := kv.submitOp(op)
	kv.mu.Unlock()
	if result == ErrClosed || result == ErrWrongLeader || result == ErrNoKey {
		reply.Err = result
	} else {
		reply.Err = OK
		reply.Value = result
	}
	return
}

func (kv *Server) HandlePut(_ context.Context, args *pb.PutArgs) (reply *pb.PutReply, err error) {
	reply = &pb.PutReply{}
	if kv.killed() {
		reply.Err = ErrClosed
		return
	}
	isLeader, leaderIndex := kv.state()
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.LeaderIndex = leaderIndex
		return
	}
	kv.mu.Lock()
	op := Op{
		Id:    args.Id,
		Kind:  "Put",
		Key:   args.Key,
		Value: args.Value,
	}
	result := kv.submitOp(op)
	kv.mu.Unlock()
	reply.Err = result
	return
}
