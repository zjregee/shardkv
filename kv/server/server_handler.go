package kv

import (
	"context"

	c "github.com/zjregee/shardkv/common"
	pb "github.com/zjregee/shardkv/proto"
)

func (kv *Server) HandleGet(_ context.Context, args *pb.GetArgs) (reply *pb.GetReply, err error) {
	defer func() {
		c.Log.Tracef(
			"[node %d] reply get request, id=%d key=%s, value=%s, err=%s",
			kv.me, args.Id, args.Key, reply.Value, reply.Err,
		)
	}()
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
	kv.mu.Lock()
	op := Op{
		Id:   args.Id,
		Kind: "GET",
		Key:  args.Key,
	}
	result := kv.submitOp(op)
	kv.mu.Unlock()
	if result == ErrClosed || result == ErrWrongLeader || result == ErrNoKey {
		reply.Err = pb.Err_OK
	} else {
		reply.Err = pb.Err_OK
		reply.Value = result
	}
	return
}

func (kv *Server) HandleModify(_ context.Context, args *pb.ModifyArgs) (reply *pb.ModifyReply, err error) {
	defer func() {
		c.Log.Tracef(
			"[node %d] reply modify request, id=%d kind=%s key=%s, value=%s, err=%s",
			kv.me, args.Id, args.Kind, args.Key, args.Value, reply.Err,
		)
	}()
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
	kv.mu.Lock()
	op := Op{
		Id:    args.Id,
		Kind:  args.Kind,
		Key:   args.Key,
		Value: args.Value,
	}
	_ = kv.submitOp(op)
	kv.mu.Unlock()
	reply.Err = pb.Err_OK
	return
}

func (kv *Server) HandleConfigQuery(_ context.Context, args *pb.ConfigQueryArgs) (reply *pb.ConfigQueryReply, err error) {
	defer func() {
		c.Log.Tracef(
			"[node %d] reply config query request, id=%d peers=%v leader_index=%d err=%s",
			kv.me, args.Id, reply.Peers, reply.LeaderIndex, reply.Err,
		)
	}()
	if kv.killed() {
		reply.Err = pb.Err_ErrClosed
		return
	}
	reply.Err = pb.Err_OK
	reply.Peers = make([]string, len(kv.peers))
	copy(reply.Peers, kv.peers)
	_, leaderIndex := kv.state()
	reply.LeaderIndex = leaderIndex
	return
}
