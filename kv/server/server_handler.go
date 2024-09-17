package server

import (
	"context"

	c "github.com/zjregee/shardkv/common"
	pb "github.com/zjregee/shardkv/proto"
)

func (kv *Server) HandleGet(_ context.Context, args *pb.GetArgs) (reply *pb.GetReply, nullErr error) {
	reply = &pb.GetReply{}
	defer func() {
		c.Log.Infof(
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
	err, value := kv.submitOp(op)
	kv.mu.Unlock()
	reply.Err = err
	reply.Value = value
	return
}

func (kv *Server) HandleModify(_ context.Context, args *pb.ModifyArgs) (reply *pb.ModifyReply, nullErr error) {
	reply = &pb.ModifyReply{}
	defer func() {
		c.Log.Infof(
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
	err, _ := kv.submitOp(op)
	kv.mu.Unlock()
	reply.Err = err
	return
}

func (kv *Server) HandleConfigQuery(_ context.Context, args *pb.ConfigQueryArgs) (reply *pb.ConfigQueryReply, nullErr error) {
	reply = &pb.ConfigQueryReply{}
	defer func() {
		c.Log.Infof(
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
