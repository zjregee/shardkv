package server

import (
	"context"

	l "github.com/zjregee/shardkv/common/logger"
	pb "github.com/zjregee/shardkv/proto"
)

func (kv *Server) HandleConfigQuery(_ context.Context, args *pb.ConfigQueryArgs) (reply *pb.ConfigQueryReply, nullErr error) {
	reply = &pb.ConfigQueryReply{}
	defer func() {
		l.Log.Infof(
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
