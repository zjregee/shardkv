package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zjregee/shardkv/common/utils"
	pb "github.com/zjregee/shardkv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type session struct {
	isInMultiTxn bool
	startTs      uint64
	buffers      *buffers
	peers        []*grpc.ClientConn
	leaderIndex  int32
}

func newSession(peers []string, leaderIndex int32) (*session, error) {
	session := &session{}
	session.isInMultiTxn = false
	session.peers = make([]*grpc.ClientConn, 0)
	session.leaderIndex = leaderIndex
	for _, peer := range peers {
		conn, err := grpc.NewClient(peer, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, err
		}
		session.peers = append(session.peers, conn)
	}
	return session, nil
}

func (session *session) Close() {
	for _, conn := range session.peers {
		_ = conn.Close()
	}
}

func (session *session) TxnGet(args ...string) string {
	if len(args) != 2 {
		return "usage: GET <key>"
	}
	getArgs := &pb.TxnGetArgs{}
	getArgs.Id = utils.Nrand()
	getArgs.Key = []byte(args[0])
	if session.isInMultiTxn {
		getArgs.StartTs = session.startTs
	} else {
		getArgs.StartTs = uint64(time.Now().UnixMilli())
	}
	for {
		if session.leaderIndex == -1 {
			session.leaderIndex = int32(utils.Nrand()) % int32(len(session.peers))
		}
		client := pb.NewKvServiceClient(session.peers[session.leaderIndex])
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
		reply, err := client.HandleTxnGet(ctx, getArgs)
		cancel()
		if err != nil {
			session.leaderIndex = -1
			continue
		}
		switch reply.Err {
		case pb.Err_ErrClosed, pb.Err_ErrWrongLeader:
			session.leaderIndex = -1
		case pb.Err_OK:
			return string(reply.Value)
		case pb.Err_ErrNoKey:
			return ""
		}
	}
}

func (session *session) TxnModify(args ...string) string {
	kind := strings.ToUpper(args[0])
	if kind == "SET" || kind == "APPEND" {
		if len(args) != 3 {
			return fmt.Sprintf("usage: %s <key> <value>", kind)
		}
	} else {
		if len(args) != 2 {
			return fmt.Sprintf("usage: %s <key> <value>", kind)
		}
	}
	m := &pb.Mutation{}
	m.Key = []byte(args[1])
	switch kind {
	case "SET":
		m.Kind = pb.MutationKind_Put
		m.Value = []byte(args[2])
	case "APPEND":
		m.Kind = pb.MutationKind_Append
		m.Value = []byte(args[2])
	case "DEL":
		m.Kind = pb.MutationKind_Delete
	}
	if session.isInMultiTxn {
		session.buffers.addMutation(m)
		return ""
	}
	return session.commitTxn([]*pb.Mutation{m})
}

func (session *session) TxnMulti() string {
	if session.isInMultiTxn {
		return ""
	}
	session.isInMultiTxn = true
	session.startTs = uint64(time.Now().UnixMilli())
	session.buffers = newBuffers()
	return ""
}

func (session *session) TxnExec() string {
	if !session.isInMultiTxn {
		return ""
	}
	session.isInMultiTxn = false
	return session.commitTxn(session.buffers.mutations())
}

func (session *session) commitTxn(ms []*pb.Mutation) string {
	prewriteArgs := &pb.TxnPrewriteArgs{}
	prewriteArgs.Id = utils.Nrand()
	prewriteArgs.StartTs = session.startTs
	prewriteArgs.Mutations = ms
	success := false
	for !success {
		if session.leaderIndex == -1 {
			session.leaderIndex = int32(utils.Nrand()) % int32(len(session.peers))
		}
		client := pb.NewKvServiceClient(session.peers[session.leaderIndex])
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
		reply, err := client.HandleTxnPrewrite(ctx, prewriteArgs)
		cancel()
		if err != nil {
			session.leaderIndex = -1
			continue
		}
		switch reply.Err {
		case pb.Err_ErrClosed, pb.Err_ErrWrongLeader:
			session.leaderIndex = -1
		case pb.Err_OK, pb.Err_Duplicate:
			success = true
		case pb.Err_ErrConflict:
			return "can't commit due to transaction conflict"
		}
	}
	commitArgs := &pb.TxnCommitArgs{}
	commitArgs.Id = utils.Nrand()
	commitArgs.StartTs = session.startTs
	commitArgs.CommitTs = uint64(time.Now().UnixMilli())
	commitArgs.Keys = make([][]byte, 0)
	for _, m := range ms {
		commitArgs.Keys = append(commitArgs.Keys, m.Key)
	}
	for {
		if session.leaderIndex == -1 {
			session.leaderIndex = int32(utils.Nrand()) % int32(len(session.peers))
		}
		client := pb.NewKvServiceClient(session.peers[session.leaderIndex])
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
		reply, err := client.HandleTxnCommit(ctx, commitArgs)
		cancel()
		if err != nil {
			session.leaderIndex = -1
			continue
		}
		switch reply.Err {
		case pb.Err_ErrClosed, pb.Err_ErrWrongLeader:
			session.leaderIndex = -1
		case pb.Err_OK, pb.Err_Duplicate:
			return ""
		}
	}
}
