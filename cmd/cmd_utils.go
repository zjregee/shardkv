package main

import (
	"context"
	"fmt"
	"strings"

	c "github.com/zjregee/shardkv/common"
	pb "github.com/zjregee/shardkv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type session struct {
	peers       []*grpc.ClientConn
	leaderIndex int32
}

func newSession(peers []string, leaderIndex int32) (*session, error) {
	session := &session{}
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

func Get(session *session, args ...string) (string, error) {
	if len(args) != 2 {
		return "", fmt.Errorf("usage: GET <key>")
	}
	getArgs := pb.GetArgs{
		Id:  c.Nrand(),
		Key: args[1],
	}
	for {
		if session.leaderIndex == -1 {
			session.leaderIndex = int32(c.Nrand()) % int32(len(session.peers))
		}
		client := pb.NewKvServiceClient(session.peers[session.leaderIndex])
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
		reply, err := client.HandleGet(ctx, &getArgs)
		cancel()
		if err != nil {
			session.leaderIndex = -1
			continue
		}
		switch reply.Err {
		case pb.Err_ErrClosed, pb.Err_ErrWrongLeader:
			session.leaderIndex = -1
		case pb.Err_OK:
			return reply.Value, nil
		case pb.Err_ErrNoKey:
			return "", nil
		}
	}
}

func Modify(session *session, args ...string) (string, error) {
	command := strings.ToUpper(args[0])
	if command == "SET" || command == "APPEND" {
		if len(args) != 3 {
			return "", fmt.Errorf("usage: %s <key> <value>", command)
		}
	} else {
		if len(args) != 2 {
			return "", fmt.Errorf("usage: %s <key>", command)
		}
	}
	modifyArgs := pb.ModifyArgs{
		Id:    c.Nrand(),
		Kind:  command,
		Key:   args[1],
		Value: "",
	}
	if command != "DEL" {
		modifyArgs.Value = args[2]
	}
	for {
		if session.leaderIndex == -1 {
			session.leaderIndex = int32(c.Nrand()) % int32(len(session.peers))
		}
		client := pb.NewKvServiceClient(session.peers[session.leaderIndex])
		ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
		reply, err := client.HandleModify(ctx, &modifyArgs)
		cancel()
		if err != nil {
			session.leaderIndex = -1
			continue
		}
		switch reply.Err {
		case pb.Err_ErrClosed, pb.Err_ErrWrongLeader:
			session.leaderIndex = -1
		case pb.Err_OK, pb.Err_Duplicate:
			return "OK", nil
		}
	}
}
