package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/zjregee/shardkv/common/utils"
	pb "github.com/zjregee/shardkv/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	RPC_TIMEOUT = time.Second * 3
)

const (
	red   = "\033[31m"
	green = "\033[32m"
	reset = "\033[0m"
)

var commands = map[string]func(*session, ...string) (string, error){
	"GET":    Get,
	"SET":    Modify,
	"APPEND": Modify,
	"DEL":    Modify,
	"MULTI":  Modify,
	"EXEC":   Modify,
}

func repl(session *session) {
	scanner := bufio.NewScanner(os.Stdin)
	success := true
	fmt.Println("Type 'cluster-info' to get cluster information or 'exit' to quit.")
	for {
		if !success {
			success = true
			fmt.Print(red + "> " + reset)
		} else {
			fmt.Print(green + "> " + reset)
		}
		if !scanner.Scan() {
			break
		}
		input := scanner.Text()
		input = strings.TrimSpace(input)
		if input == "exit" {
			break
		}
		if input == "clear" {
			fmt.Print("\033[H\033[2J")
			continue
		}
		if input == "cluster-info" {
			fmt.Println("Cluster info:")
			fmt.Println("  Peers:")
			for i, peer := range session.peers {
				fmt.Printf("   node %d: %s\n", i, peer.Target())
			}
			fmt.Printf("  Leader Index: %d\n", session.leaderIndex)
			continue
		}
		args := strings.Fields(input)
		if len(args) == 0 {
			continue
		}
		command := strings.ToUpper(args[0])
		handler, ok := commands[command]
		if !ok {
			success = false
			fmt.Println("unknown command")
			continue
		}
		result, err := handler(session, args...)
		if err != nil {
			success = false
			fmt.Println(err.Error())
		} else {
			fmt.Println(result)
		}
	}
}

func main() {
	var port int
	var address string
	flag.IntVar(&port, "port", 8000, "Port of any server in the ShardKV cluster.")
	flag.StringVar(&address, "address", "127.0.0.1", "Address of any server in the ShardKV cluster.")
	flag.Parse()
	target := fmt.Sprintf("%s:%d", address, port)
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("Failed to connect to the ShardKV cluster.")
		return
	}
	queryArgs := pb.ConfigQueryArgs{
		Id: utils.Nrand(),
	}
	client := pb.NewKvServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), RPC_TIMEOUT)
	reply, err := client.HandleConfigQuery(ctx, &queryArgs)
	cancel()
	if err != nil || reply.Err != pb.Err_OK {
		fmt.Println("Failed to connect to the ShardKV cluster.")
		return
	}
	session, err := newSession(reply.Peers, reply.LeaderIndex)
	if err != nil {
		fmt.Println("Failed to connect to the ShardKV cluster.")
		return
	}
	repl(session)
}
