package main

import (
	"flag"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	l "github.com/zjregee/shardkv/common/logger"
	"github.com/zjregee/shardkv/kv/server"
)

type addrList []string

func (l *addrList) String() string {
	return strings.Join(*l, ",")
}

func (l *addrList) Set(value string) error {
	*l = strings.Split(value, ",")
	return nil
}

func main() {
	var kv_peers addrList
	var raft_peers addrList
	var index int
	flag.Var(&kv_peers, "kv_peers", "comma-separated list of peer addresses used in kv")
	flag.Var(&raft_peers, "raft_peers", "comma-separated list of peer addresses used in raft")
	flag.IntVar(&index, "index", -1, "index of the current peer")
	flag.Parse()
	svr := server.MakeServer(kv_peers, raft_peers, int32(index))
	l.Log.Infof("Starting server at index %d with kv_peers: %v, raft_peers: %v", index, kv_peers, raft_peers)
	svr.Serve()
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, syscall.SIGINT, syscall.SIGTERM)
	<-stopChan
	l.Log.Infoln("Shutting down server...")
	svr.Kill()
	time.Sleep(time.Second)
}
