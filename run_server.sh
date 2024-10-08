#!/usr/bin/env bash

go run kv/main.go \
    -kv_peers="127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002" \
    -raft_peers="127.0.0.1:8000,127.0.0.1:8001,127.0.0.1:8002" \
    -index=$1
