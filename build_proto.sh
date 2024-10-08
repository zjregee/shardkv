#!/usr/bin/env bash

protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/kv.proto proto/raft.proto

go mod tidy
./format_check.sh
