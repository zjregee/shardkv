#!/usr/bin/env bash

goimports -w .
golangci-lint run ./...
