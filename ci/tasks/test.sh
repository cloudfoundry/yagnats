#!/bin/bash
set -e

export PATH=$HOME/gopath/bin:$PATH
go get -v github.com/nats-io/gnatsd
go get gopkg.in/check.v1
go get -v ./...
go build -v ./...

go test -race
