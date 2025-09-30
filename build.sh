#!/bin/bash
set -eo pipefail

echo "Building chrsplit..."

echo "linux/amd64"
GOOS=linux   GOARCH=amd64  go build  -o bin/chrsplit      -ldflags "-s -w" -trimpath ./

echo "darwin/amd64"
GOOS=darwin  GOARCH=arm64  go build  -o bin/chrsplit_mac  -ldflags "-s -w" -trimpath ./

echo "windows/amd64"
GOOS=windows GOARCH=amd64  go build  -o bin/chrsplit.exe  -ldflags "-s -w" -trimpath ./
