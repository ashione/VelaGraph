#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
test "$(protoc --version)" = "libprotoc 29.3"
test "$(protoc-gen-go --version)" = "protoc-gen-go v1.36.5"
test "$(protoc-gen-go-grpc --version)" = "protoc-gen-go-grpc 1.5.1"
mkdir -p examples/go/managed/gen/managedv1
protoc -I crates/replicated-graph/proto \
  --go_out=examples/go/managed/gen/managedv1 --go_opt=paths=source_relative \
  --go_opt=Mpublic_managed.proto=example.com/velagraph/managed/gen/managedv1 \
  --go-grpc_out=examples/go/managed/gen/managedv1 --go-grpc_opt=paths=source_relative \
  --go-grpc_opt=Mpublic_managed.proto=example.com/velagraph/managed/gen/managedv1 \
  public_managed.proto
