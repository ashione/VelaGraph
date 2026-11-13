# Typed internal data-local query transport

This opt-in slice transports **observed** query sessions and data-local fragments
using `velagraph.internal.query.v1.QueryService` over gRPC/Protobuf. It does not
download whole snapshots into a coordinator and does not wrap JSON in protobuf.
The existing graph-node `--grpc-snapshot ADDRESS` endpoint hosts both the snapshot
service and this separate query service during migration.

## Explicit selection

Start graph nodes with the usual trusted-network options plus
`--grpc-snapshot 127.0.0.1:PORT`. Construct a separate static catalog whose
`endpoint` fields name those gRPC ports, then run:

```sh
graph-coordinator serve-grpc --endpoint 127.0.0.1:9400 \
  --catalog grpc-catalog.json --allow-insecure-development
```

The ordinary `query` client still talks to the coordinator's existing interface.
`CoordinatorService::start_grpc` is the equivalent library entrypoint. Existing
`serve`/`CoordinatorService::start` continue to explicitly select TCP. There is no
protocol probing, fallback, application-request retry, or leader-following.
The tonic channel may reconnect a transport connection; this is not application
replay or reopening a lost query session. A wrong port or
identity fails. One channel per unique shard is established during startup and
reused across query Open/Fragment/Close calls, rather than connecting per fragment.
Hierarchy jobs use typed OpenHierarchy (including the conservative stored-row byte
bound), followed by the same typed Fragment/Close calls. Each job retains its
selected clients; it never falls back to the old shard transport.

## Preserved semantics

- Open freezes the same existing shard-local read view after the same leader
  barrier. Its descriptor binds identity, shard, epoch, sequence, incarnation and
  capability token. This is **not** a globally consistent snapshot.
- Fragments cover existing existence, outgoing/incoming adjacency, reverse-edge
  verification, property predicates, labels, values, property projection, scans
  and hierarchy batches. Gremlin parsing and execution rules are unchanged.
- Scan continuation and results are typed fields; order, duplicates, IDs and
  property value types are preserved. Query failures remain separate from
  transport failures, with no partial-success output.
- Both listeners share the **actual same query session registry** and original
  execution/admission limits. A descriptor can be used/closed through either
  transport; restarting the node still invalidates its session capabilities.
- Caller remaining deadlines are transmitted and shortened by server deadlines.
  Cancellation does not claim to roll back completed work; existing fixed session
  TTL remains the cleanup boundary after a disconnected/cancelled caller.
- Shared operation admission is held through protobuf decoding, execution and
  response-body consumption. HTTP/2 flow control does not replace application
  admission. Slow readers retain their permits until their bodies are released.
- Existing traversal accounting remains conservative, including its existing
  legacy-encoding cost estimates; these are accounting checks, **not JSON network
  payloads**. No traversal, session, byte, member or result cap is raised.

## Deliberate exclusions and removal plan

The gRPC-backed coordinator supports observed hierarchy count/row jobs but still
rejects explicit consistency requests with typed `Unsupported`. Fixed/renewable
query sessions have a separate [managed-gateway gRPC entrypoint](INTERNAL_GRPC_FIXED_QUERIES.md).
The coordinator's **client-facing query/job envelope is still legacy TCP/JSON**;
this slice migrates only coordinator-to-shard job sessions. Managed control and
Raft also remain separate migrations. Do not interpret this slice as completion
of the protocol-unification roadmap.

The original job state machine retains cumulative budgets, shard parallelism,
immutable page/replay behavior, cancellation and leases. Explicit TCP selection
is retained for differential tests until all callers migrate; it is not a second
query engine. Storage record encoding and consensus are unchanged.

The endpoint remains plaintext and unauthenticated for trusted development
networks. Matching a configured identity is not authentication. Session tokens
are private capabilities, not user credentials or authorization for public APIs.
Generated Rust is checked in; normal builds require no protoc. Maintainers can
regenerate with `scripts/generate-internal-query-proto.sh`.

## Verification

`grpc_query_process` exercises actual graph-node processes with TCP/gRPC
coordinators against the same data and compares ordered results and failures,
including hierarchy count jobs, snapshot stamps and repeated Advance replay.
Codec tests cover typed shapes and malformed messages. Existing snapshot and
legacy query suites remain regression gates. Successful tests establish semantic
compatibility for this slice, not a throughput improvement or production readiness.
