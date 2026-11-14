# Typed internal data-local query transport

This service transports **observed** query sessions and data-local fragments
using `velagraph.internal.query.v1.QueryService` over gRPC/Protobuf. It does not
download whole snapshots into a coordinator and does not wrap JSON in protobuf.
The graph-node `--graph ADDRESS` endpoint hosts snapshots and this separate
query service; no additional legacy listener is opened.

## Default coordinator transport

Start graph nodes with the usual trusted-network options and
`--graph 127.0.0.1:PORT`. Catalog `endpoint` fields name those gRPC ports:

```sh
graph-coordinator serve --endpoint 127.0.0.1:9400 \
  --catalog grpc-catalog.json --allow-insecure-development
```

`serve` exposes typed `CoordinatorService.Query` and `.Job` ingress and uses
gRPC shard backends. All query and `hierarchy-*` CLI commands use typed gRPC:

```sh
graph-coordinator query --endpoint 127.0.0.1:9400 \
  --partition 1 --query 'g.V(1)' --allow-insecure-development
```

`serve-grpc` and the `--grpc` flag are removed and rejected. `GrpcCoordinatorService`
and `CoordinatorClient` are the corresponding library APIs. Existing deployments
must migrate both ends together: `serve` no longer accepts the old TCP wire, and
unflagged clients no longer speak it. Catalog endpoints must name typed shard ports.
The hybrid library `CoordinatorService::start_grpc` (TCP ingress with gRPC
shard backends) is removed under #264. Use `GrpcCoordinatorService` and
`CoordinatorClient` together instead; the full ordered-row/error/job differential
suite now crosses typed ingress as well as typed shard transport.
`CoordinatorService::start` now aliases this same typed service; `distributed::query`,
`jobs::request` and explicit consistency APIs also use gRPC. Both ends and shard
catalog endpoints must migrate together. There is no legacy coordinator listener,
protocol probing, fallback, application-request retry or leader-following.
Convenience calls include connection establishment within the same total deadline;
use `CoordinatorClient` to reuse a channel across multiple requests.
The tonic channel may reconnect a transport connection; this is not application
replay or reopening a lost query session. A wrong port or
identity fails. One bounded channel per unique shard is configured during startup, connected
lazily only when an admitted operation needs the shard, and reused across query Open/Fragment/Close calls, rather than connecting per fragment.
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

The coordinator supports ordinary observed queries and Start/StartRows/Read/
Advance/Cancel jobs with typed outputs, failures, snapshot stamps and values.
Explicit `IndependentShardSnapshots` negotiation is represented in protobuf and
acknowledged in the response. `GlobalCommittedSnapshot` and unknown requirements
fail closed before shard contact, session creation, job allocation or token lookup.
A strict client rejects a missing/mismatched acknowledgement from an older peer;
it cannot silently accept ordinary independent-snapshot results as a global cut.
This is not distributed MVCC or managed consistency. Fixed/renewable queries have
a separate [managed-gateway contract](INTERNAL_GRPC_FIXED_QUERIES.md).

The original job state machine retains cumulative budgets, shard parallelism,
immutable page/replay behavior, cancellation and leases. GraphService library paths migrate separately under [#278](GRAPH_LIBRARY_GRPC.md). Gateway has migrated
under #276. Unix
read gateway retirement is already complete. Storage records and consensus are
unchanged; this slice does not establish production readiness or resolve #247.

The endpoint remains plaintext and unauthenticated for trusted development
networks. Matching a configured identity is not authentication. Session tokens
are private capabilities, not user credentials or authorization for public APIs.
Generated Rust is checked in; normal builds require no protoc. Maintainers can
regenerate with `scripts/generate-internal-query-proto.sh`.

## Verification

`grpc_query_process` exercises actual graph-node processes with typed coordinator
convenience and reusable clients and compares ordered results and failures,
including hierarchy count jobs, snapshot stamps and repeated Advance replay.
Codec tests cover typed shapes and malformed messages. Existing snapshot and
legacy query suites remain regression gates. Successful tests establish semantic
compatibility for this slice, not a throughput improvement or production readiness.

## Typed coordinator ingress limits and lifecycle

One shared Jobs registry and two admitted operations serve the entire endpoint.
Admission covers decode, execution and response-body consumption; accepted
connections are also bounded. Protobuf pre-scans reject oversized/repeated or
malformed structures before allocation. Existing traversal, result and retained
job ceilings remain unchanged. Server and caller remaining deadlines shorten
execution and delivery; no application request is transparently retried.

The registry expires idle jobs every 100ms. Shutdown/drop stops listening, cancels
connections/in-flight calls and releases local retained jobs/slots. Remote shard
views may remain until their existing hierarchy lease expires (120 seconds);
shutdown is not a synchronous remote-view close guarantee. An Advance timeout can
consume its registry entry; a lost Start response can leave an unknown token until
lease expiry. Neither case is proof that nothing happened. Existing replay and
cancel contracts in [hierarchy jobs](HIERARCHY_COUNT_JOBS.md) remain authoritative.
