# Managed service process preview

`graph-service` is a standalone loopback host for the
[managed HTTP preview](PUBLIC_HTTP_PREVIEW.md) and typed public managed gRPC,
using the same existing managed cohort execution and recovery identity. It is not a cluster bootstrap tool,
public production gateway, global restore controller or the final
v0.0.1 release. Both adapters share one managed gateway and its original
canonical commands, query sessions and resource budgets.

## Prerequisite and command

The coordinator and all configured members must already be live, activated and
bound to the same persisted managed authority/cohort. Each participant must have
its matching managed graph service. The CLI verifies the configured identities
through the existing `Gateway::bind_remote_local_queries` startup. It cannot
activate an arbitrary cohort, clear an offline restore marker or force READY.
No query falls back to observed per-shard views.

```sh
cargo run --locked -p replicated-graph --bin graph-service -- \
  --config service.json \
  --http 127.0.0.1:8080 \
  --grpc 127.0.0.1:8081 \
  --allow-insecure-development
```

All configuration endpoints and both public listeners must be loopback.
`--http` is required; `--grpc` is optional and explicit (omit it for the
HTTP-only preview). The two flags bind separate ports; neither protocol silently
falls back to the other. The port is explicit; port 0 requests an ephemeral loopback port. An optional
`--timeout-ms` (1..60000, default 15000) supplies the maximum request budget;
method limits, caller timeouts and session expiry can shorten it. Unknown or
repeated command flags fail before startup. No credentials or telemetry payloads
are logged.

The service writes one JSON line to stdout after both validated gateway and HTTP
listener bind, and, when requested, the gRPC listener also binds. The line
contains `pid`, `http`, `grpc` (null when omitted), `api`, and scope metadata.
A failed second bind shuts down the first listener and gateway before returning
a startup error. The state
`serving_live_managed_cohort` describes this process, **not** proof that a global
checkpoint was loaded or that future requests cannot fail. It explicitly reports
`restore_supported: false` and `production_ready: false`. The existing managed
TCP adapter is hosted on an unpublished ephemeral loopback port; that internal
compatibility listener is not a supported public endpoint.

Ctrl-C and, on Unix, SIGTERM trigger orderly gRPC, HTTP and managed-gateway
shutdown.
A startup identity mismatch or listener failure exits nonzero without publishing
a service announcement. No automatic restart, retry, graph reinitialization or
cohort replacement is performed.

## Server-owned configuration

The JSON file is at most 64 KiB, with exactly these top-level fields:

- `binding`: the existing `managed_gateway::Binding`, including authority,
  cohort and `PartitionEqualsShardV1` mapping.
- `coordinator`: `{ "endpoint": "127.0.0.1:PORT", "identity": ... }`.
- `members`: ordered entries
  `{ "endpoint": "127.0.0.1:PORT", "identity": ..., "graph_endpoint": "127.0.0.1:PORT" }`.

`identity` is the existing immutable managed RPC `Identity`: group/node,
versioned-read target, role, authority and cohort. `graph_endpoint` is the
corresponding participant's actual graph port; it is never guessed from the
managed port or accepted from a user query. The same identity supplies the graph
route's group/node. Member ordering and membership must match the binding.
Provide these values from the deployment that activated the nodes; do not copy
made-up identity values just to make a service start.

The gRPC schema is
[`public_managed.proto`](../crates/replicated-graph/proto/public_managed.proto),
with WriteService Submit/Status/Abort and QueryService Open/Fetch/Close. Rust
clients are available under `managed_gateway::public_proto`; `Context.version`
is 1 and the caller supplies a finite timeout. gRPC carries typed protobuf
values and binary tokens, while HTTP `/preview1` retains its documented JSON
preview encoding. They are semantically equivalent adapters, not the same wire
format or a claim that HTTP is protobuf JSON. A session opened over either
protocol can be fetched/closed through the other on this same gateway.

This host does not change existing internal RPC protocols. Existing managed
control and graph paths remain their currently supported transports. It does not
merge or depend on an unmerged internal gRPC change.

## Process acceptance scope

`public_service_process` starts three independent durable managed node processes
(coordinator plus two shards, each a single-replica group), a real `graph-service` process, and a separate HTTP/gRPC
client test process. The test explicitly checks five distinct PIDs. It exercises
cross-shard edge paths, HTTP-to-gRPC original-ID write replay, old fixed-V result
stability, minimum-V visibility, session fetch/close in both protocol directions,
graceful service restart and rejection of old process-local
session identities. A wrong cohort configuration or a failed second listener bind must exit
without announcing any listener.

The node activation helper is a private ignored test entrypoint for fresh test
directories. It is not a production activation or restore API. The test does not
install snapshots or bypass offline restore guards. Restarting only the service
while all data nodes remain live is **not** global recovery, node failover or
multi-host performance evidence. Run the exact submitted commit and record its
result before calling this acceptance passed:

```sh
cargo test --locked -p replicated-graph --bin graph-service
cargo test --locked -p replicated-graph --test public_service_process
```

## Deadline and delivery caveat

The public gRPC adapter uses an independent watchdog to reclaim a response
stalled by HTTP/2 flow control. At its RPC/session deadline it conservatively
closes that physical connection, so other multiplexed RPCs on the same channel
can also fail. This preview does not promise per-stream-isolated cancellation.
A disconnected/timed-out write has an unknown outcome, not an implicit rollback;
recover the same original external ID and exact mutation. A successful gRPC
trailer handoff means the transport consumed the body, not proof the socket
flushed or the client application received it. See
[the gRPC lifecycle and test boundary](PUBLIC_GRPC_SERVICE.md).
