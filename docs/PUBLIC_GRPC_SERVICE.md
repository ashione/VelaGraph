# Typed public managed gRPC slice

The loopback service exposes `velagraph.public.managed.v1.WriteService`
(Submit, Status, Abort) and `QueryService` (Open, Fetch, Close). The schema is
[`public_managed.proto`](../crates/replicated-graph/proto/public_managed.proto);
generated Rust clients and messages are exported at
`replicated_graph::managed_gateway::public_proto`. This is an experimental
fixed-topology service, not a production release or the complete #218 migration.

Use [graph-service](PUBLIC_SERVICE_CLI.md) with explicit `--http` and `--grpc`
loopback addresses, or attach `Gateway::bind_grpc` to an already validated live
managed gateway. The returned service provides `local_addr` and `shutdown`.
The startup path cannot activate arbitrary nodes, install a checkpoint, remove an
offline restore guard or override graph readiness. No public Raft, participant,
raw Plan/KV, membership or administration service is registered.

## Shared behavior, distinct wire formats

Both public adapters and the original managed TCP entry use the same
`Invocation`, state, canonical mutation identity, original-Plan recovery,
certified fixed-V query executor, admission and retention budgets. There is no
extra TCP proxy hop between a public adapter and the business handler, and no
observed-read fallback. Internal managed/node RPCs still use their currently
supported transport; this public slice does not claim they all became gRPC.

HTTP remains the isolated [preview JSON contract](PUBLIC_HTTP_PREVIEW.md), not
protobuf JSON. gRPC carries real typed protobuf messages, not opaque JSON bytes.
The same domain write ID/command replays across adapters, and a session can be
fetched or closed through either on its owner gateway. Converting tokens between
protobuf bytes and HTTP lowercase hex does not change their identity. A final
stable HTTP/protobuf-JSON contract and migration/removal of the preview schema
remain separate work; do not preserve two permanent business implementations.

Clients must supply `Context { version: 1, timeout_millis: ... }`, with a finite
positive budget at most 60000 ms. A gRPC timeout header may only shorten the
configured/context/method budget. Omitted or unsupported context is an error.
For generated clients, set request and response message caps explicitly to
64 KiB and 1 MiB respectively, reuse a bounded channel, and do not enable generic
application retry/hedging for mutations. Server caps and checked codecs apply
regardless of client configuration. The provided implementation does not enable
gRPC compression.

The read selector is a required oneof: latest certified, exact V (including
zero), or minimum V. Queries retain one immutable certified V and original
completeness through all pages. This is not linearizable latest or a certificate
that arbitrary trusted low-level writers preserved all adjacency. Every
unsupported Gremlin feature fails explicitly. Query jobs, historical business
time, custom read profiles and arbitrary graph transactions are not exposed by
this slice. Limits are unchanged from the existing managed execution path.

All eight currently supported managed mutation forms are typed. Scalar presence
is mandatory: Null, absent properties, integer/real and finite values remain
distinct. Properties/conditions reject duplicate keys, and semantic validation
still uses the existing domain rules. In particular, an empty conditional patch
is invalid rather than an unconditional write. The canonical durable command is
computed from the domain mutation, never from protobuf field order/bytes.

## Errors and ownership

Application errors use gRPC status with a typed `ErrorDetail` reason. Preserve
Committed/Aborted ordinal and visibility separately, and Absent/Pending
observations. Lost response, cancellation or a transport deadline never proves a
write aborted. Recover/inspect the same original external ID and exact command,
including after a channel reconnect. A delayed request can still admit after an
Absent observation. Session identities are process-local, not credentials or
restartable bookmarks; wrong owner/restart/expiry is an error, not a new latest
query.

One shared eight-operation budget spans protobuf decoding, execution, encoding
and response-body ownership; accepted HTTP/2 connections are separately capped.
The gRPC checked codec scans the versioned schema before prost allocation. It
rejects malformed/unknown fields, duplicate singular/oneof fields, excessive
nesting and structural counts, as well as frame limits. Semantic DTO validation
still follows the scan. A successfully generated response also passes the same
structural budget, avoiding a server response its generated client must reject.
The optional generation script reproduces all checked-codec hooks; normal builds
do not run protoc or require a JVM.

Every future gRPC page envelope is checked before the first gRPC page is
returned. A successful proof is cached on the immutable retained result,
including the first gRPC use of an HTTP/TCP-created session. Subsequent fetches
still check liveness/expiry and encode their own page, without quadratic
whole-result revalidation. Failed checks are not cached.

## Deadline and slow-reader boundary

The remaining deadline decreases across body decoding, domain execution,
encoding and response ownership. Context and operation caps are published before
execution; session expiry shortens the response deadline. Blocking work keeps
its reservation until it actually stops.

A timer inside `Body::poll_frame` alone is insufficient: HTTP/2 can be waiting on
flow-control capacity without polling the body again. Each admitted RPC therefore
has a bounded owned watchdog independent of body polling. At deadline it
conservatively closes **that physical connection**, releasing stalled streams
and their owned state. Other concurrent RPCs on the same connection can also
fail; this preview does not promise per-stream-isolated deadline cancellation.
Writes affected by that close still have unknown outcomes. Successful normal
completion/drop cancels the watchdog; there is no detached unbounded task list.

A successful gRPC trailer handoff means transport consumption, not proof the
socket flushed or the application received the response. Unknown delivered
session descriptors remain governed by the original finite lease. Before that
handoff, dropped/erroring responses reclaim their newly opened session. Neither
case is a write rollback. Do not use these tests as a network-delivery SLA.

## Evidence required

The targeted suite includes strict type/identity mapping, malformed protobuf
preallocation checks and an actual raw h2 zero-window client. The latter keeps
the connection driver alive without releasing stream capacity, separately
exercises a short RPC budget and short retained lease, and checks shared permits
and undelivered sessions are reclaimed while a prior committed write is still
recoverable. The [process test](PUBLIC_SERVICE_CLI.md#process-acceptance-scope)
adds three independent data/control nodes, a separate dual-protocol service and
client process. Record actual exact-commit results; source presence is not a pass.

Global checkpoint restoration, multi-replica failover, multi-host throughput,
authentication, browser-origin protection and stable final HTTP v1 remain out of
scope. Loopback is not an authorization boundary. Those capabilities are not implied by shipping this experimental slice. The
finite [v0.0.1 release gates](RELEASE_PLAN.md), including clean startup and exact
CI/acceptance evidence, still need to be evaluated separately.
