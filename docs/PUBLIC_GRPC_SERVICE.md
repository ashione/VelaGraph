# Typed public managed gRPC slice

The loopback service exposes `velagraph.public.managed.v1.WriteService`
(Submit, Status, Abort), `QueryService` (Open, Fetch, Close), and
`FixedSessionService` (Open, Read, Traverse, Page, Close), and
`ManagedQueryJobService` (Start, Get, Renew, Fetch, Cancel). The schema is
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

Both public adapters and the typed library Gateway use the same
`Invocation`, state, canonical mutation identity, original-Plan recovery,
certified fixed-V query executor, admission and retention budgets. There is no
extra TCP proxy hop between a public adapter and the business handler, and no
observed-read fallback. Internal managed/node and graph RPCs now also use typed gRPC after #279.

HTTP remains the isolated [preview JSON contract](PUBLIC_HTTP_PREVIEW.md), not
protobuf JSON. gRPC carries real typed protobuf messages, not opaque JSON bytes.
The same domain write ID/command replays across adapters, and a session can be
fetched or closed through either on its owner gateway for the data-local
`QueryService` contract. The separate `FixedSessionService` is gRPC-only; the
HTTP preview does not expose these fixed-snapshot operations. Converting tokens between
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
unsupported Gremlin feature fails explicitly. Historical business time and arbitrary graph transactions are not exposed.
Version-2 identified library requests may explicitly select the existing bounded
Parity2MiBV1 or LocalRead8MiBV1 capture profile; this does not raise any budget.
Limits are unchanged from the existing managed execution path.

All eight currently supported managed mutation forms are typed. Scalar presence
is mandatory: Null, absent properties, integer/real and finite values remain
distinct. Properties/conditions reject duplicate keys, and semantic validation
still uses the existing domain rules. In particular, an empty conditional patch
is invalid rather than an unconditional write. The canonical durable command is
computed from the domain mutation, never from protobuf field order/bytes.

## Fixed-snapshot sessions: separate from data-local query sessions

[#266](https://github.com/ashione/VelaGraph/issues/266) adds typed access to the
existing fixed-snapshot operations. `FixedSessionService.Open` captures the
bounded complete managed cohort once. Omitted `version` means the latest safe
certified version at admission; `version: 0` means exact zero, not latest.
Subsequent Read, Traverse and Page requests retain that exact V even when later
writes or deletions become visible. No minimum-version wait is added here.

`FixedSession` and the existing data-local `Session` are different protobuf
messages and different server registries. They are not interchangeable. Unlike
`QueryService.Open`, fixed Open does not execute and retain one query: a client
can run multiple bounded queries against its captured snapshot. `Read` preserves
the earlier restricted single-start read subset; `Traverse` uses the existing
fixed traversal executor. Neither accepts arbitrary Gremlin or new grammar.
`Page` materializes one bounded traversal and binds its replayable cursor to the
session, query and page size. A different paged query on that same retained
result is rejected, not silently recomputed. Unpaged traversal does not replace
that result. Close, expiry, a wrong incarnation or restart invalidates the token;
requests never recapture latest automatically.

All methods share the original Invocation/admission/deadline and response
ownership machinery. Fixed snapshot acquisition, retained results and individual
messages keep their existing capacity limits. This is not a data-local scaling
improvement. HTTP parity for this additional service and for the asynchronous
managed job service remains future work; typed job RPCs are described below.
The library Gateway now binds this typed service instead of JSON/TCP; #218
remains open for cross-language, compatibility and service-surface evidence;
graph library transport retirement is complete in #279.

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

For data-local `QueryService`, every future gRPC page envelope is checked
before the first gRPC page is returned. A successful proof is cached on the immutable retained result,
including the first gRPC use of an HTTP/library-created session. Subsequent fetches
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
client process. Its fixed-session assertions cover a cross-partition edge at
V3 remaining visible after deletion at V4, complete Read/Traverse rows, duplicate
start bag pagination/replay, changed query/cursor/version/incarnation rejection,
closed sessions, an unavailable future V, and restart invalidation. Record
actual exact-commit results; source presence is not a pass.

Global checkpoint restoration, multi-replica failover, multi-host throughput,
authentication, browser-origin protection and stable final HTTP v1 remain out of
scope. Loopback is not an authorization boundary. Those capabilities are not implied by shipping this experimental slice. The
finite [v0.0.1 release gates](RELEASE_PLAN.md), including clean startup and exact
CI/acceptance evidence, still need to be evaluated separately.

## Managed query jobs

`ManagedQueryJobService` adds typed Start/Get/Renew/Fetch/Cancel on this same
listener. Start uses a nonzero 16-byte operation ID, optional exact V (absence
selects latest at worker read-cut admission), Query, page size and JobPolicy.
Its JobHandle is distinct from synchronous and materialized session tokens.
Get exposes no partial rows; Fetch requires Ready and preserves immutable V,
query and page bindings. Renew is explicit and never refreshes the selected V.

A dropped Start reply does not cancel admitted work: replay the same ID and exact
arguments while retained. Do not generate a new ID after an uncertain reply.
Cancelled metadata retains its shared slot until hard lifetime; restart invalidates
handles. Capturing renewal is Busy, and a delayed renewal cannot revive expired
work. Every full page is preflighted against both logical and typed envelopes
before Ready. No JSON tunnel, extra listener or second execution engine is used.
See [the complete job contract and validation matrix](MANAGED_QUERY_JOBS.md).

## Gateway library migration (#276)

`Gateway::bind*` and `managed_gateway::request` retain their domain operations,
but the announced port now speaks typed gRPC only. Upgrade peers together; no
wire sniffing, JSON fallback or transparent write retry is provided. Public HTTP
remains intentional. `Gateway` binding now requires loopback, matching the public
adapter: a formerly non-loopback legacy address is explicitly rejected rather
than silently expanding public-service exposure.

The library client uses context version 2 with a nonzero call nonce and exact
authority/cohort/mapping binding. The server checks it before domain execution;
typed replies and application errors acknowledge the binding and nonce. This is
placement/correlation validation, not authentication. Old version-1 generated
clients omit identity and retain their existing contract. An older server rejects
version 2 instead of silently accepting a request without its required binding.

One client admission permit covers connection setup, encoding, dispatch and
validation within the original absolute deadline. A timeout or lost response is
Unknown, not Abort: inspect/recover the original external ID and exact command.
The shared executor, fixed V, page/job/session ownership and delivery cleanup
are unchanged. No global snapshot, new transaction or higher throughput claim
follows from this transport migration.
