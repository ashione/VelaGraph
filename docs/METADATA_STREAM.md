# Bounded metadata RPC stream

Implementation under validation for [#258](https://github.com/ashione/VelaGraph/issues/258),
a prerequisite of the default RPC migration [#256](https://github.com/ashione/VelaGraph/issues/256).
This document is not evidence that the default acceptance tests have passed.

## Purpose and semantics

A typed bidirectional Metadata RPC amortizes unary HTTP/2 setup across repeated
storage reads. It does not cache an epoch or sequence: every `check_view` sends a
new request and the server invokes the same `Operation::Metadata` against the
selected replica. An epoch mismatch still fences the read. This remains a locally
applied epoch check with the existing check/read race, not a quorum barrier,
transaction snapshot or global consistency upgrade.

Only Metadata uses this stream. Probe and Write remain unary RPCs; snapshot
transfer and query-language semantics do not change. No query, dataset, CoreHR
entity or traversal shape selects a special implementation. There is no unary
or legacy transport fallback when the streaming endpoint is unavailable.

## Isolation and correlation

An adapter's clones share a lazy metadata actor and its **dedicated Channel**.
They do not share that TCP connection with the adapter's unary writes. Connection
loss fails current requests; only a later explicit call can establish a new
stream. In-flight requests are never transparently replayed. Unary write timeout
continues to mean an unknown outcome with the original request ID.

Each stream has increasing nonzero request IDs and its own pending map and
response receiver. Cancellation or timeout cannot redirect an old reply to a new
request. No global endpoint registry, epoch polling, session nonce, unbounded
history of completed IDs, or cross-stream response forwarding is needed.

## Explicit budgets

The stream uses a separately admitted lifetime, rather than holding a unary
operation permit for the entire response body. Each message still acquires the
existing shared operation budget. Idle streams must not consume all execution
slots. The server processes metadata sequentially and reserves bounded output
capacity before executing a request; no task is spawned for every frame.

The stream limit is tied to `MAX_IN_FLIGHT`, not to graph member count. Dedicated
transport isolation adds connection cost. The proposed total accepted graph
connection budget is `2 * MAX_IN_FLIGHT`; snapshots also consume this total.
It is not a promise that this many independent adapters can perform every
protocol simultaneously. Saturation must fail explicitly and release resources
when connections or streams terminate. Implementation validation must cover
multiple independent adapters and unary progress while stream capacity is full.

Metadata schemas contain fixed-width scalar values, small tagged error variants,
and an optional exact-length write identity in the shared error DTO. They contain
no strings, arbitrary properties or mutation batches. Their named frame bound is
validated against every legal variant at maximum scalar values, with strict
pre-decode rejection of malformed or oversized input. The bound is not a limit
on graph size, database memory or a user query's result size.

A bounded application queue alone does not bound transport memory. With pinned
Hyper 1.12.0, `proto/h2/server.rs` sets a **400 KiB** send buffer per stream and
**1 MiB** stream/connection receive windows; Tonic's unset window options retain
those Hyper defaults. Metadata's codec yields at 1 KiB plus at most one 512-byte
message, and the application output queue holds one message. These are separate
payload/accounting bounds, not exact RSS guarantees: include transport framing,
headers, allocations, sockets, a partial frame, and the other admitted services.
The raw-H2 slow-reader test advertises a deliberate 64-byte *client response*
window. Its observed input backpressure point does not measure server RSS or
replace the production receive-window configuration. Recheck these dependency
defaults when upgrading the transport stack.

## Deadlines, disconnect and shutdown

Every caller retains its original absolute deadline through initialization,
queueing, transmission, waiting and delivery. Activity on another request never
renews that deadline. A message already handed to tonic cannot be withdrawn;
its expired response is discarded rather than exposed as a successful read.
Server-local relative budgets do not claim synchronized wall clocks across hosts.

Partial input, idle streams and blocked application output have bounded actor
lifetimes. A stream timeout terminates its work without aborting the entire HTTP/2
connection: arbitrary gRPC clients and proxies may multiplex other operations on
that connection. The official adapter uses a dedicated Metadata Channel, but
server correctness does not depend on external clients doing so.

If HTTP/2 backpressure stops polling a response body, that body keeps its stream
permit and bounded transport buffer until the client consumes/cancels it, the
connection closes, or the server shuts down. There is deliberately no claim that
an uncooperative slow reader returns the slot within 60 seconds. Up to the stream
limit can remain stalled; new streams are rejected explicitly while the separate
unary operation budget remains available. This is a bounded availability cost,
not permission to release accounting while memory remains retained. Individual
call cancellation releases its pending entry by the actor's bounded deadline
cleanup; stream cancellation, final owner drop and shutdown must additionally
release the stream/channel/task resources. Test these as distinct boundaries.

## Required evidence

Before accepting #258, record the exact source tree and independent review;
check legal/malformed frame boundaries, concurrent response correlation,
cancellation and delayed replies, disconnection without replay, epoch changes,
queue deadlines, saturation, slow readers, and complete shutdown. Compare
executed metadata counts and cold/warm latency against unary RPC and TCP in
matched default and release builds. The original default-debug complex suites
must pass without longer deadlines, skipped freshness checks or profile-only
changes. Loopback measurements are not multi-host throughput guarantees.
