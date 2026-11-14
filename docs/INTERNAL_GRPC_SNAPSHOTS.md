# Internal gRPC snapshot transport

> Current transport baseline: main `40e2b9a` (#279, 2026-11-14). Supported
> graph, coordinator, Gateway, managed-member, Raft and control network paths use
> typed gRPC; old JSON/TCP library listeners and fallback are retired. Public HTTP
> remains intentional. Historical transport details below are not a supported
> compatibility mode. See [current RPC audit](RPC_MIGRATION_STATUS.md).


`graph-node --graph ADDRESS` hosts typed snapshot, fragment/session, single-shard
operation and query RPC services on one graph endpoint. Raft and driver/control
use their separate RPC ports. This is plaintext trusted-network development
transport, not authentication. Tracking: #218 and #256.

## Start and use

Use `--graph 127.0.0.1:50051` with `--allow-insecure-development` and the usual
node/storage arguments. The redundant `--grpc-snapshot` option is removed and
rejected; there is no hidden legacy graph TCP listener. Upgrade callers together.

Rust callers use `remote::grpc::SnapshotClient::connect(address, identity,
deadline)`, then `open(shard, remaining_budget)`, `page(descriptor, cursor,
remaining_budget)` and `close(descriptor, remaining_budget)`. Cloning the client
shares one tonic `Channel`, not a new TCP connection. There are no application
retries, leader forwarding, endpoint discovery, or JSON fallback. The channel may
reconnect after a connection failure, but does not replay an application RPC.

The caller owns the **whole-operation deadline** and passes the remaining budget
at each step. A new call must not reset an overall download deadline. `remote::grpc::paged_snapshot` provides the complete bounded download used by
RemoteStorageNode; it retains one absolute deadline through connect, pages and close. Close on success and
on failures when remaining budget permits. Lost Open responses and abandoned
sessions expire after the original fixed lifetime; cancellation does not consume
a page cursor or renew the session.

## Shared semantics and limits

Listener-free `GraphState` owns the shared Raft/state handles and snapshot
session pool. Multiple typed library listeners can share that state;
neither GraphService nor product graph-node starts a legacy listener. Open uses the existing leader read barrier and atomic capture.
Page uses the existing descriptor/cursor/epoch validation and frozen entries;
Close releases the same session. A token opened through one typed listener can
be used or closed through another typed adapter sharing that GraphState. These
shared-state calls cannot exceed
the shared four sessions / 128 MiB retained-byte budget. Tokens remain random
16-byte capabilities tied to an incarnation, descriptor and cursor position.

- Four sessions, at most 64 MiB / 1,000,000 rows each, 128 MiB total retained bytes.
- Pages retain the 2 MiB raw key/value and 4,096-row limits; no partial-success
  truncation. The protobuf response wire cap includes bounded entry overhead.
- Fixed 60-second session lifetime; pages never extend it.
- Existing 16-operation service admission is shared by typed adapters, from decode through
  response-body EOF/drop, not only handler execution. Client calls use
  the existing process-wide 16-operation admission. Overload fails explicitly.
- The typed graph listener accepts at most 32 live connections (including the
  adapter's separately reused Metadata Channel) and 16 HTTP2 streams per connection;
  snapshot request messages are capped at 4 KiB before protobuf decoding. Metadata
  streams have their [own explicit resource budget](METADATA_STREAM.md). No compression
  is enabled. These are operation/payload bounds, not an RSS guarantee.
- Positive operation budgets are at most 60 seconds and capped by the configured
  server deadline. `grpc-timeout` and a client `timeout_at` bound transport work;
  the payload carries remaining milliseconds for cooperative operation checks.
  A flow-controlled body keeps its permit while it cannot drain; expiry never
  releases a budget for bytes still owned by the transport. Shutdown allows one
  configured deadline for graceful drain, then cancels the actual sockets and
  waits at most one more deadline before abort/join. Slow readers cannot make
  shutdown wait indefinitely.

The service uses standard gRPC status codes and typed `Status.details` for leader
hints, unactivated state, epoch fences and closed/expired sessions. Clients never
parse human-readable status text. An unknown typed application error fails
closed as a protocol error; it cannot become a successful or partial snapshot.
Dropping a call may race with server completion. An admitted Open can therefore
retain a session until Close/TTL even when no response arrives. No cancellation
claim is made about undoing work already executed.

## Contract and regeneration

The separate schema is
[`internal_snapshot.proto`](../crates/replicated-graph/proto/internal_snapshot.proto),
package `velagraph.internal.snapshot.v1`. Existing disk-record protobuf schemas
and on-disk formats are unchanged. Generated Rust is checked in. **Normal builds
require no protoc**, satisfying the existing repository build rule.

Maintainers can run `scripts/generate-internal-snapshot-proto.sh`, then
`cargo fmt --all` and inspect the diff. This optional tool uses the official
`tonic-prost-build` 0.14.9 and the crates.io `protoc-bin-vendored` 3.2.0 package in
a temporary codegen project. Runtime dependencies tonic/tonic-prost, Tokio streams/utilities and http-body
are MIT licensed;
prost is Apache-2.0 licensed; the generated transport replaces hand-written
framing, while protobuf gives independently specified typed fields.

## Verification and exit path

`cargo test --locked -p replicated-graph --test grpc_snapshot_process` exercises
three actual graph-node/Raft processes, typed listener/shared-state pages, frozen
views, shared-session capacity, malformed typed requests, identity/follower and
close errors, cancellation/replay, physical connection reuse, 16 zero-window
HTTP2 responses exhausting shared admission, and bounded real-process shutdown. The ignored
fixed-TTL extension can be run explicitly with `-- --ignored`. Unit tests cover
typed conversion, malformed tokens, error-details decoding and wire overhead.
These tests are correctness/transport evidence, not multi-machine throughput or
latency improvements. See the PR for exact commands and outcomes.

1. This slice is opt-in and keeps the old snapshot operations for differential
   validation. Do not call the overall internal migration complete.
2. The separate observed **data-local query Open → Fragment → Close** slice is
   described in [INTERNAL_GRPC_QUERIES.md](INTERNAL_GRPC_QUERIES.md). Next migrate
   fixed-V/cohort and job-session identity without changing accounting/semantics.
3. Switch snapshot callers to persistent gRPC clients after both-path functional,
   overload, deadline, recovery and performance gates pass. No silent fallback.
4. Delete old snapshot framing/dispatch only after the callers have moved and
   release/migration documentation names the removed contract. Do not retain a
   permanent second compatibility stack for this experimental protocol.
5. Move managed and Raft RPC families in separate reviewed slices; external
   gRPC/HTTP write/query APIs share the application layer, not this internal
   snapshot service or its authority boundary.

## Unresolved seed quorum evidence

[Snapshot seed investigation](SNAPSHOT_QUORUM_INVESTIGATION.md) tracks #247's
single observed CI read-barrier failure and failure-only fixture diagnostics.
Successful reruns are not a fix; the fixture preserves the original error and
collects bounded post-error replica status without mutation retry.
