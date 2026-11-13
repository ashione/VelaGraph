# Internal gRPC snapshot transport (first migration slice)

This optional, **plaintext trusted-network** endpoint implements typed Protobuf
`Open`, `Page`, and `Close` using tonic/HTTP2. It is not a JSON-over-gRPC bridge and
it is **not** the public graph write/Gremlin query service. It does not migrate
Raft, managed transactions or hierarchy channels. The same endpoint now also hosts
the separate [observed data-local query service](INTERNAL_GRPC_QUERIES.md); the
snapshot RPC contract here remains unchanged.
Those remain on their existing transports. Related tracking: #218.

## Start and use

Add `--grpc-snapshot 127.0.0.1:50051` to the existing `graph-node` invocation,
which still requires `--graph` and `--allow-insecure-development`. Neither
identity matching nor session bearer tokens authenticate the caller. Keep both
ports isolated from untrusted clients; TLS/authorization are not implemented.

Rust callers use `remote::grpc::SnapshotClient::connect(address, identity,
deadline)`, then `open(shard, remaining_budget)`, `page(descriptor, cursor,
remaining_budget)` and `close(descriptor, remaining_budget)`. Cloning the client
shares one tonic `Channel`, not a new TCP connection. There are no application
retries, leader forwarding, endpoint discovery, or JSON fallback. The channel may
reconnect after a connection failure, but does not replay an application RPC.

The caller owns the **whole-operation deadline** and passes the remaining budget
at each step. A new call must not reset an overall download deadline. This slice
exposes page primitives, not a new complete-download helper. Close on success and
on failures when remaining budget permits. Lost Open responses and abandoned
sessions expire after the original fixed lifetime; cancellation does not consume
a page cursor or renew the session.

## Shared semantics and limits

The TCP and gRPC ports share the exact same Raft/state handles and snapshot
session pool. Open uses the existing leader read barrier and atomic capture.
Page uses the existing descriptor/cursor/epoch validation and frozen entries;
Close releases the same session. A token opened by either protocol can be used or
closed through the other during migration. Cross-protocol calls cannot exceed
the shared four sessions / 128 MiB retained-byte budget. Tokens remain random
16-byte capabilities tied to an incarnation, descriptor and cursor position.

- Four sessions, at most 64 MiB / 1,000,000 rows each, 128 MiB total retained bytes.
- Pages retain the 2 MiB raw key/value and 4,096-row limits; no partial-success
  truncation. The protobuf response wire cap includes bounded entry overhead.
- Fixed 60-second session lifetime; pages never extend it.
- Existing 16-operation service admission is shared with TCP, from decode through
  response-body EOF/drop, not only handler execution. Client calls use
  the existing process-wide 16-operation admission. Overload fails explicitly.
- gRPC accepts at most 16 live connections and 16 HTTP2 streams per connection;
  request messages are capped at 4 KiB before protobuf decoding. No compression
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
three actual graph-node/Raft processes, legacy/gRPC differential pages, frozen
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
