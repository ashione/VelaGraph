# Typed managed coordinator/member gRPC

Implementation slice of [#218](https://github.com/ashione/VelaGraph/issues/218),
tracked by [#235](https://github.com/ashione/VelaGraph/issues/235). This changes
transport, not the restricted fixed-cohort transaction or consistency contract.
It is not completion of the all-RPC migration.

## Contract and deployment

`d2_raft::managed_rpc::grpc::Server` and `Client::new_grpc` use the versioned
`velagraph.internal.managed.v1.ManagedService` schema in
`crates/d2-raft/proto/internal_managed.proto`. All 18 original internal operations
have typed protobuf payloads and replies: coordinator commands/summary/terminal
proof/journal; request and target lookup; member preparation, retained preparation,
visibility/prefix/read; capacity confirmation/grant; and retained-history
status/page/sync/advance. No protobuf field is a JSON envelope. Canonical durable
commands and proof hashes remain the original domain encoding, never arbitrary
protobuf bytes. Journal evidence is replayed through the original validation;
terminal proofs use their checked constructor.

The gRPC listener is **internal trusted plaintext**, not an authenticated public
API. Do not expose it via the public graph listener. Public writes/queries stay
on the separate public HTTP/gRPC services. It cannot activate a new cohort,
restore a graph, or override readiness. Startup runs the original leader-observed
role/cohort readiness probe. Capacity confirmation uses the explicitly configured
member clients; selecting gRPC requires gRPC clients for those members too.

Clients cache one HTTP/2 channel per explicitly constructed endpoint/identity;
clones share the channel. Full identity, operation tag and nonce are checked in
each reply. There is no peer replacement, protocol negotiation, fallback to TCP,
or automatic application retry. A transport failure after a write has an unknown
outcome: retain the original external ID and canonical command and use the
existing status/recovery protocol. Reconnecting a channel is not rollback.

The shared managed client admission remains four active calls and four bounded
additional waiters, including legacy clients; CPU work retains the existing
process-wide slots through actual completion. The gRPC listener admits at most
four active calls through request decode, execution and response delivery, with
eight bounded connections. Pre-prost scanning bounds each collection and total
allocation. The 32 MiB frame limit is a transport bound, not a graph-size limit.
Calls retain the decreasing caller budget, capped by the server deadline. A
response watchdog cancels its connection if HTTP/2 flow control prevents progress;
this conservatively fails other calls on that same channel rather than retaining
unbounded stalled responses. Application mutation outcome remains unknown after
cancellation. Shutdown stops acceptance and closes owned connections within the
configured deadline; it does not reverse committed state.

## Service migration selection

`graph-service` now always uses typed gRPC for **both** configured managed
coordinator/member endpoints and graph fragment/session endpoints. All addresses
must name the corresponding gRPC listeners. The temporary `--internal-grpc`
selection flag is removed and rejected, rather than retained as a redundant alias.
Legacy managed endpoints fail the startup identity probe. A graph channel
connection is not typed-service readiness: incompatible graph endpoints fail
the first graph operation, without fallback. This completes
[#242](https://github.com/ashione/VelaGraph/issues/242), not the entire RPC inventory.
Library-level old transports remain only until their own replacement/removal gates.

The public HTTP adapter remains supported by design. In-process Rust calls,
RocksDB filesystem I/O, and S3-compatible object-store HTTP are not custom
inter-node graph transports and are not replaced by this service migration.

## Remaining communication inventory

| Path | State after this slice |
| --- | --- |
| Public managed HTTP and typed gRPC | Shared domain handlers; loopback preview |
| Snapshot, observed fragments, fixed/renewable graph sessions | Typed gRPC available; old TCP retained for migration parity |
| Managed coordinator/member control and writes | This typed gRPC slice; explicit client/server selection |
| Raft peer replication, votes, snapshot transfer | Typed gRPC peer transport (#239); driver control remains JSON/TCP |
| Observed hierarchy jobs | Shard Open/Fragment/Close typed gRPC (#236); coordinator ingress remains TCP/JSON |
| graph-service managed state | Listener-free Service lifecycle; only configured public HTTP/gRPC listeners are bound (#244) |
| Legacy library gateway listener, test/control drivers | Still need separate migration/removal; graph-service no longer starts this listener |

The exit gate is to make supported service paths RPC-only, remove superseded
framing/client/server paths after independent differential review, and test that
no hidden fallback remains. #218 stays open until that gate and all other
communication paths pass. No global recovery or multi-host HA claim follows from
same-host independent-process transport tests.

## Reproduction

Generated Rust is checked in. Normal builds do not require protoc. Maintainers
may regenerate it with `scripts/generate-internal-managed-proto.sh`, which retains
the bounded codec hook; protobuf field numbers/names must not be reused.

Run the managed codec/adversarial tests and `grpc_managed_process` process tests,
then the existing managed lifecycle/CoreHR gate, ordinary workspace tests,
formatting and Clippy on the exact candidate commit. Record passed, failed and
not-run stages separately. A short process test is not a substitute for the
1,025-write retained-history/recovery acceptance.

## Listener-free service lifecycle

`managed_gateway::Service::remote_local_queries_grpc` initializes the same bounded
managed state without opening a compatibility TCP listener. HTTP and gRPC adapters
share its session/job cleanup and stop signal. Expiry runs even when no requests
arrive; shutdown/drop stops both adapters and cancels retained jobs/sessions, but
does not undo a durable write or resolve an unknown outcome. The legacy `Gateway`
wrapper still provides explicitly selected TCP library entrypoints. Removing that
wrapper and other control/observed ingress paths remains #218 work.

Validation includes actual-process owned socket-inode checks on Linux: only the
announced public ports listen. Lifecycle tests keep adapter handles alive while
dropping/shutting down Service, check listeners close, and verify expired/retained
session reservations are released. These tests are not multi-host recovery proof.
