# Typed managed coordinator/member gRPC

Implementation slice of [#218](https://github.com/ashione/VelaGraph/issues/218),
tracked by [#235](https://github.com/ashione/VelaGraph/issues/235). This changes
transport, not the restricted fixed-cohort transaction or consistency contract.
It is not completion of the all-RPC migration.

## Contract and deployment

`d2_raft::managed_rpc::Server` (the typed server) and `Client::new` use the versioned
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
member clients; every member endpoint must speak this same typed protocol.

Clients cache one HTTP/2 channel per explicitly constructed endpoint/identity;
clones share the channel. Full identity, operation tag and nonce are checked in
each reply. There is no peer replacement, protocol negotiation, fallback to TCP,
or automatic application retry. A transport failure after a write has an unknown
outcome: retain the original external ID and canonical command and use the
existing status/recovery protocol. Reconnecting a channel is not rollback.

The shared managed client admission remains four active calls and four bounded
additional waiters; CPU work retains the existing
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
The managed member library now also uses gRPC exclusively: its former JSON
framing/client/listener is removed. `Client::new_grpc` names the same typed
constructor and is not a compatibility transport or fallback. Both constructors
require a running Tokio runtime; clones share a channel. Other graph/gateway
legacy APIs remain subject to their separate removal gates.

The public HTTP adapter remains supported by design. In-process Rust calls,
RocksDB filesystem I/O, and S3-compatible object-store HTTP are not custom
inter-node graph transports and are not replaced by this service migration.

## Remaining communication inventory

| Path | State after this slice |
| --- | --- |
| Public managed HTTP and typed gRPC | Shared domain handlers; loopback preview |
| Snapshot, observed fragments, fixed/renewable graph sessions | Typed gRPC available; old TCP retained for migration parity |
| Managed coordinator/member control and writes | Typed gRPC only, including library Client/Server; old JSON/TCP wire removed |
| Raft peer replication, votes, snapshot transfer | Typed gRPC peer transport (#239); driver control is typed gRPC (#251) |
| Observed hierarchy jobs | Shard Open/Fragment/Close typed gRPC (#236); typed coordinator Query/Job ingress (#248), default CLI transport (#260); library query/job/consistency ingress migrated to typed RPC (#274) |
| graph-service managed state | Listener-free Service lifecycle; only configured public HTTP/gRPC listeners are bound (#244) |
| Single-shard graph-node and catalog/routing storage | [Default typed operations, snapshots and single-partition query](RPC_STORAGE_ROUTING.md); graph-node no longer opens legacy TCP (#256). Graph library callers/fixtures migrate under [#278](GRAPH_LIBRARY_GRPC.md) |
| Legacy library graph | [GraphService retirement](GRAPH_LIBRARY_GRPC.md) removes its old wire; Gateway library uses identified typed gRPC (#276), observed coordinator uses typed RPC (#274); graph-service has no hidden legacy listener |
| Unix read gateway | Removed under #262; [migration and retained semantic coverage](GRAPH_UDS_GATEWAY.md). No replacement Unix listener |

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
does not undo a durable write or resolve an unknown outcome. The library `Gateway` wrapper now binds the same public typed gRPC adapter.
Its domain request API uses identified version-2 calls with exact binding/nonce
validation, including materialized fixed sessions, explicit bounded capture
profiles and asynchronous jobs. The previous JSON listener/client is removed;
see [migration](PUBLIC_GRPC_SERVICE.md#gateway-library-migration-276).
The separate graph library transport is still tracked by #218.

Validation includes actual-process owned socket-inode checks on Linux: only the
announced public ports listen. Lifecycle tests keep adapter handles alive while
dropping/shutting down Service, check listeners close, and verify expired/retained
session reservations are released. These tests are not multi-host recovery proof.


## Managed library transport retirement

The client nonce/identity/operation checks, original IDs and status recovery,
member-capacity configuration, shared admission, and decreasing deadlines are
unchanged. The listener is now the same typed implementation used by production
services. There is no wire detection and no request retry. A former JSON port is
not usable after this change. Public HTTP remains intentional and unrelated.

Adversarial fixtures use real protobuf services. Some fixtures describe durable
records with local JSON values before conversion; no JSON envelope crosses the
network. Strict durable-record parsing still rejects malformed records, whereas
protobuf collection/field/envelope checks protect the live RPC boundary. Tests
for foreign identities/nonces, unavailable outcomes, capacity, history, retained
reads and end-to-end process behavior must pass on the final commit. This change
was separate from GraphService retirement (#278). Gateway and observed coordinator
library ingress migrated under #276 and #274. See [graph library migration](GRAPH_LIBRARY_GRPC.md)
for the remaining graph adapter and its independent validation gates.
