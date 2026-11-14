# Trusted remote managed-node transport

> Current transport baseline: main `40e2b9a` (#279, 2026-11-14). Supported
> graph, coordinator, Gateway, managed-member, Raft and control network paths use
> typed gRPC; old JSON/TCP library listeners and fallback are retired. Public HTTP
> remains intentional. Historical transport details below are not a supported
> compatibility mode. See [current RPC audit](RPC_MIGRATION_STATUS.md).


Status: experimental, fixed-placement internal storage/transaction RPC. The
managed gateway can use a coordinator and participants in separate OS processes
through `Gateway::bind_remote(RemoteConfig, ...)`. Its existing local `Config`
constructor remains supported. This is not production deployment, multi-machine
validation, authenticated transport, replica failover, or D4/D5 completion.

Current peers require internal managed RPC **v6**
([S4 retained control](RETAINED_GATEWAY_CONTROL.md)); this is the domain protocol
version, not an old JSON framing mode. The Gateway library uses identified
version-2 public gRPC contexts; generated public v1 clients remain separately supported. The historical A1/A2.1/v4 sections at the end record earlier protocol
slices, not the version or ordinary control path to deploy now.

## Boundaries

The external managed gateway protocol, `PartitionEqualsShardV1` mapping,
canonical command bytes, caller external IDs, persisted transaction Plans,
Gremlin grammar/subset, and process-local read session tokens are unchanged.
Only the location of its storage-node capabilities changes. Public graph reads
remain standard Gremlin text parsed by the official parser. Internal opaque KV
snapshots are storage plumbing, not a second public graph query language. The
older remote `GlobalCommittedSnapshot` request remains Unsupported.

Each internal listener owns exactly one already activated node and pins its
(group, node, role, shard, epoch, mode, authority, complete sorted cohort).
Startup verifies the live role and zero-baseline cohort after a leader barrier.
Every operation rechecks the pinned contract and uses a leader barrier. The
caller cannot forward requests to arbitrary nodes, activate or import a shard,
change roles or ownership, replace an address, or discover another leader.
Restart must use the same persisted node identity and explicit endpoint contract;
a changed role, epoch, mode or cohort fails closed.

The available operations are coordinator journal/decision commands, participant
prepare, applied-prefix readiness, consume one terminal decision, certify a
terminal prefix, and bounded materialization at an exact version. Commands do
not expose general raw write, role activation or cohort replacement RPCs.
The supplied journal is trusted coordinator evidence, not a signed certificate.
This plaintext channel requires trusted peers and network isolation. Identity
fields, correlation IDs and evidence comparisons provide mismatch detection,
not authentication, TLS, authorization or malicious-peer proof.

## One cut, one durable request

`cohort_reads::acquire_endpoints` is the shared local/remote acquisition path. It:

1. Captures bounded coordinator authority/cohort/resolved-prefix metadata after
   a leader barrier.
2. Verifies every configured member against the whole persisted sorted cohort.
3. Reads each member's leader-observed visibility information and validates the
   authority, cohort and empty baseline. Without an explicit version it chooses
   min(coordinator resolved prefix, all member applied prefixes).
4. Fetches the exact segmented terminal projection at the chosen V, then reads
   every member at exactly V using that proof and the remaining aggregate
   row/byte/replay budget. An explicit V is never lowered.
5. Rechecks each returned shard, epoch, V, baseline, authority, cohort, applied
   watermark and payload budget. Remote prefix/read replies include the actual
   participant-retained terminal projection; the client compares it exactly
   against the coordinator proof. The participant derives proof and captures rows
   under the same state lock. Request/Plan archives never enter this read path.
6. Constructs the private complete SnapshotSession only after all members pass.

A readiness watermark is not a read certificate. A missing, lagging, mismatched,
expired or unavailable member fails the entire capture. No partial result or
mixed-cut session escapes. Existing sessions retain materialized rows and V while
participants continue writing or restart; this is not a durable lease or GC pin.

Gateway admission still atomically records external ID, canonical command,
complete Plan and Begin. Execution uses only the admitted Plan. Internal prepare
checks the original admitted request body, participant intent and CAS when the
transaction has a durable request record. Trusted low-level Begin-only callers
remain responsible for their exact plan. Coordinator and participant restart
recovery never regenerates transaction identities, changes CAS conditions or
revalidates an admitted request against fresh data.

Disconnect, timeout and lost replies have unknown outcomes. There is no
transparent retry, automatic Abort or new-ID substitution. Retain the original
external ID and exact command, then inspect/retry it. Known Commit remains Commit
when participant catch-up is incomplete; visibility is separately reported.
Nonparticipants must consume terminal ordinals too before a new common V exists.

## Resource and time limits

The transport uses typed gRPC/Protobuf with a shared bounded channel,
explicit domain protocol version and operation/correlation identity. Unknown
fields, malformed frames, duplicate snapshot keys, identity mismatches and
unexpected reply variants fail closed. Length checks precede receive allocation;
serialization stops at the encoded frame limit rather than allocating an
unbounded result first. Journal objects are embedded once, not nested JSON byte
arrays or escaped strings, so existing 16 MiB journal capacity remains consumable.
A full journal plus an admitted Plan (at most 256 KiB) and the bounded identity
envelope fit below 32 MiB even allowing byte-array expansion. Very large manual
Begin-only intents can exceed the transport limit and fail Capacity; terminal
Abort/advance messages and existing admitted request resolution still fit.

The frame limit is 32 MiB in each direction. At most four internal outbound calls
per process and four accepted handlers per listener are admitted. CPU-heavy
encoding, decoding and state materialization use a separately bounded shared
pool; reservations remain held if awaiting callers time out. In-memory node
callers share the same protocol checks without socket framing. The transport
limits are not an RSS guarantee: bounded decoded objects, retained journals,
state-machine history, RocksDB and allocator overhead are additional.

Each call uses one positive total deadline, at most 30 seconds, covering encoding,
connect, input/output, leader barrier, state work and reply transmission. The
higher-level acquisition/gateway deadlines still cover the entire multi-node
operation, not one fresh budget per shard. CPU work is bounded but cannot be
forcibly interrupted; post-deadline results are rejected and work reservations
survive until completion. A deadline does not undo a submitted Raft command.
The maximum encodable remote snapshot may be smaller than the local 64 MiB raw
snapshot allowance; oversized materializations return Capacity, never pages or
truncated success. Gateway captures retain their existing 2 MiB raw aggregate,
10,000-row and 100,000-replay-operation limits.

## Validation scope

`crates/replicated-graph/tests/managed_process.rs` starts three independent child
processes with distinct PIDs, listening ports and durable directories: one
coordinator and two participant groups. Each group has one Raft replica. The
parent gateway/client process has no data-node handles. Tests exercise typed
managed writes and standard Gremlin sessions over the complete TCP path, partial
apply and old/new V behavior, participant and coordinator kill/reopen, exact
persisted Plan/external-ID recovery, conflicting retries, both cross-shard
edge directions and a lagging nonparticipant that must consume its decision.
Wrong node/group/role/epoch/cohort contracts and member outages fail closed. Transport unit tests cover bounded framing, strict identity,
protocol mismatch and slow-client deadlines.

These tests do not validate multi-host behavior, multi-replica consensus traffic,
network partitions, automatic failover, TLS/authentication, indexed MVCC,
history reclamation, online placement changes or production scalability. The
existing consensus transports and storage formats are not changed by this slice.

## Historical A1 evidence separation

[Exact segmented terminal-prefix evidence](TERMINAL_PREFIX_EVIDENCE.md) defines the
versioned proof, protocol-v2 fail-closed upgrade boundary, complete-prefix validation
and unchanged durable recovery rules. Prepare and advance retain full Journals;
read evidence remains O(V), with old-V bytes independent of later Plan archives.

## Historical A2.1 request point recovery

[The durable derived request index](DURABLE_REQUEST_INDEX.md) adds LookupRequest
and advances internal RPC to version 3. Recovery and admission winner readback
return only one exact original binding and its same-lock transaction state after
a leader barrier. Internal versions 1/2 fail closed; the external managed graph
protocol and A1 proof format are unchanged. Coordinator disk metadata v8 migrates
legacy v5 atomically and snapshots rebuild its derived index. Full Journals remain
in command validation, execute/status, prepare/advance and authoritative storage.
The 256-request, 1,024-transaction and 16 MiB Journal caps are unchanged.

### Historical incremental catch-up (RPC v4)

[Bounded history suffixes](INCREMENTAL_DECISION_CONSUMPTION.md) now replace
archive-bearing Advance in managed graph catch-up. The full replayed Journal
remains durable authority, existing caps remain, and all cohort members must use
v4 before enabling the flow. v3 has no automatic fallback.

## Current retained control and storage

The merged [S4 contract](RETAINED_GATEWAY_CONTROL.md) replaces ordinary
execute/status/retry/abort and Prepare archive exchanges with exact target
evidence, fixed raw-history cuts, bounded HistoryPage/SyncHistory and
PrepareRetained. Complete raw validation remains mandatory; smaller retained
responses are not constant-time or bounded-total-history work.

Bound coordinator/member durable state now uses disk v16/v17 active checkpoints,
and bound request indexes have been retired after strict legacy validation.
Cold replay, explicit full-history/terminal-proof paths and SnapshotV1 retain
history-sized work. [Both capacity profiles](COHORT_CAPACITY_PROFILE.md) keep the
16 MiB reserved history bound and local D1 limits; only fresh ByteBound16MiBV1
cohorts replace legacy count caps. There is no v5 fallback, automatic downgrade,
streamed SnapshotV1 delivery or unlimited lifecycle.
