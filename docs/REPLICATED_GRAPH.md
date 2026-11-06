# Replicated graph-record slice

`crates/replicated-graph` connects the existing `GraphRecordStore` through the
synchronous D1 `StorageNode` trait to an actual three-voter openraft group.
It is an **in-process correctness prototype**, not a network graph service.
`GraphEngine` and its K3 transaction API still use the local kernel. The same
Gremlin parser/operators can also execute through this replicated D1 adapter.
Routing, placement, shard migration, cross-shard transactions,
network authentication and a production client protocol are not implemented.

## Construction and execution

Initialize a Raft group using `d2_raft::Cluster` and activate its empty state
machine with `LogCommand::D1V1(D1Command::Activate { shard, epoch })`. Inspect the
nested application result; a successful Raft write is not necessarily an
accepted D1 command. On the runtime hosting a `NodeHandle`, construct
`ReplicatedStorageNode::new(&node, shard, deadline)` and then
`GraphRecordStore::new(adapter, shard, partition)`.

The root graph crate and D2 crate remain independent. Only the adapter crate
depends on both. It adds no registry dependencies: Tokio, openraft and test
helpers were already in the workspace (MIT / Apache-2.0 compatible licenses).

Use `replicated_graph::run_blocking(move || ...)` for synchronous graph calls
from async code, including single-thread Tokio runtimes. The closure runs on
the blocking pool and submits futures to the node's host runtime through
`Handle::spawn`. Plain synchronous callers outside any runtime are supported
while the host runtime is running. Calling synchronous adapter reads/writes
directly on a Tokio worker fails with `Unsupported`. The D1
`check_execution_context` preflight runs before GraphRecordStore acquires its
mutation mutex, so rejection cannot first stall the worker behind another write.
StorageNode decorators must forward this hook as well as the existing methods.
There is no nested
`block_on`, `block_in_place`, or runtime creation/destruction in these calls.
The constructor must be invoked on the runtime actually hosting the node; the
adapter does not own that runtime or keep it alive.

Each adapter and its clones share an admission limit of 32 outstanding D1
operations. Admission exhaustion is `ResourceExhausted`; no unbounded adapter
queue is built. Each accepted operation has both an async timeout and a bounded
synchronous wait, so a stalled/stopped runtime cannot strand a caller forever.
The deadline applies per D1 operation, not to an entire graph traversal or an
arbitrary `run_blocking` closure. Graph work remains bounded by existing graph
limits. Cancellation of a started blocking closure does not cancel its work.

## Reads and snapshots

`read_view` calls `ensure_linearizable()` on the contacted replica. This
confirms leadership with a quorum and waits until the read log id is applied.
It then captures D1 shard, epoch, sequence and the entire opaque map under one
state-machine read lock. Metadata is never combined with separately sampled
entries. Limits (1,000,000 rows and 64 MiB raw key/value bytes) are checked
before cloning, with byte accounting stopping as soon as the budget is exceeded.
The adapter never returns a partial snapshot as success.

`ReadView::from_entries`, `get` and `scan` provide a safe owned-data boundary for
external adapters without exposing RocksDB or mutable snapshot entries. A
retained view is immutable across writes, leader changes and snapshot installs.
Point reads and scans use that view, not a new current read. They check logical
shard and the contacted replica's locally observed epoch before reading the
view; they do not perform another quorum read. Thus a historical retained view
is not a fresh linearizable read and an isolated replica may not yet know about
a later epoch change. Acquire a new leader read view for a current observation.
Writes always enforce the authoritative replicated epoch. Caller-provided
shard ids must identify one logical shard globally; there is no cluster UUID or
cryptographic view capability in this prototype.

As in the local prototype, copied views have no expiry lease. The row/byte caps
bound each view's raw content, not process RSS or the number of views retained
by callers. Production use needs pinned snapshots, lifetime/admission controls
and a bounded remote read protocol.

## Writes, errors and exact retries

Every mutation is the existing atomic conditional D1 batch. The adapter maps
opaque operations without decoding or changing `graph/v1` bytes. It inspects
nested D1 responses and preserves typed `Conflict`, `Fenced`,
`IdempotencyConflict` and `ResourceExhausted` errors. A follower returns typed
`NotLeader { leader }` where openraft supplies a hint. Quorum/read or stopped-node
failures return `Unavailable`; deadline expiry returns
`Timeout { request_id }`, containing the original write identity when relevant.
No automatic routing, retry, new identity, or condition/epoch refresh occurs.

A timeout, shutdown or lost response **does not prove a write aborted**. A log
entry already submitted can still commit after the waiting future is cancelled.
For retryable graph mutations, use the explicit preparation API:

```rust,ignore
let prepared = run_blocking({
    let store = store.clone();
    move || store.prepare_create_vertex(&vertex)
}).await??;
let retry_identity = prepared.clone();
run_blocking(move || store.commit_prepared(&prepared)).await??;
// If the response is lost, select the current leader explicitly and submit
// retry_identity through that store's commit_prepared. Do not prepare again.
```

All six vertex/edge CRUD mutations have `prepare_*` methods. Preparation is
read-only and validates against one snapshot. `PreparedGraphMutation` has
private immutable request bytes, identity, original epoch, sequence condition
and target partition. It may be cloned and submitted to another store for the
same logical shard/partition. `commit_prepared` does no new reads or validation;
it resubmits exactly those bytes and that condition. Wrong target is rejected.
Identical retries return the original sequence without changing graph data.
A definite `Conflict` means no commit: prepare anew to revalidate the whole
mutation. An epoch change fences even an otherwise exact retry; this slice does
not offer an unsafe implicit epoch refresh. The handle is in-memory, not a
durable client retry journal. Client-process loss still needs separate design.
Convenience CRUD calls generate a new identity on each invocation and are not
an unknown-outcome recovery API.

The D2 hard limits remain unchanged: 10,000 operations / 16 MiB raw bytes per
batch, 4,096 retained successful requests / 64 MiB accounted dedup budget, no
eviction or reset. New writes fail closed when full; already retained retries
still work. Do not bypass this limit with new adapters or epoch increments.

## Administrative and transport boundaries

`StorageAdmin::export`, `import` and `set_epoch` explicitly return `Unsupported`.
Local export drops dedup, so silently reusing it would destroy the retry
contract. Use the existing Raft snapshot path for replica recovery. Explicit
D1V1 epoch compare-and-set remains a low-level administrative operation; this
adapter does not implement routing or membership management.

This adapter accepts in-process `NodeHandle`s, not remote addresses. It does not
add commands to the existing TCP control port, forward to arbitrary endpoints,
or claim that the D1 raw byte limits fit every JSON/network frame. Existing TCP
frame constraints, unauthenticated transport and legacy control-port restrictions
remain unchanged. No TLS, authorization or production transport safety is claimed.

## Compatibility

No graph, Raft log or persisted snapshot format changes. The root D1 trait adds
an execution-context preflight with a default success implementation for local
nodes; wrappers around context-restricted nodes must forward it. New
`StorageNodeError` variants require updating downstream exhaustive matches.
Existing convenience graph CRUD signatures and local behavior are unchanged.

## Verification

- Root prepared-mutation tests cover all CRUD, local reopen, stale condition,
  target/epoch fencing and an unknown committed result resubmitted unchanged
  through a replacement adapter without a fresh read.
- Atomic D2 snapshot tests interleave writes/read captures and verify sequence
  and both data values agree, with exact row/byte boundary rejection.
- Real three-node uncertain-commit regression locks apply until a client timeout,
  then verifies eventual commit, exact duplicate retry and epoch fencing.
- A deterministic lock-contention regression verifies direct Tokio CRUD and
  prepared commits reject before the graph mutex while legitimate blocking work
  completes successfully on a single-thread runtime.
- Adapter unit tests cover typed mapping, admission/timeout identity, direct
  runtime-worker rejection, single-thread runtime bridging and stopped Raft.
- `crates/replicated-graph/tests/cluster.rs` exercises the actual three-node
  durable Raft graph path and fault/recovery cases. It uses the in-process drop
  policy, not a claim of production network fault coverage. Node shutdown/restart
  here is graceful; this is not additional graph-process SIGKILL or power-loss
  evidence. Existing process tests retain their separate documented scope.

All scans and preparation loops retain existing graph limits. Snapshot copy
loops stop on row/byte bounds; waits have deadlines. Existing D2 logical
snapshots and apply-map clones remain prototype costs, not scalability evidence.

## Explicit paged snapshot reads

The adapter supports D1 `scan_page` with an opaque exclusive cursor tied to the
original immutable view and range. Each page checks execution context, shard,
and the contacted replica's epoch, just like `scan`; it never reacquires a
snapshot or silently changes sequences after failover. The new API does not
change the existing all-or-error `scan` contract or query-level budgets. See
[the D1 pagination contract](STORAGE_NODE_INTERFACE.md#explicit-bounded-pagination)
for row/byte limits, oversized-row errors, cursor lifetime, and compatibility.

## Single-shard Gremlin over the adapter

`GraphRecordStore::execute_gremlin("g.V().out().count()")` now executes the same
standard Gremlin v0 parser and operators as `GraphEngine`. Run it inside
`run_blocking`, like graph CRUD. It pins one leader-barrier D1 view and routes all
paged scans and point reads through `ReplicatedStorageNode`; no view access skips
context or epoch checks. `GraphRecordStore::read_view` exposes a retained
`GraphRecordReadView`; its `execute_gremlin` repeats queries at that same sequence.
New leader/quorum reads can fail while a previously acquired view remains usable
until epoch fencing. Neither path automatically routes to a new leader.

The query row cap is 10,000, not the CRUD adjacency convenience cap of 1,000.
Pages are at most 256 rows / 16 MiB, and individual records share the 16 MiB bound.
Oversized records, unsupported pagination, stale epochs, incompatible runtime
contexts, read-barrier failures and query limits return errors, never truncated
success. See [QUERY_SPEC.md](QUERY_SPEC.md) for the precise contract.

This closes only the bounded, in-process, single-shard replicated query path.
K3 write-overlay transactions still belong to `GraphEngine`; cross-shard queries,
client/server query protocols, routing and production availability remain absent.
