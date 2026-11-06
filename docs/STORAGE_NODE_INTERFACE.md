# Storage node interface v0

Status: v0 proposal for P0/D1; a single-shard local adapter is implemented (see
[Implementation status](#implementation-status)). This document defines the
narrow boundary between the graph kernel and a replicated storage node. It is
deliberately smaller than a database API: the node understands opaque
key/value bytes and one-partition batches, not vertices, edges, Cypher, or query
plans.

K1 is the current local implementation: RocksDB, `u64` ids, partition-scoped
keys, deterministic protobuf-compatible records, and single-`WriteBatch`
atomicity. See [`GRAPH_FORMAT.md`](GRAPH_FORMAT.md).

## Implementation status

The first local adapter is implemented in [`src/storage_node.rs`](../src/storage_node.rs)
with tests in [`tests/storage_node.rs`](../tests/storage_node.rs). It owns one
`ShardId` and one opaque key space in RocksDB, with separate `data` and `meta`
column families. `batch_write` updates data, the applied sequence, and the
idempotency record in one atomic write. Optional `batch_write_conditional` compares
the read-view sequence under that same write lock. Stale conditions return typed
`Conflict { expected, current }` without writing data, sequence, or dedup metadata.
Retries return `Duplicate`, conflicting operations or conditions return `IdempotencyConflict`, and stale epochs return `Fenced`. Read views
are stable and bounded, scans enforce half-open ranges plus row/byte limits, and
`StorageAdmin` provides export/import plus epoch advancement.

This is deliberately not the full interface: transactions (`begin`), shard
routing, split/transfer, follower reads, and mixed-shard rejection are not
implemented, and the node has no leader/ownership identity yet (it never returns
`NotLeader`; any caller that presents the stored epoch is accepted). The adapter
currently copies the data partition into each read view (bounded to 1,000,000
rows / 64 MiB) instead of pinning a borrowed RocksDB snapshot; a production
adapter must replace this with a real snapshot. Once that cap is exceeded,
`read_view`/`get`/`scan` return `ResourceExhausted`; `export` enumerates the
data directly so an operator can inspect or migrate it with a different adapter,
while this adapter's `import` rejects oversized inputs. Batches are capped at 10,000 operations / 16 MiB. Dedup records are
retained indefinitely in this prototype; bounded retention is not implemented.
`import` is one-shot, requires a pristine target (sequence 0 and no live keys),
and copies the source epoch; a new owner must call `set_epoch` after import
before accepting writes on both sides.

[`src/graph_records.rs`](../src/graph_records.rs) adds a graph-record adapter over
any `StorageNode`. It reuses the K1 `graph/v1` key layout and record codec for
vertices, edges, and outgoing/incoming adjacency entries, and applies each
mutation as one `batch_write`. The adapter covers create/update/delete for
vertices and edges, label-filtered adjacency reads, cascade deletion, and
reopen. Before returning adjacency results or submitting a cascade batch, it
checks every scanned adjacency key against its edge record (direction, endpoint,
label, and id) and requires an empty adjacency value. A mismatch or missing edge
is `CorruptData`; a failed cascade leaves both data and applied sequence unchanged.
This is validation of the scanned index, not a full-graph integrity audit: absent
adjacency entries cannot be discovered without scanning edge records.
Mutations through one store are serialized. Each mutation validates against one
read view and submits that view's sequence as a conditional batch, so independent
stores sharing a conditional-capable node cannot both commit from the same
sequence. The losing mutation returns the typed storage `Conflict` wrapped in
`Error::StorageNode`; retry the entire graph operation to re-read and validate.
There is no automatic retry. The conservative shard-wide check also conflicts on
unrelated writes, including other partitions in the shard. Direct opaque-byte
writers remain responsible for graph validity; this adapter cannot prevent them
from writing malformed graph records after a valid mutation. It caps adjacency
reads (and therefore a single cascade delete) at 1,000 incident edges and
returns a storage error instead of a partial result, and leaves multi-shard
routing and transactions to D3/D4. Each graph mutation invocation uses a fresh
OS-random 128-bit request id, so identical creates from independent stores are
not mistaken for retries. Entropy failure returns a storage error without a
write. Explicit `prepare_*` methods return an immutable `PreparedGraphMutation`;
retain and resubmit it through `commit_prepared` after an unknown outcome, including
through another leader adapter for the same shard/partition. This preserves the
original identity, bytes, epoch and condition without new reads. It is not a durable
client journal. A new convenience CRUD invocation is a new attempt. See the
[in-process Raft adapter contract](REPLICATED_GRAPH.md). `GraphEngine` still
uses its own RocksDB handle, so the existing K1 path remains authoritative until
the engine is wired through this adapter.

### Local performance snapshot

`examples/storage_node_bench.rs` is a reproducible local benchmark:

```text
cargo run --release --example storage_node_bench --locked
```

It writes 100,000 opaque keys with 100-byte values in 100-operation,
`sync=true` batches, then measures a stable read view, point gets, a full scan,
and reopen. Read operations report the fastest of five trials. On 2026-11-06, three runs on an Apple M2 Pro (macOS 14.7.8) produced:

| Operation | Observed |
| --- | ---: |
| batch_write | 393k–404k ops/s (0.25 ms per 100-op batch) |
| read_view (100k rows copied) | ~34.4 ms |
| get | 875k–996k ops/s (~1.0–1.1 µs/op) |
| scan (100k rows) | 6.7M–8.4M rows/s (~12–15 ms) |
| reopen | 113–130 ms |

These are single-machine, local-APFS numbers, not a production or replicated
capacity claim. The read view copies the partition, so its cost grows with the
shard size; `sync=true` is included in the write result.

## Goals

- Let the kernel and distributed layers evolve independently.
- Make the atomic unit explicit: one partition-local batch.
- Support an in-process RocksDB adapter now and a Raft-replicated shard later.
- Make retries, fencing, snapshots, and migration first-class, not caller
  folklore.
- Keep the interface byte-oriented so storage format changes do not leak into
  replication or routing code.

## Non-goals for v0

- Cross-partition transactions.
- Cross-partition edge creation (see “Boundary edges”).
- Raft implementation details, leader election, or membership.
- Query parsing/planning, graph semantics, or secondary indexes.
- Multi-tenant quotas and billing.

## Identity and ownership

```text
ShardId      u64       physical replication unit
PartitionId  u32       K1 key-space partition (already in every graph key)
Epoch        u64       monotonically increasing shard ownership term
NodeId       u64       storage/replica identity
RequestId    [u8; 16]  client idempotency key
```

A shard owns one or more partitions (or a key range inside partitions). A node
serves a shard only while it believes it is the leader for the current `Epoch`.
Every write carries `(shard, epoch)`; a stale epoch is fenced, never applied.

Keys are opaque bytes. The graph format's `graph/v1/p/<partition>/...` prefix is
a kernel convention the node must not reinterpret. Range scans are half-open
`[start, end)` byte ranges and may be bounded by a row limit and a byte limit.

## Operations

### Reads

```text
get(key, read_view) -> Option<value>
scan(range, limit, byte_limit, read_view) -> stream of (key, value)
```

`None` means the key is absent; it is not an error. Reads are served from a
`ReadView`:

```text
ReadView { shard, epoch, sequence }
```

- `sequence` is the applied state-machine sequence, not wall-clock time.
- A leader read at the latest sequence is linearizable.
- A snapshot read pins a sequence for the duration of a query/transaction.
- Follower reads are allowed only when the caller sets `allow_stale`; the
  response reports the observed sequence.

### Writes

```text
batch_write(shard, epoch, request_id, operations) -> WriteResult
batch_write_conditional(request, expected_sequence: Option<Sequence>) -> WriteResult
```

`operations` is an ordered list of `put(key, value)` and `delete(key)`. The whole
batch is atomic or fails as a unit; there is no partial success. `WriteResult`
contains the applied sequence and a committed/duplicate/dry-run status.

`request_id` is the idempotency key. A retry with the same id and identical
operations and condition returns the original result; the same id with different
operations or condition (including `Some` versus `None`) is an
`IdempotencyConflict`. The dedup window is bounded and documented by the node
(proposal: at least 10 minutes or the Raft log retention window, whichever is
shorter).

`expected_sequence = None` preserves the original unconditional behavior.
`Some(sequence)` is an atomic shard-wide compare-and-write precondition, not a
per-key version or a promise of cross-shard transactions. The caller must obtain
it from the exact read view used for all validation reads. Adapters that do not
support this operation return `Unsupported` for `Some`, never an unconditional
write. The default trait method preserves existing implementations and callers.

Local write ordering is:

1. Validate shard, epoch, nonempty operations and size limits.
2. Acquire the node's write lock and recheck epoch.
3. Look up the request id. Identical operations and condition return the original
   `Duplicate` result, even if the sequence has advanced. Different identity
   returns `IdempotencyConflict` before the condition check.
4. For a new request, compare the expected sequence with the current sequence.
   A mismatch returns `Conflict { expected, current }` and consumes no request id.
5. Check sequence overflow; atomically write data, the incremented sequence, and
   the dedup record. Epoch revalidation, dedup, condition and overflow checks, and the
   commit remain under the same lock.

Thus a retry of a known committed conditional batch succeeds as `Duplicate`,
while a new stale request cannot mutate any state. Fencing still takes precedence
over deduplication. After a conflict, refresh the view and revalidate the complete
operation, rather than changing a condition on a previously committed request.

A write must not mix keys owned by different shards. That is a client routing
error, not a partially applied transaction.

### Single-shard transaction

```text
begin(shard, epoch, options) -> TransactionHandle
txn.get / txn.scan / txn.put / txn.delete
txn.commit() / txn.abort()
```

v0 only promises a transaction whose keys are owned by one shard. The graph
kernel's K3b now provides snapshot reads, conservative first-committer-wins
conflict detection, one-batch atomic commit, and a process-crash recovery
harness. D4 builds cross-shard coordination above this interface, never inside
it.

### Snapshots

`read_view()` returns a pinned sequence for a bounded lease. The node must reject
or renew expired views; a query that exceeds its lease must not silently change
its snapshot. The target behavior is cheap snapshot creation that does not copy
the graph; the current local adapter's copied view is an explicitly documented
prototype exception.

### Split / migration hooks

```text
admin.export_range(shard, range) -> snapshot stream + manifest
admin.import_range(target_shard, manifest, stream) -> installed sequence
admin.freeze(shard, epoch) -> frozen shard
admin.install(shard, epoch, manifest) -> new owner
admin.abort_migration(shard) -> rollback
```

Export is at a pinned sequence. Import validates the manifest, key range,
checksums, and sequence before making the range visible. The last step is an
epoch bump in the catalog; until then the old owner remains authoritative.
Rollback discards imported data and leaves the source authoritative.

Migration must be restartable and idempotent. A partially imported range is
never visible.

## Error model

```text
NotFound             not an error for get; scan simply ends
InvalidArgument      malformed range, empty batch, mixed-shard batch
NotLeader            include leader hint and current epoch when known
Fenced               stale epoch; refresh route and retry
Unavailable          no quorum / temporarily unreachable; retryable with backoff
IdempotencyConflict  same request id, different bytes
Conflict             expected/current sequence mismatch; retryable by re-reading
ResourceExhausted    scan/row/byte limit
Corrupt              checksum/format/version failure; do not auto-retry
Unsupported          unknown interface/feature version
```

Retry policy is part of the contract: `Fenced`, `Unavailable`, and `Conflict`
are retryable after refreshing route/read view; `Corrupt`,
`IdempotencyConflict`, and `InvalidArgument` are not.

## Consistency contract

- A committed batch is visible to every later leader read at a sequence greater
  than or equal to its `WriteResult.sequence`.
- A snapshot read sees exactly the state at its pinned sequence.
- The current local adapter copies the partition at `read_view` rather than
  pinning a RocksDB snapshot; the copy is bounded (1,000,000 rows / 64 MiB) and
  is a prototype limitation, not the target production behavior.
- Writes from one client are not reordered by retries because `request_id`
  deduplicates them.
- v0 makes no cross-shard atomicity promise. D4 adds it explicitly.

## Versioning and negotiation

```text
INTERFACE_VERSION = 0
features = bitset
```

The client sends its version and required feature bits on connect. The node
rejects unknown required features with `Unsupported`; optional features are
reported in the handshake. The current in-process adapter has only
`interface_version()`, not a negotiated RPC handshake or feature bitset.
`batch_write_conditional` is an additive optional operation at interface version
0; its default implementation returns `Unsupported` for a condition. Existing
`WriteRequest` struct literals and unconditional `batch_write` calls are unchanged.
The additive typed `Conflict` variant requires exhaustive error matches to be
updated. A future transport must explicitly encode and negotiate conditional
support, never drop the condition. D2 does not implement this synchronous trait
method. Its independently versioned `D1V1` application commands now replicate
these write semantics after explicit activation; legacy `TxnBatch` does not.
See [`D2_D1_CONTRACT.md`](D2_D1_CONTRACT.md) for the different persistence,
snapshot and retained-request budget boundaries. The separate
[`replicated-graph` adapter](REPLICATED_GRAPH.md) implements this trait in-process;
a remote graph service remains unimplemented.

Local dedup metadata gains an optional `expected_sequence` field. Missing means
`None`, so existing records remain readable. Unconditional writes omit the field
and retain the old representation. Conditional writes persist the condition,
including across reopen. An older binary does not understand conditional request
identity: do not downgrade a shard with conditional writes and replay those
request ids through the old unconditional API. Export/import still deliberately
omits dedup records and therefore does not preserve retry identity across
migration. Graph keys and record bytes remain v1 and unchanged; no graph data
migration is needed.

## Rust sketch

```rust
pub trait StorageNode: Send + Sync {
    fn read_view(&self) -> Result<ReadView>;
    fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>>;
    fn scan(&self, range: KeyRange, limit: ScanLimit, view: &ReadView)
        -> Result<Vec<KeyValue>>;
    fn batch_write(&self, request: WriteRequest) -> Result<WriteResult>;
    fn batch_write_conditional(&self, request: WriteRequest,
        expected_sequence: Option<Sequence>) -> Result<WriteResult>;
    fn begin(&self, request: TransactionRequest) -> Result<Box<dyn Transaction>>;
    fn admin(&self) -> &dyn StorageAdmin;
}
```

The trait is intentionally synchronous and byte-oriented. Async/RPC adapters
wrap it; the graph kernel sees the same operations whether the node is local
RocksDB or a replicated shard.

## Read sequence

```text
client -> route(shard, epoch) -> node
client -> node.read_view() -> {shard, epoch, sequence}
client -> node.get/scan(..., view)
```

If `Fenced`, the client refreshes the route and retries with the new epoch.

## Write sequence

```text
client -> route(shard, epoch)
client -> batch_write(shard, epoch, request_id, ops)
node: validate owner/epoch -> append to replication log -> apply -> dedup[request_id]
node -> {committed, sequence}
```

The batch is applied only after replication commit. A retry with the same
`request_id` returns the recorded result even if the response was lost.

## Boundary edges

K1 requires both endpoints of an edge in one partition. Real graphs have edges
that cross partition boundaries. v0 therefore **rejects** cross-partition edge
creation; it does not pretend the edge exists somewhere.

The D1 decision must choose one placement policy before cross-shard writes are
enabled:

1. **Source-owned edge + target boundary index**: edge record and out-adjacency
   live with the source; target keeps a lightweight incoming reference. A
   cross-shard batch is required and cannot be v0-atomic.
2. **Co-locate endpoints**: route an edge only when both endpoints are in the
   same shard; otherwise the query planner performs a distributed join. Simple,
   but can strand high-degree vertices.
3. **Duplicate edge on both endpoint shards**: doubles writes and needs conflict
   resolution. Not recommended for v0.

The recommendation is (1) after D4 supplies cross-shard coordination, with (2)
as the safe default until then.

## Acceptance tests for the local adapter

- `get` returns `None` for a missing key and never invents an error.
- A batch with a failed operation writes nothing.
- Retrying the same `request_id` and bytes is idempotent; different bytes is
  `IdempotencyConflict`.
- A stale epoch returns `Fenced` and the old owner cannot apply writes.
- A stale/future condition returns typed `Conflict` and changes no data,
  sequence, or dedup metadata. A refreshed uncommitted request can succeed.
- Conditional duplicates preserve the original result after other commits and
  reopen; changed conditions are idempotency conflicts, and stale epochs fence
  even exact retries. Legacy dedup records remain readable.
- Deterministic multi-store races cover identical/different same-id creation,
  endpoint deletion versus edge creation, and label/endpoint changes versus
  cascade deletion, in both winner orderings. Exactly one commits and the other
  cannot partially change records or sequence.
- `scan` respects start/end, row limit, and byte limit; a range with no data
  returns an empty result.
- A snapshot read is stable across concurrent committed writes.
- Export/import preserves every key/value and sequence; a failed import leaves
  the source authoritative and the target empty.
- A mixed-shard batch is rejected before any write.

## Open P0 decisions

- Shard by vertex-id hash/range vs. explicit tenant partition; the K1 key
  already carries a partition, but the mapping is not frozen.
- Read linearizability: leader-only reads vs. lease/follower reads.
- Raft library choice and snapshot transfer format.
- Boundary-edge policy above.
- Maximum batch size, scan limits, and dedup retention.
