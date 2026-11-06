# Single-partition transactions and recovery (K3) v0

Status: the K3 boundary is frozen. K3a (snapshot read view), K3b
(snapshot-isolated write transactions and conservative conflict detection), and
K3c (process-crash recovery harness) are implemented. Autocommit writes still
use the old no-fsync behavior. This document is the contract the implementation
and its tests satisfy.

Related contracts: [`GRAPH_FORMAT.md`](GRAPH_FORMAT.md) (keys and records),
[`STORAGE_NODE_INTERFACE.md`](STORAGE_NODE_INTERFACE.md) (single-shard
transaction placeholder), and [`IMPLEMENTATION_RULES.md`](IMPLEMENTATION_RULES.md)
(R4 atomicity, R5 determinism, R8 tests).

## 1. Scope and boundary

K3 v0 applies to **one open `GraphEngine` owning one physical partition in one
OS process**, and to graph keys only.

- It MUST NOT span multiple `GraphEngine` handles, partitions, processes, or
  the SQL table namespace.
- It MUST NOT provide cross-partition or cross-shard atomicity. D4 owns that.
- Existing autocommit graph methods remain callable and keep their documented
  behavior; transactions are additive.
- The SQL engine is not changed. A transaction cannot mix SQL rows and graph
  records.
- K3 v0 does not promise global serializability, distributed reads, or
  idempotent client retries. D1/D4 add those above this layer.

The boundary is deliberately narrow because a transaction manager that is not
partition-local would force routing and consensus decisions into the kernel.

## 2. Isolation and conflict decision

K3 v0 provides **snapshot isolation with conservative first-committer-wins**.

Each transaction:

1. pins a RocksDB snapshot and its sequence number at `begin`;
2. reads only from that snapshot (plus its own buffered writes);
3. at commit, acquires the engine commit lock and checks that the database's
   latest sequence number is still the snapshot sequence;
4. if any write committed after the snapshot, aborts with
   `TransactionConflict`; otherwise validates, writes one batch, and commits.

The sequence check is intentionally conservative: a write to any key after the
snapshot conflicts, including a write to a disjoint key. This avoids maintaining
a second per-key version index, and it guarantees that two overlapping writers
cannot both commit. It can abort transactions that a key-level implementation
would allow; that is an accepted v0 trade-off.

Consequences the implementation and tests MUST state:

- A transaction's reads are repeatable for its lifetime.
- A transaction never observes another transaction's uncommitted writes.
- A transaction sees its own buffered writes.
- Read-only transactions do not conflict and do not write.
- Write skew is not possible between overlapping committed transactions because
  at most one overlapping writer commits; this is stronger than plain SI but
  still not a promise of global serializability.
- A conflict is reported, never retried silently.

## 3. Read views

`GraphEngine::read_view()` returns a borrowed read view that pins a RocksDB
snapshot. The first K3 slice exposes point and adjacency reads, but no writes;
bounded scan reads and Gremlin integration follow in later K3 slices:

```rust
pub struct GraphReadView<'a> { /* snapshot + partition */ }

impl GraphEngine {
    pub fn read_view(&self) -> GraphReadView<'_>;
}

impl GraphReadView<'_> {
    pub fn sequence(&self) -> u64;
    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>>;
    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>>;
    pub fn out_edges(&self, vertex_id: u64, label: Option<&str>) -> Result<Vec<Edge>>;
    pub fn in_edges(&self, vertex_id: u64, label: Option<&str>) -> Result<Vec<Edge>>;
}
```

Rules:

- A read view MUST be stable across concurrent commits until it is dropped.
- Read limits from `QUERY_SPEC.md` still apply to scans; a view must not become an
  unbounded full-graph escape hatch.
- The sequence is the pinned RocksDB sequence, not wall-clock time.
- Each `GraphEngine::gremlin` / `GremlinQuery::execute` execution pins one
  read view after parsing, before any graph reads. Start records, adjacency,
  edge records and target vertices all use this sequence; concurrent commits
  are visible only to subsequent executions.
- The query-only backend uses checked bounded scans and an incremental checked
  adjacency visitor. Public read-view point/adjacency APIs remain unchanged;
  there is no new public query-on-existing-view API in this slice.
- A view holds a RocksDB snapshot; callers should keep its lifetime bounded.
  Snapshot creation copies no graph data.

## 4. Transaction API

```rust
impl GraphEngine {
    pub fn begin(&self) -> Result<GraphTransaction<'_>>;
}

pub struct GraphTransaction<'a> { /* snapshot, overlay, state */ }

impl GraphTransaction<'_> {
    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>>;
    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>>;
    pub fn create_vertex(&mut self, vertex: &Vertex) -> Result<()>;
    pub fn update_vertex(&mut self, vertex: &Vertex) -> Result<()>;
    pub fn delete_vertex(&mut self, id: u64) -> Result<bool>;
    pub fn create_edge(&mut self, edge: &Edge) -> Result<()>;
    pub fn delete_edge(&mut self, id: u64) -> Result<bool>;
    pub fn commit(self) -> Result<()>;
    pub fn abort(self);
}
```

The exact Rust names may be adjusted during implementation, but these semantics
are normative.

### Transaction state

- `begin` creates the snapshot and an empty write overlay.
- Reads consult the overlay first, then the snapshot. A delete in the overlay
  hides the snapshot value.
- Mutations validate immediately where possible and are revalidated under the
  commit lock. No mutation touches RocksDB before `commit`.
- Each transaction records its read sequence, touched keys, and bounded encoded
  write set.
- A transaction is single-use. Any operation after `commit` or `abort` returns
  `TransactionClosed`.
- Dropping a transaction without `commit` is equivalent to `abort`; it writes
  nothing.

### Commit algorithm

Under the engine commit lock:

1. Reject if the transaction is already closed, or if it has no writes
   (read-only commit is a successful no-op).
2. If `db.latest_sequence_number() != read_sequence`, return
   `TransactionConflict` without writing.
3. Revalidate the complete overlay against the current database state: ids,
   labels, duplicate creates, missing endpoints, and all edge/adjacency updates.
4. Build exactly one `WriteBatch` containing record changes and both adjacency
   indexes, including cascades from vertex deletion.
5. Write the batch with `WriteOptions::set_sync(true)` so a successful commit is
   WAL-durable before the call returns.
6. Mark the transaction committed and release the lock.

If any step fails, the batch is not written. There is no partial commit and no
best-effort repair inside the transaction path.

### Edge and cascade rules

A transaction MUST preserve the K1 invariants:

- both endpoints of a created edge exist in the overlay/snapshot;
- edge ids are unique;
- deleting a vertex deletes every incident edge and both adjacency entries;
- an edge record and its two adjacency entries are in the same batch;
- no cross-partition keys are accepted.

Because K3 v0 is one partition, a mixed-partition transaction is rejected at the
API boundary rather than detected halfway through commit.

## 5. Limits and errors

Proposed limits (values may be tuned before implementation, but must be explicit
and tested):

- maximum encoded write set: 16 MiB;
- maximum operations per transaction: 10,000;
- maximum read/scan rows: the query limits, or 10,000 for direct scans.

Proposed typed errors:

| Error | Meaning |
|---|---|
| `TransactionConflict` | another write committed after the snapshot |
| `TransactionClosed` | commit/abort/operation on a closed transaction |
| `TransactionTooLarge` | operation or byte limit exceeded |
| `UnsupportedTransaction` | request outside the single-partition K3 boundary |

Existing validation errors (`VertexAlreadyExists`, `VertexNotFound`,
`EdgeEndpointNotFound`, `InvalidGraphLabel`, `CorruptData`, RocksDB errors) are
reused and abort the transaction without writing.

## 6. Durability and recovery

K3 uses RocksDB's WAL as the only commit log. There is no application-level WAL
and no second source of truth.

- A transaction commit sets `sync=true`; once `commit()` returns, the batch is
  durable under the process/OS crash model exercised by K3 tests.
- RocksDB WAL replay on `GraphEngine::open` recovers committed batches. A batch
  is the atomic unit, so recovery never exposes an edge without its adjacency
  entries.
- Existing autocommit methods retain their current default WAL behavior (no
  per-statement fsync) until a separate compatibility change migrates them.
  K3 must not claim that those autocommit writes are synced.
- Writes that were not acknowledged as committed may be lost after a crash; that
  is allowed and documented. They must never appear half-written.
- The SQL engine's durability behavior is out of scope.

### Recovery tests

The implementation must add a deterministic crash-point test, not only a clean
drop/reopen:

1. a child process opens a graph, begins a transaction, writes a vertex/edge
   plus adjacency, and signals "about to commit";
2. the parent kills the child with `SIGKILL` at the selected point;
3. the parent reopens the graph and asserts either the whole batch is present or
   none of it is, and that adjacency invariants hold;
4. a second point after a successful commit signal asserts the batch is present.

Running this test on CI must not depend on wall-clock sleeps or a fixed port. If
the harness cannot be made deterministic on all supported platforms, the PR must
say which crash window is covered by the automated test and which is documented
but not exercised.

K3c implements this in `tests/recovery.rs`. The parent spawns the same test
binary as a child, seeds a synced base transaction, and kills the child at two
deterministic points: after buffering but before `commit`, and after `commit`
returns. The test asserts that the first loses the whole buffered batch and the
second keeps the vertex, edge, and both adjacency entries. It uses process
`SIGKILL`/`Child::kill`, not a simulated close, and has no sleeps or ports. It
does not inject a failure inside RocksDB's WAL write; that interior window relies
on RocksDB's WAL atomicity and is documented rather than exercised by this
harness.

## 7. Compatibility and migration

- No graph key or payload change is introduced by K3. Snapshot/read-view and
  transaction state are runtime state, not persisted formats.
- Existing `GraphEngine` autocommit callers compile unchanged.
- Existing Gremlin APIs are unchanged; each execution now reads a single
  snapshot rather than potentially mixing live reads across concurrent commits.
- Reopen tests must show old data remains readable and that a committed
  transaction survives reopen.
- D1 can later wrap the same commit unit in a storage-node `batch_write`; D4
  must not reuse this local commit path for cross-shard coordination.

## 8. Acceptance tests

- Snapshot stability: read view before a committed write sees the old value
  after the write; a new view sees the new value.
- Read-your-writes: a transaction reads its own buffered vertex/edge before
  commit; another reader cannot see it.
- Conflict: begin, commit an autocommit write, then commit the transaction ->
  `TransactionConflict`, and none of its writes appear.
- Atomic failure: a transaction with a missing edge endpoint or duplicate id
  writes nothing.
- Atomic success: create two vertices and an edge in one transaction; after
  commit, the edge, both adjacency entries, and both vertices are visible
  together.
- Cascade: delete a vertex in a transaction; all incident edges and adjacency
  entries are removed in the same commit.
- Reopen: committed transaction data survives close/reopen; uncommitted
  buffered writes do not.
- Read-only commit is a no-op.
- Closed transaction operations return `TransactionClosed`.
- Limits: write-set byte/operation limits return `TransactionTooLarge` before
  any write.
- Crash harness: the documented crash windows above.

## 9. Deferred

- Cross-partition/cross-shard transactions (D4).
- Global serializability, distributed snapshots, and follower reads (D3/D4).
- Per-key version indexes and non-conservative conflict detection.
- Transaction retry, idempotency keys, and client request IDs (D1 node layer).
- SQL/graph combined transactions.
- Gremlin through replicated storage needs a bounded, paginated read interface;
  the local query backend does not alter the D1 scan contract or provide Raft reads.

## 10. Implementation slices

1. **K3a — read views (implemented):** snapshot-backed `GraphReadView`,
   point/adjacency reads, and stability tests. Gremlin now pins one view per
   execution with checked bounded scans and incremental adjacency reads.
2. **K3b — transactions (implemented):** overlay, validation, one-batch synced
   commit, conflict detection, cascade tests.
3. **K3c — recovery (implemented):** process-crash harness in
   `tests/recovery.rs` and the durability/recovery documentation above.

Each slice is a separate PR with its own green CI; K3 is not complete until all
three acceptance sets pass.

## Isolated v2 append-only experiment

[EXPERIMENTAL_CROSS_EDGES.md](EXPERIMENTAL_CROSS_EDGES.md) documents a separate
versioned namespace and explicitly non-atomic source/reverse append protocol.
It does not change this document's existing v1/K3/Gremlin contract or complete
D4/D5. Its out/in observations cannot certify missing-index completeness.
