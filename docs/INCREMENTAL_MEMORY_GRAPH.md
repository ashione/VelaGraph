# Incremental memory graph (experimental)

Status: implemented as an optional local library backend in this proposal;
upstream integration, remote CI and production qualification are separate.
This follows the fixed-version read-model work without changing its API.
Tracking: [#370](https://github.com/ashione/VelaGraph/issues/370), milestone
[v0.0.3](https://github.com/ashione/VelaGraph/milestone/3). Implementation PR is linked below before publication.

## Problem and scope

Rebuilding an entire materialized graph after each small update repeatedly
scans and decodes unchanged records. The new backend retains one current
ordinary property graph and stages only affected records and adjacency lists.
It is useful when a caller already has a durable source and needs a reusable
read model. It is not a new durable authority or a replacement for `GraphEngine`.

The kernel understands `Vertex`, `Edge`, labels/properties, caller-assigned graph
identity, local sequence and opaque metadata. It has no object names, tenant IDs,
query keywords, hierarchy parent rules or source-specific cursor semantics.
Multiple edges, cycles and self-loops are allowed. Temporal assertion history,
managed/cohort versions, cross-partition edges and distributed certification are
not provided by this API. Existing disk-backed and temporal paths are unchanged.

The earlier local prototype supplied useful evidence, but its private workload
is not a public acceptance fixture. This PR carries generated tests and a
reproducible generated-data example; validation claims belong to the tested
revision, not every later commit.

## Alternatives and selected representation

- Full reconstruction is simple and remains useful for initial loading, large
  refreshes or recovery. Its cost grows with the entire graph.
- A mutable graph protected by a read/write lock avoids retained versions, but
  a long reader delays publication.
- Copying the entire graph per update avoids blocking old readers but duplicates
  unchanged records and indexes.

The selected implementation supports both `Locked` and `Cow` read ownership
using the same transaction staging and query executor. Vertex and edge tables
have 256 ID-mixed shards. Records use shared immutable allocations. Each vertex
has independent incoming/outgoing adjacency, split into sorted chunks containing
at most 256 edge IDs. COW copies touched table shards and adjacency chunks, not
all payloads. Empty table allocations are released and sparse tables compacted.

## APIs and ownership

```rust
use velagraph::{
    Edge, IncrementalGraph, IncrementalLimits, IncrementalMode, Vertex,
};
# fn example() -> Result<(), Box<dyn std::error::Error>> {
let graph = IncrementalGraph::new(
    "source/generation/partition", IncrementalMode::Cow,
    IncrementalLimits::default(),
)?;
let mut transaction = graph.begin()?;
transaction.put_vertex(Vertex::new(1))?;
transaction.put_vertex(Vertex::new(2))?;
transaction.put_edge(Edge::new(10, 1, 2, "LINK"))?;
transaction.set_metadata(b"caller-progress")?;
transaction.commit()?;
let view = graph.read()?;
let result = view.execute_gremlin("g.V(1).out('LINK').id()")?;
# Ok(())
# }
```

`new` creates an empty sequence-zero graph. `from_records` creates a private,
validated sequence-zero graph from a complete vertex/edge inventory. Duplicate
IDs or missing endpoints reject the whole graph. Both require a nonempty identity
of at most 1,024 UTF-8 bytes. The caller must assign different identities to
unrelated sources/generations; identity is neither authentication nor a global
allocator. A token contains that identity and the local publication sequence.

`read()` acquires the latest published version. All records, indexes and metadata
in that handle belong to one version. Public point reads borrow immutable data;
missing records return `None`. Raw vertex/edge iterators return ascending IDs,
using an ID buffer bounded by admitted graph size. Raw adjacency returns ascending
edge IDs and errors on a missing start. These are graph-library accessors, not
an unbounded query-language escape hatch. Gremlin retains its own stricter limits.

Read handles borrow the graph and are not `Send`, including the COW enum variant.
Acquire a handle in its reader thread and share the graph through `Arc` instead.
In Locked mode a handle holds a read lock: release every same-thread read handle
before committing, otherwise the caller blocks itself. COW readers retain an
immutable internal version and may continue while new versions are published.

## Transaction state and failure behavior

`begin()` pins a version and stages a private overlay. Reads consult staged
records first. `put_vertex` replaces labels/properties while retaining adjacency;
`put_edge` upserts a complete edge, detaching old endpoints and attaching new
ones. Both endpoints must already exist in the transaction view at that operation.
Deletion of a vertex removes every incident edge and both directional entries;
a self-loop is processed once. Missing deletes are charged no-ops.

A mutation/read-budget error poisons the transaction. It cannot subsequently
commit a partially failed overlay. `commit` releases its original read handle,
acquires the write lock and validates the complete identity/sequence token.
Any intervening publication causes `TransactionConflict`; no implicit retry is
performed. All validation, retained/copy budgets and COW backpressure checks
precede installation. A successful commit publishes records, indexes, metadata
and sequence together. Every success increments sequence, including empty or
metadata-only commits. Sequence overflow rejects rather than wrapping.

Locked publication installs the validated changes in place. COW constructs a new
state from shared tables and then replaces the current state. Old readers retain
old records and old metadata. Resource errors never turn into partial results.
Rust allocation failures and arbitrary process termination are not recoverable
transaction errors. Poisoned locks fail closed.

## Bounds and logical accounting

`IncrementalLimits` can only tighten positive defaults:

| Limit | Default / maximum |
|---|---:|
| Combined current vertices and edges | 1,000,000 |
| Current graph logical retention | 256 MiB |
| Transaction work units | 10,000 |
| Transaction logical work bytes | 16 MiB |
| Retained old COW versions | 8 |
| Conservative current + pinned version fees | 512 MiB |
| Opaque caller metadata | 64 KiB |
| Cooperative transaction preparation | 5 seconds |

Work units include reads, internal index changes and control work, not just public
method calls. Work bytes cover metadata copying, complete records, and conservative
bounds for structural table changes and adjacency cloning/movement. Immutable
record fees are cached and recalculated for replacements. Retained fees include
record/container capacity and coarse table-entry charges. COW version fees count
whole retained versions conservatively, even when they share allocations.
`copied_index_entries` and `copied_adjacency_ids` count Arc COW copies only; they
are not total allocation counts, CPU work or bytes copied by every container.

These are logical budgets, not allocator/RSS limits, and they do not compose into
a multi-tenant service budget automatically. A caller must handle old-reader
backpressure. Lock acquisition cannot be preempted by the preparation deadline.
Long Locked readers may therefore delay a commit beyond five seconds.

`IncrementalQueryLimits` can tighten 65,536 point/source/candidate operations and
64 MiB logical record/metadata work. Each Gremlin query also retains the existing
64 MiB query-owned memory budget, parser/hop/result limits, and cooperative
5-second execution deadline. Work accounting does not repeatedly sample the
clock: the shared executor checks read/operator/result boundaries, and scans
and adjacency check every candidate and completion. No periodic-check exemption
is introduced. Construction costs and parsing are not query execution time.

## Checkpoint format and publication

This is a separately versioned local read-model file, not a RocksDB key/value
layout, physical database backup or remote object-store protocol. It is an
explicit isolated R2/R4 scope extension. Existing graph/v1, graph/v2 and temporal
records are not migrated or reinterpreted. The private prototype did not have a
published compatibility guarantee; files must match this documented version.

A version-one file is at most 64 MiB and consists of:

1. Eight ASCII bytes `VGIM0001`.
2. Little-endian `u64` JSON payload length.
3. Little-endian `u64` FNV-1a checksum of the payload (corruption detection only).
4. UTF-8 JSON with ordered fields `identity`, `sequence`, `metadata`, `vertices`,
   `edges`. Metadata is an array of bytes; vertex and edge arrays are sorted by
   ID. Record fields use existing serde `Vertex` / `Edge` / `Value` representations;
   property names are canonical. Unknown top-level payload fields are rejected.

The golden vector is `tests/fixtures/incremental_checkpoint_v1.bin`. A format
change requires a new magic/version, migration/refusal note and updated vectors;
do not change a golden merely to accept different output. Other format versions,
length/checksum errors, duplicate IDs and invalid endpoints are rejected.

`checkpoint` captures one read view, serializes to a bounded buffer, writes a
unique same-directory temporary file, syncs it, renames it and syncs the parent
directory. An error after rename can leave a complete new file visible. This is
not a promise of rollback after filesystem failure. Locked checkpoint readers
block publication for their lifetime; COW readers can retain the captured version.

`restore` accepts a regular local file and expected identity. It validates format
and contents and rebuilds a private graph before returning it. Record strings,
labels and metadata are compacted; adjacency is rebuilt in tightly packed sorted
chunks, avoiding capacity inflation for identical data. Restore/full loading has
its own graph retention/record bounds and a cooperative 10-second index-build
budget, independent of small mutation-batch limits. File I/O, checksum and JSON
decoding precede that index-build budget and may allocate separately from retained
state. No file authentication, hostile-host race protection or hard RSS bound is
claimed.

A memory commit is **not a durable acknowledgement**. Recovery requires the latest
completed checkpoint plus a caller-owned replayable suffix. Opaque metadata lets
the caller publish graph changes and its own cursor together; the kernel does not
certify an upstream snapshot cut, combine independent event streams, define CDC
ordering, or guarantee retry identity across an arbitrary source change.

## Compatibility, rollout and verification

The API is optional; no existing store, transport, query dialect, persisted key or
service route switches to it. `IncrementalQueryLimits` is independent of temporal
query limits. The root error enum adds an `IncrementalMemory` variant with typed
state/I/O context; exhaustive downstream matches may need an additional arm.
Existing paths do not emit that new variant.

Start with library integration and source-specific recovery validation. Do not
replace service caches until source identity/offset contracts, checkpoints,
reader lifetimes, aggregate memory limits and failure policies are established.
The existing temporal materialized model has not been moved to this backend.

Acceptance covers atomic failure/poisoned transactions, identity/version conflicts,
edge replacement/cascade/self-loops, deterministic queries, exact query/work bounds,
pinned-version backpressure, retained capacity release, labeled/precise-budget
checkpoint restore, corruption/format rejection and concurrent record/metadata
coherence. The generated `incremental_memory` example compares incremental updates
with rebuilding the same logical graph and checks results at every version.

Local proof does not establish multi-machine HA, production CPU/QPS, power-loss
recovery or safe CDC publication. Those remain separate integration work.

## Delivery

Tracking: #370, v0.0.3; the implementation PR link is added on publication. This is
not completion of the broader managed recovery or CDC milestones.
