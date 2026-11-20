# Incremental memory graph (experimental)

Status: implemented as an optional local library backend in this proposal;
upstream integration, remote CI and production qualification are separate.
This follows the fixed-version read-model work without changing its API.
Tracking: [#370](https://github.com/ashione/VelaGraph/issues/370), milestone
[v0.0.3](https://github.com/ashione/VelaGraph/milestone/3). Implementation: [PR #371](https://github.com/ashione/VelaGraph/pull/371).

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
commit a partially failed overlay. Locked `commit` releases its original read
handle, acquires the write lock and validates the complete identity/sequence token.
Any intervening publication causes `TransactionConflict`; no implicit retry is
performed. All validation, retained/copy budgets and COW backpressure checks
precede installation. A successful commit publishes records, indexes, metadata
and sequence together. Every success increments sequence, including empty or
metadata-only commits. Sequence overflow rejects rather than wrapping.

Locked publication installs the validated changes in place. COW serializes commit
preparation with a per-graph writer mutex that readers never acquire. It validates
the base token and admits current + candidate preparation peak + pinned version
fees before cloning the candidate. The peak starts at the complete base fee and
adds positive metadata, vertex-entry (including adjacency), and edge deltas before
crediting any decreases. This covers shrinking batches and arbitrary hash-map
installation order. Index copies, updates and compaction run outside the publication
lock. It then takes a short write lock, rechecks the token, deadline and pinned
limits, and swaps the complete state. Retired strong references and discarded
candidates are released outside the publication lock. Old readers retain old
records and old metadata.

Only one COW commit candidate is prepared at a time, including its reclamation.
Concurrent transactions can still stage independently, each under its existing
transaction limits; this is not a global quota for all staged transaction memory.
A writer whose base became stale returns `TransactionConflict` before copying;
there is no automatic retry. COW checks its cooperative deadline after waiting
for writer serialization and publication, and around candidate table mutations.
The locks themselves are not cancellable. A single allocation, table mutation or
destructor is not preemptible.

This uses two logical versions during preparation, with shared unchanged records
and index shards. It does not maintain two independent full graph buffers or
replay updates into a standby graph. Slow readers can retain additional versions
under the existing limits. This shortens version-acquisition stalls; it does not
reduce traversal or response construction work, guarantee reader fairness, or
remove allocation and memory-bandwidth contention. Resource errors never turn into partial results.
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
| Conservative current + candidate + pinned version fees | 512 MiB |
| Opaque caller metadata | 64 KiB |
| Cooperative transaction preparation | 5 seconds |

Work units include reads, internal index changes and control work, not just public
method calls. Record work admission precedes semantic validation, including validation that
constructs an error containing a label or property name. This applies to borrowed
`apply` and owned transaction puts. Bulk construction/restore checks each record
using allocation-free compacted-size accounting before shrinking capacities or
validating it; this preserves exact-retention restore with overallocated inputs.
Work bytes cover metadata copying, complete records, and conservative
bounds for structural table changes and adjacency cloning/movement. Immutable
record fees are cached and recalculated for replacements. Retained fees include
record/container capacity and coarse table-entry charges. COW version fees count
whole versions conservatively, even when they share allocations. COW admission
includes current plus the conservative candidate construction peak during
preparation even without old readers; the final candidate size alone is not enough. A tightened `max_pinned_accounted_bytes` that fits only the final state
may therefore reject a commit accepted by the earlier implementation. Leave room
for both versions and retained readers; limits and defaults are not raised.
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
   property names are canonical. The core enables serde_json `float_roundtrip` so
   finite Real values preserve their binary64 bits even in standalone core builds.
   Unknown top-level payload fields are rejected.

The golden vector is `tests/fixtures/incremental_checkpoint_v1.bin`. A format
change requires a new magic/version, migration/refusal note and updated vectors;
do not change a golden merely to accept different output. Other format versions,
length/checksum errors, duplicate IDs and invalid endpoints are rejected.

`checkpoint` captures one read view, serializes to a bounded buffer, writes a
unique same-directory temporary file, syncs it, renames it and syncs the parent
directory. An error after rename can leave a complete new file visible. This is
not a promise of rollback after filesystem failure. Locked checkpoint readers
block publication for their lifetime; COW readers can retain the captured version.

Checkpoint serialization captures one coherent graph version, then releases the
read view before disk I/O. It writes and syncs a private temporary file first.
Publication uses an independently opened, advisory-locked empty
`.velagraph-checkpoint.lock` in the canonical parent directory. Under that lock
it validates an existing target's bounded framing/checksum and token envelope,
then conditionally renames and syncs the directory. The directory-level lock also
covers path aliases and case-insensitive target names; different directories are
independent. Encoding and temporary-file writes can run concurrently.

An older sequence, different identity, or equal sequence with different payload
bytes returns `TransactionConflict` without replacing the target. Equal-version,
identical bytes are permitted and still complete file and directory sync, including
retries after a prior post-rename error. Existing framing/checksum/token-envelope
corruption and non-regular targets fail closed; they are not treated as missing.
The token-envelope check is not a full
record/index reconstruction; restore performs that validation separately.

All cooperating threads/processes must use this protocol. The lock sidecar stays
on disk and must not be removed/replaced while writers run; locks release when
the handle/process closes. Checkpoint target names with case-insensitive prefixes
`.velagraph-checkpoint.lock` or `.incremental-` are reserved for protocol files.
An occupied nonempty lockfile fails without truncation. Cleanup only removes a
temporary file successfully created by that call. These stricter overwrite and
reserved-name rules do not change checkpoint payload bytes or the format version.
Old writers that ignore the lock and hostile filesystem replacement are outside
this coordination guarantee. Checkpoint calls have no preemptible disk-lock/I/O
deadline. Publication of different files in one directory is serialized.

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

### Local COW publication comparison

`cargo run --release --locked -p velagraph --example incremental_publication`
uses 65,536 generated vertices, four readers and 120 batches of 32 vertex updates
scheduled 1 ms apart. Reader warmup finishes before the barrier. Active samples
start within the writer start/end window, including paced gaps; idle acquisitions
are reported separately. Each read verifies 32 properties against the version's
opaque metadata. These are direct record reads, not Gremlin or RPC timings.

Three alternating-order comparisons against `aa39ed2`, using identical source and
dependency versions, ran on macOS arm64 Mac14,10 (12 logical CPUs, 32 GiB), Rust
1.100.0 release. The candidate measured here was `28968a5`, before the subsequent
checkpoint/admission correctness fixes. These are historical measurements of the
short-publication change. The table gives the median of three per-run quantiles:

| Metric | `aa39ed2` COW | Short-publication COW |
|---|---:|---:|
| Active version acquisition p95 | 358.500 us | 0.334 us |
| Active version acquisition p99 | 1,406.583 us | 0.500 us |
| Active 32-property read including acquisition p99 | 1,411.666 us | 25.458 us |
| Update p50 | 229.541 us | 253.708 us |
| Update p95 | 1,495.042 us | 1,321.375 us |

Acquisition p95 ranged from 262.042 to 417.667 us before and was 0.334 us in all
three candidate runs. Active sample counts differ because blocked readers finish
fewer paced iterations within the writer window. Scheduler, allocator and final
reader reclamation still affect latency. This is contention evidence, not a
throughput ceiling or a guarantee of faster uncontended queries or updates.

## Delivery

Tracking: [#370](https://github.com/ashione/VelaGraph/issues/370), [PR #371](https://github.com/ashione/VelaGraph/pull/371), v0.0.3. This is
not completion of the broader managed recovery or CDC milestones.
