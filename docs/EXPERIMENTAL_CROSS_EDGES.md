# Isolated append-only graph/v2 experiment

Status: functional experiment, **not D4 transactions or complete D5 traversal**.
The existing graph/v1 format, single-partition CRUD, K3 transactions and Gremlin
APIs are unchanged. No v1 vertex participates in this experiment. Applications
must explicitly create separate v2 vertices; this is not automatic migration.

## Reviewed scope exceptions

R2: a separately versioned, documented v2 namespace is added below, with golden
vectors and reopen tests. R4/AGENTS single-batch edge atomicity and failed-create
writes-nothing remain the v1 contract. V2 cross-shard append is an explicit
exception: one source batch and one target batch, without rollback. R12 and the
D1 boundary-edge non-goal gain only this experimental client-side coordinator;
the opaque StorageNode protocol and existing graph adapters are unchanged.
No production deployment, automatic repair, migration, TLS/authentication,
complete cross-shard Gremlin, or distributed snapshot guarantee is added.

## Identity and append-only mode

`VertexRef { partition: u32, id: u64 }` qualifies endpoints. `EdgeRef` has the
same fields, with partition always the source vertex's partition. Edge IDs are
unique per source partition. Same local IDs in different partitions are distinct.
Canonical edge data exists only at the source. A target index contains a source
qualified edge reference, not a second copy of the edge payload.

`AppendOnlyStore` provides low-level participant preparation/read primitives.
`ExperimentalReadSession::prepare_edge` is the full endpoint-checking operation:
it prepares both participants before any submission. Vertex and edge creation
reject existing keys; conditional shard-sequence checks fence concurrent writers.
There are no v2 update/delete APIs. V1 APIs cannot see, modify or delete v2 keys,
so a v1 cascade cannot break v2 endpoints. Direct opaque D1/admin operations
remain trusted escape hatches, as for v1. No integrity guarantee covers raw key
modification. V1 and v2 prepared values are different types and cannot be passed
to each other's commit API.

## Write outcome and exact retry

1. `experimental_read_session(timeout)` reads one metadata snapshot, resolves
   every registered partition and pins one data view per unique shard.
2. `prepare_vertex` or `prepare_edge` validates against those frozen views.
   Edge preparation checks both v2 endpoints before any write, and assigns
   independent immutable random request IDs to participant batches.
3. `experimental_commit(&prepared, timeout)` sends source canonical edge plus
   source out index as one conditional shard batch, then target in index as a
   second conditional shard batch. If both participants share one shard, their
   operations are combined into one batch validated at that same sequence.
   Combine rejects duplicate keys; it is not an overwrite escape hatch.
4. Only acknowledgement of every batch returns `AllBatchesAcknowledged`.
   This means **acknowledged writes**, not global atomic visibility, a transaction
   commit, or proof of future availability.

A definite source rejection is `StageOutcome::Rejected`. A definite target
rejection after the source acknowledgement is `Incomplete`; the source remains
stored. Timeout, unavailability or ambiguous storage failures are `Unknown`,
including the stage, its stable request ID and whether the source was
acknowledged in this call (`source_acknowledged_this_attempt`). These statuses
are attempt-local: a source rejection on retry says nothing about whether a
previous attempt persisted source or target data. An unknown result may have committed. No error claims
rollback. Non-stage preparation/admission errors submit nothing in that call,
but say nothing about earlier attempts with the same prepared value.

Keep `PreparedAppend` before submission. Exact retries preserve bytes, request
IDs, original sequence, epoch and placement. Only explicit `NotLeader` permits
bounded candidate selection after submission; unknown writes are never retried
automatically. Retrying replays the source exactly and then the target; source
dedup allows a previously acknowledged source to be recognized. An original
sequence conflict may remain permanently incomplete. There is no automatic
re-preparation, retry journal, repair or recovery coordinator. Losing the prepared
value can leave orphaned canonical/index data with no supported repair API.

Preparation/session and each explicit commit have their own bounded deadline;
all I/O phases *within* one call share its absolute deadline. Holding a prepared
value does not extend a read session. Commit uses the prepared frozen placements,
not newly registered catalog entries. Epoch fencing is still checked by storage.
There is no placement migration or ownership change support.

## Read contract: observed is not complete

Sessions freeze the entire catalog and one adapter/view per unique shard. Each
actual point/index read goes through that adapter's context/epoch checks. A
session never refreshes a view or switches replicas. Metadata sequence and
per-shard `(group, shard, epoch, sequence)` stamps are exposed. These are a
snapshot vector, **not a global snapshot**; sequences are not comparable clocks.

`get_edge(EdgeRef)` checks the canonical record, source out index, target in
index, and both endpoints. A visible missing or inconsistent component is a
typed `Incomplete` error. Absent canonical data returns None in that source
snapshot, not proof that an uncertain write never committed. Source/target
snapshots acquired at different times can produce `Incomplete` even after both
writes eventually succeed; open a new session to make a new observation.

`observed_adjacency(VertexRef, Out|In)` follows only the requested index. Each
observed entry resolves the canonical edge and verifies both sides. Order is
`(edge ID, source partition)`; parallel edges retain multiplicity, and a
self-loop appears once in each direction. There is no `both` API in this slice.
A visible corrupt/incomplete entry or any required read failure aborts the whole
call without returning a partial vector.

**An absent index entry cannot be discovered by this API.** Every successful
result is `ObservedAdjacency { edges, completeness: Unverified }`, including
empty results. In particular, a source-only failed append can be invisible to
incoming adjacency, while outgoing adjacency fails when it encounters the
missing reverse index. This is not an exhaustive adjacency API, even in a
successful transport call. No legacy Gremlin traversal, count, path, global
scan or complete-result API is wired to these observations. A future complete
query needs a visibility/coordinator protocol or bounded global integrity check;
removing the explicit uncertainty flag would require another contract review.

## Bounds

- One shared resolver admission slot per session/commit; existing 32-operation
  admission and dispatch limits apply.
- At most 32 catalog partitions / 8 unique data shards per session; at most
  200,000 retained rows and 16 MiB aggregate raw snapshot key/value bytes.
- Existing adapter snapshot/frame caps still apply. Snapshot admission occurs
  after acquisition, as in GLOBAL_SCAN.md; this is not a pre-allocation RSS cap.
- Timeout must be positive and no more than 60 seconds. All participant attempts
  in a call share one absolute deadline. CPU checks are cooperative.
- Individual v2 record envelopes are at most 1 MiB; encoding/decode uses the existing v1 codec. Caller input, cloning and
  pre-return encoding allocations are not charged by this persisted-record cap;
  it is not a pre-allocation heap bound.
- Adjacency scans allow at most 1,000 entries / 1 MiB key/value bytes. Response
  admission charges edge struct, label and property/name/value bytes plus fixed
  property fees, capped at 1 MiB. One bounded decoded edge can temporarily
  overlap already admitted results; this is a logical bound, not total heap/RSS.
- No unbounded scan, fanout, retry or recursive repair loops. A finite participant
  list and each placement's finite candidate list bound writes.

## On-disk graph/v2 format

All numeric key fields are big endian; `/` is a literal separator. Labels retain
v1's `[A-Za-z_][A-Za-z0-9_]{0,63}` rule. Indices have empty values.

```
graph/v2/p/<partition:u32>/vertex/<id:u64>
graph/v2/p/<source-partition:u32>/edge/<id:u64>
graph/v2/p/<source-partition:u32>/out/<source-id:u64>/<label>/<owner:u32><edge:u64>
graph/v2/p/<target-partition:u32>/in/<target-id:u64>/<label>/<owner:u32><edge:u64>
```

V2 record envelopes are canonical protobuf-compatible fields in this exact
order. Readers reject noncanonical/unknown envelope fields in this experiment.
Embedded v1 records retain their own existing validation/forward-compatibility.

```
uint32 version = 1; // 2
uint32 kind = 2; // 1 vertex, 2 edge
fixed32 partition = 3; // vertex's partition, or edge's TARGET partition
bytes record_v1 = 4; // unchanged complete canonical v1 vertex/edge Record
```

The source partition comes from the canonical edge key; nested edge `from/to`
are local IDs. Vertex envelope partition must equal its key. Properties retain
the deterministic sorted v1 encoding. This explicit nested record format does
not silently reinterpret v1 bytes and introduces no JSON encoding.

Golden: vertex partition 1, ID 7, no labels/properties:
`080210011d010000002208080110011a020807`.
Edge owner partition 1, ID 9, source ID 7, target partition 2 / ID 7, label K:
`080210021d02000000220f080110021a0908091007180722014b`.
Tests verify complete persisted keys and bytes, not just round trips.

Migration: none. V1 keys remain readable by existing APIs; no in-place conversion
is attempted. V2 callers explicitly create isolated data, and must retain these
versioned semantics on reopen. A future incompatible format needs another
version and migration plan.

## Evidence and limits

Storage tests cover golden bytes, clean reopen, exact dedup, duplicate/stale
creates, combined same-shard writes, partial source visibility, missing local
endpoints, v1 isolation/cascade, corrupt records/indexes and limits. Real TCP
integration starts independent metadata plus two data processes and exercises
cross-shard source/target identities, out/in observations, parallel edges,
same-shard partitions and self-loop, pinned views, stale target partial failure,
exact retry and unavailable-source/target outcome reporting. The unavailable
process test stops a participant before submission; it does not simulate a lost
ACK after remote commit. A local adapter wrapper separately applies a batch then
returns Timeout to check exact retry deduplication. Each group is a single
voter: this is functional routing/format evidence, not replication resilience,
crash recovery, failover, network partition correctness or production security.
