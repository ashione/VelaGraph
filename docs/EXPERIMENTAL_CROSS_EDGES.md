# Isolated graph/v2 experiment

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

## Identity and append-only creation

`VertexRef { partition: u32, id: u64 }` qualifies endpoints. `EdgeRef` has the
same fields, with partition always the source vertex's partition. Edge IDs are
unique per source partition. Same local IDs in different partitions are distinct.
Canonical edge data exists only at the source. A target index contains a source
qualified edge reference, not a second copy of the edge payload.

`AppendOnlyStore` provides low-level participant preparation/read primitives.
`ExperimentalReadSession::prepare_edge` is the full endpoint-checking operation:
it prepares both participants before any submission. Vertex and edge creation
reject existing keys; conditional shard-sequence checks fence concurrent writers.
The additive properties-only update API below is opt-in; there is no v2 delete
API. V1 APIs cannot see, modify or delete v2 keys,
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

## Additive edge property mutation

`ExperimentalReadSession::prepare_edge_properties(EdgeRef, &EdgePropertyPatch)`
returns a separate immutable `PreparedEdgeMutation`. Its patch has a sorted
`set` map and `remove` set. An empty patch or any overlapping key is Invalid.
Setting Null stores a present Null; removing an absent property is permitted.
Nonempty patches that leave the same values still submit a conditional write.
Property names and values use the existing graph validation/codec: finite reals
(including signed zero), exact i64 integers, text, booleans and Null. The final
encoded record must fit the existing 1 MiB cap. No new format or migration is
introduced. Identity, label, endpoints and both adjacency indexes are immutable.

Preparation first verifies the canonical record, source out index, target in
index and both endpoints in the original frozen session. A missing canonical
edge or incomplete visible edge is rejected with `Graph(Incomplete(edge))`.
It prepares exactly one conditional source-shard Put; there is no target write.
The low-level `AppendOnlyStore::prepare_source_properties` is only a participant
primitive: it verifies the exact expected canonical source encoding and the
source index/endpoint, not the remote reverse index/endpoint. Applications should use the session API.
The existing low-level append preparation and `PreparedAppend` contract remain
unchanged; create still rejects any existing canonical key.

`RouteResolver::experimental_commit_mutation(&prepared, timeout)` submits that
original source batch. The distinct `MutationAcknowledgement::AllBatchesAcknowledged`
means historical acknowledgement of this request, not proof that its values
remain current. `edge()`, `kind()` and `operation_id()` identify the mutation;
the operation ID is the single participant's `source_request_id()`.
The prepared object retains original bytes, sequence, epoch and placement.
A shard-sequence CAS fences races, including unrelated writes to that source
shard; conflicts are conservative and do not trigger automatic re-preparation.
The original append error Stage/Rejected/Unknown contract applies unchanged.
There is no reverse stage for a property mutation. Unknown may have committed:
retain and explicitly retry the same prepared object, never recompute it.

Dedup checks precede sequence CAS (but not epoch fencing). Retrying an old create
or mutation after a newer property update acknowledges history without restoring
old properties. Changed bytes or condition with the same request ID conflict.
No durable client journal, automatic recovery, rollback, repair or ID reuse is
added. Endpoint immutability under supported v2 APIs makes a source-only CAS
sufficient for this properties slice; raw D1/admin edits remain outside the
integrity guarantee, including target edits after preparation. Future endpoint
mutation/deletion requires a separately reviewed protocol.

Existing sessions and all observed traversal APIs retain their frozen views;
a fresh session observes new source properties. Snapshot vectors remain non-global,
and all successful observed results remain Unverified. This is an additive Rust
coordination API, not new Gremlin mutation syntax or a complete transaction API.
Tests cover exact values, Null/removal, bad/oversize patches, source invalidation,
CAS races, reopen, lost acknowledgements, changed retry identities and historical
retry after newer writes, plus normal-path independent process routing.

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
self-loop appears once in each direction. This primitive has no `both` variant;
the observed traversal below combines both directions with qualified-edge deduplication.
A visible corrupt/incomplete entry or any required read failure aborts the whole
call without returning a partial vector.

**An absent index entry cannot be discovered by this API.** Every successful
result is `ObservedAdjacency { edges, completeness: Unverified }`, including
empty results. In particular, a source-only failed append can be invisible to
incoming adjacency, while outgoing adjacency fails when it encounters the
missing reverse index. This is not an exhaustive adjacency API, even in a
successful transport call. No legacy Gremlin or complete-result API is wired to these observations.
The separate observed traversal below preserves their uncertainty. A future complete
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


## Isolated observed Gremlin traversal

`ExperimentalReadSession::observed_gremlin(start_partition, text)` is an additive,
explicitly experimental D5 functional slice. It parses standard Gremlin with the
existing official grammar, then accepts `g.V()` or explicit `g.V(id, ...)` and
`out`, `in`, `both`, `hasLabel`, `has(key)`, `has(key,value)`, `limit`, terminal
`path` or terminal `count`. Explicit start IDs belong to the supplied partition;
later hops follow qualified endpoints. Empty `V()` scans every partition in the
frozen catalog and ignores `start_partition`, even if that argument is unregistered.
An empty catalog yields an empty stream (or observed count zero). Global start
order is `(local ID, partition ID)`, matching the separate v1 global scan API.
There is no project-specific Gremlin syntax. All `E` starts, `values`, `id` and
`label` remain typed `ExperimentalRouteError::Unsupported` here.
Parser/validation failures retain the original typed error under `Query`.
No unsupported form falls back to v1 or another query API.

`ObservedTraversal` carries ordered rows, `completeness: Unverified`, the catalog
sequence and all per-shard stamps. Vertex rows contain `VertexRef`. Path rows
contain `ObservedPathElement::Vertex(VertexRef)` and `Edge(EdgeRef)`, with source
vertex followed by edge/vertex pairs. Parallel edges therefore retain distinct
paths even when endpoints coincide. Each input expands in `(edge ID, source
partition)` order; `both` deduplicates only the identical qualified edge, so a
self-loop appears once while parallel edges and same local IDs across partitions
remain distinct. Explicit starts preserve argument order and duplicates; missing
starts contribute no row. `hasLabel` matches any supplied vertex label.
`has(key)` requires a present property, including a present Null; equality uses
the same strict typed comparison as v0, without numeric/text coercion. A missing
key does not match Null. Filters reload properties from the original pinned view
and preserve input order, including duplicates and qualified traversal paths.

Limits act at their original step position, count counts the observed stream,
and terminal path does not materialize properties. A leading limit short-circuits
explicit start acquisition once that many existing vertices were admitted. For
global starts, a leading limit acquires at most N vertices per partition and
retains the globally smallest N candidates. All shards were already pinned by
session creation; a leading limit does not bypass unavailable shards. Acquisition
uses pages of at most 128 rows and 1 MiB + 64 raw key/value bytes. V2 records
remain bounded to 1 MiB. Pagination never falls back to an all-or-error scan.
Only an immediately leading limit can bound start acquisition; a later limit,
filter or count cannot hide an earlier resource error. Default output
cap is 100 unless an explicit limit occurs; count observes pre-default-cap rows.
The existing parser enforces three edge steps and limit 1..1000.
The shared `GremlinQuery::parse` entrypoint enforces the same 16 KiB and
64 raw structural-mark admission as all other Gremlin calls; there is no second
v2 parser or divergent complexity guard. The typed `GremlinComplexityLimit`
is preserved under `ExperimentalRouteError::Query`. See QUERY_SPEC.md for its
conservative quoted-string counting and platform verification limits.
Every successful result, including an empty stream or scalar count, is still
Unverified. Count is **not** an exhaustive graph cardinality. A visible missing
reverse index aborts the query, while an entirely absent incoming index may be
undetectable. This API must not be represented as a complete Gremlin query or
strongly consistent graph result.

One session holds one resolver permit, one frozen catalog and one pinned adapter
view per unique shard. All parsing, primitive reads, operators and final return
check the session's original absolute deadline; queries do not reset or extend it.
A retained session remains frozen after later writes. There is no global snapshot,
view refresh, replica switch, retry or per-hop deadline. Checks are cooperative,
not preemption of parser/decoder/allocator work or one storage dispatch.

Each execution additionally uses one cumulative 64 MiB logical admission budget,
never refunded, for encoded global-start pages, decoded start/filter vertices, observed edge payloads,
ordering entries, row/path construction and final result slots/stamps. Fixed
fees are 128 bytes per vertex/row/property/output slot, 256 per observed edge,
64 per stamp, 128 per global page row, actual encoded global-page bytes, actual
label/property text bytes, and actual path element sizes.
A query admits at most 10,000 rows per stage and 10,000 work units across all
stages: one per explicit start, decoded global-start candidate or filter lookup,
adjacency invocation, and returned edge. Global candidates from all partitions
share this 10,000-work-unit cap; leading-limit candidates discarded during global
merge still consume work/bytes. Thus a leading limit is not a guarantee that a
query fits the work budget (for example, 32 partitions each supplying 1,000
candidates exceed it). These are logical units, **not** the physical read count: each adjacency
invocation internally verifies canonical/index/endpoint records. Existing primitive
caps remain 1,000 entries / 1 MiB index bytes and 1 MiB decoded adjacency output.
Snapshot buffers and primitive read/decode scratch are excluded from the query
budget and retain their separately documented bounds. One bounded primitive
result or decoded start page can exist before cumulative admission, then is charged before retention
in query rows/maps. Caller text, parser memory, allocator spare capacity, teardown
and process RSS are not covered. There is no claim of precise live-heap accounting.
Internal row, byte or work exhaustion errors; a later limit/count cannot hide it.

Evidence: the independent metadata/two-data-process fixture exercises qualified
identity, directional hops, both ordering, parallel edges/self-loop paths,
global starts and strict property filters, counts, limit placement, absent starts/labels, unsupported forms,
byte exhaustion and bounded multi-hop expansion, frozen views, and explicit incomplete-result propagation.
Single-voter normal-path evidence does not establish failover, repair, distributed
snapshot isolation or production transport security.

Unit regressions additionally verify exact cumulative-byte/work-unit boundaries
and rejection of an already-expired retained session without resetting its deadline.

### Additive projection result API (version 2)

`observed_gremlin_v2(start_partition, text)` adds `values(key)` and `label()`
to the observed subset above. It returns `ObservedTraversalV2` with
`ObservedRowV2::{Vertex, Path, Count, Value { source, value }}`. `source` is the
qualified `VertexRef` supplying the scalar, including after a cross-shard hop.
This is a result-API version, not another storage encoding. The original
`observed_gremlin`, `ObservedTraversal` and `ObservedRow` are unchanged: their
`Eq` contracts and exhaustive enum variants remain available, and their entrypoint
still explicitly rejects scalar projections. Opt into the new method/types to
consume projections; no data migration is needed.

Values retain the existing strict `Value` types. Missing properties contribute
no row; a present `Null` contributes a Null row and counts as one. `label()`
returns the first **stored** vertex label (labels are ordered), or empty Text for
an unlabeled vertex, exactly as v1. Neither projection sorts or deduplicates;
parallel edges and repeated starts preserve duplicate scalar rows, even with
identical source/value. Different partitions retain distinct qualified sources.
Only `limit` and terminal `count` may follow a scalar projection. An invalid
following step fails with `Query(UnsupportedGremlinArgument)` even on empty
input. The official parser's arity, type and terminal-step rules still apply.
`E` starts and `id()` remain unsupported.

The new types derive `PartialEq`, deliberately not `Eq`: the public `Value`
contains `f64`, so caller-constructed NaN is not self-equal and signed zeros
compare equal. The graph's existing write validation and record decoder reject
NaN and infinities. Finite floats (including the sign bit of zero), integers,
booleans, text and Null are passed through without coercion or normalization.
No equality behavior in SQL, graph Value or the original observed API changes.

Projection happens in step order before later limits/counts, and all lookup,
value-payload and row charges share the same cumulative 10,000-work-unit,
64-MiB logical byte budget as start acquisition, filters and hops. A later limit
cannot erase earlier work. Default 100-row presentation applies only when no
explicit limit and no terminal count; internal caps still return errors. All
reads use the original session adapters/snapshot vector and absolute deadline.
Results, including projected counts and empty results, remain `Unverified`.
There is no completeness, failover, repair, TLS or global-snapshot claim.

## Additive edge observations (v3)

`observed_gremlin_v3(start_partition, text)` supports the v2 vertex subset and
adds `E()` / `E(ids)` followed by `has`, `hasLabel`, `values(key)`, `label`,
`limit`, `path`, and `count`. The additive official-AST adapter also supports the edge and endpoint
steps below. Edge streams reject vertex adjacency (`out`/`in`/`both` and
`outE`/`inE`/`bothE`); vertex streams reject endpoint steps, and `id` remains
unsupported. Validation precedes acquisition, even on empty input. Only limit/count may follow a scalar projection. The older
`observed_gremlin` and `observed_gremlin_v2` APIs still reject edge starts;
their public enums and Eq/PartialEq contracts are unchanged.

Explicit edge IDs belong to start_partition, retain argument order and
repetitions, and omit missing IDs. E() ignores start_partition and merges all
frozen catalog partitions in (local edge ID, source-owner partition) order.
`ObservedRowV3::Edge` holds an EdgeRef, scalar provenance uses
`ObservedElementRef::{Vertex, Edge}`, and an edge-start path has one qualified
Edge path element. Edge label is the canonical edge's label. Property matching
is strict; values drops missing properties and retains explicit Null.

Every acquired edge candidate is checked against canonical record, outgoing
and incoming indexes, and both endpoints on the original session snapshots.
An inconsistent candidate fails the entire query with typed Incomplete; a
filter cannot hide it. This is still **Unverified**, including empty/scalar/
count results: a missing canonical record with a residual reverse index cannot
be found by the canonical scan. The snapshot vector is not a global snapshot,
and this feature is not a completeness audit, transaction, repair, retry, or
replica-switch mechanism.

Pages are at most 128 records / 1 MiB + 64 bytes, checked for limits, key and
record identity, strict order, and continuation view/range/last-key binding.
Scans require a pinned node for stability; unsupported pagination has no scan
fallback. Each query has one cumulative 10,000-work-unit / stage-row cap and
64 MiB logical byte budget, with a default output cap of 100. These are not
RSS limits; bounded primitive scratch and snapshot storage are excluded.
Leading limit reads at most that many candidates per partition and keeps the
global prefix, without checking excluded candidates. A later limit/count does
not excuse earlier budget errors. Session construction still pins every shard,
and all operations retain its original absolute deadline.


### Edge adjacency and endpoints in v3

The same v3 entrypoint additionally supports `outE()`, `inE()`, `bothE()`
(with zero or one label), and `outV()`, `inV()`, `bothV()` (no arguments).
These are official Gremlin grammar steps, accepted by the additive
`ObservedGremlinQuery` AST adapter. `GremlinQuery::parse` and all legacy
execution entrypoints still reject them with `UnsupportedGremlinStep`, even
on empty input. The existing public `GremlinStep` enum is unchanged; the new
`ObservedGremlinStep` enum wraps old steps and adds the six new forms.
No v1 query, storage encoding or SQL behavior changes.

`outE`/`inE`/`bothE` consume vertices and emit qualified edges. Each input
expands in `(edge ID, source partition)` order. `bothE` unions directions by
qualified edge identity, so a self-loop appears once, consistently with the
existing observed `both` contract. This explicit experimental deduplication
must not be mistaken for a claim of full TinkerPop traversal compatibility.
Parallel edges, duplicate input traversers and edges with the same local ID
in different owner partitions remain separate bag rows.

`outV` emits the source vertex, `inV` emits the target vertex, and `bothV`
emits source then target for each input edge. Endpoint rows are not sorted
or deduplicated: a self-loop produces two equal vertex rows with `bothV`,
matching its two endpoint positions. This differs intentionally from `bothE`'s
qualified-edge union. Every endpoint lookup revalidates the canonical edge,
both indexes and endpoints using the same session views. A missing or corrupt
visible component fails the entire execution, including before a following
filter or limit. An absent incoming index is still invisible to incoming
adjacency; adding steps does not certify completeness.

Paths append only the elements visited by each new step: for example
`V(7).outE().inV().path()` yields `[Vertex, Edge, Vertex]`, while
`E(9).bothV().path()` yields two `[Edge, Vertex]` paths. Existing
`out`/`in`/`both` still append both edge and vertex. Edge/vertex filters and
values/label projections work after either transition; provenance is the
qualified current element. Only limit/count can follow scalars. Invalid
stream transitions are typed `ExperimentalRouteError::Unsupported` even if
prior steps would produce an empty stream. Unsupported grammar/arity retains
the parser's typed error under `Query`.

Global V/E start order and leading-limit acquisition are unchanged. Operators
preserve input bag order and limits execute at their original position.
All stages share one cumulative byte/work budget and the original snapshot
vector and absolute deadline. Endpoint verification charges one work unit and
the decoded edge payload per input, plus each retained row/path. The three
adjacency-step limit counts out/in/both/outE/inE/bothE together; endpoint
steps are bounded by the shared structural admission and row/work limits.
No retry, view refresh, replica switch or deadline reset is introduced.
Every successful output, including empty streams, paths and scalar counts,
is still Unverified, never a global-completeness or global-snapshot claim.
