# Experimental durable local temporal assertions (format 2)

Status: first **single-partition durable indexed-traversal slice** of #196. It is not a
production temporal query engine, managed/cohort V, complete Gremlin backend, distributed
transaction, global checkpoint, or migration of existing graph data. The parent
[bitemporal reference](BITEMPORAL_REFERENCE.md) is the semantic baseline.

## API and semantics

`temporal_store::TemporalStore::open` owns one dedicated RocksDB database. It
refuses any nonempty database without the exact format marker, including v1 graph
data. There is no import, upgrade, timestamp inference or access to v1/v2 IDs.
`TemporalMutation` contains a canonical Vertex, canonical Edge, or interval absence,
plus `[from,to)` in signed UTC epoch microseconds (`None` is unbounded future).
Properties and labels are versioned with their enclosing record.

`commit` atomically writes assertions and a consecutive **local** system-version
counter in one RocksDB WriteBatch with `sync=true`. Versions are generated under
one commit mutex, not supplied from wall clocks. Empty commits advance the local
prefix without assertions. All inputs validate before a write; invalid intervals,
overlapping assertions for the same identity in one batch, or resource overflow
write nothing and do not advance the counter. Vertex and edge identities form
separate namespaces. Logical identity can have new immutable revision IDs; this
is not reuse of v1/v2 permanent qualified IDs.

`view(Some(V), T)` pins one RocksDB snapshot, validates `V <= snapshot.head`, and
uses V/T throughout. V=0 is the empty initialized store. `view(None,T)` resolves
this local snapshot's current head once; it does not imply a distributed common
prefix or linearizable cluster latest. A version with no local change is readable.
Every read lazily seeks one identity's descending system-version index, then
selects the first revision containing T. A later correction masks only its valid
interval; old system views are unchanged. Absent revisions mask earlier assertions
only over the specified interval. No whole graph or full history is copied into
a query view, and commits do not copy the graph.

The view retains the snapshot head already validated at creation. This is
distinct from an explicitly selected older V: adjacency candidates are checked
against the snapshot head for corruption, then against V for eligibility.

This experimental namespace permits edges to be asserted before endpoints exist.
`get_edge` returns a visible edge only when both vertices are visible at the same
V/T. This explicitly limited R4 scope exception does **not** weaken v1 edge
creation/cascade rules. Correcting an endpoint or deleting a vertex can change
edge visibility without deleting its historical assertions. Write-time temporal
referential constraints over entire intervals are not provided.

## Versioned format and canonical payloads

All numeric fields below are big-endian. Bytes are deterministic and golden-tested.

```
graph/temporal/v2/format -> 02
graph/temporal/v2/head   -> u64 local committed version (initially 0)
graph/temporal/v2/revision/<kind:u8><id:u64><~V:u64><ordinal:u32> -> assertion
```

Kind is 1 for vertex, 2 for edge. Bitwise-complementing V makes versions sort
newest first per identity. Ordinal is zero-based within the atomic batch; same
identity intervals at one V cannot overlap, so ordinal does not resolve conflicts.
A key stores immutable revision identity separately from the logical identity.

The assertion envelope is exactly:

```
byte 0       : format version = 1
byte 1       : has-end flag = 0 or 1
bytes 2..10  : valid-from i64, two's complement
bytes 10..18 : valid-to i64 when flag=1; exactly zero when flag=0
byte 18      : has-payload flag = 0 or 1
bytes 19..   : existing graph/v1 canonical record bytes when flag=1
```

Absence flag 0 requires exactly 19 bytes. Payload flag 1 requires a nonempty
canonical record of the key's kind with the same ID. It reuses `graph::codec`
without redefining property types, protobuf fields, or their golden vectors.
Envelope flags, version, lengths, intervals, key length/version/ordinal and
selected payload identity are checked. An unknown envelope or malformed record
errors, not a false missing result. Envelope integrity is checked for each
visited revision; canonical payload decoding/validation occurs for the selected
record. RocksDB WAL/SST checksums provide physical corruption detection; no new
application checksum or cryptographic authentication is claimed.

## Bounds and error behavior

* At most 256 mutations and 16 MiB total encoded record/key bytes per batch.
* At most 4,096 visited revisions and 64 MiB raw record/key bytes per identity
  resolution. Crossing a bound errors, never returns an incomplete answer.
* At most three identity resolutions per direct edge lookup (edge + endpoints).
* Batch overlap validation is quadratic in at most 256 entries. Lookups scan
  one bounded identity chain, not every entity. Bounds include nonmatching
  intervals. No eviction or old-history deletion is performed to make room.

These are new local experimental work bounds, not relaxations of existing graph,
cohort or memory limits. Caller-provided input memory and RocksDB allocations are
not a total RSS guarantee. Temporal indexed adjacency uses the candidate contract below. The explicit-ID Gremlin
backend described below already uses cooperative deadline and memory accounting.

## Recovery, retention and migration

Each acknowledged commit requests a synced WAL batch. Reopen recovers the version
counter and all revisions together. Tests kill a child before commit and after
acknowledgment and verify complete absence/presence; they do not inject failure
inside RocksDB's WAL implementation or simulate hardware durability failures.
No application WAL, client retry journal, deduplication or automatic retry exists;
a storage error with uncertain outcome must not be interpreted as safe retry.

All history is retained indefinitely by this slice. Runtime views pin RocksDB;
this does not define a complete audit retention/GC policy. There is no purge,
install, restore API or #124 prerequisite workaround. The dedicated third database
is **not automatically included** in the existing two-DB Raft checkpoint or
object-store upload paths. Do not use those backups as temporal recovery evidence.
Future physical placement must integrate authoritative temporal data explicitly.

The format is opt-in and does not mutate existing v1/v2 records. Future incompatible
changes need another namespace/format version and explicit reviewed migration;
no old database can silently inherit an invented valid-time origin. The store is
not currently connected to Raft, managed sessions or remote storage.

## Validation and next step

Ordinary unit tests cover golden bytes, corrections/future effective values,
interval absence/endpoint visibility, fixed snapshots, failed batch atomicity,
adjacent intervals, empty commits, version errors, incompatible database refusal,
corruption and history work limits. A separate dense time-cell oracle validates
40 system versions x 16 valid instants **after close/reopen**. Process-death tests
run in `tests/temporal_recovery.rs`. No throughput/scale claim follows from them.

Next: global scans, then certified distributed V/T with shared session/page
identity. #196 remains open. CoreHR is one eventual acceptance
fixture, not a storage label or optimization rule.


## Official Gremlin point-query integration

### Query reuse and typed bindings

Related performance work: [#336](https://github.com/ashione/VelaGraph/issues/336).
Bound start IDs retain the text parser's nonnegative `i64` integer domain;
accepting `u64` Rust input does not admit IDs above `i64::MAX`.

`TemporalStoreView::execute_query(&GremlinQuery)` executes an already validated
query at the view's V/T. `GremlinQuery::prepare(GremlinParameters)` declares which
start IDs and existing `Has` literal-value steps are variable. The declaration
uses step indices, not literal matching: equal constants in undeclared positions
remain unchanged. No placeholder syntax or alternate grammar is introduced.

`PreparedGremlinQuery::bind(GremlinBindings)` checks declaration/count agreement,
preserves the V/E start kind, and requires each value to have the same `Value`
variant as the template literal. Reals must be finite. Empty variable starts
are rejected rather than becoming a global scan. Duplicate start IDs and their
order are retained. Keys, labels, limits and operators are not bindable in this
initial API.

Before execution, bound arguments pass the original text/structural limits using
a canonical standard-Gremlin representation. Caller values are bounded before
cloning/rendering. Rendering is for admission accounting only; the official
parser is not invoked again. This retains the conservative structural-mark
limit even for marks inside literal strings.

`execute_prepared` binds and calls the same checked executor as textual queries.
The global-start restriction, fresh deadline and fresh read/memory budgets are
preserved. A multi-root binding is one query and shares its cumulative budget;
it does not obtain one independent allowance per root. Budget errors return no
partial successful result. Subsequent executions create fresh budgets.

These APIs are additive and local. They do not add managed/distributed temporal
traversals, change stored records, or change the existing text-query contract.

### Parsed-plan reuse

`TemporalStoreView::execute_gremlin` caches successfully parsed `GremlinQuery`
plans by exact query text in the owning `TemporalStore`. Plans contain syntax
only: no read snapshot, V/T, result, deadline, or execution budget is retained.
The temporal global-start guard is checked on every call, including cache hits.
Every admitted execution creates a fresh backend and budget. Parse errors are
not cached.

The cache uses least-recently-used eviction, at most 1,024 entries and 1 MiB of
retained key text. Text above 4 KiB is parsed and executed normally without cache
admission. Parsed plans remain subject to the original parser's text and
structural limits; these key-text bounds are not a process RSS limit. The cache
is in-memory only and starts empty on reopen. It does not change the on-disk
format or the managed temporal slice.

Cache lookup/admission uses a short store-local mutex; parsing and execution are
outside it. Concurrent misses can parse the same text independently. A poisoned
cache is treated as unavailable and falls back to parsing; storage and execution
errors are never suppressed. Optional `query-profiling` counters distinguish
actual parse calls from cache hits.

`TemporalStoreView::execute_gremlin(text)` uses the existing official parser and
shared `GremlinQuery` executor through a lazy `QueryReadBackend`. Explicit
`g.V(ids...)` and `g.E(ids...)` support the existing local steps: has,
hasLabel, values, id, label, limit, terminal count and path. Duplicate inputs,
input order, missing-property behavior, strict values and existing default caps
are inherited unchanged. Indexed out/in/both accept vertex traversers;
`g.E(...).out()` remains a type error. `path()` reports the actual fixed-V/T traversal objects.

Global `V()`/`E()` starts reject **before any records are read**,
including when input IDs are absent or the graph is empty. Existing unsupported
language features still fail through the official parser. There is no custom
syntax, scan-to-copy fallback or per-query materialized graph. Valid time and
system version are selected by the view API, not embedded into Gremlin text.

Each query shares the existing logical memory budget and cooperative deadline.
Raw visited key/value bytes are charged while decoding, and canonical decoded
records use the existing accounted codecs before owned allocations. Cumulative
history work across all requested IDs and both edge endpoints is bounded to
4,096 visited revisions and adjacency candidates combined / 64 MiB, rather than resetting for each lookup. Limits,
corruption and deadline failures are errors, not partial results. RocksDB's
iterator allocation and allocator overhead are outside the logical memory
accounting, as in the existing kernel; no hard RSS bound is claimed.

Ordinary tests exercise reopen-to-Gremlin corrections/future views, fixed old
views after writes, edge endpoint absence, duplicate/order/path projection,
empty-input rejection, malformed syntax, memory/deadline/cumulative-work limits
and corruption before filters. Format 2 adds the candidate index below. Distributed session/page coordinates remain open.

### Query-local record reuse

Each Gremlin execution owns separate vertex and edge memos at its immutable
snapshot and V/T. They retain validated records and genuine absence, never
errors. Edge entries include successful endpoint validation. IDs, root order,
duplicate traversers and parallel paths are unchanged. Hits still check the
deadline; misses retain the cumulative actual revision/byte read limits.

Each memo admits at most 512 entries and 512 KiB of logical charges, for at most
1,024 entries / 1 MiB total. Every slot costs 128 bytes plus the full original
decoded-record charge. Returned copies are independently charged. A hit moves
the cached record to the caller and only best-effort retains a new copy.
The same query budget reclaims optional memo entries before rejecting required
allocations; no query retry, limit increase or partial success is introduced.
Reservations are released on eviction or execution drop. These are logical
accounting limits, not an allocator/RSS guarantee.
### Reusable version cursor

One execution lazily opens at most one snapshot raw iterator for vertex and edge
record misses. Each requested identity positions at its version bound and tests the
valid interval, with unchanged cumulative work limits and conservative raw-byte
accounting. Decode borrows the selected key/value; it does not first copy them
into owned boxes. Iterator errors propagate before absence can be returned.
The record cursor is separate from adjacency iterators to allow their visitors
to resolve edges and endpoints safely. Native iterator memory remains excluded
from logical accounting under the existing RocksDB memory boundary.

Adjacency scans also reuse a query-local raw cursor and borrow its key/value
slices. The cursor is taken out of its retained slot while a visitor runs; a
reentrant scan can use a separate temporary cursor and ordinary sequential scans
reuse the retained one. Every seek/next checks cursor status before absence is
returned. Candidate order, skipped-candidate work charges and raw-byte accounting
are unchanged, and callbacks can still return errors. Neither cursor nor record
memo is retained across executions. Snapshot HEAD is not re-read for each scan.

Start rows retain their original order, duplicates, missing-ID handling and
leading-limit short circuit. There is no eager decode of later roots, no new
query syntax, no multi-key I/O batching, and no asynchronous prefetch. Temporal
version selection already consumes the candidate value, so a subsequent MultiGet
would repeat reads rather than remove them.

An exhausted identity scan can already be positioned at the first key of the
next identity. A later lookup reuses this unconsumed boundary key only when it
matches the requested identity and is at or after that identity's selected-V
lower bound. Otherwise it seeks normally. This does not speculate forward or
skip revision validation: status, envelope, V/T, payload, endpoint and work-budget
checks still run. EOF and successful selected records do not create lookahead.
The marker belongs to the execution's record cursor and never crosses views.

Point reads construct the fixed-width revision key in a stack buffer and borrow
its identity prefix from that buffer. Owned keys required by writes or storage
adapter ranges use the same encoder. This removes temporary key allocations;
it does not change key bytes, seek order, visited revisions, decoding, visibility
checks, work charges or query limits. Golden vectors cover both record kinds
and integer boundaries; existing databases remain readable without migration.

`cargo run --locked --release -p velagraph --example temporal_record_seek_bench`
reproduces this workload class with generated edge intervals. It creates four
temporal edges per child, only the last visible at the fixed query time, reopens
the store, warms ten times and checks all 200 timed results. Parsing and setup are
outside timing. Use the same example source on compared revisions; the reported
single-process warm latencies are not a service-throughput or cold-cache claim.

### Reproducing query-reuse checks

Run the portable synthetic regressions, including opt-in reuse counters:

```sh
cargo test --locked -p velagraph --features query-profiling
cargo run --locked --release -p velagraph --example temporal_query_reuse_bench
```

The example creates 1,280 roots pointing to a shared vertex and compares explicit
parse-per-call, cached text and typed prepared execution. Each of three trials
rotates lane order, warms each lane once and measures three rounds, checking every
result. It reports per-trial latency for one repeated ID and 1,280 changing IDs.
Input formatting, graph construction, one-time preparation and result validation
are outside timing; the parse-per-call lane includes parsing. All lanes use the
same current record/cursor backend, so this isolates plan-reuse behavior rather
than measuring a before/after read-layer speedup. Use release builds without
`query-profiling` for timing. This is a warm, single-process synthetic comparison,
not an allocation/RSS, cold-cache or distributed-service throughput benchmark.

## Format 2 indexed adjacency and upgrade boundary

Format 2 uses a **new namespace and marker**. It refuses existing format-1
stores without modifying them; no automatic migration or incomplete backfill is
provided. Retain the old database and use the previous reader if needed. Create a
fresh experimental store and replay the original assertions to preserve their
local versions; never invent missing history or valid-time origins.

```
graph/temporal/v2/adjacency/<incoming:u8><vertex:u64><edge:u64> -> first-system-version:u64
```

Direction is 0 (out) or 1 (in). Each canonical edge assertion writes both
candidates in the **same synced WriteBatch** as its revision/head. A candidate's
first version is never replaced. Same-batch duplicates have the same first V.
Historical endpoint/label changes append candidates; deletion never removes
old candidates needed by old views. Candidates are sorted by edge ID. Every
candidate is charged, including future or invisible ones. Candidates newer than
the chosen V are skipped, but a first V beyond the pinned snapshot head is corrupt.
For eligible candidates the canonical edge and both endpoints are resolved at
the exact same V/T, then current direction/label is checked. Old endpoint and
label candidates cannot make stale relationships visible. The shared executor
retains its documented bag/order/self-loop/path behavior.

There is no full-edge scan or materialized graph fallback. Index growth depends
on distinct historical endpoint/edge pairs, not only current degree; all history
is retained. Query-wide 4,096 combined candidate/revision and 64 MiB work bounds,
existing hop/result/memory caps and deadline apply. This is bounded local
functionality, **not a large-scale or distributed performance claim**. Malformed
candidate bytes error. RocksDB checksum detection is not a proof against arbitrary
logical key deletion; this slice adds neither index repair nor tamper authentication.

Tests include a dense independent 49-version x 9-time x 6-vertex graph oracle
for all three directions, endpoint rewrites/deletion, labels, fixed snapshots,
paths, reopen, candidate golden bytes/corruption/budgets and process death around
commit. Existing two-DB checkpoints still do **not** include this third database.

### Local validation sample for #209

On the cloud x86_64 development container (9 reported logical CPUs), a debug
build with 33 vertices / 32 edges, one synced 65-record commit, and one parsed
`g.V(1).out().count()` executed 100 warmed times produced: commit 1.42 ms;
query p50 2.37 ms, p95 3.60 ms, max 8.98 ms. One warm-up was excluded; every
result was checked against 32. Command:
`cargo test --locked -p velagraph --lib temporal_adjacency_timing -- --ignored --nocapture`.
This is a reproducible smoke measurement, **not** a throughput, tail-latency,
release-build, history-heavy, distributed or comparative benchmark. Parsing is
excluded. The timing test is manually invoked and has no environment-dependent
pass threshold. Whole `velagraph` package validation: 247 passed, 0 failed,
4 ignored helpers/manual timing; all-target package clippy, fmt and diff checks
passed. Other workspace packages were not rerun for this isolated change.

## Managed state-machine vertex slice (#196)

`GraphSnapshot::prepare_temporal_vertices` now prepares bounded vertex assertions
for one or more members of an **existing** managed cohort. It returns the existing
`Plan`: Commit/Abort, original-ID retries, durable request admission and shard CAS
all use the existing protocol, not another temporal coordinator. The dedicated
`TemporalStore` above is neither opened nor imported by this path. Edge assertions
and cross-shard temporal traversal are rejected/not exposed in this slice.

`GraphSnapshot::temporal(T)` binds a business instant to that snapshot's certified
cohort V and original expiry. Point reads use the corresponding member's immutable
`ReadView`, never its current database. The mapping is:

`cohort V -> certified member local_sequence -> temporal head in those entries`.

The temporal head is a consecutive count of that member's committed temporal
batches, **not** the local sequence or cohort V. Untouched members and aborted
transactions do not advance it. A two-member correction remains invisible until
the existing cohort protocol can certify its complete prefix. Reads of an older
session remain at their original V/T while newer writes apply. Unknown members,
expired sessions, invalid intervals, overlapping assertions, unsupported edge
mutations and malformed temporal metadata return errors, not partial answers.
The new point adapter reuses format-2 revision keys/envelopes and graph codecs.
No new format, migration, Gremlin dialect or wall-clock-to-V mapping is introduced.

Limits: at most 256 assertions per transaction, the existing managed 16 MiB/operation
limits, and at most 4096 candidate revisions / 64 MiB per point read. Encoded batches
are checked incrementally against the aggregate limit. The original bounded
snapshot materialization and retained-history limits still apply; this is not an
indexed distributed-history performance claim. The immutable business-time view
borrows its parent session rather than adding another session registry or lease.

### Durability and recovery boundary

These managed assertions, format marker and local head are opaque bytes in the
**existing state-machine database**, committed atomically with the existing shard
CAS/transaction state. Durable-node reopen can therefore recover them with that
state machine; no third temporal DB is required for this placement. Original
request recovery and retained certified V still use existing checks.

This does **not** make the standalone `TemporalStore` part of the two-DB physical
checkpoint. There is no API accepting that independent DB as a managed member,
no import fallback, and no production temporal gateway wired to it. No global
checkpoint manifest, restore/rejoin READY gate, live installation, history GC or
purge-fence bypass is added. A locally reopened member is not proof of a completely
restored cohort. Global checkpoint recovery remains separately blocked until all
required members/coordinator, topology, cut and temporal data are validated before
query admission. Do not advertise this slice as production temporal recovery.

## Owned fixed-V read models

`TemporalStore::materialize` validates one source snapshot and returns an owned
`TemporalReadModel`. It compiles revision/absence selection once, retains all T
at the chosen local V, and uses typed covering adjacency rows with the existing
Gremlin executor. Source candidates must exactly match canonical positive history;
construction does not repair mismatches. See [the read-model contract](MATERIALIZED_TEMPORAL_READS.md)
for eager validation, construction/query limits, accounting, ownership and explicit
refresh. The standalone format and disk-query defaults above are unchanged.
