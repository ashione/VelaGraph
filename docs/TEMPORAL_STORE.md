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
