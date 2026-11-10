# Graph query specification v0

Status: normative contract for K2; the v0 subset is implemented. This document
constrains the public query surface; it is intentionally narrower than Apache
TinkerPop Gremlin as a whole.

Version: `0`.

## 1. Normative language

The graph query language is **Gremlin**, as defined by Apache TinkerPop's
official [`Gremlin.g4`](https://github.com/apache/tinkerpop/blob/master/gremlin-language/src/main/antlr4/Gremlin.g4)
grammar.

- The parser MUST be generated from that grammar (currently `gremlin-rs`); a
  hand-written lexer, grammar, or syntax variant is not allowed.
- Queries are standard Gremlin text. There are no project-specific steps,
  aliases, sigils, or SQL-like `MATCH` syntax.
- A step that is not in the v0 subset below MUST fail with
  `UnsupportedGremlinStep`; it MUST NOT be silently ignored, rewritten, or
  approximated.
- The Rust API is an execution API behind the text surface, not a second public
  dialect.

The v0 subset is a deliberate first milestone, not a claim of full Gremlin
compatibility. Adding a step requires updating this document and its conformance
tests in the same change.

## 2. Accepted input

- Exactly one traversal starting with `g` is accepted per request.
- `;`-separated lists, mutations (`addV`, `addE`, `property`, `drop`), lambdas,
  bytecode, and server scripts are rejected.
- Query text MUST be valid UTF-8 and at most 16 KiB.
- Before invoking the recursive official parser, input MUST contain at most
  64 raw structural marks from `()[]{},.:;`. Marks inside quoted strings and
  comments count too. This conservative resource admission does not tokenize,
  validate, normalize, or rewrite Gremlin; the official grammar still decides
  syntax. Above the limit, return `GremlinComplexityLimit` before parsing.
  Counting raw marks avoids hiding recursion behind malformed quotes, escapes,
  comments, or lexer recovery. It covers argument lists, chains, and nesting,
  including unsupported constructs, at every `GremlinQuery::parse` entrypoint.
- Compatibility change: previously accepted long lists/chains or punctuation-heavy
  string values can now fail admission even below 16 KiB. For example `g.V(...)`
  accepts up to 62 simple IDs with no other steps; duplicate IDs still count and
  retain their order. Long ordinary Unicode/alphanumeric string values remain
  governed by the byte limit. This is not a per-step or token count.
- String literals use Gremlin quoting rules (single or double quotes); the
  parser decides validity.

## 3. v0 step subset

| Step | Form | Result |
|---|---|---|
| `V` | `g.V()` | all vertices, ascending id |
| `V` | `g.V(id, ...)` | vertices in argument order |
| `E` | `g.E()` | all edges, ascending id |
| `E` | `g.E(id, ...)` | edges in argument order |
| `hasLabel` | `.hasLabel(label, ...)` | filter by any listed label |
| `has` | `.has(key)` | filter to elements with the property |
| `has` | `.has(key, value)` | filter by strict equality |
| `out` | `.out()` / `.out(label)` | outgoing adjacent vertices |
| `in` | `.in()` / `.in(label)` | incoming adjacent vertices |
| `both` | `.both()` / `.both(label)` | both directions |
| `values` | `.values(key)` | property values; missing property yields no row |
| `id` | `.id()` | element ids |
| `label` | `.label()` | vertex labels / edge label |
| `limit` | `.limit(n)` | first `n` rows in deterministic order |
| `path` | `.path()` | traverser paths |
| `count` | `.count()` | number of rows |

Rules:

- `hasLabel` accepts one or more labels and matches if any label is present.
- `out`/`in`/`both` accept zero or one label in v0; more than one is
  `UnsupportedGremlinArgument`.
- `has(key, value)` supports only `Text`, `Integer`, `Real`, `Boolean`, and
  `Null` values. Predicates (`eq`, `within`, `gt`, …) are deferred.
- `limit(n)` requires `1 <= n <= 1000`; the default cap is 100.
- `path` and `count` are terminal in v0. `count` MUST be the last step.
- `out`/`in`/`both`/`values`/`id`/`label` are only valid on a traverser of the
  appropriate element type; applying a vertex step to an edge is
  `InvalidGremlinTraverser`, not an empty result.
- Mutations and `outE`/`inE`/`bothE` are deferred; they are not part of v0.

## 4. Semantics

- Steps execute left to right over the current traverser set.
- Start order is deterministic: `V()`/`E()` by ascending id; explicit ids keep
  argument order and duplicates.
- Edge expansion order is ascending edge id. A path may revisit a vertex; v0 has
  no `repeat`/`until`, so depth is statically bounded by the number of
  traversal steps.
- No step may expand beyond 3 edge steps. A query that would do so is rejected
  at planning time with `TraversalLimitExceeded`; it is never silently cut.
- `limit(n)` returns the first `n` rows after the preceding steps, in the
  deterministic order above.
- `count()` returns a single integer row.
- `path()` returns vertex/edge ids in traversal order; it does not materialize
  properties.
- `values(key)` emits one row per matching property value; absent properties
  emit nothing.
- `label()` emits the first stored vertex label (labels are ordered) or the
  edge label, as `Text`. `hasLabel` still matches any stored label.
- `id()` emits an `Integer` row; an id above `i64::MAX` is
  `UnsupportedGremlinValue` because v0 does not add a separate id row type.
- A missing vertex/edge id yields no row, not an error.

### Single-node read consistency

Each execution of a parsed query pins one RocksDB snapshot before its first
read. All start scans and point reads, adjacency entries, edge payloads and
target vertices use that same sequence through successful return. An atomic
concurrent commit is therefore observed entirely before or after the query's
snapshot, never as a mixture. Reusing a parsed query acquires a new snapshot
on each execution. Parsing precedes snapshot acquisition and the execution
deadline includes snapshot acquisition; parsing itself is outside that deadline.

This local path is not a Raft read or a transaction with a query-visible write
overlay. The same parser and operators also support the D1 path below.

### Single-shard D1 and replicated queries

`GraphRecordStore::execute_gremlin` and `GremlinQuery::execute_records` acquire
exactly one `GraphRecordReadView` after parsing. Every start scan, point lookup,
adjacency entry, edge and target vertex uses that view through the originating
storage adapter. With `ReplicatedStorageNode`, acquisition requires one leader
linearizability barrier. The query then reads the immutable D1 sequence, without
another barrier per point or page. This is a single-shard, in-process prototype,
not cross-shard snapshot isolation, routing, or a production query service.

A retained `GraphRecordReadView` supports `execute_gremlin`, or
`GremlinQuery::execute_record_view`, without refreshing the sequence. Already
acquired views can remain readable after loss of quorum or leadership; new reads
can fail with typed storage errors. Every actual point/page read still performs
adapter context and epoch fencing. A retained view cannot bypass a changed epoch
or enable synchronous calls from a Tokio worker. Use `replicated_graph::run_blocking`
for this adapter, including retained-view queries.

D1 queries require `scan_page`; unsupported pagination fails explicitly, without
falling back to live reads or an all-or-error scan. Start pages contain at most
256 rows; a leading `limit` reduces the last page's row bound. Adjacency is visited
in pages and retains only the query's bounded output; it is not constrained by
the separate 1,000-edge CRUD adjacency convenience API. Cross-label ordering,
parallel-edge multiplicity, self-loop deduplication and errors use the same query
operators as the local engine.

D1 views retain their existing 1,000,000-row / 64 MiB total acquisition bounds.
Query pages and individual graph records have an additional 16 MiB key-plus-value
budget (`GRAPH_QUERY_PAGE_BYTES`). A record exceeding that budget fails with
`StorageNode(ResourceExhausted)` for point reads or scans, never partial success.
A leading limit need not read or decode later records. This D1-only byte bound
does not change the existing local RocksDB query contract or graph encoding.
The 5-second cooperative execution deadline includes new-view acquisition; it
cannot preempt a single adapter dispatch, decode, allocation or destructor.
In addition, every execution shares a 64 MiB query-owned logical accounted-byte
budget across raw buffers, decode scratch, rows, paths, ordering maps, and result
construction. This closes repeated-large-property amplification that row and
page bounds alone cannot constrain. The exact fees, admission points, ownership
rules, and exclusions are specified in [QUERY_MEMORY.md](QUERY_MEMORY.md).
This is not a heap/RSS limit. In particular, retained views, adapter-private
allocation, allocator overhead, and parsing are excluded. Queries previously
accepted can now fail with `TraversalLimitExceeded`; no partial success is returned.

## 5. Value mapping

| Gremlin literal | Stored `Value` |
|---|---|
| string | `Text` |
| integer | `Integer` (must fit `i64`) |
| decimal | `Real` (finite only) |
| boolean | `Boolean` |
| null | `Null` |

Collections, maps, dates, UUIDs, and byte arrays are deferred and MUST fail with
`UnsupportedGremlinValue`. There is no implicit text/number coercion.

## 6. Results

A query returns ordered rows. A row is one of:

- `Vertex(u64)`
- `Edge(u64)`
- `Value(Value)`
- `Path(Vec<PathElement>)`
- `Count(u64)`

The exact Rust enum names may change, but the ordering and row cardinality above
are normative. `truncated` is not a success state for an internal cap: hitting a
resource limit is an error.

## 7. Errors

| Error | Meaning |
|---|---|
| `GremlinParse` | invalid Gremlin text or multiple queries |
| `GremlinComplexityLimit` | raw structural-mark budget exceeded before parsing |
| `UnsupportedGremlinStep` | valid Gremlin, step outside v0 |
| `UnsupportedGremlinArgument` | step in v0, argument form outside v0 |
| `UnsupportedGremlinValue` | literal type outside v0 |
| `InvalidGremlinLimit` | limit outside 1..=1000 |
| `TraversalLimitExceeded` | hop, row, accounted-byte, or deadline limit exceeded |
| `InvalidGremlinTraverser` | a step is applied to the wrong element type |
| `GraphNotFound` | reserved for strict lookup APIs; `V(id)`/`E(id)` remain empty |

Syntax errors and unsupported features are distinct. No error may be downgraded
to an empty result.

## 8. Resource limits

- Query text: 16 KiB.
- Parser admission: 64 raw structural marks (`()[]{},.:;`), including quoted
  and commented text. This bound is tested in isolated subprocesses on a 2 MiB
  worker-thread stack, including parser, validation and parse-tree destruction.
  It is not a guarantee for arbitrarily small caller stacks or a parsing deadline.
  Revalidate it when upgrading the pinned parser/runtime or supported platforms.
- Edge steps: 3.
- Rows per query: 10,000 before explicit `limit`.
- Default `limit`: 100; maximum explicit `limit`: 1000.
- Query-owned accounted bytes: 64 MiB per execution (see [accounting contract](QUERY_MEMORY.md)).
- Query deadline: 5 seconds (implementation may make this configurable).

Limits are enforced before materializing unbounded results.

## 9. Conformance tests

A change to the query layer MUST include:

- A parse test for every documented form and a rejection test for every
  unsupported form.
- An execution test for every v0 step, including empty and missing-property
  cases.
- Determinism tests with multiple vertices/edges and duplicate ids.
- A limit test proving `TraversalLimitExceeded` rather than truncation.
- A persistence test proving queries see reopened data.
- A test proving an unsupported step does not fall back to a scan or a dialect.

## 10. Non-goals for v0

- Full TinkerPop Gremlin, Gremlin Server, GraphSON, or bytecode.
- Cypher or any second query dialect.
- `repeat`/`until`, `match`, `where`, `select`, `as`, `union`, `group`,
  `order`, `aggregate`, `math`, lambdas.
- Mutations through the query language (use the graph API).
- Cross-partition traversals; that is D5.
- Query planning/cost optimization; v0 is a deterministic interpreter.

### Enforcement details

The row cap applies to every materialized intermediate traverser set, including
expansion after an earlier `limit`. A later `limit` or `count` does not exempt
an oversized preceding step. A leading `limit` can bound the initial scan.
Adjacency indexes are visited incrementally; retained expansion rows are capped
before insertion, then returned in ascending edge-id order per input traverser.

Execution checks the deadline during scans and operators and before returning
success, including a traversal with no chained steps. Checks are cooperative:
an individual RocksDB call, record decode, allocation or destruction cannot be
preempted. The execution deadline begins after parsing. The row cap bounds row
cardinality. The separate accounted-byte budget is a logical admission limit,
not a total heap or process RSS bound; see [QUERY_MEMORY.md](QUERY_MEMORY.md).

## 11. Additive distributed global-scan entrypoint

The static route resolver has a separate [global scan API](GLOBAL_SCAN.md) for
`V()`/`E()` followed by order-preserving `hasLabel`, `has`, `values`, `label`
and `limit` steps, with optional terminal `count()`. It uses this official
parser and the same element operators, but rejects explicit IDs, `id`, paths
and adjacency. Limits execute in their original positions; only an immediately
leading limit can bound candidate acquisition. Missing properties, strict
equality and invalid scalar traversers follow the single-partition rules.
Single-partition execution above is unchanged. Ordinary
rows carry partition IDs and sort by `(local ID, partition ID)`; count and limit
are global. The default output cap is 100, the total candidate cap is 10,000,
and one logical memory budget covers all partitions. A single metadata snapshot
and one view per shard yield a per-shard snapshot vector, not a globally
consistent snapshot. See the linked contract for fanout, snapshot admission,
end-to-end deadline, all-or-error semantics and memory exclusions.

## Isolated v2 append-only experiment

[EXPERIMENTAL_CROSS_EDGES.md](EXPERIMENTAL_CROSS_EDGES.md) documents a separate
versioned namespace and explicitly non-atomic source/reverse append protocol.
It does not change this document's existing v1/K3/Gremlin contract or complete
D4/D5. Its out/in observations cannot certify missing-index completeness.

The additive v2 `observed_gremlin` entrypoint reuses this parser but has a separate
Unverified result type. Its global/explicit-ID V starts, out/in/both, hasLabel,
has(key)/has(key,value), path, limit and count subset is specified in EXPERIMENTAL_CROSS_EDGES.md. Qualified path
vertices and edges preserve partition identity. This does not change v1 semantics
or claim complete D5 traversal; a scalar count remains an incomplete observation.

The additive `observed_gremlin_v2` result API also supports `values(key)` and
`label()` on vertices, followed only by limit/count. It retains qualified source
provenance on scalar rows and ordered bag multiplicity. Missing properties drop
rows; present Null remains; label is the first stored label or empty Text.
`ObservedRowV2`/`ObservedTraversalV2` use Value's PartialEq (NaN is not self-equal,
signed zeros compare equal); original observed Eq types/API remain unchanged.
Shared budgets, deadline, snapshots and Unverified status are specified in
EXPERIMENTAL_CROSS_EDGES.md; these are not complete D5 results.

The additive experimental `observed_gremlin_v3` also accepts bounded E starts
with has/hasLabel/values/label/limit/path/count and qualified edge provenance.
Old observed APIs still reject E. See [v3 edge observations](EXPERIMENTAL_CROSS_EDGES.md#additive-edge-observations-v3)
for ordering, strict validation, shared budgets and Unverified semantics; this
does not broaden v1 global traversal or establish a consistent global snapshot.

## Additive observed parser API

`ObservedGremlinQuery::parse` uses the same official parser, start parsing,
text/structural admission, literal validation, and terminal `path`/`count`
checks as `GremlinQuery::parse`. It additionally recognizes standard
`outE`/`inE`/`bothE` with zero or one validated label, and
`inV`/`outV`/`bothV` with zero arguments. Its `ObservedGremlinStep` wraps v0
steps as `Existing(GremlinStep)` so the legacy exhaustive enum is unchanged.
All six adjacency operations (`out`/`in`/`both` and `outE`/`inE`/`bothE`)
share the three-step cap. Endpoint steps are bounded by structural admission.

This additive AST is for the separately typed experimental observed v3 API
specified in [EXPERIMENTAL_CROSS_EDGES.md](EXPERIMENTAL_CROSS_EDGES.md), not
an expansion of v0 execution. `GremlinQuery::parse` continues to reject all
six new operations with `UnsupportedGremlinStep`; existing query surfaces
retain that parser. Parsing does not establish element types or observed
completeness; the experimental executor enforces its own narrower contract.

### Reviewed experimental edge deletion extension

The opt-in v2 edge mutation API additionally supports permanent tombstone deletion.
It is an explicit R2/R4/R12 scope exception, not a change to v1 single-batch,
failed-create, cascade, K3 or query contracts. Cross-shard source tombstone/out
removal and target in removal are staged, not atomic; same-shard deletion uses
one CAS batch. IDs are never reused. Reads validate visible residual indexes and
charge tombstone candidates before skipping them. Recovery and GC are unsupported.
See [deletion contract and versioned format](EXPERIMENTAL_CROSS_EDGES.md#additive-permanent-edge-deletion)
for upgrade restrictions, bounds and attempt-local retry outcomes.

## Experimental server-coordinated observed traversal

The separately typed [distributed query API](DISTRIBUTED_QUERY.md) accepts an
explicit V(ids) start, up to three combined out/in steps (the official parser hop limit),
ordered has/hasLabel/limit, terminal path/count and values followed by
limit/count. It uses this official parser and strict value semantics, but an
independently documented static-catalog scope and conservative cumulative
resource budget. It is always Unverified and does not expand v1 completeness or
cross-partition guarantees.
### Experimental server-coordinator vertex deduplication

The separately bounded [server-coordinated subset](DISTRIBUTED_QUERY.md) accepts
standard no-argument `dedup()` on vertex streams. It compares partition-qualified
identity, retains the first traverser/path in input order, and permits subsequent
vertex steps or terminal `count()`. Scalar/path/edge deduplication, scopes,
arguments and `by()` are explicitly unsupported. This additive observed AST step
does not enlarge the v0 subset table. The routing-v3 extension is described
below; older observed executors remain unchanged. All
upstream validation and cumulative resource limits still apply before dedup.

### Experimental server global V scan

The [server global vertex scan](SERVER_GLOBAL_SCAN.md) adds `g.V()` with
ordered has/hasLabel/limit and terminal count on the v2 vertex namespace. Its
stable order is (partition ID, local ID), distinct from the older client global
scan order. It uses data-local pages and fixed per-shard sessions, explicit
scan budgets and Unverified results, never a global atomic snapshot claim.
The bounded [global scan-to-traversal composition](GLOBAL_VERTEX_TRAVERSAL.md)
extends this with an ordered scan-filter/dedup/limit prefix then out/in, filters,
limit, dedup and terminal count/selected valueMap. It preserves separate scan
and traversal budgets on the same sessions. Joined results have no implicit
100-row truncation. Global E/path remain Unsupported. Terminal single-key values after at least one
out/in/repeat is supported: absent properties emit no row, present Null emits Null,
and ordered bag multiplicity plus qualified source identity are preserved. Limits
remain at their original vertex-stream position. Scan-only terminal values now
requires a preceding explicit vertex limit (see SERVER_GLOBAL_SCAN.md); no-limit
scan values and all scalar post-processing remain Unsupported. No-hop map/dedup follow the server global scan contract below. One bounded
repeat is now admitted after the global scan prefix with the same combined
32-depth check and cumulative traversal budget.

### Experimental bounded repeat AST

The additive observed parser recognizes one standard bounded
`repeat(body).times(n)`, `repeat(body).emit().times(n)`, or
`repeat(body).times(n).emit()` group and represents it as one
`ObservedGremlinStep::Repeat { incoming, label, times, emit }`. The body must be
exactly one `out()`/`in()` vertex adjacency with zero or one validated label.
The official anonymous-traversal forms `__.out('reports')` and `out('reports')`
(and incoming equivalents) are accepted without rewriting query text.

- `times` requires an integer literal in `1..=32`, using the existing supported
  integer literal mapping. Zero and negative values are
  `UnsupportedGremlinArgument`; more than 32 is `TraversalLimitExceeded`.
  Zero is explicitly unsupported, not an interpretation of `times(0)` semantics.
- `emit()` is unconditional and post-body only. The two supported post-body
  modulator orders have the same AST; neither emits the original start rows.
- Pre-emit/pre-times, conditional emit, until, nested or multiple repeats,
  named repeats, multi-step bodies, `both`, edge-producing bodies, labels
  (`as`) and `by` modulators remain unsupported. A missing or non-adjacent
  `times` is rejected rather than executed as an unbounded traversal.
- Ordinary adjacency outside the repeat retains the independent three-step
  parser bound. The server coordinator additionally validates the combined
  repeated and ordinary depth against 32 before acquiring sessions.
- The same 64-mark/16-KiB admission and terminal `path`/`count` rules apply.
  Stream-type validation remains an executor responsibility, including empty
  scalar/edge streams. The server accepts repeat for explicit V starts and global V scan prefixes
  under the bounded composition contract; hierarchy jobs still require explicit roots.

This is a server-coordinated experimental capability, not an extension of v0
or the client-side observed executor. The legacy `GremlinStep` enum and
`GremlinQuery::parse` remain unchanged and reject repeat. The client-side
`observed_gremlin_v3` validator explicitly rejects this additive AST step.
All executions retain their existing cumulative resource bounds and
Unverified completeness; this does not implement general Gremlin loops.

See [native execution semantics](SERVER_HIERARCHY.md) for ordered bags, cycles,
emission, budgets and fixed-session consistency.

## Explicit large hierarchy result jobs

The separate asynchronous job API admits the existing finite single-adjacency
repeat grammar without terminal count, optionally with terminal dedup, and
returns ordered qualified vertex identities in result pages only after full
validation. This does not change synchronous result limits or add Gremlin syntax.
See [ordered hierarchy result pages](HIERARCHY_RESULT_PAGES.md) for exact ordering,
materialization limits, fixed snapshots, preparation versus result delivery,
immutable offset cursors and failure semantics.

The explicit result-job slice additionally admits terminal standard
`valueMap('key',...)` with 1–8 distinct keys. It preserves qualified-vertex
provenance and standard singleton-list property-map semantics, including missing
versus Null. The local/v1 and legacy observed execution paths reject this step;
the distributed synchronous path is specified below. The job
mode has an explicit separate bounded projection phase; it does not enlarge existing count or ID-only
budgets. See [property pages](HIERARCHY_PROPERTY_PAGES.md).

### Distributed synchronous selected-property maps

The isolated server coordinator supports terminal
`valueMap('key', ...)` after its supported vertex traversals, filters, `dedup()`
and limits. This is not a new v1/local query feature. One to eight distinct,
nonempty UTF-8 keys of at most 64 bytes are required; `valueMap()` (all
properties), tokens, maps followed by another step and scalar-to-map remain
explicitly unsupported or invalid. Global maps after the supported scan-to-out/in
composition and no-hop global scans are allowed. Admission occurs before
reading even an empty stream.

Each result has qualified vertex provenance and a canonical property map whose
values are singleton lists. Missing properties are omitted, explicit Null is
`[Null]`, and a vertex with no requested properties yields an empty map. Vertex
stream order and duplicate multiplicity are preserved. Upstream traversal and
integrity checks run before projection; no property failure returns a partial
success. For explicit-ID starts the usual default 100-result cap applies before property
loading when there is no explicit limit. Joined global starts have no implicit cap. Selected maps are bounded to 2 KiB of tagged flat
property JSON per vertex, matching the hierarchy map cap.

This path uses the existing fixed per-shard sessions and unchanged synchronous
5-second, 64-MiB, 10,000-work and 10,000-row limits. It does not use hierarchy job
allowances, extend leases, retry RPCs or claim globally atomic snapshots. New
clients, coordinator and data nodes must be upgraded together for these queries:
`QueryRow::PropertyMap` is an additive wire variant and data nodes now accept the
existing `Project` leaf in ordinary sessions. Old query forms and hierarchy job
protocols are unchanged; older peers fail explicitly rather than dropping fields.

## Managed-session Gremlin subset

The experimental `managed_gateway` uses standard Gremlin text and the existing
`ObservedGremlinQuery::parse` official-grammar adapter, including its text,
structural-mark, literal and terminal-step validation. It does not add grammar,
aliases, steps or a typed external read language. Its private executor uses a
certified fixed-cohort GraphSnapshot at the already opened session version V.
This does not enable the older remote GlobalCommittedSnapshot query mode.

The request includes a partition solely to locate one explicit `g.V(id)` or
`g.E(id)` start under the existing partition-equals-shard mapping. E IDs are
canonical source-owned edge IDs in that partition. Exactly one ID is supported;
V()/E() scans, multiple IDs, and all other starts are Unsupported, not partial
results. Missing IDs yield no row. The full AST is checked before any read, so
empty starts cannot hide unsupported steps or invalid element/scalar transitions.

Supported composition:

- An optional first step `outE()` or `inE()` (zero or one label) requires a vertex
  start and returns canonical qualified edges, including cross-partition edges.
  No other adjacency or endpoint step is supported in this slice. Adjacency after
  a preceding limit or projection is explicitly unsupported/invalid, never moved.
- `values(key)`, `id()` and `label()` project vertex/edge elements to scalar rows.
  Only limit and terminal count may follow a projection. Missing properties emit
  no row; explicit Null remains. Vertex label is the first stored label, or empty
  Text; an edge label is its stored label. ID projection requires i64.
- `limit(n)` uses the existing 1..=1000 range and executes exactly where written.
  `count()` is terminal and counts the current stream, before the implicit default
  result cap. With no explicit limit, non-count results keep the first 100 rows
  after the supported steps, matching the existing default result-cap semantics.
- Adjacency order is ascending edge ID with canonical source partition as the
  deterministic tie-break for equal IDs. Single-ID starts have no reordering or
  implicit duplication. Limits and projections preserve stream order.

Results use the existing qualified QueryRow element/value/count shapes. They do
not return ad-hoc whole-record point payloads. Scalar provenance is retained.
QueryData completeness stays Unverified against arbitrary low-level writes that
might omit indexes, independently of its session's certified common version.
All canonical edges, both edge halves, endpoints and properties are checked at V;
no current or weaker read may fill a missing result.

Unsupported features include filters, out/in/both, bothE, endpoint steps, path,
dedup, repeat, valueMap, mutations and terminal iterators. Parse/Unsupported/
InvalidArgument/resource failures remain distinct using the existing query error
mapping. All failures are all-or-error with no partial rows. The former unpublished
typed read request fields are rejected by strict deserialization.

A complete observed adjacency is bounded by 1000 entries/1 MiB accounted payload;
a later filter-label/limit/count does not bypass that validation cap. Read captures
are bounded by the gateway's fixed snapshot budget, output logical rows by 1 MiB,
and the full encoded response including JSON escaping by 1 MiB. Parser/execution
and encoding/sending share at most five seconds, shortened by the connection
budget and session expiry. Expired or closed sessions fail even for an empty
query result. The wider managed gateway lifetime/recovery and memory exclusions
are specified in [MANAGED_GRAPH_GATEWAY.md](MANAGED_GRAPH_GATEWAY.md).

### Additive managed fixed-version Traverse and TraversePage

The separate `Traverse` and `TraversePage` operations now reuse the routing-v3
executor over the session's certified GraphSnapshot instead of extending the
small `Read` operation above. They accept V/E explicit starts or full-cohort
scans, out/in/both, outE/inE/bothE, inV/outV/bothV, has/hasLabel, values/label/id,
limit, terminal path/count. The official parser and whole-stream-type validation
run before even empty input. Repeated traversers, qualified edge/vertex paths,
parallel edges and deterministic `(ID, partition)` ordering preserve existing
v3 semantics. `id()` emits Integer(i64) with qualified vertex/edge provenance;
only limit/count may follow. IDs above i64::MAX fail with UnsupportedGremlinValue
(mapped to Unsupported by the managed gateway), without wrapping or partial rows.
A later limit/count cannot hide overflow; an earlier limit restricts the input
at its original position. Repeat/valueMap and other unsupported steps reject.
Standard no-argument dedup is supported on vertex and edge streams, preserving
the first qualified identity, path and order. Scalar/path dedup and modulators
remain unsupported, even on empty input. Every input is charged as work and every
retained identity is admitted before allocation under the existing cumulative
budgets; upstream errors cannot be hidden. See MANAGED_FIXED_TRAVERSALS.md for
the identity, limit-position and memory-accounting contract. Single-partition
V2Observed server queries share this extension and retain their original scope
fencing; V1 server queries are unchanged (see REMOTE_QUERY.md).

Every primitive uses the same V. Pagination fully evaluates and bounds the
result, then validates all page envelopes before emitting the first page. One
immutable result per session has exact query/page-size binding and replayable
random continuation tokens; continuation cannot refresh V or extend TTL. The
old `Read` subset, observed routing metadata and unrestricted remote
GlobalCommittedSnapshot Unsupported behavior are unchanged. See
[managed traversal and paging contract](MANAGED_FIXED_TRAVERSALS.md) for complete
ordering, limits, trust, retained-result accounting and compatibility details.

### Server global no-hop projection and identity dedup

The experimental server coordinator accepts `g.V()` with ordered has/hasLabel,
limit and vertex dedup steps, optionally followed by count or terminal selected-key
valueMap. Vertex identity includes partition; scanning emits each identity once,
so dedup is exact identity and never collapses equal IDs across partitions.
Filters and limits keep their positions. Maps preserve scan order and the original
retained shard sessions; missing properties are omitted and present values are
singleton lists. Empty/all-key maps, map post-processing and scalar dedup remain
unsupported. Existing scan output limits and separately accounted synchronous
projection budgets apply. See SERVER_GLOBAL_SCAN.md. This does not change the
Unverified consistency contract or enable hierarchy-job global starts.

Global scan prefixes may include vertex `dedup()` before the first out/in/repeat.
The scan already emits unique (partition,id) identities, so prefix dedup preserves
its ordered roots exactly and limits keep their positions. This does not dedup
subsequent traversers: parallel edges, convergence and repeat emission preserve
bag multiplicity unless a separate downstream dedup is explicitly present.
No extra scan, budget, refresh or default truncation is introduced.

Scan-only terminal `values(key)` requires an explicit preceding vertex `limit`
(1..1000); filter/dedup/limit positions remain unchanged. Missing properties emit
no row without refilling the vertex limit, present Null emits Null, and equal
values retain separate qualified source rows in scan order. No-limit values,
multi-key values and scalar post-processing are rejected before execution.
The existing fixed-session scan and traversal budgets apply independently.
