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
explicit V(ids) start, up to three out steps (the official parser hop limit),
ordered has/hasLabel/limit, terminal path/count and values followed by
limit/count. It uses this official parser and strict value semantics, but an
independently documented static-catalog scope and conservative cumulative
resource budget. It is always Unverified and does not expand v1 completeness or
cross-partition guarantees.