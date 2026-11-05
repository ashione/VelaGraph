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
| `UnsupportedGremlinStep` | valid Gremlin, step outside v0 |
| `UnsupportedGremlinArgument` | step in v0, argument form outside v0 |
| `UnsupportedGremlinValue` | literal type outside v0 |
| `InvalidGremlinLimit` | limit outside 1..=1000 |
| `TraversalLimitExceeded` | more than 3 edge steps or row cap exceeded |
| `InvalidGremlinTraverser` | a step is applied to the wrong element type |
| `GraphNotFound` | reserved for strict lookup APIs; `V(id)`/`E(id)` remain empty |

Syntax errors and unsupported features are distinct. No error may be downgraded
to an empty result.

## 8. Resource limits

- Query text: 16 KiB.
- Edge steps: 3.
- Rows per query: 10,000 before explicit `limit`.
- Default `limit`: 100; maximum explicit `limit`: 1000.
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
