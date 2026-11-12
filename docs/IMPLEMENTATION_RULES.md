# Implementation rules

These rules apply to the graph kernel, query layer, storage-node boundary, and
their tests. `QUERY_SPEC.md` defines behavior; this file defines how changes must
be made. A PR that violates a rule needs an explicit, reviewed exception in the
same PR.

## R1 — Standard language, no dialect

- Gremlin text MUST be parsed by the official TinkerPop grammar. Do not add a
  hand-written grammar, lexer, parser, step alias, or project-specific syntax.
- Do not translate unsupported Gremlin into a different language or silently
  approximate it. Reject it with the documented error.
- The public surface is standard Gremlin; Rust builders are internal execution
  plumbing, not a second dialect.
- A new supported step requires a `QUERY_SPEC.md` update and conformance tests
  in the same change.

## R2 — Storage format is a contract

- Graph keys and record payloads follow `GRAPH_FORMAT.md`.
- Do not add ad-hoc key prefixes, JSON envelopes, or alternate encodings.
- Any key/payload change requires a new format version, a migration note, golden
  vectors, and a reopen test.
- Properties are canonical (sorted by name) and IDs are caller-supplied `u64`;
  do not introduce implicit ID generation or renumbering.

## R3 — Bounded work by default

- Every scan and traversal must have a row/byte/hop limit; no unbounded
  full-graph scan in a query path.
- Enforce `QUERY_SPEC.md` limits before materializing results. Internal caps are
  errors, never a silent `truncated` success.
- Prefer adjacency indexes over scanning all edges; a query plan must not
  degrade to a full scan when an index covers the step.
- New loops need a termination argument in the PR description.

## R4 — V1 atomicity and partition boundaries

- Use one `WriteBatch` for a vertex/edge plus its adjacency updates.
- Validate both endpoints before creating an edge; a failed create writes
  nothing.
- v0 rejects cross-partition edges. Do not fake them with duplicated records.
- Do not claim multi-statement or cross-partition transactions before K3/D4.
- Snapshot/read-view semantics are K3 work; the frozen boundary is
  [`TRANSACTIONS.md`](TRANSACTIONS.md). Until it is implemented, document the
  actual read behavior.

## R5 — Determinism

- Output order is by `u64` id unless the query specifies another order.
- Property maps are canonical; encoding is deterministic and round-trips.
- Tests must not depend on hash-map iteration, wall-clock time, or thread
  scheduling.
- Golden vectors are updated only with a format change, never to make a failing
  test pass.

## R6 — Errors are part of the API

- Use typed errors (`thiserror`) with stable variants; no string matching.
- Separate syntax errors, unsupported features, invalid arguments, and storage
  failures.
- Never swallow a storage/decode error, and never return partial success as
  success.
- Error messages may name the step/key but must not leak private data.

## R7 — Types are strict

- Stored values are `Null`, `Integer(i64)`, `Real(f64)`, `Text`, `Boolean`.
- Reject non-finite reals, out-of-range ids, invalid UTF-8 labels, and implicit
  numeric/text coercion.
- Query literals map exactly as `QUERY_SPEC.md` says; unsupported types are
  errors.

## R8 — Tests are the gate

Before requesting review, run and record:

```sh
cargo fmt --all --check
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
git diff --check
```

- Add unit tests for encoding/parsing and integration tests for user-visible
  behavior.
- Cover empty graphs, missing ids, duplicate ids, cycle-shaped inputs, limit
  boundaries, invalid/unimplemented steps, and reopen.
- No network-dependent tests in the default suite.

## R9 — Dependencies

- Prefer the standard library and existing dependencies.
- The Gremlin parser must come from the official-grammar crate; do not vendor a
  modified grammar.
- No build-time JVM, ANTLR generation, or `protoc` requirement. Generated code
  is checked in or supplied by the crate.
- New dependencies need a license check and a one-line rationale in the PR.

## R10 — Public API and compatibility

- Public types are documented and semver-relevant.
- Do not expose internal key layouts or RocksDB handles as public API.
- Breaking changes need a migration note; additive changes should be preferred.
- Keep shared graph types and storage independent of unrelated language
  frontends; the legacy SQL engine has been removed.

## R11 — Review and delivery

- Self-review the full diff before every commit; fix findings first.
- Keep changes scoped. Do not mix query-language work with unrelated features
  or distributed transactions.
- CI must be green; do not merge with a known failing check.
- The agent must not merge or approve its own PR; a person merges.

## R12 — Scope boundaries

- v0 query language is Gremlin only; Cypher/GQL is not a second implementation.
- v0 graph is a single partition; cross-partition traversal is D5, cross-shard
  transactions are D4.
- Storage-node interface changes follow `STORAGE_NODE_INTERFACE.md` once merged.
- The removed SQL surface is not a compatibility target; see
  [the migration note](SQL_REMOVAL.md).

## PR checklist

- [ ] Behavior matches `QUERY_SPEC.md` / `GRAPH_FORMAT.md` / D1 interface.
- [ ] No custom dialect, parser, or silent fallback.
- [ ] Limits and error paths tested.
- [ ] Deterministic ordering tested.
- [ ] `fmt`, `test`, `clippy`, `diff --check` pass.
- [ ] README/roadmap/spec updated when the contract changes.

## Reviewed v2 experiment exception

The isolated graph/v2 append-only API in [EXPERIMENTAL_CROSS_EDGES.md](EXPERIMENTAL_CROSS_EDGES.md)
is an explicit R2/R4/R12 scope exception: new versioned protobuf-compatible
envelopes and golden/reopen tests, two non-atomic cross-shard batches, and
observed-only adjacency with unverified completeness. V1 contracts are unchanged.
The separate observed Gremlin entrypoint is an additive R12/D5 experimental
scope exception, with qualified paths and explicitly Unverified results. No v1
Gremlin/query-layer behavior inherits this exception.

The additive properties-only v2 mutation is also covered by this explicit
experiment exception: preparation verifies a complete observed edge; one
conditional source-shard canonical Put changes properties only. It adds no
format, identity/label/endpoint mutation, target write, or distributed transaction.
V2 creation remains append-only and old exact retries retain historical dedup.

### Reviewed experimental edge deletion extension

The opt-in v2 edge mutation API additionally supports permanent tombstone deletion.
It is an explicit R2/R4/R12 scope exception, not a change to v1 single-batch,
failed-create, cascade, K3 or query contracts. Cross-shard source tombstone/out
removal and target in removal are staged, not atomic; same-shard deletion uses
one CAS batch. IDs are never reused. Reads validate visible residual indexes and
charge tombstone candidates before skipping them. Recovery and GC are unsupported.
See [deletion contract and versioned format](EXPERIMENTAL_CROSS_EDGES.md#additive-permanent-edge-deletion)
for upgrade restrictions, bounds and attempt-local retry outcomes.

The opt-in vertex properties API is covered by the v2 experiment exception:
one conditional canonical Put preserves qualified identity, labels and adjacency,
with bounded patch admission and exact historical retry. Vertex deletion is
explicitly Unsupported; absence of observed indexes is never a deletion proof.
No v1, Gremlin syntax, live record format or distributed transaction change.

### Experimental local temporal assertion scope

The opt-in [temporal store](TEMPORAL_STORE.md) is an explicit R2/R4 scope exception
for a new versioned dedicated namespace and interval-aware assertion semantics.
Canonical graph payloads and validation are reused. Edge assertions can precede
endpoints, but point reads expose an edge only when both endpoints exist at the
same V/T. No v1/v2 write, deletion, tombstone or migration behavior changes.
This exception provides no new Gremlin syntax, cross-partition transaction or
purge/install permission.
