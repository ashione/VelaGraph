# Repository rules for contributors and coding agents

Read these before changing behavior:

1. [`README.md`](README.md) — user-visible behavior and limitations.
2. [`docs/ROADMAP.md`](docs/ROADMAP.md) — kernel and distributed scope.
3. [`docs/GRAPH_FORMAT.md`](docs/GRAPH_FORMAT.md) — key layout and record encoding.
4. [`docs/QUERY_SPEC.md`](docs/QUERY_SPEC.md) — normative graph query behavior.
5. [`docs/TRANSACTIONS.md`](docs/TRANSACTIONS.md) — K3 single-partition
   transaction and recovery boundary.
6. [`docs/IMPLEMENTATION_RULES.md`](docs/IMPLEMENTATION_RULES.md) — mandatory
   implementation constraints.
7. [`docs/STORAGE_NODE_INTERFACE.md`](docs/STORAGE_NODE_INTERFACE.md) — D1
   storage-node boundary.

## Non-negotiables

- Graph queries are standard Gremlin text parsed from the official TinkerPop
  grammar. Do not invent a dialect, alias, or hand-written parser.
- Unsupported Gremlin must fail explicitly; never silently ignore, rewrite, or
  return a partial success.
- Keep the K1 key layout and deterministic record encoding; format changes need
  a version, migration note, and golden vectors.
- One `WriteBatch` per vertex/edge mutation; both endpoints must exist; v0 has
  no cross-partition edges.
- Every traversal is bounded. Internal limits are errors, not `truncated`
  successes.
- Preserve the SQL engine's documented behavior; do not couple SQL and graph
  internals.
- Add tests for behavior, errors, limits, determinism, and reopen. Run:
  `cargo fmt --all --check`, `cargo test --locked`,
  `cargo clippy --all-targets --locked -- -D warnings`, `git diff --check`.
- Self-review the full diff before every commit. Do not merge or approve your
  own PR; a person merges.

## Scope

The current milestones are K2 (standard Gremlin subset), K3 (single-shard
snapshot transactions), and D1 (storage-node interface). Do not implement
cross-partition transactions, Raft, or Cypher as part of a query-layer change.
