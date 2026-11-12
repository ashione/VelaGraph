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
8. [`docs/D2_CONSENSUS.md`](docs/D2_CONSENSUS.md) — D2 Raft library decision and
   prototype slice.

## Non-negotiables

- Graph queries are standard Gremlin text parsed from the official TinkerPop
  grammar. Do not invent a dialect, alias, or hand-written parser.
- Unsupported Gremlin must fail explicitly; never silently ignore, rewrite, or
  return a partial success.
- Keep the K1 key layout and deterministic record encoding; format changes need
  a version, migration note, and golden vectors.
- V1: one `WriteBatch` per vertex/edge mutation; both endpoints must exist; v0 has
  no cross-partition edges. The separately reviewed append-only v2 experiment
  follows `docs/EXPERIMENTAL_CROSS_EDGES.md`; never apply its non-atomic semantics
  to v1 APIs or claim its observed adjacency is complete.
- Every traversal is bounded. Internal limits are errors, not `truncated`
  successes.
- The legacy SQL engine is removed. Keep graph property types and errors
  independent of SQL parsing or table schemas.
- Add tests for behavior, errors, limits, determinism, and reopen. Run:
  `cargo fmt --all --check`, `cargo test --locked`,
  `cargo clippy --all-targets --locked -- -D warnings`, `git diff --check`.
- Self-review the full diff before every commit. Do not approve or merge your
  own PR. Maintainers control integration; a separately reviewing maintainer or
  explicitly owner-authorized automation may merge after independent review and
  applicable checks. These instructions do not themselves grant merge permission.

## Scope

The repository includes the local graph kernel, Raft-backed storage nodes,
static routing, observed cross-shard queries and restricted managed-cohort
transactions/reads. Consult `docs/CURRENT_STATUS.md`, the roadmap and the exact
entrypoint contract rather than treating the original K2/K3/D1 milestone list as
the current implementation inventory.

Keep each change scoped to its issue and acceptance criteria. A query-layer
change must not silently alter consensus, cross-partition atomicity, storage
formats or language support. Observed v2, managed fixed-version and local v1
contracts remain distinct; progress in one is not completion of general D4/D5.
