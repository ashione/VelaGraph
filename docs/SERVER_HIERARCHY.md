# Native bounded hierarchy traversal

The experimental server coordinator accepts one standard Gremlin `repeat`
whose body is exactly one vertex `out()` or `in()` step, optionally filtered by
one edge label. The official parser's AST is used directly; there is no custom
syntax, query-text rewriting, client application loop, or unbounded recursion.

Supported examples:

- `g.V(200000).repeat(__.out('reports_to')).times(3).path()`
- `g.V(200000).repeat(__.out('reports_to')).emit().times(32).path()`
- `g.V(205000).repeat(__.out('parent_department')).times(8).emit()`
- `g.V(200200).repeat(__.in('parent_department')).emit().times(3).count()`
- `g.V(20).repeat(__.in('reports_to')).emit().times(2).dedup().count()`

The official anonymous-traversal forms `__.out(...)` and `out(...)` (and the
corresponding `in` forms) are accepted. Both post-repeat modulator orders
`repeat(...).emit().times(n)` and `repeat(...).times(n).emit()` have the same
bounded post-emit meaning. Only no-argument emit is supported.

This is a deliberate first hierarchy slice. `until`, pre-repeat emit, nested or
multiple repeats, predicates, arbitrary repeat bodies, mutation, labelled loops,
`by`, global starts and edge starts remain explicitly unsupported. The old v0
parser/executor and client-side observed executor do not gain repeat execution.

## Depth, paths, cycles and ordering

- `times(n)` is mandatory and accepts integer depths **1 through 32**. Depth zero
  is explicitly unsupported in this slice; use the original `g.V(ids)` for the
  zero-hop starting vertices. Values above 32 fail resource admission before
  acquiring any session. This does not claim alternative semantics for the full
  TinkerPop `times(0)` form.
- The maximum expanded depth, including ordinary steps before/after repeat, is
  32. Ordinary unrolled adjacency still retains its separate parser limit of
  three. No query can use multiple repeats to evade the depth bound.
- Without emit, the repeat result is exactly the frontier after n edges. Paths
  that end earlier contribute no row. If all paths end, execution can stop early
  and returns an empty frontier.
- Post-emit produces the frontiers at depths 1 through n. It does not emit the
  initial vertices. Each terminal-depth row is included once, not emitted twice.
  Earlier emitted rows remain even when their branches later end.
- Order is breadth-first by depth for emit, then prior frontier/input order,
  then the ordinary edge ordering within each expansion. Incoming adjacency
  uses `(edge ID, source partition)` order. Exact-depth mode returns the final
  frontier in that same deterministic expansion order.
- The traversal is an ordered **bag of paths**. Parallel edges, duplicate starts,
  converging branches, self-loops and cycles are retained. There is no implicit
  visited-node set, acyclicity assertion or simple-path filter. A cycle can
  produce repeated vertices up to the requested finite depth.
- A later `dedup()` reduces partition-qualified vertex identity while preserving
  the first emitted traverser's path. Later vertex steps can expand duplicates
  again. Filters, limit, projections, path and count outside repeat retain their
  existing supported semantics. Emit can produce variable-length paths, each
  validated against the exact incoming/outgoing direction sequence.

`emit().times(32)` means at most 32 requested levels, not an unlimited ancestor
closure. It is exhaustive for a known hierarchy only when its depth is within
that bound and all required observations are present. Completeness remains
Unverified; no graph-wide absence proof is added.

## Native execution and failure boundary

The coordinator performs the rounds itself using existing Out/In fragments on
exactly the same retained data-shard sessions. A path that revisits a shard uses
its original snapshot, never a fresh read or new lease. There is no client-side
per-hop orchestration, session refresh, retry, or fallback snapshot download.
All acquired views still form an independently pinned vector, not one globally
atomic snapshot. Canonical edges, endpoints and reverse indexes retain the
existing corruption validation, including observations filtered out by label.

The existing explicit-ID budget remains **64 MiB cumulative logical bytes,
10,000 work units, 10,000 intermediate/output rows**, the existing per-adjacency
cap and maximum **5-second** request deadline. There is no new larger hierarchy
budget. Every repeated expansion is charged normally, every loop round consumes
work, and retaining emitted paths alongside the next frontier is charged before
cloning/allocating. Work and bytes are never reset between rounds or refunded by
later limit/dedup/count. Broad descendant searches can therefore fail before
reaching n; a later `limit(1)` cannot turn that failure into a successful prefix.

Deadline, budget, corrupt observation, unavailable/expired session or epoch
failure aborts the whole result, including previously emitted rows. The default
100-row output cap applies after required execution, not as permission to stop
validating the repeat early. A user-requested finite times bound is traversal
semantics; exceeding an internal resource bound is always an error.

## Validation

Regression tests compare native exact-depth results with ordinary unrolled
traversals and independent CoreHR input-model loops. Real processes cover a
32-step self-loop, a parallel-edge cross-shard cycle, duplicate starts, emit
bags, qualified dedup, empty/dead-end paths, corruption and over-budget repeats.
The native request uses one original session per configured shard across all
rounds. Parser tests preserve official grammar admission and explicitly reject
unsupported/unbounded forms before execution. Full-scale CoreHR results are
reported separately for their exact tested candidate.
