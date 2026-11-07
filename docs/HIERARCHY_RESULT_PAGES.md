# Ordered hierarchy vertex result pages (experimental)

The explicit hierarchy job API adds actual vertex-result pages for large, finite
organization traversals. Count jobs remain unchanged. This API does not stream
internal work pages or silently enlarge synchronous Gremlin query limits.

## Query and API

`StartRows { start_partition, text, page_size }` accepts page sizes 1..=1000 and
only explicit `g.V(ids).repeat(single in/out).times(1..32)[.emit()][.dedup()]`.
The existing two post-emit modulator orders and optional edge label are admitted
through the official Gremlin AST. Bare traversal returns qualified vertex
identities, not properties or complete vertex records. The optional terminal
[valueMap extension](HIERARCHY_PROPERTY_PAGES.md) has a separate explicit
projection-stage budget and returns ProjectedPage. Count, path, limit,
filters, extra hops, global starts and arbitrary sorting are unsupported here.

Start returns `Preparing { token, next_sequence: 0, ready: false, ... }`.
`Advance { token, sequence }` performs bounded server-side work and returns only
Preparing progress. No row is visible until the entire finite traversal has
succeeded and `ready` is true. Advance retains the existing latest-sequence-only
replay rule and remove-before-await failure boundary.

After Ready, `Read { token, cursor: 0 }` returns
`Page { token, cursor, rows, next_cursor, done, completeness, snapshots }`.
Follow `next_cursor` until `done: true`. An empty result has one empty final page
at cursor zero. This is a real result page: `rows` contains ordered qualified
vertex identities, not a count or frontier-progress buffer.

The cursor is an explicit row offset within the token's immutable result, not an
opaque signed cursor. Nonterminal offsets must be page-size aligned and less
than result length. Any valid earlier offset can be read again or accessed out
of order with identical content; there is no advancing read cursor to corrupt.
The page size is fixed at Start. Cancel invalidates all offsets. Tokens remain
private temporary bearer capabilities, as for existing jobs.

## Ordering and duplicates

Rows follow breadth-first depth order for emit, then prior traverser order,
then canonical outgoing edge-ID order or incoming `(edge ID, source partition)`
order. Explicit starts retain their argument order, including duplicates.
Exact-depth traversal returns only its requested final frontier. Missing starts
and dead-end paths contribute no rows. The initial roots are not post-emitted.

Every duplicate root, parallel edge, converging path and bounded cycle retains
its own traverser occurrence. The count executor's weighted BTreeMap is not
used for ordered rows. Optional terminal dedup keeps the first qualified vertex
occurrence in the result stream; it never suppresses subsequent traversal.
Different partitions with the same local ID remain distinct. Roots, direction,
label, depth, emit/exact mode and page size are selectable; arbitrary `order()`
or descending sort is not implemented.

## Snapshots, failure and delivery

All work uses the original pinned shard-session vector. Every observed edge,
including filtered-out labels, retains canonical, endpoint and reverse-index
validation. No session is reopened or refreshed. Completion remains Unverified:
this is observed adjacency, not global completeness or a globally atomic view.

Only after complete evaluation are data sessions closed and immutable rows made
readable. Every page retains the original snapshot stamps and original fixed
120-second job expiry. Ready or Read does not renew that expiry. Corruption,
work/byte exhaustion, interrupted Advance or a failed session before Ready
invalidates the evaluation and exposes no row pages. Cancellation cannot stop
blocking work already begun, but cannot leave resumable partial state.

A delivery failure, expiry or cancellation after Ready can interrupt delivery of
an already evaluated result. Only receipt of the last page establishes complete
delivery. There is no restart persistence, token recovery or automatic retry of
an ambiguous Advance. Re-reading an immutable page does no graph work.

## Bounds and accounting

Result mode is capped at 250,000 materialized rows and 750,000 simultaneously
owned frontier, next-frontier, result and dedup-set entries combined. A frontier
alone has no separate 250,000 cap; it is governed by the combined bound. These
are explicit limits of the new result mode, not the count executor's billion-path
capacity. Internal caps return errors, never truncated successful results.

The original 1 GiB cumulative logical-byte budget, eight million work units,
16-parent micro-steps, 16,384 observation-page bound, 1,000-neighbor limit and
per-request deadline remain. Ordered queue/result/membership insertion and
scratch allocations are charged before allocation. Each result job reserves
4096 + 512 × page_size bytes once for bounded page-delivery workspace; maximum
516,096 bytes. Serialized maximum-width vertex identities and two typed/encoded
buffer copies are covered by an executable proof. Replaying immutable pages does
not reset or add graph-work charges. This accounting is not an RSS guarantee;
concurrent response copies remain bounded by the existing 16 coordinator handlers.

Result jobs retain one of the same two process-wide job permits even after Ready,
until cancellation or expiry. They do not gain a separate admission pool or use
the 128 small completed-count slots to retain unbounded large results. There is
no disk spool, new storage format or changed count-query budget.

## CLI

- `hierarchy-rows-start --partition N --query GREMLIN --page-size N`
- `hierarchy-advance --token HEX32 --sequence N` until Ready
- `hierarchy-read --token HEX32 --cursor N` until done
- `hierarchy-cancel --token HEX32` releases retained results

All commands also require the existing endpoint and explicit trusted-network
flag. They send one request each; they do not execute graph traversals locally.
This development wire extension requires matching coordinator/client versions;
existing count operations and node transport are unchanged.

## Full CoreHR validation

A release run on one cloud machine with three real data-node processes and a
coordinator verified all fixture KV records for 200,000 people / 5,000 departments,
then ran eleven result cases against an independent BFS over input relationships.
The complete CEO result delivered and checked every one of 199,999 qualified
identities across 200 actual pages. Preparation took 6.045 seconds and total
preparation/delivery/replay checking took 6.360 seconds in this run. The dedup CEO
case also delivered all 199,999 identities. These are single-run observations,
not a latency SLA or a like-for-like comparison with count-only queries.

The matrix also covers duplicate manager roots, terminal dedup, exact-depth and
empty results, page size one, ordered roots, department descendants, and a full
duplicate-CEO result that must be rejected before Ready because its 399,998 rows
exceed the explicit output cap. Concatenated ordered identities, immutable page
replay, fixed snapshot stamps and cancellation are checked, not only counts.
The full-scale CI runs this same runner and fail-closed evidence validator for
smoke and full fixtures alongside all existing representative queries/counts.

[Machine-readable evidence](benchmarks/corehr-result-pages.json) records the
exact source/binary/fixture hashes and all case observations. This is synthetic
single-host validation, not multi-host throughput or fault-recovery evidence.
