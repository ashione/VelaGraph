# Bounded global vertex scan into traversal

The experimental server coordinator accepts a global vertex-filter prefix and
then up to three ordinary out/in steps, without downloading shard snapshots or
re-parsing rewritten query text. The official Gremlin AST is split at its first
adjacency step. There is no schema-, label-, population- or answer-specific path.

## Supported pipeline

- Start: `g.V()` over the immutable startup catalog.
- Scan prefix: any supported `has`, `hasLabel` and `limit`, in their original order.
- Traversal suffix: `out`/`in` (up to three total), `has`, `hasLabel`, `limit`,
  vertex-identity `dedup`, and optionally terminal `count` or selected `valueMap`.
- A suffix ending in vertices returns qualified identities. `valueMap` keeps the
  existing 1–8 distinct keys, UTF-8 length and per-map byte bounds, missing/Null
  semantics, and singleton-list values.

Examples:

- `g.V().has('region','west').limit(8).out('member_of').dedup().count()`
- `g.V().has('name','Operations').in('member_of').has('active',true).limit(20).valueMap('name','country')`
- `g.V().limit(4).has('active',true).out().in().dedup()`

At least one out/in step is required for this additive surface. Global `E`,
`repeat`, `path`, `values`, `both`, no-hop `dedup`/`valueMap`, and other unsupported
steps fail explicitly. Existing scan-only and explicit-ID contracts stay intact.

## Order, multiplicity and completeness

The scan supplies seeds in `(partition, local vertex ID)` order. Limits in the
scan prefix remain before traversal; they are not pushed past filters or edges.
Every partition/page carries the remaining prefix limit. The complete admitted
seed stream is then passed to the existing bounded traversal in that order.
Parallel edges and repeated destinations preserve their multiplicity until an
explicit dedup. Dedup uses qualified identity and preserves first occurrence.
Limits in the suffix act at their exact original positions.

Neither the joined scan prefix nor its final result has an implicit 100-row cap.
A successful response is the complete requested bounded result, or the stream
restricted by explicit limits. Too much work, memory, output, adjacency or seed
materialization fails with a typed error, never a truncated success. The existing
scan-only default 100 and explicit-ID default 100 are not changed.

## Two cumulative budgets, one set of sessions

The coordinator and each data session keep separate scan and traversal counters:
scan is bounded to 256 MiB / 1,000,000 work units, traversal to 64 MiB / 10,000
work units. The counters never reset between pages, partitions, filters, edges,
dedup or projection; neither stage can borrow unused allowance from the other.
Scan session acquisition, pages and seed storage charge the scan allowance.
Materialized traversal seeds, expansion, filtering, dedup, property loading and
final encoding charge the traversal allowance. The seed/result materialization
cap remains 10,000, and the encoded response remains at most 2 MiB.

All unique shard views are pinned before scanning. Scan, traversal and property
reads use exactly those descriptors, the original epoch checks and the same
five-second deadline. There is no session reopen, retry or timeout extension.
The result is still `Unverified` with a per-shard snapshot vector, not a globally
atomic snapshot or a completeness certificate. Missing reverse/canonical edges,
malformed records/pages, stale sessions and resource failures yield no partial
result. Data-node scan and traversal counters are separate on that same session;
clients cannot regain budget by switching fragment types repeatedly.

The wire representation is unchanged, but all participants should be upgraded
for this composition: older coordinators reject it and older data nodes may
reject traversal after a long scan because they share one cumulative counter.

## Validation scope

Independent process tests use two deterministic generated graphs, four partitions
on three shards, repeated local IDs, parallel edges, cycles, ordered limits across
128-record pages, and missing/Null properties. They compare exact ordered vectors
from fixture inputs, including 101 returned identities/maps without an implicit
cap. Missing reverse/canonical records remain whole-query failures, including
when a later filter or limit would otherwise hide them.

The CoreHR fixture now contains 102 cases, including 12 global-composition cases:
10 successful shapes (seven samples each) and two expected Unsupported shapes.
The small 2,000-person/50-department matrix and 200,000-person/5,000-department
server matrix passed strict independent validators. On the full fixture the
legacy client is explicitly rejected at its existing snapshot capacity before
running queries; this is not a successful full-scale legacy-client run. Full
CoreHR CI on the final commit is required alongside normal tests and review.
