# Bounded global vertex scan into traversal

The experimental server coordinator accepts a global vertex-filter prefix and
then bounded out/in/both and repeat steps, without downloading shard snapshots or
re-parsing rewritten query text. The official Gremlin AST is split at its first
adjacency or repeat step. There is no schema-, label-, population- or answer-specific path.

## Supported pipeline

- Start: `g.V()` over the immutable startup catalog.
- Scan prefix: any supported `has`, `hasLabel`, vertex `dedup` and `limit`, in their original order.
- Traversal suffix: `out`/`in`/`both` (up to three total), `has`, `hasLabel`, `limit`,
  vertex-identity `dedup`, and optionally terminal `count`, single-key `values`, or selected `valueMap`.
- One native `repeat(single out/in).times(1..32)` with optional post-emit is
  admitted, including before/after ordinary hops. Combined ordinary/repeated
  depth is at most 32; the separate ordinary three-hop bound is unchanged.
- A suffix ending in vertices returns qualified identities. `valueMap` keeps the
  existing 1–8 distinct keys, UTF-8 length and per-map byte bounds, missing/Null
  semantics, and singleton-list values.

Examples:

- `g.V().has('region','west').limit(8).out('member_of').dedup().count()`
- `g.V().has('name','Operations').in('member_of').has('active',true).limit(20).valueMap('name','country')`
- `g.V().limit(4).has('active',true).out().in().dedup()`

At least one out/in/both or repeat step is required for this additive surface. Global `E`,
`path`, and other unsupported steps fail explicitly.
Terminal `values(key)` emits one scalar per traverser with that property, preserving
ordered bag multiplicity and qualified vertex provenance. Missing properties emit
no row; present Null emits Null. Equal scalars are not deduplicated. Filters and
limits preceding values retain their original vertex-stream positions.
No-hop values requires an explicit preceding vertex limit under the separate
[scan-only contract](SERVER_GLOBAL_SCAN.md). Multiple keys and any step following
values remain Unsupported.
Values uses the existing fixed-session property fragment and traversal charges;
there is no new budget, implicit 100-row truncation, session refresh or wire format.
No-hop `dedup`/`valueMap` follow the separate [scan-only contract](SERVER_GLOBAL_SCAN.md).
Explicit-ID contracts stay intact.

## Order, multiplicity and completeness

The scan supplies seeds in `(partition, local vertex ID)` order. Limits in the
scan prefix remain before traversal; they are not pushed past filters or edges.
Every partition/page carries the remaining prefix limit. The complete admitted
seed stream is then passed to the existing bounded traversal in that order.
Parallel edges and repeated destinations preserve their multiplicity until an
explicit dedup. Dedup uses qualified identity and preserves first occurrence.
Limits in the suffix act at their exact original positions.

Repeat follows the existing native breadth-first semantics: exact depth returns
only the final frontier; post-emit returns each depth in order, excluding initial
roots. Cycles, converging paths and parallel edges retain multiplicity. Terminal
dedup does not prune later traversal. All depth and shape validation precedes
scan acquisition even for empty roots. Nested/multiple repeats and
conditional termination remain Unsupported. This synchronous extension does not
add global roots to hierarchy jobs or paginated result APIs.

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

The CoreHR fixture now contains 122 query cases, including six bidirectional
cases and the existing global-composition cases. Supported server queries use
seven samples each; unsupported shapes retain explicit rejection expectations.
Eight repeat shapes cover named ancestors, selected ancestor properties, exact
depth, descendants, overlapping roots with bag/dedup, a membership hop before
department ancestors, and empty roots. These expectations use input edges only.
The small 2,000-person/50-department matrix and 200,000-person/5,000-department
server matrix must pass strict independent validators for this extension. On the full fixture the
legacy client is explicitly rejected at its existing snapshot capacity before
running queries; this is not a successful full-scale legacy-client run. Full
CoreHR CI on the final commit is required alongside normal tests and review.

Global scan prefixes may include vertex `dedup()` before the first out/in/repeat.
The scan already emits unique (partition,id) identities, so prefix dedup preserves
its ordered roots exactly and limits keep their positions. This does not dedup
subsequent traversers: parallel edges, convergence and repeat emission preserve
bag multiplicity unless a separate downstream dedup is explicitly present.
No extra scan, budget, refresh or default truncation is introduced.


Ordinary `both()` / `both(label)` also joins at the first adjacency step. It
merges the two validated directions per input by `(edge ID, source partition)`,
emitting an exact self-loop once while preserving parallel-edge and repeated-root
bags. Global path and repeat-body both remain unsupported. The existing three
ordinary-hop / 32 combined-depth limits include both; fixed sessions, complete
result semantics and separate cumulative budgets are unchanged. See the
[bidirectional contract](DISTRIBUTED_QUERY.md#bidirectional-vertex-adjacency).
