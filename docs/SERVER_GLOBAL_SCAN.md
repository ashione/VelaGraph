# Experimental server-coordinated global vertex scan

The distributed coordinator now accepts `g.V()` over **all partitions in its
immutable startup catalog**, with ordered `has`, `hasLabel`, `limit` and terminal
`count`. This is the append-only v2 vertex namespace, not the legacy v1 graph.
The request's `start_partition` must still identify a catalog partition but does
not select the scan scope. No live metadata discovery or selected-partition API
is added.

Examples:

- `g.V().count()`
- `g.V().hasLabel('person').has('active',true).count()`
- `g.V().hasLabel('department').limit(1000)`
- `g.V().limit(80).hasLabel('person').count()`

A bounded [scan-to-traversal composition](GLOBAL_VERTEX_TRAVERSAL.md) additionally
allows a filter/limit/vertex-dedup prefix followed by out/in, filters, dedup, limit and terminal
count or selected valueMap. Global E scans, path, ordering/modulators, recursive
traversal and user-visible pagination remain Unsupported. A no-hop global scan additionally accepts vertex `dedup()` between its
filters/limits and terminal selected-key `valueMap('key',...)`. Scan identities
are already unique by (partition,id), so dedup preserves each row and does not
merge equal local IDs from distinct partitions. Projection returns singleton-list
property maps with missing keys omitted, in unchanged scan order; it reads only
the selected vertices from the same retained sessions. Projection after an edge
hop continues to use the existing traversal path. Empty/all-key valueMap, map
post-processing, scalar dedup remain Unsupported.
No-hop projection retains the existing default final limit of 100 when no limit
is explicit, the 1,000-row cap, the scan budget, and a separate existing 64 MiB
traversal/projection budget for projection fragments and retained maps. Final
encoded response copies are charged to the existing scan budget. It does not
download whole-shard snapshots. Explicit-ID behavior is unchanged.
All input is parsed with the official grammar; no rewriting or alternate syntax
is used.

## Order and pipeline semantics

The server scan order is ascending **(partition ID, local vertex ID)**, independent
of catalog registration order. Equal local IDs in different partitions remain
distinct. This intentionally differs from the legacy client-side global-scan
API's (local ID, partition ID) order; callers must not assume identical prefixes
between these separately typed APIs.

Filters use the same strict property equality and label membership semantics as
explicit-ID fragments. Every limit consumes rows at its original position:
`limit(80).hasLabel('person')` filters the first 80 vertices, while
`hasLabel('person').limit(80)` finds the first 80 people. Remaining limits carry
across every page and partition. A limit can stop the source once no subsequent
row could reach the output. For scan-only non-count queries without an explicit limit,
a final implicit limit of 100 supplies the ordinary output cap. Count has no
implicit 100-row cap. Empty input gives count zero.

Scanning is a bounded iterator, not eager adjacency expansion: it reads only
needed pages, and does not inspect the unseen suffix after a limit. All records
in an acquired page are decoded and checked **before** filters or limits can
hide corruption, including nonmatching records. Page read-ahead can therefore
report corruption beyond the final returned vertex in that same page. This
contract does not certify the integrity of unvisited vertices or any edges.

## Data movement, snapshots and failures

All unique data-shard sessions are pinned before any page is requested. Pages
use those exact retained views, token/incarnation, epoch fencing and original
request deadline. Two catalog partitions on one shard reuse its session.
No per-page refresh, replica retry, or fallback download is permitted.

The data node uses `AppendOnlyStore::vertex_page` on its frozen local view. It
validates canonical key shape, strictly increasing IDs, record envelope,
partition and record ID before evaluating filters. Only matching IDs or a
page-local count and bounded progress metadata cross the network. Vertex
properties, raw KV pages and whole-shard snapshots are not downloaded to the
coordinator. Existing session acquisition can still copy a bounded whole shard
**locally on the data node**; this is not a zero-copy-storage claim.

Each successful answer retains `Unverified` and the sorted actual per-shard
snapshot vector. Those independently pinned views are **not one globally atomic
snapshot**. A missing shard/session, stale epoch, malformed record/progress,
resource failure or timeout returns an error, never a partial list or count.
Cleanup and expiry are the same as explicit-ID coordination. Older peers that
do not recognize the additive Scan fragment fail explicitly; mixed-version
scan execution has no fallback.

## Explicit scan budgets

This is a new budget class, **not** a relaxation of explicit-ID traversal caps:

- Global scan: 1,000,000 cumulative work units and 256 MiB cumulative logical
  allocation/transport charges across the coordinator and all shards.
- Each storage page: at most 128 records and 1 MiB + 64 bytes of encoded KV.
  A first record that does not fit is an error, never a stalled empty page.
- At most 32 filter/limit operations in a fragment; query text/structural limits
  remain as documented for the official parser.
- At most 1,000 returned vertex rows (or the ordinary default 100). Count is a
  checked scalar accumulator, not an array of all matching vertices.
- Work charges every visited record, page/request, and reached pipeline step;
  bytes charge encoded records, conservative decode overhead, output references
  and transport. Filtered-out data still consumes work/bytes. Nothing refunds
  previously consumed allowances.
- The existing 5-second maximum request/session deadline, 2 MiB encoded result,
  process concurrency limits and local retained-snapshot limits remain unchanged.
- Explicit-ID traversal still has 10,000 work units, 64 MiB cumulative logical
  bytes, 10,000 intermediate rows and its existing adjacency cap.

These are conservative logical bounds, not a process-RSS promise. The only
per-page vertex materialization is the bounded local page; the coordinator
retains at most 1,000 references and constant-size count/progress state. Large
records or long filter pipelines may exhaust the budget before 1,000,000
vertices. A following limit/count cannot convert exhaustion into success.

## Regression evidence

The process test uses three real data nodes and a small CoreHR model plus 401
isolated page probes (including equal local IDs in distinct partitions). It
checks complete count, multi-page ordered output, cross-partition limits,
filter/limit placement, empty filters and ordinary explicit-ID regressions.
Unit tests cover bounded continuation, first-row byte failure, exhausted work,
corruption before filtering, plan exclusions and qualified output validation.
Large-scale results are reported separately for their exact tested candidate.

Global scan prefixes may include vertex `dedup()` before the first out/in/repeat.
The scan already emits unique (partition,id) identities, so prefix dedup preserves
its ordered roots exactly and limits keep their positions. This does not dedup
subsequent traversers: parallel edges, convergence and repeat emission preserve
bag multiplicity unless a separate downstream dedup is explicitly present.
No extra scan, budget, refresh or default truncation is introduced.
