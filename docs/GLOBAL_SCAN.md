# Bounded global Gremlin scans (development prototype)

`RouteResolver::execute_global_scan(ScanScope, query, timeout)` adds a read-only,
client-side global scan over static local or remote catalog placements. Existing
single-partition APIs and their Gremlin behavior are unchanged. This is a small
D5 scan slice, not cross-partition edge traversal or distributed transactions.

## Query and identity contract

Input is standard Gremlin, parsed by the existing official-grammar parser before
any I/O. Traversals start with `g.V()` or `g.E()`, followed by any sequence of
`hasLabel(label, ...)`, `has(key)`, `has(key, value)`, `values(key)`, `label()`
and `limit(n)`, optionally ending with `count()`. Limits retain the existing
1..=1000 range and repeated limits execute in their original positions.
Explicit IDs, `id()`, adjacency steps (`out`, `in`, `both`), paths, and all other
unsupported steps fail explicitly. No text is rewritten and no independently
truncated query results are joined.

Filters retain input order and use the single-partition operator semantics:
`has` uses strict typed equality, present Null is distinct from a missing key,
and `hasLabel` matches any stored label. `values` emits nothing for a missing
property; `label` emits the first stored vertex label (or empty Text for an
unlabelled vertex) or the edge label. Projections retain their origin partition
and do not sort by projected values. Element-only steps applied to a nonempty
scalar stream return `InvalidGremlinTraverser`, as in single-partition execution;
empty streams remain empty. Count is terminal.

`ScanScope::All` freezes the catalog's registered partition set.
`ScanScope::Selected(Vec<u32>)` freezes that explicitly selected set; duplicate
or unknown partitions are errors. An empty selection is allowed. Every ordinary
result is a `QualifiedGremlinRow { partition, row }` with a Vertex, Edge or
projected Value row.
Local IDs can collide across partitions; collisions remain distinct rows.
Order is ascending `(local ID, partition ID)`, independent of registration,
selection, replica or snapshot acquisition order. `GlobalScanOutput::Count(n)`
is one global count with no artificial partition attribution.

An explicit limit applies globally, so `g.V().limit(2)` returns at most two rows,
and `g.V().limit(2).count()` returns at most two. Without an explicit limit,
ordinary output is capped at 100, after checking the entire candidate set's
10,000-row bound. Count ignores the default output cap but not that bound.
More than 10,000 candidates is `TraversalLimitExceeded`, not partial success.
Two partitions with 6,000 candidates each therefore fail without a leading
limit, even though each fits by itself. The executor scans each partition's
bounded initial candidates using the same ledger, then sorts global identities before applying the operators left to
right. Only an immediately leading limit permits per-partition top N acquisition
and global top N retention. A later filter, projection, count or limit never
waives the 10,000-start-candidate check, even if it would discard every row.

For example, `g.V().has('x').limit(2)` finds the first two matching elements,
whereas `g.V().limit(2).has('x')` tests only the first two global elements and can
return fewer than two. Likewise `g.V().values('x').limit(2)` skips missing
properties before choosing two output values. A leading limit need not decode
later graph records, but never skips acquiring any required shard snapshot.

Candidate acquisition retains identities rather than all record payloads.
Element operators reload from the same retained partition view and reuse the
ordinary operator implementation; this is not a fresh read or replica failover.
All reloads, raw/decoded overlap, projected payloads, intermediate slots and
final output reservation holders use the same query budget. The extra point
reads are bounded by candidate count and the parsed query's 16 KiB size limit.

## Consistency, failures and deadline

One fresh metadata snapshot chooses all placements. Each unique shard acquires
one data view through the existing leader barrier and fixed candidate selection.
Partitions sharing that shard reuse its exact Arc-backed view. Reads after
acquisition never refresh a view or switch replica. The result reports the
metadata sequence and one `ShardReadStamp { group, shard, epoch, sequence }` per
unique shard, sorted by shard ID.

This is a **per-shard snapshot vector**, not a global consistent snapshot.
Shards are acquired at different times; their sequence numbers are not comparable
timestamps. Cross-shard serializability, linearizability and snapshot isolation
are not promised. Catalog registrations after the frozen catalog read do not
change the running query. Existing applied-epoch check/read races remain.

All required shards are acquired before scanning, including for `limit(1)`.
Unknown partition/replica, unavailable data, fencing, corruption, deadline and
resource failures abort the whole call; no partial rows/count are returned.
One operation admission permit covers the entire query. The timeout must be
positive and at most 60 seconds; the effective query deadline is the smaller
of the requested timeout and five seconds. Parsing is excluded. Catalog,
all candidate attempts, all data acquisition, reads, ordering and final return
share that single absolute deadline. CPU checks are cooperative, not preemption
of an individual allocation, decode, native call or destructor.

## Resource contract and exclusions

- At most 32 selected partitions and 8 unique data shards per call.
- Retained data snapshots together admit at most 200,000 key/value pairs and
  16 MiB raw key-plus-value bytes. Reused shard views are counted once.
- Existing adapter per-view limits still apply. Remote acquisition is limited
  to 2 MiB raw bytes / 100,000 rows per shard, with 16 MiB encoded frames.
  Local adapters retain their own existing larger per-view bounds.
- One 64 MiB query-owned logical accounted-byte ledger covers all partition
  scans and overlapping decoded records, page buffers, global candidate IDs,
  projected values, intermediate and output rows. The normal
  [query memory exclusions](QUERY_MEMORY.md) apply.
  Global candidate and final-output vector slots are reserved before allocation.
- A fixed 4096-byte query control fee covers execution controls, not the
  separately fanout-bounded router maps, graph-store wrappers or snapshot vector.

These are logical admission limits, **not total heap or process RSS limits**.
Snapshot payload totals are checked after adapter acquisition but before the
new snapshot is retained for execution. Already admitted snapshots can overlap
one additional adapter-bounded incoming snapshot and transport/decode buffers
while acquiring/rejecting it. Snapshot map overhead, allocator overhead and
adapter-private allocations are excluded. The caller's scope/query/parser,
metadata snapshot, and bounded routing objects are separate. This version does
not claim streaming remote snapshots or a pre-allocation aggregate heap bound.
Independent calls/resolvers also have separate query ledgers. TLS, authentication,
failover resilience, online migration, cross-edge formats and transactions remain
separate future work.

## Verification

`global_scan` integration tests launch actual independent metadata and two data
`graph-node` processes. They exercise overlapping vertex/edge IDs, ordering,
global limit/count/default cap, filters/projections and limit position, strict
types, late matching properties, >10k aggregate rows, same-shard partition reuse,
unknown partitions and unavailable data without partial success. Kernel tests
exercise the official parsed subset, byte ledger, empty results, row boundaries
and deadline rejection. Deterministic acquisition-checkpoint tests interleave
catalog registration and same-shard writes, verify the pinned result versus the
next query, exhaust the shared deadline, and check admission release. Additional
real in-process groups cover 8/9-shard fanout and aggregate 2 x 9 MiB snapshots.
This is functional scan evidence, not a fault-tolerance,
security or global snapshot-isolation claim.
