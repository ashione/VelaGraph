# Generic bitemporal graph design plan

Status: proposal under [#196](https://github.com/ashione/VelaGraph/issues/196),
not an implemented API, storage format, or claim of arbitrary historical reads.
Existing certified fixed-V reads establish a transaction-prefix view, not a
business-valid-time view. Indexed MVCC and history retention remain separate work.

## Two coordinates, one distributed view

A proposed view binds cohort identity, committed system version V, and business
valid time T. System versions come from the commit protocol, not client wall
clocks. A wall-clock-to-version mapping needs an explicit contract; a timestamp
alone is not a globally certified version. Default now/latest is resolved once
at admission, and every shard, traversal fragment and result page keeps V/T.
The currently certifiable common prefix is not necessarily linearizable latest.

For example, a department transfer effective November 1 is recorded November 10.
A query using current knowledge for November 5 sees the corrected assignment;
a query using knowledge available November 6 must preserve the earlier answer.
A future effective date supports preview of already-recorded plans, not a
prediction of unrecorded future events.

## Generic records and query semantics to freeze

- Vertex and edge revisions carry half-open valid intervals [from,to).
  Define precision, timezone normalization, open endpoints, adjacent coalescing,
  overlapping corrections, deletion and absence before choosing physical keys.
  Initially evaluate properties versioned with their containing record;
  independent property timelines require a separate demonstrated need.
- Separate logical entity/relationship identity from immutable revision identity.
  Do not silently reuse the current permanently reserved qualified edge IDs.
  Corrections append a system version rather than erase what was previously known.
- Atomically adjust related intervals across shards; define conflicts and
  endpoint existence at the requested time. Multiple memberships are ordinary
  parallel relationships. Unique-primary and acyclic-hierarchy constraints are
  optional schema/business policies, not hardcoded CoreHR labels.
- Start with point-in-time graph views. Every edge and endpoint on a returned
  path must coexist at T. Later interval-path queries must explicitly compute
  common validity; edges that existed at unrelated times cannot imply a
  historically real path. History enumeration and interval aggregation are
  separate query capabilities, not implied by point-in-time snapshots.
- Bind view coordinates through the query API/session and reuse the official
  Gremlin parser/executor; do not invent Gremlin syntax. Unsupported combinations
  fail explicitly. API names and wire/storage layouts are not frozen here.

## Delivery order and evidence

1. Freeze an ADR with semantic examples, versioned format/migration and retention
   contracts, comparing mature systems before selecting implementation.
2. Build a single-shard reference model and independent two-dimensional oracle:
   boundary instants, late arrival, repeated corrections, future changes,
   multiple edges, endpoint existence, deletion and pre-correction knowledge.
3. Integrate real managed same-V/T distributed execution. Test concurrent writes,
   pages, partial apply, Abort, lagging members, original-ID retry and restart.
   Do not substitute observed shard snapshots for global certification.
4. Extend #111's 200,000-person / 5,000-department fixture plus non-HR graph cases.
   Keep small semantic gates in ordinary CI and full historical/dynamic workloads
   in dedicated long jobs. Report correctness, scale and performance separately.
5. Profile temporal adjacency/index and immutable-base-plus-delta designs.
   Measure write amplification, retained history, cold/warm latency and memory;
   avoid full-history reconstruction per query or full-graph copying per write.

## Retention, recovery and object storage

Runtime query pins, promised audit/history retention and backup retention differ.
No active old-V reader does not by itself authorize dropping history. Define GC
against both promised temporal dimensions and explicit retention policies;
unavailable history must error rather than silently return latest.

WAL, checkpoints and manifests must retain authoritative temporal revisions,
format versions and sufficient index-rebuild evidence. Object storage with local
caches is a durability placement, not a temporal or distributed-consistency
guarantee. Verify missing objects, mixed generations/cuts and interrupted restore.
This planning document implements no GC, purge/install, capacity relaxation,
live migration or new object-store upload path.

## Integration and references

Track implementation acceptance in #196 rather than duplicate checklists here.
Reuse #109 history/recovery, #110 consistent-query capacity, #111 independent
CoreHR oracle and #187/#188 read-your-writes lower-bound foundations. Coordinate storage with #191/#192 and physical
checkpoints with #193/#194; neither completes temporal semantics. Existing
purge/install prerequisites remain independent and are not bypassed.
Necessary validation counters belong to each slice; the #122 product
observability platform remains lower priority. Existing feature/performance work
continues while temporal format implications are designed.

References:
- [XTDB 1.x bitemporality](https://v1-docs.xtdb.com/concepts/bitemporality/):
  semantic inspiration, not an engine/dependency selection or parity claim.
- [Certified cohort reads](CERTIFIED_COHORT_READS.md)
- [Managed local gateway](MANAGED_LOCAL_GATEWAY.md)
- [Atomic managed edge replacement](MANAGED_EDGE_REPLACEMENT.md)
