# CoreHR simulation results (2026-11-07)

## Verdict

The requested 200,000-person / 5,000-department synthetic graph was loaded and
verified across three real data processes. Supported outgoing cross-shard
queries matched an independent relationship-list oracle. This is **not** a claim
that general CoreHR queries are fully supported: broad client queries hit their
aggregate snapshot cap, and the distributed server rejects incoming traversal,
global scans and several standard Gremlin operations.

The full process exited **0** with `complete: true` and `query_failures: 0`.
That means every supported case succeeded and every declared unsupported case
rejected as expected; it does not turn rejected queries into supported queries.

Machine-readable measurements: [corehr-results.json](corehr-results.json).

## Exact scope

- Measured [PR #56](https://github.com/ashione/superagent-playground/pull/56)
  engine revision: `31a56611bdfe6d269d7dcc6d15cb5032b7dd2bad`, based
  on main `6ac282f`. These measurements retain the exact tested revision;
  consult the PR for subsequent merge or CI changes.
- Node binary SHA-256:
  `b1d248c03dbfcfe65d1fbd2794d368df935ee3a88e1b94ed8c32913bbc078752`.
- Linux x86-64, AMD EPYC 9V74 host, nine visible/affinity CPUs; effective CPU
  quota was not exposed. Release/optimized archived graph-node, with a dev/unoptimized harness
  (harness debug symbols disabled). This mixed-profile setup is explicit, not
  a directly comparable baseline for an all-debug node run.
- One metadata OS process and three data OS processes, **one replica each**.
  Coordinator is a TCP service in the harness process. All traffic is plaintext
  loopback. This is not a multi-machine or high-availability benchmark.
- Synthetic data only. Details and reproduction:
  [COREHR_SIMULATION.md](COREHR_SIMULATION.md).

## Dataset and correctness

- 205,000 vertices: 200,000 people and 5,000 departments.
- 199,999 employee-to-manager edges, 4,999 child-to-parent department edges,
  250,000 person-to-department memberships; **454,998 edges total**.
- Exactly 40,000 people have multiple departments, including 10,000 with three.
- 303,483 edges cross partitions (66.7%). No duplicated local substitute edges.
- 1,569,994 persisted graph KV records, **106,887,389 raw key/value bytes**.
- All 1,569,994 KV records were read back through paginated snapshots and
  compared byte-for-byte with the encoded fixture, including both adjacency
  indexes. Each shard had exactly the expected count, with no extra records.
- The small 2,000-person / 50-department run completed all **32 client cases**:
  27 successful oracle checks and five explicit unsupported-operation checks.
- The full run completed all **32 server cases**: 15 supported cases, each
  repeated seven times (**105 successful observations**), and 17 explicitly
  rejected cases. No supported sample failed or differed from the oracle.
- Bag multiplicity was checked with duplicated starts and a colleagues-through-
  departments count; the oracle verifies the starting employee appears three
  times through three memberships, rather than silently deduplicating.
- Two bounded application loops additionally verified complete six-level manager
  and department ancestry. These are separate one-hop requests, not native
  recursion and not one global snapshot.

## Capacity, load and resource measurements

Each shard started pristine with the unchanged 64 MiB D1 retry-state limit.
The harness explicitly upgraded each to the candidate's 128 MiB limit and
checked the typed response. Actual accounted retained bytes after import were
69,161,438 / 69,176,495 / 69,069,392, exactly equal to the fixture estimates.
Those values exceed 64 MiB; this run would not fit that default in three shards.
The upgrade did not change the graph query limits.

- Offline fixture encoding: **9.76 s**.
- Raft fixture import: **21.51 s**, 315 batches, up to 5,000 operations and
  2 MiB raw bytes per batch. Every response was fresh `Committed` at the
  expected sequence, not a duplicate retry.
- Import-batch latency: p50 **58.46 ms**, p95 **119.97 ms**, max **170.06 ms**.
- Exact paginated readback: **7.36 / 7.37 / 7.42 s** per data shard.
- Full harness wall time: **100.50 s**, including load, verification, negative
  checks and queries, excluding fixture generation and compilation.
- Database file bytes at the final sample: **830,778,518** (792.3 MiB), across
  metadata and data process directories. This is logical file length, not a
  stable post-compaction disk-allocation measurement.
- Final data-node RSS: **826.6 / 753.2 / 673.9 MiB**. Individual node high-water
  RSS: **826.6 / 753.2 / 737.1 MiB**. Harness RSS/high-water RSS: **251.9 MiB**.
  These are per-process observations, not a synchronized combined peak.

Import uses existing public v2 encoding followed by bounded D1 writes. It is
**not** ordinary graph-mutation throughput and does not test normal per-edge
cross-shard commit/partial-failure recovery.

## Representative full-scale queries

Each supported case below has seven serial samples, with fresh server snapshot
setup included. These are smoke latency distributions, not statistically stable
production p95s or concurrency throughput measurements.

| Query | p50 | p95 | Outcome |
| --- | ---: | ---: | --- |
| Employee 200000's manager | 382.59 ms | 464.21 ms | Exact qualified ID |
| Employee 20's three departments | 372.13 ms | 419.77 ms | Exact ordered IDs |
| Employee 200000's manager, three hops | 438.39 ms | 464.36 ms | Exact qualified ID |
| Department 205000's ancestor, three hops | 389.63 ms | 443.22 ms | Exact qualified ID |
| Employee → departments → parent departments | 429.69 ms | 499.66 ms | Exact ordered IDs |
| Department-label-filtered membership count | 412.51 ms | 482.97 ms | Count 3 |
| Employee name projection | 388.69 ms | 407.70 ms | Exact synthetic name |
| Duplicate employee starts → departments | 399.95 ms | 440.05 ms | Six rows, multiplicity preserved |

Across the 15 supported cases, per-case p50 was **372.13–438.39 ms** and p95
**407.70–573.49 ms**. Missing-vertex and root-without-manager cases returned the
expected empty result, not an error.

## Current query gaps and next work

1. **Broad client-query capacity:** full-scale session acquisition rejected with
   `ResourceExhausted("global scan fanout or aggregate snapshot limit exceeded")`
   after 7.67 s. Its aggregate limit is **200,000 KV / 16 MiB**, far below this
   fixture. Increasing D1 capacity does not resolve it.
2. **Incoming traversal at scale:** server distributed queries reject `in`, so
   direct reports, department descendants, department member lists and
   colleagues joins are supported by the smaller client path but **not verified
   as available at full scale**. Server-side incoming traversal with bounded
   work is the most direct functional next step.
3. **General-query coverage:** server global `g.V()` is Unsupported. Recursive
   `repeat`, `range` pagination, `order`, predicate arguments and `dedup` reject
   explicitly. `limit` is a bounded prefix, not full pagination. Full counts and
   broader joins need a separately designed bounded server execution path.
4. **Snapshot setup and memory:** even point-query results contain all three
   shard stamps, and one-hop and three-hop latencies are similar. This suggests
   setup/snapshot work is a significant cost, but no profiler attribution is
   claimed. Lazy shard acquisition / retained snapshot reuse merit measurement.
   The large gap between raw payload and process RSS also needs profiling.
5. **Production boundary:** all v2 results remain `Unverified`; no global
   consistency, atomic cross-shard write, HA, failure recovery or secure remote
   transport claim is made.

The harness changes only examples, a reproduction script and documentation.
No query grammar, storage format, production default or resource limit was
silently changed to make this simulation pass.

## Validation gates

`cargo fmt --all --check`, the fixture invariant test, and
`cargo clippy --all-targets --locked -- -D warnings` passed. The full
`cargo test --locked` command was run and stopped at eight existing Unix-domain-
socket gateway tests with `Operation not permitted (os error 1)` in this cloud
sandbox. It is **not reported as a full green workspace suite**. Production
files are byte-identical to the capacity candidate; the new smoke/full harness
and independent review above passed. No publishing, merging or deployment is
part of this local validation report.
