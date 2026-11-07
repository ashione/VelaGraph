# CoreHR full-scale incoming-query validation

## Result

The integrated candidate passed the **200,000-person / 5,000-department**
workload across three real data processes: **26 supported query cases × seven
samples = 182 correct observations**, plus six explicitly unsupported cases.
The process exited **0**, `complete: true`, `query_failures: 0`.

This includes managers, direct reports, reporting descendants, department
ancestors/descendants, multi-department membership, members, filtered members,
mixed-direction joins, counts, scalar projections and bounded prefixes.
It is not a claim of complete Gremlin or universal HR-query support.

[Machine-readable results](corehr-incoming-results.json) and
[reproduction/model details](COREHR_SIMULATION.md) accompany this report.
[The earlier outgoing-only baseline](COREHR_RESULTS.md) is retained separately.

## Precisely what was tested

- [PR #56](https://github.com/ashione/superagent-playground/pull/56) capacity
  candidate `31a56611bdfe6d269d7dcc6d15cb5032b7dd2bad`, plus
  [PR #55](https://github.com/ashione/superagent-playground/pull/55),
  incoming-query head `80437daf2c0304a162b1b888908e33844637a785`, tree
  `2b58bd9244bdf9804698a6902dc1914d359c3b4e`. PR #55 was subsequently merged
  with that same tested tree. These measurements were made on the explicit
  local integration of those revisions, not PR #55 alone; subsequent CI-only
  or merge commits do not retroactively change the measured revision.
- Release/optimized graph-node, dev/unoptimized harness. Linux x86-64 on an AMD
  EPYC 9V74 host with nine visible/affinity CPUs; effective CPU quota unavailable.
- Metadata and each of three data groups run as separate OS processes, with
  **one replica per group**. Coordinator runs in the harness over loopback TCP.
  No HA or multi-machine/network benchmark is claimed.
- Exactly the same encoded graph bytes as the baseline and the failed first
  incoming run: all three fixture SHA-256 digests match after regeneration.
- 205,000 vertices; 454,998 edges; 303,483 cross-partition edges. Exactly 40,000
  people have multiple departments, including 10,000 with three.
- Every one of **1,569,994 KV records** was verified byte-for-byte through
  paginated snapshots. Oracle calculations are independent relationship-list
  scans and retain exact ordered qualified IDs and bag multiplicity.
- Every shard started at the default 64 MiB D1 capacity, then explicitly and
  successfully moved to the candidate's 128 MiB setting. Actual retained
  accounting matched 69,161,438 / 69,176,495 / 69,069,392 bytes. No query/work
  budget was raised to make the workload pass.

## Representative full-scale query results

Seven serial samples per supported case. p95 is only a seven-sample smoke
statistic, not a stable production service-level estimate. Snapshot setup and
loopback transport are included.

| Query | Verified outcome | p50 | p95 |
| --- | --- | ---: | ---: |
| CEO's direct reports | 8 people, exact order | 412.79 ms | 482.57 ms |
| Two-level reporting descendants | 64 people | 427.06 ms | 457.15 ms |
| Employee 20's departments | 3 departments | 373.25 ms | 418.48 ms |
| Department ancestor, three hops | Exact ancestor ID | 362.16 ms | 378.24 ms |
| Department descendants, two hops | 25 departments | 902.18 ms | 987.45 ms |
| Department members | 180 people | 488.57 ms | 604.67 ms |
| Active department members | 176 people | 558.52 ms | 594.90 ms |
| Employee → departments → colleagues, limit 25 | Exact 25-row prefix | 534.49 ms | 575.47 ms |
| Colleagues-through-departments count | 324 traversal rows | 570.22 ms | 639.40 ms |
| Duplicate starts → departments | 6 rows, duplicates preserved | 402.65 ms | 423.95 ms |

The 324-row join count is **not 324 unique employees**. A person belonging to
three shared departments can appear three times; person 20 does so in this
fixture. `dedup` remains unsupported. Do not use this count as a unique HR
headcount without a separately verified deduplication step.

Complete six-level manager and department-ancestor chains also passed as bounded
application loops of one-hop queries, taking 2.69 s and 2.51 s. These are not
native recursive Gremlin and do not use one atomic snapshot across requests.

## Failure found and fixed by the large workload

The first incoming implementation passed the small fixture but failed the full
query `g.V(200002).in('parent_department').in('parent_department')` on all seven
attempts with `ResourceExhausted`, despite the expected answer having only 25
rows. Dense employee-membership edges on the same departments also consume
validation work. The failed run is preserved and is not counted as a pass.

The fix removed redundant reverse-index validation already established by the
same pinned snapshot, and discarded nonmatching-label candidates only **after**
required validation. It did not increase limits, silently truncate results, or
skip corruption checks on nonmatching labels. The final full-scale run returned
all 25 expected departments on all seven attempts. Independent adversarial
coverage checks wrong labels, targets/owners, missing source endpoints,
cross-owner identity collisions, self-loops and high-fan-in resource rejection.

The first failed integration also used a debug node, while the baseline and final
run use release nodes. Its 85.93 s import cannot be directly compared with the
release runs or called an incoming-code performance regression. The resource
failure itself was deterministic, not a timeout, and required the code fix.

## Load and resources

- Offline-encoded fixture import through bounded Raft D1 writes: **21.86 s**,
  315 fresh committed batches, at most 5,000 operations / 2 MiB raw bytes each.
- Batch latency: p50 **60.70 ms**, p95 **118.09 ms**, max **139.66 ms**.
- Full harness wall time: **137.82 s**, including loading, all-KV verification,
  rejected queries, supported queries and application chains; compilation and
  fixture generation are excluded.
- Final database logical file bytes: **830,782,082** (792.3 MiB). This is not a
  stable post-compaction allocation measurement.
- Final data-node RSS/high-water RSS: **1007.5 / 844.5 / 824.5 MiB**. Harness
  final RSS **171.3 MiB**, high-water RSS **227.9 MiB**. Per-process samples are
  not a synchronized combined memory peak.

This measures an offline-encoded import, not normal per-edge graph-API mutation
throughput or cross-shard commit recovery. Memory remains much larger than the
raw payload, and every point-query result still includes all three shard stamps;
snapshot setup/caching and memory overhead need further profiling.

## Remaining gaps

- Full client-side observed sessions still reject the aggregate **200,000 KV /
  16 MiB** cap; the full-scale successful queries use server coordination.
- Global `g.V()` starts/counts, `repeat`, `range`, `order`, predicate arguments
  and `dedup` were explicitly rejected. `limit` is a prefix, not continuation or
  offset pagination. Unique cross-department headcount remains a gap.
- Mixed server traversals remain bounded to three hops. High fan-in and
  cumulative work can still legitimately reject; small output is not proof of
  small scan/validation work.
- All v2 results remain `Unverified` with independent shard snapshots. No global
  consistency, cross-shard transaction, mutation-failure recovery, HA or secure
  production transport guarantee is claimed.

## Final validation

The exact combined candidate passed **439 workspace tests**, with zero failures,
two existing ignored tests and eight explicitly excluded Unix-domain-socket
cases. Those eight were observed to fail with sandbox `EPERM` in the earlier
unfiltered aggregate run; this is **not a claim of a fully green default suite**.
The fixture model test passed separately, and formatting and
`cargo clippy --all-targets --locked -- -D warnings` passed. The integrated
capacity files and incoming-query files match their respective candidate
manifests byte-for-byte. No repository merge or deployment is implied.
