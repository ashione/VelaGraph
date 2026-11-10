# CoreHR unique-membership validation

Historical measurements below describe the tested revisions, not the current
client subset. As of the qualified-element dedup change (2026-11-10), routing-v3
also supports element `dedup()`; see [the current contract](EXPERIMENTAL_CROSS_EDGES.md).
The original measurements and rejection counts are preserved unchanged.

This is a follow-up to [the incoming-query run](COREHR_INCOMING_RESULTS.md), not a rewrite of its historical results. All people and departments are synthetic. The dataset is unchanged: 200,000 people, 5,000 departments, 454,998 edges, 40,000 people in multiple departments, and 303,483 cross-partition edges.

## Tested version and environment

The engine combines main `ae984387128e18217cf3ae1863b61abb3b8a78d5` with the eight files from PR #58, initial head `fd65e606cbf07c6fa427f85d9a76ea61b485a8ab`, tree `c4c8f4c1e4792066589d63caa76f3d1f63544438`. Those files are individually hashed in the machine-readable report. The harness is the nine-file PR #57 package with the model and runner extensions described here. This records the tested candidate, not a claim that a later merge was rerun.

Cloud Linux, AMD EPYC 9V74, nine visible CPUs, approximately 9.7 GiB system memory, overlay disk. Release graph-node, dev/unoptimized harness, `TOKIO_WORKER_THREADS=2`; one metadata process and three data processes, one replica per group, plaintext loopback. The coordinator runs in the harness. No concurrent benchmark or compiler was run during the final measurements.

## Correctness

Both the 2,000-person/50-department smoke fixture and the full fixture completed all 40 cases. Each run had 35 supported cases sampled seven times: **245 exact oracle matches, zero unexpected query failures**, plus five explicit server Unsupported results. All 1,569,994 full-fixture KV records were independently read back and compared byte for byte. The application-loop manager and department chains also completed.

New server cases verify qualified-vertex deduplication in first-occurrence order, without changing the existing bag cases:

| Full-fixture query | Exact result |
| --- | ---: |
| Multidepartment colleague join, bag count | 324 |
| Same join, dedup count/full list | 217 distinct people |
| Dedup then active filter/count | 206 |
| Limit 184 then dedup | 183 people |
| Dedup then limit 184 | 184 people |
| Repeated start followed by repeated dedup/count | 1 |
| Repeated start, department traversal, dedup/count | 3 departments |
| Empty subordinate stream, dedup/count | 0 |
| Dedup first complete path | 217 ordered paths |

Person 20 appears three times in the bag but only once after dedup. The first-path oracle includes vertex and edge kind, owner partition, ID, and the entire traversal path. A separate Python reconstruction from the dataset formulas confirmed the full and smoke ordered lists, paths, counts, and non-vacuous limit-order difference, without reading graph database results. The model's unit tests assert these distinctions.

The full-list and path cases explicitly use `limit(1000)` to override the documented default 100 output rows. An initial smoke attempt omitted this limit and correctly received the first 100 of 211 unique smoke results; this was a harness expectation mistake, not an engine regression. Earlier smoke attempts also exposed a missing distinction between client and server dedup support. A subsequent harness attempt correctly recognized the direct client `Unsupported` variant but then wrongly applied the older query-parser error allowlist as a second check; the final harness preserves the exact variant assertion and applies the parser allowlist only to other rejected cases. These failed local runs are retained separately and excluded from successful results.

## Client/server boundaries

Dedup is supported by the server coordinator only. All nine new dedup cases explicitly assert client `Unsupported` in the small fixture. Its complete client matrix has 27 correct results and 13 expected rejections. The full fixture's broad client session fails with `ResourceExhausted` before query execution because its snapshot exceeds the existing aggregate cap; no full-fixture client query is counted as successful.

Server global starts/counts, recursive repeat, range pagination, sorting, and predicate arguments remain explicitly unsupported. Dedup does not remove upstream scan/work charges or lift resource budgets. V2 results remain `Unverified`; one replica per group does not test HA, and application loops do not provide an atomic global snapshot.

## Measurements

These are single-host synthetic observations, not production performance guarantees. The larger matrix is not directly comparable to the earlier run's total time.

- Full harness wall time: **179.71 s**; smoke: **19.42 s**.
- Offline-encoded fixture import through D1/Raft: **22.31 s**, 315 batches. Batch p50/p95: **60.73/126.92 ms**. This is not normal graph mutation throughput.
- Explicit per-shard D1 retained-state capacity: 64 MiB to 128 MiB, unchanged from the earlier incoming run.
- Logical database bytes: **830,780,767** (792.3 MiB).
- End-of-run data-process RSS/HWM: **828.1, 844.1, 826.7 MiB**. Harness RSS/HWM: **234.2 MiB**. These are process observations, not a total-memory or peak-container measurement.

| Query | p50 ms | p95 ms | Samples |
| --- | ---: | ---: | ---: |
| Complete unique colleague list | 571.98 | 623.63 | 7 |
| Unique colleague count | 603.35 | 634.48 | 7 |
| Active unique colleague count | 685.18 | 758.45 | 7 |
| First complete path per unique colleague | 610.60 | 663.98 | 7 |

## Reproduction and evidence

Run `scripts/corehr_simulation.sh NEW_OUTPUT_DIRECTORY` from the repository root. It builds a release node and dev harness, generates both deterministic fixtures, imports into fresh process directories, and runs the complete oracle matrix. It refuses an existing output path. Keep the manifest, per-sample JSONL, node logs, binary/source hashes, and process exit status; absence of the final `complete:true` record is not a pass.

The paired [JSON report](corehr-dedup-results.json) preserves provenance, query summaries, resource measurements, and explicit rejection categories. Raw fixture databases and failed-run logs are retained as local evidence, not committed. Final local validation for the extended harness passed formatting, both model tests, and all-target Clippy. The prior 439-test non-UDS aggregate belongs to the incoming-engine run; it is not claimed as a fresh aggregate for this extension. PR #58's engine CI and this harness's eventual exact-head CI must be checked separately.
