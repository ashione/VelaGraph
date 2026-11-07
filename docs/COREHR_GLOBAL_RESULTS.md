# CoreHR global vertex-scan validation

This report adds a global-scan experiment to the earlier [incoming](COREHR_INCOMING_RESULTS.md) and [dedup](COREHR_DEDUP_RESULTS.md) runs. It does not replace their historical measurements. All data is synthetic and deterministic.

## Exact tested integration

The engine is the frozen ten-file PR #60 candidate, head `9ea6f9baf38a0c0a1c0b78cf1ca9b33c1b3fd52f`, tree `f123c1efeca0ebfcdd48f9a387c8bf67eb3fa1b6`, based on main `0b691fa8` with the earlier CoreHR harness. The model and runner extend that harness with separate server/client global oracles and exact rejection classification. The paired JSON report records source and archived-binary hashes; a later merge does not retroactively change the measured revision.

Cloud Linux, AMD EPYC 9V74, nine visible CPUs, approximately 9.7 GiB system memory, overlay disk. Release graph-node and dev/unoptimized harness, `TOKIO_WORKER_THREADS=2`. One metadata process, three data processes, one replica per group, and a coordinator in the harness communicate over plaintext loopback. No concurrent compiler or benchmark ran during the final measurements.

## Dataset and complete result

The unchanged full dataset contains 200,000 people, 5,000 departments, 199,999 reporting edges, 4,999 department-parent edges and 250,000 membership edges. Of 454,998 total edges, 303,483 cross partitions. Forty thousand people have multiple department memberships.

Both the 2,000-person/50-department smoke and full runs completed **55 cases: 50 supported cases × seven samples = 350 exact matches**, four explicit `Unsupported` rejections, and one explicit `InvalidArgument` rejection for `limit(0)`. Both exited zero with `complete:true` and `query_failures:0`. Every one of the 1,569,994 full-fixture KV records was read back and compared byte for byte. All earlier explicit-ID bag, dedup, path and relationship cases remain in the matrix; the application-loop ancestor chains also complete.

The 15 supported global cases verify:

| Query or property | Full-fixture exact result |
| --- | ---: |
| All vertices | 205,000 |
| People | 200,000 |
| Departments | 5,000 |
| Active people | 190,091 |
| Inactive people | 9,909 |
| Active departments | 4,716 |
| People with multiple departments | 40,000 |
| CN people | 40,012 |
| Missing label | 0 |
| Limit 300 before department filter/count | 0 |
| Department filter before limit 300/count | 300 |
| Default global prefix | 100 ordered vertices |
| Explicit prefix spanning storage pages | 300 ordered vertices |
| Active prefix | 300 ordered vertices |
| CN departments | 996 ordered vertices across all three partitions |

Active status is calculated from the actual model's salted SplitMix64 formula, not an assumed 80% or exact 95% proportion. A separate Python reconstruction checks every new count and ordered prefix from the formulas without database results. Unit tests enforce the distinct API orderings, active/inactive complement and observable filter/limit difference.

## Ordering, limits and boundaries

The server's order is **(partition ID, local vertex ID)**; the legacy client global API orders **(local vertex ID, partition ID)**. The fixture therefore supplies two independent expected sequences. For example, the server starts with qualified vertex `[1,3]`, while the client starts with `[2,1]`. A 300-row prefix crosses 128-row storage pages; the 996-row CN-department query crosses all three partitions. This is internal paging, not a user-visible pagination or sorting API.

The small client matrix completes 55 cases with 41 correct results and 14 expected rejections. Server-only dedup remains explicitly rejected by the client. The full broad-client snapshot is still `ResourceExhausted` before query execution; no full client query is counted as successful.

The official grammar restricts explicit limit to 1..=1000. `limit(0)` must reject: client `InvalidGremlinLimit(0)`, server `InvalidArgument`. Two initial smoke attempts were stopped by harness error-category allowlists that omitted these exact variants; their logs and databases are retained separately and excluded from passing evidence. No production change or grammar relaxation was made to accommodate them.

Global scans support bounded `has`, `hasLabel`, `limit` and terminal `count`, not global adjacency joins, global dedup, projection/path, edge scans, recursion, order, predicate arguments or user pagination. Existing explicit-ID traversal capabilities are unchanged. Four matrix cases continue to assert `Unsupported` for repeat, range, order and predicate arguments.

The scan uses its separately documented 256 MiB cumulative logical-byte and one-million-work budget, with at most 128 records per storage page. Explicit-ID traversal retains its existing 64 MiB/10,000-work caps. **The five-second request deadline was not increased.** These are logical limits, not process-RSS promises. Session acquisition can still copy a bounded shard view locally on the data node; successful global execution does not imply zero-copy storage. Results remain `Unverified`, and independently pinned shard views are not one atomic global snapshot. One replica per group does not test HA.

## Measurements

Single-host synthetic measurements are not production capacity guarantees. The larger query matrix makes total runtime incomparable to earlier experiments as a speedup or regression.

- Full harness wall time: **284.33 seconds**; smoke: **38.73 seconds**.
- Offline-encoded import through D1/Raft: **22.28 seconds**, 315 batches, batch p50/p95 **61.17/125.58 ms**. This is not ordinary graph mutation throughput.
- Explicit D1 retained-state capacity remains 128 MiB per shard, upgraded from 64 MiB as in prior runs.
- Logical database bytes: **830,780,800** (792.3 MiB).
- Data-process end-of-run RSS/HWM: **828.4, 845.1, 829.2 MiB**; harness RSS/HWM: **255.4 MiB**. These observations exclude total container/cache accounting.

| Global query | p50 ms | p95 ms | Correct samples |
| --- | ---: | ---: | ---: |
| All-vertex count | 1278.33 | 1367.79 | 7 |
| Person count | 1290.59 | 1315.07 | 7 |
| Active-person count | 1328.35 | 1377.02 | 7 |
| CN-person count | 1467.29 | 1488.10 | 7 |
| Missing-label count | 1338.53 | 1477.02 | 7 |
| Explicit 300-row prefix | 385.47 | 391.47 | 7 |
| All 996 CN departments | 1419.39 | 1557.52 | 7 |

Global queries including empty-result scans stayed within the unchanged five-second deadline in all recorded samples. Seven samples are a smoke distribution, not a stable production p95 or concurrent-load benchmark.

## Reproduction and validation

Run `scripts/corehr_simulation.sh NEW_OUTPUT_DIRECTORY` from the repository root. The script refuses an existing output path, builds a release node/dev harness, generates both deterministic fixtures and runs the entire matrix. Retain manifests, per-sample JSONL, node logs, source/binary hashes and actual process exit status. A partial log without the final completion record is not a pass.

Final local checks passed formatting, three model tests and all-target Clippy. A fresh full workspace aggregate was not run for this harness-only extension; engine-candidate and eventual harness-head hosted CI are separate evidence. The paired [JSON report](corehr-global-results.json) preserves exact measurements and rejection categories. Raw databases, failed-run artifacts and local process logs are not committed. Earlier performance reports remain unchanged.
