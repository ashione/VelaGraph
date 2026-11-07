# CoreHR native hierarchy validation

This is a new experiment, not a replacement for the historical [global scan](COREHR_GLOBAL_RESULTS.md), [dedup](COREHR_DEDUP_RESULTS.md) or [incoming](COREHR_INCOMING_RESULTS.md) reports. All records are synthetic.

## Tested implementation

The frozen engine changes are the 14-file PR #62 candidate, original head `a74df84928fc5557ef192798e6bc83d94eaca42c`, tree `3af218687f8b1851f4251767786e0c49af9c0069`, based on main `c2d01855`. The independent harness includes the global-oracle upgrade and the model/runner changes recorded here. PR #62's later integration head `e88a7f6b5b46cbed78cd52cafeae4b6961a5c67f` preserves these production-file hashes; the measured binary is not retroactively relabeled as that merge. Source and archived binary hashes are in the paired JSON report.

Cloud Linux on AMD EPYC 9V74, nine visible CPUs and approximately 9.7 GiB system memory, overlay storage. Release graph-node, dev harness, `TOKIO_WORKER_THREADS=2`; one metadata process, three data processes, one replica per group, coordinator in the harness and plaintext loopback. No other compiler or benchmark ran during final measurements.

## Complete correctness result

The full dataset remains 200,000 people, 5,000 departments and 454,998 edges: 199,999 reporting, 4,999 department-parent and 250,000 membership edges, including 303,483 cross-partition edges. Every one of its **1,569,994 KV records** was read back and compared byte for byte.

Both smoke (2,000 people/50 departments) and full runs completed **78 cases**:

- 68 supported cases × seven samples = **476 exact oracle matches**.
- Five `Unsupported`, one `InvalidArgument`, and four `ResourceExhausted` results, each classified exactly.
- Zero unexpected failures, process exit zero, final `complete:true`.

The 18 new positive cases are native Gremlin requests executed by the server coordinator. They are not the older per-hop application loops. Both `repeat(...).emit().times(n)` and `repeat(...).times(n).emit()` are checked. The original application-loop checks remain separate regression evidence.

| Native full-fixture result | Exact expectation |
| --- | --- |
| Person 200000, emit through requested depth 32 | 25000, 3125, 391, 49, 6, 1 |
| Department 205000, emit through requested depth 32 | 201000, 200200, 200040, 200008, 200002, 200001 |
| Exact sixth level, either hierarchy | One root vertex |
| Beyond the root or exact level 32 | Empty frontier/count zero |
| Person 1, exact second descendant level | 64 ordered people |
| Person 1, emit first two levels | 72 ordered people, 69 active |
| Person 1, emit first three levels/count | 584 |
| Department 200002, exact second level | 25 |
| Same department, emit first two levels/count | 30 |
| Repeated person start, ancestor emit/count | Bag count 12 |
| Same repeated start followed by dedup/path | Six retained qualified paths |

Full vertex-and-edge paths are checked, including owner partition and every edge ID. Emit order is breadth-first by level, then input/frontier and edge order; starting vertices are not emitted as level zero. A separate Python reconstruction builds tree relationships and edge IDs directly from numeric formulas and checks all 18 positive cases, ordered paths and multiplicities without reading database query results. Four model tests include partition routing, level order, path lengths, duplicate bags and existing global/dedup invariants.

The actual full trees have six ancestor levels for the chosen leaves; `times(32)` is a bounded request that terminates when the frontier empties, not proof of a 32-level dataset. Smoke leaves have four person levels and three department levels. A preliminary model-test assumption that both smoke trees exceeded three levels was corrected before process runs; its failed unit log is retained. The synthetic CoreHR trees are acyclic, so this full workload is not evidence for arbitrary cyclic organizations; the engine's separate process tests cover cycles and self-loops.

## Explicit rejection and execution boundaries

The four resource rejections are intentional safety tests:

1. Emitting all descendants of person 1 through 32 levels and counting exceeds the unchanged traversal budget.
2. Adding a later `limit(1)` to that broad traversal still fails; no partial prefix or count is accepted.
3. `times(33)` fails depth admission.
4. An ordinary hop plus `times(32)` also fails the combined depth-32 bound, even if the ordinary hop would produce no row.

Thus this result does **not** claim a single request can enumerate or count the entire broad reporting tree under the CEO. Bounded narrower descendant queries pass, while broad closure is explicitly rejected. Budget failures are not included as successful latency samples.

`times(0)` is explicitly Unsupported. The older unbounded repeat-without-times case, range pagination, order and predicate arguments remain Unsupported; `limit(0)` remains InvalidArgument. Until, pre-emit, nested/multiple repeats, arbitrary bodies, global/edge/scalar starts and general recursive graph query support are outside this slice. See [the hierarchy contract](SERVER_HIERARCHY.md).

The existing **64 MiB logical-byte, 10,000-work and five-second** explicit-ID bounds are unchanged. Repeated expansion and retained emitted paths are charged cumulatively; later count/limit/dedup cannot refund them. One native request uses the coordinator's retained shard sessions for every round, not fresh client reads. These independently pinned views remain `Unverified`, not a globally atomic snapshot; one replica per group does not establish HA.

The smoke client matrix covers all 78 cases: 41 correct results and 37 expected rejections, including every new native-repeat case. The full broad-client snapshot still rejects its existing capacity limit before query execution. No full-scale client query or client-side repeat support is claimed.

## Measurements

The whole full harness took **344.02 seconds**, smoke **46.20 seconds**. The changed matrix prevents comparing total wall time to earlier reports as a performance regression or speedup.

- Offline-encoded D1/Raft import: **21.43 seconds**, 315 batches; batch p50/p95 **59.42/116.54 ms**. This is not ordinary graph mutation throughput.
- Explicit D1 retained-state capacity remains 128 MiB per shard, upgraded from 64 MiB as in earlier runs.
- Logical database bytes: **830,780,926** (792.3 MiB).
- End-of-run data-process RSS/HWM: **828.9, 845.4, 827.4 MiB**; harness: **219.9 MiB**. These are process observations, not total container/cache accounting.

| Native query | p50 ms | p95 ms | Correct samples |
| --- | ---: | ---: | ---: |
| Person ancestor emit, requested depth 32 | 416.49 | 437.26 | 7 |
| Department ancestor paths, requested depth 32 | 376.62 | 417.58 | 7 |
| Person exact second descendant level | 456.74 | 475.26 | 7 |
| Person three-level emitted count, 584 | 738.01 | 769.55 | 7 |
| Department two-level emitted count, 30 | 982.03 | 1016.58 | 7 |
| Duplicate-start ancestor dedup paths | 473.20 | 517.58 | 7 |

These are seven-sample single-host synthetic observations, not stable production p95s or concurrent-load capacity guarantees.

## Reproduction and evidence

Run `scripts/corehr_simulation.sh NEW_OUTPUT_DIRECTORY` from the repository root. The script refuses existing output, builds the release node/dev harness and retains both fixtures and process evidence. Require the complete manifest matrix, per-sample results, all-KV verification, real exit status and final completion record; partial logs are not a pass.

Final local checks passed formatting, four model tests and all-target Clippy. No fresh full workspace aggregate is claimed for the harness extension. Engine and eventual harness-head hosted CI are separate evidence. The [JSON report](corehr-hierarchy-results.json) records exact provenance, results, resource data and rejection categories. Raw databases/logs remain local evidence and are not committed. All earlier reports are preserved unchanged.
