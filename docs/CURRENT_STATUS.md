# Current capability and work index

Reviewed on **2026-11-13** with main baseline
[`045debf`](https://github.com/ashione/VelaGraph/commit/045debfbd5ccce8ae7331fdac02fd886e251037d)
and the #256/#258 default-storage RPC migration in this change.
This is an API and implementation index, not a new benchmark or an assertion that
all milestone exit criteria have passed. Linked PRs/issues show later changes;
the remaining outcomes below must not be inferred from a merged dependency slice.

## Choose an entrypoint

| Path | Entry and contract | Consistency and scope |
| --- | --- | --- |
| Local v1 kernel | `GraphEngine`; [K3](TRANSACTIONS.md), [Gremlin v0](QUERY_SPEC.md) | One partition/process; snapshot reads and explicit single-partition atomic transactions. The legacy standalone SQL engine is removed ([migration](SQL_REMOVAL.md)). |
| Replicated/routed v1 | `GraphRecordStore`, `ReplicatedStorageNode`, `RemoteStorageNode`, `RouteResolver`; [replicated](REPLICATED_GRAPH.md), [remote routing](REMOTE_ROUTING.md) | Per-shard leader-barrier views and exact conditional retry identities. Static placement, not cross-shard query snapshot isolation. |
| Observed v2/routing-v3 | `ExperimentalReadSession`, server query/distributed query APIs; [v2](EXPERIMENTAL_CROSS_EDGES.md), [server queries](REMOTE_QUERY.md), [distributed queries](DISTRIBUTED_QUERY.md) | Qualified identities, per-shard snapshots, Unverified completeness. Cross-shard mutation batches are staged and non-atomic. Query shapes differ by entrypoint. |
| Managed fixed cohort | `managed_graph`, `managed_gateway::Gateway`; [transactions](MANAGED_GRAPH_TRANSACTIONS.md), [gateway](MANAGED_GRAPH_GATEWAY.md), [fixed-V traversals](MANAGED_FIXED_TRAVERSALS.md) | Explicit empty-baseline cohort and complete member participation; typed coordinated writes, durable external-ID/original-Plan recovery and certified same-V sessions. Local or fixed remote nodes. No general catalog certification or automatic failover. |

The older `ReadConsistency::GlobalCommittedSnapshot` still returns Unsupported
for the catalog/distributed query and job APIs. Managed sessions are a separate
contract, not a fallback or a relabelling of observed results. Certified version
consistency also does not certify complete adjacency against arbitrary trusted
low-level KV writers: managed QueryData retains Unverified completeness.

All query paths use the official Gremlin grammar, but their accepted subsets are
not interchangeable. Unsupported steps/compositions and internal resource limits
fail explicitly. Public graph mutations here are typed APIs, not Gremlin scripts.

### Query and write additions already merged

- Observed server-coordinated scans support ordered filters/limits, qualified
  element dedup and selected-property maps. Scan-to-traversal supports bounded
  out/in/repeat and terminal values/count/valueMap within its contract. Scan-only
  `values(key)` requires an explicit preceding vertex limit. See
  [global scan](SERVER_GLOBAL_SCAN.md) and [composition](GLOBAL_VERTEX_TRAVERSAL.md).
- Observed hierarchy [count](HIERARCHY_COUNT_JOBS.md),
  [identity pages](HIERARCHY_RESULT_PAGES.md) and
  [property pages](HIERARCHY_PROPERTY_PAGES.md) are bounded job APIs, not arbitrary
  query pagination and not managed certified-V execution.
- Shared routing-v3 and managed Traverse/TraversePage support qualified vertex/edge
  `id()` ([#139](https://github.com/ashione/VelaGraph/pull/139)) and no-argument
  element `dedup()` ([#142](https://github.com/ashione/VelaGraph/pull/142)). Dedup
  preserves the first path and step order. The shared executor also supports
  scalar dedup after values/label/id, retaining the first equal value and source;
  path dedup and modulators reject. See the
  [scalar contract](MANAGED_FIXED_TRAVERSALS.md#scalar-deduplication).
  Bounded `range(low, high)` selects positions on vertex/edge/scalar streams at
  the same V, preserving ordered bags and qualified provenance. This is not
  source-scan pushdown or a capacity expansion.
  Standard `simplePath()`/`cyclicPath()` filter actual visited element identities,
  preserving qualified bags and legacy provenance paths at the same V
  ([contract](MANAGED_FIXED_TRAVERSALS.md#bounded-simple-and-cyclic-path-filters)).
  Standard `is(literal)` also filters scalar streams in place with strict typed
  equality, preserving ordered bags and source provenance; predicates remain
  rejected ([contract](MANAGED_FIXED_TRAVERSALS.md#strict-scalar-filtering)).
  The same shared executor supplies single-partition V2Observed server queries.
  These additions do not change legacy v1/v2 or the original smaller managed Read;
  each retains its existing subset.
- Managed typed writes support vertex/edge creation, property patches, edge
  deletion and conditional property patches
  ([#144](https://github.com/ashione/VelaGraph/pull/144)). Conditions preserve exact
  type/presence identity and original CAS/Plan on retry. Vertex deletion/cascades,
  endpoint/label mutation and arbitrary application transactions remain unsupported.

The original managed Traverse/TraversePage APIs first materialize a cohort snapshot, then execute
locally over it. They support one bounded native out/in repeat with optional post-body emit and
combined depth at most 32, within unchanged cumulative limits. They do **not**
support data-local distributed fragments or hierarchy jobs. Terminal selected
vertex valueMap is available under the same fixed-V/session limits. Capture is bounded to 10,000 rows, 2 MiB copied
payload and 100,000 replay operations. A retained complete result has at most
10,000 rows/1 MiB serialized bytes; paging does not increase those bounds. Consult
[the full contract](MANAGED_FIXED_TRAVERSALS.md) for defaults, deadlines and
additional cumulative work/memory limits. These are ceilings, not capacity SLAs.

### Data-local execution and public service now available

The separate [data-local gateway](MANAGED_LOCAL_GATEWAY.md) keeps graph data on
participants, executes a bounded supported subset at certified V and merges
ordered fragments with 1–8 shard lanes. [Renewable jobs](MANAGED_QUERY_JOBS.md)
are a separate lifecycle. Neither makes the original materialized Traverse API
data-local or completes full-scale managed CoreHR acceptance.

The [graph-service CLI](PUBLIC_SERVICE_CLI.md) hosts HTTP `/preview1` and typed
public gRPC over shared handlers/budgets. Its internal managed and graph routes
are gRPC-only. This is a loopback development preview requiring an already
activated fixed cohort, not cluster bootstrap, production serving or global
restore. Graph channel connection is not typed-service readiness; an incompatible
graph endpoint can fail its first query even after a startup announcement.

A separate [managed temporal vertex slice](TEMPORAL_STORE.md#managed-state-machine-vertex-slice-196)
uses existing member state machines and fixed V/T point reads. It does not import
the standalone temporal database or expose temporal edges/public temporal queries.

## Storage and protocol boundaries

Merged dependency slices:

| Slice | Delivered boundary | What it does not remove |
| --- | --- | --- |
| [S1 archive reads](AUTHORITATIVE_EVENT_LEDGER.md), [#107](https://github.com/ashione/VelaGraph/pull/107) | Actual coordinator LookupRequest/HistoryPage retain bounded target/page output; obsolete bound request index retired after strict migration validation. | Complete raw verification and history-sized I/O. |
| [S2 coordinator checkpoint](COORDINATOR_ACTIVE_CHECKPOINT.md), [#112](https://github.com/ashione/VelaGraph/pull/112) | Bound durable coordinator resident/ordinary apply-candidate state becomes active checkpoint, disk v16. | Original history, unresolved intervals, full raw closure and recovery/snapshot materialization. |
| [S3 member checkpoint](MEMBER_ACTIVE_CHECKPOINT.md), [#113](https://github.com/ashione/VelaGraph/pull/113) | Bound durable member uses shared active state and retained raw projections, disk v17. | Participant intents, D1 retry/data history, exact old IDs/Plans/V or independent quotas. |
| [S4 retained control](RETAINED_GATEWAY_CONTROL.md), [#117](https://github.com/ashione/VelaGraph/pull/117) | Ordinary managed execute/status/retry/abort and Prepare use retained target/cut evidence, internal RPC v6. | Explicit full-history/terminal-proof operations, cold replay, complete SnapshotV1 or lifetime bounds. |

The merged [HistoryStatus change](HISTORY_STATUS_VALIDATION.md)
([#146](https://github.com/ashione/VelaGraph/pull/146), closing #145) derives active
durable member visibility identity, history cursor and applied watermark from
one verified publication cut per RPC. Each new call still verifies raw/terminal
authority; no cross-request cache or capacity increase is added. Removing one
duplicate scan does not remove O(history) work or establish an end-to-end
Submit/lifecycle speedup. Legacy/in-memory fallback remains separately checked.

Observed coordinator queries and hierarchy jobs have typed gRPC ingress (#248),
selected by `serve-grpc` plus `--grpc` clients; shared observed execution/Jobs
contracts remain unchanged. Legacy coordinator ingress remains retirement work,
not a fixed/managed consistency endpoint. See [transport](INTERNAL_GRPC_QUERIES.md).

The public graph-service host now initializes listener-free managed `Service`
state: only configured HTTP/gRPC listeners are opened. Expiration and shutdown/drop
cleanup remain shared; explicitly selected library `Gateway` TCP entrypoints are
still pending #218 cleanup. See [service host](PUBLIC_SERVICE_CLI.md).

Single-shard Probe/Metadata/Write also have an explicit
[typed RPC service and CLI](SHARD_OPERATIONS_GRPC.md). Catalog/routing adapters now use typed snapshots/operations by default;
graph-node --graph is a single typed listener and --grpc-snapshot is removed.
Per-read Metadata uses a [bounded stream](METADATA_STREAM.md), retaining every
applied-epoch check; Probe/Write stay unary. Original default-debug regressions
and resource/freshness evidence remain the #258 acceptance gate, not a changed
test profile or a loopback p50 improvement alone.
Explicit library legacy fixtures and coordinator/gateway/Unix entrypoints remain
separate retirement work; this does not complete the all-RPC inventory.


The retained legacy managed TCP protocol uses **RPC v6** and legacy gateway
framing **v1**. Typed managed gRPC has its own versioned service/Context; public
gRPC, HTTP preview and durable formats are separate version namespaces. See
[internal managed RPC](INTERNAL_GRPC_MANAGED.md),
[Raft gRPC](RAFT_GRPC.md) and [public gRPC](PUBLIC_GRPC_SERVICE.md).
Raft peer traffic is now typed gRPC (no mixed old/new Raft wire cluster support);
driver/control is now a separate [typed gRPC service](CONTROL_GRPC.md); remaining
legacy/Unix compatibility paths remain in the
[communication inventory](INTERNAL_GRPC_MANAGED.md#remaining-communication-inventory). Bound durable
coordinator/member metadata is **v16/v17**. Historical v5/v8/v9/v10/v11–v15
references in foundational documents describe actual legacy modes/migration
inputs and earlier slices, not permission to run mixed-version peers or downgrade.
Raw/terminal formats and history/proof versions remain profile-specific. Refer
to each migration contract before changing binaries or importing state.

`LegacyCountV1` remains the default and retains 256 external requests and 1,024
transactions. Only new empty, unbound cohorts may select `ByteBound16MiBV1`, which
replaces count gates with byte-derived structural guards. Both keep the **16 MiB
reserved canonical history budget**, 16 MiB participant intent budget, separate
D1 4,096 retained-commit slots/byte tiers, and request/Plan/query limits. Closing
holes or advancing a checkpoint refunds no historical capacity. There is no GC,
ID expiry, epoch reset, online profile conversion or unlimited write lifetime.

Cold open, complete SnapshotV1 build/install and remaining full-history/proof
operations are not bounded independently of retained history. Current snapshots
are fully materialized, not file-backed streams. The purge/install safety work
[#124](https://github.com/ashione/VelaGraph/issues/124) and streaming delivery
[#118](https://github.com/ashione/VelaGraph/issues/118) remain open and are **not
implemented or released by this documentation update**. Active checkpoints are
not the future remote disaster-recovery backup plan in [D6a](ROADMAP.md#d6a-远程-checkpoint-与灾难恢复未来设计尚未实现).

## Open work and dependencies

| Work | Remaining outcome |
| --- | --- |
| [#108](https://github.com/ashione/VelaGraph/issues/108), [#109](https://github.com/ashione/VelaGraph/issues/109) | Remaining full-history paths, bounded cold recovery/snapshot closure and any separately reviewed finite-lifecycle expansion. S1–S4 are merged; their parent work is not complete. |
| [#124](https://github.com/ashione/VelaGraph/issues/124) → [#118](https://github.com/ashione/VelaGraph/issues/118) | Purge/install publication safety prerequisite, then owned file-backed snapshot delivery and safe spool accounting. Neither means bounded install or a lifted 16 MiB budget. |
| [#110](https://github.com/ashione/VelaGraph/issues/110) → [#111](https://github.com/ashione/VelaGraph/issues/111) | Data-local certified-V query capacity, then full 200,000-person/5,000-department managed dynamic consistency with an independent oracle and recovery evidence. #111 also depends on #109. |
| [#127](https://github.com/ashione/VelaGraph/issues/127) | Gateway phase timing is merged in #133; further lifecycle/resource attribution, remote subphases and controlled overhead evidence remain. [#143](https://github.com/ashione/VelaGraph/pull/143) is merged; its diagnostics do not complete product observability. |
| [#128](https://github.com/ashione/VelaGraph/issues/128) | Bounded paged-snapshot failure diagnostics landed in #131. The original intermittent failure's cause remains unproven; a green rerun alone is not a root-cause fix. |
| [#122](https://github.com/ashione/VelaGraph/issues/122) | Low-priority product observability/tuning/operations plan, separate from test diagnostics. |

Automatic routing convergence, online migration, production transport security,
full D4/D5 acceptance and multi-machine availability/performance remain separate
milestones. [Physical checkpoints](PHYSICAL_CHECKPOINT.md) and
[object-store transport](OBJECT_STORE_CHECKPOINT_API.md) are implemented narrow
slices: restoration remains guarded offline-only, not global checkpoint recovery,
live rejoin or object-store-authoritative storage. Neither a merged narrow slice nor a completed
same-host functional fixture closes them.

## Verification and source map

| Contract | Source and representative tests |
| --- | --- |
| Fixed cohort/common V | [cohort_reads.rs](../crates/d2-raft/src/cohort_reads.rs), [versioned_reads.rs](../crates/d2-raft/src/versioned_reads.rs), [managed_process.rs](../crates/replicated-graph/tests/managed_process.rs) |
| Gateway envelope, mutations, bounds | [managed_gateway.rs](../crates/replicated-graph/src/managed_gateway.rs), [managed_graph.rs](../crates/replicated-graph/src/managed_graph.rs), [conditional tests](../crates/replicated-graph/src/managed_gateway_condition_tests.rs) |
| Active state, exact raw authority, disk versions | [coordinator_checkpoint.rs](../crates/d2-raft/src/coordinator_checkpoint.rs), [member_apply.rs](../crates/d2-raft/src/member_apply.rs), [state_codec.rs](../crates/d2-raft/src/state_codec.rs), [member checkpoint tests](../crates/d2-raft/src/member_checkpoint_tests.rs) |
| Capacity and retained control | [capacity_policy.rs](../crates/d2-raft/src/capacity_policy.rs), [managed_rpc.rs](../crates/d2-raft/src/managed_rpc.rs), [retained RPC tests](../crates/d2-raft/src/retained_rpc_tests.rs) |
| Observed versus managed queries | [query specification](QUERY_SPEC.md), [remote query tests](../crates/replicated-graph/tests/remote_query.rs), [distributed query tests](../crates/replicated-graph/tests/distributed_query.rs), [managed process tests](../crates/replicated-graph/tests/managed_process.rs) |

The [ordinary CI](../.github/workflows/ci.yml) runs fmt, workspace tests and strict
all-target Clippy. The [CoreHR workflow](COREHR_CI.md) separately requires the
full **observed** scenario and a fresh **managed 1,025-write release lifecycle**
with original-ID/Plan, old-V, restart and snapshot evidence. The long lifecycle
is ignored in ordinary debug tests, intentionally required through the release
gate. The small managed process tests are additional functional coverage, not a
substitute for either gate or full-scale managed consistency.

Historical `*_RESULTS.md`, performance reports, `docs/benchmarks/` and
`docs/evidence/` retain their original source/profile/environment context. Do not
rewrite their numbers as current measurements or transfer a passing old artifact
to a different tree. Record passed, failed and not-run checks separately. Missing,
partial, ignored or stale lifecycle evidence is not a successful acceptance run.

## Atomic managed relationship replacement

[ReplaceEdge](MANAGED_EDGE_REPLACEMENT.md) composes old-edge deletion and a new
qualified edge creation into one durable managed Plan across at most four shards.
The shared participant assembler preserves the old tombstone transaction ID,
original per-shard CAS, six operations and unchanged bounds. Fixed-V queries
remain before-or-after while observed reads retain their existing limitations.
No in-place endpoint mutation, vertex cascade, general transaction program or
capacity/recovery claim is added. Native overlap/conflict tests and independent
process gateway/reopen tests are listed in the contract.
