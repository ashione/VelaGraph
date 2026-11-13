# Architecture evolution: selective lessons from NebulaGraph

Status: accepted planning direction, **not implemented capabilities**. Owner
requested incorporation on 2026-11-13. Baseline checked: main `0cd607d` (#234).
This consolidates existing issues rather than creating a parallel roadmap.
Sources below describe **NebulaGraph Community 3.8**, not Enterprise 5.x.

## Decisions and delivery gates

### 1. Separate responsibilities before separating deployments

Query/Coordinator owns Gremlin planning, distributed execution, result merging
and query lifetime. Graph Storage owns adjacency/index access and local operators;
the lower D1/Raft layer remains graph-agnostic. Meta/Control owns schema, placement,
route epochs, peer identity and recovery/readiness state. Public HTTP and gRPC
remain adapters over shared handlers; private consensus/admin APIs stay private.

Acceptance under #218/#221: document which layer owns each operation and state,
version its RPC boundary, and test identity, deadlines, admission, cancellation
and errors across it. A query node can scale independently only after session
ownership and metadata/routing contracts work; no immediate three-service rewrite
or new repository is required. Uniform RPC is not a distributed optimizer.

### 2. Move bounded graph work to data, not one edge per RPC

Under #110, extend the existing data-local executor rather than replace it:
batch neighbor expansion by shard; push eligible filters, projections and local
aggregates down; merge cross-shard frontiers with bounded fan-out and backpressure.
Maintain operator order, bag multiplicity, qualified identities and pinned V.
Only push a transformation when equivalence is proven; limit/dedup/filter order
and global aggregates cannot be approximated by independent per-shard results.

Acceptance: independent oracle equivalence with pushdown enabled/disabled,
cross-shard loops and parallel edges, high-degree/skewed/non-HR graphs, concurrent
writes and old-view pagination. Record RPC count, bytes, work, peak memory and
p50/p95/p99 on the same build/hardware, separating planner, RPC and storage time.
Avoid moving whole raw adjacency sets to the coordinator when an equivalent
bounded local operator can answer; explicitly requested edge results remain
supported where documented. No unbounded frontier, hidden truncation or
CoreHR-specific fast path. #111 retains the full-scale managed dynamic gate;
observed CoreHR success is not that evidence.

### 3. Treat ingress and ownership routing as different problems

#221 owns gateway selection/session routing; #222 owns partition placement,
leader discovery and eligible replicas. Route refresh is bounded and versioned;
stale-route errors/redirects preserve original deadline and pinned view. Load
balancing may choose only replicas able to serve the required certified V.

A migration gate must prove baseline copy + delta catch-up, an agreed applied
cut, durable old-writer fencing, ownership publication and explicit handling of
old sessions, crashes and rollback. Merely incrementing catalog epoch is not
fencing. Separate data balance, leader balance and query ingress balance.
Reserve protocol space for future partition split/merge rather than copying
Nebula 3.8's fixed partition-count constraint. This is not a claim that online
membership or movement works today. Keep #222's recovery prerequisites.

### 4. Preserve consistent reads while making writes sustainable

Local Raft consensus is not a cross-graph snapshot or transaction protocol.
Under #109/#110/#223, study participant-scoped coordination and indexed MVCC
before replacing fixed-cohort certification or gateway serialization. Latest
resolves one safe V at admission; it does not refresh on each fragment/page.
Commit decisions, applied watermarks and adjacency completeness remain distinct.

Any retention/reclamation proposal must separately account for active query pins,
promised historical V, original request-ID/Plan retry guarantees, unresolved
transactions and backup roots. No active reader does not imply deletion is safe.
Define retention/expired-view errors before reclamation; preserve existing
promises until an explicit versioned contract is approved. #196 adds valid-time
and system-time retention, not an excuse to collapse the two dimensions.
Validate sustained writes, long/paged readers, delayed participants and recovery
across multiple cuts. A larger constant or a green 1025 run is not sustainability.
No GC or durable purge implementation is authorized by this planning document.

### 5. Keep recovery, backup transport and storage authority separate

#191 remains the owner of global checkpoint/restore and the later object-store
architecture. A global manifest must cover a certified cut, all required shards,
metadata and any advertised temporal data. Verify its immutable closure before
READY; missing/corrupt objects, mixed generations and partial restore fail closed.
Preserve #193's offline-only restore guard until live rejoin is independently safe.

Object-store authority additionally requires a remote-durable ACK contract,
manifest publication/CAS, cache validity, log suffix availability, writer fencing
and retention roots. Checkpoint upload alone proves none of these. Reuse the
existing S3-compatible API/SeaweedFS fixture; test S3/OSS/TOS conformance separately.
#124 remains an independent unresolved prerequisite where applicable; this plan
neither publishes its code nor bypasses it.

## Priority and release mapping

1. Now: #218 RPC convergence and removal of replaced paths after differential
   tests. Keep HTTP as the required public adapter. #235/#238 and implementation
   PRs are tracked separately; unmerged work is not the baseline.
2. Core next: #110 operator pushdown and #109 sustainable history/recovery;
   #111 validates managed scale. #222 routing design can proceed in parallel,
   but enabling migration waits for its recovery/fencing gates.
3. Elasticity: #221/#222/#223, including concurrent migration/write/query oracles
   and separate multi-machine performance evidence.
4. Later: #191 object-store authority, #122 product observability and #220 Portal;
   necessary correctness/diagnostic signals are not deferred. #215 ontology stays
   deferred. See [release plan](RELEASE_PLAN.md) / #224; no release is created here.

## Executable plan and dependency order

These are work packages in existing issues, not new milestone objects. A package
may deliver independently reviewed slices; its full acceptance is not inferred
from one merged PR. Existing release definitions remain authoritative.

| Package | Existing owners | Prerequisites / parallel work | Completion evidence |
| --- | --- | --- | --- |
| A. Protocol and layer contracts | #218, #221 | Current service baseline; independent Raft/control/query migrations may run in parallel | Complete method/owner/transport inventory; typed RPC differential tests, identity/deadline/cancel/admission parity, no implicit TCP fallback; remove replaced paths after validation |
| B. General distributed execution | #110, #111 | Use A's shard contract; planning and local operator proof may proceed before all migrations finish | Enabled/disabled pushdown oracle equivalence; bounded frontier and merge; skew/high-degree/non-HR plus concurrent fixed-V queries; measured work/bytes/memory and full managed-scale gate |
| C. Sustainable versions and recovery | #108, #109, #118, #191, #193, #196 | Parallel with B; obey #124 where required; preserve current lifetime/read promises | Multiple checkpoint cuts with original-ID recovery, old-V readers and complete immutable manifest closure; corrupt/partial recovery never READY; separately specify retention and temporal coverage |
| D. Elastic routing and concurrent writes | #221, #222, #223 | Design during A/B/C; enable moves only after C and membership/fencing gates; #127 attribution before removing serialization | Two gateway ownership tests, stale-route/fenced-writer rejection, real move/catch-up/crash rollback with independent oracle; disjoint/conflicting write tests and multi-machine evidence before scaling claims |
| E. Remote authority and product views | #191, #122, #220 | Separate remote-durability ADR and recovery gate for authority; stable public API for Portal; core work stays higher priority | Disposable-cache recovery of acknowledged writes only after remote-authority proof; provider conformance; bounded actionable diagnostics and real browser failure/cancel/NotReady tests |

A supports the scoped v0.0.1 service gate; B/C support v0.0.2, D supports
v0.0.3, and product views in E support v0.0.4. Object-store authority remains a
separately scoped later gate, not implicitly bundled into v0.0.4. Retention/GC
requires its own reviewed contract; this table does not approve deletion or
weaken existing blocked-operation boundaries. Ontology (#215) remains deferred.

## Official references and deliberate non-adoptions

- [Architecture and services](https://docs.nebula-graph.io/3.8.0/1.introduction/3.nebula-graph-architecture/1.architecture-overview/)
  and [Meta](https://docs.nebula-graph.io/3.8.0/1.introduction/3.nebula-graph-architecture/2.meta-service/):
  borrow responsibility boundaries, not an immediate process/repository split.
- [Graph](https://docs.nebula-graph.io/3.8.0/1.introduction/3.nebula-graph-architecture/3.graph-service/)
  and [Storage](https://docs.nebula-graph.io/3.8.0/1.introduction/3.nebula-graph-architecture/4.storage-service/):
  borrow graph-aware batched calls and pushdown, not a new query language or
  graph-aware Raft. RocksDB/Multi-Raft separation is not object-store authority.
- [System design](https://docs.nebula-graph.io/3.8.0/8.service-tuning/3.system-design/):
  do not infer cross-graph isolation from Raft or weaken VelaGraph's certified view.
- [Balance](https://docs.nebula-graph.io/3.8.0/8.service-tuning/load-balance/)
  and [super nodes](https://docs.nebula-graph.io/3.8.0/8.service-tuning/super-node/):
  use skew/movement as generic acceptance workloads, not application-specific
  node splitting or a claim of automatic balance.
- [Official Go client](https://github.com/vesoft-inc/nebula-go): Nebula uses Thrift;
  this comparison does not change VelaGraph's selected gRPC framework.
