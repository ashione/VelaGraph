# Release milestones and acceptance

Status: planned, not released. Initial owner direction recorded 2026-11-12;
v0.0.5–v0.0.8 and GitHub Pages approved 2026-11-14.
Tracking: [#224](https://github.com/ashione/VelaGraph/issues/224).

Versions start at **v0.0.1**, only after their finite acceptance scope passes.
These are experimental prereleases, not production, HA or state-of-the-art claims.
The sequence is a prioritization aid, not a calendar commitment. Independent work
can proceed in parallel; an umbrella issue is not closed by shipping one slice.

## Verified GitHub milestone registry

Titles and repository milestone numbers below were checked against the live
[GitHub milestones](https://github.com/ashione/VelaGraph/milestones) on 2026-11-15.
These are existing planning objects, not delivered features or release acceptance.
All eight were open with no due date; v0.0.5–v0.0.8 had no assigned issues at this
check. Implementation slices still need explicit tracking and acceptance evidence.

| Milestone title | Repository milestone number |
| --- | --- |
| v0.0.1 — Fixed-topology experimental service | [1](https://github.com/ashione/VelaGraph/milestone/1) |
| v0.0.2 — Sustainable consistency and verified recovery | [2](https://github.com/ashione/VelaGraph/milestone/2) |
| v0.0.3 — Elastic routing and bounded concurrency | [3](https://github.com/ashione/VelaGraph/milestone/3) |
| v0.0.4 — Observable interactive product preview | [4](https://github.com/ashione/VelaGraph/milestone/4) |
| v0.0.5 — Remote-authoritative storage | [5](https://github.com/ashione/VelaGraph/milestone/5) |
| v0.0.6 — Distributed bitemporal graph | [6](https://github.com/ashione/VelaGraph/milestone/6) |
| v0.0.7 — Production operations and compatibility | [7](https://github.com/ashione/VelaGraph/milestone/7) |
| v0.0.8 — Scale and performance qualification | [8](https://github.com/ashione/VelaGraph/milestone/8) |

## v0.0.1 — bounded fixed-topology experimental service

Implementation boundary: [public managed service design](PUBLIC_SERVICE_DESIGN.md)
(shared handlers, protocol parity, delivery ownership and readiness gates).
The [HTTP preview](PUBLIC_HTTP_PREVIEW.md), [typed public gRPC](PUBLIC_GRPC_SERVICE.md)
and [service host](PUBLIC_SERVICE_CLI.md) are staged implementations, not proof of completed clean-startup or release
gates. The finite preview feature/compatibility matrix must be explicit; the current
[candidate inventory](V0_0_1_CAPABILITY_MATRIX.md) records that surface and its
remaining risk dispositions, without declaring this acceptance complete.

Deliver a small usable service before promising elastic operation:

- One reproducible loopback startup and clean-environment write/query examples.
- Public **gRPC and HTTP both support writes and supported standard Gremlin
  queries**, through the same handlers and errors. A read-only RPC demo is not
  enough. [#218](https://github.com/ashione/VelaGraph/issues/218) supplies the
  staged typed protocol migration, including one complete internal read/session
  flow and reusable bounded channels; publish the exact migrated/nonmigrated
  method matrix rather than claim all internal Raft/control RPCs have migrated.
- Preserve existing certified fixed V, supported minimum-version semantics,
  original-ID write recovery and full-result correctness. Automatic latest means
  selecting one eligible consistent view at admission, not refreshing mid-query
  or promising linearizability.
- Test deadline/cancel, resource rejection, unknown write outcomes, cursor owner
  identity and restarts. A restoring or uncertified target graph cannot serve
  queries. Existing guarded offline backup/restore is documented as such, not
  advertised as live global recovery.
- Static topology and one gateway are sufficient. Record capacity/retention and
  supported protocol/storage compatibility, including explicit Unsupported and
  NotReady boundaries. Do not hide weak observed reads behind certified labels.
- Any defect affecting this advertised scope blocks release unless that feature
  is explicitly excluded with a reproducible limitation. In particular,
  [#128](https://github.com/ashione/VelaGraph/issues/128) diagnostics or one green
  run are not proof of root-cause repair.

The first scoped service portion of
[#221](https://github.com/ashione/VelaGraph/issues/221) is needed here; full
multi-gateway balancing is not. #218 spans later migrations as well and stays
open until its complete acceptance is met.

## v0.0.2 — sustainable consistent graph and verified recovery

The next core gate is sustained operation, not a larger constant:

- [#108](https://github.com/ashione/VelaGraph/issues/108),
  [#109](https://github.com/ashione/VelaGraph/issues/109) and
  [#118](https://github.com/ashione/VelaGraph/issues/118): bounded active/retained
  authority, complete streamed recovery and explicit lifetime/retention rules.
  Preserve exact original IDs/Plans and previously promised old views.
- [#110](https://github.com/ashione/VelaGraph/issues/110) and
  [#111](https://github.com/ashione/VelaGraph/issues/111): real managed certified
  same-version full-scale dynamic CoreHR with an independent oracle, also
  exercising generic non-HR adversarial graphs. Observed full-scale CI does not
  satisfy this gate.
- Global checkpoint/restore under
  [#191](https://github.com/ashione/VelaGraph/issues/191): one certified cut,
  complete immutable manifest closure, node/cohort/format validation and explicit
  restore completion before the target graph becomes READY. Cover missing or
  corrupt objects, partial restore, restart and delayed members. Reuse physical
  and object transport foundations, not another backup engine.
- Keep [#193](https://github.com/ashione/VelaGraph/issues/193)'s local offline
  restore guard until separately proven safe live recovery exists. Global backup
  does not automatically satisfy that local issue or authorize rejoin.
  [#124](https://github.com/ashione/VelaGraph/issues/124)'s unresolved purge/install
  prerequisite remains an independent blocker wherever required; this plan
  neither implements nor bypasses it.
- State RPO, durability ACK, supported upgrade/recovery modes and remaining
  failure restrictions. A periodic remote checkpoint is not zero-RPO remote
  write durability, object-store authority or a disposable local cache.

Recovery correctness is not deferred from v0.0.1: that version must safely reject
unsupported recovery states. This phase expands the supported recovery capability.

## v0.0.3 — elastic routing and bounded concurrency

Three linked missing-capability issues:

1. [#221](https://github.com/ashione/VelaGraph/issues/221): ingress balancing,
   verified gateway session/job ownership, readiness-aware selection and bounded
   reusable channels. A continuation must reach its actual owner or fail; random
   routing must not recapture a newer V.
2. [#222](https://github.com/ashione/VelaGraph/issues/222): versioned discovery and
   route convergence; load selection only among replicas eligible for the
   required V; shard migration with baseline/delta catch-up, ownership publication
   and old-owner fencing. Leader routing is not least-loaded arbitrary writes.
3. [#223](https://github.com/ashione/VelaGraph/issues/223): remove accidental
   gateway-wide write serialization only after
   [#127](https://github.com/ashione/VelaGraph/issues/127) attribution, preserving
   conflicts, exact retry, decision order and certified visibility.

Acceptance includes concurrent writes/queries during real-process move/restart,
stale routes, old cursors, lost ACK, admission pressure and independent graph
oracles. Profile identical workload/build/hardware, with p50/p95/p99, throughput
and resource amplification. Separate multi-machine tests are required before
any horizontal-throughput or HA claim. Research established systems before
freezing uncertain designs; no workload-specific fast path.

## v0.0.4 — observable interactive product preview

After service contracts and core gates are stable:

- [#220](https://github.com/ashione/VelaGraph/issues/220): bounded read-only web
  portal for standard Gremlin, graph/table results and properties, using the same
  HTTP handler and pinned-view rules.
- [#122](https://github.com/ashione/VelaGraph/issues/122): actionable query,
  admission, write visibility and restore diagnostics, with bounded metrics,
  traces and logs; no sensitive graph payload logging by default.
- Real browser/service tests for failure, cancellation, pagination and NotReady;
  onboarding and small Rust/Go examples; reproducible operational scenarios.
- A complete production security, HA and compatibility support statement needs
  its own evidence. A portal and dashboards do not make this production-ready.

## v0.0.5 — Remote-authoritative storage

Planned acceptance under [milestone 5](https://github.com/ashione/VelaGraph/milestone/5),
extending [#191](https://github.com/ashione/VelaGraph/issues/191) beyond v0.0.2
checkpoint backup:

- Define remote-durable write ACK and the required commit/log closure; validate
  immutable manifest/CAS publication, writer fencing and bounded cache and
  index-plus-delta reads.
- Recover every acknowledged write after total local-cache loss. Missing or
  corrupt objects or log suffixes fail closed before READY; preserve original
  IDs, fixed views and existing recovery prerequisites, including #124 and #193.
- State measured RPO/RTO and independently validate supported S3, OSS and TOS
  behavior, not only an S3-compatible CI backend. Require exact-source independent
  review and reproducible failure/recovery evidence.

This is not implemented remote authority, a release date or a SOTA claim.

## v0.0.6 — Distributed bitemporal graph

Planned acceptance under [milestone 6](https://github.com/ashione/VelaGraph/milestone/6),
extending [#196](https://github.com/ashione/VelaGraph/issues/196):

- Reuse temporal and managed execution for atomic cross-shard vertex/edge changes
  and traversals at one system version V and valid time T; cover retrospective
  corrections, future-effective changes, deletion semantics, public query
  entrypoints and stable old-view pagination.
- Restore every advertised temporal record/index from one verified checkpoint
  cut before READY. Test concurrent writes, partial apply/Abort, original-ID
  recovery and process restart.
- Use independent two-dimensional CoreHR and non-HR oracles, including full-scale
  dynamic data and bounded history/resource costs. Local temporal tests or managed
  vertex-only support do not satisfy this gate.

Core milestones and recovery prerequisites remain unchanged. This is planned
scope, not a completed distributed temporal feature or a release commitment.

## v0.0.7 — Production operations and compatibility

Planned acceptance under [milestone 7](https://github.com/ashione/VelaGraph/milestone/7):

- Validate real multi-machine node/leader failures, network partitions, sustained
  load and recovery drills with explicit availability, RPO/RTO and residual risks.
- Define and test rolling upgrades, supported mixed protocol/storage versions,
  migrations and feasible rollback without silent corruption or view changes.
  Existing unsupported combinations stay explicit until separately validated.
- Complete scoped authentication/authorization, internal service isolation,
  transport protection, resource/tenant isolation and bounded audit behavior.
  Publish deployment/compatibility and failure matrices; require exact-source
  review and reproducible fault, upgrade and security tests.
- Deliver a complete repository-maintained **GitHub Pages** project site: project
  introduction, verified getting started, documentation navigation, supported
  versions and known limitations, contributor guidance and release navigation.
  Reuse repository docs as the maintained source, with reproducible automatic
  build/deployment and link, mobile-layout and published navigation/content checks
  against the verified release. Clearly distinguish released and planned features.

Reuse v0.0.3 elasticity and v0.0.4 observability. The Pages site is a separate
required deliverable from #220's interactive graph-query Portal and #122's
operational observability; neither substitutes for it. The site is planned, not
built or deployed by this plan. This milestone is not automatic production/HA or
SOTA certification. Necessary security gates apply before any public deployment,
not only when reaching v0.0.7.

## v0.0.8 — Scale and performance qualification

Planned evidence/acceptance under
[milestone 8](https://github.com/ashione/VelaGraph/milestone/8):

- Freeze exact source, hardware/network, dataset generator, query shapes,
  concurrency, consistency and write-durability settings. Validate the roadmap
  target of 10 million vertices and 50 million edges with independent correctness
  oracles; the target is not an achieved benchmark.
- Measure real multi-machine scaling, mixed concurrent reads/writes, skew and
  high-degree graphs, cold/warm caches and sustained historical workloads. Report
  throughput, p50/p95/p99, CPU/RSS, network/storage amplification, remote request
  cost and failure behavior.
- Compare mature systems only under equivalent semantics and documented
  configurations. Unsupported operations are not successful samples. Reuse
  #110/#111/#127 evidence without replacing core correctness gates or introducing
  CoreHR-specific optimizations.

Performance regression checks apply throughout all milestones. This stage
consolidates reproducible scale qualification and honest limitations; it promises
neither a benchmark result nor a release date or SOTA status.

## Parallel and later work

- [#196](https://github.com/ashione/VelaGraph/issues/196) bitemporal V/T continues
  in reviewed slices alongside core work. Only implemented and tested operations
  can be listed in a release; the complete distributed gate is v0.0.6.
  Any advertised temporal path must participate in
  the claimed recovery boundary; a third DB cannot be silently omitted.
- #191 object-store authority, remote-durable logs, disposable local caches and
  immutable indexes are scoped to v0.0.5, beyond v0.0.2 checkpoint backup. Do not
  bundle a storage rewrite into the first release. S3/OSS/TOS behavior requires provider-specific conformance, not just
  one S3-compatible CI backend.
- [#215](https://github.com/ashione/VelaGraph/issues/215) ontology remains deferred
  until basic functionality is ready; no new implementation is started here.
- Full internal gRPC unification proceeds via #218's reviewed slices. Remove
  replaced duplicate paths after differential validation, without permanent
  compatibility scaffolding or changes to persistent record namespaces.

## Release checklist for every version

1. Freeze a finite feature/support matrix and identify prerequisites; move scope
   explicitly rather than marking an unfinished umbrella issue complete.
   Substantial designs require complete proposals and design documents under
   [the contribution policy](../CONTRIBUTING.md#proposals-and-design-records),
   linked to tracking issues, implementation PRs and milestone/Project items.
   This release plan is not a substitute for those design records.
2. Independently review the exact merged commit. Run applicable normal/targeted
   CI and the complete release lifecycle/CoreHR gates required by that scope,
   with source/artifact identity and passed/failed/not-run recorded. Docs PRs
   should not trigger every expensive release acceptance run.
3. Verify clean-environment quickstart, protocol/storage upgrade notes, supported
   restore instructions, license/provenance and known issues.
4. Only then create the version tag and GitHub prerelease for that exact commit,
   attach validated binaries/checksums if provided, and publish factual notes.
   A merged planning PR or an existing test is not release acceptance.

Keep this plan, #224 and GitHub milestone metadata aligned, using the verified
repository milestone numbers above. Assign implementation slices to their relevant
release and keep Project tracking current without marking unfinished umbrella
criteria complete. Planning metadata is not release or implementation acceptance.
