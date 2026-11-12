# Release milestones and acceptance

Status: planned, not released. Owner direction recorded 2026-11-12.
Tracking: [#224](https://github.com/ashione/VelaGraph/issues/224).

Versions start at **v0.0.1**, only after their finite acceptance scope passes.
These are experimental prereleases, not production, HA or state-of-the-art claims.
The sequence is a prioritization aid, not a calendar commitment. Independent work
can proceed in parallel; an umbrella issue is not closed by shipping one slice.

## v0.0.1 — bounded fixed-topology experimental service

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

## Parallel and later work

- [#196](https://github.com/ashione/VelaGraph/issues/196) bitemporal V/T continues
  in reviewed slices alongside core work. Only implemented and tested operations
  can be listed in a release. Any advertised temporal path must participate in
  the claimed recovery boundary; a third DB cannot be silently omitted.
- #191 object-store authority, remote-durable logs, disposable local caches and
  immutable indexes remain a later separately scoped architecture milestone,
  beyond v0.0.2 checkpoint backup. Do not bundle a storage rewrite into the first
  release. S3/OSS/TOS behavior requires provider-specific conformance, not just
  one S3-compatible CI backend.
- [#215](https://github.com/ashione/VelaGraph/issues/215) ontology remains deferred
  until basic functionality is ready; no new implementation is started here.
- Full internal gRPC unification proceeds via #218's reviewed slices. Remove
  replaced duplicate paths after differential validation, without permanent
  compatibility scaffolding or changes to persistent record namespaces.

## Release checklist for every version

1. Freeze a finite feature/support matrix and identify prerequisites; move scope
   explicitly rather than marking an unfinished umbrella issue complete.
2. Independently review the exact merged commit. Run applicable normal/targeted
   CI and the complete release lifecycle/CoreHR gates required by that scope,
   with source/artifact identity and passed/failed/not-run recorded. Docs PRs
   should not trigger every expensive release acceptance run.
3. Verify clean-environment quickstart, protocol/storage upgrade notes, supported
   restore instructions, license/provenance and known issues.
4. Only then create the version tag and GitHub prerelease for that exact commit,
   attach validated binaries/checksums if provided, and publish factual notes.
   A merged planning PR or an existing test is not release acceptance.

GitHub milestone metadata should mirror these scopes, using actual milestone
IDs and assigning implementation issues to their relevant release. The planning
document and #224 are not substitutes for verified milestone creation.
