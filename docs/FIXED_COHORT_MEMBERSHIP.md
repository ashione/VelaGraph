# Fixed cohort membership and execution width

The managed fixed-V data-local gateway and its opt-in query jobs accept 1..=64
fixed data members, the existing `transaction_decisions::MAX_PARTICIPANTS`
protocol ceiling. This is a bounded vertical slice, not unlimited membership,
online membership changes, or a larger Raft voting group.

## Design references

CockroachDB separates ranges (each its own Raft group) from the cluster's node
population: [range/Raft design](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md).
Trino separately controls [task concurrency](https://trino.io/docs/current/admin/properties-task.html)
and [query memory](https://trino.io/docs/current/admin/properties-resource-management.html).
We follow that separation of dimensions, not their implementation or performance:
fixed data membership, per-query execution width, and resource admission are
separate. None of these references establish correctness for VelaGraph.

## Layer audit

- **Trust:** complete cohort/profile/authority/target and endpoint/group/node
  equality remain mandatory for every member, including members after index 7.
  Identity tokens and terminal proofs are not cryptographic authentication.
  Transport remains plaintext within one trusted deployment.
- **Protocol/storage:** Cohort and participant decoders already bound vectors at
  64. Existing journal/checkpoint/snapshot formats support that bound. No persisted
  encoding, profile, request format or journal limit changes. Invalid 65-member
  inputs remain rejected before an unbounded vector can be retained.
- **Consensus:** data members are separate shard groups, not 64 replicas in one
  group. No election, quorum, replication or membership-transition code changes.
- **Query routing:** fixed execution uses the same ownership checks with the
  protocol's 64-route/64-shard bound. The public observed StaticCatalog retains
  its 32-partition/8-shard contract; legacy routing/global scans are unchanged.
- **Scheduling:** width remains 1..=8 (default 3), at most one fragment per session
  at once. A wider cohort is visited in bounded waves; ordered bag merge, failure
  propagation, capture-dimension reservations and serial fallback are unchanged.
- **Certification:** every original member participates in admission, full-frame
  preflight and capture. The gateway still opens members serially, subtracting
  actual dimensions from the same aggregate 10,000-row / 2 MiB / 100,000-operation
  budget. Membership does not multiply the available capture credit.
- **Resources:** two coordinator executions, existing per-node eight-session and
  eight-worker pools, fixed result/control budgets, 16 MiB graph frames, 16 MiB
  journal budget, deadlines, leases and cleanup are unchanged. Full cohort
  identities/proofs are repeated per descriptor: metadata and certification work
  grow with membership, bounded here by 64, and are not a process RSS guarantee.
  A legal 64-member configuration may still fail capacity or deadline admission.

## Compatibility and operational boundary

Deploy upgraded gateways and graph endpoints together before using more than
eight fixed data members. An old graph endpoint rejects the new-sized cohort;
there is no negotiated downgrade, changed V, replica substitution or partial
success. The persisted cohort is still immutable; this change does not add a
ninth member to an existing bound eight-member deployment. Provision a new empty
cohort under the existing protocol, rather than editing persisted identities.

Dynamic managed writes retain their original protocol, full terminal evidence
and visibility rules. Original V must still produce its original result after
later writes or member restart. A missing required member fails admission, even
if a query appears not to need its rows. Adjacency completeness remains Unverified.
No history GC, durable purge fence, compaction bypass, new storage layout,
production authentication or query syntax is introduced.

## Acceptance and next boundary

`cohort_membership` runs nine independent durable data-node OS processes plus
one independent coordinator, on loopback with one replica per shard. It uses
normal managed writes and graph TCP endpoints, not an imported static fixture.
It checks duplicate-root ordered paths spanning all nine members, widths 1 and 8,
old-V results after a subsequent write, latest-V visibility, query-job admission
and result replay, failure of a missing ninth member, and historical results after
that member reopens. This is functional acceptance, not multi-host HA or a
throughput benchmark. The fixture explicitly uses the existing 60-second gateway
request/deployment maximum because dynamic writes retain serial capture/decision
work. Member catch-up now uses [bounded fanout](BOUNDED_COHORT_FANOUT.md).
An earlier 15-second fixture failed with Unknown during its 16th managed write,
before query execution. No production deadline, lease or per-node budget is raised;
more members do not imply a latency guarantee. Unit tests cover 9 and 64 structural admissions, 65-member
serde rejection, late-route identity/endpoint failures, and unchanged observed
and execution-width limits.

Beyond 64 requires a separately versioned protocol/capacity review rather than
raising this constant: bound repeated proof/identity encoding, certify/pin wider
cohorts without multiplying memory, retain fail-closed partial acquisition cleanup,
and validate multi-host resource/failure behavior. Online repartitioning also
requires an ownership/membership transition design. These remain future work.

The nine-data-process acceptance passed in 192.74 seconds, with four focused
membership boundary tests also passing. [Machine-readable evidence](benchmarks/fixed-cohort-membership.json)
records source/binary/log hashes and the earlier short-deadline failure.
