# Bounded full-cohort catch-up

A cohort's member count, a Raft group's voting replicas, query lanes and catch-up
lanes are distinct controls. Fixed cohorts admit up to 64 members; this does not
claim 64-member throughput, online resizing or multi-host production readiness.

## Critical path and design

The serial baseline on main `169024d` was measured with 3, 9 and 12 independent
loopback data processes plus one coordinator, one replica per process, twelve
ordinary vertex submissions per configuration. At nine members/write eight,
Submit took 6.33 seconds: full-cohort advancement 4.31 seconds, snapshot 0.90
seconds and decision 0.81 seconds. These small-data measurements identify a
critical path; they are not a large-data scalability or HA qualification.

Full-cohort advancement and status certification use owned waves of at most
three members by default. Deployments may select 1..=4 catch-up lanes independently
of query lanes through `GraphCohort::with_member_parallelism`, or the remote
gateway's `bind_remote_with_member_parallelism` (and diagnostic variant).
`bind_remote_local_queries_with_concurrency` configures query/member limits
independently. Local-handle gateways currently use the default. Width one is the
serial reference. Invalid widths are rejected before contacting nodes.

Each member retains its existing sequence:

1. Observe its own history status.
2. Pin its own coordinator history cut and transfer exact pages in order.
3. Consume successive retained ordinals, retrying an uncertain operation once
   with identical arguments.
4. Compare the complete original terminal proof.

The fixed terminal proof and cohort identity are checked before fanout. No later
wave starts until the preceding wave succeeds. A failed lane is retained while
the already-issued wave drains under the same
outer deadline; its first error in cohort order prevents later waves. Outer
cancellation drops pending client futures and prevents later dispatch. No client
task is spawned or detached.
Already accepted durable operations can finish after cancellation: retry the
original request identity, never infer abort or roll back from a timeout.

The existing 30-second decision and 30-second catch-up budgets remain distinct;
status retains its five-second visibility budget. Waves and retries do not renew
a deadline. Visibility is true only after every original member certifies. A
failure or timeout preserves the authoritative decision with `visible: false`.
The private fanout helper selects errors in cohort order after draining the
issued wave. A deadline can interrupt that drain. Callers expose incomplete
visibility, never a partial certificate.

## Bounds and deliberately serial work

At most W member futures and W current history pages are retained per catch-up.
Each page still admits at most 32 events and 4 MiB canonical event bytes; each
managed RPC frame remains separately capped at 32 MiB. Encoding, decoding and
server replay can hold additional copies: these are logical bounds, not RSS
claims. The gateway retains eight admitted connections and its submission mutex;
only one submission advances at a time per gateway. Status fanout is bounded by
connections times W. The maximum four lanes aligns with the existing four
process-wide outbound RPC
slots; concurrent unrelated requests can still cause bounded overload. Existing
process-wide outbound RPC admission and server
blocking-work admission remain in force. A direct GraphCohort caller still owns
its outer concurrent-request admission.

Participant preparation/decision order is unchanged. Snapshot readiness and
capture, fixed-query member capture, and their exact aggregate remaining
row/byte/work budgets remain serial. Giving each concurrent capture the entire
remaining budget would multiply resource use; equal division would reject
previously valid skewed shards. This change does neither. Session, query-job,
result, lane, protocol and durable-history limits remain unchanged.

## Mature-system references

- CockroachDB [ParallelIO](https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/changefeedccl/parallel_io.go)
  preserves conflict-key order while bounding independently runnable work.
- CockroachDB [DistSender](https://github.com/cockroachdb/cockroach/blob/master/pkg/kv/kvclient/kvcoord/dist_sender.go)
  separately bounds async sends and defines response/error collection policy.
- etcd [Raft storage ordering](https://github.com/etcd-io/raft/blob/main/raft.go)
  and [inflight tracking](https://github.com/etcd-io/raft/blob/main/tracker/inflights.go)
  distinguish ordering and message/byte limits; this is an analogy, not a change
  to VelaGraph's consensus implementation.
- etcd [API uncertainty](https://etcd.io/docs/v3.6/learning/api_guarantees/#operation-completed)
  explains why cancellation cannot establish that a durable operation did not run.
- Trino [exchange controls](https://trino.io/docs/current/admin/properties-exchange.html)
  distinguish concurrency from available shared buffering.

## Measured results

Raw per-write data and executable/source hashes are retained in
[benchmark evidence](benchmarks/bounded-cohort-fanout.json). Each configuration
has twelve ordered submissions on a fresh cohort, with no warmup exclusions.
These medians combine increasing history positions; they are not repeated
steady-state samples or confidence intervals.

- Nine members: width one Submit median 5.785 s and cohort median 3.884 s;
  default width three 3.227 s and 1.515 s respectively.
- Twelve members: width one 7.843 s and 5.746 s; width three 4.221 s and 2.071 s.
- Three members: width one 1.589 s and 0.798 s; width three 1.201 s and 0.335 s.
- Width four: nine members 3.033 s and 1.423 s; twelve members 3.959 s and
  1.768 s. The remaining serial phases prevent interpreting lane count as
  proportional end-to-end speedup.

The exact bounded-admission candidate `35e60ac` was then measured with one
frozen executable across all three widths. Nine-member Submit/cohort medians
were 4.997/3.443 seconds at width one, 2.996/1.379 at width three and
2.935/1.380 at width four. Twelve-member medians were 7.534/5.470,
4.498/2.228 and 4.402/1.905 seconds respectively. Raw samples are retained
separately from the earlier fanout-only data. These remain single-run,
small-data critical-path measurements, not a statistically powered benchmark.

All measured writes required a fully visible Commit. Snapshot and decision costs
remain, and still grow with history. Single-run small-data observations establish
that overlapping independent member work reduces this measured critical path;
they do not establish sustained throughput or performance at the 64-member limit.

## Reproduction and remaining work

Run the ignored `cohort_submission_attribution` test in `cohort_membership` with
`VELAGRAPH_BENCH_MEMBER_WIDTH=1`, `3`, or `4`, exclusively on the same machine.
It reports raw per-submission timings for 3/9/12 members and validates visible
commits. Parent phase spans include children. Concurrent child spans overlap:
never add them together or interpret their sum as wall time.

The normal nine-member process regression covers fixed-V queries, ordered bag
paths, dynamic writes, query jobs, missing members and durable restart. Wave
unit tests cover maximum in-flight work, error stop and cancellation release.
Existing exact-page/ordinal retry and no-archive-fallback tests remain required.

Further work must address history-length CPU cost, serial aggregate-budget
capture and larger graph/history/process scales with independently measured
memory and tail latency. No claim here covers those remaining bottlenecks.

## Bounded admission and contention acceptance

The latency data above describe the first fanout-only candidate, commit
`7f9ffc8`, not the admission follow-up. In an unisolated ordinary process-test
run, `independent_managed_processes_partial_commit_and_durable_recovery` returned
its first known Commit with `visible: false`; 23 tests passed and one failed.
The old-prefix concurrent-write test passed in that run. Preserve this result:
it must not be relabeled as the older pin-expiration failure or as a green suite.
The log proves incomplete certification, not its precise internal cause. Shared
four-slot outbound saturation is the source-backed likely explanation.

The follow-up opts gateway-owned coordinator/member clients into bounded
FIFO admission. Raw `Client::new` and nested forwarding remain fail-fast. All outbound calls, including legacy fail-fast calls, share eight total
admission tickets held for the complete RPC. At most four active permits may
be allocated, including permits reserved for an unpolled FIFO waiter; at most
four other admitted calls wait without an active allocation. A waiting call
keeps one FIFO acquisition future and consumes its original deadline. Timeout
or cancellation releases its total ticket and any active reservation. Full
admission still rejects. Whole-journal compatibility calls and capacity confirmation do not
wait, avoiding a new nested forwarding dependency.

Queue eligibility checks shallow payload lengths and retained capacities before
waiting: request/plan byte vectors and participant vectors, at most 10,000 batch
operations with at most 16 MiB logical data and 32 MiB retained byte capacities,
and at most 32 structurally bounded history events. Existing private terminal
proof bounds remain authoritative. No frames are encoded in the waiting queue.
These are payload/container bounds, not an RSS guarantee: caller-owned plans,
pre-admission clones, CPU work and allocator overhead remain separately bounded
by their existing owners. The original four active RPC and CPU limits remain.

FIFO prevents newer admitted RPC acquisitions from overtaking registered
waiters; it does not promise equal bandwidth between whole transactions or
acceptance outside a full queue. Four active plus four waiting calls cannot
promise success for three simultaneous width-three cohorts. The ignored
`cohort_multi_request_contention` workload exercises nine real data processes,
widths 1/3/4, shared and distinct gateways, continuous Status competing with
writes, followed by deliberately excessive Status load and exact-ID recovery.
Its six configurations passed after the handoff fix: 24 of 24 writes completed
while the observer remained active, and 231 of 231 competing Status calls
certified the original committed ordinal. All six overload cases recovered the
same request identity at ordinal six and opened final snapshot V6. Raw results,
latencies, source hashes and retained failures are in
[contention evidence](benchmarks/bounded-cohort-contention.json).
This is one controlled run per configuration, not sustained throughput or a
general starvation guarantee. Overload produced 14 visible, 29 known but not
visible, and five Capacity-or-Unknown Status results. Capacity and Unknown were
recorded together, so not every such result proves admission rejection.
An exact-candidate frozen-executable repeat passed in 104.62 seconds, again
with 24 visible competing writes, 231 successful Status calls and six same-ID
recoveries. Its overload Status distribution was 15 visible, 29 committed but
not visible and four Capacity-or-Unknown responses. The two runs remain
separate records rather than pooled steady-state samples.

The initial independent-waiter-ticket draft passed widths one and three, but
failed the two-request width-four case with incomplete visibility. A temporary
diagnostic reproduced `Capacity`, not a five-second timeout. Separate waiter
tickets double-counted a promoted but unpolled active permit during FIFO
handoff. Total-call tickets fix that accounting race without increasing the
eight-call envelope; a deterministic successor-during-handoff test covers it.
The failing run remains evidence, not a green result. Deterministic unit tests
also cover cancellation after an active permit is reserved but before repoll.

A separate top-level process-fixture mutex isolates otherwise unrelated
acceptance scenarios before their timers/sessions start. It leaves intentional
concurrent operations, deadlines and assertions inside each scenario unchanged.
That mutex is not a production overload solution or fairness result.

The admission design follows the [Tokio semaphore FIFO contract](https://docs.rs/tokio/1.54.1/tokio/sync/struct.Semaphore.html)
and distinguishes queue capacity from active capacity as explained by
[Tower's admission composition](https://docs.rs/tower/0.5.3/tower/builder/struct.ServiceBuilder.html).
No dependency was added. Internal capacity-confirmation forwarding forcibly
uses fail-fast admission even when a caller supplies a queued endpoint client.

## Validation boundary for admission candidate

Candidate `35e60ac` passed the d2 library (344 tests), graph library (339),
normal nine-process regression, distributed queries, experimental cases and
remaining TCP/SQL/doc suites. The isolated managed-process target passed 24
with two ignored. All-target Clippy, formatting and diff checks passed.
The full workspace command stopped at the Unix gateway target: eight tests
failed on sandbox Unix-socket permission/startup errors, while two passed.
No access restriction was bypassed, and this is not a fully green workspace
result. The release 1,025-operation lifecycle and remote CI were not run for
this candidate. Results do not cover later main integration changes.
