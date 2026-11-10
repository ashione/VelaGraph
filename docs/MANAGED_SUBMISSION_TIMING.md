# Managed submission phase timing (Issue #127)

`Gateway::bind` and `bind_remote` keep diagnostics disabled. Explicitly choose
`bind_with_timing` or `bind_remote_with_timing` to receive a process-local
`managed_submission_timing::Reports` receiver alongside the gateway. Drain it
with `try_recv`/`try_iter` from the owning fixture. There is no environment switch,
wire field, disk record, automatic logging, or production metrics framework.

Each report correlates one Submit attempt by its existing external ID. Retries
produce separate reports with that same ID; the fixture must associate the
receiver with its process/run identity. No Plan, graph payload, command bytes,
credentials, error strings, or generated transaction payload are copied into it.
Reports are emitted on return or cancellation, before response encoding/delivery;
requests rejected before Submit dispatch do not produce reports. Status, Abort,
reads, and startup validation are not instrumented.

## Exact phase model

Elapsed values use a monotonic clock and include scheduling and RPC waits.
Each slot aggregates duration and invocation count; zero calls means not reached.
Spans close on errors and cancellation too, so a count does not certify success.

| Slot | Boundary | Nesting |
| --- | --- | --- |
| Queue | Await existing submission mutex | Submit |
| Canonical | Deadline check, canonical command, external ID check | Submit |
| Recovery | Recover and check original request/Plan | Submit |
| Snapshot | Capture and materialize certified cohort snapshot | Submit |
| Validation | Prepare mutation, including blocking-pool queue | Submit |
| Admission | Admit request and recover exact retained Plan | Submit |
| Execute | Graph execution through returned decision/visibility | Submit |
| Decision | Target check, Begin, preparation, Decide and recovery | Execute |
| Prepare | One participant prepare attempt, including Prepared ACK | Decision |
| HistorySync | History status/pages/synchronization for prepare or catch-up | Prepare or Cohort |
| Cohort | Terminal proof, full-cohort catch-up, final evidence | Execute |
| Consume | Retained decision application loop for one member (possibly empty) | Cohort |
| Visibility | `prefix_terminal` evidence for one member | Cohort |

`Report.elapsed` spans Submit dispatch to return/drop. Parent durations include
child durations. **Do not sum slots as wall time.** HistorySync combines its two
call sites; counts are aggregated across participants and history pages are not
retained. Gaps include small local checks, record construction and instrumentation
work. These gateway-side phase spans do not attribute CPU vs I/O, RPC transport vs
remote authoritative validation/reducer/persistence, or individual member cost.

`Returned` means the local handler returned a Write reply, including Aborted and
`visible: false`. `Error` may leave an unknown durable outcome. `Interrupted`
means the future was dropped; it is not an abort or proof that nothing committed.
Use the normal original-ID/Plan recovery and response semantics. Detached blocking
snapshot/preparation work already allowed by the existing path may continue after
cancellation; an interrupted report measures only the observed wait.

## Bounds and compatibility

There are 13 fixed slots per attempt and at most 64 completed reports queued per
gateway. Existing connection admission bounds active attempts. Counters and
elapsed aggregates saturate; no per-event, per-page or per-participant vector is
retained. A full/disconnected receiver loses reports via `try_send` without
waiting, retrying, or changing the operation result. Missing reports are unknown,
not zero time. Existing request/workflow/snapshot/connection budgets are unchanged.

Disabled spans perform no diagnostic clock reads, allocation or locking. Enabled
attempts allocate one fixed accumulator; span exit briefly updates its mutex and
attempt exit tries the bounded channel. No observer callback or I/O runs in a
span. This is nonzero overhead and can consume existing deadlines, just like
other diagnostic work; there is no zero-overhead or deadline-invariance claim.
The success equivalence test prints disabled/enabled functional elapsed samples
without asserting timing thresholds. They are not a performance benchmark or a
causal estimate of instrumentation overhead.

Tests cover enabled/disabled success, exact retry identity, old-version reads,
identity mismatch, validation rejection, stale preparation rejection preserving
the original durable request and Pending status, explicit Abort, and terminal
retry. Cancellation at the existing queue boundary and full/disconnected sinks
exercise diagnostic lifetime and loss without relaxing write assertions. Real managed
RPC servers also cover remote enabled/disabled success, exact original-ID retry,
identity rejection and old-version reads. A bounded forwarding proxy holds the
first target lookup after actual durable admission; canceling that handler records
Interrupted and closes the reached phases, while the original request/Plan remains
recoverable and its retry commits without a new snapshot or admission. Existing
wire golden vectors remain unchanged.

## Lifecycle attribution

The existing release lifecycle runner explicitly sets
`BYTE_BOUND_PROCESS_ATTRIBUTION=1`. The test harness then selects the timing
binding. Other harness invocations and production bindings remain disabled unless
explicitly selected. Correctness gates still independently require the exact
release test, all 1,025 writes, immutable proofs/original Plans, every durable
pending/verified checkpoint, ACK-loss recovery and all snapshot restores.
No diagnostic output can turn an incomplete lifecycle into a pass.

The harness drains at most the 64-report queue capacity per observation and
retains only 13 cumulative phase slots plus Returned/Error/Interrupted totals.
It emits JSON lines prefixed `byte-bound lifecycle attribution:` to the already
retained `test.stderr`, at the existing write milestones and before/after recovery
boundaries. Reports retain the original source/ELF hashes, fixture-owner PID,
attempt-local sequence, elapsed time, verified ordinal and stage. Aggregate
reports do not retain per-request IDs, Plans, graph values or error strings.
Counters restart with each fixture attempt; never add snapshots of cumulative
counters together or sum nested phases as wall time. Dropped, disconnected or
unobserved reports remain **unknown**, not zero and not successful operations.
On error/cancellation, a final best-effort record is marked incomplete and omits
process samples rather than reading potentially stale PIDs. Hard process death
can prevent that record entirely.

At each explicit observation, the fixture reads only its owner and three owned
child PIDs, each through at most two 16 KiB Linux `/proc` files. `stat` supplies
process start ticks, user/system CPU ticks and current resident pages; `io`
supplies character and storage-byte counters. Parse, permission, platform and
missing-file failures are JSON null, independently for CPU/memory and I/O. The
runner records OS/architecture, logical CPU count, clock ticks/second and page
bytes in its existing `execution.json`. Compare deltas only within the same run,
PID and start-ticks identity. Restarts create new identities; pre/post samples
do not account for the unsampled shutdown/startup intervals. These are sampled
counters, not peaks, fsync counts, disk latency, per-operation causal attribution,
CPU-model identification or cgroup throttling measurements. The shared parent
also runs the test runtime; its counters include fixture and diagnostic work.
Ordinary tests can run concurrently, so their parent counters include other tests.

The existing unignored `byte_bound_independent_processes_small_lifecycle_recovery`
case executes the same complete small recovery contract with diagnostics disabled
then enabled, inside one original 240-second total budget. It does not add a
concurrent fixture or change default test parallelism. Both modes preserve
original-ID retries and rejected changed requests; the enabled mode checks all
17 actual Submit reports without timing thresholds. Its explicitly unrecorded source/ELF
labels are not a release acceptance identity. Parser and aggregation tests cover
missing/malformed/oversized input, command names containing parentheses, wrong
PIDs, duplicate I/O fields, completion types and repeated drains.

Instrumentation and resource sampling have nonzero cost and consume the existing
budgets. No causal overhead estimate or speedup is claimed. Remote authoritative
verification/reducer/persistence subphases, controlled overhead measurements and
an evidence-backed duplicate-work optimization remain follow-up work. #127 stays
open; this is attribution support, not a performance or HA milestone.
