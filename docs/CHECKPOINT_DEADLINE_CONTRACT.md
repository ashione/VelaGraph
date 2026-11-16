# Deterministic checkpoint read deadline contract

Status: implemented and validated locally, pending CI. Tracking: #318 within
#191. This transport slice supplements the midstream-failure tests; it changes no production budget,
format, restore guard or provider guarantee.

## Existing contract and missing evidence

`CheckpointStore::verify` and `download` each wrap their entire operation in one
Tokio timeout. `read_bounded` and `verify_file` do not renew it. A forever-pending
body test proves a timeout exists, but cannot distinguish that global budget
from an accidentally renewed timeout on every GET or body item.

The new synthetic integration suite uses valid library-produced receipts,
manifest bytes and chunks with finite delays. It covers both public APIs in
three cases: two items in the manifest body, two items in a later chunk body,
and one delayed item in each of the manifest and later chunk GETs. Each case
has a successful 3s+3s control and a 6s+6s failure under one 10s operation budget.
Every individual delay is below the budget. Six tests cover twelve operations.

## Clock and cancellation design

Tokio's existing locked version gains `test-util` and `sync` only as dev features;
there is no new package/version or lockfile change. Fixture upload runs before
pausing time. During each measured operation a runnable yielding task prevents
idle auto-advance, especially while `tokio::fs` awaits its real blocking pool.
An RAII guard aborts that task and resumes the clock on success or unwind.

Each custom body first polls/registers its Sleep, then reports its object key,
item number and deadline to the driver. Only the driver advances the clock,
stepping 1ms past each body deadline for Tokio's millisecond timer wheel. Thus
the successful 3s+3s control finishes at exactly 6.002s of virtual time.
When the next body deadline is 12s, the driver first advances to 10s+1ms and
polls the operation while that body remains pending. The 1ms margin avoids
clock-wheel boundary ambiguity. Jumping directly to 12s would be invalid:
Tokio Timeout polls the inner future first and might accept an already-ready
result. If an intentionally broken reset-budget implementation remains pending
at 10s+1ms, the driver allows its valid body to finish at 12s, exposing success
where the global contract requires `Deadline`.

An independent standard-library thread can wake a stalled asynchronous handshake
after 20 real seconds. It is stopped and joined on every exit. This is only a
test watchdog, not the acceptance threshold, and does not claim to interrupt
arbitrary synchronous blocking code. The injected streams have no blocking I/O.

## Assertions and fault sensitivity

The failure assertion first checks `Error::Deadline`, before secondary counters,
so mutant evidence identifies an incorrect successful result directly. Tests
also check both body registrations, the exact yielded prefix, stream drops,
GET order/count, and the absence of completion or pending manifests. Manifest
failures create no destination; later chunk failures preserve the already-synced
Raft file but make no durability promise for the unfinished state file.
Success controls compare all output files or exact verification counts. All
non-GET object-store methods panic; no mutation can silently pass the test.
Elapsed virtual time alone is not sufficient evidence.

Two temporary local mutation checks were executed: remove the outer timeout and
place it around each GET body read, then remove it and place it around each body
item. Against the first mutant, both cross-GET cases failed with an incorrect
`Ok(())` result (4 other cases passed). Against the second, all six cases failed
with `Ok(())`. The production source was restored byte-for-byte before final
validation; no mutant is committed. These tests make no cloud S3 certification
or live-recovery claim.

## Local validation

The final six-test suite passed twenty consecutive runs after restoring production
source, followed by the complete package suite (26 passed, one real-S3 test
ignored). Package all-target Clippy with `-D warnings`, workspace formatting and
diff checks passed. Production source and `Cargo.lock` have no changes. The
initial exact-timer-boundary harness stalled and was caught by its real-time
watchdog; the corrected explicit 1ms timer-wheel steps are covered by these
final repeated runs, not treated as a production defect.
