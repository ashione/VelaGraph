# Member history extension at a verified head

This follow-up builds on PR #345 (`0cb6770`). It changes member history-page
merging only. The 1025-write lifecycle, timeouts, recovery checks, original IDs,
Plans, old-version proofs, three snapshot restores and CI tiers are unchanged.

## Repeated work

Member apply first creates `VerifiedArchive` from one pinned RocksDB snapshot.
That operation verifies the complete raw namespace/hash chain, the active
checkpoint projection, and the complete derived terminal index. An accepted
`MemberOverlay` contains only original commands that subsequently passed the
shared checkpoint transition checks.

Previously, even a new page starting exactly at that overlay's current head
walked all old events again to check overlap. There is no overlapping event in
that case. The new path validates the full incoming page and domain as before,
requires equality of the complete head cursor (not just event count), then
passes every new event through the unchanged `append` implementation.

Overlapping pages and retries from an older position still use the original
walk. A same-count substituted root cannot select the shortcut. Coordinator
paths are deliberately unchanged. Full verification still precedes each new
member operation; cold replay and snapshot validation are unchanged.

This is operation-local reuse, with no new persisted field, cross-operation
cache, full-history collection, resource limit, dependency or RPC change. The
shortcut computes the head using the already bounded accepted suffix. It adds
no retained allocation. New-admission identity lookups and other raw projections
can still scan history, so this does not change overall lifetime complexity to
constant or linear time.

## Validation and measurement

The regression compares the original overlap-walk branch with the optimized
branch on both profiles at 31/32/33 and 1024/1025 boundaries, where permitted.
It checks checkpoint, command and cursor equality, exact scan-count reduction,
old-page retry, empty/nonempty pages after accepted overlay commands, forged
roots/domains/counts, invalid final commands and unchanged input after failure.
New operations and cold reopen must still reject corrupted old history.

The opt-in `member_head_extension_cost` benchmark alternates original/optimized
order with two warmups and twelve measured samples per branch. Both arms include
complete member verification before merging the same terminal page, using the
same database, process and binary. The 32/256/1025-transaction fixtures use
Begin/Prepared/Abort histories with out-of-order completion; they are not the
gateway's full retained-Plan workload. Setup, Raft persistence and gateway/RPC
work are outside these measurements.

```sh
cargo test --release --locked -p d2-raft --lib \
  state_machine::event_ledger::tests::member_head_extension_cost \
  -- --exact --ignored --nocapture
```

## Completed hosted baseline

The follow-up was integrated with main `5c7e2c3` in local candidate `879fa22b`.
On macOS arm64 with Rust 1.95.0, `cargo test --locked` passed 1306 tests with
zero failures and 44 explicitly ignored tests/fixtures. Strict all-target Clippy,
formatting, diff checks and the 38 lifecycle-validator Python tests passed.
The first restricted-sandbox Rust attempt could not bind loopback ports; the
complete rerun with loopback permission passed without changing tests. Python
validation used Python 3.11 and a canonical temporary directory (the system
Python 3.9 lacks `tomllib`, and secure no-follow checks reject macOS `/var`
symlink paths). The opt-in release benchmark is measured separately.

The first optimization's manual full run
[44444738204](https://github.com/ashione/VelaGraph/actions/runs/44444738204)
passed at `0cb677001d7ae7881dfcca71d8eb22ffe9a2fcee`, with COMPLETE evidence
for the `full-1025` tier. The lifecycle step took 40m56s; the test executable
took 2455.413 seconds, excluding compilation and artifact handoff.

| Recorded span | Seconds |
| --- | ---: |
| 1025 gateway submissions | 2411.822 |
| Three process reopens combined | 1.143 |
| Three snapshot export/install operations | 2.672 |
| Three complete recovery verifications | 26.689 |

Submissions account for 98.22% of test wall time. These rows are selected spans,
not an additive partition of the entire test; nested diagnostic phases must not
be summed. This supports optimizing the submission path rather than reducing
recovery checks. The historical PR #340 test took 2478.179 seconds, but that is
an uncontrolled single-run comparison and does not establish a speedup.

This run used Linux x86_64 with four visible logical CPUs. Sampled cumulative
CPU across process incarnations was at least 3094.08 CPU-seconds; maximum sampled
aggregate process RSS was 476.21 MiB. Sampling misses work after the final sample
and between samples, so these are not exact CPU cost or peak-memory measurements.
The actual runner CPU quota and competing load were not established.

The artifact `managed-lifecycle-44444738204-1` records source manifest SHA-256
`a81d456c7725f594dcaceb59c0de052a58c3392ede1045c87d9f792d5988cb08`
and executable SHA-256
`8caa80beaf72db9e018d0a55bcd3b9549fbd1613f83e7f95505fb88aea7f08cc`.
It excludes this follow-up and the later main integration. Full lifecycle
validation on the new candidate and comparable before/after measurements remain
required; a faster microbenchmark is not evidence of a full-lifecycle speedup.
