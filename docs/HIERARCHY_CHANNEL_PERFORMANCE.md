# Bounded hierarchy connection reuse

This builds on PR69's bounded shard waves. The protocol, ownership, lifetime,
compatibility and accounting rules are documented in
[Hierarchy count jobs](HIERARCHY_COUNT_JOBS.md#bounded-hierarchy-transport-channels).

## Measurement

The final matched release A/B/B/A comparison used six samples per version, the
same benchmark driver and fixture, fresh databases, and two Tokio worker threads.
The coordinator is embedded in the driver; both driver and data nodes were built
against each corresponding implementation. The baseline is main `52a214b9546828dc11151464fc1f6ab8089bd68d`.

- Baseline seconds: 5.565, 5.536, 5.824, 5.674, 5.341, 5.376.
- Candidate seconds: 4.537, 4.611, 4.589, 4.781, 4.511, 4.853.
- Median: **5.550 s → 4.600 s**, a **17.12% reduction**.

All twelve CEO counts were 199,999 with fixed snapshots and verified replay.
This is one cloud machine with three data shards and one replica per shard, not
a multi-host or high-availability test. Query times exclude fixture import.
Six samples are a regression observation, not a latency SLA or throughput claim.

The final 200,000-person/5,000-department matrix passed all nine cases with three
samples each plus two probes in 127.598 seconds including import/verification.
It verified all 1,569,994 KV rows, exact bag/dedup counts, fixed snapshots,
sequential progress, replay, cancellation, and absence of partial counts.
The existing synchronous resource guards still reject as specified.
113 library tests, two distributed process tests, one remote process test and
all-target Clippy passed locally. Remote CI remains a separate merge gate.

## Why this direction

Separate application instrumentation recorded 17,213 shard fragment RPCs per CEO
query and summed connect durations of 0.964–1.019 seconds over three samples.
Concurrent phase durations overlap; these sums are neither reducible wall time
nor proof that network latency alone caused the prior result. This candidate
combines bounded connection reuse, compact envelopes and TCP_NODELAY on channels;
the comparison does not isolate their individual effects.

A partition-rotation experiment regressed the matched median by 31.37% and was
rejected. A vectored-write-only experiment showed no demonstrated gain (median
3.99% slower, overlapping ranges). Neither experiment is included here.

The original 512-byte fixed charge is unchanged. A maximal-width typed wire test
checks both copies of all framing, with at most 504 bytes of envelope overhead.
No work/byte budget is raised, snapshot reopened, or corruption check skipped.
Channel tests cover reuse, bad replies, EOF/lost acknowledgement, cancellation,
first-byte deadlines, real 1,024-operation retirement, exact-session reconnect
and retained cumulative accounting. Direct cache-saturation fallback and full
service shutdown integration are remaining test gaps, not claimed coverage.

[Machine-readable evidence](benchmarks/corehr-hierarchy-channels.json) contains
all samples, matrix results, source/binary/fixture hashes and raw evidence hashes.
