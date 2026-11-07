# Bounded small-frame JSON decoding

## Scope and mechanism

The graph RPC transport decodes JSON bodies of at most 4,096 bytes directly.
Larger bodies still use the existing `blocking_json` / `block_in_place`
boundary. The shared frame reader and dedicated hierarchy-channel reader use
the same helper. Encoding and the separate Raft TCP transport are unchanged.
This avoids repeated Tokio worker handoffs for short parses. The threshold
uses actual body bytes, not graph labels, fields, topology, query text, expected
answers or CoreHR identities. There is no new parser or wire format.

Both paths use the same serde_json decoder and typed messages. Frame-size
admission, parser strictness, caller-specific deadline/error ordering, connection
permits, protocol/sequence/snapshot checks and no-retry behavior are unchanged.
The shared reader checks the deadline before and after decode. The dedicated
channel retains its original successful-decode deadline check and error ordering;
it does not gain a fresh timeout. Current-thread executors and calls outside a
runtime already decoded inline and retain that behavior.

Four KiB is a conservative input-size bound, not a hard CPU-time bound or a
preemption guarantee. Large-frame decoding still lets Tokio replace an occupied
worker. Small-frame load can affect scheduling; a latency improvement in one
query must not be interpreted as proof that every concurrent workload improves.

## Matched end-to-end experiment

The measured baseline is main `040c72560843eaa0354e83674de6f6e10e343488`.
Merged main `4eb93ee49073595b341a3ab2fb586c5cedf2a574` has identical production
Rust; PR82 added tests/documentation. Measured candidate commit:
`c651cad94cc4f66d0a2054c79ac61e322e846635` (tree
`2c85ce8fc38ec465d2909cc0403597fdf484a484`). Later integration onto main83
`3e5508b46e60db725b00234e14c808a460c411a4` preserves the new consistency
negotiation and is verified separately; these old-baseline timings are not
presented as a new-main timing measurement. The branch subsequently also
integrates main84 `d2a1c7e3068b830114f161e13462cd098f6172c0`; its broader
global-repeat functionality is preserved, and exact-head correctness CI is
rerun. No new-main speed claim is inferred from the old measurements.

Both variants use frozen release node/driver binaries, the same deterministic
200,000-person/5,000-department fixture and two Tokio workers per process. Three
data shards run as independent processes on one cloud host. There is no profiling
instrumentation. Every group starts fresh processes and databases. The metric is
Start plus Advance until the complete count, excluding import and final
replay/cancel. All 24 counts were 199,999, with fixed snapshots and verified replay.

Two orders were declared before measurement: A/B/B/A, then B/A/A/B, with three
samples per process. Thus each variant has 12 samples but only four independent
process runs; samples within a process are not independent replications.

- First pass median: 4.938 s → 4.568 s (7.49% lower).
- Reverse-order confirmation: 4.944 s → 4.472 s (9.55% lower).
- Combined median: 4.944 s → 4.517 s (8.64% lower).
- Baseline range: 4.380–5.139 s; candidate range: 4.064–4.670 s.

The overlapping ranges and single host matter. These observations are not a
universal speedup, multi-machine throughput, production tail-latency target or
availability claim. The fixture is a regression workload, not a prerequisite for
the optimization. Machine-readable evidence retains every sample and the input,
binary and raw-output hashes in [the measurement record](benchmarks/small-frame-decode.json).

## Generic scheduling load control and tradeoff

A separate release probe uses the identical test-only harness in both variants:
eight real loopback echo lanes, 2,000 exchanges per lane, two Tokio workers,
and body sizes 1,024 / 4,096 / 8,192 bytes. A/B/B/A followed by B/A/A/B gives
four processes per variant. Every size in all eight runs completed 16,000
exchanges (32,000 frames) correctly with zero timed-out lanes.

Median frame rates, baseline → candidate:

- 1,024 bytes: 45,806 → 69,043 frames/s.
- 4,096 bytes: 47,506 → 60,145 frames/s.
- 8,192-byte large-frame control: 46,302 → 46,464 frames/s.

Scheduling is a tradeoff, not an across-the-board latency win. The median of
per-run sampled 1 ms ticker p99 values at 4,096 bytes increased from **886 to
1,162 microseconds** (about +0.276 ms); p95 increased from 704 to 819 µs.
At 1,024 bytes p99 was 863 → 810 µs, and at 8,192 bytes 843 → 869 µs. The
largest sampled lateness anywhere was 4.615 ms. These are timer-lateness
observations, **not actual RPC latency or Raft heartbeat p99**. They do not
establish a production scheduling SLO or eliminate starvation risk under other
loads. Review this tradeoff before adopting the optimization for tail-sensitive
workloads.

The ticker uses Skip, so missed ticks are not individually weighted in its
percentiles. Raw per-run p95/p99/max, sample counts and elapsed time are retained
in [the scheduler record](benchmarks/small-frame-scheduler.json). There is no
warmup; body sizes have fixed order. Encoding, allocation, validation and
loopback costs are included. Each lane's client/server halves use try_join in
one task, unlike independent production endpoint runtimes. The 30-second
per-size limit is cooperative, with a separate process watchdog.

The manual probe is ignored in ordinary CI, without flaky fixed-latency gates:

```sh
cargo test -p replicated-graph --lib --locked --release \
  frame_scheduler_probe::generic_frame_scheduler_load_probe \
  -- --ignored --nocapture --test-threads=1
```

## Validation

Non-CoreHR tests exercise real Request/Frame/Answer bodies padded to exact
4,095 / 4,096 / 4,097 bytes; unknown/duplicate fields, wrong types and trailing
junk remain rejected. Complete balanced shallow/deep JSON covers the parser's
recursion bound independently of typed-field rejection. Tests prove decode
actually starts before an expired-success rejection on both sides of the
threshold, and that a >4 KiB decode on a single-worker runtime still lets an
unrelated task advance through the blocking boundary. Existing protocol,
sequence, descriptor, cancellation and deadline checks remain in place.

On the main83 integration, 149 library tests, eight distributed process tests,
one remote-query process test, and all-target Clippy passed; the subsequently
added complete-recursion test and Clippy were rerun successfully. Formatting
and diff checks passed. The manual load probe was run separately as above.
Full ordinary and full-scale CoreHR CI on the published head remain required
before merge. A successful benchmark is not a correctness gate.
