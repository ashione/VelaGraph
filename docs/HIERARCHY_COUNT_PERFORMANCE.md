# CoreHR hierarchy count: bounded Advance coalescing

This experiment measures one caller with a real coordinator and four independent
Raft processes on one cloud machine. It is not a production-network latency or
concurrent-throughput claim. The fixture has 200,000 people, 5,000 departments,
and 1,569,994 verified key/value rows. The query is:

```gremlin
g.V(1).repeat(__.in('reports_to')).emit().times(32).count()
```

The exact expected answer is 199,999. Full typed snapshot stamps remain fixed
throughout each job; no partial count is returned. Terminal replay and cancellation
are checked. No caps or leases were raised.

## Matched release measurements

Driver, coordinator and data nodes all use release builds and two Tokio worker
threads per process. Identical driver and fixture bytes are used on both sides.
Each run imports into a fresh database. There are three sequential queries per
run, in baseline/candidate/candidate/baseline order, without concurrent builds or
benchmarks. Query walltime includes RPCs and the harness's progress logging.

| Version | Six query times (seconds) | Median | Client requests per query |
|---|---|---:|---:|
| Baseline | 9.799, 8.698, 9.157, 8.518, 8.460, 8.822 | 8.760 | 12,503 |
| Coalesced | 7.242, 7.147, 7.355, 7.006, 7.010, 6.734 | 7.079 | 1,565 |

All twelve queries return the exact count and pass replay/snapshot checks. Median
walltime decreases 19.2%; client job requests, including Start, decrease 87.5%.
These are small-sample descriptive results, not confidence intervals or an SLA.
The historical roughly 19-second observation included a debug coordinator and is
not a comparable baseline.

The baseline is PR64 tree `bee260eac7ac3179c268d488328cfdd5c28dd5a3`.
The candidate starts from compact PR66 tree
`31a05e7c18cbdad0b1e788ebdcec27b0403413be` plus this change. Thus this is not a
strict single-change comparison against PR66. The measured non-dedup bag query
does not enable compact state; its relevant execution-path change is coalescing.
Raw sample observations and resource readings are in
[`benchmarks/corehr-ceo-coalescing.json`](benchmarks/corehr-ceo-coalescing.json).

## What remains expensive

A separate application-instrumented release run, excluded from the comparison,
measured each awaited fragment call and each Advance. Each query still executes:

- 12,514 adjacency fragment RPCs, representing 200,000 vertex reads;
- 4,698 reverse/canonical verification RPCs, representing 199,999 edges;
- one start-existence RPC.

These 17,213 calls remain serial. Their summed walltime is approximately 96–97%
of the time spent inside Advance. The timer includes serialization, transport,
remote storage/execution, decoding and scheduling; it does not isolate CPU or
network cost. System-call profiling was unavailable because ptrace is prohibited.
Application timing did not require changing that restriction.

The profile recorded approximately 889.8MB of cumulative logical byte charges and
2,646,926 work units, below the unchanged 1 GiB and 8,000,000 caps. These logical
budgets are not resident-memory limits. Per-process memory observations are in
the JSON record; fixture loading also contributes to high-water marks.

## Scope and next step

Each Advance now completes up to eight existing 16-row micro-pages, with a 50 ms
soft yield check between pages. Each page retains the same edge buffer limit;
all work, byte, identity, mass, request deadline and lease checks stay cumulative.
Failure/cancellation never makes a partly consumed job resumable; replay remains
one response per Advance sequence.

This removes client polling overhead, not shard-local subtree aggregation.
Bounded shard concurrency or fewer fragment round trips are the next hypotheses,
but need separately reviewed global-budget reservation, deterministic result
combination and failure/cancellation tests. Merely cloning a remaining budget
into parallel calls would risk exceeding the cumulative cap and is not acceptable.
