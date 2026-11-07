# Bounded hierarchy shard parallelism

This extends the PR68 micro-page scheduler. Every micro-page still expands at
most 16 frontier vertices and retains at most 16,384 observed edges. The batch
scheduler runs at most three different shard sessions concurrently; one session
is never used concurrently, even when multiple partitions map to it. Wave replies
are restored to their original input indices before the existing weighted merge.
The next depth and label filtering remain downstream of integrity validation.

## Conservative reservation proof

Each hierarchy-open reply supplies the maximum raw key-plus-value length in the
same immutable snapshot used by that session. No records are decoded or excluded
from that maximum. Normal session descriptors and fragment messages are unchanged.

Let R be the maximum of that snapshot size, 256, and input edge-label length plus
256. This also covers an absent derived reverse-index key. A successful metered
get costs at most G = 128 + 5R bytes and one work unit. Successful leaf bounds are:

| Leaf | Bytes | Work |
| --- | --- | --- |
| Exists | G | 1 |
| VerifyReverse | 2G | 2 |
| ResolveIncoming | 3G | 3 |
| In | G + 128 + 1000 × (384 + 5R) | 2002 |
| Out | In bound + 3000G | 5002 |

The adjacency bound includes the source/target vertex get, scan overhead, at most
1,000 returned rows and per-entry accounting. Out includes canonical record,
outgoing index and source-vertex validation for every observed edge, including
unrelated labels. Batch overhead and the maximum 2 MiB encoded response charged
twice are added. Request encoding uses maximum-width remaining counters, so its
size bounds the actual reserved-budget request. All arithmetic is checked.

Before dispatch, the sum of reservations must fit the remaining global byte and
work budgets. After successful waves, only returned actual costs are settled. A failed job
is terminal; failed replies do not supply a complete coordinator cost ledger. The
scheduler falls back to the existing serial path, with its whole remaining
allowance, when two independent bounds cannot fit, overflow, or are unavailable.
It does not split a budget evenly and does not retry after an exhausted fragment.
Thus it does not introduce rejection caused by budget fragmentation. Existing
RPC admission limits, finite leases, and deadlines can still reject workloads
under concurrent load. This is not an arbitrary-load equivalence claim.

## Failure and resource behavior

No tasks are detached: at most three RPC futures are polled as one structured
wave. Cancellation drops all local futures, and the existing job registry rule
prevents resumption after any interrupted Advance. Already-started data workers
retain their own deadline and session meter. The coordinator waits for all wave
replies, validates them, and only then installs outputs. An error is terminal and
never returns a partial count.

Hierarchy worker-slot acquisition waits asynchronously within the request and
session deadlines; other fragment admission remains unchanged. The development
wire format adds a hierarchy-open reply, so coordinator and data nodes must be
upgraded together. There is no mixed-version protocol negotiation.

## Scope of the performance evidence

Matched release A/B/B/A runs use the same driver source, fixture, two Tokio worker
threads, fresh databases and three query samples per process. The coordinator is
embedded in the driver, so both the driver and nodes are built against the
corresponding implementation. A setup attempt combining an old coordinator with
new nodes failed before measurement and is excluded, not reported as a query
performance sample.

The measured query is the 200,000-person CEO reporting hierarchy count; all correct
results must equal 199,999. This is one machine running three data shards with one
replica each, not a multi-host network test or a high-availability benchmark.
Snapshot fixture import time is excluded from query latency and is not a claim
about production graph-write throughput. Six samples per version are a narrow
regression observation, not a latency SLA or broad throughput characterization.

## Measured result

The final matched release A/B/B/A comparison produced:

| Version | Six query seconds | Median |
| --- | --- | --- |
| PR68 baseline | 7.911, 7.147, 6.701, 6.381, 6.569, 6.482 | 6.635 s |
| Bounded parallel waves | 5.538, 5.479, 5.484, 6.071, 5.731, 5.416 | 5.511 s |

The observed median reduction is **16.94%**. Each result was 199,999; snapshot
stamps and replay were verified. Client request counts remained 1,564–1,565.
This improvement is separate from PR68's polling-round-trip reduction. It must
not be compared as a like-for-like change with the earlier debug-driver 19-second
observation. Most partition-ordered frontier micro-pages can still touch only
one shard; independent shard waves do not imply all work runs three-way parallel.

The final complete 200,000-person/5,000-department run passed all nine count cases
with three samples each, plus two algorithm probes, in 142.899 seconds including
fixture import and verification. It compared all 1,569,994 KV records, checked
fixed snapshot vectors, exact counts, sequential progress, replay, cancellation,
and the absence of partial counts. The old synchronous resource guards still
reject as specified. All 103 library tests, two distributed process tests, one
remote process test, formatting and all-target Clippy checks passed locally.
Remote CI remains a separate merge gate.

[Machine-readable evidence](benchmarks/corehr-shard-parallelism.json) includes
all samples, the full matrix, source/binary/fixture hashes, and raw log hashes.

