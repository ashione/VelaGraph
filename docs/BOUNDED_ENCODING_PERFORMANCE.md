# Generic bounded JSON encoding

## Change and applicability

The distributed response encoder now amortizes deadline-clock reads across
small serializer writes, rather than reading the clock for every JSON token.
It is generic over `Serialize`. It does not inspect the graph, query text,
labels, topology, property names, fixture, or expected answer. There is no
CoreHR execution special case and no topology-specific batch-size rule.

Every individual write still checks the exact 2 MiB output limit. Small-write
deadline checks are at most 8 KiB apart; larger individual writes are checked
before copying, and encoding is checked at entry and completion. This is a
cooperative byte interval, **not a wall-time cancellation guarantee**. An expired
or failed encoder never returns partial successful output. A single counter
replaces repeated clock calls; no new buffer is allocated. Encoded JSON, costs,
query limits, snapshot identity, integrity checks and retry behavior are unchanged.

This affects the shared distributed response encoding path, including typed
fragments and coordinator responses. It does not change the SQL engine or claim
to optimize every other encoder in the repository.

## Correctness beyond CoreHR

Normal library CI exercises exact byte equality with `serde_json` at 8 KiB and
2 MiB boundaries, escaped/Unicode text, oversized output, a serializer failing
after partial output, and expiration after a final short write. The generic
result-distribution test covers empty and large counts, sparse identities,
repeated high-fanout bags, long paths, null/integer/real/boolean/text values,
and oversized path collections. These are serialization invariants, not claims
that arbitrary hand-constructed paths are valid query results.

The existing non-CoreHR real-process distributed graph suite independently
covers `knows` edges, cross-shard paths, parallel edges, cycles/self-loops,
large properties, and corruption/resource failures. All 131 library tests,
all-target Clippy, two distributed process tests and one remote-query process
test passed locally. The exact candidate source commit
also passed both ordinary and full-scale CI. Every later head still requires
its own CI before merge; profiling is not correctness evidence.

## Matched end-to-end observation

Baseline source: `09773b319af380ca7da1524395aa3734bb8f3975`.
Candidate source: `a78575827a832e71f1e1ba52fff761deba025fd9`
(tree `10faa4b4162b201f253a2b03473c9add23b48ab2`). All 189 candidate blobs
were verified against that tree after the measurement. This evidence update
changes only documentation, not production/test Rust source. Both variants
were explicitly rebuilt in release mode, with identical driver and fixture, two Tokio threads per process, and no
profiling instrumentation. Order: A/B/B/A, fresh databases for each process run,
three query samples per process, six samples but only two process runs per variant.

The existing 200,000-person / 5,000-department workload is used as one demanding
regression workload, **not as a prerequisite or specialization of the optimization**.
All twelve CEO count results were 199,999 with exact fixed snapshots and successful
replay. Import is excluded; Start/snapshot preparation is included.

- End-to-end median: **4.713 s → 4.285 s**, **9.08% lower**.
- Baseline range: 4.369–5.232 s. Candidate range: 4.128–4.487 s.
- Predeclared secondary observation, after Start: median 4.290 s → 3.912 s.
- Start itself: medians 0.393 s and 0.383 s.

An earlier iteration observed 7.19% lower median. Its samples are retained
separately in the evidence and are not pooled with this final-source run.
The variation and overlapping ranges matter: this narrow result is not
statistical proof of a universal speedup, throughput, production latency, or high availability.
All shards are separate processes on **one cloud host**, not physical multi-host
execution. Only this workload has an end-to-end speed measurement here.

Separate diagnostic instrumentation found 0.720–0.765 seconds summed over three
nodes in bounded admission encoding. Those nested/concurrent sums and timer
overhead are not wall-clock savings. They motivated this experiment but are not
mixed into its timing samples.

[Machine-readable evidence](benchmarks/bounded-encoding.json) preserves every
sample plus source, binary, driver, fixture and raw-output hashes.
The [benchmark-only driver patch](benchmarks/bounded-encoding-driver.patch)
reproduces selection of three CEO samples in the existing count-job driver;
it is not applied to production sources or normal CI. Full CI keeps broader
query coverage instead of selecting this one measured case.
