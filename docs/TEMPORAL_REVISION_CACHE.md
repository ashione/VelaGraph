# Bounded temporal revision reuse

The local `TemporalStore` reuses immutable revision prefixes across point reads
and Gremlin executions. The same mechanism serves arbitrary vertex and edge
identities, properties and valid-time intervals. It has no dataset, service,
transport or hierarchy dependency. The format-2 storage and query contracts are
unchanged; managed/distributed and materialized read paths are not modified.

## Read and publication rules

A cold lookup captures only the rows its existing bounded scan visits. It does
not prefetch the rest of the identity's history. A prefix that ends at a matching
assertion may answer another valid time only when one of its rows matches. A
complete prefix may also prove absence. A cache admission failure falls back to
the existing scan, without truncating history or turning a storage error into
absence. Selected canonical payloads still decode and validate under the caller's
budget; edge endpoint visibility is checked afresh at the same V/T.

Each block has an index ordered by interval start and a prefix maximum end. A
binary search skips starts after T; maximum-end pruning skips prefixes that
cannot overlap T. Among overlapping intervals, the smallest original scan
ordinal wins, preserving newest-system-revision priority and half-open intervals,
including unbounded future and signed time extremes. The index is an in-memory
accelerator, not a new RocksDB index or record encoding.

Cache entries are store-local and guarded by both the selected logical version
range and the RocksDB snapshot sequence range. Only a successful, contiguous
commit extends an unchanged identity's ranges. The old upper version and physical
sequence must equal the acknowledged commit's base. Changed identities, skipped
cache locks, unknown write-result gaps and sequence gaps cause misses. A reader
may publish a block from an older pinned view after a newer commit; that block
starts at its exact original coordinates and cannot certify newer coordinates.
No error is replaced with an assumed successful write.

## Bounds, ownership and budgets

- 512 direct-mapped mutex slots per open store, using a store-local randomized
  hash. A collision changes cache residency only.
- At most 1 MiB of accounted retained/in-progress block storage per store,
  including encoded assertion bytes, row/index arrays and conservative block
  overhead. Fixed slot storage is separate. This is allocation accounting, not
  an allocator/RSS measurement.
- At most 256 KiB accounted bytes and 2,048 revisions per block. Both are cache
  admission bounds; they do not lower the existing query history limits.
- Retention is reserved before copying bytes. An evicted/replaced block stays
  charged until its final `Arc` reader releases it. Building a replacement also
  needs budget; saturation can prevent new admissions, which safely use storage.
- Warm Gremlin lookups charge the same logical rows and bytes as the original
  scan, including nonmatching revisions before the winner. Raw-row peak memory
  and selected-payload memory reservations remain enforced. If aggregate
  admission could fail, the scan replays in original row order to preserve the
  error precedence of competing work and memory limits.
- Query controls are checked before and after cache lookup. Index loops are
  bounded by the cache's row cap. Cache retention is separate from per-query
  output/memo accounting and cannot make a query budget optional.

Cross-version reuse adds a bounded slot walk to commits after durability has
been acknowledged. It is a read/write tradeoff, not a claim that writes are free.
The cache does not eliminate payload decoding, serialize results, materialize an
entire graph or provide a new public cache-coherence API.

## Verification and reproduction

`tests/temporal_revision_cache.rs` covers interval boundaries, old pinned views,
corrections, absence/recreation, endpoint checks, reopen, store isolation, oversized
history/payload fallback and cold/warm budget and cancellation behavior. The
profiling test verifies that an unchanged identity can avoid both seek and
iterator creation after an unrelated commit. Unit tests cover overlap precedence,
incomplete-prefix misses, contiguous acknowledgements and last-reader charging.
The existing corruption and dense temporal-oracle tests remain applicable.

Run:

```sh
cargo test --locked -p velagraph
cargo test --locked -p velagraph --features query-profiling,async-read
cargo run --release --locked --example temporal_revision_cache -- 10000
```

The example constructs one synthetic vertex with 1, 32 or 256 disjoint intervals.
It checks every returned value and reports a cold first query, warm varying-time
queries, unrelated commits and reads after those commits separately. Copy the
identical example to the baseline checkout, build both release binaries first,
then run them serially in alternating order. Do not compare timing during builds
or treat the one-sample cold statistic as a stable latency distribution. These
local query timings exclude network/RPC costs and make no production p99,
throughput, total-memory or concurrent-write claim.

### Local synthetic measurement

Baseline `dd03bd5`, candidate implementation in this PR; Apple M2 Pro, macOS
arm64, Rust 1.100.0 release, RocksDB linked with optimization. Both binaries were
built before timing. Three serial paired runs (baseline/candidate,
candidate/baseline, baseline/candidate), 10,000 verified warm queries per shape
per run, no pacing or concurrent writer. Values are medians of run means in µs.

| Revisions per identity | Baseline | Candidate | Mean reduction |
|---:|---:|---:|---:|
| 1 | 2.194 | 1.343 | 38.8% |
| 32 | 3.518 | 1.243 | 64.7% |
| 256 | 14.119 | 1.317 | 90.7% |

Every matched run improved its warm mean. This measures one hot identity with
varying valid times, not general graph throughput. Reads after unrelated commits
also improved in all three shapes. The one cold query per run was slower in the
candidate median (approximately 27/18/41 µs versus 22/12/31 µs for 1/32/256
revisions); these are observations, not cold-tail estimates. Building blocks and
the interval index has a cold-path cost. Unrelated commit means varied by shape
and run; the first two shapes' medians rose about 5–8%, while the third fell.
The bounded cache notification is additional write work, so no write-speedup
claim is made. Saturation, identity churn and concurrent-write performance are
not established by this benchmark.
