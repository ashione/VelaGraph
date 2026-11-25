# Packed native results: validation and performance

This follow-up compares the original JSON Go query path at `dba65bf` with packed
results and explicit result views in PR #377. The original C JSON operations remain
available. All data is generated; no private service requests are included.

## Why the original boundary was expensive

The original Go path decoded every row and then every scalar through a new JSON
Decoder. In the initial allocation profile across the 1/16/256-row numeric scan
benchmarks, `decodeVariant` accounted for approximately 79.5% of sampled allocated
objects including callees. A 256-row operation allocated about 770 KB and 10,786
Go heap objects with profiling enabled. These are allocation attribution numbers,
not CPU-time percentages or process RSS. The macOS CPU profile was dominated by
runtime/native synchronization frames and is not used as an engine CPU breakdown.

Packed results remove result JSON encoding/decoding and the full native-to-Go
result-buffer copy. `Query` still returns owned Go objects; `QueryView` holds an
immutable native buffer and exposes synchronized scalar/path access and text
comparison. Native graph query execution, Rust result construction/packing, JSON
request encoding/decoding, plan binding and cancellation management remain.
`Text`, `Metadata`, and `Materialize` deliberately copy when owned data is wanted.

## Same-request end-to-end comparison

Apple M2 Pro, macOS arm64, Rust 1.100.0 release, Go 1.23.2 without race/profiling.
The unchanged executable request set has 257 vertices and 256 edges: one point
query, 16 distinct point subqueries in one batch, and 256 adjacent numeric values.
Every ordered result is checked. Both versions use cancellable contexts and the
same prepared/literal plans; they return the same owned Go BatchResult.

Three independent before/after process pairs alternate order. Each workload uses
100 warmups and 1,000 measured calls. Builds and correctness tests finish before
serial timing. Old executables use the retained original native library, and new
executables use the candidate library. Executable/library SHA256 provenance is
retained with local evidence. Parsing setup, result assertions and plan creation
are outside timing; request encoding, cancellation/watchers, native execution,
result adaptation and native release are inside. All figures below are medians
of the three per-run quantiles/means, not pooled sample quantiles.

Times are microseconds per complete call. `batch16` is one call containing 16
subqueries; it is not 16 independent FFI calls.

| Workload | Input plan | Before P50 | After P50 | Speedup | Before P95 | After P95 |
|---|---|---:|---:|---:|---:|---:|
| point | go-ffi-text | 140.791 | 133.667 | 1.05x | 184.666 | 178.041 |
| point | go-ffi-prepared | 9.959 | 4.500 | 2.21x | 15.875 | 8.334 |
| batch16 | go-ffi-text | 2095.792 | 2091.125 | 1.00x | 2487.042 | 2430.166 |
| batch16 | go-ffi-prepared | 68.792 | 23.459 | 2.93x | 87.250 | 33.041 |
| fanout256 | go-ffi-text | 1113.459 | 422.792 | 2.63x | 1382.209 | 552.583 |
| fanout256 | go-ffi-prepared | 750.250 | 121.875 | 6.16x | 988.500 | 146.250 |

The text path still pays the official parser on every call. Packed output greatly
reduces large-result costs; prepared plans remain useful for repeated small
queries. These results do not establish RPC throughput, cold-store performance,
update contention or an application-wide latency guarantee.

## Go heap allocations and explicit views

The separate `BenchmarkNativePrepared` fixture uses bounded global scans of
1/16/256 generated numeric vertices, with the same compiled prepared query reused
within each case. The original benchmark source/request shape is unchanged.
Each row below is the median of three unprofiled 1-second benchmark repetitions.
This fixture differs from the point/batch/fanout set above; its times are not
interchangeable with that table.

`BenchmarkNativeView` additionally reads every scalar, checks the sum and closes
the view inside timing. The owned benchmark checks returned row count. View
latency is therefore a separate consumption measurement, not a claim that every
view call must outperform owned Query. Its heap use stays nearly constant because
no Go row array is materialized. These are Go allocations only, excluding native
allocations, caller-retained results, and total RSS.

| Rows | API | Mean time (us/op) | Go bytes/op | Go allocs/op |
|---:|---|---:|---:|---:|
| 1 | JSON Query | 12.452 | 4195 | 66 |
| 1 | Packed Query | 6.120 | 1049 | 16 |
| 1 | QueryView + consume + Close | 6.515 | 912 | 14 |
| 16 | JSON Query | 63.073 | 49160 | 700 |
| 16 | Packed Query | 16.520 | 2989 | 16 |
| 16 | QueryView + consume + Close | 15.610 | 914 | 14 |
| 256 | JSON Query | 770.406 | 770194 | 10785 |
| 256 | Packed Query | 117.163 | 33748 | 16 |
| 256 | QueryView + consume + Close | 119.078 | 914 | 14 |

Successful ID/scalar/path-element access and text comparison separately assert
zero Go allocations with `testing.AllocsPerRun`. QueryView itself still allocates
its fixed invocation/owner/control objects. Text or full Materialize returns owned
data and can allocate in proportion to that requested output.

## Correctness and lifetime

- Independent ABI1 JSON-vs-packed parity for every row/scalar kind, including
  unsigned IDs, minimum signed integer, negative zero, Unicode/NUL, duplicate starts
  and empty text/results. Additional native/C and decoder tests cover the signed
  maximum, empty paths and present-empty metadata. Temporal coverage checks V/T integer results,
  absent metadata, later mutation and store closure with a result still alive.
- Real C acquire/release test: retire both original buffer and store, read the
  exact integer through an independent lease, then release and verify accounting.
- Go access/Close stress plus a deterministic queued-Close test: an admitted read
  keeps actual C memory/reservation alive; Close waits and releases exactly once.
  No borrowed slice/string escapes safe Go accessors.
- Exact 104-byte numeric result admission succeeds; 103 bytes rejects. Cancelled
  and expired packing, allocation failure and lease-handle exhaustion release
  pending slots/permits and leave acquisition output zeroed on failure.
- Decoder checks lengths, arithmetic, partitions, flags, tags, UTF-8 and finite
  numbers. Payload offsets must follow canonical row order with no overlap/gaps
  or trailing bytes. This prevents small aliased frames from amplifying owned
  materialization. The alias regression failed before the guard was added.
- Go decoder fuzz: 424,765 executions over 10 seconds, one worker, no failures.
  This is bounded fuzz evidence, not exhaustive proof.
- Independent Rust/C and Go ownership/bounds reviews found no remaining blocker
  after the test and canonical-layout changes.

## Gates and reproducibility

Native ABI: 15 tests passed. Real C, Go `-race`, non-cgo SDK and native Clippy pass.
Complete local gates:

- `cargo test --locked`: 1,395 passed, 46 ignored by repository configuration.
- `cargo test --locked -p velagraph --features query-profiling`: 373 passed, 6 ignored.
- `cargo test --locked -p velagraph-ffi`: 15 passed.
- Default, profiling and native all-target Clippy passed with `-D warnings`.
- Rust formatting, Go formatting and `git diff --check` passed.

These are local macOS results. The PR's Linux/macOS hosted checks are reported
separately and rerun when the follow-up commit is pushed.

The pre-change implementation and original validation are retained at `dba65bf`
and [initial validation](NATIVE_FFI_VALIDATION.md). Build matching release libraries
and Go executables before timing. Run `sdk/go/native/example` serially on each
revision for the complete call table. For allocation evidence use:

```sh
cd sdk/go
go test -tags velagraph_native ./native -run '^$' \
  -bench '^BenchmarkNative(Prepared|View)$' -benchtime=1s -count=3
```

For the pre-change allocation run, copy only `BenchmarkNativePrepared` and its
standard imports from the current `sdk/go/native/native_bench_test.go` into the
`dba65bf` checkout; that shared benchmark uses only the original API. QueryView's
benchmark is candidate-only. Use the matching native library for each executable.
Use `-cpuprofile`/`-memprofile` in a separate run, not the latency comparison.
Full binary format and lifecycle contract: [NATIVE_FFI_PACKED.md](NATIVE_FFI_PACKED.md).

## Per-run complete-call evidence

Times below are microseconds. The before/after columns in the summary use the
median of these three values for each workload/plan/phase.

| Run | Phase | Workload | Plan | P50 | P95 |
|---:|---|---|---|---:|---:|
| 1 | before | point | go-ffi-text | 142.750 | 208.709 |
| 1 | before | point | go-ffi-prepared | 10.084 | 16.583 |
| 1 | before | batch16 | go-ffi-text | 2141.292 | 3274.167 |
| 1 | before | batch16 | go-ffi-prepared | 68.792 | 88.875 |
| 1 | before | fanout256 | go-ffi-text | 1111.583 | 1382.209 |
| 1 | before | fanout256 | go-ffi-prepared | 705.208 | 954.083 |
| 1 | after | point | go-ffi-text | 135.917 | 172.208 |
| 1 | after | point | go-ffi-prepared | 4.500 | 7.958 |
| 1 | after | batch16 | go-ffi-text | 2076.750 | 2438.250 |
| 1 | after | batch16 | go-ffi-prepared | 23.375 | 33.625 |
| 1 | after | fanout256 | go-ffi-text | 423.792 | 552.583 |
| 1 | after | fanout256 | go-ffi-prepared | 121.875 | 146.250 |
| 2 | after | point | go-ffi-text | 132.583 | 181.834 |
| 2 | after | point | go-ffi-prepared | 4.417 | 8.334 |
| 2 | after | batch16 | go-ffi-text | 2144.625 | 2430.166 |
| 2 | after | batch16 | go-ffi-prepared | 23.541 | 33.041 |
| 2 | after | fanout256 | go-ffi-text | 422.792 | 540.667 |
| 2 | after | fanout256 | go-ffi-prepared | 121.875 | 146.000 |
| 2 | before | point | go-ffi-text | 140.791 | 184.666 |
| 2 | before | point | go-ffi-prepared | 9.959 | 15.875 |
| 2 | before | batch16 | go-ffi-text | 2082.125 | 2475.459 |
| 2 | before | batch16 | go-ffi-prepared | 70.208 | 87.166 |
| 2 | before | fanout256 | go-ffi-text | 1121.750 | 1350.250 |
| 2 | before | fanout256 | go-ffi-prepared | 750.250 | 988.500 |
| 3 | before | point | go-ffi-text | 138.167 | 181.709 |
| 3 | before | point | go-ffi-prepared | 9.666 | 15.625 |
| 3 | before | batch16 | go-ffi-text | 2095.792 | 2487.042 |
| 3 | before | batch16 | go-ffi-prepared | 67.958 | 87.250 |
| 3 | before | fanout256 | go-ffi-text | 1113.459 | 1461.667 |
| 3 | before | fanout256 | go-ffi-prepared | 756.125 | 991.583 |
| 3 | after | point | go-ffi-text | 133.667 | 178.041 |
| 3 | after | point | go-ffi-prepared | 5.375 | 11.667 |
| 3 | after | batch16 | go-ffi-text | 2091.125 | 2411.458 |
| 3 | after | batch16 | go-ffi-prepared | 23.459 | 31.250 |
| 3 | after | fanout256 | go-ffi-text | 418.625 | 554.458 |
| 3 | after | fanout256 | go-ffi-prepared | 120.542 | 148.417 |
