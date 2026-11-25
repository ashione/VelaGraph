# Native ABI1 validation: initial JSON path

Historical source build: `dba65bf` against `b4ce038`, associated with
[issue #376](https://github.com/ashione/VelaGraph/issues/376).
Packed results and current Go Query performance are covered in
[the follow-up comparison](NATIVE_FFI_ZERO_COPY_VALIDATION.md).

Local platform: Apple M2 Pro, macOS arm64, Rust 1.100.0, Go 1.23.2.
Only generated/public data is used. This report does not establish production,
RPC or hierarchy-service performance.

## Correctness and lifecycle

- Controlled memory commits cover COW and Locked, cascaded edge deletion, new
  edges, incoming adjacency, metadata and version. Each pre-publication checkpoint
  is injected with cancellation; rejection preserves the old state, successful
  publication installs the complete new state, and later writers can proceed.
- Checkpoint cancellation preserves the existing destination and cleans temporary
  files; temporal multi-mutation cancellation is checked again after reopen.
  Two temporal conditional writers starting at the same V have exactly one winner.
- A second query can be cancelled after the first result is retained; all results
  are discarded and reservations released. Shared operation, work-byte, live-byte
  and cumulative-row limits are checked, including temporal's stricter 4,096 visits.
- ABI tests cover invalid pointers within the documented validation boundary,
  unknown ABI/no output write, wrong handle kinds, undersized copy/no output write,
  all resource admissions, retired Arc ownership, temporal path-lock retention,
  uncertain initialization, allocation failure and malformed checkpoint payloads.
- Real C layout/linkage tests and Go cgo race tests exercise native calls. Go covers
  concurrent query/write/version consistency, store/plan Close races, context
  cancellation, all row shapes, `uint64` IDs, `int64` extremes, negative zero,
  Unicode and resource counters returning to baseline. Non-cgo SDK tests pass.
- Independent source review covered core publication/budgets and the native/Go
  boundary. The deadline-to-cancellation misclassification was fixed and retested;
  reviewers found no remaining blocking correctness issue.

## Gates

| Check | Local result |
|---|---|
| `cargo test --locked` | 1395 passed, 46 intentionally ignored; complete rerun passed |
| `cargo test --locked -p velagraph --features query-profiling` | 373 passed, 6 ignored |
| `cargo test --locked -p velagraph-ffi` | 12 passed |
| Default / profiling / native all-target Clippy | Passed with `-D warnings` |
| Real C consumer / native Go `-race` / non-cgo whole SDK | Passed |
| Release Rust and Go examples | Three complete serial runs; all ordered result assertions passed |
| Rust fmt / Go formatting / diff whitespace | Passed |

The native CI workflow adds Linux and macOS C/Go jobs. Local results above are
macOS evidence; hosted Linux/macOS status is reported separately by the PR checks.

The first default run hit an existing test synchronization race in
`d2-raft::state_machine::member_checkpoint_tests::process_death_after_member_sync_recovers_commit_and_exact_retry`:
`cannot parse integer from empty string`. Its parent kills the child when the
`synced` marker file exists, before necessarily observing its completed integer write.
The source is unchanged by this PR; an isolated rerun passed. The full default
suite then passed, including this test; the isolated pass alone was not used as
the complete gate.

## Timing method

The release Rust baseline and Go program use 257 vertices/256 edges. Each workload
has 100 warmups and 1,000 measured calls, with every result checked outside timing.
Three independent process runs alternate Rust/Go order; all timing is serial,
after builds/tests finish. P50/P95 are medians of per-run quantiles; Mean is the
median of per-run means. These are not pooled sample quantiles. `batch16` is 16 point subqueries in one batch call.

Rust uses parsed queries and times control/view acquisition plus batch execution.
Go uses a cancellable context and times the complete SDK query path: encoding,
control watcher and admission, cgo/native execution, JSON result encoding, copying,
resource release and typed Go decoding. Preparation is outside timing. Prepared
Go includes plan lookup/binding; this is full embedding overhead, not an isolated
cgo transition or a matched allocator/result-lifetime microbenchmark.

## Recorded release results

Times are microseconds per complete batch call.

| Workload | Path | P50 | P95 | Mean |
|---|---|---:|---:|---:|
| point | rust-direct | 0.75 | 0.96 | 0.79 |
| point | go-ffi-text | 152.67 | 220.67 | 157.00 |
| point | go-ffi-prepared | 10.08 | 17.29 | 11.22 |
| batch16 | rust-direct | 9.58 | 22.62 | 11.22 |
| batch16 | go-ffi-text | 2208.42 | 3673.38 | 2465.16 |
| batch16 | go-ffi-prepared | 68.88 | 84.75 | 73.54 |
| fanout256 | rust-direct | 97.38 | 120.79 | 102.04 |
| fanout256 | go-ffi-text | 1092.92 | 1337.12 | 1118.68 |
| fanout256 | go-ffi-prepared | 747.21 | 1223.12 | 913.11 |

Prepared plans remove repeated official-parser work: observed P50 improved about
15.1x for one point query, 32.1x for 16 point subqueries, and 1.46x for 256 returned
values relative to the text FFI path. This is not a Go-vs-Rust speedup: prepared
FFI still costs more than direct Rust, and returned-value serialization/copying
and SDK lifecycle costs remain. The benchmark does not independently attribute
percentages to those components or measure update contention, cold stores, RSS,
RPC or sustained-load throughput.

The fanout query explicitly includes `limit(256)` to override Gremlin's existing
100-row default result cap. Initial incomplete exploratory output without that
limit failed the row assertion and is excluded from all reported measurements.

Per-run quantiles (microseconds), retained to make the three-run aggregation
reviewable:

| Run | Workload | Path | P50 | P95 |
|---:|---|---|---:|---:|
| 1 | point | rust-direct | 1.750 | 1.916 |
| 1 | batch16 | rust-direct | 9.583 | 22.625 |
| 1 | fanout256 | rust-direct | 97.375 | 120.792 |
| 1 | point | go-ffi-text | 143.625 | 220.666 |
| 1 | point | go-ffi-prepared | 10.083 | 17.292 |
| 1 | batch16 | go-ffi-text | 2177.292 | 2939.416 |
| 1 | batch16 | go-ffi-prepared | 67.292 | 80.791 |
| 1 | fanout256 | go-ffi-text | 1165.208 | 3290.625 |
| 1 | fanout256 | go-ffi-prepared | 747.209 | 1223.125 |
| 2 | point | go-ffi-text | 154.584 | 691.375 |
| 2 | point | go-ffi-prepared | 11.292 | 82.458 |
| 2 | batch16 | go-ffi-text | 2238.834 | 3673.375 |
| 2 | batch16 | go-ffi-prepared | 69.750 | 84.750 |
| 2 | fanout256 | go-ffi-text | 1092.916 | 1309.334 |
| 2 | fanout256 | go-ffi-prepared | 735.542 | 976.125 |
| 2 | point | rust-direct | 0.750 | 0.958 |
| 2 | batch16 | rust-direct | 9.375 | 23.334 |
| 2 | fanout256 | rust-direct | 97.792 | 125.416 |
| 3 | point | rust-direct | 0.750 | 0.792 |
| 3 | batch16 | rust-direct | 9.750 | 12.250 |
| 3 | fanout256 | rust-direct | 94.333 | 115.667 |
| 3 | point | go-ffi-text | 152.666 | 176.250 |
| 3 | point | go-ffi-prepared | 9.833 | 14.291 |
| 3 | batch16 | go-ffi-text | 2208.417 | 3854.958 |
| 3 | batch16 | go-ffi-prepared | 68.875 | 96.917 |
| 3 | fanout256 | go-ffi-text | 1090.792 | 1337.125 |
| 3 | fanout256 | go-ffi-prepared | 754.833 | 2620.167 |
