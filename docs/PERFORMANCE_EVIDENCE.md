# Performance evidence and benchmark plans

Use the evidence for the entrypoint, workload and consistency contract it
actually measures. Passing correctness CI is not a comparative speed result.

| Evidence | Scope and reproduction | Not established |
| --- | --- | --- |
| Local Rust traversal | [#402](https://github.com/ashione/VelaGraph/pull/402) and [#405](https://github.com/ashione/VelaGraph/pull/405); run `cargo run --release --locked --example bounded_traversal_bench`. #405 compares paired runs on synthetic shapes with a pinned local RocksDB view. | RPC performance, mixed-load tails or scale-out. |
| Direct RocksDB via Go FFI | [Reproduce the paired release baseline](benchmarks/direct-rocksdb-ffi.md) for revision selection, incoming edges and bounded tree batches. | Network latency, concurrent throughput or Pod memory. |
| Local temporal reads and native embedding | [Query reuse](TEMPORAL_STORE.md#query-reuse-and-typed-bindings), [revision cache](TEMPORAL_REVISION_CACHE.md), [materialized reads](MATERIALIZED_TEMPORAL_READS.md), [native validation](NATIVE_FFI_VALIDATION.md). These have different read, construction and memory boundaries. | Public service throughput or a cross-system performance claim. |
| CoreHR correctness and regression | [CoreHR CI](COREHR_CI.md) records full observed scenario measurements and a separate event-specific managed lifecycle gate. | Full-scale certified managed performance or multi-machine HA. |
| Public mixed reads and writes | [#368](https://github.com/ashione/VelaGraph/issues/368) tracks the reproducible workload, including paired Go end-to-end and Rust query timings. | A delivered benchmark or a current baseline. |

## External benchmark plan

Start with [LDBC SNB Interactive v1](https://ldbcouncil.org/benchmarks/snb/interactive/):
validate a small generated dataset against a reference oracle before measuring a
larger dataset through the supported public API. Record driver/dataset versions,
operation coverage, mismatches and unsupported operations. A smoke run or
partial workload is not an official benchmark result; follow the
[LDBC fair-use and auditing rules](https://ldbcouncil.org/benchmarks/fair-use-policies/)
before making public comparisons. No VelaGraph LDBC result is claimed here.

Record the exact revision, binaries, hardware, topology, warm-up, dataset,
arrival rate, workload, offered/achieved throughput, errors and per-operation
latency percentiles. Compare equal consistency and durability settings. Report
scale-out and recovery under fault separately, without inferring either from
single-host throughput.
