# Native C ABI and Go SDK (ABI1)

The optional `velagraph-ffi` crate embeds the local engine in the caller's process.
It builds a C `cdylib`/`staticlib`; `sdk/go/native` is the optional cgo binding.
Design and scope: [issue #376](https://github.com/ashione/VelaGraph/issues/376).
The Rust core gains additive controlled/batch methods and two `Error` variants;
consumers using exhaustive error matches must handle cancellation and deadline
errors when updating. This experimental source API does not start a service or require tenant-specific
adapters. Keep header, library and SDK on the same source revision.

## Build and run

Requires stable Rust, a C/C++ compiler, Clang/libclang and Go 1.23 or newer. See
[CONTRIBUTING](../CONTRIBUTING.md#development-environment) for RocksDB dependencies.

```sh
cargo build --locked --release -p velagraph-ffi
# From the repository root; Linux uses LD_LIBRARY_PATH instead.
export CGO_LDFLAGS="-L$(pwd)/target/release"
export DYLD_LIBRARY_PATH="$(pwd)/target/release"
export LD_LIBRARY_PATH="$(pwd)/target/release"
cd sdk/go
go run -tags velagraph_native ./native/example
```

The [Go example](../sdk/go/native/example/main.go) generates a small graph,
executes and validates point, batch and adjacency queries, and releases stores
and prepared plans. Without both `cgo` and `velagraph_native`, the package builds
but returns `ErrUnavailable`; existing pure-Go gRPC clients remain usable.
The header is [include/velagraph.h](../include/velagraph.h). The standalone
[C consumer](../crates/velagraph-ffi/tests/smoke.c) demonstrates calls and ownership.
Default linkage selects the shared library. A static-link application must also
supply platform/C++ dependencies; no portable static-link flags are promised.

```go
store, err := native.OpenMemory(ctx, native.MemoryOptions{
    Identity: "example", Vertices: []native.Vertex{{ID: 1}},
})
if err != nil { return err }
defer store.Close()
result, err := store.Query(ctx, []native.Query{{Text: "g.V(1)"}}, native.QueryOptions{})
```

## Backend contracts

| Capability | Memory (`cow` or `locked`) | Temporal RocksDB |
|---|---|---|
| Initialize | Private full-load, sequence 0; duplicate IDs/missing endpoints reject | Open/reopen existing temporal format; may initialize disk |
| Write | Ordered atomic mutations and metadata, expected sequence | Up to 256 temporal assertions in a synced WAL batch, expected V |
| Read | One current view per query batch; stale numeric sequence rejects | One Rocks snapshot and V/T per batch; explicit T in microseconds |
| Delete vertex | Cascades incident edges | Interval absence assertion; no edge cascade |
| Save | Versioned local checkpoint/restore | Reopen; checkpoint export unsupported |
| Durability | Successful apply is memory-only; explicit checkpoint required | Existing temporal sync-WAL contract |
| Query starts | Supported ordinary graph subset, including bounded global starts | Existing temporal subset; global starts require a supported index |

`INFO` reports capabilities and effective mutation/query-operation ceilings.
No managed-cohort, distributed snapshot, remote routing or cross-partition atomicity
is added. Both backends use the official Gremlin parser and existing executor;
unsupported language or capabilities are errors. Storage IDs are `uint64`, including
zero and `UINT64_MAX`; explicit Gremlin starts and prepared start bindings retain
the existing signed `int64` parser range. Global ordinary rows preserve full IDs.

## ABI1 operations and JSON schema

`vg_abi_version()` returns 1. `vg_call(abi, operation, store, input, input_len,
control, timeout_ms, max_output_bytes, reply)` is synchronous. Scalars/handles are
fixed-width C integers; no Rust struct, string or allocator crosses the ABI.
An unsupported ABI returns `UNSUPPORTED` without touching the output layout.

| Operation | Store argument | Required payload | Success reply |
|---|---|---|---|
| `OPEN=1` | 0 | Memory or temporal object below | `value`: store handle |
| `APPLY=2` | Store | `expected_version`, `mutations`; optional metadata | `value`: committed sequence/V |
| `QUERY_BATCH=3` | Store | `queries`; optional version/T/limits | `value`: read version; `buffer`: JSON result |
| `INFO=4` | Store | Empty byte range | `buffer`: JSON capabilities/state |
| `CHECKPOINT=5` | Memory store | `{"path":"..."}` | No result buffer |
| `PREPARE=6` | 0 | `text`; optional `bind_start_ids`, `value_steps` | `value`: plan handle |

Input objects reject unknown fields. JSON numbers preserve `uint64` IDs and `int64`
properties; clients must not route integers through floating-point decoders.

- Memory open: `{"backend":"memory","identity":"name","mode":"cow",
  "vertices":[{"id":1,"labels":[],"properties":{"n":{"Integer":7}}}],
  "edges":[],"metadata":[0,255]}`. Mode defaults to `cow`. Optional `restore`
  replaces initial records/metadata and checks identity. Optional `limits` fields:
  `max_records`, `max_accounted_bytes`, `max_batch_operations`, `max_batch_bytes`,
  `max_pinned_versions`, `max_pinned_accounted_bytes`; values must satisfy existing
  kernel bounds. Omitted values use kernel defaults; zero-valued Go limit fields
  are omitted by the SDK and therefore select defaults.
- Temporal open: `{"backend":"temporal","path":"/path/to/store"}`.
- Vertex: required `id`, optional `labels` (array of strings) and `properties` (map).
  Edge: required `id`, `from`, `to`, `label`; optional `properties`.
- Scalar property: `"Null"`, `{"Integer":-7}`, `{"Real":1.5}`,
  `{"Text":"hello"}`, `{"Boolean":true}`. Reject non-finite reals and coercions.
- Apply: `{"expected_version":0,"mutations":[{"op":"put_vertex","vertex":{"id":2}}]}`.
  Other mutations: `put_edge` with `edge`; `delete_vertex`/`delete_edge` with `id`.
  Temporal mutations each require `valid:{"from":10,"to":20}`; absent/null `to`
  means unbounded, intervals are half-open. Memory rejects valid-time intervals.
  **Memory metadata is replaced on every apply: omitted or `[]` clears it.**
  Metadata uses byte-number arrays, not base64. Temporal metadata is unsupported.
- Query: `{"queries":[{"text":"g.V(1)"}]}`. A temporal query additionally requires
  `valid_time_micros`; optional `version` selects a historical V. Memory accepts
  only its captured current sequence and rejects valid time.
- Prepared query: `{"prepared":123,"start_ids":[1,2],"values":[{"Integer":7}]}`.
  Use exactly one of `text` or `prepared`. `value_steps` selects the existing
  supported predicate step positions; values are positional typed bindings, not
  textual substitution. A bound-start plan requires nonempty `start_ids`; an
  unbound plan requires it absent. Plans are immutable and store-independent.
- Query `limits`: `max_queries`, `max_rows`, `max_work_operations`, `max_work_bytes`,
  `max_live_bytes`. Positive values can only tighten the common ceilings below.
  The batch ceiling does not override Gremlin's default 100-row result cap; an
  explicit query `limit(...)` selects a larger supported result size.
- Result: `{"version":0,"metadata":[],"results":[{"rows":[{"Vertex":1}]}]}`.
  Row variants are `Vertex`/`Edge` (uint64), `Value` (scalar), `Count` (uint64),
  `Path` (array of `{"Vertex":id}` or `{"Edge":id}`). Temporal metadata is null.
  Empty results are empty arrays; order and duplicates follow the query contract.
- Info includes `abi_version`, `backend`, `identity`, `version`, `metadata`, `mode`,
  `max_graph_bytes`, `capabilities`, `max_batch_mutations`, `max_query_work`.
  Inapplicable fields may be null. Info is diagnostic, not a transaction token
  valid across stores or a guarantee that the next write will succeed.

## Ownership, cancellation and write outcomes

A successful open/prepare/control creation returns an owned handle. Call
`vg_release` exactly once for each C handle; Go `Close` is idempotent. Handle IDs
are process-local, monotonically assigned, typed and never reused. Do not persist
or transfer them to another process. Closing retires a handle;
already admitted operations can finish because they retain ownership. A temporal
store's path can remain locked until its last admitted operation exits.

Results own their bytes independently of store lifetime. Use `vg_buffer_size`,
`vg_buffer_copy`, then `vg_release`. Undersized copies return `BUFFER_TOO_SMALL`
and leave the destination unchanged. There is no borrowed result pointer.
Retained buffer/plan capacity remains charged until the final admitted owner exits,
including an operation racing handle retirement. Pending handles are reserved
before open/prepare, so handle exhaustion cannot discard an acknowledged write.

Controls are monotonic: `vg_control_new`, `vg_cancel`, `vg_release`. Go context
watchers are joined before releasing the control, and the calling goroutine stays
inside the native call until it no longer borrows input. Do not mutate input slices,
maps or options concurrently with a call. Context deadline cancellation maps to
`context.DeadlineExceeded`; ordinary cancellation maps to `context.Canceled`.

Checks surround parsing, view acquisition, query work and safe publication points.
Mutex waits, filesystem operations, parsing/serialization of individual records
and RocksDB I/O cannot be forcibly preempted. A deadline is cooperative, not a hard
latency SLA. Cancellation before publication rejects the whole batch. After the
last safe check, publication can win; successful writes remain successful even
when their context is cancelled. No partial query batch is returned on error.

Status codes: `OK=0`, `INVALID=1`, `HANDLE=2`, `UNSUPPORTED=3`, `LIMIT=4`,
`CONFLICT=5`, `CANCELLED=6`, `DEADLINE=7`, `IO=8`, `INTERNAL=9`,
`OUTCOME_UNKNOWN=10`, `BUFFER_TOO_SMALL=11`, `BUSY=12`, `CORRUPT=13`.
Errors carry bounded inline UTF-8 text (255 bytes plus NUL); branch on codes.

An I/O error or unwind during potentially publishing work may report
`OUTCOME_UNKNOWN`. Checkpoint failure after rename may leave the new file visible;
temporal initialization/write failure may have changed disk. Inspect/reopen state
before deciding whether to retry; the ABI supplies no idempotency key or automatic
retry. Successful write acknowledgments use `reply.value` directly, avoiding any
fallible result-buffer allocation after commit. COW/Locked updates remain atomic.

## Resource limits and process boundary

| Resource | Ceiling/default |
|---|---|
| Input / encoded output | 64 MiB each; output default 8 MiB |
| Aggregate wire reservations and retained capacity | 128 MiB |
| Retained prepared plans | 64 MiB; conservative fee `4 * text_bytes + 4096` |
| Registered handles / simultaneous work calls | 4,096 / 16 |
| Native timeout | Default 5 s, maximum 60 s |
| Query count / cumulative rows per batch | 64 / 10,000 |
| Common batch work operations / work bytes / live accounting | 65,536 / 64 MiB / 64 MiB |
| Temporal revision/candidate visits | Stricter 4,096 ceiling shared across the batch |
| Memory mutations / metadata | 10,000 / 64 KiB; kernel work limits may reject earlier |

Prepared plans reuse parsing across calls and stores; read records and temporal
cursors are reused only inside the current fixed-view batch.
Each batch uses one view, one backend and cumulative budgets; earlier query
results remain charged while later queries run. Registered-handle count excludes
retired resources held by admitted operations. `vg_runtime_stats` reports current
registry counts and reservations; quiesce callers when checking for leaks.
Release, cancel and stats bypass work admission. The Go SDK bounds concurrent
encoding and applies conservative preflight estimates before JSON allocation.

These are logical/work/wire bounds, **not process RSS**. Decoded object overhead,
allocator overhead, RocksDB caches and caller-owned data add memory. Multiple
stores have separate kernel retention budgets: applications must bound aggregate
store memory and lifecycle. JSON ABI1 copies and decodes results; it is not a
zero-copy interface. Panic unwinding is contained at the C boundary, but OOM,
abort, segmentation faults and native crashes share the host process failure domain.
C callers must supply valid, aligned, disjoint readable/writable ranges. The ABI
validates null, alignment and length; it cannot validate arbitrary native pointers.

## Validation and performance method

```sh
cargo test --locked -p velagraph-ffi
cargo build --locked -p velagraph-ffi
python3 scripts/test_native_ffi.py --lib-dir target/debug
cargo clippy --locked -p velagraph-ffi --all-targets -- -D warnings
```

The native workflow runs C interop, Go race tests and non-cgo SDK checks on Linux
and macOS. Default/core profiling CI remains enabled separately. Core tests inject
cancellation before publication and during a later batch query; ABI tests exercise
exhaustion, retired ownership, uncertain initialization and allocation failure.
Go tests cover concurrent close/query/write, context errors and lossless scalars.

For timing, build release artifacts first, then run
`cargo run --locked --release -p velagraph --example native_ffi_baseline` and the
release-linked Go example serially, without overlapping builds or tests. Both use
257 vertices and 256 edges with identical ordered output assertions, 100 warmups
and 1,000 samples per workload. Rust timing includes control/view acquisition and
batch execution with parsed queries; Go timing uses a cancellable context and
includes request encoding, cgo, native execution/serialization, copying, release
and typed Go decoding. Setup,
prepare and correctness checks are outside timing. These generated warm local
measurements isolate embedding overhead; they are not RPC, production or hierarchy
comparisons. See [native validation](NATIVE_FFI_VALIDATION.md) for recorded results.
