# Go managed API interoperability check

This module exercises the [VelaGraph Go client SDK](../../../sdk/go/README.md)
against a fresh managed cohort and loopback Rust gRPC gateway. Its go.mod
uses the SDK in the same checkout so changes can be tested together.

## Generate and build

Use Go 1.23.6, protoc 29.3, protoc-gen-go v1.36.5 and
protoc-gen-go-grpc v1.5.1. Install generators from their official Go modules at
those exact versions. The generation script checks tool versions and never
downloads tools.

From the repository root:

```sh
bash scripts/generate-go-managed.sh
cd examples/go/managed
go mod download
go build -mod=readonly -o interop ./cmd/interop
```

Bindings come directly from
`crates/replicated-graph/proto/public_managed.proto`; there is no separately
maintained Go schema. Generated files live in the SDK at `sdk/go/gen/managedv1`.
Runtime dependencies are pinned in go.mod; go.sum locks their checksums.
CI regenerates bindings before building and runs the Rust fixture with
`VELAGRAPH_GO_INTEROP_BIN` set to the absolute binary path.

## Chainable Graph API coverage

The real-process fixture creates two parallel cross-shard edges and compares
the results before and after the second write. A 128-walk input returns
100 element rows or a count of 128, and an oversized raw protobuf request
fails at the gateway. Set `VELAGRAPH_GRAPH_BENCH=1`
on the Rust interoperability test to also time 40 paired requests per API.
It uses four warmup pairs, then alternates GraphService.Execute with QueryService.Open at the same exact V
on the same warm channel and prints p50/p95. Text Close is timed separately.
The text API retains a session and the typed API returns rows directly. These
timings therefore measure the public endpoints, including capture, validation
and network calls. They do not isolate parser time.

### Reproduce the measurements

From the repository root, with the Go and Rust build prerequisites installed:

```sh
bench_dir=$(mktemp -d)
(cd examples/go/managed && go build -mod=readonly -o "$bench_dir/interop" ./cmd/interop)
cargo test --release --locked -p replicated-graph --lib graph_plan_benchmark_v1 -- --ignored --nocapture
VELAGRAPH_GO_INTEROP_BIN="$bench_dir/interop" VELAGRAPH_GRAPH_BENCH=1 \
  cargo test --release --locked -p replicated-graph --test public_service_go_process \
  go_client_against_real_managed_service -- --ignored --exact --nocapture
```

One local macOS arm64 release run (rustc 1.95.0, Go 1.23.2, same-host loopback)
measured the following times. The Rust microbenchmark used 50 warmups and
2,000 iterations of a two-hop path on repeated IDs. It averaged **658.2 µs**
for the official text parser and fixed-plan admission, and **0.207 µs** for
cloning and validating the typed input. Neither timed loop performs network
I/O, protobuf decoding, capture or execution.

| Two-hop count on two shards, 40 warm paired requests | p50 | p95 |
| --- | ---: | ---: |
| GraphService.Execute | 9.68 ms | 10.91 ms |
| QueryService.Open (text, full count in first page) | 11.16 ms | 12.19 ms |
| QueryService.Close (measured separately) | 0.18 ms | 0.22 ms |

The end-to-end figures include the actual shard reads on a small fixed cohort.
They are a local sample, collected while other tests were running. Plan
construction accounts for only part of the difference between these endpoints.

## Run against a fresh fixture

```sh
./interop -endpoint 127.0.0.1:PORT -binding-proto /path/to/binding.pb \
  -partition 7 -first-id 9007199254740993
```

`binding.pb` is a serialized public protobuf Binding supplied by the Rust fixture.
It contains placement identity, not credentials. Every version-2 RPC supplies a
fresh 16-byte correlation nonce and verifies the echoed identity. Endpoints are
unauthenticated plaintext development endpoints; do not expose them publicly.

The candidate contains the following acceptance checks. Passing remains subject
to the exact candidate's CI; this document does not claim an unrun test passed.

The test checks:

- Unsupported envelope version and malformed Gremlin via standard
  `status.Code` and `status.Details` decoded as `ErrorDetail`.
  Raw custom trailer decoding is deliberately not accepted.
- Internal ManagedService/Call, RaftService/Vote and ShardOperations/Call are absent from the public listener.
- Three fresh typed vertices in the start partition (the third after query capture),
  a fourth vertex on the second shard, two parallel cross-shard edges,
  absent/committed Status, and intentional
  same-ID/same-mutation Submit replay only after confirmed commit.
- Exact integer properties at i64 minimum/maximum and `2^53 + 1`, vertex IDs
  above `2^53`, plus real, UTF-8 text, boolean and explicit null property values.
  A missing property has no value and is not confused with explicit null.
- Open/Fetch/Close over two pages, correlated query/cursor, immutable session
  version, completeness, exact row identities and post-close rejection.
  A write between pages must not enter the retained two-row view; a fresh latest
  view must observe all three rows at a later version.

QueryService uses supported explicit-ID element queries for paging; it does not support valueMap. Property values are verified separately through FixedSessionService.Traverse at the same exact retained V, after the third write. Both session types use independent 32-byte tokens and 16-byte incarnations. Their completeness is explicitly Unverified, not silently promoted to certified adjacency completeness.

All operations share a 90-second overall deadline; each request advertises a
5-second server budget. gRPC automatic retry is disabled. A failed Submit is
reported as uncertain rather than automatically retried. Bounded Status polling
may observe a pending result. A successful run prints one PASS summary and exits
zero; any missing assertion/error detail fails nonzero.

The fixture must be fresh: existing external IDs fail rather than silently
reusing data. This is same-host Rust/Go interoperability evidence only, not HA,
mixed-version upgrade, multi-host performance or production security evidence.

## Error-trailer upgrade

The public server now sends standard `google.rpc.Status` with exactly one
`Any` of type
`type.googleapis.com/velagraph.public.managed.v1.ErrorDetail`.
Go's standard `status.Details()` must decode it without a custom raw-trailer
parser. Code, message and domain reason are checked consistently.

Old preview servers emitted a bare ErrorDetail trailer. Upgrade Rust clients and
servers together: replace direct `ErrorDetail::decode(status.details())` with
`managed_gateway::decode_grpc_error_detail(&status)`. No legacy fallback or
mixed-version guarantee is provided. Total rich-status bytes retain the 1 MiB
limit; structural/identity checks occur before allocation, and the domain client
correlates the complete binding and nonce.

See [public error compatibility](../../../docs/PUBLIC_GRPC_ERROR_COMPATIBILITY.md)
for the exact bounds, uncertain-write semantics and acceptance limits.
