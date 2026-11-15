# Go managed API interoperability check

This is a generated-client integration example, not a production SDK or cluster
bootstrap. The Rust harness starts a fresh real managed cohort and loopback public
gRPC gateway, then invokes this binary. No mock server is used.

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
maintained Go schema. Generated files live under `gen/managedv1`.
Runtime dependencies are pinned in go.mod; go.sum locks their checksums.
CI regenerates bindings before building and runs the Rust fixture with
`VELAGRAPH_GO_INTEROP_BIN` set to the absolute binary path.

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
- Three fresh typed vertex writes (the third after query capture),
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
