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
  -partition 7 -first-id 900001
```

`binding.pb` is a serialized public protobuf Binding supplied by the Rust fixture.
It contains placement identity, not credentials. Every version-2 RPC supplies a
fresh 16-byte correlation nonce and verifies the echoed identity. Endpoints are
unauthenticated plaintext development endpoints; do not expose them publicly.

The test checks:

- Unsupported envelope version and malformed Gremlin via standard
  `status.Code` and `status.Details` decoded as `ErrorDetail`.
  Raw custom trailer decoding is deliberately not accepted.
- The internal ManagedService method is absent from the public listener.
- Two fresh typed vertex writes, absent/committed Status, and intentional
  same-ID/same-mutation Submit replay only after confirmed commit.
- Integer, real, UTF-8 text, boolean and explicit null property values.
  A missing property has no value and is not confused with explicit null.
- Open/Fetch/Close over two pages, correlated query/cursor, immutable session
  version, completeness, exact row identities and post-close rejection.

All operations share a 90-second overall deadline; each request advertises a
5-second server budget. gRPC automatic retry is disabled. A failed Submit is
reported as uncertain rather than automatically retried. Bounded Status polling
may observe a pending result. A successful run prints one PASS summary and exits
zero; any missing assertion/error detail fails nonzero.

The fixture must be fresh: existing external IDs fail rather than silently
reusing data. This is same-host Rust/Go interoperability evidence only, not HA,
mixed-version upgrade, multi-host performance or production security evidence.
