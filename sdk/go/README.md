# VelaGraph Go client SDK

Import `github.com/ashione/VelaGraph/sdk/go` as package `velagraph`. The SDK
requires Go 1.23 or later. It includes the generated public gRPC bindings in
`github.com/ashione/VelaGraph/sdk/go/gen/managedv1`. Applications can build
directly against the module without installing protoc or building Rust.

## Use from a separate module

To use a local VelaGraph checkout containing the SDK, run these commands in
your application's module, substituting the path to the checkout:

```sh
go mod edit -replace github.com/ashione/VelaGraph/sdk/go=/path/to/VelaGraph/sdk/go
go get github.com/ashione/VelaGraph/sdk/go@v0.0.0
```

Pass your gRPC connection and the gateway's configured cohort binding to
`NewClient`. The caller owns the connection and closes it when finished.
The function below counts two-hop walks at one exact certified version.

```go
package queries

import (
    "context"

    "github.com/ashione/VelaGraph/sdk/go"
    pb "github.com/ashione/VelaGraph/sdk/go/gen/managedv1"
    "google.golang.org/grpc"
)

func CountReports(ctx context.Context, conn *grpc.ClientConn, binding *pb.Binding, version uint64) (uint64, error) {
    client, err := velagraph.NewClient(conn, binding)
    if err != nil {
        return 0, err
    }
    result, err := client.At(version).V(7, 42).Out("reports").In("reports").Count().Execute(ctx)
    if err != nil {
        return 0, err
    }
    return result.Data.Rows[0].GetCount(), nil
}
```

Reuse the client and connection across queries. Construction and chaining
are local. Each `Execute` invokes one unary GraphService RPC. To disable
configured transport retries, create the channel with `grpc.WithDisableRetry()`.
The managed gateway preview uses plaintext on trusted loopback endpoints.

## Query contract

- Use `client.V(partition, ids...)` for the latest certified version,
  `client.At(v)` for an exact version (including zero), or `client.AtLeast(v)`
  for a minimum version. The response includes the version actually read.
- Supply 1–32 IDs in one start partition. ID order and duplicates are preserved.
  Append up to three `Out` or `In` hops with an optional edge label.
- Execute the chain to return vertices, or append a terminal `Path()` or
  `Count()`. Vertex and path results retain the first 100 rows. Count covers
  the complete admitted traversal, including multiplicity.
- The SDK sets a five-second timeout, shortened by the caller's deadline.
  Server and transport errors propagate through `Execute`. Successful results
  are checked against the call identity, version selector and completeness.
  For `Count()`, the SDK also requires exactly one count row, including when
  the count is zero.

See the [RPC contract](../../docs/PUBLIC_GRPC_SERVICE.md#graphserviceexecute)
for server limits and the [interoperability fixture](internal/interop/README.md)
for end-to-end coverage and benchmarks.

## Development

Run `go test -race ./...` from this directory. After changing the public
protobuf schema, regenerate the checked-in bindings from the repository root
with `bash scripts/generate-go-managed.sh`.

## Optional in-process native engine

`native` is a separate cgo package for embedding local memory/temporal stores.
Enable `cgo` and `velagraph_native`, and link the matching ABI1 library. Default
pure-Go builds retain the existing gRPC SDK and get `ErrUnavailable` from native
operations. See [native build, ownership and limits](../../docs/NATIVE_FFI.md)
and [executable example](native/example/main.go).

`native.Store.Query` returns owned Go rows through the packed result path.
`QueryView` retains immutable native bytes for direct ID/scalar/path access and
text comparison; Close the view explicitly. `Text`, `Metadata` and `Materialize`
copy only when Go-owned output is requested. Input JSON and native kernel result
construction remain; this is zero-copy access across the result boundary.
