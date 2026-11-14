# Public managed gRPC error compatibility

This change covers the public `velagraph.public.managed.v1` services only.
It does not change HTTP preview responses, internal Raft/managed/graph error
protocols, durable commands, storage bytes or query semantics.

## Standard rich status contract

An application error uses the normal gRPC status code and message and a
`grpc-status-details-bin` trailer containing the standard protobuf
`google.rpc.Status` layout:

- `code` (field 1) equals the gRPC status code.
- `message` (field 2) equals the gRPC status message and the stable domain
  reason name, such as `QUERY_SYNTAX`.
- `details` (field 3) contains exactly one `google.protobuf.Any`.
- Its `type_url` is exactly
  `type.googleapis.com/velagraph.public.managed.v1.ErrorDetail`.
- Its `value` is the existing typed public `ErrorDetail`, including the
  correlation identity when applicable.

The server's small private prost DTOs encode these standard layouts; no custom
outer envelope or JSON tunnel is introduced. Standard generated Go clients use
`status.Code(err)` and `status.Convert(err).Details()`; the decoded detail must
be `*managedv1.ErrorDetail`. Application reason and transport code must agree.
For example, `QUERY_SYNTAX` maps to `InvalidArgument` and
`SESSION_UNAVAILABLE` maps to `FailedPrecondition`.

## Coordinated client/server upgrade

Previously this preview put bare `ErrorDetail` bytes directly in the trailer.
That was not the standard `google.rpc.Status` envelope and could not be decoded
correctly by ordinary Go rich-status helpers. This correction is intentionally
incompatible with that old trailer interpretation.

Upgrade public servers and Rust clients together. Rust integrations must replace
`ErrorDetail::decode(status.details())` with the strict
`managed_gateway::decode_grpc_error_detail(&status)` helper, or independently
decode and validate the standard envelope. Rebuild against the matching revision.
An old server's bare trailer is rejected; there is no format sniffing, fallback
decoder or automatic downgrade. This is not a mixed-version rolling-upgrade
guarantee. The protobuf public package and request versions remain unchanged:
request v1 omits identity, while v2 requires it.

Uncertain writes must not be resent with a different external ID or altered
mutation. The integration test deliberately replays the identical ID and mutation
only after it has verified the original committed decision. A transport failure
or missing detail is not evidence that a write aborted.

## Bounds and correlation

Before prost allocates the decoded envelope, a schema-aware scan checks the
complete nested Status/Any/ErrorDetail structure. The existing 1 MiB total
response limit remains; the status message is additionally limited to 128 bytes.
Unknown fields, duplicate singular fields, multiple Any details, a different
type URL, malformed/truncated bytes and invalid reason/code combinations reject.
Existing recursive field/message budgets and the identity's 64-member bound
remain. Nonces and mode IDs retain their exact 16-byte widths.

The outer code must equal the gRPC code; the detail reason must map to that code.
Both message copies must agree with the reason name. The standalone decoder
checks the envelope and identity structure, not identity against a particular
request. The domain client separately checks the nonce and complete binding
against its original request. Identity is correlation and placement information,
not authentication. Public endpoints remain unauthenticated loopback development
services.

Errors raised before application decoding or by a disconnected transport may
have no application detail. They retain their existing transport/error handling;
they do not become a fabricated terminal write result.

## Verification and limits

The Go example and Rust harness add executable acceptance checks, not a claim
that every check has already passed. Acceptance requires the exact candidate's
CI results, including its independently running Rust service and generated Go
client. See [the Go example](../examples/go/managed/README.md) for commands.

The candidate checks `i64::MIN`, `i64::MAX`, and `2^53 + 1` as integer
properties, plus vertex IDs above `2^53`, without float or JSON-number
conversion. Explicit null remains distinct from a missing property's zero
values. After opening a paginated query at V, it commits a third vertex: the
retained session must still return its original two rows at V, while a newly
opened latest view must have a later V and three rows.

This demonstrates the intended bounded same-host interoperability contract.
It does not establish arbitrary-language SDK coverage, public/internal feature
parity, rolling upgrades, multi-host performance, HA or production readiness.
