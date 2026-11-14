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

## Proposal and design decision (milestone 1)

Tracking: [issue #281](https://github.com/ashione/VelaGraph/issues/281),
[PR #283](https://github.com/ashione/VelaGraph/pull/283), and milestone 1
(the v0.0.1 release gate). Completion means interoperable public behavior with
evidence for the final candidate, not merely a generated client or merged schema.

### Problem and alternatives

The preview's bare detail trailer gave Rust a domain error but violated the
standard rich-status envelope consumed by other languages. A successful protobuf
build alone could not reveal this wire-level incompatibility.

| Option | Benefit | Cost / decision |
| --- | --- | --- |
| Standard Status with one typed Any | Ordinary gRPC code/details APIs; typed correlation; explicit bounded validation | Coordinated upgrade of the previous Rust trailer decoder. **Selected.** |
| Keep bare protobuf and ship a custom Go decoder | Least server change | Makes a language-specific exception permanent and does not satisfy ordinary gRPC interoperability. Rejected. |
| Encode the reason and identity only in status message | Simple display and no detail DTO | Loses typed fields, invites string parsing, cannot preserve full identity semantics. Rejected. |

A minimal private prost representation of the standard outer messages avoids a
new runtime dependency without changing their wire layout. It does not define a
new proprietary wrapper. The public generated ErrorDetail remains the payload.

### Interface semantics and bounded failure

The contract above is normative: exact Any type URL, one detail, matching
transport/envelope code, matching message/reason, and request-correlated v2
identity. Parsing occurs under the existing total size and recursive budgets,
including the outer envelope, rather than allocating an unbounded wrapper
first. Malformed or contradictory errors fail closed; absence of a domain
detail does not prove that a write failed to commit. There is no automatic
write replay to compensate for decoding or connection failures.

Migration changes public error trailers only. Storage records, canonical
durable commands, internal RPCs, request versions, and query execution remain
unchanged. There is deliberately no old/new trailer fallback. Deploy the
matching public server/client pair; rollback also requires a matching pair,
not one side alone. This proposal does not claim mixed-version rolling upgrade
support.

### Acceptance history and remaining release gate

The review of the initial candidate identified the bare-trailer incompatibility,
so the implementation changed to standard Status/Any rather than weakening the
Go standard-details assertions. A subsequent real Go run on candidate
`2cea8d` reached the first paginated QueryService Open and failed with
`QUERY_UNSUPPORTED`. This was a fixture/planner mismatch: that bounded query
service does not accept terminal valueMap. It was not evidence of integer
precision loss.

The revised test uses two existing supported paths, without expanding the
production planner solely to pass this test:

- QueryService Open/Fetch/Close checks qualified vertex IDs, pagination,
  old-view two-row stability after a third write, and a later latest view.
- FixedSessionService opens the same old V and Traverse returns property maps
  for exact scalar, null-versus-absence, and integer-boundary checks. Its session
  is independent and is also closed.
- Write Submit/Status and confirmed same-ID/same-mutation replay retain their
  original assertions; public/internal method isolation and standard error
  code/details remain mandatory.
- Rust negative-envelope tests cover malformed, oversized, duplicate,
  contradictory, wrongly typed, and structurally invalid identity cases.

These are acceptance requirements and recorded debugging outcomes, not a claim
that the latest combined head has passed. Final acceptance still requires
exact-head generated-code freshness, dependency locking, gofmt, Rust tests,
real-process Go interoperability, independent review, and applicable CI against
the actual merge tree. Record the final run and head in #281/#283 before marking
the milestone gate complete. Same-host proof does not establish HA,
multi-host performance, authentication, or general rolling-upgrade safety.
