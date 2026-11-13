# Typed fixed-version query sessions over gRPC

This extends the optional internal gRPC endpoint with a distinct
`FixedQueryService`. It transports the existing fixed-version and renewable
query session operations; it does **not** replace managed transaction control,
Raft, the public gateway wire protocol, or the observed-query coordinator.

## Explicit deployment choice

Use `Gateway::bind_remote_local_queries_grpc(config, routes, bind, deadline,
parallelism, member_parallelism)` to select gRPC graph endpoints. Each
`LocalGraphRoute.endpoint` must be the explicitly configured gRPC port, while
`RemoteConfig` continues to contain the separate managed-control Clients.
Identity matching is checked before binding. No port is inferred from another
port, selected by a query, probed, retried at application level, or silently
redirected to TCP. Existing constructors explicitly retain their TCP transport.

The server uses `GrpcSnapshotService::start` with a `GraphService::start_managed`
source. The source and gRPC service share the real query session registry and
operation admission. Snapshot, observed-fragment and fixed-fragment services
share the endpoint, but their session and consistency contracts remain distinct.
A cloned tonic channel reuses a connection; transport reconnect is not request
replay or permission to reopen a lost session.

## Protocol and unchanged execution contract

The operations are OpenFixed, OpenRenewable, Renew, FragmentFixed and CloseFixed.
All request/response fields, including the complete terminal-prefix evidence,
cohort members, modes, authority, capture accounting, profile, lease generation
and request sequence, are typed Protobuf fields. There are no JSON payloads.
Normal builds consume checked-in generated code and do not require protoc.

Before Protobuf decoding allocates collections, both fixed-service clients and
servers scan the encoded message against collection and proof-size bounds. The
new fixed service deliberately rejects duplicate known singular fields and
Protobuf groups instead of accepting merge semantics that could bypass those
bounds. Ordinary generated messages encode these fields once; unknown fields
remain supported within the bounded message. Checked proof validation still
runs after decoding. Fixed-fragment and close replies retain the legacy
`MAX_QUERY_RESPONSE_BYTES + 1024` limit; proof-bearing opens keep the separate
full-frame limit.

Terminal-prefix transport conversion goes through a checked D2 boundary. It
retains the wire format version and validates the same complete, contiguous
terminal prefix rather than deriving a new version or accepting a boolean claim.
Segment/member/prepared/byte bounds and the existing proof/state comparison must
still pass. These proofs certify the existing protocol's history; they do not
provide network authentication or expand the consistency guarantee.

The gateway still selects and certifies V through the existing managed-control
path, pins that V at every graph node, compares the full retained proof and
identities, and applies the same aggregate capture and execution budgets. The
parallel executor dispatches through an explicit selected client without
changing its scheduling or accounting. Historical and latest selection retain
their original contracts; this transport does not add a new read-consistency mode.

- Fixed-fragment sequence numbers are strictly advancing. Duplicate/out-of-order
  fragments retain the existing failure/poison behavior, not an invented replay
  guarantee. Renewal generation replay retains its original grant semantics.
- Deadline narrowing includes existing preflight work. Lease/hard-lifetime
  origins, generation validation and expiry are not reset at the transport.
- Closing, expiry and cancellation keep the existing shared resource lifetimes.
  Caller cancellation is not evidence that remote work rolled back or never ran.
- Application D1 errors remain typed, including capture/history limits and
  authority/fencing errors. Transport failure does not mean transaction Abort.
- Existing resource caps and conservative legacy-encoding accounting remain;
  those accounting checks are not a JSON wire transport. gRPC flow control does
  not replace shared operation/byte/session admission.

## Limits and migration sequence

This remains an unauthenticated plaintext trusted-development-network service.
The external gateway still uses its original protocol. Managed writes, control
RPC, restore, Raft, public HTTP/gRPC service APIs and observed hierarchy job
orchestration are separate migrations. No storage format changes are introduced.

Once the remaining callers use the explicit typed services and the equivalent
semantic/resource tests pass, remove their replaced legacy dispatch. Do not keep
two independent query engines. This slice deliberately uses the same execution,
proof validation, lease state and admission implementations.

Validation uses real managed node processes with separate TCP/gRPC gateways,
fixed-V query/result comparisons and the existing renewal and failure contracts.
Focused passing tests are not claims of production readiness, multi-host
performance, authentication or a completed full protocol migration.
