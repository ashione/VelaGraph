# Typed fixed-version query sessions over gRPC

> Current transport baseline: main `40e2b9a` (#279, 2026-11-14). Supported
> graph, coordinator, Gateway, managed-member, Raft and control network paths use
> typed gRPC; old JSON/TCP library listeners and fallback are retired. Public HTTP
> remains intentional. Historical transport details below are not a supported
> compatibility mode. See [current RPC audit](RPC_MIGRATION_STATUS.md).


The mandatory typed graph endpoint includes a distinct
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
redirected to a legacy wire. Existing constructors now use the same typed transport.

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
The external Gateway now uses typed public RPC. Managed writes, control RPC,
Raft and observed hierarchy job orchestration also use typed services. Restore
and production-readiness work remain separate. No storage format changes are introduced.

The remaining callers and legacy dispatch were migrated in #279, using the same
execution, proof validation, lease state and admission implementations rather
than two independent query engines.

Validation uses real managed node processes and typed gateways,
fixed-V query/result comparisons and the existing renewal and failure contracts.
Focused passing tests are not claims of production readiness, multi-host
performance, authentication or a completed full protocol migration.
