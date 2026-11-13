# Public managed service: first implementation boundary

Status: **target design with implemented loopback preview slices**.
The [HTTP preview](PUBLIC_HTTP_PREVIEW.md), [typed gRPC slice](PUBLIC_GRPC_SERVICE.md)
and [service host](PUBLIC_SERVICE_CLI.md) reuse a shared invocation/delivery seam.
The final stable HTTP/schema, clean startup and full release gates remain open. Reviewed against
main `fa202c51` on 2026-11-13. The source of truth for runnable endpoints is the
linked preview/CLI contracts, not the proposed stable routes below.
Tracks [#218](https://github.com/ashione/VelaGraph/issues/218),
[#221](https://github.com/ashione/VelaGraph/issues/221), and the
[v0.0.1 acceptance gate](RELEASE_PLAN.md). No release gate is marked complete.

## Decision and reference systems

Use one managed application service with two thin public adapters: typed
`velagraph.public.managed.v1` gRPC and HTTP/JSON. They call the same in-process handler,
not one another over an extra loopback TCP hop, and do not select independent
write or query engines. Internal node/control/Raft services stay on separate
listeners and cannot be reached through public method dispatch.

This follows the useful boundary in
[etcd's gRPC gateway](https://etcd.io/docs/v3.6/dev-guide/api_grpc_gateway/): HTTP
and gRPC expose one model, including consistent error representations. It does
not copy etcd's consistency guarantees or propose embedding its Go gateway in
this Rust service. Define the public protobuf first; use its documented JSON
mapping rather than two hand-maintained semantic schemas. Integer IDs and
versions must remain lossless for JavaScript clients (decimal strings in JSON),
and bytes use base64. Null, absent and finite real values remain distinct.

[gRPC deadlines](https://grpc.io/docs/guides/deadlines/) require the remaining
budget to decrease across calls; a fresh timeout at each internal RPC is not
end-to-end deadline propagation.
[gRPC cancellation](https://grpc.io/docs/guides/cancellation/) also leaves the
application responsible for terminating its work. Dropping a response future
is not proof an already-started mutation or blocking worker stopped.

[Neo4j Bolt's explicit PULL/DISCARD lifecycle](https://neo4j.com/docs/bolt/current/bolt/message/)
is useful evidence that result consumption and execution need separate state.
Here, immutable bounded pages and explicit close retain VelaGraph's existing
contract. Do not copy Bolt's discard/commit semantics into managed writes.

## Verified source seams and gaps

| Existing main seam | Reuse | Required change before an endpoint claim |
| --- | --- | --- |
| `managed_gateway::State::execute` | Typed Submit/Status/Abort and certified fixed-V operations | Extract a crate-private shared application entry, without a public raw `Operation` tunnel. |
| `managed_gateway::canonical` | Exact original external ID, binding and canonical command | Convert both wire schemas into the same domain mutation before canonicalization. Never hash protobuf bytes. |
| `serve`, `OpenDelivery`, retained guards | Admission held during work, new-session cleanup on failed delivery, expiry checks during send | Move delivery ownership into a transport-neutral guard held through response-body completion/drop; returning a handler future is too early. |
| `OpenLocalQuery`, `OpenLocalQueryAfter`, `FetchLocalQuery` | Complete bounded execution, one certified V and replayable immutable pages | Preserve the actual query subset, full-result metadata and owner-bound cursor. No observed fallback. |
| `Gateway::bind_config` | Validates configuration and certified zero baseline before binding | Baseline V=0 is not evidence that a requested restored cut has loaded. Add target-specific startup/readiness state; do not advertise global restore. |
| `State::connections`, `memory`, `submit` | Shared finite admission, retention, serialized submit | All adapters must consume these same budgets. Two protocols cannot double capacity or create unbounded queues. |

Internal observed/fixed transports (#228/#230), the public adapters/CLI
(#232/#234), managed control (#237), Raft peers (#239), observed hierarchy shard
jobs (#236) and RPC-only service routes (#243) are merged. Public HTTP/gRPC share
`Invocation` and delivery ownership; `graph-service` uses gRPC managed/graph
clients. Remaining legacy ingress/listeners are tracked by #218. These slices do
not implement stable HTTP v1, global restore readiness or complete the release.

The seam table above records the design responsibilities: the shared handler,
canonical mapping and delivery guards now exist; target-specific restore readiness
and the final wire contract still require implementation/acceptance.

## Finite first endpoint contract

One fixed topology, one gateway incarnation, explicit loopback listeners and
explicit development-only opt-in. No non-loopback fallback, public deployment,
authentication claim, multi-gateway routing or production security claim.

| Public RPC | HTTP action (proposed) | Domain action |
| --- | --- | --- |
| `SubmitWrite` | `POST /v1/writes:submit` | `Submit` with nonzero external ID and typed supported mutation |
| `GetWriteStatus` | `POST /v1/writes:status` | `Status` with the same ID **and exact mutation** |
| `AbortWrite` | `POST /v1/writes:abort` | Existing explicit `Abort`; never inferred from disconnect |
| `OpenQuery` | `POST /v1/queries:open` | `OpenLocalQuery` or `OpenLocalQueryAfter` |
| `FetchQuery` | `POST /v1/queries:fetch` | Existing `FetchLocalQuery` |
| `CloseQuery` | `POST /v1/queries:close` | Existing `CloseLocalQuery` |
| `GetServiceState` | `GET /v1/service` | Liveness/readiness and immutable capability metadata |

These routes are proposed, not runnable quickstart commands. Supported writes
are the existing managed mutations, not arbitrary Gremlin update scripts, raw
KV operations, participant plans, checkpoint installation or membership changes.

`OpenQuery` accepts standard supported Gremlin text and qualified start
partition, page size and lease. Its read selector is an explicit oneof:
latest certified, exact V, or minimum V. Zero is a valid exact version, not the
absence sentinel. The minimum-V branch maps only to the existing supported
short-query path. Exact V plus minimum V is invalid, not silently prioritized.
Historical business time, custom profiles and renewable jobs are separate later
API slices, with no silent conversion to ordinary reads.

Latest selects one certified safe prefix for the complete configured cohort.
The response includes that V and the current completeness classification.
Certified same-V is not linearizability and does not certify adjacency against
arbitrary trusted low-level writers. Never upgrade `Unverified` to `Complete`.
All pages keep the original V and result; cursor expiry or owner loss is an
error, not an implicit rerun against latest. Preserve original 60-second session
lifetime and current row/byte/operation ceilings; paging is not a larger query.

## Response and failure semantics

Use typed public messages for values, qualified entities, write statuses,
sessions, query rows and domain errors. Do not expose serialized Rust/OpenRaft
enums as the public wire contract or put arbitrary JSON inside protobuf bytes.
Version the public package independently from durable storage codecs.

- Write status retains Absent, Pending, Committed and Aborted. Committed/Aborted
  carry ordinal and visibility separately; committed-but-not-visible is not
  turned into a failed transaction or a successful read-your-writes promise.
- Absent is an observation, not permission to use a fresh external ID. A delayed
  request can still be admitted. Retry/recover the same ID and exact mutation.
- Stable domain error details accompany transport status. Invalid input maps to
  INVALID_ARGUMENT / HTTP 400; unsupported to UNIMPLEMENTED / 501; identity or
  precondition conflict to FAILED_PRECONDITION / 409; capacity to
  RESOURCE_EXHAUSTED / 429; not-ready/unavailable to UNAVAILABLE / 503;
  deadline to DEADLINE_EXCEEDED / 504. Preserve the distinction between expired
  session and bad identity. Never parse error strings to select these codes.
- For admitted or admission-uncertain writes, lost response, deadline and cancel
  mean **outcome unknown**, not Abort. Client documentation must require status
  or exact-ID recovery even if no application error body arrived. No generic
  proxy/gRPC automatic replay or hedging of writes.
- Existing protocol errors that conflate causes are not evidence for a more
  precise public status. Preserve Unknown unless the application actually knows
  no write was admitted. Read cancellation is separate from write outcome.

## Deadline, admission and delivery ownership

Compute one monotonic end at request admission using the minimum of configured
method limit and caller deadline. Body reading, decoding, any admitted queue,
execution, encoding and response delivery all consume this same end. Check it
again before any immediately-ready side effect or send. RPC timeout and retained
session/job lease are separate clocks; a new fetch cannot refresh the query V.

Admission happens before reading/decoding an unbounded body. Enforce encoded
bytes, repeated-element/cardinality and domain bounds before attacker-controlled
allocation amplification. Decode JSON incrementally with a hard request cap and
reject ambiguous duplicate keys/fields; protobuf required oneofs/presence and
finite-value validation use the same domain rules. Bound connections, in-flight
requests and any waiting queue separately; HTTP/2 multiplexing does not grant
unlimited application work. Keep existing request/response budgets until a
separately measured change justifies new ones.

A shared `Invocation` owns the work permit and a `DeliveryGuard`. New query
sessions are registered under that guard; cancel, encode failure, expired lease
or failed body delivery removes/invalidates newly created state. Existing fetch
sessions are not recreated or refreshed. A successful kernel/body handoff still
cannot prove client receipt; unknown delivered tokens expire under the existing
lease. Retained payload guards stay alive until body completion/drop. Blocking
work retains its own permit until it truly ends and checks cancellation/deadline
where supported; do not count a dropped async future as freed capacity.

This extraction is the first executable slice. It must preserve TCP behavior
under differential tests before the adapters are wired. It must not return only
`Reply` and release reservations before transport encoding.

## Readiness and restore gate

Use one owner-controlled target lifecycle, initially NotReady. A listener being
alive only answers liveness and cannot make the graph READY. Configuration,
expected authority/cohort/epoch and required data loading/certification all
precede admission of writes and queries. An in-process operator cannot bypass
this with a public `set_ready` flag or HTTP endpoint.

For this first service, supported startup is the existing verified live managed
cohort. **Offline restored outputs remain unsupported and NotReady.** Do not
remove their startup marker or synthesize a global restore certificate. A
future validated restore controller may move its target through Restoring and
Certifying to Ready only after every required checkpoint component and agreed
cut is loaded and verified. Missing/mismatched members or corruption fail closed.
This does not implement or bypass #124 or #191.

Readiness is advisory, never a reusable proof for subsequent requests: each
query still acquires its own certified view and every write uses existing
identity validation. If the target is explicitly withdrawn/stopping, reject new
work and invalidate/cancel affected retained work before exposing that state as
available again. Healthy unrelated targets are not taken down by a target-local
restore. The initial single-target service does not claim this multi-target
feature is implemented.

## Parallel implementation and finite acceptance

1. **Shared runtime owner:** extract bounded invocation/delivery lifecycle from
   `managed_gateway.rs`; preserve ordinary TCP tests and add cancellation/body
   drop/capacity tests. Own this file exclusively during the extraction.
2. **Schema/adapter owner:** public versioned protobuf, documented JSON mapping,
   tonic and HTTP adapters. Work against the agreed shared invocation contract;
   use a maintained HTTP stack, never a hand-written HTTP parser. Check licenses
   for any added direct dependency and check generated code in (no build-time
   protoc). Both adapters share the same handler instance and budget.
3. **Verification/CLI owner:** process fixture and one-command loopback startup,
   gRPC/HTTP clients, independent result oracle and schema golden vectors. Do not
   concurrently rebuild into another worker's mutable target outputs.

Acceptance uses actual separate node processes, both real transports, and the
same workload/source/build. Include at least:

- Create two vertices and an edge; query via both protocols, compare typed rows,
  order, V and completeness. Retry the same write over the other protocol and
  prove one original identity/decision. Same ID with changed mutation fails.
- Concurrent write: old query pages stay at old V; new minimum-V query waits for
  the required certified prefix or fails, never returns a lower V.
- Lost ACK/connection drop around Submit; recover exact ID/command. Assertions
  do not infer abort from the transport error or from one Absent observation.
- Unsupported Gremlin and mutation, absent/Null and integer/real distinction,
  u64/i64 extremes, nonfinite reals, duplicate keys and oversized collections.
- Both protocols together hit the **same** admission/retention ceiling. Slow
  readers, deadline during body receive/encode/send, client cancel and shutdown
  cannot leak an unbounded queue or free active worker reservations early.
- Session fetch/close across protocols on the same owner works; wrong owner,
  restart, modified cursor/query, expiry and close cannot recapture another V.
- Uncertified startup and offline restore marker reject queries/writes through
  both protocols; liveness may succeed while readiness does not. No mock-only
  test is counted as verified global restore.

Record focused checks separately from complete release CI, and attach failures
as well as successes. The implemented preview has independent-process dual-protocol
coverage in `public_service_process`, plus adapter-level lifecycle tests. See
[the exact implemented matrix](PUBLIC_SERVICE_CLI.md#process-acceptance-scope).
That does not complete every target-design requirement above, the final HTTP
contract, clean cluster bootstrap or restore readiness. README may link this
bounded preview with its prerequisites; release acceptance remains separate. No performance, production or full-gRPC-migration claim is
inferred from introducing HTTP/2.
