# Graph library typed RPC migration (#278)

This change retires the graph library JSON/TCP wire, not the graph execution
engine. `GraphService::start`, `start_identified` and `start_managed` now bind the
same typed graph adapter as graph-node. `GraphState` remains listener-free and
shared session/admission pools still bound all adapters. There is no protocol
sniffing, old listener or fallback. Existing deployed old-wire clients must be
migrated together with servers; mixed old/new wire peers are not supported.

## API changes

- Supply configured `RemoteIdentity { group, node }` on every graph call. The
  former unidentified `remote::snapshot`, `metadata` and `write` helpers are
  removed; use their `_identified` variants. Optional-identity paging/query
  wrappers reject `None`. Group zero is not permission to invent a node identity.
- Single-shot Snapshot keeps its original 2 MiB / 100,000-row bounds, leader
  barrier, order and epoch validation. It is not silently replaced with the
  larger retained paged-snapshot contract.
- Snapshot Open/Page/Close, observed query fragments, hierarchy jobs and fixed /
  renewable sessions use existing typed services and original domain handlers.
  Shared channels replace the old hierarchy connection protocol. Reconnecting
  does not renew a session, reset cumulative budgets, recapture a snapshot or
  replay an application operation.
- ShardOperations adds typed original D1 managed-mode Inspect/Enable/Write/Status
  and single-shot Snapshot messages. These are distinct from the restricted
  cohort transaction services. Existing protobuf field numbers are unchanged.
- Convenience clients create lazy channels so application admission precedes
  connection/HTTP2 handshake. The caller's remaining absolute budget covers
  transport and decoding. Unknown mutation outcomes retain original IDs; there
  is no automatic mutation replay.

## Validation boundaries

Semantic tests now use real protobuf/HTTP2 peers or two typed listeners sharing
one state. Controlled peers preserve error, ordering, concurrency, cancellation,
lease and memory-retention assertions. A separate raw HTTP2 fault peer emits an
oversized gRPC length prefix without a body, or withholds the ACK after receiving
an entire typed write. Pure retired JSON frame codec/scheduler tests are removed;
they are not claimed as tests of gRPC. Durable JSON formats and logical payload
accounting are not network compatibility modes and remain unchanged.

The issue and PR record exact-head code generation, formatting, independent
review and CI/CoreHR evidence. Source changes alone are not a passing gate or a
production-readiness claim. #218 and #247 remain open until their own acceptance
criteria are met. Public HTTP, Raft semantics, on-disk formats, and object-store
HTTP are outside this retirement.
