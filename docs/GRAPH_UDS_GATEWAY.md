# Unix read gateway retirement (#262)

The experimental Linux-only `replicated_graph::gateway` module and its
length-prefixed JSON Unix socket client/server are removed. This is an explicit
breaking library API removal before v0.0.1, not a compatible wire upgrade.
There is no replacement Unix listener, fallback transport, automatic reconnect,
or automatic exposure of another endpoint. Stored graph, catalog and Raft data
formats are unchanged; no data migration is required.

## Migration boundaries

- Embedders can keep using `routing::RouteResolver::read_view` and its retained
  `RoutedReadView`. It pins the chosen snapshot, placement and original absolute
  deadline. The application keeps the Raft runtime alive and calls synchronous
  graph operations through `run_blocking`. Dropping all view clones releases
  the session. This is an in-process API, not a new network gateway.
- Configured managed cohorts can use [graph-service](PUBLIC_SERVICE_CLI.md)
  through public HTTP or typed gRPC with the same managed handlers. These APIs
  require the documented cohort/baseline and supported query contracts; they
  are **not** a drop-in arbitrary single-partition UDS session replacement.
- The [typed storage/routing](RPC_STORAGE_ROUTING.md) and observed coordinator
  services are trusted internal/development APIs with separate consistency
  contracts. A fresh query is not a substitute for several reads on one pinned
  view. Do not expose internal administrative services as a public replacement.
- Unix directory mode and peer-UID checks had an OS-local trust boundary.
  Plaintext loopback RPC does not reproduce it. Keep services on the documented
  trusted development boundary; this retirement adds no authentication or TLS.
- An old application's stale socket file is operator-owned. Stop the old
  process and inspect/remove the path explicitly; the new library does not
  unlink it or silently bind another port.

## Regression inventory

The removed `tests/gateway.rs` suite exercised a removed protocol: Unix path and
UID checks, JSON framing/length rejection, socket idle/slow-peer handling and
Unix-specific connection admission/restart. Retaining those tests would retain
the obsolete server. Their deletion is **not** evidence of equivalent gRPC
security or resource bounds.

Domain behavior stays covered by the unmodified routing implementation and
retained `tests/routing.rs` suite:

- `failover_keeps_retained_pages_and_queries_historical_then_fences_epoch`:
  an old view remains at sequence 2 after a new write and leader change, while
  the fresh view is sequence 3; both Gremlin counts and old pagination are
  checked. Mutating Gremlin is rejected; epoch fencing remains explicit.
- `routed_crud_and_retained_session_absolute_deadline`: retained view and clone
  expire without deadline renewal, while a fresh view works.
- `independent_expired_sessions_still_consume_slots_and_runtime_rejects`:
  expired but retained handles still charge admission and release on drop.
- Typed service process tests remain separate gates for their actual protocols;
  their success does not imply Unix peer-UID authorization or a complete #218.

Historical benchmark documents mentioning Unix tests describe their original
runs, not a currently available API. The remaining legacy library managed and
coordinator transports remain separate #218 retirement work.
