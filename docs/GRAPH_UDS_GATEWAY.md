# Same-host read-only graph gateway v1

This Linux-only library exposes the existing static graph resolver to other
processes on the same host through a Unix domain socket. It is **not** a
cross-host replica transport, production network service, Gremlin Server,
GraphSON endpoint, multi-tenant authorization system, or completion of D3.
The existing D2 TCP/control listeners are unchanged and remain outside this
security boundary. Do not expose those listeners as an alternative graph API.

## Trust and lifecycle

The embedding application owns and keeps the Raft runtime running, constructs
one `RouteResolver`, and passes a clone to `GatewayServer::bind`. All admitted
connections share its existing operation/dispatch pools. The gateway does not
create a catalog or register replicas on behalf of a remote caller.

Use an operator-managed directory owned by the service UID with mode `0700`.
The socket is mode `0600`. Existing socket paths are rejected, not silently
unlinked. Peer credentials must match the configured UID; the client also
checks the expected server UID. This authorizes that OS identity to read the
partitions exposed by the resolver. It is not a per-partition ACL and does not
protect against root or an attacker controlling the trusted UID.

An unclean server exit can leave a stale socket. Confirm the old server has
stopped and explicitly remove that path before restarting. Never delete an
unknown socket merely because connecting to it failed. A restarted server does
not restore sessions. Clients must explicitly open a new session, which may
observe a newer snapshot.

## Protocol and snapshots

Each connection opens at most one read session. Requests are sequential, with
no multiplexing, session tokens, write endpoint, admin endpoint or raw D1 API.
Frames have a four-byte unsigned big-endian length followed by UTF-8 JSON.
Lengths are checked before body allocation. Requests are at most 64 KiB and
responses at most 1 MiB. Response serialization uses a bounded writer rather
than building an unbounded buffer and checking afterward. Compression is absent.

The v1 operations are:

- `open_read`: protocol version `1`, partition ID, and positive `budget_ms`
  not exceeding 60,000. Returns the pinned data sequence, epoch and metadata
  sequence after the existing metadata and data leader barriers.
- `gremlin`: standard Gremlin text executed on that exact retained view.
- `close`: closes the session and releases its resources.

Unknown operations, fields, versions and malformed JSON fail explicitly.
Write/prepare/commit/admin requests and mutating Gremlin are rejected. The
existing official-grammar parser and documented Gremlin subset are reused.
No query-language, stored graph, catalog or Raft encoding changes are made.
The wire representation is project-specific JSON, not GraphSON.

The client includes connection/open work in one total local monotonic budget.
The server fixes its session deadline at open and never renews it on requests.
The remaining budget is passed to the resolver; metadata lookup, candidate
selection, read barriers, execution and successful responses cannot obtain a
fresh operation budget. Existing epoch fencing and query limits remain active.
Loss of leadership never silently moves a retained session to a new snapshot.
The existing limitation that an isolated replica may not know a newer epoch
still applies; this gateway does not add instantaneous revocation.

## Resource and failure contract

At most eight connections are admitted before a connection worker is started.
There is no unbounded user-space connection or work queue. Each connection
executes one operation at a time. Connection workers retain their admission
slots until synchronous work actually exits, including after disconnect or
expiry. The underlying resolver still shares its 32-operation and 32-dispatch
caps across clones. Independently constructed servers/resolvers are separate
pools, not a process-wide quota.

Header/body reads and response writes have bounded deadlines, so a peer cannot
retain a connection indefinitely by trickling bytes or refusing to read.
The configured I/O timeout also bounds idle time between requests, even if
the overall session budget has time remaining. An idle session expires without
requiring another request. Close, EOF and
transport failure discard its view. Disconnect during synchronous query work
is observed when that bounded work returns; it is not a guarantee of preemptive
cancellation. Parsing, allocation, copying and destruction remain cooperative.
No late success or partial/truncated result is substituted for an error.

The query executor retains its 16 KiB query-text limit, row/hop caps, five-second
cooperative query deadline and 64 MiB logical query-memory budget. The response
frame cap is an additional limit: a valid query may fail because its serialized
result exceeds 1 MiB. These are not total RSS/allocator limits. Retained snapshots
and adapter-private work keep their existing separately documented bounds.

The client treats remote errors as terminal for its session and performs no
automatic reconnect, replay or snapshot refresh. Treat
transport/protocol failure as loss of the session and explicitly decide whether
to open a new one. No durable writes are available in v1. A future write protocol
must preserve a durable immutable prepared envelope, including request ID,
placement, expected sequence and mutation bytes, across unknown outcomes and
server restarts; an ephemeral server token or freshly prepared retry is not
sufficient.

## Verification scope

The gateway tests use real child processes on one Linux host, not only in-memory
or Unix socket pairs. They cover fixed versus new connection snapshots, protocol
rejection and length bounds, slow peers, directory/UID checks, capacity, cleanup,
and server restart. Test fixture writes stay outside the public wire protocol.
These tests do not establish cross-host authentication, TLS, tenant isolation,
network partition behavior, production availability or process RSS bounds.

## Dependencies

This slice reuses locked `serde`, `serde_json` and `thiserror` for strict wire
DTOs and typed errors (MIT OR Apache-2.0). Linux `rustix` supplies peer
credentials and nonblocking Unix socket creation without handwritten unsafe
system calls (Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT). These crates
were already in the workspace lockfile; no new crate versions are introduced.
