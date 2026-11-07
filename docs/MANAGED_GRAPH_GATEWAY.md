# Trusted managed graph TCP gateway

Status: experimental managed TCP facade over server-owned, in-process real Raft
handles. This is an additive restricted API, not a new Gremlin dialect or a
production distributed graph deployment. Existing remote GlobalCommittedSnapshot
Gremlin remains Unsupported. Ordinary current/observed reads are not relabelled
as certified reads.

## Configuration and trust

`replicated_graph::managed_gateway::Gateway` owns `Arc<NodeHandle>` for one
coordinator and every member of one explicitly configured fixed cohort. The
persisted empty-baseline contract is validated before accepting connections. No
remote client can activate roles, change ownership, choose participants, supply
raw Plan/KV bytes, import a baseline, or redirect a partition. Partition `u32`
equals shard `u64`, under the versioned `PartitionEqualsShardV1` mapping.

The plaintext listener must be restricted to a trusted network. There is no
TLS, authentication, authorization, multi-tenant isolation, cluster discovery,
leadership routing, multi-host Raft placement, failover, or public deployment.
The tests bind only loopback. The binding and random tokens are identity checks,
not cryptographic certification or access-control credentials. Trusted low-level
Raft/KV APIs can bypass graph invariants and must not be mixed with graph writes.
External graph queries are standard Gremlin text parsed by the existing official
TinkerPop grammar adapter. Typed graph reads are private execution plumbing only;
the former unpublished typed Read wire is rejected, not translated or retained.

## Requests, identity and unknown outcomes

Each connection carries one big-endian u32 length-prefixed JSON request and one
reply. Protocol v1 requests specify an unpredictable per-call nonce, exact
expected authority/cohort/mapping, and one typed operation. Unknown struct or
enum fields, unsupported versions, and malformed values fail closed. Clients
validate echoed binding, nonce, operation identity, session token and version
before accepting any response. There is no transparent retry or cursor protocol.

Submit accepts a nonzero external 128-bit request ID and one of:

- CreateVertex, CreateEdge, PatchVertex, PatchEdge, DeleteEdge.
- Property values are tagged Null/Integer/Real/Text/Boolean. Integer 1 and real
  1.0 remain different commands. Non-finite real values are invalid.
- Patch set/remove semantics reuse the existing graph/v2 validators; endpoint,
  label and vertex-deletion changes are unsupported.

The canonical command is compact JSON of the ordered tuple
`("managed-graph-command-v1", Binding, Mutation)`, using fixed-field structs,
explicit enum/value tags, sorted property maps and sorted remove sets. It includes
all typed parameters and the exact durable binding, but neither the transient
connection nonce nor gateway incarnation. The external ID separately indexes the
durable request record. Serialization is capped before growth. Changing a typed
command while retaining its external ID conflicts.

Submit first recovers the exact external ID and canonical command. Only when
absent does it acquire a certified snapshot and prepare a candidate. It then
atomically admits the complete Plan plus Begin and executes **only** the Plan
returned by admission. Concurrent/restarted retries retain original CAS, intent,
transaction ID and payload. They do not prepare a replacement against fresh data.
Submit admission/execution is serialized per gateway to reduce out-of-order
prepares, while Status, Abort and reads remain independent. This does not replace
coordinator ordering or a single-writer deployment requirement when multiple
gateways or trusted low-level writers coexist.

Status performs a leader-barrier recovery and inspects the durable decision. It
does not admit, prepare, decide, or catch up participants. Abort recovers the same
request and explicitly resolves it; it never creates an absent request and never
turns an existing Commit into Abort. Replies distinguish Absent, Pending,
Committed/Aborted and visible=true/false, and return the original transaction ID
when known. Known Commit with incomplete cohort application remains Commit.

Absent means no record existed at that barrier observation. A delayed first
Submit may still admit afterwards. Timeout, disconnect, server shutdown or a lost
reply is an **unknown outcome**, never evidence of Abort or permission to switch
identities. Keep the same external ID and exact command for retry/Status/Abort.
An application rejection can leave an admitted transaction Pending. Inspect it
and explicitly Abort if appropriate. Never abort another client's unresolved ID
or silently replace a stale CAS. Pending earlier ordinals can block later global
visibility; there is no automatic orphan recovery policy.

## Fixed-version read sessions

OpenSession captures every cohort shard at one certified version V. An explicit
V is never downgraded. With no V it selects the common safe applied prefix, not
the newest incompletely applied coordinator decision. Partial cross-shard apply
therefore rejects an explicit newer V while an older common version remains
readable. Writes may continue during and after capture without changing any
returned session's V.

Read accepts exactly `{session, query: {partition, text}}`. The partition locates
an explicit `g.V(id)` or `g.E(id)` start, using the same partition-equals-shard
mapping as mutations. It is routing context, not new query syntax. The private
executor consumes `ObservedGremlinQuery::parse`'s official-grammar AST. It never
matches or privately parses query text.

The initial small subset supports one explicit ID, optionally followed immediately
by `outE()`/`inE()` with zero or one label, then `values(key)`, `id()`, `label()`,
`limit(n)` and terminal `count()` as permitted by stream type. A projection can
be followed only by limit/count. Limit steps execute in order. Unsupported starts,
steps and compositions fail explicitly, including on an empty stream. See the
normative [managed-session Gremlin subset](QUERY_SPEC.md#managed-session-gremlin-subset).

Results reuse qualified QueryRow elements/scalars/counts. `g.V`/`g.E` return element
identities; properties require standard `values` projections. Missing properties
produce no row, while stored Null produces a Null value. Adjacency orders by edge
ID and source partition, using canonical source identity even for incoming edges.
The standard default 100-result cap applies after traversal, not before count;
explicit limit controls its own position. Internal resource caps always error.
All graph and edge-half validation uses the retained GraphSnapshot's same V.
QueryData retains Unverified completeness because arbitrary trusted low-level
writes can omit adjacency entries. This is separate from fixed-version consistency
and never turns weak/current reads into certified sessions. Client checks of query
echoes, row shape and qualified identity are not authentication or proof of a
malicious peer's data. No global scan, cursor, partial page or weak-read fallback
is exposed.

Tokens combine a random 128-bit gateway incarnation and a random 256-bit session
identity. Each request includes the exact descriptor and V. Close, TTL expiry or
restart makes the token unavailable; reopening never resumes a previous token.
The monotonic TTL starts before capture, is checked after graph work and response
encoding, and bounds response transmission too. Periodic expiry releases registry
references; an already active blocking worker retains its reservation until it
finishes. Known OpenSession delivery failure, cancellation or encoding failure removes
its new registry entry through a drop guard. A successful kernel socket write
is not proof the peer received the token; an undetectably lost reply is reclaimed
by TTL. A close racing an active read can fail that read closed. Read data
already received cannot be revoked.

## Bounds and lifetime

- Request body: 64 KiB; response body: 1 MiB, including envelope and JSON escaping.
  Lengths are checked before receive allocation; bounded serialization refuses
  growth rather than first creating an oversized encoded result.
- At most eight active server connections/work permits and eight client calls
  per process. Excess server connections are closed. Blocking work retains its
  permit even if the awaiting handler times out.
- At most four materialized snapshots, including write-preparation captures and
  sessions still retained by active workers. Each reserves 4 MiB, conservatively
  charging the session and GraphSnapshot's two copied maps (2 MiB payload each).
  Aggregate reserved copied payload is at most 16 MiB. Four open sessions can
  temporarily prevent a new write's validation capture until one closes/expires;
  already admitted retries, Status and Abort do not require a new capture.
- Each capture also caps aggregate rows at 10,000 and replay work at 100,000
  operations. Map/allocator metadata, bounded retained journals, per-connection
  input/output and cloned bounded results are additional; this is not an RSS cap.
- TTL is caller-selected, positive and at most 60 seconds. A configured total
  connection deadline, positive and at most 60 seconds, includes request input,
  Submit-lock waiting, dispatch, graph work, response encoding and transmission.
  Client timeout also includes connect, encode, send and receive. A timeout does
  not undo a Raft command already submitted.
- Queries retain the official 16 KiB text/64 structural-mark parser admission,
  and a five-second deadline covering parsing, execution and response encoding/
  sending, bounded again by the connection deadline and session expiry.
- Adjacency is all-or-error under the existing 1,000-entry/1 MiB graph bounds and
  the separate 1 MiB encoded response bound. A later limit does not bypass complete bounded adjacency validation.
  No automatic chunking or resource-limit truncation.

The durable coordinator journal retains at most 256 external requests and has
its existing 16 MiB capacity budget. IDs are never evicted/reused and source
history is not garbage-collected. Read tokens are process-local materialized
pins, not durable GC leases, indexed MVCC, or an indefinite availability claim.

## Validation scope

Targeted lib tests use real Raft handles behind actual bounded loopback TCP.
They cover typed create/patch/delete and old/new fixed-version reads during
writes, concurrent same-ID submissions, type/operation conflicts, dropped final
responses, partial apply/common old cuts, Commit-preserving Abort, stale admitted
CAS with explicit Abort, persistent coordinator/participant reopen with original
Plan recovery, gateway reincarnation tokens, close/expiry, binding/token/version
mismatches, strict cursor/field rejection, oversized input/output, retained-budget
exhaustion, connection admission, total deadline, and forged response identities.
Gremlin tests also cover stream-type preflight on empty results, missing versus
Null properties, projection/limit/count order, label order, qualified source IDs,
protocol golden fixtures and old typed-wire rejection. These tests do not constitute multi-host, authenticated, full-workspace CI or
production scalability validation.

## Additive remote placement

`Gateway::bind_remote(RemoteConfig, ...)` replaces owned local node handles with
fixed trusted internal RPC clients. The local constructor and this document's
external wire, canonical commands, durable Plan identities, Gremlin subset and
session/restart semantics are unchanged. The internal transport validates node,
role and complete cohort identity and returns participant-retained terminal-prefix
evidence for same-V captures. See [transport contract](REMOTE_MANAGED_TRANSPORT.md)
for bounds, trust assumptions and real independent-process validation. Earlier
"in-process handles" statements describe the original local placement only.
