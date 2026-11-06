# Static remote catalog and graph routing (development only)

This increment connects the existing immutable catalog and bounded resolver to
independent TCP server processes. It closes the normal partition-aware path:
metadata quorum snapshot → fixed data group/replica selection → graph snapshot,
prepared CRUD, and client-side standard Gremlin. It is **not D3 completion**:
no route cache, membership changes, automatic failover guarantees, migration,
cross-partition traversal, distributed transactions, or production security.

## Configuration and startup

Start each metadata/data group independently using `graph-node` from
[REMOTE_GRAPH.md](REMOTE_GRAPH.md), adding `--group <group-id>` to every process.
Assign a dedicated shard and epoch to each group, distinct data directories and
Raft/control/graph sockets to every process, and bootstrap each group's voters
separately. Reusing node IDs across groups is supported. The fixed bootstrap
membership and its Raft peer addresses remain the operator's responsibility.
Never point a group's Raft bootstrap at another group's ports.

`--group` defaults to zero for existing single-group clients. The CLI writes a
versioned `graph-service-identity.json` marker containing group/node before
opening its databases. Reopen must match exactly. Missing markers in nonempty
legacy directories, malformed markers and differing IDs fail closed, including
for group zero: automatic adoption/migration is not supported. Keep old binaries
with their old directories or create fresh prototype groups. This marker is not
a multi-process directory lease. Graph/D1/catalog/Raft data encodings are unchanged.

Configure `routing::RemoteReplica { group, node, endpoint }` entries for every
metadata replica and every registered data candidate. Construct
`RouteResolver::new_remote(metadata_placement, &metadata_nodes, &data_nodes)` on
a live Tokio runtime, then invoke synchronous operations using `run_blocking`.
The metadata list must exactly match its sorted replica IDs. At most 31,744 data
endpoints are accepted; duplicate `(group,node)` or socket addresses, and data
entries in the metadata group, fail before I/O. Every replica of a selected
placement must be configured; absent partitions and absent replicas have distinct
typed errors. Metadata must be available even to establish an unknown partition.

For initial registration, use `Catalog::new_remote(endpoint, node_id, metadata,
timeout)` on the metadata leader, then its existing `prepare_registration` and
`commit_registration` APIs. Select that endpoint from the fixed configuration
(the legacy control status may identify its node ID). Preserve the original
prepared registration for exact retries. The same CAT1 record, immutable binding
validation, capacity caps, request ID and sequence CAS are used for local and
remote catalogs. Registration does not initialize data groups or verify their
configured membership. Lookup and metadata decoding fail closed on identity,
epoch, corruption, unavailable quorum, or capacity errors.

## Identity, leader selection and bounded operation

Every identified graph RPC includes expected group/node IDs. The server rejects
a mismatch before reading or submitting a write. Nonzero-group servers reject
identity-less legacy calls. Group-zero services retain legacy support. Old v1
clients omit the optional field; old servers reject identified requests rather
than silently accepting them. Identified routing therefore requires upgraded
servers; mixed-version identified operation and binary downgrade are unsupported.

This detects misconfigured **graph endpoint** addresses, not hostile peers. All
ports remain plaintext/unauthenticated; Raft/control transport is not bound to
this graph identity. Use only an isolated trusted development network with the
explicit `--allow-insecure-development` flag. TLS/authentication remain deferred.

The resolver reuses the same closed set of concrete adapters, selection algorithm,
32-session admission, 32-dispatch admission and absolute deadline as the existing
[in-process resolver](D3_STATIC_CATALOG.md). Each operation obtains fresh metadata.
Candidate IDs and leader hints only reorder fixed configured endpoints; they
cannot introduce addresses or groups. Each candidate is visited at most once.
A read-only leader probe before writing uses a quorum barrier and metadata, not
a snapshot, so it cannot replace the prepared sequence or fail because graph
snapshot data has grown. Only a definite NotLeader permits another post-dispatch
write attempt. Timeout/disconnect/protocol loss on a write preserves its request
ID as an unknown outcome, never automatic replay. No success after deadline.
These bounded attempts are normal leader selection, not validated availability
under failures or an automatic topology service.

Read sessions pin the original adapter, epoch, sequence, view, deadline and
admission slot. All clones retain that slot until dropped. Queries, pages, point
reads and all six prepare methods use that view, never re-route or refresh it.
Remote reads still check the selected node's locally applied epoch; this is not
instantaneous cross-group revocation. Prepared mutations do not keep the session
slot; an explicit later commit gets a new operation budget and fresh metadata,
which must match the original immutable placement. No bytes, CAS, request ID or
epoch are regenerated. Retained values are not durable client journals.

All [remote snapshot/frame/connection limits](REMOTE_GRAPH.md) still apply:
64 MiB / 1,000,000 rows per complete session, 2 MiB / 4,096 rows per page,
four sessions / 128 MiB retained payload, 16 MiB encoded frames, 16 connections per
service, and 16 outbound async RPCs per client process. Cloned resolvers share
budgets; independently created resolvers have independent session/dispatch pools.
These are bounded work limits, not process RSS bounds. Catalog preparation and
queries retain their existing bounded cooperative CPU behavior.

## Verification and limits

`remote_routing` starts nine actual OS processes: three metadata voters and two
independent three-voter data groups with overlapping numeric node, vertex and
edge IDs. It verifies normal routing, catalog and graph exact retries, all CRUD,
standard Gremlin, pinned historical views, unknown partitions, configuration
errors and endpoint identity mismatch. Unit tests cover identity validation for
all RPCs, no-snapshot leader probing, durable marker rejection, shared admission
and expired dispatch scopes. Existing in-process catalog/router tests still run.

This suite deliberately does not validate remote kill/restart recovery,
leader replacement, partitions, ambiguous committed-write recovery or remote
snapshot installation during graph traffic. It proves no cross-shard snapshot
or atomic transaction, online migration, authenticated isolation or power-loss
safety. Those remain separate future work rather than implied by normal routing.
