# D3a static catalog v1

This is a bounded **in-process metadata slice**, not completion of D3. It adds
persisted partition-to-shard and immutable shard-to-Raft-group placement.
Remote leader discovery, route caching, membership changes, shard migration,
rollback, remote RPC, security, and cross-partition queries remain unsupported.
The existing single-shard graph adapter remains available; the bounded resolver
below adds fixed-candidate in-process routing.

## Bootstrap and identity

Preconfigure a dedicated metadata D1 shard, group ID, ownership epoch and sorted
replica IDs. Initialize that group and activate D1 before constructing a
`replicated_graph::catalog::Catalog` on its hosting Tokio runtime. Use
`run_blocking` to invoke synchronous catalog operations. No catalog lookup is
needed to bootstrap the metadata group. The host must supply the correct static
voter configuration and independently initialize each data group with its
assigned shard/epoch. Registration records placement; it does not create nodes,
initialize data groups, or prove that the named replicas are currently healthy.

`Placement` contains `shard: u64`, `group: u64`, `epoch: u64`, and sorted unique
`replicas: Vec<u64>` (1–31 entries). Graph partitions retain their `u32` identity.
Multiple partitions may share a shard. A shard has exactly one immutable group,
epoch and replica set; a group has at most one data shard. The metadata shard and
metadata group are reserved and cannot be registered as data placement.

A fresh catalog validates the node's immutable group identity and inclusion in
the configured replica set. Every snapshot checks the actual D1 shard/epoch and
the persisted complete metadata identity against that configuration. Bootstrap
identity disagreement fails closed. Low-level raw D1/Raft handles are trusted
administrative access, not an authorization boundary: bypassing the catalog to
overwrite its bytes is outside the safe API contract.

## Linearizable reads and registration

`snapshot` and `lookup` perform a fresh metadata leader quorum barrier using the
concrete `ReplicatedStorageNode`, followed by its atomic bounded D1 snapshot.
They never accept an arbitrary `StorageNode` implementation claiming to be
linearizable. Follower, missing quorum, stopped node, incompatible execution
context and expired deadline remain typed failures. An absent partition is
returned only after a successful quorum read; no cached/empty fallback occurs.
A retained `CatalogSnapshot` is explicitly historical, and its local `placement`
lookups do not imply another quorum read.

`prepare_registration(request_id, partition, placement)` is read-only. It reads
one snapshot, validates immutable bindings and capacity, and returns a private
immutable `PreparedRegistration`: the exact catalog bytes, D1 identity, metadata
configuration, epoch and sequence condition. `commit_registration` submits one
D1 conditional batch without a new read or automatic retry. D1 serializes CAS,
data, sequence and dedup. Two preparations against the same sequence cannot
both change the catalog. A definitive sequence `Conflict` writes nothing and
permits re-preparation. The high-level registration call never silently retries
or overwrites a competing route.

A timeout or unavailable response can have an unknown write outcome. Retain the
prepared value and submit it unchanged to the current metadata leader. The
complete configuration must match. An exact committed retry returns D1's
original sequence and `Duplicate`, even after unrelated catalog writes,
snapshot transfer or reopen. Re-preparing the same request ID is **not** exact
retry: changed bytes or condition produce `IdempotencyConflict`. A new request
ID for an identical existing binding is permitted, consumes a D1 sequence and
retained dedup entry, and changes no logical mapping. Prepared values are not a
durable client journal; client-process recovery needs additional work.

`move_partition` always returns `Unsupported`. Changing an existing epoch,
replica set, group or partition's shard through registration returns
`BindingConflict`. A catalog epoch is **not** a cross-group fence: updating
metadata cannot revoke writes accepted by an independent old owner. Movement
requires a future stop/fence/transfer protocol; none is simulated here.

## Persistent deterministic format

The dedicated metadata D1 key is ASCII `catalog/v1/state`. It is separate from
`graph/v1` and does not change graph records or the Raft D1 envelope. Its value:

1. Four bytes `CAT1` (hex `43415431`).
2. Metadata placement: shard, group, epoch (each big-endian `u64`), replica count
   (`u8`), then that many strictly ascending big-endian `u64` replica IDs.
3. Shard count (`u32` big-endian), then placements in strictly ascending shard
   order, in the same placement encoding.
4. Partition count (`u32` big-endian), then strictly ascending pairs of partition
   (`u32` big-endian) and referenced shard (`u64` big-endian).

No padding, native-endian fields, JSON envelopes or trailing bytes. All shards
must be referenced; duplicate groups/shards/partitions/replicas, missing shard
references, reserved metadata identities, malformed counts, truncation and
unknown versions fail closed. Logical catalog revision is the containing D1
sequence, not a duplicated payload field. Absence is allowed only at D1
sequence zero; a missing record in nonempty history is corruption, not reset.

Golden vector (metadata shard=9/group=8/epoch=1/replica=1, data
shard=7/group=6/epoch=2/replica=2, partition=4):

```text
434154310000000000000009000000000000000800000000000000010100000000000000010000000100000000000000070000000000000006000000000000000201000000000000000200000001000000040000000000000007
```

Incompatible future changes need a new version and explicit migration. No
migration is needed from earlier releases, which had no catalog format. Do not
reinterpret a data group's history as metadata or downgrade a populated catalog
through an older client. Raft's existing snapshot format transports the opaque
catalog row and full D1 dedup unchanged.

## Group-scoped test transport and durable reopen

`Cluster::new().in_group(group_id)` and `Registry::in_group(group_id)` select
namespaces sharing the same in-process transport storage. Routing keys are
`(group_id, node_id)`; the same numeric replica ID in two groups is independent.
Directed fault policies are also group scoped. Group zero preserves the legacy
namespace. Duplicate live registrations are rejected rather than replaced;
shutdown uses its original registration identity, so a stale handle cannot
remove a replacement or a node in another group.

Durable nodes bind group and node identity in `raft-group.json` v1. Golden bytes:
`{"version":1,"group_id":7,"node_id":1}`. Mismatched, malformed or unsupported
identity fails reopen. Nonempty legacy directories without this identity can
only be adopted as group zero. The new marker is not a change to graph, D1
snapshot or Raft log format, and is not a cross-process directory lease.
Existing TCP transport is unchanged and has no multi-group routing support.

Source compatibility: `Registry::insert` now returns a fallible result and
cannot replace an existing entry. `NodeHandle` contains private registration
metadata and is constructed through `Node`/`Cluster`, not struct literals.

## Budgets and verification scope

The catalog has at most 1,024 partitions and 1,024 shards, with 31 replicas per
placement and a 320 KiB encoded record cap. Count and byte bounds are checked
before decode loops/allocation. All serialization and validation loops are
bounded by these limits. Every operation uses one D1 read or one conditional
write with the adapter deadline and shared 32-operation admission bound.
Preparation CPU work is bounded by catalog limits, but the deadline covers the
D1 call, not an arbitrary blocking closure. The catalog alone has no retry/discovery loop; the optional bounded resolver
below supplies a separate shared deadline and admission scope.

D1 retains at most 4,096 successful requests and 64 MiB accounted dedup bytes,
with no eviction. Whole-record updates amplify retained bytes, so the catalog
can hit D1's byte budget before its logical count limit. New writes then fail
closed; retained exact retries still work. Reads currently copy D1's bounded
map (64 MiB raw/1,000,000 rows), not a production incremental catalog index.
The catalog cap is not a process RSS bound and retained snapshots are
caller-owned. This intentionally prioritizes a small verifiable safety slice
rather than throughput or production capacity.

Unit tests cover golden bytes, canonical order, malformed records, binding
immutability and budgets. Durable three-voter integration tests cover CAS
races, exact retries, metadata leader failover, fresh-learner snapshot installation
(including decoded dedup state), purge/reopen and unavailable quorums. Additional
adversarial tests cover competing bootstrap identities, metadata epoch fencing,
and deletion of the catalog row without silently resetting its history. Separate group-isolation tests cover overlapping node IDs and durable
identity rejection. Fault injection is in-process; graceful reopen is not
power-loss evidence, and these tests do not establish production network safety.

## Bounded in-process route resolver

`replicated_graph::routing::RouteResolver` is an additive, static client. It
accepts the complete fixed metadata replica set and a bounded registry of
concrete data `NodeHandle`s on their hosting runtime. Registry keys include both
`(group_id, node_id)`; at most 31,744 data handles are accepted, with duplicate
keys and metadata-group data handles rejected. All replicas named by a selected
placement must be registered. The registry never changes from a leader hint,
and no hint supplies a remote address. Bootstrap and correct membership remain
the host's responsibility, not something the resolver proves.

Every `read_view(partition, timeout)` and `commit_prepared(prepared, timeout)`
performs a **fresh linearizable metadata snapshot**, even for repeated routes
or unknown partitions. No stale catalog or last-known-route fallback exists.
The catalog still has only immutable placement: no route cache, lease, watch,
reconfiguration, migration or cross-group fencing is added.

Candidate selection is deterministic: start at the lowest configured ID and
visit each fixed candidate at most once. A `NotLeader` hint can prioritize only
an unvisited member of that placement. Self, repeated, cyclic, unknown and
foreign-group hints cannot expand the set. Metadata reads, data view acquisition
and read-only pre-write leader probes may advance on `Unavailable`. A timeout
ends the operation. Before a write, a leader quorum probe does not read graph
records, change the validation sequence or prepare a new mutation. Once a write
is dispatched, only definite `NotLeader` can advance to another candidate;
`Timeout`, `Unavailable`, `Conflict`, `Fenced`, idempotency and capacity errors
are terminal. There is no internal retry of an ambiguous write outcome.

One absolute monotonic deadline covers metadata reads, all candidates, the data
barrier/probe and write dispatch. A positive timeout of at most 60 seconds is
required. Runtime queueing consumes that same budget; a queued task first polled
after expiry does not start the operation. Waits and successful return paths
check the deadline. Bounded synchronous decode, cloning, parser/query work and
destructors are cooperative, not preemptible: a call can return its timeout
later than the deadline, but cannot return a late successful routed result.
The ordinary adapter's per-operation duration API remains available separately.

Resolver clones share a 32-operation admission pool across **all groups and
replicas**, and a second shared cap of 32 in-flight adapter dispatches. A read
session retains its operation slot until every clone is dropped, including after
its deadline expires. An expired session must be dropped, not retained as a way
to reclaim its slot. Cancelled/timed-out dispatch tasks retain their dispatch
permit until they actually exit. Repeated adapter selection cannot create new
pools. Independently constructed resolvers have independent pools; these limits
are not a process-wide quota or RSS bound. Reuse one resolver's clones for a
single client budget.

### Pinned read sessions and exact graph retries

`RoutedReadView` exposes the catalog snapshot's `metadata_sequence`, data
sequence/epoch and immutable placement. Its point reads, `scan_page`, Gremlin and
all six graph `prepare_*` methods reuse exactly the original concrete adapter and
D1 view. They do not rediscover a leader, refresh metadata or obtain another data
snapshot. A changed sequence is visible only to a new session. Session clones
keep the same deadline; they do not renew it. Paged cursors remain bound to the
original view/range. Context and locally observed epoch checks still apply on
the original adapter, including after leadership changes. This retains the
existing limitation that an isolated old replica may not yet know a newer epoch;
metadata epochs are not instantaneous revocation of historical reads.

Preparation returns an opaque `RoutedPreparedMutation` carrying the full metadata
configuration, immutable data placement and original `PreparedGraphMutation`.
The latter retains exact bytes, request ID, epoch and expected sequence. It does
not hold the read session's admission slot or deadline. Drop the session when
finished, retain the preparation, and explicitly call `commit_prepared` with a
new bounded operation budget. Fresh metadata must match both metadata identity
and the original data placement. A matching newer catalog revision due to an
unrelated registration does not invalidate the exact retry. The authoritative
replicated D1 write still validates epoch and condition atomically.

After a timeout or lost response, retry **that same value**. Successful duplicate
retry keeps the original sequence and writes nothing again. Never call a fresh
`prepare_*` to recover an unknown outcome. A definitive `Conflict` requires an
explicit fresh session and preparation; the resolver never does it automatically.
These values are in-memory, not a durable retry journal. Resolver sessions cannot
commit arbitrary CRUD directly or expose a dynamically rerouting StorageNode.
No graph, catalog, D1 or Raft wire/storage encoding changes are introduced.

Verification adds bounded-selector/admission/deadline unit tests and real
three-voter metadata plus two data groups with overlapping numeric replica and
vertex IDs. Cases cover metadata quorum loss, unknown partitions, stopped first
candidates, leadership changes, retained pages/queries, all prepared CRUD, stale
epoch/conflict and an apply-lock timeout followed by an exact duplicate retry.
Independent adversarial cases also cover admission until the last session clone
is dropped, expired sessions retaining their slots, and metadata/data waits
sharing one deadline. These are in-process fault tests, not production network
or migration evidence.
