# Bounded server-side distributed observed queries

This experimental entrypoint executes a small standard-Gremlin subset across
independent data shards without downloading their key/value snapshots to the
client or coordinator. It is additive: the existing single-partition Query RPC,
v1 queries and client-side observed traversal APIs retain their contracts.

## Deployment and scope

`remote::distributed::CoordinatorService` is a separate TCP service. Its
`StaticCatalog { partitions: Vec<PartitionRoute> }` is immutable startup
configuration. Each route identifies a partition, shard, epoch, replica identity
and endpoint. This entrypoint does **not** read a live metadata catalog and does
not report or imply a metadata sequence. Restart/reconfigure the service to
change its routes; this prototype is not online migration or automatic discovery.

The client sends `DistributedRequest { start_partition, text, timeout_millis }`.
The bounded subset accepts explicit `g.V(id, ...)` starts, up to three `out()` / `in()`
steps, each optionally label-filtered, order-preserving `hasLabel`, `has`, `limit`, vertex-identity `dedup()`, terminal `path` or
`count`, and `values(key)` followed only by `limit`/`count`. Unsupported start and
step forms fail explicitly through the official parser. A separately specified
[native bounded hierarchy repeat](SERVER_HIERARCHY.md) adds one repeated
single-adjacency body, mandatory depth 1..32, and optional post-emit. A separate [global vertex scan](SERVER_GLOBAL_SCAN.md) supports `g.V()` with
filters/limit/count and its own explicit bounded budget. No global E scan,
`both()`, mutation, alternate query dialect or textual
query rewriting is provided by this slice.

Examples:

- `g.V(1).out('knows').path()`
- `g.V(1).in('reports_to').path()`
- `g.V(205000).in('member_of').has('active',true).limit(100)`
- `g.V(20).out('member_of').in('member_of').count()` (membership bag count)
- `g.V(20).out('member_of').in('member_of').dedup().count()` (distinct people)
- `g.V(1).out('knows').out('returns').out('knows').path()`
- `g.V(1).out('knows').has('active',true).limit(2).count()`
- `g.V(1).out('knows').values('name')`

## Running the coordinator

Build the two development binaries:

```sh
cargo build --locked -p replicated-graph --bin graph-node --bin graph-coordinator
```

Assume two already running, populated data nodes expose their graph TCP ports at
`127.0.0.1:8120` and `127.0.0.1:8130`. The first must identify as group 120,
node 1, shard 120, epoch 1; the second as group 130, node 1, shard 130, epoch 1.
Save this startup configuration as `query-catalog.json`:

```json
{
  "partitions": [
    {
      "partition": 0,
      "shard": 120,
      "epoch": 1,
      "identity": { "group": 120, "node": 1 },
      "endpoint": "127.0.0.1:8120"
    },
    {
      "partition": 1,
      "shard": 130,
      "epoch": 1,
      "identity": { "group": 130, "node": 1 },
      "endpoint": "127.0.0.1:8130"
    }
  ]
}
```

The catalog file is limited to 64 KiB. Entries are route bindings, not commands
to create partitions or load graph data. For a cross-partition example, create a
v2 vertex 1 in partition 0 and a complete v2 edge from it to an existing vertex
in partition 1 using the separately documented experimental write APIs first.
No metadata service is required by this coordinator.

Start the coordinator on a trusted loopback endpoint:

```sh
cargo run --locked -p replicated-graph --bin graph-coordinator -- serve \
  --endpoint 127.0.0.1:8140 --catalog query-catalog.json \
  --timeout-ms 5000 --allow-insecure-development
```

From another terminal, submit standard Gremlin:

```sh
cargo run --locked -p replicated-graph --bin graph-coordinator -- query \
  --endpoint 127.0.0.1:8140 --partition 0 \
  --query "g.V(1).out('knows').path()" \
  --timeout-ms 5000 --allow-insecure-development
```

Success prints JSON containing `completeness`, `snapshots` and `rows`; a failure
exits with an error rather than printing partial results. Both commands require
the explicit insecure-development flag. `--timeout-ms` accepts 1 through 5000
and defaults to 5000. The effective timeout also respects the server's configured
maximum. Ctrl-C stops the server. Do not expose it to an untrusted network.

## Execution and ordering

The coordinator opens one fixed query session per unique configured shard;
partitions on that shard reuse it. Query work uses `OpenQuery`, `QueryFragment`
and `CloseQuery`, not `Snapshot` or `SnapshotPage`. Data nodes retain their
snapshot locally and execute typed bounded lookup, adjacency, validation and
filter/projection fragments. The coordinator receives qualified references,
required edge verification data and projected values, not full vertex records
or raw key/value pages. Local snapshot creation may still copy a bounded whole
shard on its data node: this is not a zero-copy storage claim.

Input order and bag multiplicity are preserved. Expansion is ordered per input
by edge ID and source partition. An edge's qualified partition is its source
partition, including when the traversal has moved to its target partition.
Parallel edges and duplicate input IDs retain multiplicity unless an explicit
`dedup()` step is applied. Paths
retain qualified vertex and edge identities at every hop, so equal local IDs in
different partitions remain distinct. Cycles and revisiting a shard are allowed:
each hop uses the original retained session, never a newly acquired snapshot.
The official parser rejects more than three ordinary unrolled incoming/outgoing
hops before any session is opened, even for a missing start or a preceding
`limit(1)`. The explicit bounded-repeat extension has its separately documented
maximum expanded depth of 32; it does not enlarge traversal resource budgets.

Limits execute in their original positions. In particular,
`out().limit(2).has('active',true)` tests only the first two expanded rows,
whereas `out().has('active',true).limit(2)` selects the first two matches.
Missing `values` properties drop rows; scalar results retain their qualified
source. Terminal count is global and retains multiplicity. The ordinary default
100-row output cap applies only when there is no explicit limit and no terminal
count, after required execution and validation. It is never applied separately
by each shard.

Incoming traversal reads the target reverse index, resolves each qualified edge
at its canonical source shard, and checks source record/outgoing index/endpoint
and target reverse index/endpoint against their original retained sessions.
Outgoing traversal checks the same endpoints from the opposite direction.
The incoming target scan already verifies the exact reverse entry and target
endpoint; source resolution matches its ID, owner, label and target. Because
both sessions are fixed, incoming execution does not repeat that same target
verification RPC. Nonmatching labels are discarded only after this validation,
not by skipping unrelated observed edges.
Edge IDs are source-partition qualified; incoming paths retain that source
owner even though the path walks from target to source. Both directions order
observed edges per input by `(edge ID, source partition)`, not by adjacent vertex
ID, and preserve bag multiplicity. All observations are validated before label
filters, even when a later filter or limit would discard them. A source tombstone
with an observed reverse index is an incomplete deletion and fails the query.
Canonical source state and the target reverse index/endpoint are checked against
their respective fixed sessions. An incomplete append, inconsistent edge or
missing endpoint is an error, not an empty result or silently omitted path.

## Consistency and failure behavior

Every successful result, including scalar count and empty results, carries
`completeness: Unverified` and a sorted vector of actual per-shard `QueryStamp`
values. These identify the replica, shard, epoch and sequence of each retained
snapshot. The vector is **not** a globally consistent snapshot. Independently
acquired shards can expose different points in a non-atomic append or update,
and observed indexes cannot prove that no entries are missing.

All required sessions are acquired before execution, even if a limit or missing
start would yield few or no rows. No step silently refreshes its snapshot or
switches replicas. A failed shard, expired session, epoch fence, mismatched
identity/stamp, corrupt observation, timeout or resource exhaustion aborts the
whole query. No partial rows or partial count are returned. Sessions are closed
on completion or failure on a best-effort basis, with server expiry as fallback.

## Resource boundary

Query text is limited to 16 KiB and retains the official parser's existing
structural admission. Admission allows at most 32 configured partitions and eight unique shards, with
two coordinator executions in flight per process. Each data process allows eight
retained query sessions and eight fragment workers; snapshot retention is capped
at 128 MiB across sessions, with a 64 MiB / 1,000,000-record per-session limit.
Snapshot acquisition reserves its full per-session byte allowance before copying
and releases the unused reservation afterward.

An explicit-ID traversal execution has a cumulative 64 MiB logical-byte allowance, 10,000 work units
and 10,000 intermediate rows. Each adjacency index scan is bounded to 1,000 entries /
1 MiB. Query/fragment encoded output is limited to 2 MiB, and transport frames
retain the existing 16 MiB hard bound. Coordinator requests are limited to
17 KiB of encoded JSON, including escaping; Gremlin text itself is limited to
16 KiB. These allowances are intentionally
conservative; network execution can exhaust its budget earlier than the local
interpreter because transport and fragment work are charged too. A later
limit or count does not excuse exceeding an earlier intermediate bound.
Individual shards must not each receive an independent full query allowance.
The coordinator and its fragments share the remaining execution allowance and
one end-to-end timeout (maximum 5 seconds), rather than restarting the timeout
for every shard or step.

Logical accounting is not a total process-RSS bound. Snapshot payloads retained
on data nodes, parser/native-call allocations, allocator overhead and bounded
transport buffers have their separately stated limits. Native snapshot creation,
decoding and allocation are not preemptible simply because the deadline passed.
Incoming resolution adds bounded per-edge source RPC work under the same
shared budget, so high fan-in may fail with ResourceExhausted even when a later
limit/count would produce a small result. The existing 1,000-entry adjacency
scan cap is not paging and does not return a successful prefix. All participating
coordinator/data binaries must support the new incoming fragments; older peers
fail rather than falling back to snapshot downloads.

The implementation is a trusted-network plaintext development experiment, not
an authenticated or encrypted public query service.

## Verification scope

`tests/distributed_query.rs` hosts the coordinator in the test process and runs
two actual independent data `graph-node` processes, each with a single voter. A
third metadata process is used only to configure the legacy interpreter oracle;
it is not used by the new coordinator. Public storage APIs generate the fixture
records. Representative one-, two- and three-hop queries are compared with the
existing `experimental_read_session().observed_gremlin_v3` interpreter. Its
legacy snapshot downloads deliberately use direct endpoints outside the audited
new query path. It checks qualified cross-partition paths with overlapping
vertex/edge IDs, cycles that revisit both shards, duplicate inputs and parallel
edges, inter-hop limit position, filtered/projected rows, counts above the
default output cap, empty starts, unsupported subsets, incomplete reverse state
and unavailable shards. A proxy checkpoint interleaves a target write after
snapshot acquisition: the running three-hop query retains the old value when
revisiting the target shard and the next query sees the update. Second-hop
incomplete reverse state aborts even when a later label/limit would discard it.
A branching multi-hop query exhausts one cumulative budget even if its
single-hop prefix fits. An additional partition mapped to the same shard
verifies one session per shard. TCP audit proxies reject snapshot-download
operations and measure data response bytes in the presence of more than 3 MiB of
unrelated vertex payload.

These tests are functional distributed-query evidence, not evidence of replica
failover, global snapshot isolation, adjacency completeness, durable query
recovery, transport security or cross-shard transactions. Broader Gremlin, more
than three hops and query optimization remain separate work.

Additional regression coverage checks cumulative resource exhaustion even before
a later limit/count, repeated session release, the command-line query client,
expired/closed/forged session tokens, previous server incarnation and an actual
state-machine epoch change. Reduced primitive scan allowances return errors,
never successful prefixes. Client validation checks terminal result shapes,
qualified path ownership and scalar provenance.

The committed CoreHR regression uses 200 synthetic people and 25 departments
across three real data processes. An input-model oracle verifies direct reports,
two-hop reports/department descendants, filtered department members, duplicate
starts, parallel memberships, mixed out/in colleague bag counts and qualified
paths. Equal local edge IDs in different source partitions check incoming
secondary ordering. This CI-scale fixture is not the 200,000-person capacity
benchmark, production HR data, recursive Gremlin or an HA/performance guarantee.

## Bounded vertex deduplication

`dedup()` operates left to right on the current vertex stream, comparing the
exact `(partition, vertex_id)` pair. It retains the first traverser, including
its full path, and preserves first-occurrence order. A same-numbered ID in a
different partition is a different vertex. Later expansion may introduce new
duplicates; another explicit `dedup()` is needed to reduce those. `limit()` before
and after deduplication can therefore produce different results. A terminal
`count()` counts the resulting stream, not the implicit default 100-row output
cap. This is vertex identity deduplication, not `simplePath()` cycle prevention.

Only the no-argument vertex form is supported. Scalar `values(...).dedup()`,
path deduplication, label arguments, scopes, `by()` modulators, and edge-stream
deduplication fail explicitly, including on empty streams. The official parser
exposes this as an additive `ObservedGremlinStep`; the legacy v0 parser and the
client-side observed executor do not gain execution support in this increment.

All preceding expansion, corruption checks, and cost charges finish before
this step. A later `dedup().limit(1).count()` cannot rescue an over-budget
expansion or hide corrupt adjacency. Each input lookup consumes work;
each unique identity reserves a conservative 128-byte tree-node allowance
before insertion, and retained output rows are charged by the ordinary row
allocator. The set is bounded by the existing row, byte, work and deadline
budgets, which are cumulative and never refunded. No shard RPC, new session,
retry, or snapshot acquisition is introduced by deduplication. Snapshot-vector
and `Unverified` completeness boundaries remain unchanged.

## Incremental hierarchy counts

[Hierarchy count jobs](HIERARCHY_COUNT_JOBS.md) use an explicit start/advance/cancel
API on the coordinator endpoint. Only terminal count (optionally dedup) over a
single bounded repeat is admitted. Progress is not a query result; complete
counts retain the Unverified snapshot-vector boundary. The API has separate
finite budgets and leases. Existing synchronous requests are unchanged.
