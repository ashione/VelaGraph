# VelaGraph (Experimental)

VelaGraph is an experimental **distributed property-graph engine** built on a RocksDB storage
kernel, with a bounded standard Gremlin subset.
The repository combines local graph storage and single-partition transactions
with Raft-backed storage nodes, static remote routing, and bounded cross-shard
query coordination. Graph records, adjacency and storage metadata live in
RocksDB key spaces.

Current graph work focuses on the CoreHR scenario: 200,000 people, 5,000
departments, reporting hierarchies, and membership in multiple departments.
The [full-scale CI scenario](.github/workflows/corehr.yml) exercises supported
queries, hierarchy counts, and ordered vertex-identity result pages against
generated reference data. This is functional validation on same-host independent
processes, not a multi-machine performance or availability claim.

## Choose the graph API

The repository has three distinct graph paths. Support and consistency do not
transfer automatically between them:

| Path | Current contract |
| --- | --- |
| Local v1 / K3 | `GraphEngine` owns one partition. Snapshot reads and explicit single-partition transactions are available. The D1 `GraphRecordStore` adapter additionally supports replicated v1 CRUD and Gremlin. |
| Observed experimental v2 | Static routed and server-coordinated cross-shard queries, bounded hierarchy jobs and selected result/property pages. Cross-shard writes are staged, not atomic. Results carry per-shard snapshots and **Unverified** completeness, not one certified graph version. |
| Managed fixed cohort | Explicitly activated empty-baseline cohort, durable external-ID/original-Plan recovery, coordinated typed vertex/edge writes and certified same-version sessions. The gateway can use local Raft handles or fixed remote endpoints. Partial application cannot open a newer certified V. This is a restricted experiment, not general D4/D5 completion. |

Managed writes support vertex/edge creation, property patches, edge deletion,
[atomic edge replacement](docs/MANAGED_EDGE_REPLACEMENT.md) and
[conditional property patches](docs/MANAGED_GRAPH_GATEWAY.md#conditional-property-patches).
Vertex deletion/cascades, arbitrary transaction programs, nonempty imported
baseline certification and dynamic routing are not supported by this path.
[Managed Traverse/TraversePage](docs/MANAGED_FIXED_TRAVERSALS.md) reuse the routing-v3
executor, including qualified vertex/edge `id()`, element `dedup()` and scalar
`values`/`label`/`id` deduplication and one bounded native out/in `repeat`, over
one materialized cohort snapshot. Repeat supports exact-depth and post-body
emission with combined depth at most 32 and unchanged cumulative resource limits.
Terminal selected vertex `valueMap` returns bounded multi-property rows at the same V.
Standard `is(literal)` filters projected values, labels and IDs with strict typed
equality, composing with dedup, positional limits and terminal count.
Bounded `range(low, high)` slices vertex, edge and scalar streams in their
current order, with zero-based half-open bounds up to 1000.
Standard `simplePath()`/`cyclicPath()` filter element traversers by repeated
actual-object identity, preserving qualified paths and bags at the same V.
They do not yet support data-local hierarchy jobs. The original gateway `Read` has its own smaller subset.

A separate opt-in [data-local fixed-V gateway](docs/MANAGED_LOCAL_GATEWAY.md)
retains certified views on participant nodes and reuses the distributed executor
for explicit `g.V(ids)` with `out`/`in` and terminal `path`/`count`. It validates
the whole immutable result before publishing replayable pages. Bounded shard-lane
execution overlaps independent fixed-V fragments while preserving ordered bags,
shared budgets and per-session sequencing. Deployment configuration can select
one through eight lanes (three by default), without enlarging query budgets.
[Fixed data membership](docs/FIXED_COHORT_MEMBERSHIP.md) is independently bounded
at 64 members; widening a cohort does not enlarge lane width or capture budgets.
The original operation retains its 10,000-row / 2 MiB / 100,000-operation capture
budget. An explicit `LocalRead8MiBV1` query profile permits 8 MiB of captured
payload with unchanged row/work/history/write/result limits and shared admission
pools. This is not an 8 MiB write lifecycle or full managed CoreHR capacity claim,
and does not replace old entrypoints or expand the separate query-job budget.

An opt-in [bounded query-job lifecycle](docs/MANAGED_QUERY_JOBS.md) separates
control RPCs from finite execution, with explicit same-V renewal, cancellation
and complete-result publication. It preserves original short-lived APIs and
does not enlarge capture/history budgets. This experimental candidate remains
under validation.

The older [explicit consistency API](docs/QUERY_CONSISTENCY_NEGOTIATION.md) still
rejects `GlobalCommittedSnapshot`, including for jobs. Managed fixed-V sessions
are a separate API; they do not silently upgrade observed reads. Even managed
query rows retain Unverified **adjacency completeness** against arbitrary trusted
low-level writes, distinct from their certified version.

Both managed capacity profiles retain a **16 MiB reserved history budget** and
separate intent, D1 retained-write and query budgets. Only new empty cohorts can
select [ByteBound16MiBV1](docs/COHORT_CAPACITY_PROFILE.md), which replaces the legacy
256-request/1,024-transaction count caps, not the lifetime byte bound. Active
checkpoints and retained gateway control are merged, but cold recovery and complete
SnapshotV1 still materialize history. No history GC or unlimited write lifetime is
available. Managed query validation uses small cross-process graphs; the full-scale
CoreHR scenario above remains on the observed path.

Automatic routing/failover, online migration, authenticated/encrypted production
transport and remote disaster recovery remain unfinished. Optional
[submission phase timing](docs/MANAGED_SUBMISSION_TIMING.md) is disabled by default.
See the [current capability and work index](docs/CURRENT_STATUS.md) for merged
slices, remaining work and source/test references. VelaGraph is not yet a
production-ready distributed database.

The Rust package and library import are both named `velagraph`. The
legacy standalone SQL engine and `rocksql` CLI have been removed; see the
[migration note](docs/SQL_REMOVAL.md).

## Direction and status

| Track | Current state |
| --- | --- |
| Graph kernel | Property records, adjacency indexes, bounded traversal and standard Gremlin v0 are implemented. K3a snapshot read views, K3b single-partition write transactions, and K3c process-crash recovery are implemented. |
| Distributed kernel | D1 has a single-shard local `StorageNode` adapter (opaque byte get/scan/batch_write, snapshot views, epoch fencing, idempotency, optional sequence-conditional batches, export/import) and a `GraphRecordStore` adapter with cross-instance stale-write rejection for vertices, edges, and adjacency; `GraphEngine` is not wired through it yet, and shard split/transfer is not done. D2 has a durable `openraft` 0.9.25 prototype with versioned replicated D1 write semantics and snapshot-preserved retry identities; `crates/replicated-graph` now connects `GraphRecordStore` to actual Raft through a bounded in-process D1 adapter, leader read barriers and retained prepared mutations for exact retries ([contract](docs/REPLICATED_GRAPH.md)). A separate [development-only TCP adapter](docs/REMOTE_GRAPH.md) now adds explicitly selected remote replicas and client-side Gremlin; a bounded [D3a static catalog](docs/D3_STATIC_CATALOG.md) now persists immutable placement in a dedicated metadata group, with quorum reads and conditional registration. Its fixed-candidate resolver pins read sessions and exact retry identities, with both in-process and [static remote](docs/REMOTE_ROUTING.md) endpoints; [bounded global V/E scans](docs/GLOBAL_SCAN.md) now merge partition-qualified results with ordered has/hasLabel filters, values/label projections and global limit/count, while migration, transport security and cross-partition transactions remain unsupported. Cross-shard edge traversal is available only through the separate opt-in observed-query experiment below. |

An explicit [single-partition server query experiment](docs/REMOTE_QUERY.md) now
executes v1 and observed v2 Gremlin on one pinned server snapshot, returning
qualified results instead of transferring the whole shard. Existing client-side
query APIs remain unchanged; v2 results are still Unverified.

A separate [bounded distributed observed query
experiment](docs/DISTRIBUTED_QUERY.md) now coordinates explicit vertex starts
and up to three unrolled incoming/outgoing/bidirectional hops, or a
[native bounded hierarchy repeat](docs/SERVER_HIERARCHY.md), across fixed data
shards on server-retained snapshots. It uses a caller-maintained static catalog, not live metadata
discovery, and still returns Unverified observations. Its separately bounded
[incremental hierarchy count jobs](docs/HIERARCHY_COUNT_JOBS.md) API advances
bounded native counts over a fixed snapshot vector without retaining full paths.
It has a separate finite lease and explicit budgets; terminal emitted dedup shares
identity state across traversal depths without suppressing cycles or increasing
the identity cap. Count jobs do not return rows; separate [ordered hierarchy result jobs](docs/HIERARCHY_RESULT_PAGES.md) provide qualified vertex-identity pages, not property or path pages. A separate [global vertex scan](docs/SERVER_GLOBAL_SCAN.md) supports server-side
filters/limit/count without downloading the graph to the coordinator. Its
[bounded traversal composition](docs/GLOBAL_VERTEX_TRAVERSAL.md) can continue
through out/in/both, one bounded repeat, filters, limits, dedup and terminal count or selected property maps,
using separate cumulative scan/traversal budgets and the same fixed sessions.

The detailed slice order and exit criteria are in [`docs/ROADMAP.md`](docs/ROADMAP.md);
the transaction/recovery contract is in [`docs/TRANSACTIONS.md`](docs/TRANSACTIONS.md);
the consensus decision is in [`docs/D2_CONSENSUS.md`](docs/D2_CONSENSUS.md).

## Features

| Area | Supported |
| --- | --- |
| Graph kernel | property vertices/edges, labels and typed properties, outgoing/incoming adjacency indexes, label-filtered traversal, cascade delete |
| Graph transactions | snapshot-backed read views, single-partition write transactions with conservative conflict detection, one synced `WriteBatch` per commit, process-crash recovery tests |
| Graph queries | standard Gremlin v0 (`V`/`E`, `out`/`in`/`both`, `has`/`hasLabel`, `values`/`id`/`label`, `limit`/`path`/`count`), 1–3 edge steps |

General cross-partition graph transactions outside the restricted managed cohort
are not supported. See [Limitations](#limitations).

## Remote graph command-line client

A development-only [`graph-client`](docs/GRAPH_CLIENT.md) now exposes catalog
registration/lookup, v1 partition/global Gremlin and explicitly experimental v2
creation/observed queries without writing Rust. The [three-process loopback
demo](docs/GRAPH_DEMO.md) builds two same-ID vertices in different shards, creates
a cross-shard edge and verifies a qualified path. Results retain snapshot vectors
and experimental `Unverified` completeness; this is not a distributed transaction,
production-secure service or durable client retry mechanism.

## Quick start

Prerequisites: a C++ toolchain and libclang (needed by `librocksdb-sys`), plus
Rust 1.95 or newer. On macOS with the Xcode Command Line Tools:

```bash
export LIBCLANG_PATH="$(dirname "$(xcrun --find clang)")/../lib"
export RUSTFLAGS="-C link-arg=-Wl,-rpath,$LIBCLANG_PATH"
```

On Linux install `clang`/`libclang-dev` if your distribution splits them out.

Build the workspace (the first build compiles RocksDB and takes a few minutes):

```bash
cargo build --workspace --locked
```

For a complete graph command-line walkthrough, follow the
[three-process loopback demo](docs/GRAPH_DEMO.md). For embedded use, see below.

## Embedding

### Graph kernel

`GraphEngine` is the local graph storage layer. It includes a standard Gremlin v0
query subset, snapshot read views, and explicit single-partition write transactions.
This `GraphEngine` API
is local; the separate `GraphRecordStore`/replicated and managed APIs above provide
the distributed experiments.

```rust
use std::collections::BTreeMap;
use velagraph::{Edge, GraphEngine, Value, Vertex};

fn main() -> velagraph::Result<()> {
    let graph = GraphEngine::open("./graph-data")?;

    let mut ada = Vertex::new(1);
    ada.labels.push("Person".into());
    ada.properties.insert("name".into(), Value::Text("Ada".into()));
    graph.create_vertex(&ada)?;

    let mut grace = Vertex::new(2);
    grace.labels.push("Person".into());
    graph.create_vertex(&grace)?;

    let mut knows = Edge::new(10, 1, 2, "KNOWS");
    knows.properties.insert("since".into(), Value::Integer(1843));
    graph.create_edge(&knows)?;

    let outgoing = graph.out_edges(1, Some("KNOWS"))?;
    assert_eq!(outgoing.len(), 1);
    assert_eq!(outgoing[0].to, 2);
    Ok(())
}
```

### Bounded traversal

`GraphEngine::traverse` is the execution foundation for the query layer. It
walks 1–3 hops over the outgoing, incoming, or both adjacency indexes, with an
optional edge label and a result limit:

```rust
use velagraph::{GraphEngine, TraversalDirection, TraversalOptions};

let graph = GraphEngine::open("./graph-data")?;
let result = graph.traverse(
    1,
    &TraversalOptions {
        max_hops: 3,
        direction: TraversalDirection::Out,
        label: Some("KNOWS".to_string()),
        limit: 100,
    },
)?;
for path in result.paths {
    println!("{:?} via {:?}", path.vertices, path.edges);
}
```

A vertex is visited at most once per request, so cycles terminate; results are
deterministic (edge id ascending) and `truncated` is set when the path limit
stopped exploration.

### Gremlin v0

The public graph query language is standard Gremlin text, parsed from Apache
TinkerPop's official `Gremlin.g4` grammar via [`gremlin-rs`](https://crates.io/crates/gremlin-rs).
No project-specific grammar or dialect is defined. `GraphEngine::gremlin`
returns ordered rows:

```rust
use velagraph::{GraphEngine, GremlinRow};

let graph = GraphEngine::open("./graph-data")?;
let result = graph.gremlin("g.V().hasLabel('Person').out('KNOWS').limit(10)")?;
for row in result.rows {
    match row {
        GremlinRow::Vertex(id) => println!("vertex {id}"),
        GremlinRow::Value(value) => println!("value {}", value.render()),
        other => println!("{other:?}"),
    }
}
```

The v0 subset is `V`/`E`, `out`/`in`/`both`, `has`/`hasLabel`,
`values`/`id`/`label`, and `limit`/`path`/`count`; it allows at most 3 edge
steps. Mutations, `outE`/`inE`/`bothE`, predicates, `repeat`, and the rest of
Gremlin are rejected explicitly rather than downgraded. The normative subset,
semantics, limits, and errors are in [`docs/QUERY_SPEC.md`](docs/QUERY_SPEC.md);
implementation constraints are in
[`docs/IMPLEMENTATION_RULES.md`](docs/IMPLEMENTATION_RULES.md).

Graph keys are `graph/v1/p/<partition>/vertex/<id>` and
`graph/v1/p/<partition>/edge/<id>` for records and
`graph/v1/p/<partition>/out/<source>/<label>/<edge>` /
`graph/v1/p/<partition>/in/<target>/<label>/<edge>` for adjacency. The `v1`
segment versions the on-disk format and the partition segment lets a future
distributed layer co-locate a vertex with its adjacency. Record payloads use a
deterministic protobuf-compatible binary format; the schema and golden vectors
are in [`docs/GRAPH_FORMAT.md`](docs/GRAPH_FORMAT.md). Edge creation and deletion
update both adjacency indexes in one `WriteBatch`; deleting a vertex cascades to
all incident edges. Writes use RocksDB's default WAL behavior (no per-statement
fsync), so a process crash may lose the most recent writes. This is a
single-process, single-partition `GraphEngine` API: vertex IDs and edge IDs are
caller-supplied `u64` values, unique within their respective partition namespaces.
Distributed APIs qualify identities with the partition. One `GraphEngine` owns
one partition and has no Raft or cross-partition transaction integration. Both
endpoints of a v1 edge must live in that partition; separate v2/managed APIs
provide the cross-partition experiments without changing this v1 contract.
Scalar property values are a deliberate first-milestone limit, not the final data model. The K3 single-partition
transaction and recovery boundary is specified in
[`docs/TRANSACTIONS.md`](docs/TRANSACTIONS.md). Snapshot-backed read views and
single-partition write transactions with conservative conflict detection are
implemented. A transaction commit uses one synced `WriteBatch`; a process-crash
harness covers the before-commit and after-commit boundaries. Autocommit graph
writes still use the old no-fsync behavior. This K3 API does not provide
cross-partition transactions.

## Architecture

Graph key layouts and deterministic property-record encoding are specified in
[GRAPH_FORMAT.md](docs/GRAPH_FORMAT.md). The local kernel uses RocksDB; D1-backed
records and replicated adapters provide separately documented distributed paths.
The graph `Value` representation and on-disk formats are unchanged by removal of
the SQL engine.

Source layout:

```text
src/value.rs                 Shared graph property values
src/graph.rs                 Local GraphEngine / K3 transactions
src/graph_records.rs         D1-backed v1 graph records
src/gremlin.rs               Official-parser Gremlin v0 execution
crates/d2-raft/              Consensus, durable D1 and managed state machines
crates/replicated-graph/     Adapters, routing, observed and managed graph APIs
scripts/                    Demo, CoreHR and retained-lifecycle validation
docs/                       Entry-specific contracts and historical evidence
```

## Testing

```bash
cargo fmt --all --check
cargo test --locked
cargo clippy --all-targets --locked -- -D warnings
git diff --check
```

The graph and distributed suites cover CRUD, deterministic encoding, query
semantics/budgets, conditional retry identities, fixed-V sessions, persistence,
corruption and independent-process recovery. See the
[source/test map](docs/CURRENT_STATUS.md#verification-and-source-map).

CI runs formatting, tests, and Clippy on Ubuntu for pushes to `main` and pull
requests; see [.github/workflows/ci.yml](.github/workflows/ci.yml). The separate
[CoreHR workflow](docs/COREHR_CI.md) requires both the full observed scenario and
a fresh release 1,025-write managed lifecycle with recovery evidence. The long
lifecycle test is ignored by ordinary debug tests, so `cargo test --locked` alone
is not full acceptance. Historical result documents are evidence for their named
source/binary, not a substitute for current-head CI.

## Limitations

The repository includes distributed experiments, but is not a production database:

- `GraphEngine` K3 transactions stay
  single-partition/single-process. Separate Raft/static-routing and managed-cohort
  APIs exist, but general distributed transactions, automatic placement/failover
  and online migration remain incomplete. A RocksDB directory has one owning
  process; independent node processes use separate directories.
- Graph TCP services require
  trusted isolated networks; they lack production authentication, authorization
  and TLS. Do not expose their low-level control or write APIs to untrusted peers.

### Single-shard replicated Gremlin prototype

The optional `replicated-graph` adapter also supports standard Gremlin v0 through
`GraphRecordStore::execute_gremlin`. Each execution pins one leader-barrier D1
view; scans and adjacency use bounded pages through the adapter. Use
`replicated_graph::run_blocking` from async code. Retained `GraphRecordReadView`
queries preserve their original sequence, while context and epoch checks remain
active. This is an in-process, single-shard path, not a distributed query service.
See [the query contract](docs/QUERY_SPEC.md) and
[replicated graph limitations](docs/REPLICATED_GRAPH.md).

### Query-owned memory admission

Local and D1 Gremlin executions now share a per-execution 64 MiB logical byte
budget for decoded records, intermediate rows/paths, query pages and result
construction. Large-property queries can fail even below the row cap. This is
not a process RSS or allocator limit; retained D1 snapshots and adapter-private
allocations are separate. See [the accounting contract](docs/QUERY_MEMORY.md).

### Bounded static routing

`replicated_graph::routing::RouteResolver` selects fixed in-process replicas from
a fresh metadata quorum read. Read sessions pin one adapter/view and deadline;
prepared writes retain their original identity across explicit retries. All
clones share admission limits across groups. See
[the static routing contract](docs/D3_STATIC_CATALOG.md#bounded-in-process-route-resolver).
The Linux-only same-host read gateway below is separate from this in-process
resolver. Static remote graph protocols are available separately below; automatic
membership changes, migration and cross-group ownership fencing remain unfinished.

### Same-host read-only graph gateway

On Linux, `replicated_graph::gateway` exposes bounded open/query/close sessions
through a Unix domain socket in a trusted owner-only directory. Peer UID checks,
fixed snapshots and deadlines, bounded frames and worker admission apply. See
[the gateway contract](docs/GRAPH_UDS_GATEWAY.md). This is a same-host process
boundary, not cross-host routing or a production network service. Existing D2
TCP/control listeners remain outside this trust boundary. Their separate
[resource limits](docs/D2_TCP_LIMITS.md) do not add authentication or make
them production-secure.

### Development-only cross-process graph access

The optional [`graph-node` and `RemoteStorageNode`](docs/REMOTE_GRAPH.md) connect
existing graph CRUD and client-side standard Gremlin to independent TCP Raft
processes. Explicitly enable plaintext development mode and use only a trusted
isolated network. This functional slice is single-shard, bounded, and does not
perform automatic retries or claim validated remote graph failover recovery.

### Static remote partition routing

The development TCP service now supports a remote persistent catalog and static
partition-to-data-group routing through `RouteResolver::new_remote`. See
[configuration, identity checks and limits](docs/REMOTE_ROUTING.md). Normal CRUD
and Gremlin are tested across nine independent processes (metadata plus two data
groups). This is not production security, validated automatic failover, online
migration, or cross-shard queries/transactions.

### Isolated cross-shard edge experiment

A separate [append-only graph/v2 experiment](docs/EXPERIMENTAL_CROSS_EDGES.md)
supports qualified vertices and source-owned edges across shards, with explicit
non-atomic staged writes and unknown/incomplete outcomes. Its out/in API returns
index observations with **unverified completeness**, not complete Gremlin results.
Its separate observed Gremlin API supports global/explicit vertex starts, directional
hops, label/property filters, qualified paths, limits and counts while retaining that uncertainty.
The additive `observed_gremlin_v2` result API also supports qualified `values(key)`
and `label()` projections without weakening the original observed types' `Eq` API.
V1 data and atomicity stay unchanged; v1 entities do not participate. No automatic
repair, migration, failover guarantee or production deployment is implied.

### Gremlin parser resource admission

All Gremlin text entrypoints now reject more than 64 raw structural marks
(`()[]{},.:;`) with typed `GremlinComplexityLimit` before invoking the official
recursive parser. The existing 16 KiB cap alone did not prevent stack overflow
from short repeated lists/chains. Marks inside strings/comments also count: this
intentionally conservative compatibility restriction avoids a second lexer and
malformed-string bypasses. See [query limits](docs/QUERY_SPEC.md#8-resource-limits).

The isolated graph/v2 experiment additionally offers `observed_gremlin_v3`
edge starts, filters and scalar projections with qualified edge provenance.
Its bounded canonical pages validate observed indexes and endpoints, but every
result remains Unverified, not a globally consistent or complete traversal.
See [experimental edge observations](docs/EXPERIMENTAL_CROSS_EDGES.md#additive-edge-observations-v3).

The observed v3 subset also supports `outE`/`inE`/`bothE` and
`outV`/`inV`/`bothV`, with qualified mixed-element paths and bag semantics.
`bothE` emits a self-loop once; `bothV` emits both endpoint positions, twice
for a loop. These steps remain unsupported by legacy Gremlin APIs and do not
make observed experimental indexes complete or globally consistent.

The opt-in v2 coordinator also supports properties-only edge updates via
`prepare_edge_properties` and `experimental_commit_mutation`. One immutable
source-shard CAS preserves identity, label, endpoints and indexes; exact retries
never overwrite newer properties. Null and explicit removal are distinct.
No automatic recovery or distributed transaction is implied.
See [edge property mutations](docs/EXPERIMENTAL_CROSS_EDGES.md#additive-edge-property-mutation).

The opt-in `prepare_edge_delete` / `experimental_commit_mutation` pair deletes
complete v2 edges using permanent versioned tombstones, never reusable EdgeRefs.
Cross-shard deletion remains staged and can be incomplete; same-shard writes
combine into one CAS batch. Observed queries verify residual indexes and charge
tombstones against scan budgets. Upgrade all readers before first deletion;
downgrade, automatic repair, tombstone GC and vertex deletion are unsupported.
See [edge deletion](docs/EXPERIMENTAL_CROSS_EDGES.md#additive-permanent-edge-deletion).

Opt-in v2 vertex properties are available through `prepare_vertex_properties`
and `experimental_commit_vertex_mutation`, retaining qualified identity and
labels in one owner-shard CAS. Null/set/remove and historical retries are explicit.
`prepare_vertex_delete` explicitly rejects deletion: observed empty adjacency
cannot prove absence of all incoming/outgoing edges or fence other shards.
See [vertex mutations](docs/EXPERIMENTAL_CROSS_EDGES.md#additive-vertex-property-mutation).

Remote adapters now acquire complete local read views through fixed snapshot
sessions and bounded pages: up to 64 MiB / one million rows, with 2 MiB pages,
shared admission, fixed TTL and strict full-result validation. This remains a
trusted plaintext experiment, not lazy remote reads or failover support. The
legacy single-Snapshot RPC is unchanged. See [paged remote snapshots](docs/REMOTE_GRAPH.md#snapshot-consistency-and-limits).

The experimental coordinator also supports [ordered hierarchy result pages](docs/HIERARCHY_RESULT_PAGES.md): explicit finite Gremlin repeat jobs evaluate against fixed shard snapshots before returning actual qualified-vertex pages. Preparation progress is separate from results, and count jobs retain their existing limits.

[Fixed-snapshot property pages](docs/HIERARCHY_PROPERTY_PAGES.md) extend prepared hierarchy jobs with terminal standard valueMap projections. Property selection is declared before preparation; no fresh reads are attached to an already closed Ready snapshot.

The bounded explicit-ID server path also supports terminal selected-property
`valueMap`, so ordinary person/department joins can return several fields per
vertex in one query. See [the map contract](docs/QUERY_SPEC.md#distributed-synchronous-selected-property-maps).
It keeps the existing synchronous budgets and per-shard snapshot limits; this
does not add global joins or general Gremlin support.

Experimental [fixed-cohort snapshot sessions](docs/CERTIFIED_COHORT_READS.md) certify empty-baseline KV participants at one retained decision version. They do not enable the remote graph GlobalCommittedSnapshot API.

Experimental [managed graph/v2 transactions](docs/MANAGED_GRAPH_TRANSACTIONS.md) connect vertex/edge creates, property updates and edge deletion to fixed-cohort commits and same-version reads. The gateway supports both in-process and fixed remote managed nodes; neither enables the older remote Global Gremlin API.

The optional [durable request admission foundation](docs/DURABLE_GRAPH_REQUESTS.md) binds external IDs to exact transaction plans before prepare. The separate restricted TCP gateway below uses this foundation.

### Experimental managed graph TCP gateway

The separate `replicated_graph::managed_gateway` API exposes typed graph/v2
mutations with durable external request IDs, read-only Status, explicit Abort,
and fixed-version certified sessions with a small standard Gremlin point/adjacency
subset over server-owned Raft handles. It is bounded plaintext for trusted networks, not multi-host Raft
placement, authentication, remote Global Gremlin, indexed MVCC or production
availability. See [the contract and limits](docs/MANAGED_GRAPH_GATEWAY.md).

The additive [remote managed-node transport](docs/REMOTE_MANAGED_TRANSPORT.md)
allows this managed gateway to use a coordinator and participants in independent
processes. External Gremlin reads, canonical request identity and persisted-Plan
recovery are unchanged. Same-version sessions require the complete cohort and
matching retained terminal-prefix evidence. This trusted plaintext, fixed-endpoint
slice does not enable the older remote GlobalCommittedSnapshot API or claim
multi-machine validation, automatic failover, authentication/TLS or production
D4/D5 completion.

Managed fixed-cohort reads now use [exact segmented terminal-prefix evidence](docs/TERMINAL_PREFIX_EVIDENCE.md)
at the chosen V rather than transferring the original request/Plan archive. This
keeps old-V evidence independent of later archive growth without changing durable
retry/recovery, retention limits, prepare/advance payloads or the older Global Unsupported
boundary. Internal managed peers now require protocol v6; evidence remains O(V).

[Durable request point recovery](docs/DURABLE_REQUEST_INDEX.md) returns one exact
original external-ID/Plan binding without transferring the full Journal. Bound
coordinators (current disk v16, with v15 migration support) project LookupRequest
and HistoryPage directly from raw history, with bounded retained results and full namespace verification against a
private, previously validated published cut. The duplicate request index is
retired atomically after strict legacy validation. Every read still scans the
whole history; this is not a speed or unlimited-lifetime claim.

The bounded [durable terminal-segment index](docs/DURABLE_DECISION_SEGMENTS.md) now
backs coordinator proofs and certified participant reads with exact absolute-ordinal
disk segments. Complete history, all lifetime caps and whole snapshots remain;
this is a recovery/read foundation, not unlimited writes or a performance claim.

Managed decision catch-up now uses [bounded event suffixes and retained ordinal
consumption](docs/INCREMENTAL_DECISION_CONSUMPTION.md) for every cohort member.
Original event history remains durable authority, SHA-256 anchors have an explicit
integrity assumption. Legacy cohorts retain 256-request/1,024-transaction count
caps; both profiles retain the 16 MiB reserved history budget and independent
local D1 limits.

The [authoritative event ledger](docs/AUTHORITATIVE_EVENT_LEDGER.md) stores
fixed-cohort Commands as the sole live decision authority. Merged
[S2 coordinator](docs/COORDINATOR_ACTIVE_CHECKPOINT.md) and
[S3 member checkpoints](docs/MEMBER_ACTIVE_CHECKPOINT.md) use disk v16/v17 and
active state instead of resident/ordinary apply-candidate full Journals on bound
durable paths. Actual earlier formats remain strict migration inputs. Full raw
closure is still checked; cold replay, explicit full-history/terminal-proof paths
and complete snapshots retain history-sized work. Metadata and current snapshot
bytes remain resident, and resolving holes releases no historical capacity.
Current internal managed RPC is v6; external gateway framing stays v1.

New empty cohorts can explicitly select the [immutable byte-budget capacity
profile](docs/COHORT_CAPACITY_PROFILE.md). Actual durable grants from every member
precede admission; original IDs/Plans and old read versions remain retained.
This profile replaces the legacy 256/1,024 count quotas with byte-derived guards,
while the 16 MiB reserved Journal, local D1 limits, complete replay/snapshots and
whole-frame bounds remain. Existing/default cohorts stay legacy; there is no
online conversion or unlimited-lifetime claim. New managed peers require RPC v6; see the [retained gateway control contract](docs/RETAINED_GATEWAY_CONTROL.md).

The [retained gateway control slice](docs/RETAINED_GATEWAY_CONTROL.md) replaces
actual managed graph full-Journal control and Prepare with anchored point evidence
and deterministic retained-cut preparation. Existing finite profiles and full
terminal-proof/SnapshotV1 boundaries remain; no capacity increase is claimed.

Global scan-to-traversal queries also accept terminal single-key `values(key)`
after out/in or bounded repeat. Ordered bags, qualified scalar provenance and
missing/Null semantics use the same retained shard sessions and budgets. Scan-only
terminal values requires an explicit preceding vertex limit (1..1000); no-limit
scan values and scalar post-processing remain unsupported. See the
[composition contract](docs/GLOBAL_VERTEX_TRAVERSAL.md).

See [bounded full-cohort catch-up](docs/BOUNDED_COHORT_FANOUT.md) for member fanout,
unchanged consistency/resource contracts, and critical-path benchmark scope.

### Experimental temporal storage

The opt-in [durable temporal point-read slice](docs/TEMPORAL_STORE.md) stores
append-only valid-time assertions in a dedicated local RocksDB database, with
atomic synced local versions and historical V/T point reads. Explicit-ID official
Gremlin point queries reuse the existing executor; global scans and adjacency
remain unsupported. It does not connect to Raft, managed sessions or checkpoints;
#196 remains open. Existing graph/v1/v2 semantics and formats are unchanged.
