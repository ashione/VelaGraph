# VelaGraph

**A Rust property-graph engine exploring consistent, bounded distributed queries.**

VelaGraph combines a RocksDB graph kernel, standard Gremlin syntax, Raft-backed
storage nodes and cross-shard query coordination. The goal is a general-purpose
distributed graph system for changing, relationship-rich data, not an engine
specialized for one dataset.

> **Experimental, not production-ready.** Several APIs deliberately have different
> consistency and capacity contracts. Choose the path below before building on it.
> Network services are unauthenticated plaintext development endpoints; the public
> HTTP/gRPC preview is loopback-only, not production-ready.

[Quick start](#quick-start) · [Capabilities](#choose-your-graph-path) ·
[Architecture](#architecture) · [Roadmap](docs/ROADMAP.md) ·
[Contribute](CONTRIBUTING.md)

## Why VelaGraph?

- **Graph-first storage:** typed vertex/edge properties, adjacency indexes and
  deterministic record encoding on RocksDB. The legacy SQL engine is removed.
- **Standard language, explicit boundaries:** an officially parsed Gremlin subset;
  unsupported steps and exhausted query budgets fail explicitly rather than
  silently producing partial success.
- **Consistent reads while data changes:** the restricted managed path coordinates
  typed mutations and pins a certified version for a query. The experimental
  observed path has a different, weaker contract.
- **Evidence-driven development:** independent-process tests, recovery checks and
  a CoreHR fixture with 200,000 people and 5,000 departments. CoreHR exercises
  generic hierarchy and membership queries; it does not introduce HR-specific
  execution rules.

Internal graph, managed, coordinator and Raft transports use typed gRPC. See
[graph library migration](docs/GRAPH_LIBRARY_GRPC.md) for completed legacy-listener retirement,
explicit-identity API changes and validation boundaries. Public HTTP
remains supported; old JSON/TCP clients are not a supported fallback.

## Quick start

### Try one graph query

Requires a current stable Rust toolchain and the native RocksDB build dependencies
(C++ compiler and Clang/libclang). See [setup](CONTRIBUTING.md#development-environment)
if they are not installed. The first build compiles RocksDB and needs time and
several GiB of disk; this is a source build, not a prebuilt download.

```sh
git clone https://github.com/ashione/VelaGraph.git
cd VelaGraph
cargo run --locked -p velagraph --example quickstart
```

```text
Created: Ada --KNOWS--> Grace
Query: g.V(1).out('KNOWS').values('name')
Result: Grace
Temporary graph removed. No server was started.
```

This [small executable example](examples/quickstart.rs) creates two vertices and
one edge, executes standard Gremlin and removes its temporary database on normal
completion. It opens no ports and can be run repeatedly. Forced process termination
may leave a temporary directory. This is the **local kernel**, not a distributed
consistency demonstration.

**Next:** [run the three-process distributed demo](docs/GRAPH_DEMO.md) for
cross-shard observed queries, or read [CONTRIBUTING](CONTRIBUTING.md) for development
and tests. The distributed demo uses plaintext loopback, single-voter groups and
explicit Unverified completeness; it does not establish node-loss tolerance.

### Serve an already activated managed cohort

The [graph-service guide](docs/PUBLIC_SERVICE_CLI.md) documents the loopback
HTTP/gRPC preview, configuration and exact launch command. It requires existing
activated managed nodes and their gRPC endpoints; it is not a one-command cluster
bootstrap or restore workflow. HTTP `/preview1` is not the final protobuf JSON
contract. See the guide's readiness and lifecycle limits before exposing a client.

### Embed the local kernel

The Rust package and import name are both **`velagraph`**. The local
`GraphEngine` API supports vertices, edges, bounded Gremlin, snapshot views and
explicit single-partition transactions. It is not itself a distributed client.
See [query semantics](docs/QUERY_SPEC.md), [transactions](docs/TRANSACTIONS.md)
and the executable [graph tests](tests/graph.rs) / [Gremlin tests](tests/gremlin.rs).
Existing callers migrating from `rocksdb_sql_table` should read the
[SQL removal and crate-rename note](docs/SQL_REMOVAL.md). RocksDB remains the
storage engine; removing SQL did not remove it.

## Choose your graph path

Support in one row does **not** imply support in the others.

| Path | What works now | Important boundary |
| --- | --- | --- |
| **Local v1 / K3** | Property graph CRUD, bounded Gremlin, snapshot views and single-partition transactions. A separate D1 graph-record adapter connects v1 to Raft. | Local edges stay in one partition. Explicit K3 commits sync their batch; local autocommit writes retain their older no-per-write-fsync behavior. |
| **Observed cross-shard experiment** | Static remote routing, server-side traversal, bounded hierarchy jobs, global vertex scan composition and selected result/property pages. | Staged writes are not atomic across shards. Reads retain per-shard snapshots and **Unverified** completeness, not one certified graph version. |
| **Managed fixed cohort** | Coordinated typed creates, property changes, edge deletion/replacement, durable request identity and same-version queries; optional data-local parallel execution and renewable query jobs. | Requires explicit activation from an empty baseline and fixed membership. No general transaction programs, vertex-delete/cascade API, dynamic placement or arbitrary imported-baseline certification. |

Managed sessions keep one certified **V**, including across result pages and
concurrent writes. They do not make arbitrary low-level writes safe or prove
adjacency completeness. The older observed `GlobalCommittedSnapshot` option
still rejects unsupported requests; it is not silently upgraded to managed reads.

For exact supported steps and entrypoints, start with:

- [Local query contract](docs/QUERY_SPEC.md)
- [Observed distributed queries](docs/DISTRIBUTED_QUERY.md) and
  [hierarchy result pages](docs/HIERARCHY_RESULT_PAGES.md)
- [Managed graph gateway](docs/MANAGED_GRAPH_GATEWAY.md),
  [fixed-version traversals](docs/MANAGED_FIXED_TRAVERSALS.md) and
  [data-local gateway](docs/MANAGED_LOCAL_GATEWAY.md)
- [Renewable query jobs](docs/MANAGED_QUERY_JOBS.md) and
  [fixed-cohort membership](docs/FIXED_COHORT_MEMBERSHIP.md)

### Capacity is explicit, not unlimited

The data-local managed executor supports 1–8 shard lanes (default 3), independent
of the fixed cohort's 64-member structural bound. These are configuration bounds,
not evidence of 64-node performance. Query capture, rows, work, leases and
admission each have separate limits. The opt-in 8 MiB query capture profile is
**not** a graph-size or process-memory ceiling and does not expand query jobs.

Both managed capacity profiles retain a **16 MiB history budget**. Active
checkpoints do not reclaim historical capacity; cold replay and complete
snapshots still do history-sized work. There is no history GC or unlimited write
lifetime. See [capacity profiles](docs/COHORT_CAPACITY_PROFILE.md) and the
[current capability index](docs/CURRENT_STATUS.md) for the full boundaries.

## Architecture

```text
Standard Gremlin / typed graph mutations
                 |
   Local kernel  |  Remote routing / query coordinator
                 |            |
                 |   Raft-backed storage nodes
                 |            |
                 +------ RocksDB records and adjacency
```

| Source | Responsibility |
| --- | --- |
| `src/graph.rs`, `src/graph_records.rs` | Local transactions and D1-backed graph records |
| `src/gremlin.rs` | Official-parser query planning/execution |
| `crates/d2-raft/` | Durable storage-node and consensus foundations |
| `crates/replicated-graph/` | Routing, observed/managed APIs, query coordination and CLI |
| `crates/checkpoint-object-store/` | Experimental immutable checkpoint transport |
| `scripts/`, `docs/` | Reproducible scenarios, contracts and evidence |

The [typed gRPC graph endpoint](docs/INTERNAL_GRPC_SNAPSHOTS.md)
provides `Open` / `Page` / `Close` over a reusable HTTP/2 channel. It retains the existing snapshot/session budgets without starting a legacy TCP listener. The same endpoint hosts the separate
[typed observed query-fragment service](docs/INTERNAL_GRPC_QUERIES.md), used by
`graph-coordinator serve` by default. A separate
[typed fixed-version session service](docs/INTERNAL_GRPC_FIXED_QUERIES.md) is selected
by the managed gateway's explicit gRPC graph-route constructor.
The default storage adapter uses a [bounded Metadata RPC stream](docs/METADATA_STREAM.md)
for each read freshness check, while Probe/Write remain unary and snapshots remain
paged. It caches transport, never epoch results; explicit stream/connection budgets
and slow-reader availability limits still apply.
[Typed managed control/write RPC](docs/INTERNAL_GRPC_MANAGED.md) adds explicit
coordinator/member gRPC clients and listeners; `graph-service` now always uses
these plus gRPC graph routes without a transport switch or fallback. It opens only
the configured public HTTP/gRPC listeners, not an unused legacy gateway port. Observed
hierarchy shard jobs and [Raft peers](docs/RAFT_GRPC.md) also have typed gRPC paths.
Observed coordinator query/job ingress now has a typed gRPC service: use
`graph-coordinator serve` and unflagged query/hierarchy client commands. The old
`serve-grpc` command and `--grpc` flag are rejected. The observed coordinator library now also uses typed RPC, including explicit
consistency negotiation. GraphService library listeners now also use typed gRPC;
[process-node driver/control](docs/CONTROL_GRPC.md) now uses typed gRPC too;
these internal observed endpoints are not the public managed API.

Single-shard Probe/Metadata/Write also have an explicit
[typed RPC service and CLI](docs/SHARD_OPERATIONS_GRPC.md). Catalog/routing adapters now use typed snapshots/operations by default;
graph-node --graph is a single typed listener and --grpc-snapshot is removed.
The old Linux UDS read gateway is [retired](docs/GRAPH_UDS_GATEWAY.md).
Retired-protocol fixtures test rejection only; no supported graph library API
selects a legacy listener. This does not complete cross-language interoperability,
HTTP feature parity, upgrade compatibility or production-readiness gates in
[#218](https://github.com/ashione/VelaGraph/issues/218).


Local RocksDB/Raft remains the durable authority. The object-store library can
upload/download immutable physical checkpoints and validates content hashes;
S3-compatible transport is tested against SeaweedFS. This is **backup transport**,
not object-store-authoritative storage, remote-durable write acknowledgement or
live-cluster recovery. Physical restoration is guarded for offline use. See the
[physical checkpoint](docs/PHYSICAL_CHECKPOINT.md) and
[object-store API](docs/OBJECT_STORE_CHECKPOINT_API.md) contracts.

## What is next?

The [roadmap](docs/ROADMAP.md) and linked GitHub issues track acceptance criteria,
not promises that an experimental slice is a finished database. Major remaining
work includes:

- Broader generic distributed queries, scalable history/retention and verified
  managed reads at full CoreHR scale.
- Recovery, failover, placement and migration. A global checkpoint must restore
  one coherent graph version before making that recovered view queryable;
  independent physical backups alone do not establish this.
- [Bitemporal graph semantics](docs/BITEMPORAL_GRAPH_PLAN.md): valid time and
  system version. An [in-memory reference](docs/BITEMPORAL_REFERENCE.md) and
  [durable local temporal slice](docs/TEMPORAL_STORE.md) are implemented.
  The latter uses a separate RocksDB database and synced local versions; it is
  connected to bounded official-Gremlin explicit-ID point queries and indexed
  out/in/both traversal, not global scans, managed/cohort V or existing checkpoints. A separate
  [managed vertex assertion slice](docs/TEMPORAL_STORE.md#managed-state-machine-vertex-slice-196)
  stores temporal vertices in existing member state machines and reads at fixed V/T;
  it does not add temporal edges, a public temporal service or global recovery.
- Object-store-authoritative storage with disposable local caches, subject to
  durability and recovery design; native S3/OSS/TOS deployment validation remains
  separate from the current checkpoint transport.
- Production security and, later, operational observability and visualization.

## Verification and contributing

**External contributors are welcome.** Useful contributions include bug
reproductions, documentation, query conformance tests, generic performance work
and implementation of scoped roadmap items. Start with
[CONTRIBUTING.md](CONTRIBUTING.md) for setup, checks and the issue/PR workflow.

[Ordinary CI](.github/workflows/ci.yml) runs formatting, tests and Clippy.
[CoreHR CI](docs/COREHR_CI.md) separately exercises the full observed scenario and
retained 1,025-write managed lifecycle/recovery. Object-store tests have their
[own workflow](.github/workflows/object-store-checkpoint.yml).
`cargo test --locked` does not run every ignored long test or every workspace
member. Passing same-host tests is not proof of multi-machine performance or HA.
Benchmark reports apply to their named source, binaries and workload, not every
subsequent commit.

## License

Project-authored code and documentation default to [Apache-2.0](LICENSE), except
where a file or component declares otherwise. The three existing workspace
crates retain `MIT OR Apache-2.0`; third-party licenses and notices are preserved.
See [licensing details](docs/LICENSING.md) and the
[OpenRaft notice](crates/d2-raft/NOTICE). This is not a claim of a completed
historical provenance audit or a complete dependency-attribution bundle.

