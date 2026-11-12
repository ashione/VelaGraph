# VelaGraph

**A Rust property-graph engine exploring consistent, bounded distributed queries.**

VelaGraph combines a RocksDB graph kernel, standard Gremlin syntax, Raft-backed
storage nodes and cross-shard query coordination. The goal is a general-purpose
distributed graph system for changing, relationship-rich data, not an engine
specialized for one dataset.

> **Experimental, not production-ready.** Several APIs deliberately have different
> consistency and capacity contracts. Choose the path below before building on it.
> TCP services are unauthenticated plaintext for trusted development networks only.

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

## Quick start

### Run a three-process graph demo

Use Linux with a current stable Rust toolchain, a C++ compiler, Clang/libclang,
Git and Python 3. CI uses Ubuntu and Rust stable. On Debian/Ubuntu the native
packages are `build-essential clang libclang-dev`. The first build compiles
RocksDB; allow time and several GiB of free disk space.

```sh
git clone https://github.com/ashione/VelaGraph.git
cd VelaGraph
cargo build --locked -p replicated-graph --bins
python3 scripts/graph_client_demo.py \
  --allow-insecure-development \
  --node target/debug/graph-node \
  --client target/debug/graph-client
```

If libclang is not discovered, set `LIBCLANG_PATH` to the directory containing
`libclang.so`. If you use `CARGO_TARGET_DIR`, adjust the two binary paths above.

The script starts one metadata process and two data processes on loopback,
creates a cross-shard relationship, and verifies:

```groovy
g.V(1).outE('knows').inV().path()
```

The expected qualified path is **vertex 10:1 → edge 10:7 → vertex 20:1**.
Completeness is **Unverified**. This demo uses non-atomic observed writes and
single-voter groups; it demonstrates neither managed transactions nor node-loss
tolerance. The plaintext flag is intentional: run only on a trusted development
machine. The script stops its child processes and retains its printed temporary
data/log directory for inspection. Repeated runs consume disk space.

See the [demo guide](docs/GRAPH_DEMO.md) for diagnostics and cleanup, and the
[CLI reference](docs/GRAPH_CLIENT.md) for commands and configuration.

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
  out/in/both traversal, not global scans, managed/cohort V or existing checkpoints.
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
