# RocksDB-backed Distributed Property Graph (Experimental)

An experimental **distributed property-graph engine** built on a RocksDB storage
kernel, with a bounded standard Gremlin subset and a separate SQL table engine.
The repository combines local graph storage and single-partition transactions
with Raft-backed storage nodes, static remote routing, and bounded cross-shard
query coordination. Catalogs, indexes, rows, graph records, and adjacency live in
RocksDB key spaces; the SQL engine is not a layer over SQLite.

Current graph work focuses on the CoreHR scenario: 200,000 people, 5,000
departments, reporting hierarchies, and membership in multiple departments.
The [full-scale CI scenario](.github/workflows/corehr.yml) exercises supported
queries, hierarchy counts, and ordered vertex-identity result pages against
generated reference data. This is functional validation on same-host independent
processes, not a multi-machine performance or availability claim.

Cross-shard graph operations remain an **opt-in experiment**: staged writes are
not atomic across shards, and query results carry per-shard snapshots and
Unverified completeness, not a globally atomic snapshot. Cross-shard transactions,
automatic failover, online migration, and production transport security remain
unfinished. The goal is a distributed graph database; this is not yet a
production-ready one.

The additive [explicit consistency API](docs/QUERY_CONSISTENCY_NEGOTIATION.md)
lets applications require a global committed snapshot without silently falling
back: that requirement currently fails with Unsupported, including for jobs.
It does not yet implement global MVCC or cross-shard transactions.
An opt-in [managed shard write fence](docs/MANAGED_SHARD_WRITES.md) persists
write-mode identity and rejects old raw writers after explicit activation. It
adds shard-local atomic batch identities only; default graph APIs are unchanged
and global transactions remain unimplemented. A separate opt-in
[coordinator decision ledger](docs/TRANSACTION_DECISION_JOURNAL.md) persists
transaction decisions on a dedicated empty shard. Opt-in
[participant intents](docs/PARTICIPANT_WRITES.md) now retain prepares and apply
trusted coordinator decisions atomically per shard through an in-process relay.
They do not yet provide global read visibility or wire existing graph writers
through distributed transactions. Explicit [versioned participant reads](docs/VERSIONED_PARTICIPANT_READS.md)
now consume contiguous applied decision prefixes and replay retained history for
one requested version; a lagging shard refuses that version. Global query admission
and certification of a common initial baseline remain unfinished.

The existing Rust package name `rocksdb-sql-table`, library imports, and SQL CLI
remain unchanged.

The SQL subset is intentionally small and strict: create/drop tables, create/drop
indexes, insert, update, delete, and single-table `SELECT` with filtering,
ordering, pagination, `DISTINCT`, `GROUP BY`, `HAVING`, and
`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`.

## Direction and status

| Track | Current state |
| --- | --- |
| Graph kernel | Property records, adjacency indexes, bounded traversal and standard Gremlin v0 are implemented. K3a snapshot read views, K3b single-partition write transactions, and K3c process-crash recovery are implemented. |
| Distributed kernel | D1 has a single-shard local `StorageNode` adapter (opaque byte get/scan/batch_write, snapshot views, epoch fencing, idempotency, optional sequence-conditional batches, export/import) and a `GraphRecordStore` adapter with cross-instance stale-write rejection for vertices, edges, and adjacency; `GraphEngine` is not wired through it yet, and shard split/transfer is not done. D2 has a durable `openraft` 0.9.25 prototype with versioned replicated D1 write semantics and snapshot-preserved retry identities; `crates/replicated-graph` now connects `GraphRecordStore` to actual Raft through a bounded in-process D1 adapter, leader read barriers and retained prepared mutations for exact retries ([contract](docs/REPLICATED_GRAPH.md)). A separate [development-only TCP adapter](docs/REMOTE_GRAPH.md) now adds explicitly selected remote replicas and client-side Gremlin; a bounded [D3a static catalog](docs/D3_STATIC_CATALOG.md) now persists immutable placement in a dedicated metadata group, with quorum reads and conditional registration. Its fixed-candidate resolver pins read sessions and exact retry identities, with both in-process and [static remote](docs/REMOTE_ROUTING.md) endpoints; [bounded global V/E scans](docs/GLOBAL_SCAN.md) now merge partition-qualified results with ordered has/hasLabel filters, values/label projections and global limit/count, while migration, transport security and cross-partition transactions remain unsupported. Cross-shard edge traversal is available only through the separate opt-in observed-query experiment below. |
| SQL engine | The documented SQL subset is supported; it remains autocommit per statement and does not participate in graph transactions. |

An explicit [single-partition server query experiment](docs/REMOTE_QUERY.md) now
executes v1 and observed v2 Gremlin on one pinned server snapshot, returning
qualified results instead of transferring the whole shard. Existing client-side
query APIs remain unchanged; v2 results are still Unverified.

A separate [bounded distributed observed query
experiment](docs/DISTRIBUTED_QUERY.md) now coordinates explicit vertex starts
and up to three unrolled incoming/outgoing hops, or a
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
through out/in, one bounded repeat, filters, limits, dedup and terminal count or selected property maps,
using separate cumulative scan/traversal budgets and the same fixed sessions.

The detailed slice order and exit criteria are in [`docs/ROADMAP.md`](docs/ROADMAP.md);
the transaction/recovery contract is in [`docs/TRANSACTIONS.md`](docs/TRANSACTIONS.md);
the consensus decision is in [`docs/D2_CONSENSUS.md`](docs/D2_CONSENSUS.md).

## Features

| Area | Supported |
| --- | --- |
| DDL | `CREATE TABLE [IF NOT EXISTS]`, `DROP TABLE [IF EXISTS]`, `CREATE [UNIQUE] INDEX [IF NOT EXISTS]`, `DROP INDEX [IF EXISTS]` |
| Types | `INT` (`INTEGER`/`BIGINT` aliases), `REAL` (`DOUBLE`/`FLOAT`), `TEXT` (`VARCHAR`/`STRING`), `BOOLEAN`; `DATE` (ISO-8601 text) and `DECIMAL`/`NUMERIC` (stored as `REAL`) for TPC-H compatibility |
| Keys | exactly one single-column primary key of type `INT` or `TEXT` |
| Indexes | single-column and composite indexes; uniqueness; equality and range lookups on a leading column prefix |
| DML | multi-row `INSERT ... VALUES`, `UPDATE ... SET ... [WHERE]`, `DELETE [WHERE]` |
| Queries | projection, `WHERE`, `GROUP BY` columns, `HAVING` on group columns and aggregates, `COUNT(*)`/`COUNT(col)`, `SUM(expr)`, `AVG(expr)`, `MIN(expr)`, `MAX(expr)`, `ORDER BY ... [ASC\|DESC] [NULLS FIRST\|LAST]`, `LIMIT`/`OFFSET`, `DISTINCT` |
| Predicates/expressions | `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `[NOT] LIKE ... [ESCAPE]`, `[NOT] BETWEEN`, `[NOT] IN`; numeric `+ - * /`, unary minus, `DATE` literals and `DATE ± INTERVAL ... DAY` |
| Storage | RocksDB WAL-backed writes, one atomic `WriteBatch` per statement, point lookups for `pk = literal`, index prefix scans |
| Graph kernel | property vertices/edges, labels and typed properties, outgoing/incoming adjacency indexes, label-filtered traversal, cascade delete |
| Graph transactions | snapshot-backed read views, single-partition write transactions with conservative conflict detection, one synced `WriteBatch` per commit, process-crash recovery tests |
| Graph queries | standard Gremlin v0 (`V`/`E`, `out`/`in`/`both`, `has`/`hasLabel`, `values`/`id`/`label`, `limit`/`path`/`count`), 1–3 edge steps |

Deliberately **not** supported: joins, subqueries, window functions, `ALTER TABLE`, `DEFAULT`/`UNIQUE`/foreign-key column
constraints, multi-statement SQL transactions, `SELECT` without `FROM`, and
cross-partition graph transactions. See [Limitations](#limitations).

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

Run the interactive shell (debug build; the first build compiles RocksDB and
takes a few minutes):

```bash
cargo run -- ./data
```

Or run SQL non-interactively:

```bash
cargo run -- ./data --exec "
  CREATE TABLE users (
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    age INT,
    active BOOLEAN
  )"

cargo run -- ./data --exec "
  INSERT INTO users (id, name, age, active) VALUES
    (1, 'Ada', 36, TRUE),
    (2, 'Grace', 45, FALSE),
    (3, 'Linus', NULL, NULL)"

cargo run -- ./data --exec "
  CREATE INDEX idx_users_age ON users (age)"

cargo run -- ./data --exec "
  SELECT id, name FROM users
  WHERE age >= 30
  ORDER BY age DESC
  LIMIT 10"
```

```
+----+-------+
| id | name  |
+----+-------+
| 2  | Grace |
| 1  | Ada   |
+----+-------+
2 row(s) in set
```

Use `--csv` for machine-readable output and `--help` for all options. In the
interactive shell, statements end with `;`; `.tables`, `.schema <table>`
(including its indexes), `.help` and `.quit` are available.

## Embedding

```rust
use rocksdb_sql_table::{Outcome, TableEngine};

fn main() -> rocksdb_sql_table::Result<()> {
    let engine = TableEngine::open("./data")?;
    engine.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")?;
    engine.execute("INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace')")?;
    engine.execute("CREATE INDEX idx_users_name ON users (name)")?;

    let results = engine.execute("SELECT id FROM users WHERE name = 'Grace'")?;
    if let Outcome::Rows(result) = &results[0] {
        assert_eq!(result.rows[0][0].render(), "2");
    }
    Ok(())
}
```

`TableEngine::execute` accepts one or more `;`-separated statements and returns
one `Outcome` per statement (`Rows`, `Modified`, `Created`, `Dropped`,
`CreatedIndex`, `DroppedIndex`). `TableEngine::list_indexes(table)` returns the
index catalog, which is also what `.schema` prints.

### Graph kernel

`GraphEngine` is the first storage layer for the graph direction. It is additive
and independent of the SQL engine; it includes a standard Gremlin v0 query
subset, snapshot read views, and explicit single-partition write transactions.
SQL statements still do not share a graph transaction, and distribution is not
implemented.

```rust
use std::collections::BTreeMap;
use rocksdb_sql_table::{Edge, GraphEngine, Value, Vertex};

fn main() -> rocksdb_sql_table::Result<()> {
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
use rocksdb_sql_table::{GraphEngine, TraversalDirection, TraversalOptions};

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
use rocksdb_sql_table::{GraphEngine, GremlinRow};

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
single-process, single-partition storage kernel: ids are globally unique `u64`
values supplied by the caller, one `GraphEngine` owns one partition, and there
is no cost-based planner, distributed transaction layer, or Raft layer yet. Both
endpoints of an edge must currently live in the same partition; cross-partition
boundary edges are a D1 design item. Scalar property values are a deliberate
first-milestone limit, not the final data model. The K3 single-partition
transaction and recovery boundary is specified in
[`docs/TRANSACTIONS.md`](docs/TRANSACTIONS.md). Snapshot-backed read views and
single-partition write transactions with conservative conflict detection are
implemented. A transaction commit uses one synced `WriteBatch`; a process-crash
harness covers the before-commit and after-commit boundaries. Autocommit graph
writes still use the old no-fsync behavior, and cross-partition transactions are
not implemented.

## SQL behavior details

- **Strict types.** Values are not implicitly converted between text and numeric
  types. `INT` is promoted to `REAL` when inserted into a `REAL` column; all
  other mismatches are errors. `BOOLEAN` accepts only `TRUE`/`FALSE`.
- **NULLs use SQL three-valued logic.** `NULL = NULL` is unknown, `WHERE` keeps
  only true rows, and `NOT UNKNOWN` remains unknown. `COUNT(*)` counts rows,
  `COUNT(col)` ignores NULLs; `SUM`/`AVG`/`MIN`/`MAX` ignore NULL inputs and
  return `NULL` when no non-NULL input remains. `SUM`/`AVG` currently return
  `REAL`; `MIN`/`MAX` preserve the input type.
- **Grouped queries stream.** A `GROUP BY`/aggregate query over a full table scan
  updates per-group state as rows are decoded, without collecting all matching
  rows; only the distinct groups are materialized. Non-aggregate queries still
  materialize their matching rows for sorting/pagination. `HAVING` is evaluated
  after aggregation and before `DISTINCT`/pagination; it may reference `GROUP BY`
  columns and aggregate functions, including aggregates not in the projection.
  It does not accept projection aliases.
- **DATE and DECIMAL are compatibility mappings.** `DATE` columns are validated
  ISO-8601 `TEXT` (`YYYY-MM-DD`), ordered lexicographically, and support
  `DATE '...'` plus `DATE ± INTERVAL '<n>' DAY`. `DECIMAL`/`NUMERIC` columns are
  stored as `REAL`; this is adequate for many TPC-H-shaped scans but is not
  exact fixed-point arithmetic. `INTERVAL` units other than `DAY` are rejected.
- **Primary keys are required** and imply `NOT NULL`. `INT` and `TEXT` keys are
  encoded order-preservingly, so rows are physically stored in primary-key order
  and `ORDER BY <pk>` needs no in-memory sort. Primary keys are case-sensitive.
- **`ORDER BY` defaults to NULLS LAST**, regardless of `ASC`/`DESC`; use
  `NULLS FIRST` to override. Ordering may reference non-projected columns.
- **`WHERE pk = literal`** uses a direct RocksDB point lookup when that is the
  entire top-level predicate. Other predicates can use a secondary index when
  the top-level `AND` conditions match a leading prefix of that index:
  equality conditions first, then at most one range condition on the next
  indexed column. Remaining predicates are still evaluated in memory, so index
  plans are conservative and always return the same rows as a table scan.
- **Index names are global.** `DROP INDEX <name>` finds the index across tables;
  names must therefore be unambiguous. `CREATE INDEX ... IF NOT EXISTS` is a
  no-op when the same name already exists on the same table; a name owned by a
  different table is an error.
- **Text identifiers** must match `[A-Za-z_][A-Za-z0-9_]*` and are capped at 64
  bytes. Unquoted and quoted identifiers are both matched exactly (no case
  folding), which differs from standard SQL.
- **Atomicity.** Each statement writes one RocksDB `WriteBatch`. A multi-statement
  script is not a transaction: earlier statements stay committed if a later one
  fails. Mutations are serialized by an in-process mutex; concurrent processes
  should not write the same database.

### TPC-H Q1/Q6 coverage

The engine accepts the SQL shape used by TPC-H Q1 and Q6, including `DATE`
literals, `DATE - INTERVAL ... DAY`, `DECIMAL` columns, grouped aggregates,
arithmetic aggregate arguments, and `ORDER BY` on grouping columns:

```sql
-- Q1
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       AVG(l_quantity) AS avg_qty,
       COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;

-- Q6
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1995-01-01'
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24;
```

The table definition is adapted to this engine's single-column primary-key
requirement: TPC-H `lineitem` has a composite `(l_orderkey, l_linenumber)` key,
so use a unique surrogate primary key (or make `l_orderkey` unique in a
benchmark copy). The query semantics over the remaining columns are unchanged.
The included `tpch_q1_q6` example generates a 200k-row lineitem-shaped dataset
and times both queries without external tools:

```bash
cargo run --release --example tpch_q1_q6 -- 200000 500
```

On an Apple M1 it loaded the data in about 2.8 s, ran Q6 in about 0.39 s and Q1
in about 0.51 s. Numbers depend on hardware and dataset shape.
This is functional coverage, not an official TPC-H benchmark result: there is
no dbgen/load pipeline, composite-key support, parallelism, or columnar scan
yet.

### Index DDL

```sql
CREATE INDEX idx_users_age ON users (age);
CREATE UNIQUE INDEX idx_users_name ON users (name);
CREATE INDEX idx_users_active_age ON users (active, age);
DROP INDEX idx_users_age;
DROP INDEX IF EXISTS idx_users_age;
```

`CREATE INDEX` accepts a comma-separated list of columns and `UNIQUE`. Index
column ordering and operator-class clauses (`ASC`, `DESC`, `USING ...`) are not
supported. Indexes are built synchronously: `CREATE INDEX` records a temporary
building state, scans the current rows, writes index entries, and then marks the
index ready. Readers and writers ignore a non-ready index, so an interrupted
build is invisible to queries and is rebuilt by rerunning `CREATE INDEX`.

`UNIQUE` indexes reject duplicate non-NULL indexed values. NULLs are distinct:
any number of rows may contain NULL in a unique index, matching SQL unique-index
semantics. Uniqueness is checked when building an index and on every subsequent
insert or update; deleting or updating a row removes its old index entries.

## Architecture

```text
row/<table>/<encoded-pk>                         -> JSON array of typed row values
meta/table/<table>                               -> JSON table schema
meta/index/<table>/<index>                       -> JSON index schema
idx/<table>/<index>/<encoded-values><encoded-pk> -> encoded primary key
```

- `meta/table/<name>` holds the table catalog entry. `CREATE TABLE` writes it;
  every row operation first resolves the schema from it.
- `meta/index/<table>/<index>` holds the index definition and its `ready` flag.
  Index definitions are kept separate from `TableSchema` so existing table
  catalogs remain readable.
- Row keys are prefixed per table, so a scan is a forward RocksDB iterator over
  one prefix. `INT` primary keys are encoded as sign-bit-flipped big-endian
  `u64`; `TEXT` keys escape NUL (`00 FF`) and terminate with `00 00`, which
  preserves byte ordering and prevents prefix collisions.
- Index keys append encoded indexed values to `idx/<table>/<index>/`, followed
  by the encoded primary key for non-unique indexes. Values are tagged by type
  and encoded order-preservingly: `NULL` first, then INT, REAL, TEXT and BOOLEAN
  in their SQL order. The primary-key suffix makes each non-unique entry unique,
  so an index scan can recover the base row without a separate lookup table.
- A unique index stores only the indexed values in the key and the encoded
  primary key in the value. A unique key may still be repeated when one of its
  columns is NULL, because NULLs are distinct; such entries include the primary
  key as a suffix.
- Index maintenance happens in the same `WriteBatch` as the base row, so a
  committed insert/update/delete cannot leave the index out of sync. `DROP TABLE`
  removes its index data and catalog entries in the same batch.
- Row payloads are JSON arrays tagged by type. This keeps values inspectable and
  the implementation small at the cost of space. The default build disables
  RocksDB compression codecs; enable the crate's codec features and set
  compression on custom `Options` if you need it.
- `TableEngine` parses SQL with [`sqlparser`](https://crates.io/crates/sqlparser)
  using `GenericDialect`, validates/executes against the catalog, and uses
  [`rust-rocksdb`](https://crates.io/crates/rocksdb) for storage.

Source layout:

```text
src/engine.rs   SQL parsing, planning, expressions, execution
src/storage.rs  RocksDB catalog/rows/indexes, scans, batches, constraints
src/key.rs      Key layout and order-preserving value encodings
src/schema.rs   Types, columns, table and index definitions
src/value.rs    Typed values, comparison, NULL semantics helpers
src/main.rs     `rocksql` CLI (REPL, .tables/.schema, CSV)
tests/sql.rs    End-to-end SQL behavior tests
```

## Testing

```bash
cargo test
```

The suite covers CRUD, point lookups, three-valued NULL logic, ordering and
pagination, `DISTINCT`, `COUNT`/`SUM`/`AVG`/`MIN`/`MAX`, `GROUP BY`, `HAVING`, TPC-H Q1/Q6-shaped
queries with `DATE` and `DECIMAL` columns, `LIKE`/`BETWEEN`/`IN`, text-key
ordering and escaping, primary-key conflicts, schema/type errors, persistence
across reopen,
table drop/recreate, index DDL, unique and composite indexes, index maintenance
on insert/update/delete, range plans, and NULL handling in unique indexes. Unit
tests in `src/key.rs` verify encoding round-trips and order preservation, and
planner tests in `src/engine.rs` verify index selection.

CI runs formatting, tests, and Clippy on Ubuntu for pushes to `main` and pull
requests; see `.github/workflows/ci.yml`. Locally run `cargo fmt --check` and
`cargo clippy --all-targets -- -D warnings` for the same checks.

## Limitations

This is a single-node storage kernel, not yet a distributed production database:

- No joins, subqueries, CTEs, window functions, or DISTINCT aggregate
  arguments. `GROUP BY` requires at least one aggregate projection or a HAVING
  aggregate; `HAVING` does not accept projection aliases.
- Indexes accelerate equality/range predicates on a leading index prefix; they
  do not accelerate `OR`, `IN`, `BETWEEN`, `LIKE`, or `IS NULL` predicates.
  There are no covering/index-only scans and no index-based ordering: matching
  base rows are still fetched and residual predicates, sorting and pagination
  are evaluated in memory. Index build is synchronous and materializes its
  `WriteBatch` in memory.
- No `ALTER TABLE`, `DEFAULT`, `UNIQUE` column constraints, `CHECK`, or
  foreign-key constraints. Indexes are created/dropped with `CREATE INDEX` /
  `DROP INDEX` only.
- `UPDATE`/`INSERT` accept literal values only (`SET age = age + 1` is rejected).
- `ORDER BY` must reference table columns, not output aliases.
- SQL has no multi-statement transactions; graph transactions are single-partition
  and single-process. Cross-partition/cross-process transactions, Raft replication,
  placement, and failover are not implemented (D2–D4); one writer process per
  database is assumed.
- Non-aggregate queries materialize all matching rows and sort results in
  memory; grouped/aggregate full scans stream rows but materialize the groups.
- `DECIMAL` is approximate (`REAL`), `DATE` is stored as text rather than a
  native date, and `INTERVAL` supports only `DAY`.
- No authentication, authorization, or SQL injection surface beyond the caller's
  own database (there is no network protocol).

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
resolver. Cross-host graph protocols, membership changes, migration and
cross-group fencing remain unimplemented.

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

Experimental [managed graph/v2 create transactions](docs/MANAGED_GRAPH_TRANSACTIONS.md) now connect vertex/edge plans to fixed-cohort commits and same-version reads. This trusted in-process slice does not enable remote Global Gremlin.
