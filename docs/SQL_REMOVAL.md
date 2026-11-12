# Removal of the legacy standalone SQL engine

VelaGraph now focuses on its property-graph kernel and distributed graph APIs.
The independent single-process SQL table engine is removed, not deprecated
behind an optional feature or retained as a compatibility layer.

## Removed public surfaces

- `TableEngine`, `Outcome`, `QueryResult`, table/column/index schema types and
  SQL-only value conversion helpers and errors.
- The `rocksql` executable, its REPL/meta-commands, SQL and CSV CLI options.
- The SQL-only `tpch_q1_q6` example, SQL tests and `sqlparser` dependency.

SQL applications must stay on a revision before this removal or migrate to a
separate SQL database. There is no automatic table-to-graph conversion: the old
SQL engine never queried the distributed graph. Preserve/export any SQL data
using the old version before upgrading applications that need those tables.
This source removal is not a command to delete database directories or old SQL
records; those records are no longer accessible through this library's API.

## Unchanged graph contracts

Graph property `Value` variants and serialized representations, graph key layouts,
record encodings, graph errors in use, Gremlin query semantics and distributed
protocols are unchanged. Existing graph data does not require a format migration.
`GraphEngine`, `GraphRecordStore` and distributed APIs remain the graph entrypoints.

## Rust package migration

The root package was renamed from `rocksdb-sql-table` to `velagraph`, and the
library import from `rocksdb_sql_table` to `velagraph`. Update Cargo dependencies,
Rust imports and fully-qualified paths, and any `cargo -p rocksdb-sql-table`
commands to `cargo -p velagraph`. No compatibility alias or SQL implementation
is retained. Workspace graph consumers and examples use the new name.

This is a source/package rename only. RocksDB remains the storage engine; graph
files, key layouts, record encodings and distributed wire protocols are unchanged.
Existing graph data needs no migration or rebuild because of this rename.

Historical evidence documents may mention SQL test counts or state that a past
change left SQL untouched. Those are observations for their named revisions,
not assertions that the removed engine remains part of the current product.
