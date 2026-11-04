# rocksdb-sql-table

A small, embeddable **table engine backed by RocksDB** with a simple SQL front
end. It is a self-contained implementation (not a SQL layer over SQLite): table
catalogs, secondary indexes, and rows live in RocksDB key spaces, and SQL
statements are parsed and executed by this crate.

The SQL subset is intentionally small and strict: create/drop tables, create/drop
indexes, insert, update, delete, and single-table `SELECT` with filtering,
ordering, pagination, `DISTINCT` and `COUNT`.

## Features

| Area | Supported |
| --- | --- |
| DDL | `CREATE TABLE [IF NOT EXISTS]`, `DROP TABLE [IF EXISTS]`, `CREATE [UNIQUE] INDEX [IF NOT EXISTS]`, `DROP INDEX [IF EXISTS]` |
| Types | `INT` (`INTEGER`/`BIGINT` aliases), `REAL` (`DOUBLE`/`FLOAT`), `TEXT` (`VARCHAR`/`STRING`), `BOOLEAN` |
| Keys | exactly one single-column primary key of type `INT` or `TEXT` |
| Indexes | single-column and composite indexes; uniqueness; equality and range lookups on a leading column prefix |
| DML | multi-row `INSERT ... VALUES`, `UPDATE ... SET ... [WHERE]`, `DELETE [WHERE]` |
| Queries | projection, `WHERE`, `ORDER BY ... [ASC\|DESC] [NULLS FIRST\|LAST]`, `LIMIT`/`OFFSET`, `DISTINCT`, `COUNT(*)`/`COUNT(col)` |
| Predicates | `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `[NOT] LIKE ... [ESCAPE]`, `[NOT] BETWEEN`, `[NOT] IN` |
| Storage | RocksDB WAL-backed writes, one atomic `WriteBatch` per statement, point lookups for `pk = literal`, index prefix scans |

Deliberately **not** supported: joins, subqueries, `GROUP BY`, aggregates other
than `COUNT`, `ALTER TABLE`, `DEFAULT`/`UNIQUE`/foreign-key column constraints,
multi-statement transactions, and `SELECT` without `FROM`. See
[Limitations](#limitations).

## Quick start

Prerequisites: a C++ toolchain and libclang (needed by `librocksdb-sys`), plus
Rust 1.88 or newer. On macOS with the Xcode Command Line Tools:

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

## SQL behavior details

- **Strict types.** Values are not implicitly converted between text and numeric
  types. `INT` is promoted to `REAL` when inserted into a `REAL` column; all
  other mismatches are errors. `BOOLEAN` accepts only `TRUE`/`FALSE`.
- **NULLs use SQL three-valued logic.** `NULL = NULL` is unknown, `WHERE` keeps
  only true rows, and `NOT UNKNOWN` remains unknown. `COUNT(*)` counts rows,
  `COUNT(col)` ignores NULLs; `COUNT` works without `GROUP BY`.
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
pagination, `DISTINCT`, `COUNT`, `LIKE`/`BETWEEN`/`IN`, text-key ordering and
escaping, primary-key conflicts, schema/type errors, persistence across reopen,
table drop/recreate, index DDL, unique and composite indexes, index maintenance
on insert/update/delete, range plans, and NULL handling in unique indexes. Unit
tests in `src/key.rs` verify encoding round-trips and order preservation, and
planner tests in `src/engine.rs` verify index selection.

CI runs formatting, tests, and Clippy on Ubuntu for pushes to `main` and pull
requests; see `.github/workflows/ci.yml`. Locally run `cargo fmt --check` and
`cargo clippy --all-targets -- -D warnings` for the same checks.

## Limitations

This is a teaching/demo-scale engine, not a production database:

- No joins, subqueries, CTEs, `GROUP BY`, `HAVING`, or aggregates beyond `COUNT`.
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
- No multi-statement or cross-process transactions; one writer process assumed.
- All matching rows and sort results are materialized in memory.
- No authentication, authorization, or SQL injection surface beyond the caller's
  own database (there is no network protocol).
