# rocksdb-sql-table

A small, embeddable **table engine backed by RocksDB** with a simple SQL front
end. It is a self-contained implementation (not a SQL layer over SQLite): table
catalogs and rows live in RocksDB key spaces, and SQL statements are parsed and
executed by this crate.

The SQL subset is intentionally small and strict: create/drop tables, insert,
update, delete, and single-table `SELECT` with filtering, ordering, pagination,
`DISTINCT` and `COUNT`.

## Features

| Area | Supported |
| --- | --- |
| DDL | `CREATE TABLE [IF NOT EXISTS]`, `DROP TABLE [IF EXISTS]` |
| Types | `INT` (`INTEGER`/`BIGINT` aliases), `REAL` (`DOUBLE`/`FLOAT`), `TEXT` (`VARCHAR`/`STRING`), `BOOLEAN` |
| Keys | exactly one single-column primary key of type `INT` or `TEXT` |
| DML | multi-row `INSERT ... VALUES`, `UPDATE ... SET ... [WHERE]`, `DELETE [WHERE]` |
| Queries | projection, `WHERE`, `ORDER BY ... [ASC\|DESC] [NULLS FIRST\|LAST]`, `LIMIT`/`OFFSET`, `DISTINCT`, `COUNT(*)`/`COUNT(col)` |
| Predicates | `=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`, `IS [NOT] NULL`, `[NOT] LIKE ... [ESCAPE]`, `[NOT] BETWEEN`, `[NOT] IN` |
| Storage | RocksDB WAL-backed writes, one atomic `WriteBatch` per statement, point lookups for `pk = literal` |

Deliberately **not** supported: joins, subqueries, `GROUP BY`, aggregates other
than `COUNT`, secondary indexes, `ALTER TABLE`, `DEFAULT`/`UNIQUE`/foreign-key
constraints, multi-statement transactions, and `SELECT` without `FROM`. See
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
  SELECT id, name FROM users
  WHERE active = TRUE OR age IS NULL
  ORDER BY age DESC
  LIMIT 10"
```

```
+----+-------+
| id | name  |
+----+-------+
| 1  | Ada   |
| 3  | Linus |
+----+-------+
2 row(s) in set
```

Use `--csv` for machine-readable output and `--help` for all options. In the
interactive shell, statements end with `;`; `.tables`, `.schema <table>`,
`.help` and `.quit` are available.

## Embedding

```rust
use rocksdb_sql_table::{Outcome, TableEngine};

fn main() -> rocksdb_sql_table::Result<()> {
    let engine = TableEngine::open("./data")?;
    engine.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")?;
    engine.execute("INSERT INTO users VALUES (1, 'Ada'), (2, 'Grace')")?;

    let results = engine.execute("SELECT name FROM users WHERE id = 2")?;
    if let Outcome::Rows(result) = &results[0] {
        assert_eq!(result.rows[0][0].render(), "Grace");
    }
    Ok(())
}
```

`TableEngine::execute` accepts one or more `;`-separated statements and returns
one `Outcome` per statement (`Rows`, `Modified`, `Created`, `Dropped`).

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
  entire top-level predicate (an `AND`/`OR` clause scans). All other predicates
  scan the table's key prefix and filter in memory. There are no secondary
  indexes, so non-key predicates are O(table scan + result size).
- **Text identifiers** must match `[A-Za-z_][A-Za-z0-9_]*` and are capped at 64
  bytes. Unquoted and quoted identifiers are both matched exactly (no case
  folding), which differs from standard SQL.
- **Atomicity.** Each statement writes one RocksDB `WriteBatch`. A multi-statement
  script is not a transaction: earlier statements stay committed if a later one
  fails. Mutations are serialized by an in-process mutex; concurrent processes
  should not write the same database.

## Architecture

```text
row/<table>/<encoded-pk>   ->  JSON array of typed row values
meta/table/<table>         ->  JSON table schema (columns, primary key)
```

- `meta/table/<name>` holds the catalog entry. `CREATE TABLE` writes it; every
  row operation first resolves the schema from it.
- Row keys are prefixed per table, so a scan is a forward RocksDB iterator over
  one prefix. `INT` primary keys are encoded as sign-bit-flipped big-endian
  `u64`; `TEXT` keys escape NUL (`00 FF`) and terminate with `00 00`, which
  preserves byte ordering and prevents prefix collisions.
- Row payloads are JSON arrays tagged by type. This keeps values inspectable and
  the implementation small at the cost of space. The default build disables
  RocksDB compression codecs; enable the crate's codec features and set
  compression on custom `Options` if you need it.
- `TableEngine` parses SQL with [`sqlparser`](https://crates.io/crates/sqlparser)
  using `GenericDialect`, validates/executes against the catalog, and uses
  [`rust-rocksdb`](https://crates.io/crates/rocksdb) for storage.
- Mutations (insert/update/delete) validate primary-key uniqueness and commit in
  one batch. `UPDATE` evaluates matches before writing, so a conflict (for
  example, moving a key onto an existing row) leaves all data unchanged.

Source layout:

```text
src/engine.rs   SQL parsing, planning, expressions, execution
src/storage.rs  RocksDB catalog/rows, scans, batches, uniqueness checks
src/key.rs      Key layout and order-preserving primary-key encoding
src/schema.rs   Types, columns, table definitions
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
and table drop/recreate. Unit tests in `src/key.rs` verify encoding round-trips
and order preservation.

Run `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` for
formatting and lint checks.

## Limitations

This is a teaching/demo-scale engine, not a production database:

- No joins, subqueries, CTEs, `GROUP BY`, `HAVING`, or aggregates beyond `COUNT`.
- No secondary indexes, so non-key predicates scan the whole table.
- No `ALTER TABLE`, `DEFAULT`, `UNIQUE`, `CHECK`, or foreign-key constraints.
- `UPDATE`/`INSERT` accept literal values only (`SET age = age + 1` is rejected).
- `ORDER BY` must reference table columns, not output aliases.
- No multi-statement or cross-process transactions; one writer process assumed.
- All matching rows and sort results are materialized in memory.
- No authentication, authorization, or SQL injection surface beyond the caller's
  own database (there is no network protocol).
