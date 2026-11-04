//! A small SQL table engine built on top of RocksDB.
//!
//! The engine stores table definitions in a catalog key space and rows in
//! primary-key order under a per-table prefix. It supports a deliberately small
//! SQL subset: `CREATE TABLE`, `DROP TABLE`, `CREATE INDEX`, `DROP INDEX`,
//! `INSERT`, `UPDATE`, `DELETE`, and single-table `SELECT` with `WHERE`,
//! `ORDER BY`, `LIMIT`/`OFFSET`, `DISTINCT`, `GROUP BY`, `HAVING` and
//! `COUNT`/`SUM`/`AVG`/`MIN`/`MAX` aggregates.
//!
//! ```no_run
//! use rocksdb_sql_table::{Outcome, TableEngine};
//!
//! # fn main() -> rocksdb_sql_table::Result<()> {
//! let engine = TableEngine::open("./data")?;
//! engine.execute("CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)")?;
//! engine.execute("INSERT INTO users (id, name) VALUES (1, 'Ada'), (2, 'Grace')")?;
//!
//! let outcomes = engine.execute("SELECT id, name FROM users ORDER BY id")?;
//! if let Outcome::Rows(result) = &outcomes[0] {
//!     assert_eq!(result.rows.len(), 2);
//! }
//! # Ok(())
//! # }
//! ```

mod engine;
mod error;
mod key;
mod schema;
mod storage;
mod value;

pub use engine::{Outcome, QueryResult, TableEngine};
pub use error::{Error, Result};
pub use schema::{Column, DataType, IndexSchema, TableSchema};
pub use value::Value;
