//! Generate a lineitem-shaped dataset and time TPC-H Q1/Q6.
//!
//! The engine requires a single-column primary key, so this benchmark uses a
//! unique surrogate `l_orderkey` and is not an official TPC-H run.
//!
//! ```bash
//! cargo run --release --example tpch_q1_q6 -- 200000 500
//! ```

use std::env;
use std::fs;
use std::time::Instant;

use rocksdb_sql_table::{Outcome, QueryResult, TableEngine, Value};

const SCHEMA: &str = "CREATE TABLE lineitem (\
    l_orderkey INT PRIMARY KEY,\
    l_partkey INT,\
    l_suppkey INT,\
    l_linenumber INT,\
    l_quantity DECIMAL(15,2),\
    l_extendedprice DECIMAL(15,2),\
    l_discount DECIMAL(15,2),\
    l_tax DECIMAL(15,2),\
    l_returnflag TEXT,\
    l_linestatus TEXT,\
    l_shipdate DATE,\
    l_commitdate DATE,\
    l_receiptdate DATE,\
    l_shipinstruct TEXT,\
    l_shipmode TEXT,\
    l_comment TEXT\
)";

const Q1: &str = "SELECT l_returnflag, l_linestatus, \
    SUM(l_quantity) AS sum_qty, \
    SUM(l_extendedprice) AS sum_base_price, \
    SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
    AVG(l_quantity) AS avg_qty, \
    COUNT(*) AS count_order \
    FROM lineitem \
    WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY \
    GROUP BY l_returnflag, l_linestatus \
    ORDER BY l_returnflag, l_linestatus";

const Q6: &str = "SELECT SUM(l_extendedprice * l_discount) AS revenue \
    FROM lineitem \
    WHERE l_shipdate >= DATE '1994-01-01' \
      AND l_shipdate < DATE '1995-01-01' \
      AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01 \
      AND l_quantity < 24";

fn main() -> rocksdb_sql_table::Result<()> {
    let mut args = env::args().skip(1);
    let rows = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(200_000);
    let batch_size = args
        .next()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(500)
        .max(1);

    let path = env::temp_dir().join(format!("rocksql-tpch-{}", std::process::id()));
    let _ = fs::remove_dir_all(&path);
    let engine = TableEngine::open(&path)?;
    engine.execute(SCHEMA)?;

    let started = Instant::now();
    for start in (1..=rows).step_by(batch_size) {
        let end = (start + batch_size - 1).min(rows);
        let mut sql = String::from("INSERT INTO lineitem VALUES ");
        for i in start..=end {
            if i > start {
                sql.push(',');
            }
            // The dates stay inside 1992..=1998, and every generated date is
            // a valid calendar date (day 1..=28 in every month).
            let year = 1992 + (i % 7);
            let month = 1 + (i % 12);
            let day = 1 + (i % 28);
            let quantity = 1 + (i % 50);
            let extended_price = 1_000 + (i % 100_000);
            let discount = (i % 11) as f64 / 100.0;
            let tax = (i % 9) as f64 / 100.0;
            let return_flag = ["N", "R", "A"][i % 3];
            let line_status = ["F", "O"][i % 2];
            sql.push_str(&format!(
                "({i}, {part}, {supplier}, {line}, {quantity}, {extended_price}, \
                 {discount:.2}, {tax:.2}, '{return_flag}', '{line_status}', \
                 DATE '{year:04}-{month:02}-{day:02}', \
                 DATE '{year:04}-{month:02}-{day:02}', \
                 DATE '{year:04}-{month:02}-{day:02}', \
                 'DELIVER IN PERSON', 'TRUCK', 'comment {i}')",
                part = i % 20_000,
                supplier = i % 10_000,
                line = i % 7 + 1,
            ));
        }
        sql.push(';');
        engine.execute(&sql)?;
    }
    println!("loaded {rows} rows in {:.2?}", started.elapsed());

    let started = Instant::now();
    let q6 = query(&engine, Q6)?;
    println!("Q6 in {:.3?}: {}", started.elapsed(), render(&q6));

    let started = Instant::now();
    let q1 = query(&engine, Q1)?;
    println!("Q1 in {:.3?}:", started.elapsed());
    for row in &q1.rows {
        println!("  {}", render_row(row));
    }

    drop(engine);
    let _ = fs::remove_dir_all(&path);
    Ok(())
}

fn query(engine: &TableEngine, sql: &str) -> rocksdb_sql_table::Result<QueryResult> {
    match engine.execute(sql)?.pop() {
        Some(Outcome::Rows(result)) => Ok(result),
        other => panic!("expected rows, got {other:?}"),
    }
}

fn render(result: &QueryResult) -> String {
    result
        .rows
        .iter()
        .map(|row| render_row(row))
        .collect::<Vec<_>>()
        .join("; ")
}

fn render_row(row: &[Value]) -> String {
    row.iter().map(Value::render).collect::<Vec<_>>().join(", ")
}
