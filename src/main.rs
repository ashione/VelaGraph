use std::env;
use std::error::Error as StdError;
use std::io::{self, IsTerminal, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use rocksdb_sql_table::{Outcome, QueryResult, TableEngine, Value};

type Result<T> = std::result::Result<T, Box<dyn StdError>>;

struct Args {
    database: PathBuf,
    exec: Vec<String>,
    csv: bool,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<()> {
    let args = parse_args()?;
    let engine = TableEngine::open(&args.database)?;

    if args.exec.is_empty() {
        repl(&engine, args.csv)?;
    } else {
        for sql in &args.exec {
            print_outcomes(engine.execute(sql)?, args.csv);
        }
    }
    Ok(())
}

fn parse_args() -> Result<Args> {
    let mut database = None;
    let mut exec = Vec::new();
    let mut csv = false;
    let mut arguments = env::args().skip(1);

    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            "--csv" => csv = true,
            "-e" | "--exec" => {
                let sql = arguments
                    .next()
                    .ok_or_else(|| "--exec requires a SQL argument".to_string())?;
                exec.push(sql);
            }
            other if other.starts_with("--exec=") => {
                exec.push(other["--exec=".len()..].to_string());
            }
            other if other.starts_with('-') => {
                return Err(format!("unknown option `{other}`; try --help").into())
            }
            other => {
                if database.is_some() {
                    return Err(format!("unexpected argument `{other}`").into());
                }
                database = Some(PathBuf::from(other));
            }
        }
    }

    Ok(Args {
        database: database.ok_or_else(|| "missing <DB_PATH>; try --help".to_string())?,
        exec,
        csv,
    })
}

fn repl(engine: &TableEngine, csv: bool) -> Result<()> {
    let stdin = io::stdin();
    let interactive = stdin.is_terminal();
    let mut buffer = String::new();

    loop {
        if interactive {
            let prompt = if buffer.trim().is_empty() {
                "rocksql> "
            } else {
                "     -> "
            };
            print!("{prompt}");
            io::stdout().flush()?;
        }

        let mut line = String::new();
        if stdin.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim();

        if buffer.trim().is_empty() && trimmed.starts_with('.') {
            if meta_command(trimmed, engine)? {
                break;
            }
            continue;
        }

        buffer.push_str(&line);
        if trimmed.ends_with(';') {
            match engine.execute(&buffer) {
                Ok(outcomes) => print_outcomes(outcomes, csv),
                Err(error) => eprintln!("error: {error}"),
            }
            buffer.clear();
        }
    }

    if !buffer.trim().is_empty() {
        eprintln!("warning: incomplete statement ignored (missing `;`)");
    }
    Ok(())
}

/// Returns `true` when the REPL should exit.
fn meta_command(command: &str, engine: &TableEngine) -> Result<bool> {
    let mut parts = command.splitn(2, char::is_whitespace);
    let name = parts.next().unwrap_or_default();
    let argument = parts.next().unwrap_or_default().trim();

    match name {
        ".quit" | ".exit" => return Ok(true),
        ".tables" => match engine.list_tables() {
            Ok(tables) if tables.is_empty() => println!("(no tables)"),
            Ok(tables) => {
                for table in tables {
                    println!("{}", table.name);
                }
            }
            Err(error) => eprintln!("error: {error}"),
        },
        ".schema" => {
            if argument.is_empty() {
                eprintln!("usage: .schema <table>");
            } else {
                match engine.table_schema(argument) {
                    Ok(schema) => println!("{}", schema.describe()),
                    Err(error) => eprintln!("error: {error}"),
                }
            }
        }
        ".help" => print_help(),
        other => eprintln!("unknown command `{other}`; type .help"),
    }
    Ok(false)
}

fn print_outcomes(outcomes: Vec<Outcome>, csv: bool) {
    let multiple = outcomes.len() > 1;
    for (index, outcome) in outcomes.into_iter().enumerate() {
        if multiple && !csv {
            if index > 0 {
                println!();
            }
            println!("-- statement {} --", index + 1);
        }
        match outcome {
            Outcome::Rows(result) => print_query_result(&result, csv),
            Outcome::Modified { affected } => println!("{affected} row(s) affected"),
            Outcome::Created { table, existed } => {
                if existed {
                    println!("table `{table}` already exists");
                } else {
                    println!("table `{table}` created");
                }
            }
            Outcome::Dropped { table, existed } => {
                if existed {
                    println!("table `{table}` dropped");
                } else {
                    println!("table `{table}` did not exist");
                }
            }
        }
    }
}

fn print_query_result(result: &QueryResult, csv: bool) {
    if csv {
        println!(
            "{}",
            result
                .columns
                .iter()
                .map(|column| csv_field(column))
                .collect::<Vec<_>>()
                .join(",")
        );
        for row in &result.rows {
            println!(
                "{}",
                row.iter()
                    .map(|value| csv_field(&value.render()))
                    .collect::<Vec<_>>()
                    .join(",")
            );
        }
        return;
    }

    let cells = result
        .rows
        .iter()
        .map(|row| row.iter().map(table_cell).collect::<Vec<_>>())
        .collect::<Vec<_>>();
    let widths = result
        .columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            cells
                .iter()
                .map(|row| display_width(&row[index]))
                .chain(std::iter::once(display_width(column)))
                .max()
                .unwrap_or(0)
        })
        .collect::<Vec<_>>();

    print_border(&widths);
    print_row(&result.columns, &widths);
    print_border(&widths);
    for row in &cells {
        print_row(row, &widths);
    }
    if !cells.is_empty() {
        print_border(&widths);
    }
    println!("{} row(s) in set", result.rows.len());
}

fn table_cell(value: &Value) -> String {
    value.render().replace('\n', "\\n").replace('\r', "\\r")
}

fn display_width(value: &str) -> usize {
    value.chars().count()
}

fn print_border(widths: &[usize]) {
    let mut border = String::from("+");
    for width in widths {
        border.push_str(&"-".repeat(width + 2));
        border.push('+');
    }
    println!("{border}");
}

fn print_row(cells: &[String], widths: &[usize]) {
    let mut line = String::from("|");
    for (cell, width) in cells.iter().zip(widths) {
        line.push(' ');
        line.push_str(cell);
        line.push_str(&" ".repeat(width.saturating_sub(display_width(cell))));
        line.push_str(" |");
    }
    println!("{line}");
}

fn csv_field(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') || value.contains('\r') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn print_help() {
    println!(
        "\
rocksql - a small SQL table engine on top of RocksDB

Usage:
  rocksql [OPTIONS] <DB_PATH>

Options:
  -e, --exec <SQL>   Execute SQL and exit; may be repeated
      --csv          Print query results as CSV
  -h, --help         Show this help

Interactive commands:
  .tables            List tables
  .schema <table>    Show a table definition
  .help              Show this help
  .quit              Exit

Examples:
  rocksql ./data --exec \"CREATE TABLE users (id INT PRIMARY KEY, name TEXT NOT NULL)\"
  rocksql ./data --exec \"INSERT INTO users VALUES (1, 'Ada')\" --exec \"SELECT * FROM users\"
"
    );
}
