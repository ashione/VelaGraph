//! Process-crash recovery tests for K3c.
//!
//! The parent test starts this same test binary as a child, seeds a committed
//! base graph, lets the child buffer a transaction, and then kills the child at
//! one of two deterministic points:
//!
//! - `before`: child is blocked after buffering, before `commit`;
//! - `after`: child has returned from the synced `commit`, then is blocked.
//!
//! No sleeps or ports are involved. A point inside RocksDB's WAL write is not
//! attempted here; that window is covered by RocksDB's own WAL guarantees.

use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use tempfile::TempDir;
use velagraph::{Edge, Error, GraphEngine, Value, Vertex};

const MODE_ENV: &str = "ROCKSQL_CRASH_MODE";
const DIR_ENV: &str = "ROCKSQL_CRASH_DIR";
const MARKER: &str = "READY_TO_COMMIT";

fn open(path: &Path) -> GraphEngine {
    GraphEngine::open(path).expect("open graph")
}

fn vertex(id: u64, label: &str, name: &str) -> Vertex {
    let mut vertex = Vertex::new(id);
    vertex.labels.push(label.to_string());
    vertex
        .properties
        .insert("name".to_string(), Value::Text(name.to_string()));
    vertex
}

fn edge(id: u64, from: u64, to: u64, label: &str) -> Edge {
    Edge::new(id, from, to, label)
}

fn seed_committed_base(path: &Path) {
    let graph = open(path);
    let mut transaction = graph.begin().unwrap();
    transaction
        .create_vertex(&vertex(1, "Person", "Ada"))
        .unwrap();
    transaction
        .create_vertex(&vertex(2, "Person", "Grace"))
        .unwrap();
    transaction.create_edge(&edge(10, 1, 2, "KNOWS")).unwrap();
    transaction.commit().unwrap();
}

fn spawn_child(path: &Path, mode: &str) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args(["--ignored", "--exact", "crash_child", "--nocapture"])
        .env(MODE_ENV, mode)
        .env(DIR_ENV, path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn crash child")
}

fn wait_for_marker(child: &mut Child) {
    let stdout = child.stdout.take().expect("child stdout");
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line == MARKER {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("child did not reach the crash marker");
}

fn kill(child: &mut Child) {
    child.kill().expect("kill child");
    child.wait().expect("wait for child");
}

#[test]
fn crash_before_commit_loses_the_buffered_batch() {
    let dir = TempDir::new().unwrap();
    seed_committed_base(dir.path());

    let mut child = spawn_child(dir.path(), "before");
    wait_for_marker(&mut child);
    kill(&mut child);

    let graph = open(dir.path());
    assert!(graph.get_vertex(1).unwrap().is_some());
    assert!(graph.get_vertex(2).unwrap().is_some());
    assert_eq!(graph.get_edge(10).unwrap().unwrap().to, 2);
    assert!(graph.get_vertex(3).unwrap().is_none());
    assert!(graph.get_edge(20).unwrap().is_none());
    assert_eq!(
        graph
            .out_edges(1, Some("KNOWS"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10]
    );
    assert!(matches!(
        graph.in_edges(3, Some("KNOWS")),
        Err(Error::VertexNotFound(3))
    ));
}

#[test]
fn crash_after_commit_keeps_the_whole_batch() {
    let dir = TempDir::new().unwrap();
    seed_committed_base(dir.path());

    let mut child = spawn_child(dir.path(), "after");
    wait_for_marker(&mut child);
    kill(&mut child);

    let graph = open(dir.path());
    assert_eq!(graph.get_vertex(3).unwrap().unwrap().id, 3);
    assert_eq!(graph.get_edge(20).unwrap().unwrap().to, 3);
    assert_eq!(
        graph
            .out_edges(1, Some("KNOWS"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [10, 20]
    );
    assert_eq!(
        graph
            .in_edges(3, Some("KNOWS"))
            .unwrap()
            .iter()
            .map(|edge| edge.id)
            .collect::<Vec<_>>(),
        [20]
    );
}

/// Runs only inside the spawned child process.
#[test]
#[ignore = "spawned by the recovery tests"]
fn crash_child() {
    let Ok(mode) = std::env::var(MODE_ENV) else {
        return;
    };
    let path = std::env::var(DIR_ENV).expect("child directory");
    let graph = open(Path::new(&path));

    let mut transaction = graph.begin().unwrap();
    transaction
        .create_vertex(&vertex(3, "Person", "Katherine"))
        .unwrap();
    transaction.create_edge(&edge(20, 1, 3, "KNOWS")).unwrap();

    match mode.as_str() {
        "before" => block_until_killed(),
        "after" => {
            transaction.commit().unwrap();
            block_until_killed();
        }
        other => panic!("unknown crash mode {other}"),
    }
}

fn block_until_killed() -> ! {
    println!("{MARKER}");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    let _ = std::io::stdin().read_line(&mut line);
    std::process::exit(0);
}
