//! Synced temporal batch recovery at deterministic before/after-commit boundaries.
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;
use velagraph::temporal_reference::Interval;
use velagraph::temporal_store::{TemporalMutation, TemporalStore};
use velagraph::{Edge, Vertex};

#[test]
fn synced_temporal_batch_survives_process_death() {
    for mode in ["before", "after"] {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = TemporalStore::open(dir.path()).unwrap();
            store
                .commit(&[TemporalMutation::Vertex {
                    valid: Interval { from: 0, to: None },
                    vertex: Vertex::new(1),
                }])
                .unwrap();
        }
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "temporal_crash_child",
                "--nocapture",
            ])
            .env("VELAGRAPH_TEMPORAL_CRASH_DIR", dir.path())
            .env("VELAGRAPH_TEMPORAL_CRASH_MODE", mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let stdout = child.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if line == "TEMPORAL_COMMIT_BOUNDARY" {
                    let _ = tx.send(());
                    break;
                }
            }
        });
        let reached = rx.recv_timeout(Duration::from_secs(20));
        let _ = child.kill();
        let status = child.wait().unwrap();
        assert!(reached.is_ok(), "child failed to reach boundary: {status}");
        let store = TemporalStore::open(dir.path()).unwrap();
        let view = store.view(None, 10).unwrap();
        assert_eq!(view.version(), if mode == "after" { 2 } else { 1 });
        assert_eq!(view.get_vertex(2).unwrap().is_some(), mode == "after");
        assert_eq!(view.get_edge(7).unwrap().is_some(), mode == "after");
        for query in ["g.V(1).out('LINK')", "g.V(2).in('LINK')"] {
            assert_eq!(
                view.execute_gremlin(query).unwrap().rows.len(),
                usize::from(mode == "after")
            );
        }
        assert!(store
            .view(Some(1), 10)
            .unwrap()
            .execute_gremlin("g.V(1).out()")
            .unwrap()
            .rows
            .is_empty());
        assert!(store
            .view(Some(1), 10)
            .unwrap()
            .get_edge(7)
            .unwrap()
            .is_none());
    }
}
#[test]
#[ignore = "spawned by temporal recovery test"]
fn temporal_crash_child() {
    let Ok(path) = std::env::var("VELAGRAPH_TEMPORAL_CRASH_DIR") else {
        return;
    };
    let store = TemporalStore::open(path).unwrap();
    let changes = [
        TemporalMutation::Vertex {
            valid: Interval { from: 5, to: None },
            vertex: Vertex::new(2),
        },
        TemporalMutation::Edge {
            valid: Interval { from: 5, to: None },
            edge: Edge::new(7, 1, 2, "LINK"),
        },
    ];
    if std::env::var("VELAGRAPH_TEMPORAL_CRASH_MODE").unwrap() == "after" {
        store.commit(&changes).unwrap();
    }
    println!("TEMPORAL_COMMIT_BOUNDARY");
    std::io::stdout().flush().unwrap();
    let mut line = String::new();
    std::io::stdin().read_line(&mut line).unwrap();
}
