//! Real independent Raft processes, graph CRUD and client-side standard Gremlin.
//! This functional test deliberately does not claim crash/failover coverage.
use d2_raft::{send_control, ControlRequest, ControlResponse};
use replicated_graph::{run_blocking, RemoteStorageNode};
use rocksdb_sql_table::{
    Edge, Error, GraphRecordStore, GremlinRow, StorageNode, StorageNodeError, Value, Vertex,
};
use std::{
    collections::BTreeMap,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    time::{sleep, timeout},
};

struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_independent_processes_graph_roundtrip() -> anyhow::Result<()> {
    timeout(Duration::from_secs(50), scenario()).await?
}

async fn scenario() -> anyhow::Result<()> {
    let mut reservations = Vec::new();
    for _ in 0..9 {
        reservations.push(TcpListener::bind("127.0.0.1:0").await?);
    }
    let addresses = reservations
        .iter()
        .map(|s| s.local_addr())
        .collect::<std::io::Result<Vec<_>>>()?;
    let members = (0..3)
        .map(|i| format!("{}={}", i + 1, addresses[3 * i]))
        .collect::<Vec<_>>()
        .join(",");
    drop(reservations);
    let dirs = (0..3)
        .map(|_| tempfile::tempdir())
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut processes = Vec::new();
    for i in 0..3 {
        let mut command = Command::new(env!("CARGO_BIN_EXE_graph-node"));
        command.args([
            "--allow-insecure-development",
            "--id",
            &(i + 1).to_string(),
            "--raft",
            &addresses[3 * i].to_string(),
            "--control",
            &addresses[3 * i + 1].to_string(),
            "--graph",
            &addresses[3 * i + 2].to_string(),
            "--shard",
            "7",
            "--epoch",
            "3",
            "--data-dir",
            dirs[i].path().to_str().unwrap(),
        ]);
        if i == 0 {
            command.args(["--bootstrap", &members]);
        }
        processes.push(Process(
            command
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        ));
    }
    let leader = timeout(Duration::from_secs(35), async {
        loop {
            for i in 0..3 {
                if let Ok(ControlResponse::Status(s)) =
                    send_control(&addresses[3 * i + 1].to_string(), ControlRequest::Status).await
                {
                    if s.leader == Some((i + 1) as u64)
                        && replicated_graph::remote::snapshot(
                            addresses[3 * i + 2],
                            7,
                            Duration::from_millis(300),
                        )
                        .await
                        .is_ok()
                    {
                        return i;
                    }
                }
            }
            sleep(Duration::from_millis(30)).await;
        }
    })
    .await?;
    timeout(Duration::from_secs(5), async {
        for i in 0..3 {
            loop {
                if replicated_graph::remote::metadata(
                    addresses[3 * i + 2],
                    7,
                    Duration::from_millis(300),
                )
                .await
                .is_ok()
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        }
    })
    .await?;
    let node = RemoteStorageNode::new(addresses[3 * leader + 2], 7, Duration::from_secs(2))?;
    let graph = Arc::new(GraphRecordStore::new(node, 7, 0));
    let g = graph.clone();
    run_blocking(move || -> rocksdb_sql_table::Result<()> {
        assert_eq!(
            g.execute_gremlin("g.V().count()")?.rows,
            vec![GremlinRow::Count(0)]
        );
        let mut a = Vertex::new(1);
        a.labels.push("person".into());
        a.properties
            .insert("name".into(), Value::Text("alice".into()));
        let mut b = Vertex::new(2);
        b.labels.push("person".into());
        b.properties
            .insert("name".into(), Value::Text("bob".into()));
        g.create_vertex(&a)?;
        let prepared = g.prepare_create_vertex(&b)?;
        let id = prepared.request_id();
        g.commit_prepared(&prepared)?;
        g.commit_prepared(&prepared)?; // Exact replay returns success, no new sequence.
        assert_eq!(prepared.request_id(), id);
        assert_eq!(g.node().read_view()?.sequence, 2);
        assert_eq!(g.get_vertex(2)?, Some(b.clone()));
        assert!(g.create_edge(&Edge::new(9, 1, 99, "knows")).is_err());
        let edge = Edge::new(9, 1, 2, "knows");
        g.create_edge(&edge)?;
        assert_eq!(g.get_edge(9)?, Some(edge));
        assert_eq!(
            g.execute_gremlin("g.V(1).out('knows').values('name')")?
                .rows,
            vec![GremlinRow::Value(Value::Text("bob".into()))]
        );
        let pinned = g.read_view()?;
        let seq = pinned.sequence();
        b.properties = BTreeMap::from([("name".into(), Value::Text("robert".into()))]);
        g.update_vertex(&b)?;
        assert_eq!(pinned.sequence(), seq);
        assert_eq!(
            pinned.execute_gremlin("g.V(2).values('name')")?.rows,
            vec![GremlinRow::Value(Value::Text("bob".into()))]
        );
        assert_eq!(g.get_vertex(2)?, Some(b));
        let mut edge = Edge::new(9, 1, 2, "knows");
        edge.properties.insert("since".into(), Value::Integer(2026));
        g.update_edge(&edge)?;
        assert_eq!(g.get_edge(9)?, Some(edge));
        g.delete_edge(9)?;
        assert_eq!(g.get_edge(9)?, None);
        g.delete_vertex(2)?;
        assert_eq!(g.get_vertex(2)?, None);
        assert_eq!(
            g.execute_gremlin("g.V().count()")?.rows,
            vec![GremlinRow::Count(1)]
        );
        assert!(g.execute_gremlin("g.V().madeUpStep()").is_err());
        let first = g.prepare_create_vertex(&Vertex::new(30))?;
        let stale = g.prepare_create_vertex(&Vertex::new(31))?;
        g.commit_prepared(&first)?;
        assert!(matches!(
            g.commit_prepared(&stale),
            Err(Error::StorageNode(StorageNodeError::Conflict { .. }))
        ));
        assert_eq!(g.get_vertex(31)?, None);
        g.delete_vertex(30)?;
        Ok(())
    })
    .await??;
    // A caller chooses the target explicitly. Followers never pretend to serve
    // a new linearizable view; no transparent forwarding or automatic replay.
    let follower = (leader + 1) % 3;
    let follower_node =
        RemoteStorageNode::new(addresses[3 * follower + 2], 7, Duration::from_secs(2))?;
    assert!(matches!(
        run_blocking(move || follower_node.read_view()).await?,
        Err(StorageNodeError::NotLeader { .. })
    ));
    let other = RemoteStorageNode::new(addresses[3 * leader + 2], 99, Duration::from_secs(2))?;
    assert!(run_blocking(move || other.read_view()).await?.is_err());
    drop(graph);
    drop(processes);
    Ok(())
}
