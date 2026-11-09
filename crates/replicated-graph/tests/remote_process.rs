//! Real independent Raft processes, graph CRUD and client-side standard Gremlin.
//! This functional test deliberately does not claim crash/failover coverage.
use anyhow::Context;
use d2_raft::{send_control, ControlRequest, ControlResponse};
use replicated_graph::{run_blocking, RemoteStorageNode};
use rocksdb_sql_table::{
    Edge, Error, GraphRecordStore, GremlinRow, KeyRange, ScanLimit, StorageNode, StorageNodeError,
    Value, Vertex,
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

#[path = "support/remote_evidence.rs"]
mod remote_evidence;
struct Process {
    child: Child,
    index: usize,
    evidence: Option<Arc<remote_evidence::Evidence>>,
}
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(evidence) = &self.evidence {
            remote_evidence::cleanup_observed(&mut self.child, self.index, evidence);
        } else {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_independent_processes_graph_roundtrip() -> anyhow::Result<()> {
    timeout(Duration::from_secs(180), scenario()).await?
}

async fn scenario() -> anyhow::Result<()> {
    let evidence = remote_evidence::Evidence::prepare()?.map(Arc::new);
    let result = scenario_with_evidence(evidence.clone()).await;
    if let Some(evidence) = &evidence {
        evidence.record(
            "scenario-result.json",
            serde_json::json!({"success":result.is_ok()}),
        );
        let verified = evidence.verify();
        if result.is_ok() {
            verified?;
        } else if verified.is_err() {
            evidence.record(
                "provenance-incomplete.json",
                serde_json::json!({"verified":false}),
            );
        }
    }
    result
}

async fn scenario_with_evidence(
    evidence: Option<Arc<remote_evidence::Evidence>>,
) -> anyhow::Result<()> {
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
        let (stdout, stderr) = if let Some(evidence) = &evidence {
            let (out, err) = evidence
                .streams(i + 1)
                .context("create child evidence streams")?;
            command.env("VELAGRAPH_REMOTE_DIAGNOSTICS", "1");
            (Stdio::from(out), Stdio::from(err))
        } else {
            (Stdio::null(), Stdio::inherit())
        };
        let child = command.stdout(stdout).stderr(stderr).spawn()?;
        if let Some(evidence) = &evidence {
            evidence.record(
                &format!("node-{}-launch.json", i + 1),
                serde_json::json!({"pid":child.id(),"node":i + 1,"graph":addresses[3 * i + 2]}),
            );
        }
        processes.push(Process {
            child,
            index: i + 1,
            evidence: evidence.clone(),
        });
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
    let paged = paged_snapshot_roundtrip(addresses[3 * leader + 2], evidence.as_deref()).await;
    if paged.is_err() {
        if let Some(evidence) = &evidence {
            evidence.record(
                "paged-roundtrip-failed.json",
                serde_json::json!({"phase":"paged_snapshot_roundtrip"}),
            );
        }
    }
    paged?;
    drop(graph);
    drop(processes);
    Ok(())
}

// This stays inside the real three-process fixture: no mock responses, automatic
// failover, or transport-security claims. The graph contains >4 MiB of payload,
// so a 2 MiB page budget necessarily requires at least three nonempty pages.
async fn paged_snapshot_roundtrip(
    endpoint: std::net::SocketAddr,
    evidence: Option<&remote_evidence::Evidence>,
) -> anyhow::Result<()> {
    use replicated_graph::remote::{close_snapshot, open_snapshot, paged_snapshot, snapshot_page};
    const PAGE_BYTES: usize = 2 * 1024 * 1024;
    const PAYLOAD_BYTES: usize = 64 * 1024;
    let budget = Duration::from_secs(20);
    // A generous per-operation budget avoids turning debug JSON serialization
    // of multiple MiB into an incidental timing assertion.
    let large_graph = Arc::new(GraphRecordStore::new(
        RemoteStorageNode::new(endpoint, 7, budget)?,
        7,
        0,
    ));
    // Build canonical graph bytes through the local public graph API, then
    // submit small remote batches. This isolates read pagination from the
    // prototype Raft transport's large-write/short-election timing behavior.
    let fixture = run_blocking(move || -> anyhow::Result<_> {
        let dir = tempfile::tempdir()?;
        let local = GraphRecordStore::new(
            rocksdb_sql_table::LocalStorageNode::open(dir.path(), 7)?,
            7,
            0,
        );
        for id in 100..172 {
            let mut vertex = Vertex::new(id);
            vertex.labels.push("large".into());
            vertex
                .properties
                .insert("payload".into(), Value::Text("x".repeat(PAYLOAD_BYTES)));
            local.create_vertex(&vertex)?;
        }
        Ok(local.node().admin().export()?.entries)
    })
    .await??;
    for (index, row) in fixture.into_iter().enumerate() {
        let (key, value) = (row.key, row.value);
        let mut request_id = [0; 16];
        request_id[..8].copy_from_slice(&(index as u64).to_be_bytes());
        request_id[8..].copy_from_slice(b"paged-fi");
        replicated_graph::remote::write(
            endpoint,
            d2_raft::D1WriteRequest {
                shard: 7,
                epoch: 3,
                request_id,
                expected_sequence: None,
                operations: vec![d2_raft::WriteOp::Put { key, value }],
            },
            budget,
        )
        .await
        .with_context(|| format!("paged fixture seed row {index}"))?;
    }
    let before = paged_snapshot(endpoint, 7, budget)
        .await
        .inspect_err(|_| {
            if let Some(evidence) = evidence {
                evidence.record(
                    "initial-download-failed.json",
                    serde_json::json!({"phase":"initial_paged_download"}),
                );
            }
        })
        .context("initial paged download")?;
    let descriptor = open_snapshot(endpoint, None, 7, budget)
        .await
        .context("open pinned snapshot")?;
    assert_eq!(descriptor.shard, before.shard);
    assert_eq!(descriptor.epoch, before.epoch);
    assert_eq!(descriptor.sequence, before.sequence);
    assert_eq!(descriptor.rows, before.entries.len());
    assert!(descriptor.bytes > 2 * PAGE_BYTES);
    let first = snapshot_page(endpoint, None, descriptor.clone(), None, budget)
        .await
        .context("first pinned page")?;
    assert!(first.next.is_some());
    let first_entries = first.entries.clone();

    // Mutate after opening and fetching the first page, then finish the old
    // session. Its remaining pages must retain the same logical snapshot.
    let g = large_graph.clone();
    run_blocking(move || -> rocksdb_sql_table::Result<()> {
        let mut vertex = g.get_vertex(108)?.expect("large vertex exists");
        vertex
            .properties
            .insert("payload".into(), Value::Text("y".repeat(PAYLOAD_BYTES)));
        g.update_vertex(&vertex)?;
        g.delete_vertex(107)?;
        let mut added = Vertex::new(173);
        added.labels.push("large".into());
        g.create_vertex(&added)?;
        Ok(())
    })
    .await??;
    let mut page = first;
    let mut entries = Vec::new();
    let mut pages = 0;
    loop {
        assert_eq!(page.descriptor, descriptor);
        assert!(!page.entries.is_empty());
        assert!(page.entries.len() <= 4096);
        let bytes: usize = page.entries.iter().map(|(k, v)| k.len() + v.len()).sum();
        assert!(bytes <= PAGE_BYTES);
        entries.extend(page.entries);
        pages += 1;
        assert!(pages <= descriptor.rows, "pagination must make progress");
        let Some(cursor) = page.next else { break };
        page = snapshot_page(endpoint, None, descriptor.clone(), Some(cursor), budget).await?;
    }
    assert!(pages >= 3);
    assert_eq!(entries.len(), descriptor.rows);
    assert_eq!(
        entries
            .iter()
            .map(|(k, v)| k.len() + v.len())
            .sum::<usize>(),
        descriptor.bytes
    );
    assert!(entries.windows(2).all(|pair| pair[0].0 < pair[1].0));
    assert_eq!(entries, before.entries);
    let replay = snapshot_page(endpoint, None, descriptor.clone(), None, budget).await?;
    assert_eq!(replay.entries, first_entries);
    close_snapshot(endpoint, None, descriptor.clone(), budget).await?;
    assert!(snapshot_page(endpoint, None, descriptor, None, budget)
        .await
        .is_err());

    let after = paged_snapshot(endpoint, 7, budget)
        .await
        .context("final paged download")?;
    assert_eq!(after.sequence, before.sequence + 3);
    assert_eq!(after.entries.len(), before.entries.len());
    assert_ne!(after.entries, before.entries);
    let expected = after.entries;
    let g = large_graph.clone();
    run_blocking(move || -> rocksdb_sql_table::Result<()> {
        let node = g.node();
        let view = node.read_view()?;
        assert!(view.payload_bytes() > 2 * PAGE_BYTES);
        assert_eq!(
            node.get(&expected[0].0, &view)?,
            Some(expected[0].1.clone())
        );
        let rows = node.scan(
            KeyRange::all(),
            ScanLimit::new(100, 16 * 1024 * 1024)?,
            &view,
        )?;
        let actual = rows
            .into_iter()
            .map(|row| (row.key, row.value))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
        let mut cursor = None;
        let mut paged = Vec::new();
        loop {
            let page = node.scan_page(
                KeyRange::all(),
                ScanLimit::new(10, 1024 * 1024)?,
                &view,
                cursor.as_ref(),
            )?;
            paged.extend(page.rows.into_iter().map(|row| (row.key, row.value)));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(paged, expected);
        assert_eq!(
            g.get_vertex(108)?.unwrap().properties.get("payload"),
            Some(&Value::Text("y".repeat(PAYLOAD_BYTES)))
        );
        assert_eq!(
            g.execute_gremlin("g.V().hasLabel('large').count()")?.rows,
            vec![GremlinRow::Count(72)]
        );
        Ok(())
    })
    .await??;
    Ok(())
}
