//! Real independent data processes; coordinator runs in this test process.
//! This verifies distributed query semantics, not failover or transport security.
use anyhow::{ensure, Context};
use replicated_graph::{
    catalog::{Catalog, Placement},
    remote::{
        self,
        distributed::{
            self, CoordinatorService, DistributedRequest, PartitionRoute, StaticCatalog,
        },
        query::{QualifiedElement, QueryCompleteness, QueryFailure, QueryRow},
        RemoteIdentity,
    },
    routing::{
        experimental::{ObservedElementRef, ObservedPathElement, ObservedRowV3},
        RemoteReplica, RouteResolver,
    },
    run_blocking,
};
use rocksdb_sql_table::{
    experimental_graph::{AppendOnlyStore, CrossEdge, VertexRef},
    Export, KeyRange, KeyValue, LocalStorageNode, ReadView, ScanLimit, StorageAdmin, StorageNode,
    StorageNodeError, Value, Vertex, WriteRequest, WriteResult,
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::{sleep, timeout},
};
const BUDGET: Duration = Duration::from_secs(20);
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[derive(Clone)]
struct Shared(Arc<LocalStorageNode>);
impl StorageNode for Shared {
    fn read_view(&self) -> Result<ReadView, StorageNodeError> {
        self.0.read_view()
    }
    fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>, StorageNodeError> {
        self.0.get(key, view)
    }
    fn scan(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
    ) -> Result<Vec<KeyValue>, StorageNodeError> {
        self.0.scan(range, limit, view)
    }
    fn scan_page(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
        cursor: Option<&rocksdb_sql_table::ScanCursor>,
    ) -> Result<rocksdb_sql_table::ScanPage, StorageNodeError> {
        self.0.scan_page(range, limit, view, cursor)
    }
    fn batch_write(&self, request: WriteRequest) -> Result<WriteResult, StorageNodeError> {
        self.0.batch_write(request)
    }
    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        sequence: Option<u64>,
    ) -> Result<WriteResult, StorageNodeError> {
        self.0.batch_write_conditional(request, sequence)
    }
    fn admin(&self) -> &dyn StorageAdmin {
        self.0.admin()
    }
}
struct Group {
    process: Process,
    _dir: tempfile::TempDir,
    route: PartitionRoute,
}
impl Group {
    async fn start(partition: u32, shard: u64) -> anyhow::Result<Self> {
        let raft = TcpListener::bind("127.0.0.1:0").await?;
        let control = TcpListener::bind("127.0.0.1:0").await?;
        let graph = TcpListener::bind("127.0.0.1:0").await?;
        let raft_addr = raft.local_addr()?;
        let control_addr = control.local_addr()?;
        let endpoint = graph.local_addr()?;
        let dir = tempfile::tempdir()?;
        drop((raft, control, graph));
        let identity = RemoteIdentity {
            group: shard,
            node: 1,
        };
        let process = Process(
            Command::new(env!("CARGO_BIN_EXE_graph-node"))
                .env("TOKIO_WORKER_THREADS", "2")
                .args([
                    "--allow-insecure-development",
                    "--group",
                    &shard.to_string(),
                    "--id",
                    "1",
                    "--raft",
                    &raft_addr.to_string(),
                    "--control",
                    &control_addr.to_string(),
                    "--graph",
                    &endpoint.to_string(),
                    "--shard",
                    &shard.to_string(),
                    "--epoch",
                    "1",
                    "--data-dir",
                    dir.path().to_str().unwrap(),
                    "--bootstrap",
                    &format!("1={raft_addr}"),
                ])
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        timeout(Duration::from_secs(30), async {
            loop {
                if remote::probe_identified(endpoint, identity, shard, Duration::from_millis(300))
                    .await
                    .is_ok()
                {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await?;
        Ok(Self {
            process,
            _dir: dir,
            route: PartitionRoute {
                partition,
                shard,
                epoch: 1,
                identity,
                endpoint,
            },
        })
    }
    async fn seed(&self, export: Export) -> anyhow::Result<()> {
        let mut batches = Vec::new();
        let mut ops = Vec::new();
        let mut bytes = 0;
        for e in export.entries {
            let size = e.key.len() + e.value.len();
            if !ops.is_empty() && (bytes + size > 70 * 1024 || ops.len() >= 128) {
                batches.push(std::mem::take(&mut ops));
                bytes = 0;
            }
            bytes += size;
            ops.push(d2_raft::WriteOp::Put {
                key: e.key,
                value: e.value,
            });
        }
        if !ops.is_empty() {
            batches.push(ops);
        }
        for (i, operations) in batches.into_iter().enumerate() {
            let mut request_id = [0; 16];
            request_id[..8].copy_from_slice(&(i as u64).to_be_bytes());
            request_id[8..].copy_from_slice(&self.route.shard.to_be_bytes());
            remote::write_identified(
                self.route.endpoint,
                self.route.identity,
                d2_raft::D1WriteRequest {
                    shard: self.route.shard,
                    epoch: 1,
                    request_id,
                    expected_sequence: None,
                    operations,
                },
                BUDGET,
            )
            .await
            .with_context(|| format!("seed shard {} batch {i}", self.route.shard))?;
        }
        Ok(())
    }
}
fn fixture() -> anyhow::Result<(Export, Export)> {
    let a_dir = tempfile::tempdir()?;
    let b_dir = tempfile::tempdir()?;
    let a = Shared(Arc::new(LocalStorageNode::open(a_dir.path(), 120)?));
    let b = Shared(Arc::new(LocalStorageNode::open(b_dir.path(), 130)?));
    let source = AppendOnlyStore::new(a.clone(), 120, 0);
    let target = AppendOnlyStore::new(b.clone(), 130, 1);
    for id in [1, 7, 9] {
        source.prepare_vertex(&Vertex::new(id))?.submit(&a)?;
    }
    for id in [1, 2, 3] {
        let mut v = Vertex::new(id);
        v.labels.push("person".into());
        v.properties
            .insert("active".into(), Value::Boolean(id != 2));
        if id != 2 {
            v.properties
                .insert("name".into(), Value::Text(format!("person-{id}")));
        }
        target.prepare_vertex(&v)?.submit(&b)?;
    }
    for (id, from, to) in [(20, 1, 1), (10, 1, 2), (30, 1, 3), (40, 1, 1)]
        .into_iter()
        .chain((1000..1105).map(|id| (id, 7, 1)))
    {
        let edge = CrossEdge {
            id,
            from: VertexRef {
                partition: 0,
                id: from,
            },
            to: VertexRef {
                partition: 1,
                id: to,
            },
            label: "knows".into(),
            properties: BTreeMap::new(),
        };
        source.prepare_source(&edge)?.submit(&a)?;
        target.prepare_reverse(&edge)?.submit(&b)?;
    }
    // Deliberately incomplete append; a query of this start must fail closed.
    let broken = CrossEdge {
        id: 900,
        from: VertexRef {
            partition: 0,
            id: 9,
        },
        to: VertexRef {
            partition: 1,
            id: 1,
        },
        label: "knows".into(),
        properties: BTreeMap::new(),
    };
    source.prepare_source(&broken)?.submit(&a)?;
    // Unrelated payload is never needed by the selected traversal.
    for id in 100..148 {
        let mut v = Vertex::new(id);
        v.properties
            .insert("payload".into(), Value::Text("x".repeat(64 * 1024)));
        target.prepare_vertex(&v)?.submit(&b)?;
    }
    Ok((a.admin().export()?, b.admin().export()?))
}

// Record each data-node operation on the wire. Reject full-snapshot downloads
// even if a future implementation accidentally adds a fallback.
#[derive(Default)]
struct FragmentGate {
    pause: AtomicBool,
    entered: Notify,
    resume: Notify,
}
struct AuditProxy {
    endpoint: SocketAddr,
    bytes: Arc<AtomicUsize>,
    requests: Arc<AtomicUsize>,
    forbidden: Arc<AtomicUsize>,
    opened: Arc<AtomicUsize>,
    gate: Arc<FragmentGate>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for AuditProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl AuditProxy {
    async fn start(target: SocketAddr) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = listener.local_addr()?;
        let bytes = Arc::new(AtomicUsize::new(0));
        let requests = Arc::new(AtomicUsize::new(0));
        let forbidden = Arc::new(AtomicUsize::new(0));
        let opened = Arc::new(AtomicUsize::new(0));
        let (b, r, f, o) = (
            bytes.clone(),
            requests.clone(),
            forbidden.clone(),
            opened.clone(),
        );
        let gate = Arc::new(FragmentGate::default());
        let worker_gate = gate.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut client, _)) = listener.accept().await else {
                    break;
                };
                let (b, r, f, o) = (b.clone(), r.clone(), f.clone(), o.clone());
                let gate = worker_gate.clone();
                tokio::spawn(async move {
                    let result = async {
                        let n = client.read_u32().await? as usize;
                        ensure!(n <= remote::MAX_FRAME_BYTES);
                        let mut data = vec![0; n];
                        client.read_exact(&mut data).await?;
                        let value: serde_json::Value = serde_json::from_slice(&data)?;
                        let operation = &value["operation"];
                        if operation.get("OpenQuery").is_some() {
                            o.fetch_add(1, Ordering::SeqCst);
                        }
                        if ["Snapshot", "SnapshotPage"]
                            .iter()
                            .any(|key| operation.get(key).is_some())
                        {
                            f.fetch_add(1, Ordering::SeqCst);
                            anyhow::bail!("full-shard download forbidden");
                        }
                        if operation.get("QueryFragment").is_some()
                            && gate.pause.swap(false, Ordering::SeqCst)
                        {
                            gate.entered.notify_one();
                            gate.resume.notified().await;
                        }
                        r.fetch_add(1, Ordering::SeqCst);
                        let mut server = TcpStream::connect(target).await?;
                        server.write_u32(n as u32).await?;
                        server.write_all(&data).await?;
                        let n = server.read_u32().await? as usize;
                        ensure!(n <= remote::MAX_FRAME_BYTES);
                        let mut data = vec![0; n];
                        server.read_exact(&mut data).await?;
                        b.fetch_add(n + 4, Ordering::SeqCst);
                        client.write_u32(n as u32).await?;
                        client.write_all(&data).await?;
                        Ok::<(), anyhow::Error>(())
                    }
                    .await;
                    if let Err(error) = result {
                        eprintln!("query audit proxy: {error:#}");
                    }
                });
            }
        });
        Ok(Self {
            endpoint,
            bytes,
            requests,
            forbidden,
            opened,
            gate,
            task,
        })
    }
}
// The old interpreter is an independent semantic oracle. Only this explicit
// oracle path downloads snapshots, over direct endpoints outside audit proxies.
fn legacy_oracle(
    metadata: PartitionRoute,
    routes: Vec<PartitionRoute>,
) -> anyhow::Result<Vec<(&'static str, Vec<QueryRow>)>> {
    let placement = |r: &PartitionRoute| Placement {
        shard: r.shard,
        group: r.identity.group,
        epoch: r.epoch,
        replicas: vec![r.identity.node],
    };
    let replica = |r: &PartitionRoute| RemoteReplica {
        group: r.identity.group,
        node: r.identity.node,
        endpoint: r.endpoint,
    };
    let metadata_placement = placement(&metadata);
    let catalog = Catalog::new_remote(
        metadata.endpoint,
        metadata.identity.node,
        metadata_placement.clone(),
        BUDGET,
    )?;
    for r in &routes {
        let mut request_id = [0; 16];
        request_id[..4].copy_from_slice(&r.partition.to_be_bytes());
        request_id[15] = 1;
        catalog.commit_registration(&catalog.prepare_registration(
            request_id,
            r.partition,
            placement(r),
        )?)?;
    }
    let data = routes.iter().map(replica).collect::<Vec<_>>();
    let resolver = RouteResolver::new_remote(metadata_placement, &[replica(&metadata)], &data)?;
    let session = resolver.experimental_read_session(BUDGET)?;
    let mut out = Vec::new();
    for text in [
        "g.V(1).out('knows').path()",
        "g.V(1,1).out('knows').path()",
        "g.V(1).out('knows').limit(2).has('active',true).count()",
        "g.V(1).out('knows').has('active',true).limit(2).count()",
        "g.V(1).out('knows').hasLabel('person').values('name')",
        "g.V(1).out('knows').values('name').limit(2).count()",
        "g.V(7).out('knows').count()",
        "g.V(7).out('knows')",
        "g.V(999).out('knows').count()",
        "g.V(1,9).limit(1).out('knows').count()",
    ] {
        let observed = session.observed_gremlin_v3(0, text)?;
        let rows = observed
            .rows
            .into_iter()
            .map(|row| match row {
                ObservedRowV3::Vertex(v) => QueryRow::Element(vertex(v.partition, v.id)),
                ObservedRowV3::Edge(e) => QueryRow::Element(QualifiedElement::Edge {
                    partition: e.partition,
                    id: e.id,
                }),
                ObservedRowV3::Count(n) => QueryRow::Count(n),
                ObservedRowV3::Path(path) => QueryRow::Path(
                    path.into_iter()
                        .map(|e| match e {
                            ObservedPathElement::Vertex(v) => vertex(v.partition, v.id),
                            ObservedPathElement::Edge(e) => QualifiedElement::Edge {
                                partition: e.partition,
                                id: e.id,
                            },
                        })
                        .collect(),
                ),
                ObservedRowV3::Value { source, value } => QueryRow::Value {
                    source: Some(match source {
                        ObservedElementRef::Vertex(v) => vertex(v.partition, v.id),
                        ObservedElementRef::Edge(e) => QualifiedElement::Edge {
                            partition: e.partition,
                            id: e.id,
                        },
                    }),
                    value,
                },
            })
            .collect();
        out.push((text, rows));
    }
    Ok(out)
}
fn vertex(partition: u32, id: u64) -> QualifiedElement {
    QualifiedElement::Vertex { partition, id }
}
fn path(id: u64, to: u64) -> QueryRow {
    QueryRow::Path(vec![
        vertex(0, 1),
        QualifiedElement::Edge { partition: 0, id },
        vertex(1, to),
    ])
}
async fn rows(endpoint: SocketAddr, text: &str) -> anyhow::Result<Vec<QueryRow>> {
    let output = distributed::query(
        endpoint,
        DistributedRequest {
            start_partition: 0,
            text: text.into(),
            timeout_millis: 5000,
        },
        BUDGET,
    )
    .await??;
    assert_eq!(output.completeness, QueryCompleteness::Unverified);
    assert_eq!(output.snapshots.len(), 2);
    for (stamp, shard) in output.snapshots.iter().zip([120, 130]) {
        assert_eq!(stamp.shard, shard);
        assert_eq!(stamp.epoch, 1);
        assert_eq!(
            stamp.identity,
            RemoteIdentity {
                group: shard,
                node: 1
            }
        );
        assert!(stamp.sequence > 0);
    }
    Ok(output.rows)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_query_crosses_real_shards_without_snapshot_downloads() -> anyhow::Result<()> {
    timeout(Duration::from_secs(180), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let a = Group::start(0, 120).await?;
    let mut b = Group::start(1, 130).await?;
    let (a_data, b_data) = run_blocking(fixture).await??;
    a.seed(a_data).await?;
    b.seed(b_data).await?;
    let metadata = Group::start(99, 110).await?;
    let metadata_route = metadata.route.clone();
    let oracle_routes = vec![a.route.clone(), b.route.clone()];
    let oracle = run_blocking(move || legacy_oracle(metadata_route, oracle_routes)).await??;
    let ap = AuditProxy::start(a.route.endpoint).await?;
    let bp = AuditProxy::start(b.route.endpoint).await?;
    let mut ar = a.route.clone();
    ar.endpoint = ap.endpoint;
    let mut br = b.route.clone();
    br.endpoint = bp.endpoint;
    let mut shared_route = ar.clone();
    shared_route.partition = 2;
    let service = CoordinatorService::start(
        "127.0.0.1:0".parse()?,
        StaticCatalog {
            partitions: vec![br, ar, shared_route],
        },
        BUDGET,
    )
    .await?;
    let endpoint = service.local_addr();
    let before = bp.bytes.load(Ordering::SeqCst);
    let opened_a = ap.opened.load(Ordering::SeqCst);
    let opened_b = bp.opened.load(Ordering::SeqCst);
    let expected = vec![path(10, 2), path(20, 1), path(30, 3), path(40, 1)];
    assert_eq!(
        rows(endpoint, "g.V(1).out('knows').path()").await?,
        expected
    );
    assert!(
        bp.bytes.load(Ordering::SeqCst) - before < 256 * 1024,
        "unrelated 3 MiB payload crossed wire"
    );
    assert_eq!(
        ap.opened.load(Ordering::SeqCst) - opened_a,
        1,
        "same-shard partitions did not reuse one snapshot"
    );
    assert_eq!(bp.opened.load(Ordering::SeqCst) - opened_b, 1);
    for (text, expected) in oracle {
        assert_eq!(
            rows(endpoint, text).await?,
            expected,
            "legacy interpreter differential: {text}"
        );
    }
    let mut twice = expected.clone();
    twice.extend(expected);
    assert_eq!(rows(endpoint, "g.V(1,1).out('knows').path()").await?, twice);
    assert_eq!(
        rows(
            endpoint,
            "g.V(1).out('knows').limit(2).has('active',true).count()"
        )
        .await?,
        vec![QueryRow::Count(1)]
    );
    assert_eq!(
        rows(
            endpoint,
            "g.V(1).out('knows').has('active',true).limit(2).count()"
        )
        .await?,
        vec![QueryRow::Count(2)]
    );
    assert_eq!(
        rows(
            endpoint,
            "g.V(1).out('knows').hasLabel('person').values('name')"
        )
        .await?,
        vec![
            QueryRow::Value {
                source: Some(vertex(1, 1)),
                value: Value::Text("person-1".into())
            },
            QueryRow::Value {
                source: Some(vertex(1, 3)),
                value: Value::Text("person-3".into())
            },
            QueryRow::Value {
                source: Some(vertex(1, 1)),
                value: Value::Text("person-1".into())
            }
        ]
    );
    assert_eq!(
        rows(
            endpoint,
            "g.V(1).out('knows').values('name').limit(2).count()"
        )
        .await?,
        vec![QueryRow::Count(2)]
    );
    assert_eq!(
        rows(endpoint, "g.V(7).out('knows').count()").await?,
        vec![QueryRow::Count(105)]
    );
    assert_eq!(rows(endpoint, "g.V(7).out('knows')").await?.len(), 100);
    assert_eq!(
        rows(endpoint, "g.V(999).out('knows').count()").await?,
        vec![QueryRow::Count(0)]
    );
    for text in [
        "g.V().count()",
        "g.V(1).out().out().count()",
        "g.E(1).count()",
        "g.V(999).values('name').out()",
    ] {
        let result = distributed::query(
            endpoint,
            DistributedRequest {
                start_partition: 0,
                text: text.into(),
                timeout_millis: 5000,
            },
            BUDGET,
        )
        .await?;
        assert!(
            matches!(
                result,
                Err(QueryFailure::Unsupported) | Err(QueryFailure::UnsupportedScope)
            ),
            "{text}: {result:?}"
        );
    }
    // Both snapshots have been acquired when the first fragment is dispatched.
    // Change target data while that fragment is paused; the running query must
    // read its original target session, and a subsequent query must see the write.
    ap.gate.pause.store(true, Ordering::SeqCst);
    let pinned = tokio::spawn(rows(endpoint, "g.V(1).out('knows').values('name')"));
    timeout(Duration::from_secs(3), ap.gate.entered.notified()).await?;
    let changed = run_blocking(|| -> anyhow::Result<Export> {
        let dir = tempfile::tempdir()?;
        let node = Shared(Arc::new(LocalStorageNode::open(dir.path(), 130)?));
        let store = AppendOnlyStore::new(node.clone(), 130, 1);
        let mut v = Vertex::new(1);
        v.labels.push("person".into());
        v.properties
            .insert("name".into(), Value::Text("changed".into()));
        v.properties.insert("active".into(), Value::Boolean(true));
        store.prepare_vertex(&v)?.submit(&node)?;
        Ok(node.admin().export()?)
    })
    .await??;
    remote::write_identified(
        b.route.endpoint,
        b.route.identity,
        d2_raft::D1WriteRequest {
            shard: 130,
            epoch: 1,
            request_id: *b"query-update-001",
            expected_sequence: None,
            operations: changed
                .entries
                .into_iter()
                .map(|e| d2_raft::WriteOp::Put {
                    key: e.key,
                    value: e.value,
                })
                .collect(),
        },
        BUDGET,
    )
    .await?;
    ap.gate.resume.notify_one();
    let old = pinned.await??;
    assert_eq!(
        old[0],
        QueryRow::Value {
            source: Some(vertex(1, 1)),
            value: Value::Text("person-1".into())
        }
    );
    let fresh = rows(endpoint, "g.V(1).out('knows').values('name')").await?;
    assert_eq!(
        fresh[0],
        QueryRow::Value {
            source: Some(vertex(1, 1)),
            value: Value::Text("changed".into())
        }
    );
    let incomplete = distributed::query(
        endpoint,
        DistributedRequest {
            start_partition: 0,
            text: "g.V(9).out('knows').count()".into(),
            timeout_millis: 5000,
        },
        BUDGET,
    )
    .await?;
    assert!(matches!(incomplete, Err(QueryFailure::IncompleteOrCorrupt)));
    let filtered_incomplete = distributed::query(
        endpoint,
        DistributedRequest {
            start_partition: 0,
            text: "g.V(9).out('different-label').limit(1).count()".into(),
            timeout_millis: 5000,
        },
        BUDGET,
    )
    .await?;
    assert!(
        matches!(filtered_incomplete, Err(QueryFailure::IncompleteOrCorrupt)),
        "label filter hid corrupt reverse state"
    );
    assert_eq!(
        rows(endpoint, "g.V(1,9).limit(1).out('knows').count()").await?,
        vec![QueryRow::Count(4)]
    );

    // Every shard spends the same cumulative allowance. A later limit/count
    // must not turn an excessive earlier expansion into a successful prefix.
    let repeated = std::iter::repeat_n("7", 30).collect::<Vec<_>>().join(",");
    for suffix in ["count()", "limit(1).count()"] {
        let exhausted = distributed::query(
            endpoint,
            DistributedRequest {
                start_partition: 0,
                text: format!("g.V({repeated}).out('knows').{suffix}"),
                timeout_millis: 5000,
            },
            BUDGET,
        )
        .await?;
        assert!(
            matches!(exhausted, Err(QueryFailure::ResourceExhausted)),
            "{exhausted:?}"
        );
    }
    // Repeated completed queries close their sessions rather than consuming the
    // process-global eight-session admission until TTL.
    for _ in 0..12 {
        assert_eq!(
            rows(endpoint, "g.V(1).count()").await?,
            vec![QueryRow::Count(1)]
        );
    }
    let cli = run_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_graph-coordinator"))
            .env("TOKIO_WORKER_THREADS", "2")
            .args([
                "query",
                "--allow-insecure-development",
                "--endpoint",
                &endpoint.to_string(),
                "--partition",
                "0",
                "--query",
                "g.V(1).out('knows').count()",
            ])
            .output()
    })
    .await??;
    ensure!(
        cli.status.success(),
        "coordinator CLI: {}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli_output: distributed::DistributedOutput = serde_json::from_slice(&cli.stdout)?;
    assert_eq!(cli_output.rows, vec![QueryRow::Count(4)]);

    assert_eq!(
        ap.forbidden.load(Ordering::SeqCst) + bp.forbidden.load(Ordering::SeqCst),
        0
    );
    assert!(ap.requests.load(Ordering::SeqCst) > 0 && bp.requests.load(Ordering::SeqCst) > 0);
    b.process.0.kill()?;
    b.process.0.wait()?;
    let unavailable = distributed::query(
        endpoint,
        DistributedRequest {
            start_partition: 0,
            text: "g.V(1).out('knows').count()".into(),
            timeout_millis: 5000,
        },
        BUDGET,
    )
    .await;
    assert!(
        !matches!(unavailable, Ok(Ok(_))),
        "missing shard returned partial success"
    );
    service.shutdown().await;
    Ok(())
}
