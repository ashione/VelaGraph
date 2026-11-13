//! Real independent shard processes; TCP versus typed gRPC fragment transport.
//! Observed semantics only: this does not claim managed/fixed or Raft migration.
use anyhow::Context;
use replicated_graph::{
    remote::{
        self,
        distributed::{
            self, CoordinatorService, DistributedRequest, PartitionRoute, StaticCatalog,
        },
        query::{QualifiedElement, QueryCompleteness, QueryFailure, QueryRow},
        RemoteIdentity,
    },
    run_blocking,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::{watch, Notify},
    task::{JoinHandle, JoinSet},
};
use tokio::{
    net::TcpListener,
    time::{sleep, timeout},
};
use velagraph::{
    experimental_graph::{AppendOnlyStore, CrossEdge, VertexRef},
    Export, KeyRange, KeyValue, LocalStorageNode, ReadView, ScanLimit, StorageAdmin, StorageNode,
    StorageNodeError, Value, Vertex, WriteRequest, WriteResult,
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
        cursor: Option<&velagraph::ScanCursor>,
    ) -> Result<velagraph::ScanPage, StorageNodeError> {
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
    _process: Process,
    _dir: tempfile::TempDir,
    route: PartitionRoute,
    grpc: SocketAddr,
}
impl Group {
    async fn start(partition: u32, shard: u64) -> anyhow::Result<Self> {
        let raft = TcpListener::bind("127.0.0.1:0").await?;
        let control = TcpListener::bind("127.0.0.1:0").await?;
        let graph = TcpListener::bind("127.0.0.1:0").await?;
        let raft_addr = raft.local_addr()?;
        let control_addr = control.local_addr()?;
        let endpoint = graph.local_addr()?;
        let grpc_listener = TcpListener::bind("127.0.0.1:0").await?;
        let grpc = grpc_listener.local_addr()?;
        let dir = tempfile::tempdir()?;
        drop((raft, control, graph, grpc_listener));
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
                    "--grpc-snapshot",
                    &grpc.to_string(),
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
            _process: process,
            grpc,
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
    let da = tempfile::tempdir()?;
    let db = tempfile::tempdir()?;
    let a = Shared(Arc::new(LocalStorageNode::open(da.path(), 120)?));
    let b = Shared(Arc::new(LocalStorageNode::open(db.path(), 130)?));
    let sa = AppendOnlyStore::new(a.clone(), 120, 0);
    let sb = AppendOnlyStore::new(b.clone(), 130, 1);
    // More than one scan page, non-sorted insertion order and overlapping IDs.
    for (node, store) in [(&a, &sa), (&b, &sb)] {
        for id in (1..=140).rev() {
            let mut v = Vertex::new(id);
            v.labels.push("person".into());
            v.properties = BTreeMap::from([
                ("nullable".into(), Value::Null),
                ("name".into(), Value::Text(format!("姓名-{id}\n\""))),
                ("signed".into(), Value::Integer(i64::MIN)),
                ("real".into(), Value::Real(-0.0)),
                ("enabled".into(), Value::Boolean(id != 2)),
            ]);
            store.prepare_vertex(&v)?.submit(node)?;
        }
    }
    for (id, from, to) in [
        (30, (0, 1), (1, 3)),
        (10, (0, 1), (1, 2)),
        (20, (0, 1), (1, 3)),
        (40, (1, 3), (0, 1)),
    ] {
        let edge = CrossEdge {
            id,
            from: VertexRef {
                partition: from.0,
                id: from.1,
            },
            to: VertexRef {
                partition: to.0,
                id: to.1,
            },
            label: "knows".into(),
            properties: BTreeMap::new(),
        };
        if from.0 == 0 {
            sa.prepare_source(&edge)?.submit(&a)?;
        } else {
            sb.prepare_source(&edge)?.submit(&b)?;
        }
        if to.0 == 0 {
            sa.prepare_reverse(&edge)?.submit(&a)?;
        } else {
            sb.prepare_reverse(&edge)?.submit(&b)?;
        }
    }
    // Incomplete source-only adjacency must fail instead of partial success.
    let incomplete = CrossEdge {
        id: 90,
        from: VertexRef {
            partition: 0,
            id: 139,
        },
        to: VertexRef {
            partition: 1,
            id: 140,
        },
        label: "missing".into(),
        properties: BTreeMap::new(),
    };
    sa.prepare_source(&incomplete)?.submit(&a)?;
    Ok((a.admin().export()?, b.admin().export()?))
}
fn request(text: &str) -> DistributedRequest {
    DistributedRequest {
        start_partition: 0,
        text: text.into(),
        timeout_millis: 5000,
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_fragments_match_tcp_across_real_shards() -> anyhow::Result<()> {
    timeout(Duration::from_secs(120), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let mut a = Group::start(0, 120).await?;
    let b = Group::start(1, 130).await?;
    let (ad, bd) = run_blocking(fixture).await??;
    a.seed(ad).await?;
    b.seed(bd).await?;
    let tcp = CoordinatorService::start(
        "127.0.0.1:0".parse()?,
        StaticCatalog {
            partitions: vec![b.route.clone(), a.route.clone()],
        },
        BUDGET,
    )
    .await?;
    let mut ar = a.route.clone();
    ar.endpoint = a.grpc;
    let mut br = b.route.clone();
    br.endpoint = b.grpc;
    let grpc = CoordinatorService::start_grpc(
        "127.0.0.1:0".parse()?,
        StaticCatalog {
            partitions: vec![br.clone(), ar.clone()],
        },
        BUDGET,
    )
    .await?;
    for text in [
        "g.V(3,1,3).values('name')",
        "g.V(1).out('knows')",
        "g.V(1).out('knows').in('knows').path()",
        "g.V(1).both('knows').path()",
        "g.V(1).out('knows').has('enabled',true).hasLabel('person').values('name')",
        "g.V(1).out('knows').has('nullable').valueMap('nullable','name','signed','real','enabled')",
        "g.V(1).values('nullable')",
        "g.V(1).values('signed')",
        "g.V(1).values('real')",
        "g.V(1).values('enabled')",
        "g.V(1).has('signed',-9223372036854775808).count()",
        "g.V(1).has('real',-0.0).count()",
        "g.V(999999).count()",
        "g.V().count()",
        "g.V().hasLabel('person').has('enabled',true).limit(200).count()",
        "g.V().has('enabled').limit(2).values('name')",
        "g.V(1).repeat(out('knows')).times(2).path()",
    ] {
        let expected = distributed::query(tcp.local_addr(), request(text), BUDGET).await??;
        let actual = distributed::query(grpc.local_addr(), request(text), BUDGET).await??;
        assert_eq!(actual, expected, "ordered TCP/gRPC differential: {text}");
        assert_eq!(actual.completeness, QueryCompleteness::Unverified);
    }
    let rows = distributed::query(grpc.local_addr(), request("g.V(1).out('knows')"), BUDGET)
        .await??
        .rows;
    assert_eq!(
        rows,
        [2, 3, 3].map(|id| QueryRow::Element(QualifiedElement::Vertex { partition: 1, id })),
        "bag multiplicity and edge-ID order are preserved"
    );
    for text in ["g.V(", "g.V(139).out('missing')", "g.V(1).unsupported()"] {
        let expected = distributed::query(tcp.local_addr(), request(text), BUDGET).await?;
        let actual = distributed::query(grpc.local_addr(), request(text), BUDGET).await?;
        assert!(
            expected.is_err(),
            "invalid query unexpectedly succeeded: {text}"
        );
        assert_eq!(actual, expected, "typed failure differential: {text}");
    }
    assert_eq!(
        distributed::query(grpc.local_addr(), request("g.V("), BUDGET).await?,
        Err(QueryFailure::Parse)
    );
    // Explicit consistency negotiation remains unsupported for this observed path.
    for required in [
        distributed::consistency::ReadConsistency::IndependentShardSnapshots,
        distributed::consistency::ReadConsistency::GlobalCommittedSnapshot,
    ] {
        assert_eq!(
            distributed::consistency::query_with_consistency(
                grpc.local_addr(),
                request("g.V(1).count()"),
                required,
                BUDGET
            )
            .await?,
            Err(QueryFailure::Unsupported)
        );
    }
    for text in [
        "g.V(1).repeat(__.out('knows')).times(1).count()",
        "g.V(1,1).repeat(__.out('knows')).emit().times(2).count()",
        "g.V(1).repeat(__.out('knows')).emit().times(2).dedup().count()",
    ] {
        assert_eq!(
            hierarchy_count(tcp.local_addr(), false, text).await?,
            hierarchy_count(grpc.local_addr(), false, text).await?
        );
    }
    assert_eq!(
        hierarchy_rows(tcp.local_addr(), false).await?,
        hierarchy_rows(grpc.local_addr(), false).await?
    );
    hierarchy_cancel(tcp.local_addr(), false).await?;
    hierarchy_cancel(grpc.local_addr(), false).await?;
    session_checks(&a).await?;
    // Teardown releases local job admission immediately; remote read leases
    // may remain until their bounded TTL, so run exact shard-slot checks first.
    coordinator_ingress_checks(&[br, ar], tcp.local_addr())
        .await
        .context("coordinator ingress checks")?;
    a._process.0.kill()?;
    a._process.0.wait()?;
    for endpoint in [tcp.local_addr(), grpc.local_addr()] {
        assert!(matches!(
            distributed::jobs::request(
                endpoint,
                distributed::jobs::Request::Start {
                    start_partition: 0,
                    text: "g.V(1).repeat(__.out('knows')).times(1).count()".into()
                },
                BUDGET
            )
            .await?,
            Err(QueryFailure::Unavailable)
        ));
    }
    grpc.shutdown().await;
    tcp.shutdown().await;
    Ok(())
}

// Legacy JSON is used only as the comparison endpoint, never carried inside
// protobuf or as fallback for the selected gRPC transport.
async fn legacy(group: &Group, operation: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(group.route.endpoint).await?;
    let bytes = serde_json::to_vec(
        &serde_json::json!({"version": 1, "identity": group.route.identity, "operation": operation}),
    )?;
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await?;
    let len = stream.read_u32().await? as usize;
    anyhow::ensure!(len <= remote::MAX_FRAME_BYTES);
    let mut bytes = vec![0; len];
    stream.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}
async fn session_checks(group: &Group) -> anyhow::Result<()> {
    use distributed::grpc::wire as w;
    let proxy = Proxy::start(group.grpc).await?;
    let mut client =
        w::query_service_client::QueryServiceClient::connect(format!("http://{}", proxy.address))
            .await?;
    let context = || {
        Some(w::Context {
            version: 1,
            identity: Some(w::Identity {
                group: group.route.identity.group,
                node: 1,
            }),
            timeout_millis: 5000,
        })
    };
    let response = legacy(
        group,
        serde_json::json!({"OpenHierarchyQuery": {"shard": 120}}),
    )
    .await?;
    let native = &response["result"]["Ok"]["OpenHierarchyQuery"]["descriptor"];
    let stamp = &native["stamp"];
    let descriptor = w::Descriptor {
        stamp: Some(w::QueryStamp {
            identity: Some(w::Identity {
                group: 120,
                node: 1,
            }),
            shard: 120,
            epoch: stamp["epoch"].as_u64().context("epoch")?,
            sequence: stamp["sequence"].as_u64().context("sequence")?,
        }),
        incarnation: serde_json::from_value(native["incarnation"].clone())?,
        token: serde_json::from_value(native["token"].clone())?,
    };
    let exists = w::Fragment {
        kind: Some(w::fragment::Kind::Exists(w::VertexId {
            partition: 0,
            id: 1,
        })),
    };
    let request = w::FragmentRequest {
        context: context(),
        descriptor: Some(descriptor.clone()),
        fragment: Some(w::Fragment {
            kind: Some(w::fragment::Kind::HierarchyBatch(w::HierarchyBatch {
                partition: 0,
                fragments: vec![exists.clone(), exists.clone()],
            })),
        }),
        remaining: Some(w::Cost {
            bytes: 1024 * 1024,
            work: 1000,
        }),
    };
    let result = client.fragment(request.clone()).await?.into_inner();
    let Some(w::fragment_response::Result::Output(output)) = result.result else {
        anyhow::bail!("batch failed");
    };
    assert_eq!(output.descriptor, Some(descriptor.clone()));
    let Some(w::fragment_value::Kind::HierarchyBatch(batch)) = output.value.and_then(|v| v.kind)
    else {
        anyhow::bail!("not typed batch");
    };
    assert_eq!(batch.values.len(), 2);
    assert!(batch
        .values
        .iter()
        .all(|v| matches!(v.kind, Some(w::fragment_value::Kind::Boolean(true)))));
    // Stop only the response direction after a fragment has reached the
    // server; aborting the caller must not invalidate the shared session.
    // This is a cancellation/replay check, not a server rollback claim.
    proxy.paused.send(true)?;
    let mut pending_client = client.clone();
    let pending_request = request.clone();
    let pending = tokio::spawn(async move { pending_client.fragment(pending_request).await });
    timeout(BUDGET, proxy.blocked.notified())
        .await
        .context("fragment reached response gate")?;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    proxy.paused.send(false)?;
    assert!(matches!(
        client.fragment(request.clone()).await?.into_inner().result,
        Some(w::fragment_response::Result::Output(_))
    ));

    // A caller deadline must bound a pending HTTP/2 response; unlike an
    // artificial benchmark delay, the gate proves what boundary is blocked.
    proxy.paused.send(true)?;
    let mut deadline_client = client.clone();
    let mut deadline_request = tonic::Request::new(request.clone());
    deadline_request.set_timeout(Duration::from_millis(200));
    let pending = tokio::spawn(async move { deadline_client.fragment(deadline_request).await });
    timeout(BUDGET, proxy.blocked.notified())
        .await
        .context("deadline response gate")?;
    let error = timeout(Duration::from_secs(2), pending)
        .await??
        .unwrap_err();
    assert!(
        matches!(
            error.code(),
            tonic::Code::DeadlineExceeded | tonic::Code::Cancelled
        ),
        "unexpected deadline status: {error}"
    );
    proxy.paused.send(false)?;
    assert!(matches!(
        client.fragment(request.clone()).await?.into_inner().result,
        Some(w::fragment_response::Result::Output(_))
    ));
    client
        .close(w::CloseRequest {
            context: context(),
            descriptor: Some(descriptor),
        })
        .await?;
    assert_eq!(
        client.fragment(request).await.unwrap_err().code(),
        tonic::Code::NotFound
    );

    // Reverse direction: a gRPC-opened token is closed through the existing TCP
    // service, then no longer exists on gRPC. An independent pool cannot pass.
    let opened = client
        .open(w::OpenRequest {
            context: context(),
            shard: 120,
        })
        .await?
        .into_inner();
    let stamp = opened.stamp.as_ref().context("stamp")?;
    let native = serde_json::json!({"stamp": {"identity": group.route.identity, "shard": stamp.shard, "epoch": stamp.epoch, "sequence": stamp.sequence}, "incarnation": opened.incarnation, "token": opened.token});
    let closed = legacy(
        group,
        serde_json::json!({"CloseQuery": {"descriptor": native}}),
    )
    .await?;
    assert_eq!(closed["result"]["Ok"], "CloseQuery");
    let request = w::FragmentRequest {
        context: context(),
        descriptor: Some(opened),
        fragment: Some(exists),
        remaining: Some(w::Cost {
            bytes: 1024,
            work: 100,
        }),
    };
    assert_eq!(
        client.fragment(request).await.unwrap_err().code(),
        tonic::Code::NotFound
    );
    for (version, millis, expected) in [
        (99, 5000, tonic::Code::DataLoss),
        (1, 0, tonic::Code::InvalidArgument),
        (1, 5001, tonic::Code::InvalidArgument),
    ] {
        let mut c = context().unwrap();
        c.version = version;
        c.timeout_millis = millis;
        assert_eq!(
            client
                .open(w::OpenRequest {
                    context: Some(c),
                    shard: 120
                })
                .await
                .unwrap_err()
                .code(),
            expected
        );
        assert_eq!(
            client
                .open_hierarchy(w::OpenRequest {
                    context: Some(c),
                    shard: 120
                })
                .await
                .unwrap_err()
                .code(),
            expected
        );
    }
    assert_eq!(
        client
            .open(w::OpenRequest {
                context: context(),
                shard: 999
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    // Build canonical v2 bytes before opening the time-bounded view. The
    // mutation itself is a real Raft write against the same running shard.
    let added = run_blocking(|| -> anyhow::Result<Export> {
        let dir = tempfile::tempdir()?;
        let node = LocalStorageNode::open(dir.path(), 120)?;
        let node = Shared(Arc::new(node));
        let graph = AppendOnlyStore::new(node.clone(), 120, 0);
        graph.prepare_vertex(&Vertex::new(9999))?.submit(&node)?;
        Ok(node.admin().export()?)
    })
    .await??;
    let old = client
        .open(w::OpenRequest {
            context: context(),
            shard: 120,
        })
        .await?
        .into_inner();
    let lookup = |descriptor: w::Descriptor| w::FragmentRequest {
        context: context(),
        descriptor: Some(descriptor),
        fragment: Some(w::Fragment {
            kind: Some(w::fragment::Kind::Exists(w::VertexId {
                partition: 0,
                id: 9999,
            })),
        }),
        remaining: Some(w::Cost {
            bytes: 1024 * 1024,
            work: 1000,
        }),
    };
    let before = client.fragment(lookup(old.clone())).await?.into_inner();
    remote::write_identified(
        group.route.endpoint,
        group.route.identity,
        d2_raft::D1WriteRequest {
            shard: 120,
            epoch: 1,
            request_id: *b"grpc-query-added",
            expected_sequence: None,
            operations: added
                .entries
                .into_iter()
                .map(|entry| d2_raft::WriteOp::Put {
                    key: entry.key,
                    value: entry.value,
                })
                .collect(),
        },
        BUDGET,
    )
    .await?;
    let after = client.fragment(lookup(old.clone())).await?.into_inner();
    assert_eq!(
        after, before,
        "existing query token must retain its fixed local read view after committed write"
    );
    let fresh = client
        .open(w::OpenRequest {
            context: context(),
            shard: 120,
        })
        .await?
        .into_inner();
    assert!(
        fresh.stamp.as_ref().context("fresh stamp")?.sequence
            > old.stamp.as_ref().context("old stamp")?.sequence
    );
    let newest = client.fragment(lookup(fresh.clone())).await?.into_inner();
    for (response, expected) in [(before, false), (newest, true)] {
        let Some(w::fragment_response::Result::Output(output)) = response.result else {
            anyhow::bail!("exists fragment failed");
        };
        assert_eq!(
            output.value.and_then(|value| value.kind),
            Some(w::fragment_value::Kind::Boolean(expected))
        );
    }
    for descriptor in [old, fresh] {
        client
            .close(w::CloseRequest {
                context: context(),
                descriptor: Some(descriptor),
            })
            .await?;
    }
    let mut held = Vec::new();
    for _ in 0..7 {
        held.push(
            client
                .open(w::OpenRequest {
                    context: context(),
                    shard: 120,
                })
                .await?
                .into_inner(),
        );
    }
    let last = legacy(
        group,
        serde_json::json!({"OpenQuery": {"shard": 120, "timeout_millis": 5000}}),
    )
    .await?;
    assert!(last["result"]["Ok"]["OpenQuery"].is_object());
    assert_eq!(
        client
            .open(w::OpenRequest {
                context: context(),
                shard: 120
            })
            .await
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    let exhausted = legacy(
        group,
        serde_json::json!({"OpenQuery": {"shard": 120, "timeout_millis": 5000}}),
    )
    .await?;
    assert_eq!(
        exhausted["result"]["Err"], "ResourceExhausted",
        "one process-wide pool across transports"
    );
    for descriptor in held {
        client
            .close(w::CloseRequest {
                context: context(),
                descriptor: Some(descriptor),
            })
            .await?;
    }
    let closed = legacy(
        group,
        serde_json::json!({"CloseQuery": {"descriptor": last["result"]["Ok"]["OpenQuery"]}}),
    )
    .await?;
    assert_eq!(closed["result"]["Ok"], "CloseQuery");
    assert_eq!(
        proxy.connections.load(Ordering::SeqCst),
        1,
        "one physical HTTP/2 connection serves all typed fragment/session RPCs"
    );
    drop(proxy);
    Ok(())
}

struct Proxy {
    address: SocketAddr,
    connections: Arc<AtomicUsize>,
    paused: watch::Sender<bool>,
    blocked: Arc<Notify>,
    task: JoinHandle<()>,
}
impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Proxy {
    async fn start(upstream: SocketAddr) -> anyhow::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let connections = Arc::new(AtomicUsize::new(0));
        let count = connections.clone();
        let (paused, pause) = watch::channel(false);
        let blocked = Arc::new(Notify::new());
        let signal = blocked.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((client, _)) = accepted else { break };
                        count.fetch_add(1, Ordering::SeqCst);
                        let mut pause = pause.clone();
                        let signal = signal.clone();
                        tasks.spawn(async move {
                            let server = TcpStream::connect(upstream).await?;
                            let (mut cr, mut cw) = client.into_split();
                            let (mut sr, mut sw) = server.into_split();
                            let requests = tokio::io::copy(&mut cr, &mut sw);
                            let responses = async {
                                let mut buf = vec![0; 16*1024];
                                loop {
                                    let n = sr.read(&mut buf).await?;
                                    if n == 0 { break; }
                                    while *pause.borrow_and_update() {
                                        signal.notify_one();
                                        if pause.changed().await.is_err() { return Ok::<(), std::io::Error>(()); }
                                    }
                                    cw.write_all(&buf[..n]).await?;
                                }
                                Ok::<(), std::io::Error>(())
                            };
                            tokio::select! { result = requests => { result?; }, result = responses => { result?; } }
                            Ok::<(), std::io::Error>(())
                        });
                    },
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Ok(Self {
            address,
            connections,
            paused,
            blocked,
            task,
        })
    }
}

async fn hierarchy_count(endpoint: SocketAddr, grpc: bool, text: &str) -> anyhow::Result<u64> {
    use distributed::jobs::{Output, Request};
    let client = JobClient::connect(endpoint, grpc).await?;
    let mut output = client
        .request(
            Request::Start {
                start_partition: 0,
                text: text.into(),
            },
            BUDGET,
        )
        .await??;
    for _ in 0..100 {
        let Output::Progress {
            token,
            next_sequence,
            complete,
            count,
            completeness,
            snapshots,
        } = output
        else {
            anyhow::bail!("not progress");
        };
        assert_eq!(completeness, QueryCompleteness::Unverified);
        if complete {
            assert!(matches!(
                client.request(Request::Cancel { token }, BUDGET).await??,
                Output::Cancelled
            ));
            return count.context("missing count");
        }
        assert!(count.is_none());
        output = client
            .request(
                Request::Advance {
                    token,
                    sequence: next_sequence,
                },
                BUDGET,
            )
            .await??;
        let replay = client
            .request(
                Request::Advance {
                    token,
                    sequence: next_sequence,
                },
                BUDGET,
            )
            .await??;
        assert_eq!(
            serde_json::to_value(&output)?,
            serde_json::to_value(replay)?
        );
        let Output::Progress {
            snapshots: actual, ..
        } = &output
        else {
            anyhow::bail!("not progress");
        };
        assert_eq!(*actual, snapshots);
    }
    anyhow::bail!("job did not terminate")
}

async fn hierarchy_rows(endpoint: SocketAddr, grpc: bool) -> anyhow::Result<Vec<QualifiedElement>> {
    use distributed::jobs::{Output, Request};
    let client = JobClient::connect(endpoint, grpc).await?;
    let mut output = client
        .request(
            Request::StartRows {
                start_partition: 0,
                text: "g.V(1).repeat(__.out('knows')).emit().times(2)".into(),
                page_size: 1,
            },
            BUDGET,
        )
        .await??;
    let Output::Preparing { token, .. } = output else {
        anyhow::bail!("not preparing");
    };
    assert!(matches!(
        client
            .request(Request::Read { token, cursor: 0 }, BUDGET)
            .await?,
        Err(QueryFailure::InvalidArgument)
    ));
    for _ in 0..100 {
        let Output::Preparing {
            ready,
            next_sequence,
            ..
        } = output
        else {
            anyhow::bail!("not preparing");
        };
        if ready {
            break;
        }
        output = client
            .request(
                Request::Advance {
                    token,
                    sequence: next_sequence,
                },
                BUDGET,
            )
            .await??;
    }
    assert!(matches!(output, Output::Preparing { ready: true, .. }));
    let mut cursor = 0;
    let mut rows = Vec::new();
    loop {
        let page = client
            .request(Request::Read { token, cursor }, BUDGET)
            .await??;
        let replay = client
            .request(Request::Read { token, cursor }, BUDGET)
            .await??;
        assert_eq!(serde_json::to_value(&page)?, serde_json::to_value(replay)?);
        let Output::Page {
            rows: next_rows,
            next_cursor,
            done,
            completeness,
            ..
        } = page
        else {
            anyhow::bail!("not page");
        };
        assert_eq!(completeness, QueryCompleteness::Unverified);
        assert!(next_rows.len() <= 1);
        rows.extend(next_rows);
        if done {
            break;
        }
        cursor = next_cursor.context("missing cursor")?;
    }
    assert!(matches!(
        client.request(Request::Cancel { token }, BUDGET).await??,
        Output::Cancelled
    ));
    assert!(matches!(
        client
            .request(Request::Read { token, cursor: 0 }, BUDGET)
            .await?,
        Err(QueryFailure::Unavailable)
    ));
    Ok(rows)
}

async fn hierarchy_cancel(endpoint: SocketAddr, grpc: bool) -> anyhow::Result<()> {
    use distributed::jobs::{Output, Request};
    let client = JobClient::connect(endpoint, grpc).await?;
    let output = client
        .request(
            Request::Start {
                start_partition: 0,
                text: "g.V(1).repeat(__.out('knows')).times(1).count()".into(),
            },
            BUDGET,
        )
        .await??;
    let Output::Progress {
        token,
        next_sequence,
        complete: false,
        count: None,
        ..
    } = output
    else {
        anyhow::bail!("expected unadvanced job");
    };
    assert!(matches!(
        client.request(Request::Cancel { token }, BUDGET).await??,
        Output::Cancelled
    ));
    assert!(matches!(
        client
            .request(
                Request::Advance {
                    token,
                    sequence: next_sequence
                },
                BUDGET
            )
            .await?,
        Err(QueryFailure::Unavailable)
    ));
    Ok(())
}

enum JobClient {
    Legacy(SocketAddr),
    Grpc(distributed::grpc::CoordinatorClient),
}
impl JobClient {
    async fn connect(endpoint: SocketAddr, grpc: bool) -> anyhow::Result<Self> {
        Ok(if grpc {
            Self::Grpc(distributed::grpc::CoordinatorClient::connect(endpoint, BUDGET).await?)
        } else {
            Self::Legacy(endpoint)
        })
    }
    async fn request(
        &self,
        request: distributed::jobs::Request,
        deadline: Duration,
    ) -> Result<distributed::jobs::Response, remote::RemoteError> {
        match self {
            Self::Grpc(client) => client.job(request, deadline).await,
            Self::Legacy(endpoint) => {
                distributed::jobs::request(*endpoint, request, deadline).await
            }
        }
    }
}

async fn coordinator_ingress_checks(
    routes: &[PartitionRoute],
    legacy: SocketAddr,
) -> anyhow::Result<()> {
    use distributed::grpc::{wire as w, CoordinatorClient, GrpcCoordinatorService};
    let directory = tempfile::tempdir()?;
    let catalog = StaticCatalog {
        partitions: routes.to_vec(),
    };
    let catalog_path = directory.path().join("catalog.json");
    std::fs::write(&catalog_path, serde_json::to_vec(&catalog)?)?;
    let reserve = TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = reserve.local_addr()?;
    drop(reserve);
    let mut process = Process(
        Command::new(env!("CARGO_BIN_EXE_graph-coordinator"))
            .env("TOKIO_WORKER_THREADS", "2")
            .args([
                "serve-grpc",
                "--endpoint",
                &endpoint.to_string(),
                "--catalog",
                catalog_path.to_str().unwrap(),
                "--allow-insecure-development",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    let client = timeout(BUDGET, async {
        loop {
            anyhow::ensure!(
                process.0.try_wait()?.is_none(),
                "coordinator exited before readiness"
            );
            if let Ok(client) =
                CoordinatorClient::connect(endpoint, Duration::from_millis(300)).await
            {
                if client
                    .query(request("g.V(1).count()"), Duration::from_millis(500))
                    .await
                    .is_ok()
                {
                    return Ok::<_, anyhow::Error>(client);
                }
            }
            sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    for text in [
        "g.V(1).out('knows')",
        "g.V(1).out('knows').in('knows').path()",
        "g.V(1).values('nullable')",
        "g.V(1).values('signed')",
        "g.V(1).values('real')",
        "g.V(1).values('enabled')",
        "g.V(1).out('knows').valueMap('nullable','name','signed','real','enabled')",
        "g.V().count()",
        "g.V(",
        "g.V(139).out('missing')",
    ] {
        assert_eq!(
            client
                .query(request(text), BUDGET)
                .await
                .with_context(|| format!("typed query {text}"))?,
            distributed::query(legacy, request(text), BUDGET).await?,
            "coordinator ingress parity: {text}"
        );
    }
    let text = "g.V(1,1).repeat(__.out('knows')).emit().times(2).count()";
    assert_eq!(
        hierarchy_count(endpoint, true, text)
            .await
            .context("typed count job")?,
        hierarchy_count(legacy, false, text).await?
    );
    assert_eq!(
        hierarchy_rows(endpoint, true)
            .await
            .context("typed rows job")?,
        hierarchy_rows(legacy, false).await?
    );
    assert_eq!(
        projected_rows(endpoint, true)
            .await
            .context("typed projected job")?,
        projected_rows(legacy, false).await?
    );
    hierarchy_cancel(endpoint, true)
        .await
        .context("typed cancel")?;
    // Malformed typed envelopes fail before a job or query is created.
    let mut raw = w::coordinator_service_client::CoordinatorServiceClient::connect(format!(
        "http://{endpoint}"
    ))
    .await?;
    for (version, millis) in [(99, 5000), (1, 0), (1, 5001)] {
        assert!(raw
            .query(w::CoordinatorQueryRequest {
                version,
                timeout_millis: millis,
                start_partition: 0,
                text: "g.V(1).count()".into(),
            })
            .await
            .is_err());
    }
    assert!(raw
        .job(w::CoordinatorJobRequest {
            version: 1,
            timeout_millis: 5000,
            request: None,
        })
        .await
        .is_err());
    assert!(raw
        .job(w::CoordinatorJobRequest {
            version: 1,
            timeout_millis: 5000,
            request: Some(w::coordinator_job_request::Request::Cancel(w::JobCancel {
                token: vec![0; 15]
            })),
        })
        .await
        .is_err());
    // Wrong transport must fail, rather than silently probing/falling back.
    assert!(distributed::query(
        endpoint,
        request("g.V(1).count()"),
        Duration::from_millis(300)
    )
    .await
    .is_err());
    if let Ok(wrong) = CoordinatorClient::connect(legacy, Duration::from_millis(300)).await {
        assert!(wrong
            .query(request("g.V(1).count()"), Duration::from_millis(300))
            .await
            .is_err());
    }
    assert!(client
        .query(request("g.V(1).count()"), BUDGET)
        .await?
        .is_ok());
    process.0.kill()?;
    process.0.wait()?;
    assert!(client
        .query(request("g.V(1).count()"), Duration::from_millis(500))
        .await
        .is_err());
    // Exercise library lifecycle independently of killing the CLI process.
    for graceful in [true, false] {
        let service =
            GrpcCoordinatorService::start("127.0.0.1:0".parse()?, catalog.clone(), BUDGET).await?;
        let address = service.local_addr();
        let held = CoordinatorClient::connect(address, BUDGET).await?;
        hierarchy_cancel(address, true).await?;
        // Leave an admitted unfinished job behind. More than the two job slots
        // across restarts must succeed only if shutdown/drop releases them.
        let retained = held
            .job(
                distributed::jobs::Request::Start {
                    start_partition: 0,
                    text: "g.V(1).repeat(__.out('knows')).times(1).count()".into(),
                },
                BUDGET,
            )
            .await??;
        assert!(matches!(
            retained,
            distributed::jobs::Output::Progress {
                complete: false,
                ..
            }
        ));
        if graceful {
            service.shutdown().await;
        } else {
            drop(service);
        }
        timeout(Duration::from_secs(2), async {
            while TcpStream::connect(address).await.is_ok() {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        assert!(held
            .query(request("g.V(1).count()"), Duration::from_millis(300))
            .await
            .is_err());
    }
    Ok(())
}

async fn projected_rows(
    endpoint: SocketAddr,
    grpc: bool,
) -> anyhow::Result<Vec<distributed::jobs::ProjectedRow>> {
    use distributed::jobs::{Output, Request};
    let client = JobClient::connect(endpoint, grpc).await?;
    let mut output = client.request( Request::StartRows {
        start_partition: 0,
        text: "g.V(1).repeat(__.out('knows')).emit().times(2).valueMap('nullable','name','signed','real','enabled')".into(),
        page_size: 1,
    }, BUDGET).await??;
    let Output::Preparing { token, .. } = output else {
        anyhow::bail!("not preparing");
    };
    for _ in 0..100 {
        let Output::Preparing {
            ready,
            next_sequence,
            ..
        } = output
        else {
            anyhow::bail!("not preparing");
        };
        if ready {
            break;
        }
        output = client
            .request(
                Request::Advance {
                    token,
                    sequence: next_sequence,
                },
                BUDGET,
            )
            .await??;
    }
    assert!(matches!(output, Output::Preparing { ready: true, .. }));
    let mut cursor = 0;
    let mut all = Vec::new();
    loop {
        let page = client
            .request(Request::Read { token, cursor }, BUDGET)
            .await??;
        let replay = client
            .request(Request::Read { token, cursor }, BUDGET)
            .await??;
        assert_eq!(serde_json::to_value(&page)?, serde_json::to_value(replay)?);
        let Output::ProjectedPage {
            rows,
            next_cursor,
            done,
            completeness,
            ..
        } = page
        else {
            anyhow::bail!("not projected page");
        };
        assert_eq!(completeness, QueryCompleteness::Unverified);
        assert_eq!(rows.len(), 1);
        all.extend(rows);
        if done {
            break;
        }
        cursor = next_cursor.context("missing cursor")?;
    }
    assert!(matches!(
        client.request(Request::Cancel { token }, BUDGET).await??,
        Output::Cancelled
    ));
    Ok(all)
}
