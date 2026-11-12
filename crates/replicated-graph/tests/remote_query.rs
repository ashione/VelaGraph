//! Real TCP/three-process pushdown; no failover or transport-security claims.
use anyhow::{ensure, Context};
use d2_raft::{send_control, ControlRequest, ControlResponse};
use replicated_graph::{
    remote::{
        self,
        query::{
            query as remote_query, QualifiedElement, QueryCompleteness, QueryFailure, QueryMode,
            QueryOutput, QueryRequest, QueryResponse, QueryRow,
        },
        RemoteError, RemoteIdentity,
    },
    run_blocking,
};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use velagraph::{
    experimental_graph::{AppendOnlyStore, CrossEdge, VertexRef},
    Edge, Export, GraphRecordStore, GremlinRow, KeyRange, KeyValue, LocalStorageNode, PathElement,
    ReadView, ScanLimit, StorageAdmin, StorageNode, StorageNodeError, Value, Vertex, WriteRequest,
    WriteResult,
};

const SHARD: u64 = 7;
const EPOCH: u64 = 3;
const BUDGET: Duration = Duration::from_secs(20);
const PAYLOAD: usize = 64 * 1024;
const QUERIES: &[&str] = &[
    "g.V().count()",
    "g.V(1).out('knows').values('name')",
    "g.V(1).out('knows').path()",
    "g.E(9).values('since')",
    "g.V(999999).count()",
    "g.V().hasLabel('large').limit(3)",
];
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
fn qualified(row: GremlinRow) -> QueryRow {
    match row {
        GremlinRow::Vertex(id) => QueryRow::Element(QualifiedElement::Vertex { partition: 0, id }),
        GremlinRow::Edge(id) => QueryRow::Element(QualifiedElement::Edge { partition: 0, id }),
        GremlinRow::Count(n) => QueryRow::Count(n),
        GremlinRow::Value(value) => QueryRow::Value {
            source: None,
            value,
        },
        GremlinRow::Path(path) => QueryRow::Path(
            path.into_iter()
                .map(|e| match e {
                    PathElement::Vertex(id) => QualifiedElement::Vertex { partition: 0, id },
                    PathElement::Edge(id) => QualifiedElement::Edge { partition: 0, id },
                })
                .collect(),
        ),
    }
}
fn request(mode: QueryMode, text: &str) -> QueryRequest {
    QueryRequest {
        shard: SHARD,
        partition: 0,
        mode,
        text: text.into(),
        timeout_millis: 5000,
    }
}

// Public local APIs create all canonical v1/v2 bytes, without hand-encoding records.
fn fixture() -> anyhow::Result<(Export, Vec<Vec<QueryRow>>)> {
    let dir = tempfile::tempdir()?;
    let node = Shared(Arc::new(LocalStorageNode::open(dir.path(), SHARD)?));
    let graph = GraphRecordStore::new(node.clone(), SHARD, 0);
    for (id, name) in [(1, "alice"), (2, "bob")] {
        let mut v = Vertex::new(id);
        v.labels.push("person".into());
        v.properties.insert("name".into(), Value::Text(name.into()));
        graph.create_vertex(&v)?;
    }
    let mut edge = Edge::new(9, 1, 2, "knows");
    edge.properties.insert("since".into(), Value::Integer(2026));
    graph.create_edge(&edge)?;
    for id in 100..148 {
        let mut v = Vertex::new(id);
        v.labels.push("large".into());
        v.properties
            .insert("payload".into(), Value::Text("x".repeat(PAYLOAD)));
        graph.create_vertex(&v)?;
    }
    graph.create_vertex(&Vertex::new(70))?;
    for id in 200..222 {
        graph.create_edge(&Edge::new(id, 70, 70, "loop"))?;
    }
    let expected = QUERIES
        .iter()
        .map(|text| {
            graph
                .execute_gremlin(text)
                .map(|r| r.rows.into_iter().map(qualified).collect())
        })
        .collect::<velagraph::Result<Vec<_>>>()?;
    let v2 = AppendOnlyStore::new(node.clone(), SHARD, 0);
    let outside = AppendOnlyStore::new(node.clone(), SHARD, 1);
    for (id, name) in [(1, "alice"), (2, "bob")] {
        let mut v = Vertex::new(id);
        v.labels.push("person".into());
        v.properties.insert("name".into(), Value::Text(name.into()));
        v.properties.insert("nullable".into(), Value::Null);
        v.properties.insert(
            "typed".into(),
            if id == 1 {
                Value::Integer(1)
            } else {
                Value::Real(1.0)
            },
        );
        v.properties
            .insert("zero".into(), Value::Real(if id == 1 { -0.0 } else { 0.0 }));
        v.properties.insert(
            "flag".into(),
            if id == 1 {
                Value::Boolean(true)
            } else {
                Value::Text("true".into())
            },
        );
        v.properties.insert(
            "unicode".into(),
            Value::Text(if id == 1 { "é" } else { "e\u{301}" }.into()),
        );
        v2.prepare_vertex(&v)?.submit(&node)?;
    }
    outside.prepare_vertex(&Vertex::new(3))?.submit(&node)?;
    let edge = CrossEdge {
        id: 9,
        from: VertexRef {
            partition: 0,
            id: 1,
        },
        to: VertexRef {
            partition: 0,
            id: 2,
        },
        label: "knows".into(),
        properties: BTreeMap::from([("since".into(), Value::Integer(2026))]),
    };
    v2.prepare_source(&edge)?
        .combine(v2.prepare_reverse(&edge)?)?
        .submit(&node)?;
    // A complete cross-partition edge exists physically on this same shard.
    // A single-partition query must reject traversing it, not hide the row.
    let cross = CrossEdge {
        id: 10,
        from: VertexRef {
            partition: 0,
            id: 2,
        },
        to: VertexRef {
            partition: 1,
            id: 3,
        },
        label: "cross".into(),
        properties: BTreeMap::new(),
    };
    v2.prepare_source(&cross)?
        .combine(outside.prepare_reverse(&cross)?)?
        .submit(&node)?;
    v2.prepare_vertex(&Vertex::new(70))?.submit(&node)?;
    for id in 200..222 {
        let edge = CrossEdge {
            id,
            from: VertexRef {
                partition: 0,
                id: 70,
            },
            to: VertexRef {
                partition: 0,
                id: 70,
            },
            // Preserve all 22 unlabeled loops while isolating a one-row
            // frontier for the full native-repeat path transport boundary.
            label: if id == 200 { "single" } else { "loop" }.into(),
            properties: BTreeMap::new(),
        };
        v2.prepare_source(&edge)?
            .combine(v2.prepare_reverse(&edge)?)?
            .submit(&node)?;
    }
    let export = node.admin().export()?;
    ensure!(
        export
            .entries
            .iter()
            .map(|e| e.key.len() + e.value.len())
            .sum::<usize>()
            > 3 * 1024 * 1024
    );
    Ok((export, expected))
}

// The forwarding listener accepts one connection only and inspects its sole
// request. Exact response bytes are counted on the wire, not estimated from rows.
async fn measured_query(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    req: QueryRequest,
) -> anyhow::Result<(QueryResponse, usize)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let proxy = listener.local_addr()?;
    let forward = tokio::spawn(async move {
        let (mut client, _) = listener.accept().await?;
        drop(listener);
        let n = client.read_u32().await? as usize;
        ensure!(n <= remote::MAX_FRAME_BYTES);
        let mut bytes = vec![0; n];
        client.read_exact(&mut bytes).await?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)?;
        ensure!(
            value["operation"].get("Query").is_some(),
            "client attempted snapshot download instead of Query"
        );
        let mut server = TcpStream::connect(endpoint).await?;
        server.write_u32(n as u32).await?;
        server.write_all(&bytes).await?;
        let n = server.read_u32().await? as usize;
        ensure!(n <= remote::MAX_FRAME_BYTES);
        let mut bytes = vec![0; n];
        server.read_exact(&mut bytes).await?;
        client.write_u32(n as u32).await?;
        client.write_all(&bytes).await?;
        Ok::<_, anyhow::Error>(n + 4)
    });
    let result = query(proxy, Some(identity), req, BUDGET).await?;
    Ok((result, forward.await??))
}

// Failure-only context for this synthetic fixture. Do not add request text to
// production diagnostics, and preserve the original typed result and budgets.
async fn query(
    endpoint: SocketAddr,
    identity: Option<RemoteIdentity>,
    req: QueryRequest,
    budget: Duration,
) -> Result<QueryResponse, RemoteError> {
    let mode = req.mode;
    let context: String = req.text.chars().take(160).collect();
    let result = remote_query(endpoint, identity, req, budget).await;
    if let Err(error) = &result {
        eprintln!("query_fixture mode={mode:?} query={context:?} error={error}");
    }
    result
}

async fn raw_query(endpoint: SocketAddr, req: &QueryRequest) -> anyhow::Result<serde_json::Value> {
    let mut socket = TcpStream::connect(endpoint).await?;
    let bytes =
        serde_json::to_vec(&serde_json::json!({"version": 1, "operation": {"Query": req}}))?;
    socket.write_u32(bytes.len() as u32).await?;
    socket.write_all(&bytes).await?;
    let size = socket.read_u32().await? as usize;
    ensure!(size <= remote::MAX_FRAME_BYTES);
    let mut bytes = vec![0; size];
    socket.read_exact(&mut bytes).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn assert_stamp(output: &QueryOutput, identity: RemoteIdentity, sequence: u64) {
    assert_eq!(output.snapshot.identity, identity);
    assert_eq!(output.snapshot.shard, SHARD);
    assert_eq!(output.snapshot.epoch, EPOCH);
    assert_eq!(output.snapshot.sequence, sequence);
    assert_eq!(output.partition, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_process_query_pushdown_is_bounded_and_does_not_download_shard() -> anyhow::Result<()>
{
    timeout(Duration::from_secs(180), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let mut reserved = Vec::new();
    for _ in 0..9 {
        reserved.push(TcpListener::bind("127.0.0.1:0").await?);
    }
    let addresses = reserved
        .iter()
        .map(|l| l.local_addr())
        .collect::<std::io::Result<Vec<_>>>()?;
    let members = (0..3)
        .map(|i| format!("{}={}", i + 1, addresses[3 * i]))
        .collect::<Vec<_>>()
        .join(",");
    drop(reserved);
    let dirs = (0..3)
        .map(|_| tempfile::tempdir())
        .collect::<std::io::Result<Vec<_>>>()?;
    let mut processes = Vec::new();
    for i in 0..3 {
        let mut c = Command::new(env!("CARGO_BIN_EXE_graph-node"));
        // Keep fixture runtime threads bounded on high-core shared hosts.
        c.env("TOKIO_WORKER_THREADS", "2");
        c.args([
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
            c.args(["--bootstrap", &members]);
        }
        processes.push(Process(
            c.stdout(Stdio::null()).stderr(Stdio::inherit()).spawn()?,
        ));
    }
    let leader = timeout(Duration::from_secs(35), async {
        loop {
            for i in 0..3 {
                if let Ok(ControlResponse::Status(status)) =
                    send_control(&addresses[3 * i + 1].to_string(), ControlRequest::Status).await
                {
                    if status.leader == Some((i + 1) as u64)
                        && remote::probe_identified(
                            addresses[3 * i + 2],
                            RemoteIdentity {
                                group: 0,
                                node: (i + 1) as u64,
                            },
                            SHARD,
                            Duration::from_millis(500),
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
    let endpoint = addresses[3 * leader + 2];
    let identity = RemoteIdentity {
        group: 0,
        node: (leader + 1) as u64,
    };
    let (fixture, expected) = run_blocking(fixture).await??;
    // Seed bounded batches, keeping each 64 KiB property record separate from
    // other large records. Group tiny edge/index rows to avoid hundreds of
    // unnecessary consensus rounds. No uncertain write is retried.
    let mut batches = Vec::new();
    let mut operations = Vec::new();
    let mut bytes = 0;
    for e in fixture.entries {
        let size = e.key.len() + e.value.len();
        if !operations.is_empty() && (bytes + size > 70 * 1024 || operations.len() >= 128) {
            batches.push(std::mem::take(&mut operations));
            bytes = 0;
        }
        bytes += size;
        operations.push(d2_raft::WriteOp::Put {
            key: e.key,
            value: e.value,
        });
    }
    if !operations.is_empty() {
        batches.push(operations);
    }
    let sequence = batches.len() as u64;
    for (index, operations) in batches.into_iter().enumerate() {
        let mut request_id = [0; 16];
        request_id[..8].copy_from_slice(&(index as u64).to_be_bytes());
        request_id[8..].copy_from_slice(b"query-fi");
        remote::write(
            endpoint,
            d2_raft::D1WriteRequest {
                shard: SHARD,
                epoch: EPOCH,
                request_id,
                expected_sequence: None,
                operations,
            },
            BUDGET,
        )
        .await
        .with_context(|| format!("seed query fixture batch {index}"))?;
    }
    assert!(
        matches!(
            remote::snapshot(endpoint, SHARD, BUDGET).await,
            Err(RemoteError::ResourceExhausted)
        ),
        "whole snapshot RPC must exceed 2 MiB"
    );
    for (text, expected) in QUERIES.iter().zip(expected) {
        let (response, bytes) =
            measured_query(endpoint, identity, request(QueryMode::V1, text)).await?;
        let output = response.with_context(|| format!("v1 {text}"))?;
        assert_eq!(output.rows, expected, "local-vs-pushdown: {text}");
        assert_stamp(&output, identity, sequence);
        assert_eq!(output.completeness, QueryCompleteness::SinglePartition);
        assert!(bytes < 4096, "query returned {bytes} wire bytes for {text}");
    }
    let scalar = |source, value| QueryRow::Value {
        source: Some(source),
        value,
    };
    let vertex = |id| QualifiedElement::Vertex { partition: 0, id };
    let edge = |id| QualifiedElement::Edge { partition: 0, id };
    let property_map = |id, properties| QueryRow::PropertyMap {
        source: vertex(id),
        properties,
    };
    for (text, expected) in [
        (
            "g.V(2,1,2).values('typed').is(1)",
            vec![scalar(vertex(1), Value::Integer(1))],
        ),
        (
            "g.V(2,1,2).values('typed').is(1.0)",
            vec![scalar(vertex(2), Value::Real(1.0)); 2],
        ),
        ("g.V(2,1,2).values('typed').is('1')", vec![]),
        (
            "g.V(2,1,2).values('flag').is(true)",
            vec![scalar(vertex(1), Value::Boolean(true))],
        ),
        (
            "g.V(2,1,2).values('flag').is('true')",
            vec![scalar(vertex(2), Value::Text("true".into())); 2],
        ),
        (
            "g.V(2,1,2).values('zero').is(-0.0)",
            vec![
                scalar(vertex(2), Value::Real(0.0)),
                scalar(vertex(1), Value::Real(-0.0)),
                scalar(vertex(2), Value::Real(0.0)),
            ],
        ),
        (
            "g.V(2,1,2).values('zero').is(0.0).dedup()",
            vec![scalar(vertex(2), Value::Real(0.0))],
        ),
        (
            "g.V(2,1,2).values('unicode').is('é')",
            vec![scalar(vertex(1), Value::Text("é".into()))],
        ),
        (
            "g.V(70,2,1).values('nullable').is(null)",
            vec![
                scalar(vertex(2), Value::Null),
                scalar(vertex(1), Value::Null),
            ],
        ),
        (
            "g.V(2,1,2).id().is(2).limit(1).is(2)",
            vec![scalar(vertex(2), Value::Integer(2))],
        ),
        (
            "g.V(2,1,2).id().limit(1).is(1).count()",
            vec![QueryRow::Count(0)],
        ),
        (
            "g.V(2,1,2).id().is(1).limit(1).count()",
            vec![QueryRow::Count(1)],
        ),
        (
            "g.V(2,1,2).id().is(2).dedup().count()",
            vec![QueryRow::Count(1)],
        ),
        (
            "g.V(2,1,2).label().is('person')",
            vec![
                scalar(vertex(2), Value::Text("person".into())),
                scalar(vertex(1), Value::Text("person".into())),
                scalar(vertex(2), Value::Text("person".into())),
            ],
        ),
        (
            "g.E(9,9).values('since').is(2026)",
            vec![scalar(edge(9), Value::Integer(2026)); 2],
        ),
        (
            "g.E(9,9).id().is(9).dedup()",
            vec![scalar(edge(9), Value::Integer(9))],
        ),
        (
            "g.E(9,9).label().dedup().is('knows')",
            vec![scalar(edge(9), Value::Text("knows".into()))],
        ),
        (
            "g.V(999999).values('missing').is(null).count()",
            vec![QueryRow::Count(0)],
        ),
        (
            "g.V(2,1,2).valueMap('name','missing')",
            vec![
                property_map(
                    2,
                    BTreeMap::from([("name".into(), vec![Value::Text("bob".into())])]),
                ),
                property_map(
                    1,
                    BTreeMap::from([("name".into(), vec![Value::Text("alice".into())])]),
                ),
                property_map(
                    2,
                    BTreeMap::from([("name".into(), vec![Value::Text("bob".into())])]),
                ),
            ],
        ),
        (
            "g.V(1,2,1).dedup().limit(1).valueMap('typed','flag','missing')",
            vec![property_map(
                1,
                BTreeMap::from([
                    ("typed".into(), vec![Value::Integer(1)]),
                    ("flag".into(), vec![Value::Boolean(true)]),
                ]),
            )],
        ),
        (
            "g.V(1).valueMap('missing')",
            vec![property_map(1, BTreeMap::new())],
        ),
        (
            "g.V(1,2,1).values('typed').dedup()",
            vec![
                scalar(vertex(1), Value::Integer(1)),
                scalar(vertex(2), Value::Real(1.0)),
            ],
        ),
        (
            "g.V(1,2,1).values('flag').dedup()",
            vec![
                scalar(vertex(1), Value::Boolean(true)),
                scalar(vertex(2), Value::Text("true".into())),
            ],
        ),
        (
            "g.V(2,1,2).values('zero').dedup()",
            vec![scalar(vertex(2), Value::Real(0.0))],
        ),
        (
            "g.V(2,1,2).values('unicode').dedup()",
            vec![
                scalar(vertex(2), Value::Text("e\u{301}".into())),
                scalar(vertex(1), Value::Text("é".into())),
            ],
        ),
        (
            "g.V(2,1,2).id().dedup()",
            vec![
                scalar(vertex(2), Value::Integer(2)),
                scalar(vertex(1), Value::Integer(1)),
            ],
        ),
        (
            "g.V(2,1,2).values('name').dedup()",
            vec![
                scalar(vertex(2), Value::Text("bob".into())),
                scalar(vertex(1), Value::Text("alice".into())),
            ],
        ),
        (
            "g.V(2,1,2).label().dedup()",
            vec![scalar(vertex(2), Value::Text("person".into()))],
        ),
        (
            "g.V(70,2,1).values('nullable').dedup()",
            vec![scalar(vertex(2), Value::Null)],
        ),
        (
            "g.E(9,9).values('since').dedup()",
            vec![scalar(edge(9), Value::Integer(2026))],
        ),
        (
            "g.E(9,9).id().dedup()",
            vec![scalar(edge(9), Value::Integer(9))],
        ),
        (
            "g.E(9,9).label().dedup()",
            vec![scalar(edge(9), Value::Text("knows".into()))],
        ),
        (
            "g.V(2,2,1).id().limit(2).dedup().count()",
            vec![QueryRow::Count(1)],
        ),
        (
            "g.V(2,2,1).id().dedup().limit(2).count()",
            vec![QueryRow::Count(2)],
        ),
        ("g.V(999999).id().dedup()", vec![]),
        ("g.E(999999).label().dedup()", vec![]),
        (
            "g.V(70).values('missing').dedup().count()",
            vec![QueryRow::Count(0)],
        ),
        (
            "g.V(1).out('knows').values('name')",
            vec![QueryRow::Value {
                source: Some(QualifiedElement::Vertex {
                    partition: 0,
                    id: 2,
                }),
                value: Value::Text("bob".into()),
            }],
        ),
        (
            "g.E(9).values('since')",
            vec![QueryRow::Value {
                source: Some(QualifiedElement::Edge {
                    partition: 0,
                    id: 9,
                }),
                value: Value::Integer(2026),
            }],
        ),
        (
            "g.V(1).outE('knows').inV().path()",
            vec![QueryRow::Path(vec![
                QualifiedElement::Vertex {
                    partition: 0,
                    id: 1,
                },
                QualifiedElement::Edge {
                    partition: 0,
                    id: 9,
                },
                QualifiedElement::Vertex {
                    partition: 0,
                    id: 2,
                },
            ])],
        ),
        (
            "g.V(1,1).outE('knows').dedup().inV().path()",
            vec![QueryRow::Path(vec![
                QualifiedElement::Vertex {
                    partition: 0,
                    id: 1,
                },
                QualifiedElement::Edge {
                    partition: 0,
                    id: 9,
                },
                QualifiedElement::Vertex {
                    partition: 0,
                    id: 2,
                },
            ])],
        ),
        (
            "g.V(2,1,2).dedup()",
            vec![
                QueryRow::Element(QualifiedElement::Vertex {
                    partition: 0,
                    id: 2,
                }),
                QueryRow::Element(QualifiedElement::Vertex {
                    partition: 0,
                    id: 1,
                }),
            ],
        ),
        (
            "g.E(9,9).dedup()",
            vec![QueryRow::Element(QualifiedElement::Edge {
                partition: 0,
                id: 9,
            })],
        ),
        (
            "g.V(1,1).dedup().out('knows').count()",
            vec![QueryRow::Count(1)],
        ),
        (
            "g.V(2,2,1).limit(2).dedup().count()",
            vec![QueryRow::Count(1)],
        ),
        (
            "g.V(2,2,1).dedup().limit(2).count()",
            vec![QueryRow::Count(2)],
        ),
        ("g.V(999999).dedup().count()", vec![QueryRow::Count(0)]),
        ("g.V().count()", vec![QueryRow::Count(3)]),
        ("g.V(999999).count()", vec![QueryRow::Count(0)]),
    ] {
        let (result, bytes) =
            measured_query(endpoint, identity, request(QueryMode::V2Observed, text)).await?;
        let output = result.with_context(|| format!("v2 {text}"))?;
        assert_eq!(output.rows, expected, "v2 {text}");
        assert_eq!(output.completeness, QueryCompleteness::Unverified);
        assert_stamp(&output, identity, sequence);
        assert!(bytes < 4096);
    }
    for (text, expected) in [
        (
            "g.V(1,1).out('knows').simplePath().path()",
            vec![QueryRow::Path(vec![vertex(1), edge(9), vertex(2)]); 2],
        ),
        (
            "g.V(1).out('knows').inE('knows').simplePath().path()",
            vec![QueryRow::Path(vec![vertex(1), edge(9), vertex(2), edge(9)])],
        ),
        (
            "g.V(1).outE('knows').inV().inE('knows').cyclicPath().path()",
            vec![QueryRow::Path(vec![vertex(1), edge(9), vertex(2), edge(9)])],
        ),
        (
            "g.V(1).outE('knows').inV().inE('knows').simplePath().count()",
            vec![QueryRow::Count(0)],
        ),
        (
            "g.V(1).out('knows').in('knows').cyclicPath().path()",
            vec![QueryRow::Path(vec![
                vertex(1),
                edge(9),
                vertex(2),
                edge(9),
                vertex(1),
            ])],
        ),
        (
            "g.V(70).out().simplePath().count()",
            vec![QueryRow::Count(0)],
        ),
        (
            "g.V(70).out().cyclicPath().count()",
            vec![QueryRow::Count(22)],
        ),
        (
            "g.V(70).both('single').cyclicPath().count()",
            vec![QueryRow::Count(1)],
        ),
        (
            "g.V(70).repeat(out('single')).times(2).emit().cyclicPath().count()",
            vec![QueryRow::Count(2)],
        ),
        (
            "g.V(70).repeat(out('single')).times(2).emit().simplePath().count()",
            vec![QueryRow::Count(0)],
        ),
        ("g.E(9,9).simplePath().count()", vec![QueryRow::Count(2)]),
        ("g.V(999999).cyclicPath().count()", vec![QueryRow::Count(0)]),
    ] {
        let output = query(
            endpoint,
            Some(identity),
            request(QueryMode::V2Observed, text),
            BUDGET,
        )
        .await??;
        assert_eq!(output.rows, expected, "{text}");
        assert_eq!(output.completeness, QueryCompleteness::Unverified);
        assert_stamp(&output, identity, sequence);
    }
    for text in ["g.V(999999).simplePath()", "g.V(70).cyclicPath()"] {
        assert_eq!(
            query(
                endpoint,
                Some(identity),
                request(QueryMode::V1, text),
                BUDGET
            )
            .await?,
            Err(QueryFailure::Unsupported),
            "{text}"
        );
    }
    for text in [
        "g.V(2).out('cross').range(0,0)",
        "g.V(2).out('cross').cyclicPath().count()",
        "g.V(2).out('cross').simplePath().range(0,0)",
        "g.E(10).range(0,0)",
        "g.V(2).out('cross')",
        "g.V(2).out('cross').valueMap('name')",
        "g.E(10).inV().valueMap('name')",
        "g.E(10)",
        "g.E().count()",
        "g.V(2).out('cross').dedup().limit(1)",
        "g.E(10,10).dedup()",
        "g.V(2).out('cross').id().dedup().limit(1)",
        "g.V(2).repeat(out('cross')).times(1).dedup().limit(1)",
    ] {
        assert_eq!(
            query(
                endpoint,
                Some(identity),
                request(QueryMode::V2Observed, text),
                BUDGET
            )
            .await?,
            Err(QueryFailure::UnsupportedScope),
            "must not hide out-of-scope row: {text}"
        );
    }
    for mode in [QueryMode::V1, QueryMode::V2Observed] {
        for (text, failure) in [
            ("g.V(", QueryFailure::Parse),
            ("g.E().valueMap('name')", QueryFailure::Unsupported),
            // Element-to-scalar filtering is invalid even on an empty root.
            ("g.V(999999).is(1)", QueryFailure::Unsupported),
        ] {
            assert_eq!(
                query(endpoint, Some(identity), request(mode, text), BUDGET).await?,
                Err(failure)
            );
        }
    }
    assert_eq!(
        query(
            endpoint,
            Some(identity),
            request(QueryMode::V1, "g.V().valueMap('name')"),
            BUDGET
        )
        .await?,
        Err(QueryFailure::Unsupported)
    );
    assert_eq!(
        query(
            endpoint,
            Some(identity),
            request(QueryMode::V1, "g.V().dedup()"),
            BUDGET
        )
        .await?,
        Err(QueryFailure::Unsupported),
        "the shared v3 extension does not expand v1"
    );
    assert_eq!(
        query(
            endpoint,
            Some(identity),
            request(QueryMode::V1, "g.V(1).id().is(1)"),
            BUDGET
        )
        .await?,
        Err(QueryFailure::Unsupported)
    );
    for text in ["g.V(999999).range(0,0)", "g.V(1).range(0,1)"] {
        assert_eq!(
            query(
                endpoint,
                Some(identity),
                request(QueryMode::V1, text),
                BUDGET
            )
            .await?,
            Err(QueryFailure::Unsupported)
        );
    }
    for (text, expected) in [
        (
            "g.V(2,1,2).range(1,3)",
            vec![
                QueryRow::Element(QualifiedElement::Vertex {
                    partition: 0,
                    id: 1,
                }),
                QueryRow::Element(QualifiedElement::Vertex {
                    partition: 0,
                    id: 2,
                }),
            ],
        ),
        (
            "g.E(9,9).range(1,2)",
            vec![QueryRow::Element(QualifiedElement::Edge {
                partition: 0,
                id: 9,
            })],
        ),
        (
            "g.V(70).out().out().range(0,150)",
            vec![
                QueryRow::Element(QualifiedElement::Vertex {
                    partition: 0,
                    id: 70
                });
                150
            ],
        ),
        (
            "g.V(70).out().out().limit(150).range(0,150)",
            vec![
                QueryRow::Element(QualifiedElement::Vertex {
                    partition: 0,
                    id: 70
                });
                150
            ],
        ),
        ("g.V(999999).range(0,0).count()", vec![QueryRow::Count(0)]),
    ] {
        let output = query(
            endpoint,
            Some(identity),
            request(QueryMode::V2Observed, text),
            BUDGET,
        )
        .await??;
        assert_eq!(output.rows, expected, "{text}");
        assert_eq!(output.completeness, QueryCompleteness::Unverified);
        assert_stamp(&output, identity, sequence);
    }
    assert_eq!(
        query(
            endpoint,
            Some(identity),
            request(
                QueryMode::V2Observed,
                "g.V(70).out().out().out().range(0,0)"
            ),
            BUDGET
        )
        .await?,
        Err(QueryFailure::ResourceExhausted)
    );
    let loop_vertex = QualifiedElement::Vertex {
        partition: 0,
        id: 70,
    };
    let loop_edge = QualifiedElement::Edge {
        partition: 0,
        id: 200,
    };
    let mut longest = vec![loop_edge, loop_vertex];
    for _ in 0..32 {
        longest.extend([loop_edge, loop_vertex]);
    }
    assert_eq!(longest.len(), 66);
    for text in [
        "g.E(200).inV().repeat(out('single')).times(32).path()",
        "g.E(200).inV().repeat(out('single')).times(32).cyclicPath().path()",
        "g.E(200).outV().in('single').repeat(out('single')).times(31).path()",
    ] {
        let output = query(
            endpoint,
            Some(identity),
            request(QueryMode::V2Observed, text),
            BUDGET,
        )
        .await??;
        assert_eq!(output.rows, vec![QueryRow::Path(longest.clone())], "{text}");
        assert_eq!(output.completeness, QueryCompleteness::Unverified);
    }
    assert_eq!(
        query(
            endpoint,
            Some(identity),
            request(
                QueryMode::V2Observed,
                "g.V(70).repeat(out('single')).times(32).emit().count()"
            ),
            BUDGET
        )
        .await??
        .rows,
        vec![QueryRow::Count(32)]
    );
    assert!(query(
        endpoint,
        Some(identity),
        request(
            QueryMode::V2Observed,
            "g.E(200).inV().repeat(out('single')).times(32).simplePath().path()",
        ),
        BUDGET,
    )
    .await??
    .rows
    .is_empty());
    for text in [
        "g.V(999999).repeat(out()).times(32).out()",
        "g.E(999999).repeat(out()).times(1)",
        "g.V(999999).id().repeat(out()).times(1)",
    ] {
        assert!(
            query(
                endpoint,
                Some(identity),
                request(QueryMode::V2Observed, text),
                BUDGET
            )
            .await?
            .is_err(),
            "accepted {text}"
        );
    }
    assert_eq!(
        query(
            endpoint,
            Some(identity),
            request(QueryMode::V1, "g.V(70).repeat(out()).times(1)"),
            BUDGET
        )
        .await?,
        Err(QueryFailure::Unsupported)
    );
    for text in [
        "g.V(999999).id().simplePath()",
        "g.E(999999).label().cyclicPath()",
        "g.V(999999).values('missing').simplePath()",
        "g.V(999999).simplePath().by('name')",
        "g.V(999999).cyclicPath().from('x')",
        "g.V(999999).simplePath().to('x')",
        "g.V(999999).repeat(out().simplePath()).times(2)",
        "g.V(999999).repeat(out().cyclicPath()).times(2)",
        "g.V(999999).is(1)",
        "g.E(999999).is(1)",
        "g.V(999999).id().is(1).out()",
        "g.E(999999).label().is('missing').inV()",
        "g.V(999999).values('missing').is(null).path()",
        "g.V(1).id().is(0).hasLabel('person')",
        "g.V(999999).id().is(eq(1))",
        "g.V(999999).id().is(gt(1))",
        "g.V(999999).id().dedup().out()",
        "g.E(999999).label().dedup().inV()",
        "g.V(999999).values('missing').dedup().path()",
        "g.V(1).values('name').dedup().hasLabel('person')",
        "g.V(999999).dedup('x')",
        "g.V(999999).dedup().by('name')",
    ] {
        assert!(
            query(
                endpoint,
                Some(identity),
                request(QueryMode::V2Observed, text),
                BUDGET
            )
            .await?
            .is_err(),
            "accepted {text}"
        );
    }
    for mode in [QueryMode::V1, QueryMode::V2Observed] {
        let complex = format!("g.V({}1)", "1,".repeat(64));
        assert_eq!(
            query(endpoint, Some(identity), request(mode, &complex), BUDGET).await?,
            Err(QueryFailure::ResourceExhausted),
            "official-parser structural admission must survive pushdown"
        );
        assert_eq!(
            query(
                endpoint,
                Some(identity),
                request(mode, "g.V().limit(0)"),
                BUDGET
            )
            .await?,
            Err(QueryFailure::InvalidArgument)
        );
        let mut exact = String::from("g.V(999999).count()");
        exact.push_str(&" ".repeat(remote::query::MAX_QUERY_TEXT_BYTES - exact.len()));
        let output = query(endpoint, Some(identity), request(mode, &exact), BUDGET).await??;
        assert_eq!(output.rows, vec![QueryRow::Count(0)]);
        assert_stamp(&output, identity, sequence);
    }
    for mode in [QueryMode::V1, QueryMode::V2Observed] {
        assert_eq!(
            query(
                endpoint,
                Some(identity),
                request(mode, "g.V(70).out().out().out().count()"),
                BUDGET
            )
            .await?,
            Err(QueryFailure::ResourceExhausted),
            "22 parallel self-loop edges must exhaust executor work/row budget"
        );
        let bounded = query(
            endpoint,
            Some(identity),
            request(mode, "g.V(70).out().limit(1).out().count()"),
            BUDGET,
        )
        .await??;
        assert_eq!(bounded.rows, vec![QueryRow::Count(22)]);
        assert_stamp(&bounded, identity, sequence);
    }
    assert_eq!(
        query(
            endpoint,
            Some(identity),
            request(
                QueryMode::V2Observed,
                "g.V(70).out().out().out().id().is(-1)"
            ),
            BUDGET
        )
        .await?,
        Err(QueryFailure::ResourceExhausted),
        "a nonmatching scalar filter must not hide upstream traversal exhaustion"
    );
    let large = query(
        endpoint,
        Some(identity),
        request(QueryMode::V1, "g.V().hasLabel('large').values('payload')"),
        BUDGET,
    )
    .await?;
    assert_eq!(
        large,
        Err(QueryFailure::ResourceExhausted),
        "oversized output must not be partial success"
    );
    let mut wrong = request(QueryMode::V1, "g.V().count()");
    wrong.shard = 99;
    assert!(matches!(
        query(endpoint, Some(identity), wrong, BUDGET).await,
        Err(RemoteError::InvalidArgument)
    ));
    assert!(matches!(
        query(
            endpoint,
            Some(RemoteIdentity {
                group: 99,
                ..identity
            }),
            request(QueryMode::V1, "g.V().count()"),
            BUDGET
        )
        .await,
        Err(RemoteError::InvalidArgument)
    ));
    let oversized = request(
        QueryMode::V1,
        &" ".repeat(remote::query::MAX_QUERY_TEXT_BYTES + 1),
    );
    assert!(matches!(
        query(endpoint, Some(identity), oversized.clone(), BUDGET).await,
        Err(RemoteError::ResourceExhausted)
    ));
    assert_eq!(
        raw_query(endpoint, &oversized).await?["result"]["Err"],
        "ResourceExhausted"
    );
    for millis in [0, 5001, u64::MAX] {
        let mut req = request(QueryMode::V1, "g.V().count()");
        req.timeout_millis = millis;
        assert!(matches!(
            query(endpoint, Some(identity), req.clone(), BUDGET).await,
            Err(RemoteError::InvalidArgument)
        ));
        assert_eq!(
            raw_query(endpoint, &req).await?["result"]["Err"],
            "InvalidArgument"
        );
    }
    let follower = (leader + 1) % 3;
    assert!(matches!(
        query(
            addresses[3 * follower + 2],
            Some(RemoteIdentity {
                group: 0,
                node: (follower + 1) as u64
            }),
            request(QueryMode::V1, "g.V().count()"),
            BUDGET
        )
        .await,
        Err(RemoteError::NotLeader { .. })
    ));
    // No writes were performed by any query, including all failing executions.
    assert_eq!(
        remote::metadata(endpoint, SHARD, BUDGET).await?,
        (SHARD, EPOCH, sequence)
    );
    drop(processes);
    Ok(())
}
