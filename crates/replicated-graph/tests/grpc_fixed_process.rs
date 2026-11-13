//! Typed fixed-query transport differential in three independent OS processes.
//! Managed control remains TCP; only data-local fragment transport is gRPC.
// Some exact shared helpers are used only by the other integration targets.
#![allow(dead_code)]

use anyhow::{anyhow, Context};
use d2_raft::{
    managed_rpc::{activate_byte_bound_member, Client, Identity, Role, Server},
    participant_writes::Authority,
    transaction_decisions::{CapacityProfile, Cohort, Command as DecisionCommand, Journal, Member},
    versioned_reads::Target,
    Cluster, D1Command, LogCommand, NodeHandle, StateMachine,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use replicated_graph::{
    managed_gateway::{
        self as gateway, Binding, EdgeData, Entity, Gateway, Mapping, Mutation, Operation, Query,
        RemoteConfig, Reply, Session, VertexData, Write, WriteStatus,
    },
    remote::query::{QueryCompleteness, QueryRow},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::{Cursor, Write as _},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};

include!("support/managed_process_fixture.rs");

use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Notify},
    task::{JoinHandle, JoinSet},
};
use velagraph::Value;

async fn routes(
    p: &Process,
) -> anyhow::Result<(gateway::LocalGraphRoute, gateway::LocalGraphRoute)> {
    fs::write(p.directory.path().join("graph.request"), b"start")?;
    fs::write(p.directory.path().join("grpc.request"), b"start")?;
    let mut addresses = Vec::new();
    for file in ["graph-ready.json", "grpc-ready.json"] {
        addresses.push(
            tokio::time::timeout(BUDGET, async {
                loop {
                    if let Ok(bytes) = fs::read(p.directory.path().join(file)) {
                        let (pid, endpoint): (u32, SocketAddr) = serde_json::from_slice(&bytes)?;
                        anyhow::ensure!(pid == p.child.id(), "stale transport readiness");
                        return Ok::<_, anyhow::Error>(endpoint);
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await??,
        );
    }
    let route = |endpoint| gateway::LocalGraphRoute {
        endpoint,
        identity: replicated_graph::remote::RemoteIdentity {
            group: p.shard,
            node: 1,
        },
        managed_identity: identity(p.shard),
    };
    Ok((route(addresses[0]), route(addresses[1])))
}
async fn submit(g: &Gateway, id: u8, mutation: Mutation) -> anyhow::Result<u64> {
    let mut external_id = [0; 16];
    external_id[..4].copy_from_slice(&[b'G', b'F', b'X', id]);
    Ok(committed(
        call(
            g,
            Operation::Submit(Write {
                external_id,
                mutation,
            }),
        )
        .await?,
    )
    .1)
}
async fn local_open(
    g: &Gateway,
    version: u64,
    query: &Query,
) -> anyhow::Result<(gateway::LocalSession, gateway::QueryPage)> {
    let Reply::OpenLocalQuery { session, page, .. } = call(
        g,
        Operation::OpenLocalQuery {
            version: Some(version),
            ttl_millis: 60_000,
            query: query.clone(),
            page_size: 1,
        },
    )
    .await?
    else {
        anyhow::bail!("expected open local query");
    };
    assert_eq!(session.version, version);
    Ok((session, page))
}
async fn local_rows(g: &Gateway, version: u64, query: &Query) -> anyhow::Result<Vec<QueryRow>> {
    let (session, mut page) = local_open(g, version, query).await?;
    let mut rows = Vec::new();
    loop {
        rows.extend(page.data.rows);
        let Some(cursor) = page.next else {
            break;
        };
        let fetch = Operation::FetchLocalQuery {
            session: session.clone(),
            query: query.clone(),
            cursor: Some(cursor),
            page_size: 1,
        };
        let reply = call(g, fetch.clone()).await?;
        assert_eq!(call(g, fetch).await?, reply, "exact result cursor replay");
        let Reply::FetchLocalQuery { page: next, .. } = reply else {
            anyhow::bail!("expected fetch");
        };
        page = next;
    }
    call(
        g,
        Operation::CloseLocalQuery {
            session: session.clone(),
        },
    )
    .await?;
    assert!(call(
        g,
        Operation::FetchLocalQuery {
            session,
            query: query.clone(),
            cursor: None,
            page_size: 1
        }
    )
    .await
    .is_err());
    Ok(rows)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fixed_grpc_matches_tcp_in_real_managed_processes() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(180), fixed_scenario()).await?
}
async fn fixed_scenario() -> anyhow::Result<()> {
    let c = Process::start(9).await?;
    let a = Process::start(7).await?;
    let b = Process::start(8).await?;
    let (at, ag) = routes(&a).await?;
    let (bt, bg) = routes(&b).await?;
    let config = || -> anyhow::Result<RemoteConfig> {
        Ok(RemoteConfig {
            coordinator: c.client()?,
            binding: binding(),
            members: vec![a.client()?, b.client()?],
        })
    };
    let tcp =
        Gateway::bind_remote_local_queries(config()?, vec![at, bt], "127.0.0.1:0".parse()?, BUDGET)
            .await?;
    let grpc = Gateway::bind_remote_local_queries_grpc(
        config()?,
        vec![ag, bg],
        "127.0.0.1:0".parse()?,
        BUDGET,
        2,
        2,
    )
    .await?;
    for (index, (partition, id)) in [(7, 1), (8, 2), (8, 3)].into_iter().enumerate() {
        submit(
            &tcp,
            index as u8 + 1,
            Mutation::CreateVertex {
                partition,
                vertex: VertexData {
                    id,
                    labels: vec!["person".into()],
                    properties: BTreeMap::from([
                        ("name".into(), Value::Text(format!("person-{id}"))),
                        ("null".into(), Value::Null),
                        ("real".into(), Value::Real(-0.0)),
                        ("int".into(), Value::Integer(i64::MIN)),
                        ("bool".into(), Value::Boolean(true)),
                    ]),
                },
            },
        )
        .await?;
    }
    let mut version = 0;
    for (index, id, to) in [(4, 20, 3), (5, 10, 2), (6, 30, 3)] {
        version = submit(
            &tcp,
            index,
            Mutation::CreateEdge {
                edge: EdgeData {
                    id,
                    from: Entity {
                        partition: 7,
                        id: 1,
                    },
                    to: Entity {
                        partition: 8,
                        id: to,
                    },
                    label: "knows".into(),
                    properties: BTreeMap::new(),
                },
            },
        )
        .await?;
    }
    for text in [
        "g.V(1).out('knows').path()",
        "g.V(1).out('knows').count()",
        "g.V(1,1).out('knows').path()",
        "g.V(1).out().in().path()",
        "g.V(999).count()",
    ] {
        let query = Query {
            partition: 7,
            text: text.into(),
        };
        assert_eq!(
            local_rows(&grpc, version, &query).await?,
            local_rows(&tcp, version, &query).await?,
            "fixed-V ordered protocol differential: {text}"
        );
    }
    let query = Query {
        partition: 7,
        text: "g.V(1).out('knows').path()".into(),
    };
    let expected = [(10, 2), (20, 3), (30, 3)]
        .into_iter()
        .map(|(edge, target)| {
            QueryRow::Path(vec![
                replicated_graph::remote::query::QualifiedElement::Vertex {
                    partition: 7,
                    id: 1,
                },
                replicated_graph::remote::query::QualifiedElement::Edge {
                    partition: 7,
                    id: edge,
                },
                replicated_graph::remote::query::QualifiedElement::Vertex {
                    partition: 8,
                    id: target,
                },
            ])
        })
        .collect::<Vec<_>>();
    assert_eq!(
        local_rows(&grpc, version, &query).await?,
        expected,
        "parallel fixed fragments preserve edge order and bag multiplicity"
    );

    let (old, first) = local_open(&grpc, version, &query).await?;
    let fetch = Operation::FetchLocalQuery {
        session: old.clone(),
        query: query.clone(),
        cursor: first.next.clone(),
        page_size: 1,
    };
    let before = call(&grpc, fetch.clone()).await?;
    let later = submit(
        &grpc,
        7,
        Mutation::CreateVertex {
            partition: 7,
            vertex: VertexData {
                id: 99,
                labels: vec![],
                properties: BTreeMap::new(),
            },
        },
    )
    .await?;
    assert!(later > version);
    assert_eq!(
        call(&grpc, fetch).await?,
        before,
        "old fixed-V retained result survives committed writes"
    );
    let added = Query {
        partition: 7,
        text: "g.V(99).count()".into(),
    };
    assert_eq!(
        local_rows(&grpc, version, &added).await?,
        vec![QueryRow::Count(0)]
    );
    assert_eq!(
        local_rows(&grpc, later, &added).await?,
        vec![QueryRow::Count(1)]
    );
    call(&grpc, Operation::CloseLocalQuery { session: old }).await?;
    job_roundtrip(&grpc, version, &query, expected).await?;
    raw_fixed_checks(&c, &a, later).await?;
    grpc.shutdown().await;
    tcp.shutdown().await;
    Ok(())
}

async fn job_roundtrip(
    g: &Gateway,
    version: u64,
    query: &Query,
    expected: Vec<QueryRow>,
) -> anyhow::Result<()> {
    let operation = Operation::StartLocalQueryJob {
        operation_id: [55; 16],
        version: Some(version),
        query: query.clone(),
        page_size: 1,
        policy: gateway::JobPolicy {
            idle_millis: 60_000,
            execution_millis: 120_000,
            lifetime_millis: 180_000,
        },
    };
    let admitted = call(g, operation.clone()).await?;
    assert_eq!(
        call(g, operation).await?,
        admitted,
        "job admission replay preserves identity"
    );
    let Reply::StartLocalQueryJob { job, .. } = admitted else {
        anyhow::bail!("expected job");
    };
    tokio::time::timeout(BUDGET, async {
        loop {
            let Reply::GetLocalQueryJob { status, .. } =
                call(g, Operation::GetLocalQueryJob { job: job.clone() }).await?
            else {
                anyhow::bail!("expected status");
            };
            if status.phase == gateway::JobPhase::Ready {
                break;
            }
            anyhow::ensure!(
                matches!(
                    status.phase,
                    gateway::JobPhase::Capturing
                        | gateway::JobPhase::Running
                        | gateway::JobPhase::Sealing
                ),
                "unexpected job phase: {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let mut cursor = None;
    let mut rows = Vec::new();
    loop {
        let operation = Operation::FetchLocalQueryJob {
            job: job.clone(),
            query: query.clone(),
            cursor,
            page_size: 1,
        };
        let reply = call(g, operation.clone()).await?;
        assert_eq!(
            call(g, operation).await?,
            reply,
            "ready job page exact replay"
        );
        let Reply::FetchLocalQueryJob {
            version: actual,
            page,
            ..
        } = reply
        else {
            anyhow::bail!("expected job page");
        };
        assert_eq!(actual, version);
        rows.extend(page.data.rows);
        cursor = page.next;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(rows, expected);
    let Reply::CancelLocalQueryJob { status, .. } =
        call(g, Operation::CancelLocalQueryJob { job: job.clone() }).await?
    else {
        anyhow::bail!("expected cancellation");
    };
    assert_eq!(status.phase, gateway::JobPhase::Cancelled);
    assert!(call(
        g,
        Operation::FetchLocalQueryJob {
            job,
            query: query.clone(),
            cursor: None,
            page_size: 1
        }
    )
    .await
    .is_err());
    Ok(())
}

async fn raw_fixed_checks(
    coordinator: &Process,
    node: &Process,
    version: u64,
) -> anyhow::Result<()> {
    use replicated_graph::remote::distributed::grpc::wire as w;
    let (tcp, grpc) = routes(node).await?;
    let client = coordinator.client()?;
    let proof = d2_raft::managed_rpc::NodeRef::Remote(&client)
        .terminal(&authority(), 1, version)
        .await?;
    let proxy = Proxy::start(grpc.endpoint).await?;
    let mut rpc = w::fixed_query_service_client::FixedQueryServiceClient::connect(format!(
        "http://{}",
        proxy.address
    ))
    .await?;
    let context = || {
        Some(w::Context {
            version: 1,
            identity: Some(w::Identity { group: 7, node: 1 }),
            timeout_millis: 5000,
        })
    };
    let open = w::FixedOpenRequest {
        profile: w::LocalReadProfile::Parity2MibV1 as i32,
        target: Some(w::Target {
            shard: 7,
            epoch: 1,
            mode: vec![7; 16],
        }),
        proof: Some(proof.clone().into()),
        capture_limit: Some(w::ReadLimit {
            rows: 10_000,
            bytes: 2 * 1024 * 1024,
            operations: 100_000,
        }),
        ttl_millis: 60_000,
        acquire_timeout_millis: 5000,
    };
    let renewable = rpc
        .open_renewable(w::RenewableOpenEnvelope {
            context: context(),
            request: Some(w::RenewableFixedOpenRequest {
                open: Some(open.clone()),
                hard_lifetime_millis: 120_000,
            }),
        })
        .await?
        .into_inner();
    let opened = renewable.opened.context("renewable opened")?;
    assert_eq!(
        opened.retained_proof, open.proof,
        "typed full terminal evidence roundtrip"
    );
    let descriptor = opened.descriptor.context("descriptor")?;
    assert_eq!(descriptor.version, version);
    assert_eq!(renewable.grant.context("initial grant")?.generation, 0);
    let renewal = w::FixedRenewEnvelope {
        context: context(),
        request: Some(w::FixedRenewRequest {
            descriptor: Some(descriptor.clone()),
            generation: 1,
            lease_millis: 60_000,
        }),
    };
    let grant = rpc.renew(renewal.clone()).await?.into_inner();
    assert_eq!(grant.generation, 1);
    assert_eq!(grant.granted_millis, 60_000);
    assert_eq!(
        rpc.renew(renewal).await?.into_inner(),
        grant,
        "renewal replay cannot extend or alter original grant"
    );
    let fragment = |descriptor: w::FixedDescriptor, sequence| w::FixedFragmentEnvelope {
        context: context(),
        request: Some(w::FixedFragmentRequest {
            descriptor: Some(descriptor),
            request_sequence: sequence,
            fragment: Some(w::Fragment {
                kind: Some(w::fragment::Kind::Exists(w::VertexId {
                    partition: 7,
                    id: 1,
                })),
            }),
            remaining: Some(w::Cost {
                bytes: 1024 * 1024,
                work: 1000,
            }),
        }),
    };
    // The byte-level gate bounds the pending caller, but could include HTTP/2
    // control frames. Cancellation therefore leaves execution unknown: close
    // this capability instead of guessing whether seq0 was consumed.
    proxy.paused.send(true)?;
    let mut pending_rpc = rpc.clone();
    let pending_request = fragment(descriptor.clone(), 0);
    let pending = tokio::spawn(async move { pending_rpc.fragment_fixed(pending_request).await });
    tokio::time::timeout(BUDGET, proxy.blocked.notified()).await?;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    proxy.paused.send(false)?;
    rpc.close_fixed(w::FixedCloseEnvelope {
        context: context(),
        descriptor: Some(descriptor.clone()),
    })
    .await?;
    assert_eq!(
        rpc.fragment_fixed(fragment(descriptor, 0))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    // Only an actual decoded application reply establishes successful seq0.
    // Exercise duplicate-sequence poisoning on a fresh independent capability.
    let descriptor = rpc
        .open_fixed(w::FixedOpenEnvelope {
            context: context(),
            request: Some(open.clone()),
        })
        .await?
        .into_inner()
        .descriptor
        .context("sequence descriptor")?;
    let output = rpc
        .fragment_fixed(fragment(descriptor.clone(), 0))
        .await?
        .into_inner();
    let Some(w::fixed_fragment_response::Result::Output(output)) = output.result else {
        anyhow::bail!("fresh seq0 failed");
    };
    assert_eq!(output.request_sequence, 0);
    assert_eq!(
        output.value.and_then(|value| value.kind),
        Some(w::fragment_value::Kind::Boolean(true))
    );
    assert_eq!(
        rpc.fragment_fixed(fragment(descriptor.clone(), 0))
            .await?
            .into_inner()
            .result,
        Some(w::fixed_fragment_response::Result::Failure(
            w::QueryFailure::IncompleteOrCorrupt as i32
        )),
        "fixed fragment duplicate poisons instead of replaying"
    );
    assert_eq!(
        rpc.fragment_fixed(fragment(descriptor.clone(), 1))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    rpc.close_fixed(w::FixedCloseEnvelope {
        context: context(),
        descriptor: Some(descriptor),
    })
    .await?;

    // Default-parity profile remains the legacy 2 MiB capture admission. An
    // actual insufficient read limit is a typed D1 error, not Protocol/DataLoss.
    let mut small = open.clone();
    small.capture_limit.as_mut().unwrap().rows = 1;
    let error = rpc
        .open_fixed(w::FixedOpenEnvelope {
            context: context(),
            request: Some(small),
        })
        .await
        .unwrap_err();
    use prost::Message;
    let detail = w::FixedErrorDetail::decode(error.details())?;
    assert!(
        matches!(
            detail.application.and_then(|error| error.kind),
            Some(w::application_error::Kind::VisibilityReadLimit(_))
        ),
        "capture limit must retain typed application error"
    );

    // Local lease expiry needs no 60-second sleep and must not be revived by
    // sending a fresh RPC deadline.
    let mut brief = open.clone();
    brief.ttl_millis = 1000;
    let expired = rpc
        .open_fixed(w::FixedOpenEnvelope {
            context: context(),
            request: Some(brief),
        })
        .await?
        .into_inner()
        .descriptor
        .context("short descriptor")?;
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        rpc.fragment_fixed(fragment(expired, 0))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );

    // gRPC-opened descriptors can be closed through the original TCP service.
    let cross = rpc
        .open_fixed(w::FixedOpenEnvelope {
            context: context(),
            request: Some(open.clone()),
        })
        .await?
        .into_inner()
        .descriptor
        .context("cross descriptor")?;
    let stamp = cross.stamp.as_ref().context("stamp")?;
    let native = serde_json::json!({"stamp": {"identity": tcp.identity, "shard": stamp.shard, "epoch": stamp.epoch, "sequence": stamp.sequence}, "incarnation": cross.incarnation, "token": cross.token, "authority": authority(), "cohort": cohort(), "version": cross.version, "mode": cross.mode});
    let closed = legacy_fixed(
        tcp.endpoint,
        serde_json::json!({"CloseFixedQuery": {"descriptor": native}}),
    )
    .await?;
    assert_eq!(closed["result"]["Ok"], "CloseFixedQuery");
    assert_eq!(
        rpc.fragment_fixed(fragment(cross, 0))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::NotFound
    );
    // All eight slots belong to one process-wide pool, not one pool per adapter.
    let mut held = Vec::new();
    for _ in 0..8 {
        held.push(
            rpc.open_fixed(w::FixedOpenEnvelope {
                context: context(),
                request: Some(open.clone()),
            })
            .await?
            .into_inner()
            .descriptor
            .context("held descriptor")?,
        );
    }
    assert_eq!(
        rpc.open_fixed(w::FixedOpenEnvelope {
            context: context(),
            request: Some(open.clone())
        })
        .await
        .unwrap_err()
        .code(),
        tonic::Code::ResourceExhausted
    );
    let exhausted = legacy_fixed(tcp.endpoint, serde_json::json!({"OpenFixedQuery": {"target": target(7), "proof": proof, "capture_limit": {"rows":10000,"bytes":2097152,"operations":100000}, "ttl_millis":60000,"acquire_timeout_millis":5000}})).await?;
    assert_eq!(exhausted["result"]["Err"], "ResourceExhausted");
    for descriptor in held {
        rpc.close_fixed(w::FixedCloseEnvelope {
            context: context(),
            descriptor: Some(descriptor),
        })
        .await?;
    }
    assert_eq!(
        proxy.connections.load(Ordering::SeqCst),
        1,
        "fixed RPCs reuse one physical HTTP/2 connection"
    );
    Ok(())
}
async fn legacy_fixed(
    endpoint: SocketAddr,
    operation: serde_json::Value,
) -> anyhow::Result<serde_json::Value> {
    let mut stream = TcpStream::connect(endpoint).await?;
    let data = serde_json::to_vec(
        &serde_json::json!({"version":1,"identity":{"group":7,"node":1},"operation":operation}),
    )?;
    stream.write_u32(data.len() as u32).await?;
    stream.write_all(&data).await?;
    let length = stream.read_u32().await? as usize;
    anyhow::ensure!(length <= replicated_graph::remote::MAX_FRAME_BYTES);
    let mut data = vec![0; length];
    stream.read_exact(&mut data).await?;
    Ok(serde_json::from_slice(&data)?)
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
