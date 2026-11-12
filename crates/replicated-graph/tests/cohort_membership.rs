//! Nine data processes plus a coordinator: fixed-V membership is not lane width.
// This target uses a subset of the shared subprocess fixture helpers.
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
    remote::query::{QualifiedElement, QueryCompleteness, QueryRow},
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

// Reuse process/snapshot mechanics, never another target's test scenarios.
include!("support/managed_process_fixture.rs");

// Capture and decision still perform serial work. Use the public
// gateway's existing maximum deployment/request deadline, not a production change.
const GATEWAY_BUDGET: Duration = Duration::from_secs(60);
async fn call_wide(g: &Gateway, b: Binding, operation: Operation) -> gateway::Result<Reply> {
    gateway::request(g.local_addr(), b, operation, GATEWAY_BUDGET).await
}

async fn graph_route(p: &Process) -> anyhow::Result<gateway::LocalGraphRoute> {
    fs::write(p.directory.path().join("graph.request"), b"start")?;
    let endpoint = tokio::time::timeout(BUDGET, async {
        loop {
            if let Ok(bytes) = fs::read(p.directory.path().join("graph-ready.json")) {
                let (pid, endpoint): (u32, SocketAddr) = serde_json::from_slice(&bytes)?;
                if pid == p.child.id() {
                    return Ok::<_, anyhow::Error>(endpoint);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(gateway::LocalGraphRoute {
        endpoint,
        identity: replicated_graph::remote::RemoteIdentity {
            group: p.shard,
            node: 1,
        },
        managed_identity: identity_in(p.shard, p.config.cohort.clone()),
    })
}
async fn read_fixed(
    g: &Gateway,
    b: &Binding,
    version: Option<u64>,
    text: &str,
) -> anyhow::Result<(gateway::LocalSession, Vec<QueryRow>)> {
    let reply = call_wide(
        g,
        b.clone(),
        Operation::OpenLocalQuery {
            version,
            ttl_millis: 60000,
            query: Query {
                partition: 20,
                text: text.into(),
            },
            page_size: 100,
        },
    )
    .await?;
    match reply {
        Reply::OpenLocalQuery { session, page, .. } => {
            anyhow::ensure!(page.next.is_none(), "fixture must fit one complete page");
            Ok((session, page.data.rows))
        }
        other => anyhow::bail!("unexpected {other:?}"),
    }
}
async fn close_fixed(
    g: &Gateway,
    b: &Binding,
    session: gateway::LocalSession,
) -> anyhow::Result<()> {
    call_wide(g, b.clone(), Operation::CloseLocalQuery { session }).await?;
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nine_data_processes_preserve_fixed_v_order_and_dynamic_writes() -> anyhow::Result<()> {
    let _phase = RETAINED_FIXTURE_PHASE.lock().await;
    tokio::time::timeout(Duration::from_secs(300), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let cohort = Cohort {
        members: (20..29)
            .map(|shard| Member {
                shard,
                epoch: 1,
                mode: [shard as u8; 16],
            })
            .collect(),
        ..cohort()
    };
    let b = Binding {
        cohort: cohort.clone(),
        ..binding()
    };
    let mut nodes = Vec::new();
    for shard in std::iter::once(9).chain(20..29) {
        nodes.push(
            Process::configured(
                shard,
                FixtureDirectory::temporary()?,
                ProcessConfig {
                    cohort: cohort.clone(),
                    members: Vec::new(),
                },
            )
            .await?,
        );
    }
    let mut routes = Vec::new();
    for node in &nodes[1..] {
        routes.push(graph_route(node).await?);
    }
    let config = || -> anyhow::Result<RemoteConfig> {
        Ok(RemoteConfig {
            coordinator: nodes[0].client()?,
            binding: b.clone(),
            members: nodes[1..]
                .iter()
                .map(Process::client)
                .collect::<Result<_, _>>()?,
        })
    };
    let serial = Gateway::bind_remote_local_queries_with_parallelism(
        config()?,
        routes.clone(),
        "127.0.0.1:0".parse()?,
        GATEWAY_BUDGET,
        1,
    )
    .await?;
    let parallel = Gateway::bind_remote_local_queries_with_concurrency(
        config()?,
        routes,
        "127.0.0.1:0".parse()?,
        GATEWAY_BUDGET,
        8,
        4,
    )
    .await?;
    let mut ordinal = 0;
    for shard in 20..29 {
        eprintln!("membership write {} shard {shard}", ordinal + 1);
        ordinal += 1;
        committed(
            call_wide(
                &serial,
                b.clone(),
                Operation::Submit(Write {
                    external_id: [ordinal; 16],
                    mutation: Mutation::CreateVertex {
                        partition: shard,
                        vertex: VertexData {
                            id: 1,
                            labels: vec!["person".into()],
                            properties: BTreeMap::new(),
                        },
                    },
                }),
            )
            .await?,
        );
    }
    for shard in 20..29 {
        eprintln!("membership write {} shard {shard}", ordinal + 1);
        ordinal += 1;
        committed(
            call_wide(
                &serial,
                b.clone(),
                Operation::Submit(Write {
                    external_id: [ordinal; 16],
                    mutation: Mutation::CreateEdge {
                        edge: EdgeData {
                            id: shard as u64,
                            from: Entity {
                                partition: 20,
                                id: 1,
                            },
                            to: Entity {
                                partition: shard,
                                id: 1,
                            },
                            label: "links".into(),
                            properties: BTreeMap::new(),
                        },
                    },
                }),
            )
            .await?,
        );
    }
    // Two roots preserve duplicate bag occurrences; target checks cover all nine shards.
    let text = "g.V(1,1).out().path()";
    let expected: Vec<_> = (0..2)
        .flat_map(|_| {
            (20..29).map(|shard| {
                QueryRow::Path(vec![
                    QualifiedElement::Vertex {
                        partition: 20,
                        id: 1,
                    },
                    QualifiedElement::Edge {
                        partition: 20,
                        id: shard as u64,
                    },
                    QualifiedElement::Vertex {
                        partition: shard,
                        id: 1,
                    },
                ])
            })
        })
        .collect();
    eprintln!("membership fixed reads");
    let (old, one) = read_fixed(&serial, &b, None, text).await?;
    anyhow::ensure!(old.version == 18 && one == expected, "serial ordered bag");
    let (many, eight) = read_fixed(&parallel, &b, Some(old.version), text).await?;
    anyhow::ensure!(
        many.version == old.version && eight == expected,
        "eight-lane equivalence"
    );
    close_fixed(&parallel, &b, many).await?;
    // A later committed write changes only new V, including the ninth data member.
    ordinal += 1;
    committed(
        call_wide(
            &serial,
            b.clone(),
            Operation::Submit(Write {
                external_id: [ordinal; 16],
                mutation: Mutation::CreateEdge {
                    edge: EdgeData {
                        id: 99,
                        from: Entity {
                            partition: 20,
                            id: 1,
                        },
                        to: Entity {
                            partition: 28,
                            id: 1,
                        },
                        label: "links".into(),
                        properties: BTreeMap::new(),
                    },
                },
            }),
        )
        .await?,
    );
    let (historical, rows) = read_fixed(&parallel, &b, Some(old.version), text).await?;
    anyhow::ensure!(rows == expected, "historical V changed after write");
    close_fixed(&parallel, &b, historical).await?;
    let (latest, rows) = read_fixed(&parallel, &b, None, "g.V(1).out().count()").await?;
    anyhow::ensure!(
        latest.version == 19 && rows == vec![QueryRow::Count(10)],
        "new V visibility"
    );
    close_fixed(&parallel, &b, latest).await?;
    close_fixed(&serial, &b, old).await?;
    // Jobs use the same wider fixed cohort, without relaxing their lease or result bounds.
    let job_query = Query {
        partition: 20,
        text: text.into(),
    };
    let start = Operation::StartLocalQueryJob {
        operation_id: [100; 16],
        version: Some(18),
        query: job_query.clone(),
        page_size: 100,
        policy: gateway::JobPolicy {
            idle_millis: 30000,
            execution_millis: 60000,
            lifetime_millis: 90000,
        },
    };
    let reply = call_wide(&parallel, b.clone(), start.clone()).await?;
    anyhow::ensure!(
        call_wide(&parallel, b.clone(), start).await? == reply,
        "job admission replay"
    );
    let Reply::StartLocalQueryJob { job, .. } = reply else {
        anyhow::bail!("expected job");
    };
    tokio::time::timeout(BUDGET, async {
        loop {
            let Reply::GetLocalQueryJob { status, .. } = call_wide(
                &parallel,
                b.clone(),
                Operation::GetLocalQueryJob { job: job.clone() },
            )
            .await?
            else {
                anyhow::bail!("status");
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
                "job failed {status:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await??;
    let fetch = Operation::FetchLocalQueryJob {
        job: job.clone(),
        query: job_query,
        cursor: None,
        page_size: 100,
    };
    let reply = call_wide(&parallel, b.clone(), fetch.clone()).await?;
    anyhow::ensure!(
        call_wide(&parallel, b.clone(), fetch).await? == reply,
        "job result replay"
    );
    let Reply::FetchLocalQueryJob { version, page, .. } = reply else {
        anyhow::bail!("fetch");
    };
    anyhow::ensure!(
        version == 18 && page.data.rows == expected && page.next.is_none(),
        "job fixed-V ordered bag"
    );
    call_wide(&parallel, b.clone(), Operation::CancelLocalQueryJob { job }).await?;
    // All-member certification is required, including members beyond the old cap.
    nodes[9].stop()?;
    let original = Write {
        external_id: [1; 16],
        mutation: Mutation::CreateVertex {
            partition: 20,
            vertex: VertexData {
                id: 1,
                labels: vec!["person".into()],
                properties: BTreeMap::new(),
            },
        },
    };
    let unavailable = call_wide(&parallel, b.clone(), Operation::Status(original.clone())).await?;
    anyhow::ensure!(
        matches!(
            unavailable,
            Reply::Write {
                status: WriteStatus::Committed {
                    ordinal: 1,
                    visible: false
                },
                ..
            }
        ),
        "late unavailable member must not certify visibility: {unavailable:?}"
    );
    anyhow::ensure!(
        read_fixed(&parallel, &b, None, text).await.is_err(),
        "missing ninth member succeeded"
    );
    // Reopen the ninth member's durable state at the original endpoints/identity.
    let last = &mut nodes[9];
    fs::remove_file(last.directory.path().join("ready.json"))?;
    last.child = Process::launch(last.directory.path(), last.shard, last.address, false)?;
    last.ready().await?;
    committed(call_wide(&parallel, b.clone(), Operation::Submit(original)).await?);
    // Its graph port is new after restart; rebuild explicit routes rather than infer/fail over.
    let mut routes = Vec::new();
    for node in &nodes[1..] {
        routes.push(graph_route(node).await?);
    }
    let restarted = Gateway::bind_remote_local_queries_with_parallelism(
        RemoteConfig {
            coordinator: nodes[0].client()?,
            binding: b.clone(),
            members: nodes[1..]
                .iter()
                .map(Process::client)
                .collect::<Result<_, _>>()?,
        },
        routes,
        "127.0.0.1:0".parse()?,
        GATEWAY_BUDGET,
        3,
    )
    .await?;
    let (session, rows) = read_fixed(&restarted, &b, Some(18), text).await?;
    anyhow::ensure!(
        session.version == 18 && rows == expected,
        "reopened member changed historical V"
    );
    close_fixed(&restarted, &b, session).await?;
    restarted.shutdown().await;
    serial.shutdown().await;
    parallel.shutdown().await;
    Ok(())
}

/// Reproducible process-local attribution, not a throughput or large-data claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit exclusive multi-configuration process benchmark"]
async fn cohort_submission_attribution() -> anyhow::Result<()> {
    use replicated_graph::managed_submission_timing::Phase;
    let width: usize = std::env::var("VELAGRAPH_BENCH_MEMBER_WIDTH")
        .unwrap_or_else(|_| "3".into())
        .parse()?;
    anyhow::ensure!((1..=4).contains(&width));
    let _phase = RETAINED_FIXTURE_PHASE.lock().await;
    for count in [3u64, 9, 12] {
        let cohort = Cohort {
            members: (20..20 + count)
                .map(|shard| Member {
                    shard,
                    epoch: 1,
                    mode: [shard as u8; 16],
                })
                .collect(),
            ..cohort()
        };
        let b = Binding {
            cohort: cohort.clone(),
            ..binding()
        };
        let mut nodes = Vec::new();
        for shard in std::iter::once(9).chain(20..20 + count) {
            nodes.push(
                Process::configured(
                    shard,
                    FixtureDirectory::temporary()?,
                    ProcessConfig {
                        cohort: cohort.clone(),
                        members: Vec::new(),
                    },
                )
                .await?,
            );
        }
        let (g, reports) = Gateway::bind_remote_with_member_parallelism_and_timing(
            RemoteConfig {
                coordinator: nodes[0].client()?,
                binding: b.clone(),
                members: nodes[1..]
                    .iter()
                    .map(Process::client)
                    .collect::<Result<_, _>>()?,
            },
            "127.0.0.1:0".parse()?,
            GATEWAY_BUDGET,
            width,
        )
        .await?;
        for index in 1..=12u8 {
            committed(
                call_wide(
                    &g,
                    b.clone(),
                    Operation::Submit(Write {
                        external_id: [index; 16],
                        mutation: Mutation::CreateVertex {
                            partition: (20 + u64::from(index - 1) % count) as u32,
                            vertex: VertexData {
                                id: u64::from(index),
                                labels: vec!["benchmark".into()],
                                properties: BTreeMap::new(),
                            },
                        },
                    }),
                )
                .await?,
            );
            let r = reports.try_recv()?;
            eprintln!(
                "COHORT_BENCH {}",
                serde_json::json!({
                    "members":count,"member_width":width,"write":index,"wall_us":r.elapsed.as_micros(),
                    "snapshot_us":r.phases[Phase::Snapshot as usize].elapsed.as_micros(),
                    "decision_us":r.phases[Phase::Decision as usize].elapsed.as_micros(),
                    "cohort_us":r.phases[Phase::Cohort as usize].elapsed.as_micros(),
                    "history_sum_us":r.phases[Phase::HistorySync as usize].elapsed.as_micros(),
                    "consume_sum_us":r.phases[Phase::Consume as usize].elapsed.as_micros(),
                    "visibility_sum_us":r.phases[Phase::Visibility as usize].elapsed.as_micros(),
                })
            );
        }
        g.shutdown().await;
        drop(nodes);
    }
    Ok(())
}

/// Competing requests remain concurrent inside each isolated process fixture.
/// This is a fairness/overload acceptance workload, not a throughput qualification.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "exclusive multi-request contention acceptance"]
async fn cohort_multi_request_contention() -> anyhow::Result<()> {
    let _fixture = RETAINED_FIXTURE_PHASE.lock().await;
    for width in [1, 3, 4] {
        for shared_gateway in [true, false] {
            tokio::time::timeout(
                Duration::from_secs(180),
                contention_case(width, shared_gateway),
            )
            .await??;
        }
    }
    Ok(())
}
async fn contention_case(width: usize, shared_gateway: bool) -> anyhow::Result<()> {
    use std::sync::atomic::{AtomicBool, Ordering};
    let cohort = Cohort {
        members: (20..29)
            .map(|shard| Member {
                shard,
                epoch: 1,
                mode: [shard as u8; 16],
            })
            .collect(),
        ..cohort()
    };
    let b = Binding {
        cohort: cohort.clone(),
        ..binding()
    };
    let mut nodes = Vec::new();
    for shard in std::iter::once(9).chain(20..29) {
        nodes.push(
            Process::configured(
                shard,
                FixtureDirectory::temporary()?,
                ProcessConfig {
                    cohort: cohort.clone(),
                    members: Vec::new(),
                },
            )
            .await?,
        );
    }
    let config = || -> anyhow::Result<RemoteConfig> {
        Ok(RemoteConfig {
            coordinator: nodes[0].client()?,
            binding: b.clone(),
            members: nodes[1..]
                .iter()
                .map(Process::client)
                .collect::<Result<_, _>>()?,
        })
    };
    let writer = Gateway::bind_remote_with_member_parallelism(
        config()?,
        "127.0.0.1:0".parse()?,
        GATEWAY_BUDGET,
        width,
    )
    .await?;
    let reader = if shared_gateway {
        None
    } else {
        Some(
            Gateway::bind_remote_with_member_parallelism(
                config()?,
                "127.0.0.1:0".parse()?,
                GATEWAY_BUDGET,
                width,
            )
            .await?,
        )
    };
    let read_address = reader.as_ref().unwrap_or(&writer).local_addr();
    let mutation = |id: u8| Write {
        external_id: [id; 16],
        mutation: Mutation::CreateVertex {
            partition: 20,
            vertex: VertexData {
                id: u64::from(id),
                labels: vec!["contention".into()],
                properties: BTreeMap::new(),
            },
        },
    };
    let original = mutation(1);
    committed(call_wide(&writer, b.clone(), Operation::Submit(original.clone())).await?);
    let done = AtomicBool::new(false);
    let observing = AtomicBool::new(true);
    let start = tokio::sync::Barrier::new(2);
    let writes = async {
        start.wait().await;
        let mut latencies = Vec::new();
        let mut overlap = 0;
        for id in 2..=5 {
            let started = std::time::Instant::now();
            committed(call_wide(&writer, b.clone(), Operation::Submit(mutation(id))).await?);
            latencies.push(started.elapsed().as_micros());
            overlap += usize::from(observing.load(Ordering::Acquire));
        }
        done.store(true, Ordering::Release);
        Ok::<_, anyhow::Error>((latencies, overlap))
    };
    let observations = async {
        start.wait().await;
        let mut latencies = Vec::new();
        for _ in 0..256 {
            let started = std::time::Instant::now();
            let result = gateway::request(
                read_address,
                b.clone(),
                Operation::Status(original.clone()),
                GATEWAY_BUDGET,
            )
            .await?;
            anyhow::ensure!(
                matches!(
                    result,
                    Reply::Write {
                        status: WriteStatus::Committed {
                            ordinal: 1,
                            visible: true
                        },
                        ..
                    }
                ),
                "bounded two-request workload failed certification: {result:?}"
            );
            latencies.push(started.elapsed().as_micros());
            if done.load(Ordering::Acquire) {
                break;
            }
        }
        anyhow::ensure!(
            done.load(Ordering::Acquire),
            "Status cap reached before writer completed: possible starvation"
        );
        observing.store(false, Ordering::Release);
        Ok::<_, anyhow::Error>(latencies)
    };
    let ((writes, overlap), observations) = tokio::try_join!(writes, observations)?;
    anyhow::ensure!(
        overlap == 4 && !observations.is_empty(),
        "every write must complete while Status load remains active"
    );
    // Deliberately exceed the admitted RPC envelope. Queue-full/connection
    // admission can reject, but never invent Abort or change the original ID.
    let overloaded = mutation(6);
    let mut flood = tokio::task::JoinSet::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(9));
    for _ in 0..8 {
        let binding = b.clone();
        let original = original.clone();
        let barrier = barrier.clone();
        flood.spawn(async move {
            barrier.wait().await;
            gateway::request(
                read_address,
                binding,
                Operation::Status(original),
                GATEWAY_BUDGET,
            )
            .await
        });
    }
    barrier.wait().await;
    let initial = call_wide(&writer, b.clone(), Operation::Submit(overloaded.clone())).await;
    let initial_visible = match initial {
        Ok(Reply::Write {
            status:
                WriteStatus::Committed {
                    ordinal: 6,
                    visible,
                },
            ..
        }) => Some(visible),
        Err(gateway::Error::Capacity | gateway::Error::Unknown) => None,
        other => anyhow::bail!("overload changed decision semantics: {other:?}"),
    };
    let (mut flood_visible, mut flood_incomplete, mut flood_rejected) = (0, 0, 0);
    while let Some(result) = flood.join_next().await {
        match result? {
            Ok(Reply::Write {
                status:
                    WriteStatus::Committed {
                        ordinal: 1,
                        visible: true,
                    },
                ..
            }) => flood_visible += 1,
            Ok(Reply::Write {
                status:
                    WriteStatus::Committed {
                        ordinal: 1,
                        visible: false,
                    },
                ..
            }) => flood_incomplete += 1,
            Err(gateway::Error::Capacity | gateway::Error::Unknown) => flood_rejected += 1,
            other => anyhow::bail!("overload fabricated status: {other:?}"),
        }
    }
    let (_, recovered) =
        committed(call_wide(&writer, b.clone(), Operation::Submit(overloaded)).await?);
    anyhow::ensure!(recovered == 6, "overload retry created another transaction");
    let Reply::OpenSession(session) = call_wide(
        &writer,
        b.clone(),
        Operation::OpenSession {
            version: None,
            ttl_millis: 60_000,
        },
    )
    .await?
    else {
        anyhow::bail!("missing latest snapshot");
    };
    anyhow::ensure!(
        session.version == 6,
        "status load changed write identity or final prefix"
    );
    call_wide(&writer, b.clone(), Operation::Close { session }).await?;
    eprintln!(
        "COHORT_LOAD {}",
        serde_json::json!({
            "members":9,"member_width":width,"shared_gateway":shared_gateway,
            "visible_writes":writes.len(),"writes_while_observing":overlap,
            "write_wall_us":writes,"status_wall_us":observations,
        "overload_initial_visible":initial_visible,"overload_recovered_ordinal":recovered,
        "overload_status_visible":flood_visible,"overload_status_incomplete":flood_incomplete,
        "overload_status_rejected":flood_rejected,
            "active_rpc_limit":4,"additional_admitted_rpc_limit":d2_raft::managed_rpc::MAX_OUTBOUND_WAITERS,
            "total_admitted_rpc_limit":4+d2_raft::managed_rpc::MAX_OUTBOUND_WAITERS,
            "interpretation":"single fresh-cohort controlled contention run; limits are configuration, not measured RSS or throughput"
        })
    );
    writer.shutdown().await;
    if let Some(reader) = reader {
        reader.shutdown().await;
    }
    Ok(())
}
