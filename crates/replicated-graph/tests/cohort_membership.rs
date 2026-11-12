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

// Full-cohort writes perform serial work across nine members. Use the public
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
    let parallel = Gateway::bind_remote_local_queries_with_parallelism(
        config()?,
        routes,
        "127.0.0.1:0".parse()?,
        GATEWAY_BUDGET,
        8,
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
    anyhow::ensure!(
        read_fixed(&parallel, &b, None, text).await.is_err(),
        "missing ninth member succeeded"
    );
    // Reopen the ninth member's durable state at the original endpoints/identity.
    let last = &mut nodes[9];
    fs::remove_file(last.directory.path().join("ready.json"))?;
    last.child = Process::launch(last.directory.path(), last.shard, last.address, false)?;
    last.ready().await?;
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
