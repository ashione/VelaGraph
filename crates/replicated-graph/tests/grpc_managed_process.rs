//! Managed coordinator/member typed gRPC acceptance in independent OS processes.
//! Both historical capacity profiles are compared with the legacy transport.
//! The public gateway test driver remains TCP; all managed control/read/write
//! hops in the gRPC cohort use explicit gRPC clients, never a TCP fallback.
#![allow(dead_code)]
use anyhow::{anyhow, Context};
use d2_raft::{
    managed_rpc::{activate_byte_bound_member, Client, Identity, NodeRef, Role, Server},
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
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Cursor, Write as _},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::Arc,
    time::Duration,
};
use velagraph::Value;
include!("support/managed_process_fixture.rs");

async fn process(
    shard: u64,
    binding: &Binding,
    grpc: bool,
    members: Vec<(u64, SocketAddr)>,
) -> anyhow::Result<Process> {
    let directory = FixtureDirectory::temporary()?;
    if grpc {
        fs::write(directory.path().join("managed-grpc.request"), b"explicit")?;
    }
    Process::configured(
        shard,
        directory,
        ProcessConfig {
            cohort: binding.cohort.clone(),
            members,
        },
    )
    .await
}
async fn rows(
    gateway: &Gateway,
    binding: &Binding,
    session: &Session,
    text: &str,
) -> anyhow::Result<Vec<QueryRow>> {
    match call_bound(
        gateway,
        binding.clone(),
        Operation::Traverse {
            session: session.clone(),
            query: Query {
                partition: 7,
                text: text.into(),
            },
        },
    )
    .await
    .with_context(|| format!("managed query: {text}"))?
    {
        Reply::Traverse {
            session: returned,
            data,
            ..
        } => {
            assert_eq!(returned, *session);
            assert_eq!(data.completeness, QueryCompleteness::Unverified);
            Ok(data.rows)
        }
        other => anyhow::bail!("unexpected traverse reply: {other:?}"),
    }
}
async fn restart(process: &mut Process) -> anyhow::Result<()> {
    process.stop()?;
    fs::remove_file(process.directory.path().join("ready.json"))?;
    process.child = Process::launch(
        process.directory.path(),
        process.shard,
        process.address,
        false,
    )?;
    process.ready().await
}
fn vertex_write(external: u8, partition: u32, id: u64) -> Write {
    Write {
        external_id: [external; 16],
        mutation: Mutation::CreateVertex {
            partition,
            vertex: VertexData {
                id,
                labels: vec!["person".into()],
                properties: BTreeMap::from([
                    ("name".into(), Value::Text(format!("person-{id}"))),
                    ("integer".into(), Value::Integer(i64::MIN)),
                    ("null".into(), Value::Null),
                    ("real".into(), Value::Real(-0.0)),
                    ("boolean".into(), Value::Boolean(true)),
                ]),
            },
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn managed_grpc_matches_tcp_and_retains_identity_after_restart() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(180), async {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let tcp = scenario(profile, false).await?;
            let grpc = scenario(profile, true).await?;
            assert_eq!(
                grpc, tcp,
                "complete typed values/results differ for {profile:?}"
            );
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
async fn scenario(profile: CapacityProfile, grpc: bool) -> anyhow::Result<Vec<Vec<QueryRow>>> {
    let mut binding = binding();
    binding.cohort.profile = profile;
    let mut a = process(7, &binding, grpc, vec![]).await?;
    let mut b = process(8, &binding, grpc, vec![]).await?;
    let mut c = process(9, &binding, grpc, vec![(7, a.address), (8, b.address)]).await?;
    assert_eq!(
        BTreeSet::from([a.child.id(), b.child.id(), c.child.id(), std::process::id()]).len(),
        4
    );
    let c_client = c.client()?;
    let a_client = a.client()?;
    let b_client = b.client()?;
    let config = || RemoteConfig {
        binding: binding.clone(),
        coordinator: c_client.clone(),
        members: vec![a_client.clone(), b_client.clone()],
    };
    let g = Gateway::bind_remote(config(), "127.0.0.1:0".parse()?, BUDGET).await?;
    let wrong_constructor = if grpc { Client::new } else { Client::new_grpc };
    let wrong_transport = wrong_constructor(
        c.address,
        c_client.identity().clone(),
        Duration::from_millis(200),
    )?;
    assert!(
        NodeRef::Remote(&wrong_transport)
            .summary(&authority(), 1)
            .await
            .is_err(),
        "transport must not fallback/negotiate"
    );
    let mut wrong_identity = c_client.identity().clone();
    wrong_identity.node += 1;
    let constructor = if grpc { Client::new_grpc } else { Client::new };
    let wrong = constructor(c.address, wrong_identity, BUDGET)?;
    assert_eq!(
        NodeRef::Remote(&wrong).summary(&authority(), 1).await,
        Err(d2_raft::managed_rpc::Error::Identity)
    );
    if profile == CapacityProfile::ByteBound16MiBV1 {
        let grants = NodeRef::Remote(&c_client)
            .confirm_capacity(&authority(), 1)
            .await?;
        assert_eq!(grants.len(), 2);
        assert_eq!(
            NodeRef::Remote(&c_client)
                .confirm_capacity(&authority(), 1)
                .await?,
            grants
        );
    }
    let first = vertex_write(1, 7, 1);
    let first_reply = call_bound(&g, binding.clone(), Operation::Submit(first.clone())).await?;
    assert_eq!(committed(first_reply.clone()).1, 1);
    assert_eq!(
        call_bound(&g, binding.clone(), Operation::Submit(first.clone())).await?,
        first_reply
    );
    assert_eq!(
        call_bound(&g, binding.clone(), Operation::Status(first.clone())).await?,
        first_reply
    );
    let mut conflict = first.clone();
    conflict.mutation = vertex_write(1, 7, 99).mutation;
    assert_eq!(
        call_bound(&g, binding.clone(), Operation::Status(conflict.clone())).await,
        Err(gateway::Error::Identity)
    );
    assert_eq!(
        call_bound(&g, binding.clone(), Operation::Submit(conflict)).await,
        Err(gateway::Error::Identity)
    );
    let old = open_bound(&g, binding.clone(), Some(1)).await?;
    assert_eq!(
        rows(&g, &binding, &old, "g.V(1).out('knows').count()").await?,
        vec![QueryRow::Count(0)]
    );
    committed(
        call_bound(
            &g,
            binding.clone(),
            Operation::Submit(vertex_write(2, 8, 2)),
        )
        .await?,
    );
    let edge = Write {
        external_id: [3; 16],
        mutation: Mutation::CreateEdge {
            edge: EdgeData {
                id: 10,
                from: Entity {
                    partition: 7,
                    id: 1,
                },
                to: Entity {
                    partition: 8,
                    id: 2,
                },
                label: "knows".into(),
                properties: BTreeMap::new(),
            },
        },
    };
    let edge_reply = call_bound(&g, binding.clone(), Operation::Submit(edge.clone())).await?;
    assert_eq!(committed(edge_reply.clone()).1, 3);
    assert_eq!(
        rows(&g, &binding, &old, "g.V(1).out('knows').count()").await?,
        vec![QueryRow::Count(0)],
        "old V changed"
    );
    let current = open_bound(&g, binding.clone(), None).await?;
    assert_eq!(current.version, 3);
    let expected = vec![QueryRow::Path(vec![
        QualifiedElement::Vertex {
            partition: 7,
            id: 1,
        },
        QualifiedElement::Edge {
            partition: 7,
            id: 10,
        },
        QualifiedElement::Vertex {
            partition: 8,
            id: 2,
        },
    ])];
    assert_eq!(
        rows(&g, &binding, &current, "g.V(1).out('knows').path()").await?,
        expected
    );
    let mut output = Vec::new();
    for query in [
        "g.V(1).out('knows').path()",
        "g.V(1).out('knows').valueMap('name','integer','null','real','boolean')",
        "g.V(1).out('knows').count()",
        "g.V(999).count()",
    ] {
        output.push(rows(&g, &binding, &current, query).await?);
    }
    let original = NodeRef::Remote(&c_client)
        .request(&authority(), 1, edge.external_id)
        .await?;
    assert!(original.record.is_some());
    call_bound(&g, binding.clone(), Operation::Close { session: old }).await?;
    call_bound(&g, binding.clone(), Operation::Close { session: current }).await?;
    g.shutdown().await;
    restart(&mut a).await?;
    restart(&mut b).await?;
    restart(&mut c).await?;
    // Reuse the original clients/channels after the remote processes restart.
    // Original immutable request identity still drives recovery; reconnect does
    // not replay application writes or acquire a new identity.
    let reopened = Gateway::bind_remote(
        RemoteConfig {
            binding: binding.clone(),
            coordinator: c_client.clone(),
            members: vec![a_client.clone(), b_client.clone()],
        },
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    assert_eq!(
        call_bound(&reopened, binding.clone(), Operation::Status(edge.clone())).await?,
        edge_reply
    );
    assert_eq!(
        call_bound(&reopened, binding.clone(), Operation::Submit(edge.clone())).await?,
        edge_reply
    );
    assert_eq!(
        NodeRef::Remote(&c_client)
            .request(&authority(), 1, edge.external_id)
            .await?,
        original,
        "original canonical command/Plan changed after restart"
    );
    let recovered = open_bound(&reopened, binding.clone(), Some(3)).await?;
    assert_eq!(
        rows(
            &reopened,
            &binding,
            &recovered,
            "g.V(1).out('knows').path()"
        )
        .await?,
        expected
    );
    call_bound(&reopened, binding, Operation::Close { session: recovered }).await?;
    reopened.shutdown().await;
    Ok(output)
}
