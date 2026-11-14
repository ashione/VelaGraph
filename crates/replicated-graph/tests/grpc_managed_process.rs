//! Managed coordinator/member typed gRPC acceptance in independent OS processes.
//! Both capacity profiles compare default and explicit typed constructors.
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

// A rejected preface may produce connection-level HTTP/2 control frames, never
// DATA/HEADERS or a legacy length-prefixed JSON success/error response.
fn rejected_preface_controls(mut bytes: &[u8]) -> bool {
    while !bytes.is_empty() {
        if bytes.len() < 9 {
            return false;
        }
        let length = (usize::from(bytes[0]) << 16)
            | (usize::from(bytes[1]) << 8)
            | usize::from(bytes[2]);
        if length > bytes.len() - 9 || bytes[5..9] != [0; 4] {
            return false;
        }
        let payload = &bytes[9..9 + length];
        let valid = match (bytes[3], bytes[4]) {
            (4, 0) => length % 6 == 0, // SETTINGS
            (4, 1) => length == 0,     // SETTINGS acknowledgement
            (7, 0) => length >= 8,     // GOAWAY
            (8, 0) => length == 4 && payload != [0; 4], // WINDOW_UPDATE
            _ => false,
        };
        if !valid {
            return false;
        }
        bytes = &bytes[9 + length..];
    }
    true
}
#[test]
fn rejected_preface_does_not_accept_legacy_success_or_error_replies() {
    for result in [
        serde_json::json!({"Ok": {}}),
        serde_json::json!({"Err": "Protocol"}),
    ] {
        let payload = serde_json::to_vec(&serde_json::json!({
            "version": 6, "identity": {}, "nonce": 1, "operation": 8, "result": result
        }))
        .unwrap();
        let mut response = (payload.len() as u32).to_be_bytes().to_vec();
        response.extend(payload);
        assert!(!rejected_preface_controls(&response));
    }
    assert!(rejected_preface_controls(&[]));
    assert!(rejected_preface_controls(&[0, 0, 0, 4, 0, 0, 0, 0, 0]));
    assert!(!rejected_preface_controls(&[0, 0, 0, 0, 0, 0, 0, 0, 0]));
}

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
async fn managed_grpc_constructors_match_and_retain_identity_after_restart() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(180), async {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let default = scenario(profile, false).await?;
            let grpc = scenario(profile, true).await?;
            assert_eq!(
                grpc, default,
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
    // Both constructors are typed-only now. Probe the removed JSON wire itself,
    // not the default constructor, and require the server to close the stream.
    let before = NodeRef::Remote(&c_client).summary(&authority(), 1).await?;
    let payload = serde_json::to_vec(&serde_json::json!({
        "version": 6, "identity": c_client.identity(), "nonce": 1,
        "operation": { "Summary": { "authority": authority(), "epoch": 1 } }
    }))?;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut legacy = tokio::net::TcpStream::connect(c.address).await?;
    legacy.write_all(&(payload.len() as u32).to_be_bytes()).await?;
    legacy.write_all(&payload).await?;
    let response = tokio::time::timeout(Duration::from_secs(2), async {
        let mut received = Vec::new();
        let mut buffer = [0; 4096];
        loop {
            match legacy.read(&mut buffer).await {
                Ok(0) | Err(_) => break,
                Ok(bytes) => {
                    received.extend_from_slice(&buffer[..bytes]);
                    assert!(received.len() <= 64 * 1024, "legacy peer was not rejected");
                }
            }
        }
        received
    })
    .await
    .context("legacy JSON peer must close, even after HTTP/2 SETTINGS")?;
    assert!(
        rejected_preface_controls(&response),
        "retired JSON peer received a non-control or legacy response: {response:?}"
    );
    assert_eq!(
        NodeRef::Remote(&c_client).summary(&authority(), 1).await?,
        before
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
