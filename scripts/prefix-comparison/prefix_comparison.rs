//! Read-mostly measurement on disposable copies; only submits original ID1 retry.
use anyhow::{anyhow, Context};
use d2_raft::{
    managed_rpc::{Client, Identity, NodeRef, Role, Server},
    participant_writes::Authority,
    transaction_decisions::{Cohort, Journal, Member},
    versioned_reads::{ReadLimit, Target},
    Cluster,
};
use replicated_graph::{
    managed_gateway::{
        self as gateway, Binding, Gateway, Mapping, Mutation, Operation, RemoteConfig, Reply,
        VertexData, Write, WriteStatus,
    },
    managed_graph::{GraphCohort, GraphSnapshot},
};
use velagraph::{Value, Vertex};
use std::{
    fs,
    net::SocketAddr,
    path::Path,
    process::{Child, Command, Stdio},
    sync::Arc,
    time::{Duration, Instant},
};
const BUDGET: Duration = Duration::from_secs(30);
const CHILD_ENV: &str = "PREFIX_COMPARISON_DIRECTORY";
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn cohort() -> Cohort {
    Cohort {
        coordinator_shard: 9,
        coordinator_epoch: 1,
        members: [1, 2, 3]
            .map(|shard| Member {
                shard,
                epoch: 1,
                mode: [shard as u8; 16],
            })
            .to_vec(),
    }
}
fn target(shard: u64) -> Target {
    Target {
        shard,
        epoch: 1,
        mode: [shard as u8; 16],
    }
}
fn identity(shard: u64) -> Identity {
    Identity {
        group: shard,
        node: 1,
        target: target(shard),
        role: if shard == 9 {
            Role::Coordinator
        } else {
            Role::Participant
        },
        authority: authority(),
        cohort: cohort(),
    }
}
fn binding() -> Binding {
    Binding {
        authority: authority(),
        cohort: cohort(),
        mapping: Mapping::PartitionEqualsShardV1,
    }
}
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
fn fixture_vertex(id: u64) -> Vertex {
    let mut v = Vertex::new(id);
    v.labels.push("person".into());
    v.properties.insert(
        "active".into(),
        Value::Boolean(!mix(id ^ 0x414354495645).is_multiple_of(20)),
    );
    v.properties.insert(
        "country".into(),
        Value::Text(
            ["CN", "US", "DE", "SG", "GB"][(mix(id ^ 0x434f554e545259) % 5) as usize].into(),
        ),
    );
    v.properties
        .insert("name".into(), Value::Text(format!("Employee-{id:06}")));
    v.properties
        .insert("multi_department".into(), Value::Boolean(id % 5 == 0));
    v
}
fn write(id: u64) -> Write {
    Write {
        external_id: (id as u128).to_le_bytes(),
        mutation: Mutation::CreateVertex {
            partition: 1 + (id % 3) as u32,
            vertex: VertexData::from(fixture_vertex(id)),
        },
    }
}
fn committed(reply: Reply, expected: u64) {
    match reply {
        Reply::Write {
            status:
                WriteStatus::Committed {
                    ordinal,
                    visible: true,
                },
            ..
        } => assert_eq!(ordinal, expected),
        other => panic!("expected visible Commit: {other:?}"),
    }
}
fn limits() -> ReadLimit {
    ReadLimit {
        rows: 10000,
        bytes: 2 * 1024 * 1024,
        operations: 100000,
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "private child entrypoint"]
async fn prefix_node_child() -> anyhow::Result<()> {
    let directory =
        std::path::PathBuf::from(std::env::var_os(CHILD_ENV).context("child directory")?);
    let shard: u64 = std::env::var("PREFIX_COMPARISON_SHARD")?.parse()?;
    let address: SocketAddr = std::env::var("PREFIX_COMPARISON_ADDRESS")?.parse()?;
    let node = Arc::new(
        Cluster::new()
            .in_group(shard)
            .start_durable(1, directory.join("data"))
            .await?,
    );
    tokio::time::timeout(BUDGET, async {
        while node.raft.ensure_linearizable().await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    let server = Server::bind(node, identity(shard), address, BUDGET).await?;
    fs::write(
        directory.join("ready.tmp"),
        serde_json::to_vec(&(std::process::id(), server.local_addr()))?,
    )?;
    fs::rename(directory.join("ready.tmp"), directory.join("ready.json"))?;
    std::future::pending::<()>().await;
    Ok(())
}
fn copy_tree(source: &Path, dest: &Path) -> anyhow::Result<()> {
    fs::create_dir(dest)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        anyhow::ensure!(
            !kind.is_symlink(),
            "fixture must contain regular files/directories only"
        );
        let out = dest.join(entry.file_name());
        if kind.is_dir() {
            copy_tree(&entry.path(), &out)?;
        } else {
            anyhow::ensure!(kind.is_file(), "unexpected fixture file type");
            fs::copy(entry.path(), &out)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let a = entry.metadata()?;
                let b = out.metadata()?;
                anyhow::ensure!(
                    (a.dev(), a.ino()) != (b.dev(), b.ino()),
                    "hardlink forbidden"
                );
            }
        }
    }
    Ok(())
}
struct Process {
    child: Child,
    directory: tempfile::TempDir,
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Process {
    async fn start(shard: u64) -> anyhow::Result<(Self, Client)> {
        let directory = tempfile::Builder::new()
            .prefix("prefix-comparison-")
            .tempdir_in("/tmp")?;
        copy_tree(
            Path::new(&format!("/tmp/managed-scale-backup-{shard}/data")),
            &directory.path().join("data"),
        )?;
        let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = reservation.local_addr()?;
        drop(reservation);
        let child = Command::new(std::env::current_exe()?)
            .args(["--ignored", "--exact", "prefix_node_child", "--nocapture"])
            .env(CHILD_ENV, directory.path())
            .env("PREFIX_COMPARISON_SHARD", shard.to_string())
            .env("PREFIX_COMPARISON_ADDRESS", address.to_string())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?;
        let mut process = Self { child, directory };
        tokio::time::timeout(BUDGET, async {
            loop {
                anyhow::ensure!(
                    process.child.try_wait()?.is_none(),
                    "child exited before ready"
                );
                if let Ok(bytes) = fs::read(process.directory.path().join("ready.json")) {
                    let (pid, addr): (u32, SocketAddr) = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        pid == process.child.id() && addr == address,
                        "wrong child identity"
                    );
                    return Ok::<(), anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        Ok((process, Client::new(address, identity(shard), BUDGET)?))
    }
}
fn verify(snapshot: &GraphSnapshot, version: u64) -> anyhow::Result<()> {
    assert_eq!(snapshot.version()?, version);
    for i in 1..=version {
        assert_eq!(
            snapshot.vertex(velagraph::experimental_graph::VertexRef {
                partition: 1 + (i % 3) as u32,
                id: i
            })?,
            Some(fixture_vertex(i))
        );
    }
    let i = version + 1;
    assert_eq!(
        snapshot.vertex(velagraph::experimental_graph::VertexRef {
            partition: 1 + (i % 3) as u32,
            id: i
        })?,
        None
    );
    Ok(())
}
fn encoded(journal: &Journal) -> anyhow::Result<Vec<u8>> {
    journal
        .encode()
        .map_err(|e| anyhow!("journal encode: {e:?}"))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "explicit disposable stopped-fixture comparison"]
async fn prefix_comparison() -> anyhow::Result<()> {
    let label = std::env::var("PREFIX_COMPARISON_LABEL").context("measurement label")?;
    let output = std::env::var("PREFIX_COMPARISON_OUTPUT").context("output JSON path")?;
    let (_c, coordinator) = Process::start(9).await?;
    let (_a, a) = Process::start(1).await?;
    let (_b, b) = Process::start(2).await?;
    let (_d, d) = Process::start(3).await?;
    let clients = vec![a, b, d];
    let endpoints = || {
        clients
            .iter()
            .enumerate()
            .map(|(i, c)| (NodeRef::Remote(c), target(i as u64 + 1)))
            .collect()
    };
    let graph =
        GraphCohort::from_endpoints(NodeRef::Remote(&coordinator), authority(), 1, endpoints());
    let mut samples = Vec::new();
    // All diagnostic full-Journal calls are outside every capture timer.
    let before = NodeRef::Remote(&coordinator)
        .journal(&authority(), 1)
        .await?;
    assert_eq!(before.transaction_count(), 149);
    assert_eq!(before.resolved_prefix(), 149);
    let before_bytes = encoded(&before)?;
    for i in 1..=149 {
        let w = write(i);
        let record = before
            .request(w.external_id)
            .context("missing external ID")?;
        assert_eq!(
            record.request,
            serde_json::to_vec(&("managed-graph-command-v1", binding(), w.mutation))?
        );
        assert_eq!(before.transaction_at(i).unwrap().id, record.transaction_id);
    }
    let latest = graph
        .snapshot(None, limits(), Duration::from_secs(60))
        .await?;
    verify(&latest, 148)?;
    drop(latest);
    for repeat in 1..=3 {
        for version in [1, 128, 148] {
            let start = Instant::now();
            let snapshot = graph
                .snapshot(Some(version), limits(), Duration::from_secs(60))
                .await?;
            let capture_ms = start.elapsed().as_secs_f64() * 1000.;
            verify(&snapshot, version)?;
            let start = Instant::now();
            let plan = snapshot.prepare_vertex(2, &fixture_vertex(1000))?;
            let prepare_vertex_ms = start.elapsed().as_secs_f64() * 1000.;
            assert_eq!(plan.validation_version(), version);
            assert_eq!(plan.participants(), 1);
            assert_eq!(plan.id(), plan.clone().id());
            drop(snapshot);
            // Separate diagnostic capture obtains exact accounting without exposing internals.
            let stats = d2_raft::cohort_reads::acquire_endpoints(
                NodeRef::Remote(&coordinator),
                authority(),
                1,
                endpoints(),
                Some(version),
                limits(),
                Duration::from_secs(60),
            )
            .await
            .map_err(|e| anyhow!("stats capture: {e:?}"))?;
            let (mut rows, mut bytes, mut replay) = (0usize, 0usize, 0usize);
            for shard in 1..=3 {
                let s = stats
                    .shard(shard)
                    .map_err(|e| anyhow!("stats shard: {e:?}"))?;
                rows += s.entries.len();
                bytes += s
                    .entries
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>();
                replay += s.replay_operations;
            }
            samples.push(serde_json::json!({"repeat":repeat,"version":version,"capture_ms":capture_ms,"prepare_vertex_ms":prepare_vertex_ms,"rows":rows,"bytes":bytes,"replay_operations":replay,"verified_vertices":version,"next_vertex_absent":true}));
        }
    }
    let lag = graph
        .snapshot(Some(149), limits(), Duration::from_secs(60))
        .await;
    let lag_error = match lag {
        Ok(_) => anyhow::bail!("explicit lagging V149 was accepted"),
        Err(e) => {
            assert!(
                matches!(
                    &e,
                    replicated_graph::managed_graph::Error::Snapshot(
                        d2_raft::cohort_reads::Error::Application(
                            d2_raft::D1Error::VisibilityNotApplied
                        )
                    )
                ),
                "expected explicit lag failure, got {e:?}"
            );
            format!("{e:?}")
        }
    };
    let mut journal_samples = Vec::new();
    for repeat in 1..=3 {
        let start = Instant::now();
        let journal = NodeRef::Remote(&coordinator)
            .journal(&authority(), 1)
            .await?;
        let transfer_decode_ms = start.elapsed().as_secs_f64() * 1000.;
        let start = Instant::now();
        let bytes = encoded(&journal)?;
        let encode_ms = start.elapsed().as_secs_f64() * 1000.;
        assert_eq!(bytes, before_bytes);
        journal_samples.push(serde_json::json!({"repeat":repeat,"journal_encoded_bytes":bytes.len(),"journal_transfer_decode_ms":transfer_decode_ms,"journal_encode_ms":encode_ms}));
    }
    let original_record = before.request(write(1).external_id).unwrap().clone();
    let original_plan = d2_raft::request_plans::Plan::decode(&original_record.payload)
        .map_err(|e| anyhow!("retained plan: {e:?}"))?;
    assert_eq!(original_plan.id, original_record.transaction_id);
    let gateway = Gateway::bind_remote(
        RemoteConfig {
            coordinator: coordinator.clone(),
            binding: binding(),
            members: clients.clone(),
        },
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    let start = Instant::now();
    let reply = gateway::request(
        gateway.local_addr(),
        binding(),
        Operation::Submit(write(1)),
        BUDGET,
    )
    .await?;
    let retry_ms = start.elapsed().as_secs_f64() * 1000.;
    match &reply {
        Reply::Write { transaction_id, .. } => assert_eq!(*transaction_id, Some(original_plan.id)),
        other => anyhow::bail!("unexpected retry {other:?}"),
    }
    committed(reply, 1);
    let after = NodeRef::Remote(&coordinator)
        .journal(&authority(), 1)
        .await?;
    assert_eq!(encoded(&after)?, before_bytes);
    assert_eq!(after.request(write(1).external_id), Some(&original_record));
    verify(
        &graph
            .snapshot(None, limits(), Duration::from_secs(60))
            .await?,
        148,
    )?;
    gateway.shutdown().await;
    let result = serde_json::json!({"label":label,"profile":"debug, jobs=1, incremental=0","processes":5,"terminal_transactions":149,"common_version":148,"capture_samples":samples,"journal_samples":journal_samples,"explicit_v149_error":lag_error,"id1_retry_ms":retry_ms,"journal_and_exact_retained_plan_unchanged":true,"new_external_ids_submitted":0,"fixture":"independent regular-file /tmp copies; original DBs never opened"});
    fs::write(output, serde_json::to_vec_pretty(&result)?)?;
    println!("PREFIX_COMPARISON {result}");
    Ok(())
}
