// Common actual-process bootstrap and lifecycle helpers. Included at each crate
// root so current_exe() --ignored --exact managed_node_child stays valid.
use anyhow::{anyhow, Context};
use d2_raft::{
    managed_rpc::{activate_byte_bound_member, Client, Identity, Role, Server},
    participant_writes::Authority,
    transaction_decisions::{CapacityProfile, Cohort, Command as DecisionCommand, Journal, Member},
    versioned_reads::Target,
    Cluster, D1Command, LogCommand, NodeHandle, StateMachine,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use replicated_graph::managed_gateway::{Binding, Mapping};
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

const BUDGET: Duration = Duration::from_secs(15);
const CHILD_ENV: &str = "MANAGED_PROCESS_DIRECTORY";
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn cohort() -> Cohort {
    Cohort {
        profile: Default::default(),
        coordinator_shard: 9,
        coordinator_epoch: 1,
        members: [7, 8]
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
fn identity_in(shard: u64, cohort: Cohort) -> Identity {
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
        cohort,
    }
}
fn binding() -> Binding {
    Binding {
        authority: authority(),
        cohort: cohort(),
        mapping: Mapping::PartitionEqualsShardV1,
    }
}
async fn local_command(node: &NodeHandle, command: D1Command) -> anyhow::Result<()> {
    node.raft
        .client_write(LogCommand::D1V1(command))
        .await?
        .data
        .d1
        .context("missing D1 result")?
        .map_err(|e| anyhow!("fixture activation: {e:?}"))?;
    Ok(())
}

/// Only the subprocess runs this ignored entrypoint; the ordinary suite cannot
/// accidentally construct local handles in the gateway/client process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "private child-process entrypoint"]
async fn managed_node_child() -> anyhow::Result<()> {
    let directory =
        std::path::PathBuf::from(std::env::var_os(CHILD_ENV).context("child directory")?);
    let shard: u64 = std::env::var("MANAGED_PROCESS_SHARD")?.parse()?;
    let address: SocketAddr = std::env::var("MANAGED_PROCESS_ADDRESS")?.parse()?;
    let fresh = std::env::var("MANAGED_PROCESS_FRESH")? == "1";
    let config: ProcessConfig = serde_json::from_slice(&fs::read(directory.join("fixture.json"))?)?;
    restore_fixture_snapshot(&directory).await?;
    let cluster = Cluster::new().in_group(shard);
    let node = Arc::new(cluster.start_durable(1, directory.join("data")).await?);
    if fresh {
        node.raft
            .initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
            .await?;
    }
    tokio::time::timeout(BUDGET, async {
        while node.raft.ensure_linearizable().await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    if fresh {
        local_command(&node, D1Command::Activate { shard, epoch: 1 }).await?;
        local_command(
            &node,
            D1Command::EnableManagedWrites {
                shard,
                epoch: 1,
                expected_sequence: 0,
                mode_id: target(shard).mode,
            },
        )
        .await?;
        if shard == 9 {
            local_command(
                &node,
                D1Command::EnableCoordinator {
                    shard,
                    epoch: 1,
                    mode_id: target(shard).mode,
                },
            )
            .await?;
            local_command(
                &node,
                D1Command::Coordinator {
                    shard,
                    epoch: 1,
                    mode_id: target(shard).mode,
                    command: DecisionCommand::BindCohort {
                        cohort: config.cohort.clone(),
                    },
                },
            )
            .await?;
        } else {
            local_command(
                &node,
                D1Command::EnableParticipant {
                    shard,
                    epoch: 1,
                    mode_id: target(shard).mode,
                    authority: authority(),
                },
            )
            .await?;
            if config.cohort.profile == CapacityProfile::ByteBound16MiBV1 {
                activate_byte_bound_member(&node, &authority(), &target(shard), &config.cohort)
                    .await?;
            } else {
                let mut journal = Journal::new(authority().mode).map_err(|e| anyhow!("{e:?}"))?;
                journal
                    .apply(DecisionCommand::BindCohort {
                        cohort: config.cohort.clone(),
                    })
                    .map_err(|e| anyhow!("{e:?}"))?;
                local_command(
                    &node,
                    D1Command::EnableVersionedReads {
                        shard,
                        epoch: 1,
                        mode_id: target(shard).mode,
                        journal,
                    },
                )
                .await?;
            }
        }
    }
    let own_identity = identity_in(shard, config.cohort.clone());
    let server = if shard == 9 && config.cohort.profile == CapacityProfile::ByteBound16MiBV1 {
        let members = config
            .members
            .iter()
            .map(|(member, address)| {
                Client::new(
                    *address,
                    identity_in(*member, config.cohort.clone()),
                    BUDGET,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        Server::bind_byte_bound_coordinator(node.clone(), own_identity, members, address, BUDGET)
            .await?
    } else {
        Server::bind(node.clone(), own_identity, address, BUDGET).await?
    };
    // Test-only lazy graph-port control: ordinary managed fixtures do not
    // start an extra service or allocate query sessions.
    let mut graph = None;
    fs::write(
        directory.join("ready.tmp"),
        serde_json::to_vec(&(std::process::id(), server.local_addr()))?,
    )?;
    fs::rename(directory.join("ready.tmp"), directory.join("ready.json"))?;
    // Test-only filesystem control. No production RPC or public bypass is added.
    loop {
        tokio::time::sleep(Duration::from_millis(20)).await;
        if graph.is_none() && shard != 9 && directory.join("graph.request").exists() {
            let service = replicated_graph::remote::GraphService::start_managed(
                &node,
                "127.0.0.1:0".parse()?,
                shard,
                shard,
                BUDGET,
            )
            .await?;
            atomic_json(
                &directory.join("graph-ready.json"),
                &(std::process::id(), service.local_addr()),
            )?;
            graph = Some(service);
        }
        if directory.join("snapshot.request").exists() {
            node.raft.ensure_linearizable().await?;
            server.shutdown().await;
            if let Some(graph) = graph {
                graph.shutdown().await;
            }
            node.raft.shutdown().await?;
            let mut state = node.state_machine.clone();
            let snapshot = state.build_snapshot().await?;
            atomic_json(
                &directory.join("snapshot.json"),
                &FixtureSnapshot {
                    meta: snapshot.meta,
                    bytes: snapshot.snapshot.into_inner(),
                },
            )?;
            return Ok(());
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProcessConfig {
    cohort: Cohort,
    members: Vec<(u64, SocketAddr)>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct FixtureSnapshot {
    meta: openraft::SnapshotMeta<u64, openraft::BasicNode>,
    bytes: Vec<u8>,
}
fn atomic_json(path: &Path, value: &impl Serialize) -> anyhow::Result<()> {
    let temporary = path.with_extension("tmp");
    let mut file = fs::File::create(&temporary)?;
    file.write_all(&serde_json::to_vec(value)?)?;
    file.sync_all()?;
    fs::rename(temporary, path)?;
    fs::File::open(path.parent().context("checkpoint parent")?)?.sync_all()?;
    Ok(())
}
async fn restore_fixture_snapshot(directory: &Path) -> anyhow::Result<()> {
    let data = directory.join("data");
    if !data.join("state.before-snapshot.db").exists()
        || directory.join("snapshot.restored").exists()
    {
        return Ok(());
    }
    let snapshot: FixtureSnapshot =
        serde_json::from_slice(&fs::read(directory.join("snapshot.json"))?)?;
    let mut state = Arc::new(StateMachine::open(data.join("state.db"))?);
    state
        .install_snapshot(&snapshot.meta, Box::new(Cursor::new(snapshot.bytes)))
        .await?;
    anyhow::ensure!(
        state.applied().await == snapshot.meta.last_log_id,
        "snapshot applied marker changed"
    );
    drop(state);
    atomic_json(&directory.join("snapshot.restored"), &snapshot.meta)?;
    Ok(())
}
struct FixtureDirectory {
    path: PathBuf,
    _temporary: Option<tempfile::TempDir>,
}
impl FixtureDirectory {
    fn temporary() -> anyhow::Result<Self> {
        let temporary = tempfile::tempdir()?;
        Ok(Self {
            path: temporary.path().to_owned(),
            _temporary: Some(temporary),
        })
    }
    fn path(&self) -> &Path {
        &self.path
    }
}

struct Process {
    child: Child,
    directory: FixtureDirectory,
    shard: u64,
    address: SocketAddr,
    config: ProcessConfig,
}
impl Process {
    async fn start(shard: u64) -> anyhow::Result<Self> {
        Self::configured(
            shard,
            FixtureDirectory::temporary()?,
            ProcessConfig {
                cohort: cohort(),
                members: Vec::new(),
            },
        )
        .await
    }
    async fn configured(
        shard: u64,
        directory: FixtureDirectory,
        config: ProcessConfig,
    ) -> anyhow::Result<Self> {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = reservation.local_addr()?;
        let fresh = !directory.path().join("data").exists();
        atomic_json(&directory.path().join("fixture.json"), &config)?;
        if directory.path().join("ready.json").exists() {
            fs::remove_file(directory.path().join("ready.json"))?;
        }
        if directory.path().join("snapshot.request").exists() {
            fs::remove_file(directory.path().join("snapshot.request"))?;
        }
        drop(reservation);
        let child = Self::launch(directory.path(), shard, address, fresh)?;
        let mut process = Self {
            child,
            directory,
            shard,
            address,
            config,
        };
        process.ready().await?;
        Ok(process)
    }
    fn launch(
        directory: &std::path::Path,
        shard: u64,
        address: SocketAddr,
        fresh: bool,
    ) -> anyhow::Result<Child> {
        Ok(Command::new(std::env::current_exe()?)
            .args(["--ignored", "--exact", "managed_node_child", "--nocapture"])
            .env(CHILD_ENV, directory)
            .env("MANAGED_PROCESS_SHARD", shard.to_string())
            .env("MANAGED_PROCESS_ADDRESS", address.to_string())
            .env("MANAGED_PROCESS_FRESH", if fresh { "1" } else { "0" })
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()?)
    }
    async fn ready(&mut self) -> anyhow::Result<()> {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                anyhow::ensure!(
                    self.child.try_wait()?.is_none(),
                    "managed child exited before readiness"
                );
                if let Ok(bytes) = fs::read(self.directory.path().join("ready.json")) {
                    let (pid, address): (u32, SocketAddr) = serde_json::from_slice(&bytes)?;
                    anyhow::ensure!(
                        pid == self.child.id() && address == self.address,
                        "stale or wrong readiness identity"
                    );
                    return Ok::<_, anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await??;
        Ok(())
    }
    fn stop(&mut self) -> anyhow::Result<()> {
        self.child.kill()?;
        self.child.wait()?;
        Ok(())
    }
    async fn reopen(&mut self) -> anyhow::Result<()> {
        fs::remove_file(self.directory.path().join("ready.json"))?;
        self.child = Self::launch(self.directory.path(), self.shard, self.address, false)?;
        self.ready().await
    }
    fn client(&self) -> anyhow::Result<Client> {
        Ok(Client::new(
            self.address,
            identity_in(self.shard, self.config.cohort.clone()),
            BUDGET,
        )?)
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
