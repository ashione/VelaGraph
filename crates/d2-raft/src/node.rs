use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;

use openraft::Config;
use tokio::time::{sleep, timeout};

use crate::durable_log_store::DurableLogStore;
use crate::log_store::LogStore;
use crate::network::{DropPolicy, GroupId, NetworkFactory, Raft, Registry};
use crate::state_machine::StateMachine;
use crate::types::NodeId;

pub struct Node;

pub struct NodeHandle {
    registry: Registry,
    registration: Arc<()>,
    pub id: NodeId,
    pub raft: Raft,
    pub state_machine: Arc<StateMachine>,
}

impl NodeHandle {
    /// Immutable in-process consensus group associated with this replica.
    pub fn group_id(&self) -> GroupId {
        self.registry.group_id()
    }
}

impl Node {
    pub async fn start(
        id: NodeId,
        registry: Registry,
        policy: Arc<DropPolicy>,
    ) -> anyhow::Result<NodeHandle> {
        let _startup = registry.reserve_startup(id).await?;
        let config = Arc::new(
            Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()?,
        );
        let log_store = LogStore::default();
        let state_machine = Arc::new(StateMachine::default());
        let network = NetworkFactory {
            registry: registry.clone(),
            policy: policy.clone(),
            from: id,
        };
        let raft =
            openraft::Raft::new(id, config, network, log_store, state_machine.clone()).await?;
        let registration = match registry.register(id, raft.clone()).await {
            Ok(registration) => registration,
            Err(error) => {
                let _ = raft.shutdown().await;
                return Err(error);
            }
        };
        Ok(NodeHandle {
            registry,
            registration,
            id,
            raft,
            state_machine,
        })
    }

    /// Start a node whose Raft log and state machine are persisted under `dir`.
    ///
    /// `raft.db` and `state.db` are separate RocksDB directories so log purge
    /// and graph snapshots never touch each other's keys.
    pub async fn start_durable(
        id: NodeId,
        dir: impl AsRef<Path>,
        registry: Registry,
        policy: Arc<DropPolicy>,
    ) -> anyhow::Result<NodeHandle> {
        let _startup = registry.reserve_startup(id).await?;
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)
            .with_context(|| format!("create data dir {}", dir.display()))?;
        bind_durable_identity(dir, registry.group_id(), id)?;
        let log_store = DurableLogStore::open(dir.join("raft.db"))?;
        let state_machine = Arc::new(StateMachine::open(dir.join("state.db"))?);
        let config = Arc::new(
            Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()?,
        );
        let network = NetworkFactory {
            registry: registry.clone(),
            policy: policy.clone(),
            from: id,
        };
        let raft =
            openraft::Raft::new(id, config, network, log_store, state_machine.clone()).await?;
        let registration = match registry.register(id, raft.clone()).await {
            Ok(registration) => registration,
            Err(error) => {
                let _ = raft.shutdown().await;
                return Err(error);
            }
        };
        Ok(NodeHandle {
            registry,
            registration,
            id,
            raft,
            state_machine,
        })
    }

    pub async fn shutdown(handle: &NodeHandle, _registry: &Registry) {
        let _ = handle.raft.shutdown().await;
        handle
            .registry
            .remove_registration(handle.id, &handle.registration)
            .await;
    }
}

pub struct Cluster {
    pub registry: Registry,
    pub policy: Arc<DropPolicy>,
}

impl Cluster {
    pub fn new() -> Self {
        Self {
            registry: Registry::default(),
            policy: Arc::new(DropPolicy::default()),
        }
    }

    /// Create a group-scoped cluster sharing transport and fault policy storage.
    pub fn in_group(&self, group_id: GroupId) -> Self {
        Self {
            registry: self.registry.in_group(group_id),
            policy: Arc::new(self.policy.in_group(group_id)),
        }
    }

    pub async fn start(&self, id: NodeId) -> anyhow::Result<NodeHandle> {
        Node::start(id, self.registry.clone(), self.policy.clone()).await
    }

    pub async fn start_durable(
        &self,
        id: NodeId,
        dir: impl AsRef<Path>,
    ) -> anyhow::Result<NodeHandle> {
        Node::start_durable(id, dir, self.registry.clone(), self.policy.clone()).await
    }

    pub async fn shutdown(&self, handle: &NodeHandle) {
        Node::shutdown(handle, &self.registry).await;
    }

    pub async fn wait_for_leader(
        &self,
        nodes: &[NodeHandle],
        timeout_ms: u64,
    ) -> anyhow::Result<NodeId> {
        timeout(Duration::from_millis(timeout_ms), async {
            loop {
                for node in nodes {
                    if let Some(leader) = node.raft.metrics().borrow().current_leader {
                        return leader;
                    }
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("no leader elected within {timeout_ms}ms"))
    }

    pub async fn wait_for_applied(
        &self,
        node: &NodeHandle,
        index: u64,
        timeout_ms: u64,
    ) -> anyhow::Result<()> {
        timeout(Duration::from_millis(timeout_ms), async {
            loop {
                let applied = node.state_machine.applied().await;
                if applied.map(|l| l.index).unwrap_or(0) >= index {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("node {} did not apply index {index}", node.id))
    }

    pub async fn wait_for_value(
        &self,
        node: &NodeHandle,
        key: &str,
        value: &str,
        timeout_ms: u64,
    ) -> anyhow::Result<()> {
        timeout(Duration::from_millis(timeout_ms), async {
            loop {
                if node.state_machine.get(key).await.as_deref() == Some(value) {
                    return;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| anyhow::anyhow!("node {} did not observe {key}={value}", node.id))
    }
}

impl Default for Cluster {
    fn default() -> Self {
        Self::new()
    }
}

/// Versioned identity is written before creating either database. A partial or
/// unknown identity fails closed. Legacy nonempty directories belong to group 0;
/// they cannot silently acquire a new nonzero identity.
fn bind_durable_identity(dir: &Path, group_id: GroupId, node_id: NodeId) -> anyhow::Result<()> {
    #[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug)]
    #[serde(deny_unknown_fields)]
    struct Identity {
        version: u32,
        group_id: GroupId,
        node_id: NodeId,
    }
    let expected = Identity {
        version: 1,
        group_id,
        node_id,
    };
    let path = dir.join("raft-group.json");
    match std::fs::read(&path) {
        Ok(bytes) => {
            let actual: Identity =
                serde_json::from_slice(&bytes).context("decode durable group identity")?;
            anyhow::ensure!(
                actual == expected,
                "durable group identity mismatch: expected {expected:?}, found {actual:?}"
            );
            return Ok(());
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("read durable group identity"),
    }
    anyhow::ensure!(
        group_id == 0 || std::fs::read_dir(dir)?.next().transpose()?.is_none(),
        "nonzero group cannot adopt an unbound nonempty durable directory"
    );
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .context("create durable group identity")?;
    file.write_all(&serde_json::to_vec(&expected)?)?;
    file.sync_all()?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
}
