use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;

use openraft::Config;
use tokio::time::{sleep, timeout};

use crate::durable_log_store::DurableLogStore;
use crate::log_store::LogStore;
use crate::network::{DropPolicy, NetworkFactory, Raft, Registry};
use crate::state_machine::StateMachine;
use crate::types::NodeId;

pub struct Node;

pub struct NodeHandle {
    pub id: NodeId,
    pub raft: Raft,
    pub state_machine: Arc<StateMachine>,
}

impl Node {
    pub async fn start(
        id: NodeId,
        registry: Registry,
        policy: Arc<DropPolicy>,
    ) -> anyhow::Result<NodeHandle> {
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
        registry.insert(id, raft.clone()).await;
        Ok(NodeHandle {
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
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)
            .with_context(|| format!("create data dir {}", dir.display()))?;
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
        registry.insert(id, raft.clone()).await;
        Ok(NodeHandle {
            id,
            raft,
            state_machine,
        })
    }

    pub async fn shutdown(handle: &NodeHandle, registry: &Registry) {
        registry.remove(handle.id).await;
        let _ = handle.raft.shutdown().await;
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
