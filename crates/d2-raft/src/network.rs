use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};

use crate::types::{NodeId, TypeConfig};

/// In-process consensus group identity. Group zero is the legacy namespace.
pub type GroupId = u64;

pub type Raft = openraft::Raft<TypeConfig>;

/// Shared in-process node registry. No sockets or HTTP.
#[derive(Clone, Default)]
pub struct Registry {
    group_id: GroupId,
    startups: Arc<Mutex<()>>,
    nodes: Arc<RwLock<BTreeMap<(GroupId, NodeId), RegisteredNode>>>,
}

struct RegisteredNode {
    raft: Raft,
    token: Arc<()>,
}

impl Registry {
    /// Select an isolated namespace on this same in-process transport.
    pub fn in_group(&self, group_id: GroupId) -> Self {
        Self {
            group_id,
            startups: self.startups.clone(),
            nodes: self.nodes.clone(),
        }
    }

    pub fn group_id(&self) -> GroupId {
        self.group_id
    }

    /// Register a replica without replacing an existing registration.
    pub async fn insert(&self, id: NodeId, raft: Raft) -> anyhow::Result<()> {
        let _startup = self.reserve_startup(id).await?;
        self.register(id, raft).await.map(|_| ())
    }

    // Serialize startup through publication so a duplicate durable replica never
    // starts a Raft task (or sends votes) before registration rejects it.
    pub(crate) async fn reserve_startup(&self, id: NodeId) -> anyhow::Result<OwnedMutexGuard<()>> {
        let guard = self.startups.clone().lock_owned().await;
        anyhow::ensure!(
            !self.nodes.read().await.contains_key(&(self.group_id, id)),
            "node {id} already registered in group {}",
            self.group_id
        );
        Ok(guard)
    }

    pub(crate) async fn register(&self, id: NodeId, raft: Raft) -> anyhow::Result<Arc<()>> {
        let mut nodes = self.nodes.write().await;
        let key = (self.group_id, id);
        anyhow::ensure!(
            !nodes.contains_key(&key),
            "node {id} already registered in group {}",
            self.group_id
        );
        let token = Arc::new(());
        nodes.insert(
            key,
            RegisteredNode {
                raft,
                token: token.clone(),
            },
        );
        Ok(token)
    }

    pub async fn remove(&self, id: NodeId) {
        self.nodes.write().await.remove(&(self.group_id, id));
    }

    pub(crate) async fn remove_registration(&self, id: NodeId, token: &Arc<()>) {
        let mut nodes = self.nodes.write().await;
        let key = (self.group_id, id);
        if nodes
            .get(&key)
            .is_some_and(|entry| Arc::ptr_eq(&entry.token, token))
        {
            nodes.remove(&key);
        }
    }

    pub async fn get(&self, id: NodeId) -> Option<Raft> {
        self.nodes
            .read()
            .await
            .get(&(self.group_id, id))
            .map(|entry| entry.raft.clone())
    }
}

/// Directed allow/deny policy for partition tests, scoped to a consensus group.
#[derive(Clone, Debug, Default)]
pub struct DropPolicy {
    group_id: GroupId,
    denied: Arc<RwLock<BTreeSet<(GroupId, NodeId, NodeId)>>>,
}

impl DropPolicy {
    /// Select a group while retaining the same shared policy storage.
    pub fn in_group(&self, group_id: GroupId) -> Self {
        Self {
            group_id,
            denied: self.denied.clone(),
        }
    }

    pub async fn deny(&self, from: NodeId, to: NodeId) {
        self.denied.write().await.insert((self.group_id, from, to));
    }

    pub async fn allow(&self, from: NodeId, to: NodeId) {
        self.denied.write().await.remove(&(self.group_id, from, to));
    }

    /// Clear only this group's denied links.
    pub async fn clear(&self) {
        self.denied
            .write()
            .await
            .retain(|(group, _, _)| *group != self.group_id);
    }

    async fn is_denied(&self, group: GroupId, from: NodeId, to: NodeId) -> bool {
        self.denied.read().await.contains(&(group, from, to))
    }
}

#[derive(Clone)]
pub struct NetworkFactory {
    pub registry: Registry,
    pub policy: Arc<DropPolicy>,
    pub from: NodeId,
}

#[derive(Clone)]
pub struct NetworkConnection {
    registry: Registry,
    policy: Arc<DropPolicy>,
    from: NodeId,
    target: NodeId,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = NetworkConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        NetworkConnection {
            registry: self.registry.clone(),
            policy: self.policy.clone(),
            from: self.from,
            target,
        }
    }
}

fn unreachable(target: NodeId) -> Unreachable {
    let err = std::io::Error::new(
        std::io::ErrorKind::NotConnected,
        format!("node {target} unreachable"),
    );
    Unreachable::new(&err)
}

impl NetworkConnection {
    async fn peer(&self) -> Result<Raft, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        if self
            .policy
            .is_denied(self.registry.group_id(), self.from, self.target)
            .await
        {
            return Err(RPCError::Unreachable(unreachable(self.target)));
        }
        self.registry
            .get(self.target)
            .await
            .ok_or_else(|| RPCError::Unreachable(unreachable(self.target)))
    }
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let peer = self.peer().await?;
        peer.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let peer = self.peer().await.map_err(|e| match e {
            RPCError::Unreachable(u) => RPCError::Unreachable(u),
            _ => RPCError::Unreachable(unreachable(self.target)),
        })?;
        peer.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let peer = self.peer().await?;
        peer.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fault_policies_and_clients_use_registry_namespace() -> anyhow::Result<()> {
        let root = Registry::default();
        let policy = Arc::new(DropPolicy::default());
        let a_policy = policy.in_group(1);
        let a = crate::Node::start(2, root.in_group(1), policy.clone()).await?;
        let b = crate::Node::start(2, root.in_group(2), policy.clone()).await?;
        a_policy.deny(1, 2).await;
        let mut a_factory = NetworkFactory {
            registry: root.in_group(1),
            policy: policy.clone(),
            from: 1,
        };
        let mut b_factory = NetworkFactory {
            registry: root.in_group(2),
            policy: policy.clone(),
            from: 1,
        };
        let a_client = a_factory.new_client(2, &BasicNode::default()).await;
        let b_client = b_factory.new_client(2, &BasicNode::default()).await;
        assert!(a_client.peer().await.is_err());
        assert!(b_client.peer().await.is_ok());
        policy.clear().await;
        assert!(a_client.peer().await.is_err());
        a_policy.clear().await;
        assert!(a_client.peer().await.is_ok());
        a_policy.deny(1, 2).await;
        policy.in_group(2).allow(1, 2).await;
        assert!(a_client.peer().await.is_err());
        a_policy.allow(1, 2).await;
        assert!(a_client.peer().await.is_ok());
        crate::Node::shutdown(&a, &root).await;
        assert!(a_client.peer().await.is_err());
        assert!(b_client.peer().await.is_ok());
        crate::Node::shutdown(&b, &root).await;
        Ok(())
    }
}
