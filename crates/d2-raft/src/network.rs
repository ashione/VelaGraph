use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tokio::sync::RwLock;

use crate::types::{NodeId, TypeConfig};

pub type Raft = openraft::Raft<TypeConfig>;

/// Shared in-process node registry. No sockets or HTTP.
#[derive(Clone, Default)]
pub struct Registry {
    nodes: Arc<RwLock<BTreeMap<NodeId, Raft>>>,
}

impl Registry {
    pub async fn insert(&self, id: NodeId, raft: Raft) {
        self.nodes.write().await.insert(id, raft);
    }

    pub async fn remove(&self, id: NodeId) {
        self.nodes.write().await.remove(&id);
    }

    pub async fn get(&self, id: NodeId) -> Option<Raft> {
        self.nodes.read().await.get(&id).cloned()
    }
}

/// Directed allow/deny policy for partition tests.
#[derive(Debug, Default)]
pub struct DropPolicy {
    denied: RwLock<BTreeSet<(NodeId, NodeId)>>,
}

impl DropPolicy {
    pub async fn deny(&self, from: NodeId, to: NodeId) {
        self.denied.write().await.insert((from, to));
    }

    pub async fn allow(&self, from: NodeId, to: NodeId) {
        self.denied.write().await.remove(&(from, to));
    }

    pub async fn clear(&self) {
        self.denied.write().await.clear();
    }

    async fn is_denied(&self, from: NodeId, to: NodeId) -> bool {
        self.denied.read().await.contains(&(from, to))
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
        if self.policy.is_denied(self.from, self.target).await {
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
