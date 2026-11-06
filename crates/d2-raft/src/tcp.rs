//! Real TCP transport for the D2 prototype.
//!
//! One Raft node per process; peers reach each other through newline-delimited
//! JSON RPC. The wire types are the openraft request/response values, so the
//! consensus decisions stay identical to the in-process tests.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;

use crate::durable_log_store::DurableLogStore;
use crate::state_machine::StateMachine;
use crate::types::{LogCommand, NodeId, TypeConfig};

pub type Raft = openraft::Raft<TypeConfig>;

const RPC_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_LINE: usize = 64 * 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
enum RaftRpc {
    Vote(VoteRequest<NodeId>),
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

#[derive(Debug, Serialize, Deserialize)]
enum RaftReply {
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(
        Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
    ),
}

fn io_unreachable(addr: &str, message: impl Into<String>) -> Unreachable {
    Unreachable::new(&io::Error::new(
        io::ErrorKind::NotConnected,
        format!("{addr}: {}", message.into()),
    ))
}

async fn exchange(addr: &str, request: &RaftRpc) -> Result<RaftReply, String> {
    let mut stream = timeout(RPC_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| format!("connect to {addr} timed out"))?
        .map_err(|e| format!("connect to {addr}: {e}"))?;
    let _ = stream.set_nodelay(true);
    let mut payload = serde_json::to_vec(request).map_err(|e| format!("encode request: {e}"))?;
    payload.push(b'\n');
    timeout(RPC_TIMEOUT, stream.write_all(&payload))
        .await
        .map_err(|_| format!("write to {addr} timed out"))?
        .map_err(|e| format!("write to {addr}: {e}"))?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    timeout(RPC_TIMEOUT, reader.read_line(&mut line))
        .await
        .map_err(|_| format!("read from {addr} timed out"))?
        .map_err(|e| format!("read from {addr}: {e}"))?;
    if line.is_empty() {
        return Err(format!("{addr} closed the connection without a reply"));
    }
    if line.len() > MAX_LINE {
        return Err(format!("{addr} response exceeds {MAX_LINE} bytes"));
    }
    serde_json::from_str(&line).map_err(|e| format!("decode response from {addr}: {e}"))
}

async fn serve_raft(listener: TcpListener, raft: Raft) {
    loop {
        let (stream, _peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(%error, "raft listener accept failed");
                continue;
            }
        };
        let raft = raft.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_raft_connection(stream, raft).await {
                tracing::warn!(%error, "raft connection failed");
            }
        });
    }
}

async fn handle_raft_connection(stream: TcpStream, raft: Raft) -> anyhow::Result<()> {
    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    let read = timeout(RPC_TIMEOUT, reader.read_line(&mut line)).await;
    match read {
        Ok(Ok(0)) => return Ok(()),
        Ok(Ok(_)) if line.len() > MAX_LINE => anyhow::bail!("request too large"),
        Ok(Ok(_)) => {}
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => anyhow::bail!("request timed out"),
    }

    let mut stream = reader.into_inner();
    let request: RaftRpc = serde_json::from_str(&line)?;
    let reply = match request {
        RaftRpc::Vote(request) => RaftReply::Vote(raft.vote(request).await),
        RaftRpc::AppendEntries(request) => {
            RaftReply::AppendEntries(raft.append_entries(request).await)
        }
        RaftRpc::InstallSnapshot(request) => {
            RaftReply::InstallSnapshot(raft.install_snapshot(request).await)
        }
    };
    let mut payload = serde_json::to_vec(&reply)?;
    payload.push(b'\n');
    timeout(RPC_TIMEOUT, stream.write_all(&payload)).await??;
    Ok(())
}

/// Factory used by a single node to dial every peer's advertised address.
#[derive(Clone, Copy, Default)]
pub struct TcpNetworkFactory;

#[derive(Clone)]
pub struct TcpNetwork {
    target: NodeId,
    addr: String,
}

impl RaftNetworkFactory<TypeConfig> for TcpNetworkFactory {
    type Network = TcpNetwork;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        TcpNetwork {
            target,
            addr: node.addr.clone(),
        }
    }
}

fn remote<T>(
    target: NodeId,
    result: Result<T, RaftError<NodeId>>,
) -> Result<T, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
    result.map_err(|error| RPCError::RemoteError(RemoteError::new(target, error)))
}

fn remote_snapshot(
    target: NodeId,
    result: Result<InstallSnapshotResponse<NodeId>, RaftError<NodeId, InstallSnapshotError>>,
) -> Result<
    InstallSnapshotResponse<NodeId>,
    RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
> {
    result.map_err(|error| RPCError::RemoteError(RemoteError::new(target, error)))
}

impl RaftNetwork<TypeConfig> for TcpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let reply = exchange(&self.addr, &RaftRpc::AppendEntries(rpc))
            .await
            .map_err(|error| RPCError::Unreachable(io_unreachable(&self.addr, error)))?;
        match reply {
            RaftReply::AppendEntries(result) => remote(self.target, result),
            _ => Err(RPCError::Unreachable(io_unreachable(
                &self.addr,
                "unexpected reply variant",
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let reply = exchange(&self.addr, &RaftRpc::InstallSnapshot(rpc))
            .await
            .map_err(|error| RPCError::Unreachable(io_unreachable(&self.addr, error)))?;
        match reply {
            RaftReply::InstallSnapshot(result) => remote_snapshot(self.target, result),
            _ => Err(RPCError::Unreachable(io_unreachable(
                &self.addr,
                "unexpected reply variant",
            ))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let reply = exchange(&self.addr, &RaftRpc::Vote(rpc))
            .await
            .map_err(|error| RPCError::Unreachable(io_unreachable(&self.addr, error)))?;
        match reply {
            RaftReply::Vote(result) => remote(self.target, result),
            _ => Err(RPCError::Unreachable(io_unreachable(
                &self.addr,
                "unexpected reply variant",
            ))),
        }
    }
}

/// Initialize a new cluster with the given voter addresses.
///
/// This is the same path used by `d2-node --bootstrap` and by the control API;
/// real deployments should call it from their startup coordinator.
pub async fn initialize_cluster(
    raft: &Raft,
    members: BTreeMap<NodeId, String>,
) -> anyhow::Result<()> {
    let members: BTreeMap<NodeId, BasicNode> = members
        .into_iter()
        .map(|(id, addr)| (id, BasicNode { addr }))
        .collect();
    raft.initialize(members).await?;
    Ok(())
}

/// Add one learner and return the committed log index.
pub async fn add_learner(
    raft: &Raft,
    id: NodeId,
    addr: String,
    blocking: bool,
) -> anyhow::Result<u64> {
    let response = raft.add_learner(id, BasicNode { addr }, blocking).await?;
    Ok(response.log_id.index)
}

/// Change the voter set and return the committed log index.
pub async fn change_membership(raft: &Raft, ids: BTreeSet<NodeId>) -> anyhow::Result<u64> {
    let response = raft.change_membership(ids, false).await?;
    Ok(response.log_id.index)
}

/// Driver commands used by the multi-process integration test and the node CLI.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlRequest {
    Initialize {
        members: BTreeMap<NodeId, String>,
    },
    AddLearner {
        id: NodeId,
        addr: String,
        blocking: bool,
    },
    ChangeMembership {
        ids: BTreeSet<NodeId>,
    },
    Write {
        key: String,
        value: String,
    },
    /// Linearizable read, only valid on the current leader.
    Read {
        key: String,
    },
    /// Local state-machine read; used by tests to verify every replica.
    Get {
        key: String,
    },
    Status,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
    pub id: NodeId,
    pub leader: Option<NodeId>,
    pub applied: u64,
    pub last_log: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ControlResponse {
    Ack,
    Index(u64),
    Value(Option<String>),
    Status(NodeStatus),
    Err(String),
}

impl ControlResponse {
    pub fn into_result(self) -> anyhow::Result<Self> {
        match self {
            Self::Err(error) => anyhow::bail!("{error}"),
            other => Ok(other),
        }
    }
}

async fn exchange_control(addr: &str, request: &ControlRequest) -> Result<ControlResponse, String> {
    let mut stream = timeout(RPC_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| format!("connect to {addr} timed out"))?
        .map_err(|e| format!("connect to {addr}: {e}"))?;
    let _ = stream.set_nodelay(true);
    let mut payload =
        serde_json::to_vec(request).map_err(|e| format!("encode control request: {e}"))?;
    payload.push(b'\n');
    timeout(RPC_TIMEOUT, stream.write_all(&payload))
        .await
        .map_err(|_| format!("write control to {addr} timed out"))?
        .map_err(|e| format!("write control to {addr}: {e}"))?;

    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    timeout(RPC_TIMEOUT, reader.read_line(&mut line))
        .await
        .map_err(|_| format!("read control from {addr} timed out"))?
        .map_err(|e| format!("read control from {addr}: {e}"))?;
    if line.is_empty() {
        return Err(format!("{addr} closed the control connection"));
    }
    serde_json::from_str(&line).map_err(|e| format!("decode control from {addr}: {e}"))
}

/// Send one control command and return the decoded response.
pub async fn send_control(addr: &str, request: ControlRequest) -> anyhow::Result<ControlResponse> {
    exchange_control(addr, &request)
        .await
        .map_err(anyhow::Error::msg)?
        .into_result()
}

async fn serve_control(listener: TcpListener, raft: Raft, state_machine: Arc<StateMachine>) {
    loop {
        let (mut stream, _peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(error) => {
                tracing::warn!(%error, "control listener accept failed");
                continue;
            }
        };
        let raft = raft.clone();
        let state_machine = state_machine.clone();
        tokio::spawn(async move {
            if let Err(error) = handle_control_connection(&mut stream, raft, state_machine).await {
                tracing::warn!(%error, "control connection failed");
            }
        });
    }
}

async fn handle_control_connection(
    stream: &mut TcpStream,
    raft: Raft,
    state_machine: Arc<StateMachine>,
) -> anyhow::Result<()> {
    let mut line = String::new();
    let mut reader = BufReader::new(stream);
    let read = timeout(RPC_TIMEOUT, reader.read_line(&mut line)).await;
    match read {
        Ok(Ok(0)) => return Ok(()),
        Ok(Ok(_)) if line.len() > MAX_LINE => anyhow::bail!("control request too large"),
        Ok(Ok(_)) => {}
        Ok(Err(error)) => return Err(error.into()),
        Err(_) => anyhow::bail!("control request timed out"),
    }

    let stream = reader.into_inner();
    let request: ControlRequest = serde_json::from_str(&line)?;
    let response = match request {
        ControlRequest::Initialize { members } => match initialize_cluster(&raft, members).await {
            Ok(()) => ControlResponse::Ack,
            Err(error) => ControlResponse::Err(error.to_string()),
        },
        ControlRequest::AddLearner { id, addr, blocking } => {
            match add_learner(&raft, id, addr, blocking).await {
                Ok(index) => ControlResponse::Index(index),
                Err(error) => ControlResponse::Err(error.to_string()),
            }
        }
        ControlRequest::ChangeMembership { ids } => match change_membership(&raft, ids).await {
            Ok(index) => ControlResponse::Index(index),
            Err(error) => ControlResponse::Err(error.to_string()),
        },
        ControlRequest::Write { key, value } => {
            match raft.client_write(LogCommand::put(key, value)).await {
                Ok(response) => ControlResponse::Index(response.log_id.index),
                Err(error) => ControlResponse::Err(error.to_string()),
            }
        }
        ControlRequest::Read { key } => match raft.ensure_linearizable().await {
            Ok(_) => ControlResponse::Value(state_machine.get(&key).await),
            Err(error) => ControlResponse::Err(error.to_string()),
        },
        ControlRequest::Get { key } => ControlResponse::Value(state_machine.get(&key).await),
        ControlRequest::Status => {
            let metrics = raft.metrics().borrow().clone();
            let applied = state_machine
                .applied()
                .await
                .map(|log_id| log_id.index)
                .unwrap_or(0);
            ControlResponse::Status(NodeStatus {
                id: metrics.id,
                leader: metrics.current_leader,
                applied,
                last_log: metrics.last_log_index.unwrap_or(0),
            })
        }
    };
    let mut payload = serde_json::to_vec(&response)?;
    payload.push(b'\n');
    timeout(RPC_TIMEOUT, stream.write_all(&payload)).await??;
    Ok(())
}

fn create_data_dir(path: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(path)
        .map_err(|error| anyhow::anyhow!("create data dir {}: {error}", path.display()))
}

/// Configuration for one node process.
#[derive(Debug, Clone)]
pub struct TcpNodeConfig {
    pub id: NodeId,
    pub raft_addr: String,
    pub control_addr: String,
    /// Directory for the durable `raft.db` and `state.db` RocksDB stores.
    pub data_dir: PathBuf,
}

pub struct TcpNode {
    pub id: NodeId,
    pub raft_addr: String,
    pub control_addr: String,
    pub raft: Raft,
    pub state_machine: Arc<StateMachine>,
    tasks: Vec<JoinHandle<()>>,
}

impl TcpNode {
    pub async fn start(config: TcpNodeConfig) -> anyhow::Result<Self> {
        let raft_listener = TcpListener::bind(&config.raft_addr).await?;
        let control_listener = TcpListener::bind(&config.control_addr).await?;
        let raft_addr = raft_listener.local_addr()?.to_string();
        let control_addr = control_listener.local_addr()?.to_string();

        let raft_config = Arc::new(
            Config {
                heartbeat_interval: 100,
                election_timeout_min: 300,
                election_timeout_max: 600,
                ..Default::default()
            }
            .validate()?,
        );
        create_data_dir(&config.data_dir)?;
        let log_store = DurableLogStore::open(config.data_dir.join("raft.db"))?;
        let state_machine = Arc::new(StateMachine::open(config.data_dir.join("state.db"))?);
        let raft = Raft::new(
            config.id,
            raft_config,
            TcpNetworkFactory,
            log_store,
            state_machine.clone(),
        )
        .await?;

        let raft_task = tokio::spawn(serve_raft(raft_listener, raft.clone()));
        let control_task = tokio::spawn(serve_control(
            control_listener,
            raft.clone(),
            state_machine.clone(),
        ));
        Ok(Self {
            id: config.id,
            raft_addr,
            control_addr,
            raft,
            state_machine,
            tasks: vec![raft_task, control_task],
        })
    }

    /// Initialize a new cluster with this node and the supplied voters.
    pub async fn bootstrap(&self, members: BTreeMap<NodeId, String>) -> anyhow::Result<()> {
        initialize_cluster(&self.raft, members).await
    }

    pub async fn add_learner(
        &self,
        id: NodeId,
        addr: String,
        blocking: bool,
    ) -> anyhow::Result<u64> {
        add_learner(&self.raft, id, addr, blocking).await
    }

    pub async fn change_membership(&self, ids: BTreeSet<NodeId>) -> anyhow::Result<u64> {
        change_membership(&self.raft, ids).await
    }

    pub async fn shutdown(mut self) {
        for task in self.tasks.drain(..) {
            task.abort();
        }
        let _ = self.raft.shutdown().await;
    }
}
