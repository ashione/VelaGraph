//! Process node lifecycle. Raft peers and driver control use separate typed gRPC services.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::durable_log_store::DurableLogStore;
use crate::state_machine::StateMachine;
use crate::types::{LogCommand, NodeId, TypeConfig};

pub type Raft = openraft::Raft<TypeConfig>;

pub(crate) const RPC_TIMEOUT: Duration = Duration::from_secs(3);
pub(crate) const MAX_LINE: usize = 64 * 1024 * 1024;

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
    D1Capacity {
        shard: u64,
        epoch: u64,
    },
    D1SetCapacity {
        shard: u64,
        epoch: u64,
        expected_bytes: usize,
        new_bytes: usize,
    },
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
    D1Capacity(Result<crate::D1Capacity, crate::D1Error>),
    D1Admin(Result<crate::D1Result, crate::D1Error>),
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

/// Send exactly one typed control RPC. A timeout leaves mutation outcome unknown;
/// the client does not automatically retry or fall back to a legacy protocol.
pub async fn send_control(addr: &str, request: ControlRequest) -> anyhow::Result<ControlResponse> {
    crate::control_grpc::request(addr, request)
        .await?
        .into_result()
}

pub(crate) async fn execute_control(
    raft: Raft,
    state_machine: Arc<StateMachine>,
    request: ControlRequest,
) -> ControlResponse {
    let response = match request {
        ControlRequest::D1Capacity { shard, epoch } => match raft.ensure_linearizable().await {
            Ok(_) => ControlResponse::D1Capacity(state_machine.d1_capacity(shard, epoch).await),
            Err(error) => ControlResponse::Err(error.to_string()),
        },
        ControlRequest::D1SetCapacity {
            shard,
            epoch,
            expected_bytes,
            new_bytes,
        } => {
            match raft
                .client_write(LogCommand::D1V1(crate::D1Command::SetDedupCapacity {
                    shard,
                    epoch,
                    expected_bytes,
                    new_bytes,
                }))
                .await
            {
                Ok(reply) => match reply.data.d1 {
                    Some(result) => ControlResponse::D1Admin(result),
                    None => ControlResponse::Err("missing D1 capacity response".into()),
                },
                Err(error) => ControlResponse::Err(error.to_string()),
            }
        }
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
            // Each UTF-8 byte becomes at most four JSON bytes ("255,").
            // Keep 1 KiB for the fixed command/log/vote envelope. Raw public
            // Raft handles remain caller-managed and can submit larger entries.
            if key.len().saturating_add(value.len()) > (MAX_LINE - 1024) / 4 {
                return ControlResponse::Err(
                    "control write exceeds replication payload budget".into(),
                );
            }
            match raft.client_write(LogCommand::put(key, value)).await {
                Ok(response) => match response.data.d1 {
                    Some(Err(error)) => {
                        ControlResponse::Err(format!("D1 application rejected: {error:?}"))
                    }
                    _ => ControlResponse::Index(response.log_id.index),
                },
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
    response
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
    stop: watch::Sender<bool>,
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
        crate::node::bind_durable_identity(&config.data_dir, 0, config.id)?;
        let log_store = DurableLogStore::open(config.data_dir.join("raft.db"))?;
        let state_machine = Arc::new(StateMachine::open(config.data_dir.join("state.db"))?);
        let raft = Raft::new(
            config.id,
            raft_config,
            crate::raft_grpc::GrpcNetworkFactory,
            log_store,
            state_machine.clone(),
        )
        .await?;

        let (stop, receiver) = watch::channel(false);
        let raft_task = tokio::spawn(crate::raft_grpc::serve(
            raft_listener,
            config.id,
            raft.clone(),
            receiver.clone(),
        ));
        let control_task = tokio::spawn(crate::control_grpc::serve(
            control_listener,
            raft.clone(),
            state_machine.clone(),
            receiver,
        ));
        Ok(Self {
            id: config.id,
            raft_addr,
            control_addr,
            raft,
            state_machine,
            tasks: vec![raft_task, control_task],
            stop,
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
        // Once polled, cleanup owns the node even if the caller cancels its
        // wait. Otherwise cancellation between joining listeners and stopping
        // Raft would leave a live core retaining the durable stores.
        let cleanup = tokio::spawn(async move {
            let _ = self.stop.send(true);
            for task in self.tasks.drain(..) {
                let _ = task.await;
            }
            let _ = self.raft.shutdown().await;
        });
        let _ = cleanup.await;
    }
}
