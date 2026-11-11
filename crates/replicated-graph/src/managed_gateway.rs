//! Experimental plaintext TCP over trusted, pinned local or remote Raft nodes.
//! Standard Gremlin text reads and typed managed writes do not enable the older
//! remote Global Gremlin API, authenticated access, multi-host Raft placement,
//! discovery, or a durable read lease.
use crate::managed_graph::{self, GraphCohort, GraphSnapshot, Outcome, Plan};
pub use crate::managed_graph::{PropertyCondition, PropertyConditions};
use crate::managed_submission_timing::{self as timing, Attempt, Completion, Phase};
use crate::remote::query::{QueryCompleteness, QueryFailure, QueryRow};
use d2_raft::{
    cohort_reads,
    managed_rpc::{Client, NodeRef},
    participant_writes::Authority,
    transaction_decisions::Cohort,
    versioned_reads::{ReadLimit, Target},
    NodeHandle,
};
use rocksdb_sql_table::{
    experimental_graph::{CrossEdge, EdgePropertyPatch, EdgeRef, VertexPropertyPatch, VertexRef},
    Value, Vertex,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
#[path = "managed_gateway_jobs.rs"]
mod jobs;
#[path = "managed_gateway_local.rs"]
mod local;
pub use jobs::{JobGrant, JobHandle, JobPhase, JobPolicy, JobStatus};
#[path = "managed_query_lifecycle.rs"]
mod lifecycle;
#[path = "managed_gateway_pages.rs"]
mod pages;
#[path = "managed_gateway_query.rs"]
mod query;
pub use local::{LocalGraphRoute, LocalSession};
pub use pages::{PageCursor, QueryPage, MAX_PAGE_SIZE};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    task::{JoinHandle, JoinSet},
};

pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
pub const MAX_CONNECTIONS: usize = 8;
pub const MAX_SESSIONS: usize = 4;
/// Every materialization reserves both retained payload copies, including write
/// preparation snapshots and expired sessions still held by an active worker.
pub const SNAPSHOT_CHARGE: usize = 4 * 1024 * 1024;
/// Logical reservation for one immutable result, its bounded cursor registry,
/// query identity, and transient page/encoding copies. This is not an RSS bound.
pub const RESULT_CHARGE: usize = 4 * 1024 * 1024;
pub const SESSION_CHARGE: usize = SNAPSHOT_CHARGE + RESULT_CHARGE;
pub const MAX_RETAINED_BYTES: usize = MAX_SESSIONS * SESSION_CHARGE;
pub const MAX_TTL: Duration = Duration::from_secs(60);
pub const MAX_DEADLINE: Duration = Duration::from_secs(60);
const VERSION: u32 = 1;
static CLIENT_ADMISSION: Semaphore = Semaphore::const_new(MAX_CONNECTIONS);

/// Stable fail-closed transport and application errors. Unknown never means Abort.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, thiserror::Error)]
#[serde(deny_unknown_fields)]
pub enum Error {
    #[error("invalid managed gateway protocol")]
    Protocol,
    #[error("query job is not ready or has a pending lease round")]
    JobBusy,
    #[error("Gremlin query rejected: {0}")]
    Query(QueryFailure),
    #[error("binding, version, or request identity mismatch")]
    Identity,
    #[error("managed gateway resource limit exceeded")]
    Capacity,
    #[error("request rejected; an admitted request may remain pending, inspect or explicitly abort the same identity")]
    Rejected,
    #[error("property precondition did not match; this attempt did not admit a candidate, not a durable Abort")]
    PreconditionFailed,
    #[error("outcome unknown; retry or inspect the same external identity and command")]
    Unknown,
    #[error("session closed, expired, or from another gateway incarnation")]
    SessionUnavailable,
}
pub type Result<T> = std::result::Result<T, Error>;
impl From<managed_graph::Error> for Error {
    fn from(e: managed_graph::Error) -> Self {
        match e {
            managed_graph::Error::Identity => Self::Identity,
            managed_graph::Error::Uncertain => Self::Unknown,
            managed_graph::Error::Capacity => Self::Capacity,
            managed_graph::Error::InvalidConditions => Self::Protocol,
            managed_graph::Error::PreconditionFailed => Self::PreconditionFailed,
            managed_graph::Error::Snapshot(cohort_reads::Error::Unavailable) => Self::Unknown,
            managed_graph::Error::Snapshot(cohort_reads::Error::Expired) => {
                Self::SessionUnavailable
            }
            managed_graph::Error::Snapshot(cohort_reads::Error::Capacity) => Self::Capacity,
            _ => Self::Rejected,
        }
    }
}
/// Exact expected fixed-cohort contract. Partition numbers equal shard numbers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub authority: Authority,
    pub cohort: Cohort,
    pub mapping: Mapping,
}
/// The sole supported persistent partition-routing contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mapping {
    PartitionEqualsShardV1,
}
/// Partition-qualified point identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entity {
    pub partition: u32,
    pub id: u64,
}
impl Entity {
    fn vertex(self) -> VertexRef {
        VertexRef {
            partition: self.partition,
            id: self.id,
        }
    }
    fn edge(self) -> EdgeRef {
        EdgeRef {
            partition: self.partition,
            id: self.id,
        }
    }
}
/// Strict wire representation of a graph vertex.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VertexData {
    pub id: u64,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, Value>,
}
impl From<Vertex> for VertexData {
    fn from(v: Vertex) -> Self {
        Self {
            id: v.id,
            labels: v.labels,
            properties: v.properties,
        }
    }
}
/// Strict wire representation of a directed canonical edge.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EdgeData {
    pub id: u64,
    pub from: Entity,
    pub to: Entity,
    pub label: String,
    pub properties: BTreeMap<String, Value>,
}
impl EdgeData {
    fn graph(&self) -> CrossEdge {
        CrossEdge {
            id: self.id,
            from: self.from.vertex(),
            to: self.to.vertex(),
            label: self.label.clone(),
            properties: self.properties.clone(),
        }
    }
}
impl From<CrossEdge> for EdgeData {
    fn from(e: CrossEdge) -> Self {
        Self {
            id: e.id,
            from: Entity {
                partition: e.from.partition,
                id: e.from.id,
            },
            to: Entity {
                partition: e.to.partition,
                id: e.to.id,
            },
            label: e.label,
            properties: e.properties,
        }
    }
}
/// Property-only change; Null stores a value, while remove deletes the key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Patch {
    pub set: BTreeMap<String, Value>,
    pub remove: BTreeSet<String>,
}
/// No raw Plan, keys, routing list, or participant list is accepted remotely.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Mutation {
    CreateVertex {
        partition: u32,
        vertex: VertexData,
    },
    CreateEdge {
        edge: EdgeData,
    },
    PatchVertex {
        vertex: Entity,
        patch: Patch,
    },
    PatchEdge {
        edge: Entity,
        patch: Patch,
    },
    PatchVertexIf {
        vertex: Entity,
        conditions: PropertyConditions,
        patch: Patch,
    },
    PatchEdgeIf {
        edge: Entity,
        conditions: PropertyConditions,
        patch: Patch,
    },
    DeleteEdge {
        edge: Entity,
    },
    /// Delete the old relationship and create a distinct new identity at one V.
    ReplaceEdge {
        edge: Entity,
        replacement: EdgeData,
    },
}
impl Mutation {
    fn prepare(&self, snapshot: &GraphSnapshot) -> managed_graph::Result<Plan> {
        match self {
            Self::CreateVertex { partition, vertex } => snapshot.prepare_vertex(
                *partition,
                &Vertex {
                    id: vertex.id,
                    labels: vertex.labels.clone(),
                    properties: vertex.properties.clone(),
                },
            ),
            Self::CreateEdge { edge } => snapshot.prepare_edge(&edge.graph()),
            Self::PatchVertex { vertex, patch } => snapshot.prepare_vertex_properties(
                vertex.vertex(),
                &VertexPropertyPatch {
                    set: patch.set.clone(),
                    remove: patch.remove.clone(),
                },
            ),
            Self::PatchEdge { edge, patch } => snapshot.prepare_edge_properties(
                edge.edge(),
                &EdgePropertyPatch {
                    set: patch.set.clone(),
                    remove: patch.remove.clone(),
                },
            ),
            Self::DeleteEdge { edge } => snapshot.prepare_edge_delete(edge.edge()),
            Self::ReplaceEdge { edge, replacement } => {
                snapshot.prepare_edge_replace(edge.edge(), &replacement.graph())
            }
            Self::PatchVertexIf {
                vertex,
                conditions,
                patch,
            } => snapshot.prepare_vertex_properties_if(
                vertex.vertex(),
                conditions,
                &VertexPropertyPatch {
                    set: patch.set.clone(),
                    remove: patch.remove.clone(),
                },
            ),
            Self::PatchEdgeIf {
                edge,
                conditions,
                patch,
            } => snapshot.prepare_edge_properties_if(
                edge.edge(),
                conditions,
                &EdgePropertyPatch {
                    set: patch.set.clone(),
                    remove: patch.remove.clone(),
                },
            ),
        }
    }
    fn validate(&self) -> Result<()> {
        let values = match self {
            Self::CreateVertex { vertex, .. } => Some(&vertex.properties),
            Self::CreateEdge { edge } => Some(&edge.properties),
            Self::ReplaceEdge { replacement, .. } => Some(&replacement.properties),
            Self::PatchVertex { patch, .. }
            | Self::PatchEdge { patch, .. }
            | Self::PatchVertexIf { patch, .. }
            | Self::PatchEdgeIf { patch, .. } => Some(&patch.set),
            Self::DeleteEdge { .. } => None,
        };
        if values.is_some_and(|m| {
            m.values()
                .any(|v| matches!(v, Value::Real(f) if !f.is_finite()))
        }) {
            return Err(Error::Protocol);
        }
        if let Self::PatchVertexIf { conditions, .. } | Self::PatchEdgeIf { conditions, .. } = self
        {
            managed_graph::validate_property_conditions(conditions)?;
        }
        Ok(())
    }
}
/// JSON serialization of these fixed structs/enums/maps is the versioned
/// canonical encoding. Tagged Value preserves Integer(1) versus Real(1.0).
fn canonical(binding: &Binding, mutation: &Mutation) -> Result<Vec<u8>> {
    mutation.validate()?;
    encode(
        &("managed-graph-command-v1", binding, mutation),
        MAX_REQUEST_BYTES,
    )
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Write {
    pub external_id: [u8; 16],
    pub mutation: Mutation,
}
/// Tokens are random and tied to this server lifetime; they are not credentials.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Session {
    pub incarnation: [u8; 16],
    pub token: [u8; 32],
    pub version: u64,
}
/// Standard Gremlin text; partition locates explicit V/E starts. Traverse scans
/// use the complete fixed cohort, while still requiring a member partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub partition: u32,
    pub text: String,
}
/// The entire remote surface; no arbitrary plans or participant commands.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Operation {
    Submit(Write),
    Status(Write),
    Abort(Write),
    OpenSession {
        version: Option<u64>,
        ttl_millis: u64,
    },
    Read {
        session: Session,
        query: Query,
    },
    /// Bounded traversal over exactly the retained session snapshot.
    Traverse {
        session: Session,
        query: Query,
    },
    /// Replayable pages of one fully materialized bounded traversal.
    TraversePage {
        session: Session,
        query: Query,
        cursor: Option<PageCursor>,
        page_size: u16,
    },
    Close {
        session: Session,
    },
    /// Execute one bounded data-local query completely before returning Ready.
    OpenLocalQuery {
        version: Option<u64>,
        ttl_millis: u64,
        query: Query,
        page_size: u16,
    },
    /// Replay a page of a known immutable result, without accessing graph nodes.
    FetchLocalQuery {
        session: LocalSession,
        query: Query,
        cursor: Option<PageCursor>,
        page_size: u16,
    },
    CloseLocalQuery {
        session: LocalSession,
    },
    /// Process-local, operation-ID replayable admission. Accepted contains no rows.
    StartLocalQueryJob {
        operation_id: [u8; 16],
        version: Option<u64>,
        query: Query,
        page_size: u16,
        policy: JobPolicy,
    },
    GetLocalQueryJob {
        job: JobHandle,
    },
    RenewLocalQueryJob {
        job: JobHandle,
        expected_generation: u64,
    },
    FetchLocalQueryJob {
        job: JobHandle,
        query: Query,
        cursor: Option<PageCursor>,
        page_size: u16,
    },
    CancelLocalQueryJob {
        job: JobHandle,
    },
}
/// Durable decision and whole-cohort visibility are distinct observations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum WriteStatus {
    /// No admission existed at this leader-barrier observation. A delayed Submit
    /// may still admit later. Keep the same external ID and mutation.
    Absent,
    Pending,
    Committed {
        ordinal: u64,
        visible: bool,
    },
    Aborted {
        ordinal: u64,
        visible: bool,
    },
}
impl From<Outcome> for WriteStatus {
    fn from(o: Outcome) -> Self {
        match o {
            Outcome::Committed { ordinal, visible } => Self::Committed { ordinal, visible },
            Outcome::Aborted { ordinal, visible } => Self::Aborted { ordinal, visible },
        }
    }
}
/// Ordered Gremlin rows at the session version. Adjacency completeness remains
/// Unverified against arbitrary trusted low-level writes, separately from V.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueryData {
    #[serde(deserialize_with = "query::strict_rows")]
    pub rows: Vec<QueryRow>,
    pub completeness: QueryCompleteness,
}
/// Typed response whose identity is validated by the client.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Reply {
    Write {
        external_id: [u8; 16],
        transaction_id: Option<[u8; 16]>,
        status: WriteStatus,
    },
    OpenSession(Session),
    Read {
        session: Session,
        query: Query,
        data: QueryData,
    },
    Traverse {
        session: Session,
        query: Query,
        data: QueryData,
    },
    TraversePage {
        session: Session,
        query: Query,
        cursor: Option<PageCursor>,
        page_size: u16,
        page: QueryPage,
    },
    Close(Session),
    OpenLocalQuery {
        session: LocalSession,
        query: Query,
        page_size: u16,
        page: QueryPage,
    },
    FetchLocalQuery {
        session: LocalSession,
        query: Query,
        cursor: Option<PageCursor>,
        page_size: u16,
        page: QueryPage,
    },
    CloseLocalQuery(LocalSession),
    StartLocalQueryJob {
        operation_id: [u8; 16],
        job: JobHandle,
    },
    GetLocalQueryJob {
        job: JobHandle,
        status: JobStatus,
    },
    RenewLocalQueryJob {
        job: JobHandle,
        grant: JobGrant,
    },
    FetchLocalQueryJob {
        job: JobHandle,
        query: Query,
        cursor: Option<PageCursor>,
        page_size: u16,
        version: u64,
        page: QueryPage,
    },
    CancelLocalQueryJob {
        job: JobHandle,
        status: JobStatus,
    },
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    version: u32,
    request_id: [u8; 16],
    binding: Binding,
    operation: Operation,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    version: u32,
    request_id: [u8; 16],
    binding: Binding,
    result: Result<Reply>,
}

/// Handles stay owned for the service's lifetime. They must already be activated
/// with the exact persisted empty-baseline binding; no remote activation exists.
pub struct Config {
    pub coordinator: Arc<NodeHandle>,
    pub binding: Binding,
    pub members: Vec<Arc<NodeHandle>>,
}
/// Fixed internal RPC endpoints for nodes in separate trusted processes.
/// This changes placement only, not the external gateway wire or command identity.
pub struct RemoteConfig {
    pub coordinator: Client,
    pub binding: Binding,
    pub members: Vec<Client>,
}
enum OwnedNode {
    Local(Arc<NodeHandle>),
    Remote(Client),
}
impl OwnedNode {
    fn reference(&self) -> NodeRef<'_> {
        match self {
            Self::Local(node) => NodeRef::Local(node),
            Self::Remote(client) => NodeRef::Remote(client),
        }
    }
}
struct OwnedConfig {
    coordinator: OwnedNode,
    binding: Binding,
    members: Vec<OwnedNode>,
    local_routes: Option<Vec<LocalGraphRoute>>,
}
impl From<Config> for OwnedConfig {
    fn from(config: Config) -> Self {
        Self {
            local_routes: None,
            coordinator: OwnedNode::Local(config.coordinator),
            binding: config.binding,
            members: config.members.into_iter().map(OwnedNode::Local).collect(),
        }
    }
}
impl From<RemoteConfig> for OwnedConfig {
    fn from(config: RemoteConfig) -> Self {
        Self {
            local_routes: None,
            coordinator: OwnedNode::Remote(config.coordinator),
            binding: config.binding,
            members: config.members.into_iter().map(OwnedNode::Remote).collect(),
        }
    }
}
impl OwnedConfig {
    fn endpoints(&self) -> Vec<(NodeRef<'_>, Target)> {
        self.members
            .iter()
            .zip(&self.binding.cohort.members)
            .map(|(node, m)| {
                (
                    node.reference(),
                    Target {
                        shard: m.shard,
                        epoch: m.epoch,
                        mode: m.mode,
                    },
                )
            })
            .collect()
    }
    fn graph(&self) -> GraphCohort<'_> {
        GraphCohort::from_endpoints(
            self.coordinator.reference(),
            self.binding.authority.clone(),
            self.binding.cohort.coordinator_epoch,
            self.endpoints(),
        )
    }
    fn validate(&self) -> Result<()> {
        self.binding
            .cohort
            .validate()
            .map_err(|_| Error::Identity)?;
        if self.binding.authority.mode == [0; 16]
            || self.binding.authority.shard != self.binding.cohort.coordinator_shard
            || self.members.len() != self.binding.cohort.members.len()
            || self
                .binding
                .cohort
                .members
                .iter()
                .any(|m| m.shard > u64::from(u32::MAX))
        {
            return Err(Error::Identity);
        }
        Ok(())
    }
}
struct Retained {
    descriptor: Session,
    snapshot: GraphSnapshot,
    expires: Instant,
    _memory: OwnedSemaphorePermit,
    result: Mutex<Option<pages::Materialized>>,
    closed: std::sync::atomic::AtomicBool,
}
struct State {
    config: OwnedConfig,
    incarnation: [u8; 16],
    sessions: Mutex<BTreeMap<[u8; 32], Arc<Retained>>>,
    local_sessions: Mutex<BTreeMap<[u8; 32], Arc<local::Retained>>>,
    jobs: Mutex<BTreeMap<[u8; 16], Arc<jobs::Job>>>,
    job_tasks: Mutex<JoinSet<()>>,
    job_rpc_budget: Duration,
    jobs_stopping: std::sync::atomic::AtomicBool,
    memory: Arc<Semaphore>,
    connections: Arc<Semaphore>,
    submit: tokio::sync::Mutex<()>,
    timing: Option<timing::Sender>,
}
impl State {
    fn expire(&self) {
        self.expire_jobs();
        self.local_sessions
            .lock()
            .unwrap()
            .retain(|_, s| Instant::now() < s.expires);
        self.sessions
            .lock()
            .unwrap()
            .retain(|_, s| Instant::now() < s.expires);
    }
    fn get(&self, session: &Session) -> Result<Arc<Retained>> {
        self.expire();
        if session.incarnation != self.incarnation {
            return Err(Error::SessionUnavailable);
        }
        let sessions = self.sessions.lock().unwrap();
        let found = sessions
            .get(&session.token)
            .ok_or(Error::SessionUnavailable)?;
        if found.descriptor != *session {
            return Err(Error::Identity);
        }
        Ok(found.clone())
    }
    async fn capture(
        &self,
        version: Option<u64>,
        ttl: Duration,
        work: Arc<OwnedSemaphorePermit>,
    ) -> Result<Arc<Retained>> {
        self.expire();
        let memory = self
            .memory
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let expires = Instant::now() + ttl;
        let captured = cohort_reads::acquire_endpoints(
            self.config.coordinator.reference(),
            self.config.binding.authority.clone(),
            self.config.binding.cohort.coordinator_epoch,
            self.config.endpoints(),
            version,
            ReadLimit {
                rows: 10000,
                bytes: SNAPSHOT_CHARGE / 2,
                operations: 100000,
            },
            ttl,
        )
        .await
        .map_err(managed_graph::Error::from)?;
        if captured.cohort().map_err(managed_graph::Error::from)? != &self.config.binding.cohort {
            return Err(Error::Identity);
        }
        let descriptor = Session {
            incarnation: self.incarnation,
            token: random()?,
            version: captured.version().map_err(managed_graph::Error::from)?,
        };
        tokio::task::spawn_blocking(move || {
            let _work = work;
            let snapshot = GraphSnapshot::from_session(captured)?;
            if Instant::now() >= expires {
                return Err(Error::SessionUnavailable);
            }
            Ok(Arc::new(Retained {
                descriptor,
                snapshot,
                expires,
                _memory: memory,
                result: Mutex::new(None),
                closed: std::sync::atomic::AtomicBool::new(false),
            }))
        })
        .await
        .map_err(|_| Error::Unknown)?
    }
    async fn execute(
        self: &Arc<Self>,
        operation: Operation,
        work: Arc<OwnedSemaphorePermit>,
        end: Instant,
    ) -> Result<Reply> {
        before_deadline(end)?;
        match operation {
            Operation::Submit(write) => {
                let mut attempt = Attempt::new(self.timing.as_ref(), write.external_id);
                let trace = &attempt.recorder;
                let result = async {
                    // Only admitted connections wait for this lock; no detached write
                    // queues. Status, Abort and reads never acquire this mutex.
                    let queue = trace.span(Phase::Queue);
                    let _serial = self.submit.lock().await;
                    drop(queue);
                    let canonical_span = trace.span(Phase::Canonical);
                    before_deadline(end)?;
                    let bytes = canonical(&self.config.binding, &write.mutation)?;
                    if write.external_id == [0; 16] {
                        return Err(Error::Identity);
                    }
                    drop(canonical_span);
                    let graph = self.config.graph().with_timing(trace.clone());
                    let recovery = trace.span(Phase::Recovery);
                    let recovered = graph.recover_request(write.external_id, &bytes).await?;
                    drop(recovery);
                    let plan = match recovered {
                        Some(plan) => plan,
                        None => {
                            before_deadline(end)?;
                            let snapshot_span = trace.span(Phase::Snapshot);
                            let snapshot = self.capture(None, MAX_TTL, work.clone()).await?;
                            before_deadline(end)?;
                            drop(snapshot_span);
                            let validation = trace.span(Phase::Validation);
                            let mutation = write.mutation;
                            let candidate = tokio::task::spawn_blocking(move || {
                                let _work = work;
                                mutation.prepare(&snapshot.snapshot)
                            })
                            .await
                            .map_err(|_| Error::Unknown)??;
                            before_deadline(end)?;
                            drop(validation);
                            let _admission = trace.span(Phase::Admission);
                            graph
                                .admit_request(write.external_id, bytes, &candidate)
                                .await?
                        }
                    };
                    before_deadline(end)?;
                    let status = graph.execute(&plan).await?.into();
                    Ok(Reply::Write {
                        external_id: write.external_id,
                        transaction_id: Some(plan.id()),
                        status,
                    })
                }
                .await;
                attempt.completion = if result.is_ok() {
                    Completion::Returned
                } else {
                    Completion::Error
                };
                result
            }

            operation @ (Operation::Status(_) | Operation::Abort(_)) => {
                let abort = matches!(operation, Operation::Abort(_));
                let write = match operation {
                    Operation::Status(w) | Operation::Abort(w) => w,
                    _ => unreachable!(),
                };
                if write.external_id == [0; 16] {
                    return Err(Error::Identity);
                }
                let graph = self.config.graph();
                let bytes = canonical(&self.config.binding, &write.mutation)?;
                let Some(plan) = graph.recover_request(write.external_id, &bytes).await? else {
                    return Ok(Reply::Write {
                        external_id: write.external_id,
                        transaction_id: None,
                        status: WriteStatus::Absent,
                    });
                };
                before_deadline(end)?;
                let status = if abort {
                    graph.abort(&plan).await?.into()
                } else {
                    graph
                        .status(&plan)
                        .await?
                        .map_or(WriteStatus::Pending, Into::into)
                };
                Ok(Reply::Write {
                    external_id: write.external_id,
                    transaction_id: Some(plan.id()),
                    status,
                })
            }
            Operation::OpenSession {
                version,
                ttl_millis,
            } => {
                let ttl = Duration::from_millis(ttl_millis);
                if ttl.is_zero() || ttl > MAX_TTL {
                    return Err(Error::Protocol);
                }
                let session = self.capture(version, ttl, work).await?;
                before_deadline(end)?;
                let descriptor = session.descriptor.clone();
                let mut sessions = self.sessions.lock().unwrap();
                if sessions.contains_key(&descriptor.token) {
                    return Err(Error::Unknown);
                }
                sessions.insert(descriptor.token, session);
                Ok(Reply::OpenSession(descriptor))
            }
            Operation::Read { session, query } => {
                let retained = self.get(&session)?;
                if !self
                    .config
                    .binding
                    .cohort
                    .members
                    .iter()
                    .any(|m| m.shard == u64::from(query.partition))
                {
                    return Err(Error::Identity);
                }
                tokio::task::spawn_blocking(move || {
                    let _work = work;
                    before_deadline(end)?;
                    let data = query::execute(&retained.snapshot, &query, end)?;
                    retained.snapshot.version()?;
                    before_deadline(end)?;
                    Ok(Reply::Read {
                        session,
                        query,
                        data,
                    })
                })
                .await
                .map_err(|_| Error::Unknown)?
            }
            operation @ (Operation::Traverse { .. } | Operation::TraversePage { .. }) => {
                let (session, query, cursor, page_size) = match operation {
                    Operation::Traverse { session, query } => (session, query, None, None),
                    Operation::TraversePage {
                        session,
                        query,
                        cursor,
                        page_size,
                    } => (session, query, cursor, Some(page_size)),
                    _ => unreachable!(),
                };
                let retained = self.get(&session)?;
                let binding = self.config.binding.clone();
                tokio::task::spawn_blocking(move || {
                    let _work = work;
                    pages::execute(
                        &retained,
                        session,
                        query,
                        (cursor, page_size),
                        &binding,
                        end,
                    )
                })
                .await
                .map_err(|_| Error::Unknown)?
            }
            Operation::OpenLocalQuery {
                version,
                ttl_millis,
                query,
                page_size,
            } => {
                self.open_local(version, ttl_millis, query, page_size, work, end)
                    .await
            }
            Operation::FetchLocalQuery {
                session,
                query,
                cursor,
                page_size,
            } => self.fetch_local(session, query, cursor, page_size, end),
            Operation::CloseLocalQuery { session } => {
                self.get_local(&session)?
                    .closed
                    .store(true, std::sync::atomic::Ordering::Release);
                self.local_sessions.lock().unwrap().remove(&session.token);
                Ok(Reply::CloseLocalQuery(session))
            }
            Operation::StartLocalQueryJob {
                operation_id,
                version,
                query,
                page_size,
                policy,
            } => self.start_job(operation_id, version, query, page_size, policy),
            Operation::GetLocalQueryJob { job } => self.job_status(job, false),
            Operation::CancelLocalQueryJob { job } => self.job_status(job, true),
            Operation::RenewLocalQueryJob {
                job,
                expected_generation,
            } => tokio::time::timeout_at(
                tokio::time::Instant::from_std(end),
                self.renew_job(job, expected_generation, end),
            )
            .await
            .map_err(|_| Error::Unknown)?,
            Operation::FetchLocalQueryJob {
                job,
                query,
                cursor,
                page_size,
            } => self.fetch_job(job, query, cursor, page_size),
            Operation::Close { session } => {
                self.get(&session)?
                    .closed
                    .store(true, std::sync::atomic::Ordering::Release);
                self.sessions.lock().unwrap().remove(&session.token);
                Ok(Reply::Close(session))
            }
        }
    }
}
/// Separate bounded listener. Dropping/shutdown loses sessions, never aborts a
/// durable request. A lost/late response always leaves the write outcome unknown.
pub struct Gateway {
    address: SocketAddr,
    state: Arc<State>,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl Gateway {
    pub async fn bind(config: Config, bind: SocketAddr, deadline: Duration) -> Result<Self> {
        Self::bind_config(config.into(), bind, deadline, None).await
    }
    /// Serve the unchanged graph protocol over fixed trusted internal endpoints.
    pub async fn bind_remote(
        config: RemoteConfig,
        bind: SocketAddr,
        deadline: Duration,
    ) -> Result<Self> {
        Self::bind_config(config.into(), bind, deadline, None).await
    }
    /// Opt in to query-bound data-local reads on explicitly pinned graph ports.
    /// Each graph identity must match the corresponding managed Client and Binding.
    pub async fn bind_remote_local_queries(
        config: RemoteConfig,
        routes: Vec<LocalGraphRoute>,
        bind: SocketAddr,
        deadline: Duration,
    ) -> Result<Self> {
        local::validate_routes(&config, &routes)?;
        let mut config: OwnedConfig = config.into();
        config.local_routes = Some(routes);
        Self::bind_config(config, bind, deadline, None).await
    }
    /// Opt in to bounded local phase reports; draining/dropping them never gates writes.
    pub async fn bind_with_timing(
        config: Config,
        bind: SocketAddr,
        deadline: Duration,
    ) -> Result<(Self, timing::Reports)> {
        let (sender, reports) = timing::channel();
        let gateway = Self::bind_config(config.into(), bind, deadline, Some(sender)).await?;
        Ok((gateway, reports))
    }
    /// Same diagnostics with fixed remote participant/coordinator endpoints.
    pub async fn bind_remote_with_timing(
        config: RemoteConfig,
        bind: SocketAddr,
        deadline: Duration,
    ) -> Result<(Self, timing::Reports)> {
        let (sender, reports) = timing::channel();
        let gateway = Self::bind_config(config.into(), bind, deadline, Some(sender)).await?;
        Ok((gateway, reports))
    }
    async fn bind_config(
        config: OwnedConfig,
        bind: SocketAddr,
        deadline: Duration,
        timing: Option<timing::Sender>,
    ) -> Result<Self> {
        check_deadline(deadline)?;
        config.validate()?;
        let state = Arc::new(State {
            config,
            timing,
            incarnation: random()?,
            sessions: Mutex::new(BTreeMap::new()),
            local_sessions: Mutex::new(BTreeMap::new()),
            jobs: Mutex::new(BTreeMap::new()),
            job_tasks: Mutex::new(JoinSet::new()),
            job_rpc_budget: deadline.min(Duration::from_secs(5)),
            jobs_stopping: std::sync::atomic::AtomicBool::new(false),
            memory: Arc::new(Semaphore::new(MAX_SESSIONS)),
            connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
            submit: tokio::sync::Mutex::new(()),
        });
        // Validate live identities and persistent zero baseline before accepting.
        tokio::time::timeout(
            deadline,
            state.config.graph().snapshot(
                Some(0),
                ReadLimit {
                    rows: 10000,
                    bytes: SNAPSHOT_CHARGE / 2,
                    operations: 100000,
                },
                MAX_TTL,
            ),
        )
        .await
        .map_err(|_| Error::Unknown)??;
        let listener = TcpListener::bind(bind).await.map_err(|_| Error::Unknown)?;
        let address = listener.local_addr().map_err(|_| Error::Unknown)?;
        let (stop, mut stopped) = watch::channel(false);
        let worker = state.clone();
        let task = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            let mut cleanup = tokio::time::interval(Duration::from_millis(50));
            loop {
                let accepted = tokio::select! {
                    _ = stopped.changed() => break,
                    _ = cleanup.tick() => { worker.expire(); continue; },
                    _ = handlers.join_next(), if !handlers.is_empty() => continue,
                    accepted = listener.accept() => accepted,
                };
                let Ok((socket, _)) = accepted else {
                    break;
                };
                let Ok(permit) = worker.connections.clone().try_acquire_owned() else {
                    continue;
                };
                let state = worker.clone();
                let end = Instant::now() + deadline;
                handlers.spawn(async move {
                    let work = Arc::new(permit);
                    if Instant::now() < end {
                        let _ = tokio::time::timeout_at(
                            tokio::time::Instant::from_std(end),
                            serve(socket, state, work.clone(), end),
                        )
                        .await;
                    }
                });
            }
            worker.cancel_jobs();
            handlers.abort_all();
            while handlers.join_next().await.is_some() {}
            let mut jobs = std::mem::take(&mut *worker.job_tasks.lock().unwrap());
            while jobs.join_next().await.is_some() {}
            worker.jobs.lock().unwrap().clear();
            worker.sessions.lock().unwrap().clear();
            worker.local_sessions.lock().unwrap().clear();
        });
        Ok(Self {
            address,
            state,
            stop,
            task: Some(task),
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    /// Conservative logical reservation for snapshots, bounded result/cursor
    /// storage, scratch copies and active captures; not an allocator/RSS bound.
    pub fn retained_payload_charge(&self) -> usize {
        (MAX_SESSIONS - self.state.memory.available_permits()) * SESSION_CHARGE
    }
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}
// An Open is not retained on cancellation, encode failure or a known send
// failure. Successful kernel write is not proof the peer received the descriptor;
// those unknowable lost replies are reclaimed by the ordinary TTL.
struct OpenDelivery {
    state: Arc<State>,
    token: Option<[u8; 32]>,
    local_token: Option<[u8; 32]>,
}
impl Drop for OpenDelivery {
    fn drop(&mut self) {
        if let Some(token) = self.local_token {
            self.state.local_sessions.lock().unwrap().remove(&token);
        }
        if let Some(token) = self.token {
            self.state.sessions.lock().unwrap().remove(&token);
        }
    }
}
async fn serve(
    mut socket: TcpStream,
    state: Arc<State>,
    work: Arc<OwnedSemaphorePermit>,
    end: Instant,
) -> Result<()> {
    let request: Request = read_frame(&mut socket, MAX_REQUEST_BYTES).await?;
    let end = if matches!(
        request.operation,
        Operation::StartLocalQueryJob { .. }
            | Operation::GetLocalQueryJob { .. }
            | Operation::RenewLocalQueryJob { .. }
            | Operation::FetchLocalQueryJob { .. }
            | Operation::CancelLocalQueryJob { .. }
    ) {
        end.min(Instant::now() + Duration::from_secs(5))
    } else if matches!(
        request.operation,
        Operation::Read { .. }
            | Operation::Traverse { .. }
            | Operation::TraversePage { .. }
            | Operation::OpenLocalQuery { .. }
            | Operation::FetchLocalQuery { .. }
    ) {
        end.min(Instant::now() + rocksdb_sql_table::GREMLIN_DEADLINE)
    } else {
        end
    };
    if Instant::now() >= end {
        return Err(Error::Unknown);
    }
    let result = if request.version != VERSION || request.request_id == [0; 16] {
        Err(Error::Protocol)
    } else if request.binding != state.config.binding {
        Err(Error::Identity)
    } else {
        state.execute(request.operation, work.clone(), end).await
    };
    let mut delivery = OpenDelivery {
        state: state.clone(),
        local_token: match &result {
            Ok(Reply::OpenLocalQuery { session, .. }) => Some(session.token),
            _ => None,
        },
        token: match &result {
            Ok(Reply::OpenSession(s)) => Some(s.token),
            _ => None,
        },
    };
    let mut response = Response {
        version: VERSION,
        request_id: request.request_id,
        binding: state.config.binding.clone(),
        result,
    };
    // Retain the result reservation through encoding and the complete send,
    // even when another request closes/expires the registry entry meanwhile.
    let local_retained = match &response.result {
        Ok(Reply::OpenLocalQuery { session, .. } | Reply::FetchLocalQuery { session, .. }) => {
            Some(state.get_local(session)?)
        }
        _ => None,
    };
    let job_retained = match &response.result {
        Ok(Reply::FetchLocalQueryJob { job, .. }) => Some(state.get_job(job)?),
        _ => None,
    };
    let expiry = match &response.result {
        Ok(
            Reply::OpenSession(s)
            | Reply::Read { session: s, .. }
            | Reply::Traverse { session: s, .. }
            | Reply::TraversePage { session: s, .. },
        ) => Some(state.get(s)?.expires),
        Ok(Reply::OpenLocalQuery { session, .. } | Reply::FetchLocalQuery { session, .. }) => {
            Some(state.get_local(session)?.expires)
        }
        Ok(Reply::RenewLocalQueryJob { job, .. }) => Some(state.get_job(job)?.deadline()?),
        _ => None,
    };
    let (bytes, success, local_retained, job_retained) = tokio::task::spawn_blocking(move || {
        let _work = work;
        let encoded = match encode(&response, MAX_RESPONSE_BYTES) {
            Ok(bytes) => bytes,
            Err(_) => {
                response.result = Err(Error::Capacity);
                encode(&response, MAX_RESPONSE_BYTES)?
            }
        };
        if expiry.is_some_and(|end| Instant::now() >= end)
            || local_retained
                .as_ref()
                .is_some_and(|retained| retained.closed.load(std::sync::atomic::Ordering::Acquire))
        {
            return Err(Error::SessionUnavailable);
        }
        if Instant::now() >= end {
            return Err(Error::Unknown);
        }
        if let Some(job) = &job_retained {
            job.live_ready()?;
        }
        Ok((
            encoded,
            response.result.is_ok(),
            local_retained,
            job_retained,
        ))
    })
    .await
    .map_err(|_| Error::Unknown)??;
    if Instant::now() >= end {
        return Err(Error::Unknown);
    }
    if let Some(retained) = &local_retained {
        // The blocking encoder may finish before TTL while this task resumes
        // after it. timeout_at alone can poll an immediately-ready write first.
        local::live(retained, end)?;
    }
    if let Some(job) = &job_retained {
        job.live_ready()?;
    }
    let write_end = expiry.map_or(end, |expiry| expiry.min(end));
    let write_end = if let Some(job) = &job_retained {
        write_end.min(job.deadline()?)
    } else {
        write_end
    };
    let sent = tokio::time::timeout_at(
        tokio::time::Instant::from_std(write_end),
        write_bytes(&mut socket, &bytes),
    )
    .await
    .map_err(|_| {
        if expiry.is_some_and(|expiry| Instant::now() >= expiry) {
            Error::SessionUnavailable
        } else {
            Error::Unknown
        }
    })?;
    if sent.is_ok() && success {
        delivery.token = None;
        delivery.local_token = None;
    }
    sent
}
/// Performs no automatic retry. Reuse `Write.external_id` and exact mutation after
/// Unknown. Every accepted reply is checked against the request and fixed binding.
pub async fn request(
    address: SocketAddr,
    binding: Binding,
    operation: Operation,
    deadline: Duration,
) -> Result<Reply> {
    check_deadline(deadline)?;
    let _permit = CLIENT_ADMISSION
        .try_acquire()
        .map_err(|_| Error::Capacity)?;
    let request = Request {
        version: VERSION,
        request_id: random()?,
        binding,
        operation,
    };
    let end = Instant::now() + deadline;
    tokio::time::timeout_at(tokio::time::Instant::from_std(end), async {
        let bytes = encode(&request, MAX_REQUEST_BYTES)?;
        if Instant::now() >= end {
            return Err(Error::Unknown);
        }
        let mut socket = TcpStream::connect(address)
            .await
            .map_err(|_| Error::Unknown)?;
        write_bytes(&mut socket, &bytes).await?;
        let response: Response = read_frame(&mut socket, MAX_RESPONSE_BYTES).await?;
        let result = validate_reply(&request, response);
        if Instant::now() >= end {
            return Err(Error::Unknown);
        }
        result
    })
    .await
    .map_err(|_| Error::Unknown)?
}
fn validate_reply(request: &Request, response: Response) -> Result<Reply> {
    if response.version != VERSION
        || response.request_id != request.request_id
        || response.binding != request.binding
    {
        return Err(Error::Identity);
    }
    let reply = response.result?;
    let valid = match (&request.operation, &reply) {
        (
            Operation::Submit(w) | Operation::Status(w) | Operation::Abort(w),
            Reply::Write {
                external_id,
                transaction_id,
                status,
            },
        ) => {
            *external_id == w.external_id
                && match status {
                    WriteStatus::Absent => {
                        transaction_id.is_none()
                            && !matches!(request.operation, Operation::Submit(_))
                    }
                    WriteStatus::Pending => {
                        transaction_id.is_some_and(|id| id != [0; 16])
                            && matches!(request.operation, Operation::Status(_))
                    }
                    WriteStatus::Committed { ordinal, .. }
                    | WriteStatus::Aborted { ordinal, .. } => {
                        *ordinal > 0 && transaction_id.is_some_and(|id| id != [0; 16])
                    }
                }
        }
        (Operation::OpenSession { version, .. }, Reply::OpenSession(s)) => {
            s.incarnation != [0; 16] && s.token != [0; 32] && version.is_none_or(|v| v == s.version)
        }
        (
            Operation::Read { session, query },
            Reply::Read {
                session: s,
                query: q,
                data,
            },
        ) => session == s && query == q && query::valid_output(query, data, &request.binding),
        (
            Operation::Traverse { session, query },
            Reply::Traverse {
                session: s,
                query: q,
                data,
            },
        ) => {
            session == s
                && query == q
                && query::valid_traversal_output(query, data, &request.binding)
                && data.rows.len() <= pages::MAX_ROWS
                && encode(data, pages::MAX_RESULT_BYTES).is_ok()
        }
        (
            Operation::TraversePage {
                session,
                query,
                cursor,
                page_size,
            },
            Reply::TraversePage {
                session: s,
                query: q,
                cursor: c,
                page_size: size,
                page,
            },
        ) => {
            session == s
                && query == q
                && cursor == c
                && page_size == size
                && pages::valid_page(query, cursor.as_ref(), *page_size, page, &request.binding)
        }
        (Operation::Close { session }, Reply::Close(s)) => session == s,
        (
            Operation::OpenLocalQuery {
                version,
                query,
                page_size,
                ..
            },
            Reply::OpenLocalQuery {
                session,
                query: q,
                page_size: size,
                page,
            },
        ) => {
            session.incarnation != [0; 16]
                && session.token != [0; 32]
                && version.is_none_or(|v| v == session.version)
                && query == q
                && page_size == size
                && local::valid_page(query, None, *page_size, page, &request.binding)
        }
        (
            Operation::FetchLocalQuery {
                session,
                query,
                cursor,
                page_size,
            },
            Reply::FetchLocalQuery {
                session: s,
                query: q,
                cursor: c,
                page_size: size,
                page,
            },
        ) => {
            session == s
                && query == q
                && cursor == c
                && page_size == size
                && local::valid_page(query, cursor.as_ref(), *page_size, page, &request.binding)
        }
        (Operation::CloseLocalQuery { session }, Reply::CloseLocalQuery(s)) => session == s,
        (
            Operation::StartLocalQueryJob { operation_id, .. },
            Reply::StartLocalQueryJob {
                operation_id: id,
                job,
            },
        ) => operation_id == id && job.incarnation != [0; 16] && job.token != [0; 32],
        (Operation::GetLocalQueryJob { job }, Reply::GetLocalQueryJob { job: j, status }) => {
            job == j && jobs::valid_status(status)
        }
        (Operation::CancelLocalQueryJob { job }, Reply::CancelLocalQueryJob { job: j, status }) => {
            job == j
                && jobs::valid_status(status)
                && matches!(
                    status.phase,
                    JobPhase::Cancelled | JobPhase::Expired | JobPhase::Failed
                )
        }
        (
            Operation::RenewLocalQueryJob {
                job,
                expected_generation,
            },
            Reply::RenewLocalQueryJob { job: j, grant },
        ) => {
            job == j
                && expected_generation.checked_add(1) == Some(grant.generation)
                && (1..=60_000).contains(&grant.remaining_millis)
        }
        (
            Operation::FetchLocalQueryJob {
                job,
                query,
                cursor,
                page_size,
            },
            Reply::FetchLocalQueryJob {
                job: j,
                query: q,
                cursor: c,
                page_size: size,
                page,
                ..
            },
        ) => {
            job == j
                && query == q
                && cursor == c
                && page_size == size
                && local::valid_page(query, cursor.as_ref(), *page_size, page, &request.binding)
        }
        _ => false,
    };
    if valid {
        Ok(reply)
    } else {
        Err(Error::Identity)
    }
}
fn before_deadline(end: Instant) -> Result<()> {
    if Instant::now() >= end {
        Err(Error::Unknown)
    } else {
        Ok(())
    }
}
fn check_deadline(deadline: Duration) -> Result<()> {
    if deadline.is_zero() || deadline > MAX_DEADLINE {
        Err(Error::Protocol)
    } else {
        Ok(())
    }
}
fn random<const N: usize>() -> Result<[u8; N]> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(|_| Error::Unknown)?;
    if bytes == [0; N] {
        return Err(Error::Unknown);
    }
    Ok(bytes)
}
/// Refuse growth before allocation, including JSON escaping and byte expansion.
fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    struct Bounded {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl io::Write for Bounded {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(io::Error::other("frame limit"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| Error::Capacity)?;
    Ok(writer.bytes)
}
async fn read_frame<T: DeserializeOwned>(socket: &mut TcpStream, limit: usize) -> Result<T> {
    let len = socket.read_u32().await.map_err(|_| Error::Unknown)? as usize;
    if len == 0 || len > limit {
        return Err(Error::Capacity);
    }
    let mut body = vec![0; len];
    socket
        .read_exact(&mut body)
        .await
        .map_err(|_| Error::Unknown)?;
    serde_json::from_slice(&body).map_err(|_| Error::Protocol)
}
async fn write_bytes(socket: &mut TcpStream, bytes: &[u8]) -> Result<()> {
    socket
        .write_u32(u32::try_from(bytes.len()).map_err(|_| Error::Capacity)?)
        .await
        .map_err(|_| Error::Unknown)?;
    socket.write_all(bytes).await.map_err(|_| Error::Unknown)
}
#[cfg(test)]
#[path = "managed_gateway_tests.rs"]
mod tests;
