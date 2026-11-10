//! Opt-in trusted local/remote graph/v2 transactions over an empty fixed cohort.
//! No remote Global Gremlin admission. Plans are immutable in-memory retry values.
use crate::managed_submission_timing::{Phase, Recorder};
use d2_raft::participant_writes::Authority;
use d2_raft::request_index::RequestLookup;
use d2_raft::target_evidence::TargetEvidence;
use d2_raft::transaction_decisions::{
    Cohort, Command, Decision, HistoryCursor, Participant, Transaction,
};
use d2_raft::{
    cohort_reads,
    managed_rpc::{self, NodeRef},
    versioned_reads, D1Error, D1WriteRequest, NodeHandle,
};
use rocksdb_sql_table::experimental_graph::{
    AppendOnlyStore, CrossEdge, EdgeRecord, EdgeRef, ExperimentalError, PreparedBatch, VertexRef,
};
use rocksdb_sql_table::{ReadView, StorageNodeError, Vertex, WriteOp};
use std::collections::BTreeMap;
use std::time::Duration;

#[path = "managed_property_conditions.rs"]
mod conditions;
pub(crate) use conditions::validate as validate_property_conditions;
pub use conditions::{PropertyCondition, PropertyConditions};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("cohort snapshot: {0:?}")]
    Snapshot(cohort_reads::Error),
    #[error("graph: {0}")]
    Graph(#[from] ExperimentalError),
    #[error("storage: {0}")]
    Storage(#[from] StorageNodeError),
    #[error("transaction identity or mapping mismatch")]
    Identity,
    #[error("transaction result uncertain; retain and retry the same plan")]
    Uncertain,
    #[error("participant command: {0:?}")]
    Application(D1Error),
    #[error("transaction plan exceeds limits")]
    Capacity,
    #[error("invalid managed property conditions")]
    InvalidConditions,
    #[error(
        "managed property precondition did not match; no candidate was admitted by this attempt"
    )]
    PreconditionFailed,
}
impl From<cohort_reads::Error> for Error {
    fn from(e: cohort_reads::Error) -> Self {
        Self::Snapshot(e)
    }
}
impl From<managed_rpc::Error> for Error {
    fn from(error: managed_rpc::Error) -> Self {
        match error {
            managed_rpc::Error::Application(e) => Self::Application(e),
            managed_rpc::Error::Identity | managed_rpc::Error::Protocol => Self::Identity,
            managed_rpc::Error::Capacity => Self::Capacity,
            managed_rpc::Error::Unavailable => Self::Uncertain,
        }
    }
}
pub type Result<T> = std::result::Result<T, Error>;

/// Decision is authoritative even when not every member has consumed its prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Committed { ordinal: u64, visible: bool },
    Aborted { ordinal: u64, visible: bool },
}
#[derive(Debug, Clone, PartialEq, Eq)]
struct Part {
    descriptor: Participant,
    request: D1WriteRequest,
}
/// Keep this exact value across every uncertain result. Cloning retains identity;
/// preparing again creates a different transaction and is not a retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    id: [u8; 16],
    authority: Authority,
    cohort: Cohort,
    version: u64,
    parts: Vec<Part>,
}
impl Plan {
    fn durable_bytes(&self) -> Result<Vec<u8>> {
        d2_raft::request_plans::Plan {
            version: 1,
            id: self.id,
            authority: self.authority.clone(),
            cohort: self.cohort.clone(),
            validation_version: self.version,
            parts: self
                .parts
                .iter()
                .map(|p| d2_raft::request_plans::Part {
                    descriptor: p.descriptor.clone(),
                    request: p.request.clone(),
                })
                .collect(),
        }
        .encode()
        .map_err(|e| Error::Application(D1Error::CoordinatorRejected(e)))
    }
    // Graph semantics originate in a trusted admitted plan, not remote raw bytes.
    fn from_durable(bytes: &[u8]) -> Result<Self> {
        let plan = d2_raft::request_plans::Plan::decode(bytes)
            .map_err(|e| Error::Application(D1Error::CoordinatorRejected(e)))?;
        for member in &plan.cohort.members {
            u32::try_from(member.shard).map_err(|_| Error::Identity)?;
        }
        Ok(Self {
            id: plan.id,
            authority: plan.authority,
            cohort: plan.cohort,
            version: plan.validation_version,
            parts: plan
                .parts
                .into_iter()
                .map(|p| Part {
                    descriptor: p.descriptor,
                    request: p.request,
                })
                .collect(),
        })
    }
    pub fn id(&self) -> [u8; 16] {
        self.id
    }
    pub fn validation_version(&self) -> u64 {
        self.version
    }
    pub fn participants(&self) -> usize {
        self.parts.len()
    }
}

pub struct GraphSnapshot {
    session: cohort_reads::SnapshotSession,
    views: BTreeMap<u64, ReadView>,
}
impl GraphSnapshot {
    pub fn from_session(session: cohort_reads::SnapshotSession) -> Result<Self> {
        // Explicit deterministic mapping avoids caller-supplied unbound routing.
        let mut views = BTreeMap::new();
        for m in &session.cohort()?.members {
            u32::try_from(m.shard).map_err(|_| Error::Identity)?;
            let snapshot = session.shard(m.shard)?;
            views.insert(
                m.shard,
                ReadView::from_entries(
                    snapshot.shard,
                    snapshot.epoch,
                    snapshot.local_sequence,
                    snapshot.entries.clone(),
                )?,
            );
        }
        session.version()?;
        Ok(Self { session, views })
    }
    pub fn version(&self) -> Result<u64> {
        Ok(self.session.version()?)
    }
    pub(crate) fn store(
        &self,
        partition: u32,
    ) -> Result<AppendOnlyStore<crate::remote::query::FrozenNode>> {
        self.session.version()?;
        let view = self
            .views
            .get(&u64::from(partition))
            .ok_or(Error::Identity)?
            .clone();
        Ok(AppendOnlyStore::new(
            crate::remote::query::FrozenNode {
                view,
                end: self.session.expires_at()?,
            },
            u64::from(partition),
            partition,
        ))
    }
    pub fn vertex(&self, vertex: VertexRef) -> Result<Option<Vertex>> {
        let value = self.store(vertex.partition)?.get_vertex(vertex.id)?;
        self.version()?;
        Ok(value)
    }
    pub fn edge(&self, edge: EdgeRef) -> Result<Option<CrossEdge>> {
        Ok(match self.verified_edge_record(edge)? {
            Some(EdgeRecord::Live(e)) => Some(e),
            Some(EdgeRecord::Deleted(_)) | None => None,
        })
    }
    pub(crate) fn partitions(&self) -> Vec<u32> {
        // Constructor already rejects shard IDs outside the partition domain.
        self.views.keys().map(|shard| *shard as u32).collect()
    }
    pub(crate) fn verified_edge_record(&self, edge: EdgeRef) -> Result<Option<EdgeRecord>> {
        let record = self.store(edge.partition)?.get_edge_record(edge.id)?;
        match &record {
            Some(EdgeRecord::Live(e)) => self.store(e.to.partition)?.verify_reverse(e)?,
            Some(EdgeRecord::Deleted(t)) => {
                self.store(t.edge.to.partition)?.verify_deleted_reverse(t)?;
            }
            None => {}
        }
        self.version()?;
        Ok(record)
    }
    /// Index-observed adjacency, checked against both endpoints and edge halves
    /// at this common version. Not a certificate against out-of-band KV writes.
    pub fn observed_adjacency(
        &self,
        vertex: VertexRef,
        direction: rocksdb_sql_table::experimental_graph::Direction,
    ) -> Result<Vec<CrossEdge>> {
        use rocksdb_sql_table::experimental_graph::Direction;
        let entries = self
            .store(vertex.partition)?
            .observed_index(vertex.id, direction)?;
        let mut result = Vec::with_capacity(entries.len());
        let mut bytes = 0usize;
        for entry in entries {
            let edge = self
                .edge(entry.edge)?
                .ok_or(ExperimentalError::Incomplete(entry.edge))?;
            let endpoint = if direction == Direction::Out {
                edge.from
            } else {
                edge.to
            };
            if endpoint != vertex || edge.label != entry.label {
                return Err(ExperimentalError::Incomplete(entry.edge).into());
            }
            bytes = bytes.saturating_add(edge.label.len() + 128);
            for (key, value) in &edge.properties {
                bytes = bytes.saturating_add(
                    key.len()
                        + 64
                        + match value {
                            rocksdb_sql_table::Value::Text(s) => s.len(),
                            _ => 8,
                        },
                );
            }
            if bytes > 1024 * 1024 {
                return Err(Error::Capacity);
            }
            result.push(edge);
        }
        self.version()?;
        Ok(result)
    }
    pub fn prepare_vertex(&self, partition: u32, vertex: &Vertex) -> Result<Plan> {
        self.plan(vec![self.store(partition)?.prepare_vertex(vertex)?])
    }
    pub fn prepare_edge(&self, edge: &CrossEdge) -> Result<Plan> {
        let source = self.store(edge.from.partition)?.prepare_source(edge)?;
        let reverse = self.store(edge.to.partition)?.prepare_reverse(edge)?;
        let batches = if source.shard() == reverse.shard() {
            vec![source.combine(reverse)?]
        } else {
            vec![source, reverse]
        };
        self.plan(batches)
    }
    pub fn prepare_vertex_properties(
        &self,
        vertex: VertexRef,
        patch: &rocksdb_sql_table::experimental_graph::VertexPropertyPatch,
    ) -> Result<Plan> {
        let current = self
            .vertex(vertex)?
            .ok_or(ExperimentalError::MissingEndpoint(vertex))?;
        self.plan(vec![self
            .store(vertex.partition)?
            .prepare_vertex_properties(vertex, &current, patch)?])
    }
    /// Check all conditions at this fixed V and retain its original shard CAS.
    /// An intervening write rejects preparation/execution; never refresh this
    /// candidate after admission, even if the properties become equal again.
    pub fn prepare_vertex_properties_if(
        &self,
        vertex: VertexRef,
        conditions: &PropertyConditions,
        patch: &rocksdb_sql_table::experimental_graph::VertexPropertyPatch,
    ) -> Result<Plan> {
        conditions::validate(conditions)?;
        let current = self
            .vertex(vertex)?
            .ok_or(ExperimentalError::MissingEndpoint(vertex))?;
        conditions::check(conditions, &current.properties)?;
        self.plan(vec![self
            .store(vertex.partition)?
            .prepare_vertex_properties(vertex, &current, patch)?])
    }
    pub fn prepare_edge_properties(
        &self,
        edge: EdgeRef,
        patch: &rocksdb_sql_table::experimental_graph::EdgePropertyPatch,
    ) -> Result<Plan> {
        let current = self.edge(edge)?.ok_or(ExperimentalError::NotFound(edge))?;
        let source = self
            .store(edge.partition)?
            .prepare_source_properties(&current, patch)?;
        let guard = self
            .store(current.to.partition)?
            .prepare_reverse_guard(&current)?;
        self.paired_plan(source, guard)
    }
    /// Conditional source-property update with the same source CAS and target
    /// reverse guard as an unconditional managed edge property patch.
    pub fn prepare_edge_properties_if(
        &self,
        edge: EdgeRef,
        conditions: &PropertyConditions,
        patch: &rocksdb_sql_table::experimental_graph::EdgePropertyPatch,
    ) -> Result<Plan> {
        conditions::validate(conditions)?;
        let current = self.edge(edge)?.ok_or(ExperimentalError::NotFound(edge))?;
        conditions::check(conditions, &current.properties)?;
        let source = self
            .store(edge.partition)?
            .prepare_source_properties(&current, patch)?;
        let guard = self
            .store(current.to.partition)?
            .prepare_reverse_guard(&current)?;
        self.paired_plan(source, guard)
    }
    pub fn prepare_edge_delete(&self, edge: EdgeRef) -> Result<Plan> {
        let current = self.edge(edge)?.ok_or(ExperimentalError::NotFound(edge))?;
        let source = self
            .store(edge.partition)?
            .prepare_source_delete(&current)?;
        let reverse = self
            .store(current.to.partition)?
            .prepare_reverse_delete(&current)?;
        self.paired_plan(source, reverse)
    }
    fn paired_plan(&self, source: PreparedBatch, reverse: PreparedBatch) -> Result<Plan> {
        let batches = if source.shard() == reverse.shard() {
            vec![source.combine(reverse)?]
        } else {
            vec![source, reverse]
        };
        self.plan(batches)
    }
    fn plan(&self, batches: Vec<PreparedBatch>) -> Result<Plan> {
        let cohort = self.session.cohort()?.clone();
        let authority = self.session.authority()?.clone();
        // Preserve the source request identity, including delete tombstones.
        // Select it before sorting participants, and retain it on every retry.
        let id = batches.first().ok_or(Error::Identity)?.request_id();
        if id == [0; 16] {
            return Err(Error::Identity);
        }
        let mut parts = Vec::new();
        let mut bytes = 0usize;
        let mut operations = 0usize;
        for batch in batches {
            let (request, sequence) = batch.into_conditional_request();
            let member = cohort
                .members
                .iter()
                .find(|m| m.shard == request.shard && m.epoch == request.epoch)
                .ok_or(Error::Identity)?;
            let mut intent = [0; 32];
            getrandom::fill(&mut intent).map_err(|_| Error::Uncertain)?;
            if intent == [0; 32] {
                return Err(Error::Identity);
            }
            let ops: Vec<_> = request
                .operations
                .into_iter()
                .map(|op| match op {
                    WriteOp::Put { key, value } => d2_raft::WriteOp::Put { key, value },
                    WriteOp::Delete { key } => d2_raft::WriteOp::Delete { key },
                })
                .collect();
            operations = operations.saturating_add(ops.len());
            bytes = bytes.saturating_add(
                ops.iter()
                    .map(|op| match op {
                        d2_raft::WriteOp::Put { key, value } => key.len() + value.len(),
                        d2_raft::WriteOp::Delete { key } => key.len(),
                    })
                    .sum::<usize>(),
            );
            if operations > d2_raft::MAX_BATCH_OPERATIONS || bytes > d2_raft::MAX_BATCH_BYTES {
                return Err(Error::Capacity);
            }
            parts.push(Part {
                descriptor: Participant {
                    shard: member.shard,
                    epoch: member.epoch,
                    mode: member.mode,
                    intent,
                },
                request: D1WriteRequest {
                    shard: request.shard,
                    epoch: request.epoch,
                    request_id: id,
                    expected_sequence: Some(sequence),
                    operations: ops,
                },
            });
        }
        parts.sort_by_key(|p| p.descriptor.shard);
        if parts.is_empty()
            || parts
                .windows(2)
                .any(|w| w[0].descriptor.shard == w[1].descriptor.shard)
        {
            return Err(Error::Identity);
        }
        Ok(Plan {
            id,
            authority,
            cohort,
            version: self.version()?,
            parts,
        })
    }
}

/// Explicit fixed handle set; no discovery, ownership migration, or failover.
pub struct GraphCohort<'a> {
    coordinator: NodeRef<'a>,
    timing: Recorder,
    authority: Authority,
    epoch: u64,
    members: Vec<(NodeRef<'a>, versioned_reads::Target)>,
}
impl<'a> GraphCohort<'a> {
    pub fn new(
        coordinator: &'a NodeHandle,
        authority: Authority,
        epoch: u64,
        members: Vec<(&'a NodeHandle, versioned_reads::Target)>,
    ) -> Self {
        Self::from_endpoints(
            NodeRef::Local(coordinator),
            authority,
            epoch,
            members
                .into_iter()
                .map(|(n, t)| (NodeRef::Local(n), t))
                .collect(),
        )
    }
    /// Pin fixed trusted endpoints; no discovery or failover. Catch-up retries
    /// uncertain history writes once with the identical page or cut.
    pub fn from_endpoints(
        coordinator: NodeRef<'a>,
        authority: Authority,
        epoch: u64,
        members: Vec<(NodeRef<'a>, versioned_reads::Target)>,
    ) -> Self {
        Self {
            coordinator,
            timing: Recorder::default(),
            authority,
            epoch,
            members,
        }
    }
    pub(crate) fn with_timing(mut self, timing: Recorder) -> Self {
        self.timing = timing;
        self
    }
    pub async fn snapshot(
        &self,
        version: Option<u64>,
        limit: versioned_reads::ReadLimit,
        lifetime: Duration,
    ) -> Result<GraphSnapshot> {
        let session = cohort_reads::acquire_endpoints(
            self.coordinator,
            self.authority.clone(),
            self.epoch,
            self.members.iter().map(|(n, t)| (*n, t.clone())).collect(),
            version,
            limit,
            lifetime,
        )
        .await?;
        GraphSnapshot::from_session(session)
    }
    /// Recover the exact previously admitted plan. `request` must be the same
    /// canonical application command bytes; never replace a mismatched request.
    pub async fn recover_request(&self, id: [u8; 16], request: &[u8]) -> Result<Option<Plan>> {
        tokio::time::timeout(Duration::from_secs(30), async {
            let lookup = self
                .coordinator
                .request(&self.authority, self.epoch, id)
                .await?;
            self.recover_from(&lookup, request)
        })
        .await
        .map_err(|_| Error::Uncertain)?
    }
    fn recover_from(&self, lookup: &RequestLookup, request: &[u8]) -> Result<Option<Plan>> {
        self.check_cohort(&lookup.cohort)?;
        let Some(recovered) = &lookup.record else {
            return Ok(None);
        };
        let record = &recovered.record;
        if record.request != request {
            return Err(Error::Identity);
        }
        let plan = Plan::from_durable(&record.payload)?;
        if plan.authority != self.authority || plan.cohort != lookup.cohort {
            return Err(Error::Identity);
        }
        let tx = &recovered.transaction;
        if record.transaction_id != plan.id
            || tx.participants
                != plan
                    .parts
                    .iter()
                    .map(|p| p.descriptor.clone())
                    .collect::<Vec<_>>()
        {
            return Err(Error::Identity);
        }
        Ok(Some(plan))
    }
    /// Persist a request's first plan and Begin atomically before preparing any
    /// participant. Concurrent identical requests always return the first plan.
    pub async fn admit_request(
        &self,
        id: [u8; 16],
        request: Vec<u8>,
        candidate: &Plan,
    ) -> Result<Plan> {
        tokio::time::timeout(Duration::from_secs(30), async {
            let summary = self
                .coordinator
                .summary(&self.authority, self.epoch)
                .await?;
            self.check_cohort(&summary.cohort)?;
            if candidate.authority != self.authority || candidate.cohort != summary.cohort {
                return Err(Error::Identity);
            }
            let lookup = self
                .coordinator
                .request(&self.authority, self.epoch, id)
                .await?;
            if let Some(existing) = self.recover_from(&lookup, &request)? {
                return Ok(existing);
            }
            let payload = candidate.durable_bytes()?;
            // Apply independently validates the anchored original history.
            // Point recovery does not change admission or original Plan limits.
            let result = self
                .command(Command::AdmitRequest {
                    id,
                    request: request.clone(),
                    payload,
                    transaction_id: candidate.id,
                    participants: candidate
                        .parts
                        .iter()
                        .map(|p| p.descriptor.clone())
                        .collect(),
                })
                .await;
            let lookup = self
                .coordinator
                .request(&self.authority, self.epoch, id)
                .await?;
            if let Some(existing) = self.recover_from(&lookup, &request)? {
                return Ok(existing);
            }
            result?;
            Err(Error::Uncertain)
        })
        .await
        .map_err(|_| Error::Uncertain)?
    }
    async fn target(&self, plan: &Plan) -> Result<TargetEvidence> {
        let target = self
            .coordinator
            .target(&self.authority, self.epoch, plan.id)
            .await?;
        self.check_plan(plan, &target)?;
        Ok(target)
    }
    async fn command(&self, command: Command) -> Result<()> {
        self.coordinator
            .command(&self.authority, self.epoch, command)
            .await
            .map_err(Error::from)
    }
    fn check_plan(&self, plan: &Plan, target: &TargetEvidence) -> Result<()> {
        if self.authority != plan.authority
            || target.cut.cohort != plan.cohort
            || target.id != plan.id
        {
            return Err(Error::Identity);
        }
        self.check_cohort(&plan.cohort)?;
        if let Some(found) = &target.record {
            if found.transaction.participants
                != plan
                    .parts
                    .iter()
                    .map(|part| part.descriptor.clone())
                    .collect::<Vec<_>>()
            {
                return Err(Error::Identity);
            }
            if let Some(original) = &found.request {
                if Plan::from_durable(&original.payload)? != *plan {
                    return Err(Error::Identity);
                }
            }
        }
        Ok(())
    }
    fn check_cohort(&self, cohort: &Cohort) -> Result<()> {
        if cohort.validate().is_err()
            || self.authority.mode == [0; 16]
            || cohort.coordinator_shard != self.authority.shard
            || cohort.coordinator_epoch != self.epoch
            || self.members.len() != cohort.members.len()
        {
            return Err(Error::Identity);
        }
        for ((_, target), member) in self.members.iter().zip(&cohort.members) {
            if (target.shard, target.epoch, target.mode)
                != (member.shard, member.epoch, member.mode)
            {
                return Err(Error::Identity);
            }
        }
        Ok(())
    }
    /// Read the durable decision without preparing, deciding, or advancing it.
    /// A known decision survives unavailable members as `visible: false`.
    pub async fn status(&self, plan: &Plan) -> Result<Option<Outcome>> {
        let target = tokio::time::timeout(Duration::from_secs(30), self.target(plan))
            .await
            .map_err(|_| Error::Uncertain)??;
        let tx = &target.record.as_ref().ok_or(Error::Identity)?.transaction;
        let Some(decision) = tx.decision else {
            return Ok(None);
        };
        let ordinal = tx.ordinal;
        // A terminal above an earlier hole is still a known decision. Failure
        // to obtain/compare the full prefix means unknown visibility, not abort.
        let visible = matches!(
            tokio::time::timeout(Duration::from_secs(5), async {
                let proof = self
                    .coordinator
                    .terminal(&self.authority, self.epoch, ordinal)
                    .await?;
                if proof.cohort() != &plan.cohort || !proof.matches_transaction(tx) {
                    return Err(Error::Identity);
                }
                for (node, member) in &self.members {
                    node.prefix_terminal(member, &proof).await?;
                }
                Ok::<(), Error>(())
            })
            .await,
            Ok(Ok(()))
        );
        Ok(Some(match decision {
            Decision::Commit => Outcome::Committed { ordinal, visible },
            Decision::Abort => Outcome::Aborted { ordinal, visible },
        }))
    }
    /// Overall deadline covers Begin/Prepare/decision. On timeout the plan is
    /// still live; do not prepare a replacement. A known terminal decision is
    /// returned even when visibility catch-up cannot finish within the budget.
    pub async fn execute(&self, plan: &Plan) -> Result<Outcome> {
        self.run(plan, false).await
    }
    /// Explicitly request abort of this exact plan. A concurrent committed
    /// decision wins and is returned as Committed, never misreported as aborted.
    pub async fn abort(&self, plan: &Plan) -> Result<Outcome> {
        self.run(plan, true).await
    }
    async fn run(&self, plan: &Plan, abort: bool) -> Result<Outcome> {
        let _span = self.timing.span(Phase::Execute);
        let decision = tokio::time::timeout(Duration::from_secs(30), self.decide(plan, abort))
            .await
            .map_err(|_| Error::Uncertain)??;
        let ordinal = decision.ordinal;
        let outcome = decision.decision.ok_or(Error::Identity)?;
        let visible = matches!(
            tokio::time::timeout(Duration::from_secs(30), self.advance_to(plan, &decision)).await,
            Ok(Ok(()))
        );
        Ok(match outcome {
            Decision::Commit => Outcome::Committed { ordinal, visible },
            Decision::Abort => Outcome::Aborted { ordinal, visible },
        })
    }
    async fn decide(&self, plan: &Plan, abort: bool) -> Result<Transaction> {
        let _span = self.timing.span(Phase::Decision);
        let target = self.target(plan).await?;
        if let Some(found) = target.record {
            if found.transaction.decision.is_some() {
                return Ok(found.transaction);
            }
        }
        if abort {
            self.command(Command::Begin {
                id: plan.id,
                participants: plan.parts.iter().map(|p| p.descriptor.clone()).collect(),
            })
            .await?;
            return self.request_decision(plan, Decision::Abort).await;
        }
        for (node, target) in self.members.iter().filter(|(_, target)| {
            plan.parts
                .iter()
                .any(|p| p.descriptor.shard == target.shard)
        }) {
            let info = node.visibility(target).await?;
            if info.authority != plan.authority
                || info.cohort.as_ref() != Some(&plan.cohort)
                || info.baseline_sequence != 0
            {
                return Err(Error::Identity);
            }
        }
        self.command(Command::Begin {
            id: plan.id,
            participants: plan.parts.iter().map(|p| p.descriptor.clone()).collect(),
        })
        .await?;
        let target = self.target(plan).await?;
        let transaction = target.record.ok_or(Error::Identity)?.transaction;
        if transaction.decision.is_some() {
            return Ok(transaction);
        }
        for part in &plan.parts {
            let (node, member) = self
                .members
                .iter()
                .find(|(_, target)| target.shard == part.descriptor.shard)
                .ok_or(Error::Identity)?;
            let prepared = async {
                let _span = self.timing.span(Phase::Prepare);
                let evidence = self.target(plan).await?;
                let tx = &evidence.record.as_ref().ok_or(Error::Identity)?.transaction;
                // A concurrent decision is recovered below rather than issuing
                // a Prepare against a terminal source cut.
                if tx.decision.is_some() {
                    return Err(Error::Uncertain);
                }
                self.retain_for_prepare(*node, member, &evidence.cut)
                    .await?;
                let status = node
                    .prepare_retained(
                        &self.authority,
                        &part.descriptor,
                        part.request.clone(),
                        &evidence.cut,
                    )
                    .await?;
                if status != d2_raft::participant_writes::Status::Prepared {
                    return Err(Error::Uncertain);
                }
                self.command(Command::Prepared {
                    id: plan.id,
                    participant: part.descriptor.clone(),
                })
                .await
            }
            .await;
            if let Err(error) = prepared {
                // No implicit abort on rejection or unknown ACK. Read this
                // exact ID again; the caller retains the same original Plan.
                let latest = self.target(plan).await?;
                if let Some(found) = latest.record {
                    if found.transaction.decision.is_some() {
                        return Ok(found.transaction);
                    }
                }
                return Err(error);
            }
        }
        self.request_decision(plan, Decision::Commit).await
    }
    async fn request_decision(&self, plan: &Plan, requested: Decision) -> Result<Transaction> {
        let result = self
            .command(Command::Decide {
                id: plan.id,
                decision: requested,
            })
            .await;
        let latest = self.target(plan).await?;
        if let Some(found) = latest.record {
            if found.transaction.decision.is_some() {
                return Ok(found.transaction);
            }
        }
        result?;
        Err(Error::Uncertain)
    }
    async fn retain_for_prepare(
        &self,
        node: NodeRef<'_>,
        member: &versioned_reads::Target,
        cut: &HistoryCursor,
    ) -> Result<()> {
        let _span = self.timing.span(Phase::HistorySync);
        let status = node
            .history_status(&self.authority, member, &cut.cohort)
            .await?;
        // A later observation is preserved. The empty old-cut page still makes
        // SyncHistory prove that exact ancestor, independently of the reply.
        let mut base = if status.cursor.event_count > cut.event_count {
            cut.clone()
        } else {
            status.cursor
        };
        for _ in 0..=cut.cohort.profile.max_history_events() {
            let page = self
                .coordinator
                .history_page(&self.authority, self.epoch, &base, Some(cut))
                .await?;
            let result = node.sync_history(&self.authority, member, &page).await;
            match result {
                Err(managed_rpc::Error::Unavailable) => {
                    node.sync_history(&self.authority, member, &page).await?;
                }
                other => {
                    other?;
                }
            }
            if page.end == *cut {
                return Ok(());
            }
            if page.end.event_count <= base.event_count {
                return Err(Error::Identity);
            }
            base = page.end;
        }
        Err(Error::Capacity)
    }
    async fn advance_to(&self, plan: &Plan, transaction: &Transaction) -> Result<()> {
        let _span = self.timing.span(Phase::Cohort);
        let ordinal = transaction.ordinal;
        // The exact terminal target is retained from decide; do not combine a
        // different target with a numerically equal later proof.
        if transaction.id != plan.id || transaction.decision.is_none() {
            return Err(Error::Identity);
        }
        // Validate the immutable local plan without requesting its full archive.
        self.check_cohort(&plan.cohort)?;
        if plan.authority != self.authority || ordinal == 0 {
            return Err(Error::Identity);
        }
        let proof = self
            .coordinator
            .terminal(&self.authority, self.epoch, ordinal)
            .await?;
        if proof.cohort() != &plan.cohort || !proof.matches_transaction(transaction) {
            return Err(Error::Identity);
        }
        for (node, target) in &self.members {
            let history = self.timing.span(Phase::HistorySync);
            let status = node
                .history_status(&self.authority, target, &plan.cohort)
                .await?;
            let mut applied = status.applied_prefix;
            let mut base = status.cursor;
            // Pin only after observing this member, which may already retain
            // events beyond its applied watermark or beyond another member's cut.
            let mut page = self
                .coordinator
                .history_page(&self.authority, self.epoch, &base, None)
                .await?;
            let cut = page.cut.clone();
            loop {
                // An uncertain ACK retries the identical page before any new
                // cursor/cut is selected. One retry and the outer deadline bound it.
                let result = node.sync_history(&self.authority, target, &page).await;
                let synced = match result {
                    Err(managed_rpc::Error::Unavailable) => {
                        node.sync_history(&self.authority, target, &page).await?
                    }
                    other => other?,
                };
                if synced.applied_prefix < applied {
                    return Err(Error::Identity);
                }
                applied = synced.applied_prefix;
                // Concurrent sync may have advanced the member farther. Continue
                // from this checked page end, retaining this exact source cut;
                // subsequent pages are verified exact overlapping retries locally.
                base = page.end;
                if base == cut {
                    break;
                }
                page = self
                    .coordinator
                    .history_page(&self.authority, self.epoch, &base, Some(&cut))
                    .await?;
            }
            drop(history);
            let consume = self.timing.span(Phase::Consume);
            // Each successful consume strictly increases applied, bounded by the
            // admitted transaction cap. Concurrent consumption can already be ahead.
            while applied < ordinal {
                let requested = applied + 1;
                let result = node
                    .advance_retained(&self.authority, target, requested, &cut)
                    .await;
                applied = match result {
                    Err(managed_rpc::Error::Unavailable) => {
                        node.advance_retained(&self.authority, target, requested, &cut)
                            .await?
                    }
                    other => other?,
                };
            }
            drop(consume);
            let _visibility = self.timing.span(Phase::Visibility);
            // A watermark or hash root alone is never the final V certificate.
            node.prefix_terminal(target, &proof).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "managed_graph_tests.rs"]
mod tests;
