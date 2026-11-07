//! Opt-in trusted in-process graph/v2 transactions over an empty fixed cohort.
//! No remote Global Gremlin admission. Plans are immutable in-memory retry values.
use d2_raft::participant_writes::Authority;
use d2_raft::transaction_decisions::{Cohort, Command, Decision, Journal, Participant};
use d2_raft::{
    cohort_reads, participant_driver, versioned_reads, D1Command, D1Error, D1WriteRequest,
    LogCommand, NodeHandle,
};
use rocksdb_sql_table::experimental_graph::{
    AppendOnlyStore, CrossEdge, EdgeRecord, EdgeRef, ExperimentalError, PreparedBatch, VertexRef,
};
use rocksdb_sql_table::{ReadView, StorageNodeError, Vertex, WriteOp};
use std::collections::BTreeMap;
use std::time::Duration;

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
}
impl From<cohort_reads::Error> for Error {
    fn from(e: cohort_reads::Error) -> Self {
        Self::Snapshot(e)
    }
}
pub type Result<T> = std::result::Result<T, Error>;

/// Decision is authoritative even when not every member has consumed its prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Committed { ordinal: u64, visible: bool },
    Aborted { ordinal: u64, visible: bool },
}
#[derive(Debug, Clone)]
struct Part {
    descriptor: Participant,
    request: D1WriteRequest,
}
/// Keep this exact value across every uncertain result. Cloning retains identity;
/// preparing again creates a different transaction and is not a retry.
#[derive(Debug, Clone)]
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
    fn store(&self, partition: u32) -> Result<AppendOnlyStore<crate::remote::query::FrozenNode>> {
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
        let record = self.store(edge.partition)?.get_edge_record(edge.id)?;
        let value = match record {
            Some(EdgeRecord::Live(e)) => {
                self.store(e.to.partition)?.verify_reverse(&e)?;
                Some(e)
            }
            Some(EdgeRecord::Deleted(t)) => {
                self.store(t.edge.to.partition)?
                    .verify_deleted_reverse(&t)?;
                None
            }
            None => None,
        };
        self.version()?;
        Ok(value)
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
    coordinator: &'a NodeHandle,
    authority: Authority,
    epoch: u64,
    members: Vec<(&'a NodeHandle, versioned_reads::Target)>,
}
impl<'a> GraphCohort<'a> {
    pub fn new(
        coordinator: &'a NodeHandle,
        authority: Authority,
        epoch: u64,
        members: Vec<(&'a NodeHandle, versioned_reads::Target)>,
    ) -> Self {
        Self {
            coordinator,
            authority,
            epoch,
            members,
        }
    }
    pub async fn snapshot(
        &self,
        version: Option<u64>,
        limit: versioned_reads::ReadLimit,
        lifetime: Duration,
    ) -> Result<GraphSnapshot> {
        let session = cohort_reads::acquire(
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
            let journal = self.journal().await?;
            self.recover_from(&journal, id, request)
        })
        .await
        .map_err(|_| Error::Uncertain)?
    }
    fn recover_from(
        &self,
        journal: &Journal,
        id: [u8; 16],
        request: &[u8],
    ) -> Result<Option<Plan>> {
        let Some(record) = journal.request(id) else {
            return Ok(None);
        };
        if record.request != request {
            return Err(Error::Identity);
        }
        let plan = Plan::from_durable(&record.payload)?;
        self.check_plan(&plan, journal)?;
        let tx = journal
            .transaction(record.transaction_id)
            .ok_or(Error::Identity)?;
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
            let journal = self.journal().await?;
            self.check_plan(candidate, &journal)?;
            if let Some(existing) = self.recover_from(&journal, id, &request)? {
                return Ok(existing);
            }
            let payload = candidate.durable_bytes()?;
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
            let journal = self.journal().await?;
            if let Some(existing) = self.recover_from(&journal, id, &request)? {
                return Ok(existing);
            }
            result?;
            Err(Error::Uncertain)
        })
        .await
        .map_err(|_| Error::Uncertain)?
    }
    async fn journal(&self) -> Result<Journal> {
        self.coordinator
            .raft
            .ensure_linearizable()
            .await
            .map_err(|_| Error::Uncertain)?;
        self.coordinator
            .state_machine
            .coordinator_journal(self.authority.shard, self.epoch, self.authority.mode)
            .await
            .map_err(Error::Application)
    }
    async fn command(&self, command: Command) -> Result<()> {
        self.coordinator
            .raft
            .client_write(LogCommand::D1V1(D1Command::Coordinator {
                shard: self.authority.shard,
                epoch: self.epoch,
                mode_id: self.authority.mode,
                command,
            }))
            .await
            .map_err(|_| Error::Uncertain)?
            .data
            .d1
            .ok_or(Error::Uncertain)?
            .map_err(Error::Application)?;
        Ok(())
    }
    fn check_plan(&self, plan: &Plan, journal: &Journal) -> Result<()> {
        if self.authority != plan.authority
            || journal.cohort() != Some(&plan.cohort)
            || plan.cohort.coordinator_epoch != self.epoch
            || self.members.len() != plan.cohort.members.len()
        {
            return Err(Error::Identity);
        }
        for ((_, target), member) in self.members.iter().zip(&plan.cohort.members) {
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
        let journal = tokio::time::timeout(Duration::from_secs(30), self.journal())
            .await
            .map_err(|_| Error::Uncertain)??;
        self.check_plan(plan, &journal)?;
        let tx = journal.transaction(plan.id).ok_or(Error::Identity)?;
        if tx.participants
            != plan
                .parts
                .iter()
                .map(|p| p.descriptor.clone())
                .collect::<Vec<_>>()
        {
            return Err(Error::Identity);
        }
        let Some(decision) = tx.decision else {
            return Ok(None);
        };
        let ordinal = tx.ordinal;
        let visible = matches!(
            tokio::time::timeout(Duration::from_secs(5), async {
                for (node, target) in &self.members {
                    node.raft
                        .ensure_linearizable()
                        .await
                        .map_err(|_| Error::Uncertain)?;
                    node.state_machine
                        .participant_prefix_applied(
                            target.shard,
                            target.epoch,
                            target.mode,
                            ordinal,
                            &journal,
                        )
                        .await
                        .map_err(Error::Application)?;
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
        let decision = tokio::time::timeout(Duration::from_secs(30), self.decide(plan, abort))
            .await
            .map_err(|_| Error::Uncertain)??;
        let (ordinal, decision) = decision;
        let visible = matches!(
            tokio::time::timeout(Duration::from_secs(30), self.advance_to(plan, ordinal)).await,
            Ok(Ok(()))
        );
        Ok(match decision {
            Decision::Commit => Outcome::Committed { ordinal, visible },
            Decision::Abort => Outcome::Aborted { ordinal, visible },
        })
    }
    async fn decide(&self, plan: &Plan, abort: bool) -> Result<(u64, Decision)> {
        let journal = self.journal().await?;
        self.check_plan(plan, &journal)?;
        if let Some(tx) = journal.transaction(plan.id) {
            let expected: Vec<_> = plan.parts.iter().map(|p| p.descriptor.clone()).collect();
            if tx.participants != expected {
                return Err(Error::Identity);
            }
            if let Some(decision) = tx.decision {
                return Ok((tx.ordinal, decision));
            }
        }
        if abort {
            self.command(Command::Begin {
                id: plan.id,
                participants: plan.parts.iter().map(|p| p.descriptor.clone()).collect(),
            })
            .await?;
            return self.request_decision(plan.id, Decision::Abort).await;
        }
        for (node, target) in self.members.iter().filter(|(_, target)| {
            plan.parts
                .iter()
                .any(|p| p.descriptor.shard == target.shard)
        }) {
            node.raft
                .ensure_linearizable()
                .await
                .map_err(|_| Error::Uncertain)?;
            let info = node
                .state_machine
                .participant_visibility_info(target.shard, target.epoch, target.mode)
                .await
                .map_err(Error::Application)?;
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
        let journal = self.journal().await?;
        let tx = journal.transaction(plan.id).ok_or(Error::Identity)?;
        if let Some(decision) = tx.decision {
            return Ok((tx.ordinal, decision));
        }
        if !abort {
            for part in &plan.parts {
                let node = self
                    .members
                    .iter()
                    .find(|(_, t)| t.shard == part.descriptor.shard)
                    .ok_or(Error::Identity)?
                    .0;
                if let Err(error) = participant_driver::prepare(
                    self.coordinator,
                    node,
                    self.authority.clone(),
                    self.epoch,
                    part.descriptor.clone(),
                    part.request.clone(),
                )
                .await
                {
                    // No implicit abort on a timeout or a rejected Prepare. The
                    // caller can resolve/retry or explicitly abort this plan.
                    let latest = self.journal().await?;
                    if let Some(decision) = latest.transaction(plan.id).and_then(|t| t.decision) {
                        return Ok((tx.ordinal, decision));
                    }
                    return Err(match error {
                        participant_driver::Error::Application(e) => Error::Application(e),
                        _ => Error::Uncertain,
                    });
                }
            }
        }
        self.request_decision(plan.id, Decision::Commit).await
    }
    async fn request_decision(&self, id: [u8; 16], requested: Decision) -> Result<(u64, Decision)> {
        let result = self
            .command(Command::Decide {
                id,
                decision: requested,
            })
            .await;
        let latest = self.journal().await?;
        if let Some(terminal) = latest
            .transaction(id)
            .and_then(|t| t.decision.map(|d| (t.ordinal, d)))
        {
            return Ok(terminal);
        }
        result?;
        Err(Error::Uncertain)
    }
    async fn advance_to(&self, plan: &Plan, ordinal: u64) -> Result<()> {
        let journal = self.journal().await?;
        self.check_plan(plan, &journal)?;
        for (node, target) in &self.members {
            node.raft
                .ensure_linearizable()
                .await
                .map_err(|_| Error::Uncertain)?;
            let info = node
                .state_machine
                .participant_visibility_info(target.shard, target.epoch, target.mode)
                .await
                .map_err(Error::Application)?;
            if info.authority != plan.authority
                || info.cohort.as_ref() != Some(&plan.cohort)
                || info.baseline_sequence != 0
            {
                return Err(Error::Identity);
            }
            let mut applied = info.applied_prefix;
            while applied < ordinal {
                applied = versioned_reads::advance(
                    self.coordinator,
                    node,
                    plan.authority.clone(),
                    self.epoch,
                    target.clone(),
                    applied + 1,
                )
                .await
                .map_err(|_| Error::Uncertain)?;
            }
            node.raft
                .ensure_linearizable()
                .await
                .map_err(|_| Error::Uncertain)?;
            node.state_machine
                .participant_prefix_applied(
                    target.shard,
                    target.epoch,
                    target.mode,
                    ordinal,
                    &journal,
                )
                .await
                .map_err(Error::Application)?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "managed_graph_tests.rs"]
mod tests;
