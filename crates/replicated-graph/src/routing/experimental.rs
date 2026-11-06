//! Isolated v2 graph experiment. Not a distributed transaction.
use super::*;
mod traversal;
use rocksdb_sql_table::experimental_graph::{
    AppendOnlyStore, CrossEdge, Direction, EdgePropertyPatch, EdgeRecord, EdgeRef,
    ExperimentalError, PreparedBatch, VertexRef,
};
pub use traversal::{
    ObservedElementRef, ObservedPathElement, ObservedRow, ObservedRowV2, ObservedRowV3,
    ObservedTraversal, ObservedTraversalV2, ObservedTraversalV3,
};

#[derive(Debug)]
pub enum ExperimentalRouteError {
    Query(rocksdb_sql_table::Error),
    Unsupported(&'static str),
    Route(RouteError),
    Graph(ExperimentalError),
    /// The caller retains the original PreparedAppend or PreparedEdgeMutation
    /// for exact retry. A failed stage never implies rollback of earlier or
    /// uncertain writes.
    Stage {
        stage: Stage,
        outcome: StageOutcome,
        request_id: RequestId,
        source_acknowledged_this_attempt: bool,
        error: RouteError,
    },
}
impl fmt::Display for ExperimentalRouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}
impl Error for ExperimentalRouteError {}
impl From<RouteError> for ExperimentalRouteError {
    fn from(e: RouteError) -> Self {
        Self::Route(e)
    }
}
impl From<StorageNodeError> for ExperimentalRouteError {
    fn from(e: StorageNodeError) -> Self {
        Self::Route(e.into())
    }
}
impl From<ExperimentalError> for ExperimentalRouteError {
    fn from(e: ExperimentalError) -> Self {
        Self::Graph(e)
    }
}
pub type ExperimentalResult<T> = std::result::Result<T, ExperimentalRouteError>;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Source,
    Reverse,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// Describes only this submission attempt, never accumulated operation state.
/// An earlier uncertain attempt can already have persisted data.
pub enum StageOutcome {
    /// This stage was rejected in this attempt; earlier attempts are unaffected.
    Rejected,
    Incomplete,
    Unknown,
}
/// Both participant writes acknowledged; not atomic visibility or recovery.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendAcknowledgement {
    AllBatchesAcknowledged,
}
/// Missing index entries cannot be detected by an index-only adjacency read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdjacencyCompleteness {
    Unverified,
}
#[derive(Clone, Debug, PartialEq)]
pub struct ObservedAdjacency {
    pub edges: Vec<CrossEdge>,
    pub completeness: AdjacencyCompleteness,
}
#[derive(Clone, Debug)]
struct Participant {
    placement: Placement,
    batch: PreparedBatch,
}
/// Retain before submission. No automatic repair, durable client journal, or
/// re-preparation after an unknown result. Exact retry may still be fenced.
#[derive(Clone, Debug)]
pub struct PreparedAppend {
    metadata: Placement,
    source: Participant,
    reverse: Option<Participant>,
}
impl PreparedAppend {
    pub fn source_request_id(&self) -> RequestId {
        self.source.batch.request_id()
    }
    pub fn reverse_request_id(&self) -> Option<RequestId> {
        self.reverse.as_ref().map(|p| p.batch.request_id())
    }
}
/// Additive mutation kind; create APIs and their acknowledgement stay unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeMutationKind {
    Properties,
    Delete,
}
/// Historical acknowledgement of the original request, not proof that the
/// current graph state still equals this mutation's result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MutationAcknowledgement {
    AllBatchesAcknowledged,
}
/// Immutable exact retry of source and optional reverse mutation batches. Retain before
/// submission; never reprepare automatically after an unknown outcome.
#[derive(Clone, Debug)]
pub struct PreparedEdgeMutation {
    edge: EdgeRef,
    kind: EdgeMutationKind,
    prepared: PreparedAppend,
}
impl PreparedEdgeMutation {
    pub fn edge(&self) -> EdgeRef {
        self.edge
    }
    pub fn kind(&self) -> EdgeMutationKind {
        self.kind
    }
    /// The source participant request ID is also the operation ID.
    pub fn operation_id(&self) -> RequestId {
        self.prepared.source_request_id()
    }
    pub fn source_request_id(&self) -> RequestId {
        self.operation_id()
    }
    pub fn reverse_request_id(&self) -> Option<RequestId> {
        self.prepared.reverse_request_id()
    }
}
/// One frozen catalog and per-shard snapshot vector. Never a global snapshot.
pub struct ExperimentalReadSession {
    metadata: Placement,
    pub metadata_sequence: u64,
    pub snapshots: Vec<ShardReadStamp>,
    placements: BTreeMap<u32, Placement>,
    nodes: BTreeMap<u64, PinnedNode>,
    op: Arc<Operation>,
}
impl ExperimentalReadSession {
    fn store(&self, partition: u32) -> ExperimentalResult<AppendOnlyStore<PinnedNode>> {
        self.op.check(None)?;
        let p = self
            .placements
            .get(&partition)
            .ok_or(RouteError::MissingPartition(partition))?;
        Ok(AppendOnlyStore::new(
            self.nodes[&p.shard].clone(),
            p.shard,
            partition,
        ))
    }
    pub fn get_vertex(&self, vertex: VertexRef) -> ExperimentalResult<Option<Vertex>> {
        let value = self.store(vertex.partition)?.get_vertex(vertex.id)?;
        self.op.check(None)?;
        Ok(value)
    }
    /// Checks the canonical record and both observed indexes/endpoints. Absence
    /// of the canonical record means absent in the source's pinned snapshot.
    pub fn get_edge(&self, edge: EdgeRef) -> ExperimentalResult<Option<CrossEdge>> {
        let value = match self.verified_edge_record(edge)? {
            Some(EdgeRecord::Live(e)) => Some(e),
            Some(EdgeRecord::Deleted(_)) | None => None,
        };
        Ok(value)
    }
    fn verified_edge_record(&self, edge: EdgeRef) -> ExperimentalResult<Option<EdgeRecord>> {
        let value = self.store(edge.partition)?.get_edge_record(edge.id)?;
        match &value {
            Some(EdgeRecord::Live(e)) => self.store(e.to.partition)?.verify_reverse(e)?,
            Some(EdgeRecord::Deleted(t)) => {
                self.store(t.edge.to.partition)?.verify_deleted_reverse(t)?
            }
            None => {}
        }
        self.op.check(None)?;
        Ok(value)
    }
    /// Index-observed adjacency only. This does NOT certify completeness and
    /// cannot be fed into the legacy Gremlin/count completeness contract.
    pub fn observed_adjacency(
        &self,
        vertex: VertexRef,
        direction: Direction,
    ) -> ExperimentalResult<ObservedAdjacency> {
        let entries = self
            .store(vertex.partition)?
            .observed_index(vertex.id, direction)?;
        let mut edges = Vec::with_capacity(entries.len());
        let mut bytes = 0usize;
        for entry in entries {
            self.op.check(None)?;
            let e = self
                .get_edge(entry.edge)?
                .ok_or(ExperimentalError::Incomplete(entry.edge))?;
            let endpoint = if direction == Direction::Out {
                e.from
            } else {
                e.to
            };
            if endpoint != vertex || e.label != entry.label {
                return Err(ExperimentalError::Incomplete(entry.edge).into());
            }
            // Owned response admission, not total heap/RSS; individual decode
            // and immutable snapshots retain their separately stated caps.
            bytes = bytes.saturating_add(std::mem::size_of::<CrossEdge>() + e.label.len());
            for (key, value) in &e.properties {
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
                return Err(StorageNodeError::ResourceExhausted(
                    "v2 adjacency output exceeds 1 MiB".into(),
                )
                .into());
            }
            edges.push(e);
        }
        self.op.check(None)?;
        Ok(ObservedAdjacency {
            edges,
            completeness: AdjacencyCompleteness::Unverified,
        })
    }
    /// Validate a complete observed edge and prepare a single source-shard CAS.
    /// Identity, endpoints, label and both indexes are unchanged. The frozen
    /// snapshot vector is not a global snapshot or a target write condition.
    pub fn prepare_edge_properties(
        &self,
        edge: EdgeRef,
        patch: &EdgePropertyPatch,
    ) -> ExperimentalResult<PreparedEdgeMutation> {
        let current = self
            .get_edge(edge)?
            .ok_or(ExperimentalError::Incomplete(edge))?;
        let batch = self
            .store(edge.partition)?
            .prepare_source_properties(&current, patch)?;
        self.op.check(None)?;
        Ok(PreparedEdgeMutation {
            edge,
            kind: EdgeMutationKind::Properties,
            prepared: PreparedAppend {
                metadata: self.metadata.clone(),
                source: Participant {
                    placement: self.placements[&edge.partition].clone(),
                    batch,
                },
                reverse: None,
            },
        })
    }
    /// Delete a complete observed live edge. Permanently reserves its identity.
    /// Two shards are staged, never atomic; retain this exact prepared value.
    pub fn prepare_edge_delete(&self, edge: EdgeRef) -> ExperimentalResult<PreparedEdgeMutation> {
        let current = self
            .get_edge(edge)?
            .ok_or(ExperimentalError::NotFound(edge))?;
        let source = self
            .store(edge.partition)?
            .prepare_source_delete(&current)?;
        let reverse = self
            .store(current.to.partition)?
            .prepare_reverse_delete(&current)?;
        let (source, reverse) = if source.shard() == reverse.shard() {
            (source.combine(reverse)?, None)
        } else {
            (
                source,
                Some(Participant {
                    placement: self.placements[&current.to.partition].clone(),
                    batch: reverse,
                }),
            )
        };
        self.op.check(None)?;
        Ok(PreparedEdgeMutation {
            edge,
            kind: EdgeMutationKind::Delete,
            prepared: PreparedAppend {
                metadata: self.metadata.clone(),
                source: Participant {
                    placement: self.placements[&edge.partition].clone(),
                    batch: source,
                },
                reverse,
            },
        })
    }
    pub fn prepare_vertex(
        &self,
        partition: u32,
        vertex: &Vertex,
    ) -> ExperimentalResult<PreparedAppend> {
        let batch = self.store(partition)?.prepare_vertex(vertex)?;
        self.op.check(None)?;
        Ok(PreparedAppend {
            metadata: self.metadata.clone(),
            source: Participant {
                placement: self.placements[&partition].clone(),
                batch,
            },
            reverse: None,
        })
    }
    pub fn prepare_edge(&self, edge: &CrossEdge) -> ExperimentalResult<PreparedAppend> {
        let source = self.store(edge.from.partition)?.prepare_source(edge)?;
        let reverse = self.store(edge.to.partition)?.prepare_reverse(edge)?;
        let source_placement = self.placements[&edge.from.partition].clone();
        let reverse_placement = self.placements[&edge.to.partition].clone();
        let (source, reverse) = if source.shard() == reverse.shard() {
            (source.combine(reverse)?, None)
        } else {
            (
                source,
                Some(Participant {
                    placement: reverse_placement,
                    batch: reverse,
                }),
            )
        };
        self.op.check(None)?;
        Ok(PreparedAppend {
            metadata: self.metadata.clone(),
            source: Participant {
                placement: source_placement,
                batch: source,
            },
            reverse,
        })
    }
}
impl RouteResolver {
    /// Pins all partitions from one catalog read (32 partitions / 8 shards).
    /// Frozen placement and snapshots are retained; no live read fallback.
    pub fn experimental_read_session(
        &self,
        timeout: Duration,
    ) -> ExperimentalResult<ExperimentalReadSession> {
        let op = self.start(timeout)?;
        let catalog = select_read(&self.metadata.replicas, &op, None, |id| {
            self.catalogs[&id]
                .scoped(op.end, self.dispatches.clone())
                .snapshot()
                .map_err(Into::into)
        })?;
        if catalog.partition_count() > MAX_SCAN_PARTITIONS {
            return Err(scan_capacity().into());
        }
        let mut placements = BTreeMap::new();
        let mut shards = BTreeMap::new();
        for partition in catalog.partitions() {
            let p = catalog
                .placement(partition)
                .expect("catalog partition")
                .clone();
            for id in &p.replicas {
                if !self.replicas.contains_key(&(p.group, *id)) {
                    return Err(RouteError::MissingReplica {
                        group: p.group,
                        node: *id,
                    }
                    .into());
                }
            }
            shards.insert(p.shard, p.clone());
            placements.insert(partition, p);
        }
        if shards.len() > MAX_SCAN_SHARDS {
            return Err(scan_capacity().into());
        }
        let mut nodes = BTreeMap::new();
        let mut snapshots = Vec::new();
        let (mut bytes, mut rows) = (0, 0);
        for p in shards.values() {
            let (adapter, view) = select_read(&p.replicas, &op, None, |id| {
                let adapter = self.adapter(p, id, &op);
                let view = adapter.read_view()?;
                if view.epoch != p.epoch {
                    return Err(StorageNodeError::Fenced {
                        shard: p.shard,
                        epoch: p.epoch,
                        current: view.epoch,
                    }
                    .into());
                }
                Ok((adapter, view))
            })?;
            admit_snapshot(
                &mut bytes,
                &mut rows,
                view.payload_bytes(),
                view.row_count(),
            )?;
            snapshots.push(ShardReadStamp {
                group: p.group,
                shard: p.shard,
                epoch: view.epoch,
                sequence: view.sequence,
            });
            nodes.insert(
                p.shard,
                PinnedNode {
                    adapter,
                    view,
                    op: op.clone(),
                },
            );
        }
        op.check(None)?;
        Ok(ExperimentalReadSession {
            metadata: self.metadata.clone(),
            metadata_sequence: catalog.sequence(),
            snapshots,
            placements,
            nodes,
            op,
        })
    }
    /// Submit the original immutable mutation, source then optional reverse.
    /// Unknown results retain their stage and request ID; no refresh/reprepare.
    /// Exact retry acknowledges history and never reapplies over newer values.
    pub fn experimental_commit_mutation(
        &self,
        prepared: &PreparedEdgeMutation,
        timeout: Duration,
    ) -> ExperimentalResult<MutationAcknowledgement> {
        self.experimental_commit(&prepared.prepared, timeout)?;
        Ok(MutationAcknowledgement::AllBatchesAcknowledged)
    }
    /// Exact retry of frozen batches and placements. Source then reverse. No
    /// automatic unknown-outcome retry, rollback, refresh, or repair.
    pub fn experimental_commit(
        &self,
        prepared: &PreparedAppend,
        timeout: Duration,
    ) -> ExperimentalResult<AppendAcknowledgement> {
        let op = self.start(timeout)?;
        if prepared.metadata != self.metadata {
            return Err(RouteError::PlacementMismatch.into());
        }
        for p in std::iter::once(&prepared.source).chain(prepared.reverse.as_ref()) {
            for id in &p.placement.replicas {
                if !self.replicas.contains_key(&(p.placement.group, *id)) {
                    return Err(RouteError::MissingReplica {
                        group: p.placement.group,
                        node: *id,
                    }
                    .into());
                }
            }
        }
        for (stage, p) in std::iter::once((Stage::Source, &prepared.source))
            .chain(prepared.reverse.as_ref().map(|p| (Stage::Reverse, p)))
        {
            let result = select_attempt(
                &p.placement.replicas,
                &op,
                Some(p.batch.request_id()),
                |id| {
                    let adapter = self.adapter(&p.placement, id, &op);
                    if let Err(e) = adapter.probe_leader() {
                        return (Err(e.into()), true);
                    }
                    let result = p.batch.submit(&adapter).map_err(|e| match e {
                        ExperimentalError::Storage(e) => RouteError::Storage(e),
                        _ => RouteError::InvalidArgument("v2 batch submission"),
                    });
                    (result, false)
                },
            );
            if let Err(error) = result {
                let definite = matches!(
                    &error,
                    RouteError::Storage(
                        StorageNodeError::Conflict { .. }
                            | StorageNodeError::Fenced { .. }
                            | StorageNodeError::IdempotencyConflict(_)
                            | StorageNodeError::InvalidArgument(_)
                            | StorageNodeError::Unsupported(_)
                            | StorageNodeError::NotLeader { .. }
                    )
                );
                let outcome = if !definite {
                    StageOutcome::Unknown
                } else if stage == Stage::Reverse {
                    StageOutcome::Incomplete
                } else {
                    StageOutcome::Rejected
                };
                return Err(ExperimentalRouteError::Stage {
                    stage,
                    outcome,
                    request_id: p.batch.request_id(),
                    source_acknowledged_this_attempt: stage == Stage::Reverse,
                    error,
                });
            }
        }
        Ok(AppendAcknowledgement::AllBatchesAcknowledged)
    }
}
