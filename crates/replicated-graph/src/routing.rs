//! Bounded local or static remote routing for immutable catalog placement.
//!
//! Every resolver operation reads metadata afresh. Hints only reorder a fixed
//! candidate set; unavailable read-only probes may advance to another candidate.
//! After write submission, only `NotLeader` is retried. A read session pins one
//! adapter and view. Timeout/unavailable writes have unknown outcomes: keep the
//! original prepared value, and explicitly retry it, never prepare it again.
use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};

use d2_raft::NodeHandle;
use rocksdb_sql_table::{
    Edge, GraphRecordStore, GremlinResult, KeyRange, KeyValue, PreparedGraphMutation, ReadView,
    RequestId, ScanCursor, ScanLimit, ScanPage, StorageNode, StorageNodeError, Vertex,
    WriteRequest, WriteResult,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    adapter::Adapter,
    catalog::{Catalog, CatalogError, Placement, MAX_REPLICAS, MAX_SHARDS},
    RemoteStorageNode, ReplicatedStorageNode,
};

/// Shared limits across all clones, groups, candidates, and retained sessions.
pub const MAX_OPERATIONS: usize = 32;
pub const MAX_REGISTERED_REPLICAS: usize = MAX_SHARDS * MAX_REPLICAS;
pub const MAX_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

/// Global-scan fanout and aggregate retained-snapshot admission bounds.
pub const MAX_SCAN_PARTITIONS: usize = 32;
pub const MAX_SCAN_SHARDS: usize = 8;
pub const MAX_SCAN_VIEW_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_SCAN_VIEW_ROWS: usize = 200_000;

/// All partitions in one frozen catalog, or an explicit nonduplicated selection.
#[derive(Clone, Debug)]
pub enum ScanScope {
    All,
    Selected(Vec<u32>),
}

/// One component of a per-shard snapshot vector, not a global timestamp.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardReadStamp {
    pub group: u64,
    pub shard: u64,
    pub epoch: u64,
    pub sequence: u64,
}

/// A global ordered scan over independently acquired immutable shard views.
#[derive(Clone, Debug, PartialEq)]
pub struct GlobalScanResult {
    pub metadata_sequence: u64,
    pub snapshots: Vec<ShardReadStamp>,
    pub result: rocksdb_sql_table::GlobalScanOutput,
}

#[derive(Debug)]
pub enum RouteError {
    Storage(StorageNodeError),
    Catalog(CatalogError),
    Graph(rocksdb_sql_table::Error),
    InvalidArgument(&'static str),
    MissingPartition(u32),
    MissingReplica { group: u64, node: u64 },
    PlacementMismatch,
}
impl fmt::Display for RouteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(e) => write!(f, "{e}"),
            Self::Catalog(e) => write!(f, "{e}"),
            Self::Graph(e) => write!(f, "{e}"),
            Self::InvalidArgument(s) => write!(f, "invalid route argument: {s}"),
            Self::MissingPartition(p) => write!(f, "partition {p} is not registered"),
            Self::MissingReplica { group, node } => {
                write!(f, "unregistered replica ({group}, {node})")
            }
            Self::PlacementMismatch => f.write_str("prepared route identity mismatch"),
        }
    }
}
impl Error for RouteError {}
impl From<StorageNodeError> for RouteError {
    fn from(e: StorageNodeError) -> Self {
        Self::Storage(e)
    }
}
impl From<CatalogError> for RouteError {
    fn from(e: CatalogError) -> Self {
        match e {
            CatalogError::Storage(e) => Self::Storage(e),
            e => Self::Catalog(e),
        }
    }
}
impl From<rocksdb_sql_table::Error> for RouteError {
    fn from(e: rocksdb_sql_table::Error) -> Self {
        match e {
            rocksdb_sql_table::Error::StorageNode(e) => Self::Storage(e),
            e => Self::Graph(e),
        }
    }
}
pub type Result<T> = std::result::Result<T, RouteError>;

// Holding the operation permit through session lifetime bounds retained views.
// Dispatch tasks additionally hold their shared dispatch permit until completion,
// including cancellation delays after the synchronous caller times out.
struct Operation {
    end: Instant,
    _permit: OwnedSemaphorePermit,
}
impl Operation {
    fn check(&self, request_id: Option<RequestId>) -> std::result::Result<(), StorageNodeError> {
        if Instant::now() >= self.end {
            Err(StorageNodeError::Timeout { request_id })
        } else {
            Ok(())
        }
    }
}

/// One configured graph-service endpoint. The server checks both IDs on every
/// operation. This is mismatch detection, not authentication or a directory.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemoteReplica {
    pub group: u64,
    pub node: u64,
    pub endpoint: std::net::SocketAddr,
}

/// Static concrete local handles or identified remote endpoints. Construct on
/// the hosting runtime. All calls must use `run_blocking` or a plain thread.
/// Separate resolver instances have separate budgets; reuse clones for one client.
#[derive(Clone)]
pub struct RouteResolver {
    metadata: Placement,
    catalogs: Arc<BTreeMap<u64, Catalog>>,
    replicas: Arc<BTreeMap<(u64, u64), Adapter>>,
    operations: Arc<Semaphore>,
    dispatches: Arc<Semaphore>,
}
impl RouteResolver {
    pub fn new(
        metadata: Placement,
        metadata_nodes: &[NodeHandle],
        data_nodes: &[NodeHandle],
    ) -> Result<Self> {
        metadata.validate()?;
        if metadata_nodes.len() != metadata.replicas.len()
            || data_nodes.len() > MAX_REGISTERED_REPLICAS
        {
            return Err(RouteError::InvalidArgument("fixed replica registry size"));
        }
        let mut catalogs = BTreeMap::new();
        for node in metadata_nodes {
            let catalog = Catalog::new(node, metadata.clone(), MAX_OPERATION_TIMEOUT)?;
            if catalogs.insert(node.id, catalog).is_some() {
                return Err(RouteError::InvalidArgument("duplicate metadata replica"));
            }
        }
        let mut replicas = BTreeMap::new();
        for node in data_nodes {
            if node.group_id() == metadata.group {
                return Err(RouteError::InvalidArgument("metadata group is reserved"));
            }
            let adapter = ReplicatedStorageNode::new(node, 0, MAX_OPERATION_TIMEOUT)?;
            if replicas
                .insert((node.group_id(), node.id), Adapter::Local(adapter))
                .is_some()
            {
                return Err(RouteError::InvalidArgument("duplicate data replica"));
            }
        }
        Ok(Self {
            metadata,
            catalogs: Arc::new(catalogs),
            replicas: Arc::new(replicas),
            operations: Arc::new(Semaphore::new(MAX_OPERATIONS)),
            dispatches: Arc::new(Semaphore::new(MAX_OPERATIONS)),
        })
    }

    /// Build a static remote resolver. Addresses are configured by the caller,
    /// never learned from leader hints. Duplicate keys and reused addresses fail
    /// closed. Every selected placement must have all its replicas registered.
    pub fn new_remote(
        metadata: Placement,
        metadata_nodes: &[RemoteReplica],
        data_nodes: &[RemoteReplica],
    ) -> Result<Self> {
        metadata.validate()?;
        if metadata_nodes.len() != metadata.replicas.len()
            || data_nodes.len() > MAX_REGISTERED_REPLICAS
        {
            return Err(RouteError::InvalidArgument("fixed replica registry size"));
        }
        let mut catalogs = BTreeMap::new();
        let mut addresses = BTreeSet::new();
        for node in metadata_nodes {
            if node.group != metadata.group || !addresses.insert(node.endpoint) {
                return Err(RouteError::InvalidArgument(
                    "metadata identity or duplicate address",
                ));
            }
            let catalog = Catalog::new_remote(
                node.endpoint,
                node.node,
                metadata.clone(),
                MAX_OPERATION_TIMEOUT,
            )?;
            if catalogs.insert(node.node, catalog).is_some() {
                return Err(RouteError::InvalidArgument("duplicate metadata replica"));
            }
        }
        let mut replicas = BTreeMap::new();
        for node in data_nodes {
            if node.group == metadata.group || !addresses.insert(node.endpoint) {
                return Err(RouteError::InvalidArgument(
                    "reserved group or duplicate address",
                ));
            }
            let adapter = RemoteStorageNode::new_identified(
                node.endpoint,
                node.group,
                node.node,
                0,
                MAX_OPERATION_TIMEOUT,
            )?;
            if replicas
                .insert((node.group, node.node), Adapter::Remote(adapter))
                .is_some()
            {
                return Err(RouteError::InvalidArgument("duplicate data replica"));
            }
        }
        Ok(Self {
            metadata,
            catalogs: Arc::new(catalogs),
            replicas: Arc::new(replicas),
            operations: Arc::new(Semaphore::new(MAX_OPERATIONS)),
            dispatches: Arc::new(Semaphore::new(MAX_OPERATIONS)),
        })
    }

    fn start(&self, timeout: Duration) -> Result<Arc<Operation>> {
        // Fail before admission, metadata access, and any graph mutex.
        if tokio::runtime::Handle::try_current().is_ok()
            && !crate::BLOCKING_SCOPE.with(std::cell::Cell::get)
        {
            return Err(StorageNodeError::Unsupported(
                "synchronous routing on a Tokio worker; use run_blocking".into(),
            )
            .into());
        }
        if timeout.is_zero() || timeout > MAX_OPERATION_TIMEOUT {
            return Err(RouteError::InvalidArgument(
                "timeout must be positive and at most 60 seconds",
            ));
        }
        let end = Instant::now()
            .checked_add(timeout)
            .ok_or(RouteError::InvalidArgument("deadline overflow"))?;
        let permit = self.operations.clone().try_acquire_owned().map_err(|_| {
            StorageNodeError::ResourceExhausted(
                "32 route operations or retained sessions already admitted".into(),
            )
        })?;
        Ok(Arc::new(Operation {
            end,
            _permit: permit,
        }))
    }

    fn placement(&self, partition: u32, op: &Operation) -> Result<(Placement, u64)> {
        let found = select_read(&self.metadata.replicas, op, None, |id| {
            self.catalogs[&id]
                .scoped(op.end, self.dispatches.clone())
                .snapshot()
                .map_err(Into::into)
        })?;
        let placement = found
            .placement(partition)
            .cloned()
            .ok_or(RouteError::MissingPartition(partition))?;
        for id in &placement.replicas {
            if !self.replicas.contains_key(&(placement.group, *id)) {
                return Err(RouteError::MissingReplica {
                    group: placement.group,
                    node: *id,
                });
            }
        }
        Ok((placement, found.sequence()))
    }

    fn adapter(&self, p: &Placement, id: u64, op: &Operation) -> Adapter {
        self.replicas[&(p.group, id)].scoped(p.shard, op.end, self.dispatches.clone())
    }

    /// Fresh metadata and data leader barriers share one absolute deadline. Once
    /// returned, this session never changes its replica, sequence, or deadline.
    pub fn read_view(&self, partition: u32, timeout: Duration) -> Result<RoutedReadView> {
        let op = self.start(timeout)?;
        let (placement, metadata_sequence) = self.placement(partition, &op)?;
        let (adapter, view) = select_read(&placement.replicas, &op, None, |id| {
            let adapter = self.adapter(&placement, id, &op);
            let view = adapter.read_view()?;
            if view.epoch != placement.epoch {
                return Err(StorageNodeError::Fenced {
                    shard: placement.shard,
                    epoch: placement.epoch,
                    current: view.epoch,
                }
                .into());
            }
            Ok((adapter, view))
        })?;
        Ok(RoutedReadView {
            metadata_sequence,
            metadata: self.metadata.clone(),
            placement,
            partition,
            pinned: PinnedNode { adapter, view, op },
        })
    }

    /// Execute V()/E(), optional limit(n), and optional terminal count() globally.
    /// The catalog is read once, each unique shard is pinned once, and all data
    /// acquisition/reads share one absolute deadline (at most five seconds).
    /// Rows sort by (ID, partition); any required shard failure aborts the query.
    /// Snapshots form a vector, not a globally consistent snapshot. The existing
    /// single-partition Gremlin API and its broader subset are unchanged.
    pub fn execute_global_scan(
        &self,
        scope: ScanScope,
        query: &str,
        timeout: Duration,
    ) -> Result<GlobalScanResult> {
        self.execute_global_scan_observed(scope, query, timeout, |_| Ok(()))
    }

    // Acquisition checkpoints also permit deterministic race/deadline tests.
    // No callback is exposed through the public API.
    fn execute_global_scan_observed(
        &self,
        scope: ScanScope,
        query: &str,
        timeout: Duration,
        mut observer: impl FnMut(Option<u64>) -> Result<()>,
    ) -> Result<GlobalScanResult> {
        let plan = rocksdb_sql_table::GlobalScanPlan::parse(query)?;
        if timeout.is_zero() || timeout > MAX_OPERATION_TIMEOUT {
            return Err(RouteError::InvalidArgument(
                "timeout must be positive and at most 60 seconds",
            ));
        }
        let op = self.start(timeout.min(rocksdb_sql_table::GREMLIN_DEADLINE))?;
        let catalog = select_read(&self.metadata.replicas, &op, None, |id| {
            self.catalogs[&id]
                .scoped(op.end, self.dispatches.clone())
                .snapshot()
                .map_err(Into::into)
        })?;
        observer(None)?;
        op.check(None)?;
        let partitions = match scope {
            ScanScope::All => {
                if catalog.partition_count() > MAX_SCAN_PARTITIONS {
                    return Err(scan_capacity());
                }
                catalog.partitions().collect::<Vec<_>>()
            }
            ScanScope::Selected(mut selected) => {
                if selected.len() > MAX_SCAN_PARTITIONS {
                    return Err(scan_capacity());
                }
                selected.sort_unstable();
                if selected.windows(2).any(|p| p[0] == p[1]) {
                    return Err(RouteError::InvalidArgument("duplicate scan partition"));
                }
                selected
            }
        };
        // Resolve the entire selection before data I/O, never refresh metadata.
        let mut placements = BTreeMap::new();
        for partition in &partitions {
            op.check(None)?;
            let placement = catalog
                .placement(*partition)
                .ok_or(RouteError::MissingPartition(*partition))?;
            for id in &placement.replicas {
                if !self.replicas.contains_key(&(placement.group, *id)) {
                    return Err(RouteError::MissingReplica {
                        group: placement.group,
                        node: *id,
                    });
                }
            }
            placements.insert(placement.shard, placement.clone());
            if placements.len() > MAX_SCAN_SHARDS {
                return Err(scan_capacity());
            }
        }
        let mut pinned = BTreeMap::new();
        let mut snapshots = Vec::with_capacity(placements.len());
        let (mut bytes, mut rows) = (0usize, 0usize);
        for placement in placements.values() {
            let (adapter, view) = select_read(&placement.replicas, &op, None, |id| {
                let adapter = self.adapter(placement, id, &op);
                let view = adapter.read_view()?;
                if view.epoch != placement.epoch {
                    return Err(StorageNodeError::Fenced {
                        shard: placement.shard,
                        epoch: placement.epoch,
                        current: view.epoch,
                    }
                    .into());
                }
                Ok((adapter, view))
            })?;
            // Acquisition allocation is adapter-owned; charge before retaining
            // the new view alongside the already admitted snapshots. No RSS claim.
            admit_snapshot(
                &mut bytes,
                &mut rows,
                view.payload_bytes(),
                view.row_count(),
            )?;
            snapshots.push(ShardReadStamp {
                group: placement.group,
                shard: placement.shard,
                epoch: view.epoch,
                sequence: view.sequence,
            });
            pinned.insert(
                placement.shard,
                PinnedNode {
                    adapter,
                    view,
                    op: op.clone(),
                },
            );
            observer(Some(placement.shard))?;
            op.check(None)?;
        }
        let stores = partitions
            .iter()
            .map(|partition| {
                let shard = catalog
                    .placement(*partition)
                    .expect("validated partition")
                    .shard;
                (
                    *partition,
                    GraphRecordStore::new(pinned[&shard].clone(), shard, *partition),
                )
            })
            .collect::<Vec<_>>();
        // PinnedNode::read_view clones the same Arc-backed view, never a new
        // data snapshot, even when several partitions share one shard.
        let views = stores
            .iter()
            .map(|(partition, store)| Ok((*partition, store.read_view()?)))
            .collect::<rocksdb_sql_table::Result<Vec<_>>>()?;
        let result = plan.execute(&views, &|| op.check(None).map_err(Into::into))?;
        op.check(None)?;
        Ok(GlobalScanResult {
            metadata_sequence: catalog.sequence(),
            snapshots,
            result,
        })
    }

    /// Explicit exact retry only. Fresh metadata must match the immutable
    /// prepared placement. Conflict, epoch, timeout and unavailable are terminal;
    /// only `NotLeader` may select another fixed candidate within this call.
    pub fn commit_prepared(
        &self,
        prepared: &RoutedPreparedMutation,
        timeout: Duration,
    ) -> Result<()> {
        let op = self.start(timeout)?;
        if prepared.metadata != self.metadata {
            return Err(RouteError::PlacementMismatch);
        }
        let (p, _) = self.placement(prepared.mutation.partition(), &op)?;
        if p != prepared.placement {
            return Err(RouteError::PlacementMismatch);
        }
        let id = prepared.request_id();
        select_attempt(&p.replicas, &op, Some(id), |replica| {
            let adapter = self.adapter(&p, replica, &op);
            if let Err(error) = adapter.probe_leader() {
                return (Err(error.into()), true);
            }
            let result = GraphRecordStore::new(adapter, p.shard, prepared.mutation.partition())
                .commit_prepared(&prepared.mutation)
                .map_err(Into::into);
            // Once submitted, an unavailable response is ambiguous and terminal.
            (result, false)
        })
    }
}

fn scan_capacity() -> RouteError {
    StorageNodeError::ResourceExhausted(
        "global scan fanout or aggregate snapshot limit exceeded".into(),
    )
    .into()
}

fn admit_snapshot(
    bytes: &mut usize,
    rows: &mut usize,
    incoming_bytes: usize,
    incoming_rows: usize,
) -> Result<()> {
    let total_bytes = bytes.saturating_add(incoming_bytes);
    let total_rows = rows.saturating_add(incoming_rows);
    if total_bytes > MAX_SCAN_VIEW_BYTES || total_rows > MAX_SCAN_VIEW_ROWS {
        return Err(scan_capacity());
    }
    *bytes = total_bytes;
    *rows = total_rows;
    Ok(())
}

// At most one attempt per fixed replica, so even cyclic/self/foreign hints
// cannot increase the attempt bound or cause cross-group dispatch. No sleeps,
// recursive refreshes, metadata cache, or retry on ambiguous write outcomes.
#[cfg(test)]
fn select<T>(
    candidates: &[u64],
    op: &Operation,
    request_id: Option<RequestId>,
    mut attempt: impl FnMut(u64) -> Result<T>,
) -> Result<T> {
    select_attempt(candidates, op, request_id, |id| (attempt(id), false))
}

fn select_read<T>(
    candidates: &[u64],
    op: &Operation,
    request_id: Option<RequestId>,
    mut attempt: impl FnMut(u64) -> Result<T>,
) -> Result<T> {
    select_attempt(candidates, op, request_id, |id| (attempt(id), true))
}

fn select_attempt<T>(
    candidates: &[u64],
    op: &Operation,
    request_id: Option<RequestId>,
    mut attempt: impl FnMut(u64) -> (Result<T>, bool),
) -> Result<T> {
    let mut visited = BTreeSet::new();
    let mut hint = None;
    let mut last = None;
    for _ in 0..candidates.len() {
        op.check(request_id)?;
        let id = hint
            .take()
            .filter(|id| candidates.contains(id) && !visited.contains(id))
            .or_else(|| candidates.iter().copied().find(|id| !visited.contains(id)))
            .expect("one unvisited replica per bounded iteration");
        visited.insert(id);
        let (result, read_only) = attempt(id);
        match result {
            Ok(result) => {
                op.check(request_id)?;
                return Ok(result);
            }
            Err(RouteError::Storage(e @ StorageNodeError::NotLeader { .. })) => {
                if let StorageNodeError::NotLeader { leader } = &e {
                    hint = *leader;
                }
                last = Some(e);
            }
            Err(RouteError::Storage(e @ StorageNodeError::Unavailable(_))) if read_only => {
                last = Some(e);
            }
            Err(e) => return Err(e),
        }
    }
    Err(last
        .unwrap_or_else(|| StorageNodeError::Unavailable("no routing candidates".into()))
        .into())
}

/// One immutable graph request plus full static placement, retained by the
/// caller across unknown outcomes. Not a durable client journal.
#[derive(Clone, Debug)]
pub struct RoutedPreparedMutation {
    metadata: Placement,
    placement: Placement,
    mutation: PreparedGraphMutation,
}
impl RoutedPreparedMutation {
    pub fn request_id(&self) -> RequestId {
        self.mutation.request_id()
    }
    pub fn expected_sequence(&self) -> u64 {
        self.mutation.expected_sequence()
    }
    pub fn placement(&self) -> &Placement {
        &self.placement
    }
}

#[derive(Clone)]
struct PinnedNode {
    adapter: Adapter,
    view: ReadView,
    op: Arc<Operation>,
}
impl StorageNode for PinnedNode {
    fn admin(&self) -> &dyn rocksdb_sql_table::StorageAdmin {
        self.adapter.admin()
    }
    fn check_execution_context(&self) -> std::result::Result<(), StorageNodeError> {
        self.adapter.check_execution_context()?;
        self.op.check(None)
    }
    fn read_view(&self) -> std::result::Result<ReadView, StorageNodeError> {
        self.check_execution_context()?;
        self.adapter.check_view(&self.view)?;
        Ok(self.view.clone())
    }
    fn get(
        &self,
        key: &[u8],
        _view: &ReadView,
    ) -> std::result::Result<Option<Vec<u8>>, StorageNodeError> {
        self.check_execution_context()?;
        self.adapter.get(key, &self.view)
    }
    fn scan(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        _view: &ReadView,
    ) -> std::result::Result<Vec<KeyValue>, StorageNodeError> {
        self.check_execution_context()?;
        self.adapter.scan(range, limit, &self.view)
    }
    fn scan_page(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        _view: &ReadView,
        cursor: Option<&ScanCursor>,
    ) -> std::result::Result<ScanPage, StorageNodeError> {
        self.check_execution_context()?;
        self.adapter.scan_page(range, limit, &self.view, cursor)
    }
    fn batch_write(&self, _: WriteRequest) -> std::result::Result<WriteResult, StorageNodeError> {
        Err(StorageNodeError::Unsupported(
            "routed read sessions cannot commit; retain a prepared mutation".into(),
        ))
    }
}

/// Retains the original adapter, view, absolute deadline and admission slot.
/// Clones share all four. Drop all clones to release the session slot. Pages,
/// Gremlin and preparation never refresh metadata or switch to a new leader.
#[derive(Clone)]
pub struct RoutedReadView {
    metadata_sequence: u64,
    metadata: Placement,
    placement: Placement,
    partition: u32,
    pinned: PinnedNode,
}
impl RoutedReadView {
    /// Sequence of the fresh metadata snapshot that selected this placement.
    pub fn metadata_sequence(&self) -> u64 {
        self.metadata_sequence
    }
    pub fn placement(&self) -> &Placement {
        &self.placement
    }
    pub fn sequence(&self) -> u64 {
        self.pinned.view.sequence
    }
    pub fn epoch(&self) -> u64 {
        self.pinned.view.epoch
    }
    fn finished<T>(&self, value: T) -> Result<T> {
        self.pinned.check_execution_context()?;
        Ok(value)
    }
    fn graph(&self) -> GraphRecordStore<PinnedNode> {
        GraphRecordStore::new(self.pinned.clone(), self.placement.shard, self.partition)
    }
    pub fn execute_gremlin(&self, query: &str) -> Result<GremlinResult> {
        self.finished(self.graph().execute_gremlin(query)?)
    }
    pub fn get_vertex(&self, id: u64) -> Result<Option<Vertex>> {
        self.finished(self.graph().get_vertex(id)?)
    }
    pub fn get_edge(&self, id: u64) -> Result<Option<Edge>> {
        self.finished(self.graph().get_edge(id)?)
    }
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.finished(self.pinned.get(key, &self.pinned.view)?)
    }
    pub fn scan_page(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        cursor: Option<&ScanCursor>,
    ) -> Result<ScanPage> {
        self.finished(
            self.pinned
                .scan_page(range, limit, &self.pinned.view, cursor)?,
        )
    }
    fn prepared(&self, mutation: PreparedGraphMutation) -> Result<RoutedPreparedMutation> {
        self.pinned.check_execution_context()?;
        Ok(RoutedPreparedMutation {
            metadata: self.metadata.clone(),
            placement: self.placement.clone(),
            mutation,
        })
    }
    pub fn prepare_create_vertex(&self, vertex: &Vertex) -> Result<RoutedPreparedMutation> {
        self.prepared(self.graph().prepare_create_vertex(vertex)?)
    }
    pub fn prepare_update_vertex(&self, vertex: &Vertex) -> Result<RoutedPreparedMutation> {
        self.prepared(self.graph().prepare_update_vertex(vertex)?)
    }
    pub fn prepare_delete_vertex(&self, id: u64) -> Result<RoutedPreparedMutation> {
        self.prepared(self.graph().prepare_delete_vertex(id)?)
    }
    pub fn prepare_create_edge(&self, edge: &Edge) -> Result<RoutedPreparedMutation> {
        self.prepared(self.graph().prepare_create_edge(edge)?)
    }
    pub fn prepare_update_edge(&self, edge: &Edge) -> Result<RoutedPreparedMutation> {
        self.prepared(self.graph().prepare_update_edge(edge)?)
    }
    pub fn prepare_delete_edge(&self, id: u64) -> Result<RoutedPreparedMutation> {
        self.prepared(self.graph().prepare_delete_edge(id)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_admission_is_aggregate_and_overflow_safe() {
        let (mut bytes, mut rows) = (0, 0);
        admit_snapshot(
            &mut bytes,
            &mut rows,
            MAX_SCAN_VIEW_BYTES / 2,
            MAX_SCAN_VIEW_ROWS / 2,
        )
        .unwrap();
        admit_snapshot(
            &mut bytes,
            &mut rows,
            MAX_SCAN_VIEW_BYTES / 2,
            MAX_SCAN_VIEW_ROWS / 2,
        )
        .unwrap();
        assert_eq!((bytes, rows), (MAX_SCAN_VIEW_BYTES, MAX_SCAN_VIEW_ROWS));
        assert!(admit_snapshot(&mut bytes, &mut rows, 1, 0).is_err());
        assert!(admit_snapshot(&mut bytes, &mut rows, 0, 1).is_err());
        assert!(admit_snapshot(&mut bytes, &mut rows, usize::MAX, usize::MAX).is_err());
        assert_eq!((bytes, rows), (MAX_SCAN_VIEW_BYTES, MAX_SCAN_VIEW_ROWS));
    }

    fn operation(end: Instant) -> Operation {
        Operation {
            end,
            _permit: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
        }
    }
    #[test]
    fn foreign_self_and_cyclic_hints_never_expand_candidates() {
        for hints in [
            [Some(999), Some(999), Some(999)],
            [Some(1), Some(2), Some(3)],
            [Some(3), Some(1), Some(3)],
        ] {
            let op = operation(Instant::now() + Duration::from_secs(10));
            let end = op.end;
            let mut visited = Vec::new();
            let result: Result<()> = select(&[1, 2, 3], &op, None, |id| {
                assert_eq!(op.end, end);
                visited.push(id);
                Err(StorageNodeError::NotLeader {
                    leader: hints[(id - 1) as usize],
                }
                .into())
            });
            assert!(matches!(
                result,
                Err(RouteError::Storage(StorageNodeError::NotLeader { .. }))
            ));
            assert_eq!(visited.len(), 3);
            visited.sort();
            assert_eq!(visited, [1, 2, 3]);
        }
    }
    #[test]
    fn valid_hint_reorders_only_unvisited_replicas() {
        let op = operation(Instant::now() + Duration::from_secs(10));
        let mut visited = Vec::new();
        let value = select(&[1, 2, 3], &op, None, |id| {
            visited.push(id);
            if id == 1 {
                Err(StorageNodeError::NotLeader { leader: Some(3) }.into())
            } else {
                Ok(7)
            }
        })
        .unwrap();
        assert_eq!(value, 7);
        assert_eq!(visited, [1, 3]);
    }
    #[test]
    fn expired_deadline_prevents_any_attempt_and_preserves_write_identity() {
        let op = operation(Instant::now());
        let result: Result<()> = select(&[1, 2, 3], &op, Some([9; 16]), |_| {
            panic!("expired attempt")
        });
        assert!(
            matches!(result, Err(RouteError::Storage(StorageNodeError::Timeout { request_id: Some(id) })) if id == [9; 16])
        );
    }
    #[test]
    fn terminal_errors_do_not_retry_or_refresh_preparation() {
        let errors = [
            StorageNodeError::Timeout {
                request_id: Some([7; 16]),
            },
            StorageNodeError::Unavailable("unknown outcome".into()),
            StorageNodeError::Conflict {
                expected: 1,
                current: 2,
            },
            StorageNodeError::Fenced {
                shard: 7,
                epoch: 1,
                current: 2,
            },
            StorageNodeError::IdempotencyConflict([7; 16]),
            StorageNodeError::ResourceExhausted("full".into()),
        ];
        for error in errors {
            let mut error = Some(error);
            let mut calls = 0;
            let result: Result<()> = select(
                &[1, 2, 3],
                &operation(Instant::now() + Duration::from_secs(10)),
                Some([7; 16]),
                |_| {
                    calls += 1;
                    Err(error.take().expect("only one attempt").into())
                },
            );
            assert!(result.is_err());
            assert_eq!(calls, 1);
        }
    }
    #[test]
    fn unavailable_read_only_probe_can_advance_but_write_is_terminal() {
        let op = operation(Instant::now() + Duration::from_secs(10));
        let mut seen = Vec::new();
        let result = select_attempt(&[1, 2, 3], &op, Some([1; 16]), |id| {
            seen.push(id);
            (
                Err::<(), _>(StorageNodeError::Unavailable("offline or unknown".into()).into()),
                id == 1,
            )
        });
        assert!(matches!(
            result,
            Err(RouteError::Storage(StorageNodeError::Unavailable(_)))
        ));
        assert_eq!(seen, [1, 2]);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn all_clones_groups_and_candidates_share_both_admission_bounds() {
        let cluster = d2_raft::Cluster::new();
        let meta = cluster.in_group(10).start(1).await.unwrap();
        let a = cluster.in_group(20).start(1).await.unwrap();
        let b = cluster.in_group(30).start(1).await.unwrap();
        let data = [a, b];
        let resolver = RouteResolver::new(
            Placement {
                shard: 10,
                group: 10,
                epoch: 1,
                replicas: vec![1],
            },
            std::slice::from_ref(&meta),
            &data,
        )
        .unwrap();
        assert!(matches!(
            resolver.start(Duration::from_secs(1)),
            Err(RouteError::Storage(StorageNodeError::Unsupported(_)))
        ));
        crate::run_blocking(move || {
            let mut operations = Vec::new();
            for _ in 0..MAX_OPERATIONS {
                operations.push(resolver.clone().start(Duration::from_secs(1)).unwrap());
            }
            assert!(matches!(
                resolver.start(Duration::from_secs(1)),
                Err(RouteError::Storage(StorageNodeError::ResourceExhausted(_)))
            ));
            operations.pop();
            let op = resolver.clone().start(Duration::from_secs(1)).unwrap();
            let pool = resolver.dispatches.clone();
            let held = pool
                .clone()
                .try_acquire_many_owned(MAX_OPERATIONS as u32)
                .unwrap();
            for raw in resolver.replicas.values() {
                let Adapter::Local(node) = raw.scoped(7, op.end, pool.clone()) else {
                    panic!("expected local adapter")
                };
                assert_eq!(node.absolute_deadline, Some(op.end));
                assert!(Arc::ptr_eq(&node.admission, &pool));
                assert!(matches!(
                    node.dispatch(None, async { Ok(()) }),
                    Err(StorageNodeError::ResourceExhausted(_))
                ));
            }
            drop(held);
            for raw in resolver.replicas.values() {
                let Adapter::Local(node) = raw.scoped(7, op.end, pool.clone()) else {
                    panic!("expected local adapter")
                };
                node.dispatch(None, async { Ok(()) }).unwrap();
            }
        })
        .await
        .unwrap();
        cluster.in_group(10).shutdown(&meta).await;
        cluster.in_group(20).shutdown(&data[0]).await;
        cluster.in_group(30).shutdown(&data[1]).await;
    }
}

#[cfg(test)]
mod remote_configuration_tests {
    use super::*;
    fn replica(group: u64, node: u64, port: u16) -> RemoteReplica {
        RemoteReplica {
            group,
            node,
            endpoint: ([127, 0, 0, 1], port).into(),
        }
    }
    #[tokio::test]
    async fn registry_is_complete_bounded_and_group_scoped() {
        let metadata = Placement {
            shard: 90,
            group: 9,
            epoch: 1,
            replicas: vec![1, 2],
        };
        let nodes = [replica(9, 1, 8001), replica(9, 2, 8002)];
        assert!(RouteResolver::new_remote(
            metadata.clone(),
            &nodes,
            &[replica(4, 1, 8011), replica(5, 1, 8021)]
        )
        .is_ok());
        assert!(RouteResolver::new_remote(metadata.clone(), &nodes[..1], &[]).is_err());
        assert!(
            RouteResolver::new_remote(metadata.clone(), &[nodes[0], replica(9, 3, 8003)], &[])
                .is_err()
        );
        assert!(
            RouteResolver::new_remote(metadata.clone(), &[nodes[0], replica(8, 2, 8003)], &[])
                .is_err()
        );
        assert!(
            RouteResolver::new_remote(metadata.clone(), &nodes, &[replica(9, 3, 8011)]).is_err()
        );
        assert!(
            RouteResolver::new_remote(metadata.clone(), &nodes, &[replica(4, 1, 8001)]).is_err()
        );
        assert!(RouteResolver::new_remote(
            metadata.clone(),
            &nodes,
            &[replica(4, 1, 8011), replica(4, 1, 8021)]
        )
        .is_err());
        assert!(RouteResolver::new_remote(
            metadata,
            &nodes,
            &[replica(4, 1, 8011), replica(5, 2, 8011)]
        )
        .is_err());
    }
}

#[cfg(test)]
mod global_scan_tests {
    use super::*;
    use d2_raft::{Cluster, D1Command, LogCommand};
    use openraft::BasicNode;
    use rocksdb_sql_table::GlobalScanOutput;

    const TEST_TIMEOUT: Duration = Duration::from_secs(5);

    fn place(group: u64) -> Placement {
        Placement {
            group,
            shard: group,
            epoch: 1,
            replicas: vec![1],
        }
    }

    async fn start(root: &Cluster, group: u64) -> NodeHandle {
        let node = root.in_group(group).start(1).await.unwrap();
        node.raft
            .initialize(BTreeMap::from([(1, BasicNode::default())]))
            .await
            .unwrap();
        tokio::time::timeout(TEST_TIMEOUT, async {
            loop {
                if node.raft.ensure_linearizable().await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        node.raft
            .client_write(LogCommand::D1V1(D1Command::Activate {
                shard: group,
                epoch: 1,
            }))
            .await
            .unwrap();
        node
    }

    fn register(catalog: &Catalog, partition: u32, placement: Placement) {
        let mut id = [0u8; 16];
        id[..4].copy_from_slice(&partition.to_be_bytes());
        let prepared = catalog
            .prepare_registration(id, partition, placement)
            .unwrap();
        catalog.commit_registration(&prepared).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_and_shared_shard_views_stay_pinned_during_concurrent_changes() {
        let root = Cluster::new();
        let meta = start(&root, 10).await;
        let data = start(&root, 20).await;
        let catalog = Catalog::new(&meta, place(10), TEST_TIMEOUT).unwrap();
        let adapter = ReplicatedStorageNode::new(&data, 20, TEST_TIMEOUT).unwrap();
        let resolver = RouteResolver::new(
            place(10),
            std::slice::from_ref(&meta),
            std::slice::from_ref(&data),
        )
        .unwrap();
        crate::run_blocking(move || {
            register(&catalog, 1, place(20));
            register(&catalog, 2, place(20));
            for partition in 1..=3 {
                GraphRecordStore::new(adapter.clone(), 20, partition)
                    .create_vertex(&Vertex::new(1))
                    .unwrap();
            }
            let mut catalog_callbacks = 0;
            let mut shard_callbacks = 0;
            let before = resolver
                .execute_global_scan_observed(
                    ScanScope::All,
                    "g.V().count()",
                    TEST_TIMEOUT,
                    |shard| {
                        match shard {
                            None => {
                                catalog_callbacks += 1;
                                register(&catalog, 3, place(20));
                            }
                            Some(20) => {
                                shard_callbacks += 1;
                                GraphRecordStore::new(adapter.clone(), 20, 2)
                                    .create_vertex(&Vertex::new(2))
                                    .unwrap();
                            }
                            Some(other) => panic!("unexpected shard {other}"),
                        }
                        Ok(())
                    },
                )
                .unwrap();
            assert_eq!(catalog_callbacks, 1);
            assert_eq!(shard_callbacks, 1);
            assert_eq!(before.snapshots.len(), 1);
            assert_eq!(before.result, GlobalScanOutput::Count(2));
            let after = resolver
                .execute_global_scan(ScanScope::All, "g.V().count()", TEST_TIMEOUT)
                .unwrap();
            assert_eq!(after.result, GlobalScanOutput::Count(4));
            assert_eq!(after.snapshots.len(), 1);
            assert!(after.metadata_sequence > before.metadata_sequence);
            assert!(after.snapshots[0].sequence > before.snapshots[0].sequence);
            assert_eq!(resolver.operations.available_permits(), MAX_OPERATIONS);
        })
        .await
        .unwrap();
        root.in_group(10).shutdown(&meta).await;
        root.in_group(20).shutdown(&data).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn total_deadline_includes_observer_and_releases_operation_permit() {
        let root = Cluster::new();
        let meta = start(&root, 10).await;
        let resolver = RouteResolver::new(place(10), std::slice::from_ref(&meta), &[]).unwrap();
        crate::run_blocking(move || {
            let mut called = false;
            let result = resolver.execute_global_scan_observed(
                ScanScope::All,
                "g.V().count()",
                Duration::from_secs(1),
                |shard| {
                    assert_eq!(shard, None);
                    called = true;
                    std::thread::sleep(Duration::from_millis(1100));
                    Ok(())
                },
            );
            assert!(
                called,
                "catalog acquisition must finish before injected delay"
            );
            assert!(matches!(
                result,
                Err(RouteError::Storage(StorageNodeError::Timeout {
                    request_id: None
                }))
            ));
            assert_eq!(resolver.operations.available_permits(), MAX_OPERATIONS);
            assert_eq!(
                resolver
                    .execute_global_scan(ScanScope::All, "g.V().count()", TEST_TIMEOUT)
                    .unwrap()
                    .result,
                GlobalScanOutput::Count(0)
            );
        })
        .await
        .unwrap();
        root.in_group(10).shutdown(&meta).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn partition_fanout_and_duplicate_selection_fail_without_data_reads() {
        let root = Cluster::new();
        let meta = start(&root, 10).await;
        // The data handle is registered but deliberately never initialized or activated.
        let data = root.in_group(20).start(1).await.unwrap();
        let catalog = Catalog::new(&meta, place(10), TEST_TIMEOUT).unwrap();
        let resolver = RouteResolver::new(
            place(10),
            std::slice::from_ref(&meta),
            std::slice::from_ref(&data),
        )
        .unwrap();
        crate::run_blocking(move || {
            for partition in 1..=33 {
                register(&catalog, partition, place(20));
            }
            let mut pinned = 0;
            let result = resolver.execute_global_scan_observed(
                ScanScope::All,
                "g.V()",
                TEST_TIMEOUT,
                |shard| {
                    pinned += usize::from(shard.is_some());
                    Ok(())
                },
            );
            assert!(matches!(
                result,
                Err(RouteError::Storage(StorageNodeError::ResourceExhausted(_)))
            ));
            assert_eq!(pinned, 0);
            assert!(matches!(
                resolver.execute_global_scan(
                    ScanScope::Selected((1..=33).collect()),
                    "g.V()",
                    TEST_TIMEOUT
                ),
                Err(RouteError::Storage(StorageNodeError::ResourceExhausted(_)))
            ));
            assert!(matches!(
                resolver.execute_global_scan(
                    ScanScope::Selected(vec![1, 1]),
                    "g.V()",
                    TEST_TIMEOUT
                ),
                Err(RouteError::InvalidArgument(_))
            ));
            assert_eq!(resolver.operations.available_permits(), MAX_OPERATIONS);
        })
        .await
        .unwrap();
        root.in_group(10).shutdown(&meta).await;
        root.in_group(20).shutdown(&data).await;
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ninth_shard_fails_before_any_data_snapshot() {
        let root = Cluster::new();
        let meta = start(&root, 10).await;
        let mut data = Vec::new();
        for group in 20..29 {
            data.push(start(&root, group).await);
        }
        let catalog = Catalog::new(&meta, place(10), TEST_TIMEOUT).unwrap();
        let resolver = RouteResolver::new(place(10), std::slice::from_ref(&meta), &data).unwrap();
        crate::run_blocking(move || {
            for partition in 1..=9 {
                register(&catalog, partition, place(u64::from(partition) + 19));
            }
            let eight = resolver
                .execute_global_scan(
                    ScanScope::Selected((1..=8).collect()),
                    "g.V().count()",
                    TEST_TIMEOUT,
                )
                .unwrap();
            assert_eq!(eight.result, GlobalScanOutput::Count(0));
            assert_eq!(eight.snapshots.len(), 8);
            let mut pinned = 0;
            let result = resolver.execute_global_scan_observed(
                ScanScope::All,
                "g.V()",
                TEST_TIMEOUT,
                |shard| {
                    pinned += usize::from(shard.is_some());
                    Ok(())
                },
            );
            assert!(matches!(
                result,
                Err(RouteError::Storage(StorageNodeError::ResourceExhausted(_)))
            ));
            assert_eq!(pinned, 0);
            assert_eq!(resolver.operations.available_permits(), MAX_OPERATIONS);
        })
        .await
        .unwrap();
        root.in_group(10).shutdown(&meta).await;
        for (group, node) in (20..29).zip(data.iter()) {
            root.in_group(group).shutdown(node).await;
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aggregate_retained_snapshot_cap_rejects_two_individually_valid_views() {
        let root = Cluster::new();
        let meta = start(&root, 10).await;
        let a = start(&root, 20).await;
        let b = start(&root, 30).await;
        let catalog = Catalog::new(&meta, place(10), TEST_TIMEOUT).unwrap();
        let adapters = [
            ReplicatedStorageNode::new(&a, 20, TEST_TIMEOUT).unwrap(),
            ReplicatedStorageNode::new(&b, 30, TEST_TIMEOUT).unwrap(),
        ];
        let data = [a, b];
        let resolver = RouteResolver::new(place(10), std::slice::from_ref(&meta), &data).unwrap();
        crate::run_blocking(move || {
            for (partition, shard, node) in [(1, 20, &adapters[0]), (2, 30, &adapters[1])] {
                register(&catalog, partition, place(shard));
                node.batch_write(WriteRequest {
                    shard,
                    epoch: 1,
                    request_id: [9; 16],
                    operations: vec![rocksdb_sql_table::WriteOp::Put {
                        key: b"opaque-outside-graph".to_vec(),
                        value: vec![7; 9 * 1024 * 1024],
                    }],
                })
                .unwrap();
                let single = resolver
                    .execute_global_scan(
                        ScanScope::Selected(vec![partition]),
                        "g.V().count()",
                        TEST_TIMEOUT,
                    )
                    .unwrap();
                assert_eq!(single.result, GlobalScanOutput::Count(0));
                assert_eq!(single.snapshots.len(), 1);
            }
            let result =
                resolver.execute_global_scan(ScanScope::All, "g.V().count()", TEST_TIMEOUT);
            assert!(matches!(
                result,
                Err(RouteError::Storage(StorageNodeError::ResourceExhausted(_)))
            ));
            assert_eq!(resolver.operations.available_permits(), MAX_OPERATIONS);
            assert_eq!(
                resolver
                    .execute_global_scan(
                        ScanScope::Selected(vec![1]),
                        "g.V().count()",
                        TEST_TIMEOUT
                    )
                    .unwrap()
                    .result,
                GlobalScanOutput::Count(0)
            );
        })
        .await
        .unwrap();
        root.in_group(10).shutdown(&meta).await;
        root.in_group(20).shutdown(&data[0]).await;
        root.in_group(30).shutdown(&data[1]).await;
    }
}
