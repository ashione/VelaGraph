//! Single-shard, in-process bridge from synchronous graph records to real Raft.
//!
//! Construct on the runtime hosting the node. Use [`run_blocking`] for graph
//! operations from async code; direct synchronous calls on Tokio workers fail
//! explicitly instead of nesting `block_on` or blocking the executor. The host
//! runtime must remain running. The optional [`routing`] module adds bounded
//! static local and remote routing. On Linux, `gateway` adds a same-host read-only UDS
//! interface with OS peer identity checks. The optional `remote` service and
//! `RemoteStorageNode` add plaintext TCP for trusted development networks.
//! No cross-host authentication or production availability guarantee is provided.
mod adapter;
pub mod catalog;
pub mod managed_gateway;
pub mod managed_graph;
pub mod remote;
pub mod remote_storage;
pub use remote_storage::RemoteStorageNode;
#[cfg(target_os = "linux")]
pub mod gateway;
pub mod routing;

use std::cell::Cell;
use std::future::Future;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use d2_raft::{
    D1Command, D1Error, D1Result, D1WriteRequest, D1WriteStatus, LogCommand, NodeHandle,
    StateMachine, TypeConfig,
};
use rocksdb_sql_table::{
    Epoch, Export, KeyRange, KeyValue, ReadView, RequestId, ScanCursor, ScanLimit, ScanPage,
    Sequence, ShardId, StorageAdmin, StorageNode, StorageNodeError, WriteOp, WriteRequest,
    WriteResult, WriteStatus, MAX_VIEW_BYTES, MAX_VIEW_ROWS,
};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

type Result<T> = std::result::Result<T, StorageNodeError>;
thread_local! { static BLOCKING_SCOPE: Cell<bool> = const { Cell::new(false) }; }

/// Run synchronous graph work on Tokio's blocking pool. Cancellation of this
/// future does not cancel an already started closure or prove a write aborted.
/// Retain a prepared mutation before calling this when retry safety is needed.
pub async fn run_blocking<F, T>(work: F) -> Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                BLOCKING_SCOPE.with(|s| s.set(false));
            }
        }
        BLOCKING_SCOPE.with(|s| s.set(true));
        let _reset = Reset;
        work()
    })
    .await
    .map_err(|_| StorageNodeError::Unavailable("blocking graph task failed".into()))
}

/// Adapter bound to one replica and globally unique logical shard identity.
/// Clone shares a 32-operation admission bound. Select a new adapter explicitly
/// after a typed `NotLeader`; retry the identical prepared batch, not a new
/// high-level mutation, after any ambiguous outcome.
#[derive(Clone)]
pub struct ReplicatedStorageNode {
    raft: openraft::Raft<TypeConfig>,
    state: Arc<StateMachine>,
    runtime: Handle,
    shard: ShardId,
    deadline: Duration,
    absolute_deadline: Option<Instant>,
    admission: Arc<Semaphore>,
}

impl ReplicatedStorageNode {
    /// The node must be hosted by the currently entered runtime and already
    /// activated with D1V1. This constructor does not initialize or activate it.
    pub fn new(node: &NodeHandle, shard: ShardId, deadline: Duration) -> Result<Self> {
        if deadline.is_zero() {
            return Err(StorageNodeError::InvalidArgument(
                "deadline must be positive".into(),
            ));
        }
        let runtime = Handle::try_current().map_err(|_| {
            StorageNodeError::InvalidArgument("construct adapter on its node runtime".into())
        })?;
        Ok(Self {
            raft: node.raft.clone(),
            state: node.state_machine.clone(),
            runtime,
            shard,
            deadline,
            absolute_deadline: None,
            admission: Arc::new(Semaphore::new(32)),
        })
    }

    // Resolver-created clones share one dispatch pool across all groups and
    // keep the original absolute deadline, including runtime queueing time.
    fn scoped(&self, shard: ShardId, end: Instant, admission: Arc<Semaphore>) -> Self {
        let mut node = self.clone();
        node.shard = shard;
        node.absolute_deadline = Some(end);
        node.admission = admission;
        node
    }

    fn dispatch<T, F>(&self, request_id: Option<RequestId>, future: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        self.check_execution_context()?;
        let end = match self.absolute_deadline {
            Some(end) => end,
            None => Instant::now()
                .checked_add(self.deadline)
                .ok_or_else(|| StorageNodeError::InvalidArgument("deadline overflow".into()))?,
        };
        if Instant::now() >= end {
            return Err(StorageNodeError::Timeout { request_id });
        }
        let permit = self.admission.clone().try_acquire_owned().map_err(|_| {
            StorageNodeError::ResourceExhausted("32 adapter operations already in flight".into())
        })?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let task = self.runtime.spawn(async move {
            let _permit = permit;
            let result = poll_before_deadline(end, request_id, future).await;
            let _ = sender.send(result);
        });
        match receiver.recv_timeout(end.saturating_duration_since(Instant::now())) {
            Ok(result) => {
                if Instant::now() >= end {
                    Err(StorageNodeError::Timeout { request_id })
                } else {
                    result
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                task.abort();
                Err(StorageNodeError::Timeout { request_id })
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                Err(StorageNodeError::Unavailable("node runtime stopped".into()))
            }
        }
    }

    // Read-only selection before write dispatch. This is not preparation: it
    // never reads or replaces the original condition, epoch, bytes, or ID.
    fn probe_leader(&self) -> Result<()> {
        let raft = self.raft.clone();
        self.dispatch(None, async move { leader_barrier(&raft).await })
    }

    fn check_view(&self, view: &ReadView) -> Result<()> {
        if view.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(
                "view belongs to a different shard".into(),
            ));
        }
        let state = self.state.clone();
        let shard = self.shard;
        let epoch = view.epoch;
        self.dispatch(None, async move {
            let (current_shard, current, _) = state
                .d1_metadata()
                .await
                .ok_or_else(|| StorageNodeError::Unsupported("D1 is not activated".into()))?;
            if current_shard != shard {
                return Err(StorageNodeError::InvalidArgument(
                    "replica belongs to a different shard".into(),
                ));
            }
            if current != epoch {
                return Err(StorageNodeError::Fenced {
                    shard,
                    epoch,
                    current,
                });
            }
            Ok(())
        })
    }
}

// A queued task must not first poll a write after its absolute deadline.
async fn poll_before_deadline<T>(
    end: Instant,
    request_id: Option<RequestId>,
    future: impl Future<Output = Result<T>>,
) -> Result<T> {
    if Instant::now() >= end {
        return Err(StorageNodeError::Timeout { request_id });
    }
    tokio::time::timeout_at(tokio::time::Instant::from_std(end), future)
        .await
        .unwrap_or(Err(StorageNodeError::Timeout { request_id }))
}

async fn leader_barrier(raft: &openraft::Raft<TypeConfig>) -> Result<()> {
    raft.ensure_linearizable()
        .await
        .map_err(|e| {
            if let Some(hint) = e.forward_to_leader::<openraft::BasicNode>() {
                StorageNodeError::NotLeader {
                    leader: hint.leader_id,
                }
            } else if e.fatal().is_some() {
                StorageNodeError::Unavailable("Raft stopped".into())
            } else {
                StorageNodeError::Unavailable("Raft read quorum unavailable".into())
            }
        })
        .map(|_| ())
}

impl StorageNode for ReplicatedStorageNode {
    fn check_execution_context(&self) -> Result<()> {
        if Handle::try_current().is_ok() && !BLOCKING_SCOPE.with(Cell::get) {
            return Err(StorageNodeError::Unsupported(
                "synchronous Raft calls on a Tokio worker; use run_blocking".into(),
            ));
        }
        Ok(())
    }

    fn read_view(&self) -> Result<ReadView> {
        let raft = self.raft.clone();
        let state = self.state.clone();
        let shard = self.shard;
        self.dispatch(None, async move {
            leader_barrier(&raft).await?;
            let snapshot = state
                .d1_read_snapshot(MAX_VIEW_ROWS, MAX_VIEW_BYTES)
                .await
                .map_err(|_| {
                    StorageNodeError::ResourceExhausted("read view limit exceeded".into())
                })?
                .ok_or_else(|| StorageNodeError::Unsupported("D1 is not activated".into()))?;
            if snapshot.shard != shard {
                return Err(StorageNodeError::InvalidArgument(
                    "replica belongs to a different shard".into(),
                ));
            }
            ReadView::from_entries(
                snapshot.shard,
                snapshot.epoch,
                snapshot.sequence,
                snapshot.entries,
            )
        })
    }

    fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>> {
        self.check_view(view)?;
        Ok(view.get(key))
    }

    fn scan(&self, range: KeyRange, limit: ScanLimit, view: &ReadView) -> Result<Vec<KeyValue>> {
        self.check_view(view)?;
        view.scan(range, limit)
    }

    fn scan_page(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
        cursor: Option<&ScanCursor>,
    ) -> Result<ScanPage> {
        self.check_view(view)?;
        view.scan_page(range, limit, cursor)
    }

    fn batch_write(&self, request: WriteRequest) -> Result<WriteResult> {
        self.batch_write_conditional(request, None)
    }

    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        expected_sequence: Option<Sequence>,
    ) -> Result<WriteResult> {
        if request.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(
                "request belongs to a different shard".into(),
            ));
        }
        // Preflight bounds before cloning/queueing; state machine still validates.
        if request.operations.is_empty() {
            return Err(StorageNodeError::InvalidArgument("empty batch".into()));
        }
        let bytes = request.operations.iter().fold(0usize, |n, op| match op {
            WriteOp::Put { key, value } => n.saturating_add(key.len()).saturating_add(value.len()),
            WriteOp::Delete { key } => n.saturating_add(key.len()),
        });
        if request.operations.len() > d2_raft::MAX_BATCH_OPERATIONS
            || bytes > d2_raft::MAX_BATCH_BYTES
        {
            return Err(StorageNodeError::ResourceExhausted(
                "D1 batch limit exceeded".into(),
            ));
        }
        let id = request.request_id;
        let shard = request.shard;
        let epoch = request.epoch;
        let raft = self.raft.clone();
        let command = D1Command::Write(D1WriteRequest {
            shard,
            epoch,
            request_id: id,
            expected_sequence,
            operations: request
                .operations
                .into_iter()
                .map(|op| match op {
                    WriteOp::Put { key, value } => d2_raft::WriteOp::Put { key, value },
                    WriteOp::Delete { key } => d2_raft::WriteOp::Delete { key },
                })
                .collect(),
        });
        self.dispatch(Some(id), async move {
            let response = raft
                .client_write(LogCommand::D1V1(command))
                .await
                .map_err(|e| {
                    if let Some(hint) = e.forward_to_leader::<openraft::BasicNode>() {
                        StorageNodeError::NotLeader {
                            leader: hint.leader_id,
                        }
                    } else {
                        StorageNodeError::Unavailable(
                            "Raft write failed; retry original identity if outcome is unknown"
                                .into(),
                        )
                    }
                })?;
            let result = response
                .data
                .d1
                .ok_or_else(|| StorageNodeError::Corrupt("missing D1 response".into()))?
                .map_err(|e| application_error(e, shard, epoch, id))?;
            match result {
                D1Result::Write {
                    sequence,
                    status,
                    applied_operations,
                } => Ok(WriteResult {
                    sequence,
                    applied_operations,
                    status: match status {
                        D1WriteStatus::Committed => WriteStatus::Committed,
                        D1WriteStatus::Duplicate => WriteStatus::Duplicate,
                    },
                }),
                _ => Err(StorageNodeError::Corrupt("unexpected D1 response".into())),
            }
        })
    }

    fn admin(&self) -> &dyn StorageAdmin {
        self
    }
}

fn application_error(
    error: D1Error,
    shard: ShardId,
    epoch: Epoch,
    id: RequestId,
) -> StorageNodeError {
    match error {
        D1Error::RequestIndexHistory => {
            StorageNodeError::Corrupt("coordinator request index disagrees with history".into())
        }
        D1Error::RequestIndexUnavailable => {
            StorageNodeError::Unavailable("coordinator request index unavailable".into())
        }
        D1Error::Fenced { current, .. } => StorageNodeError::Fenced {
            shard,
            epoch,
            current,
        },
        D1Error::Conflict { expected, current } => StorageNodeError::Conflict { expected, current },
        D1Error::IdempotencyConflict => StorageNodeError::IdempotencyConflict(id),
        D1Error::DedupCapacity => StorageNodeError::ResourceExhausted(
            "D1 retained request budget exhausted; no eviction".into(),
        ),
        D1Error::BatchTooLarge | D1Error::SequenceOverflow => {
            StorageNodeError::ResourceExhausted("D1 write capacity exceeded".into())
        }
        D1Error::NotActivated => StorageNodeError::Unsupported("D1 is not activated".into()),
        D1Error::WrongShard { .. }
        | D1Error::EmptyBatch
        | D1Error::InvalidEpoch
        | D1Error::InvalidDedupCapacity
        | D1Error::CapacityConflict { .. } => {
            StorageNodeError::InvalidArgument("invalid D1 request".into())
        }
        D1Error::VisibilityNotEnabled
        | D1Error::VisibilityRequired
        | D1Error::VisibilityHistory
        | D1Error::VisibilityGap
        | D1Error::VisibilityUnresolved
        | D1Error::VisibilityNotApplied
        | D1Error::VisibilityReadLimit => {
            StorageNodeError::Unsupported("versioned participant boundary".into())
        }
        D1Error::ParticipantOnly
        | D1Error::ParticipantNotEnabled
        | D1Error::ParticipantAuthority
        | D1Error::ParticipantIdentity
        | D1Error::ParticipantBusy
        | D1Error::ParticipantCapacity
        | D1Error::ParticipantUnknown
        | D1Error::ParticipantDecisionConflict => {
            StorageNodeError::Unsupported("participant D1 role boundary".into())
        }
        D1Error::CoordinatorOnly
        | D1Error::CoordinatorNotEnabled
        | D1Error::CoordinatorRequiresEmptyShard
        | D1Error::CoordinatorRejected(_) => {
            StorageNodeError::Unsupported("coordinator D1 role boundary".into())
        }
        D1Error::ManagedWriteRequired
        | D1Error::ManagedWritesNotEnabled
        | D1Error::UnverifiedWriteHistory => {
            StorageNodeError::Unsupported("managed D1 write mode boundary".into())
        }
        D1Error::InvalidManagedMode | D1Error::ManagedModeConflict => {
            StorageNodeError::InvalidArgument("invalid managed D1 mode".into())
        }
        D1Error::LegacyDataPresent | D1Error::LegacyWriteDisabled => {
            StorageNodeError::Unsupported("legacy D1 mode boundary".into())
        }
    }
}

impl StorageAdmin for ReplicatedStorageNode {
    fn export(&self) -> Result<Export> {
        Err(StorageNodeError::Unsupported(
            "replicated export: use Raft snapshots, which retain dedup".into(),
        ))
    }
    fn import(&self, _: Export) -> Result<()> {
        Err(StorageNodeError::Unsupported("replicated import".into()))
    }
    fn set_epoch(&self, _: Epoch) -> Result<()> {
        Err(StorageNodeError::Unsupported(
            "replicated epoch management requires explicit D1V1 CAS".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn expired_queued_work_is_not_polled() {
        let mut polled = false;
        let future = async {
            polled = true;
            Ok(())
        };
        let result = poll_before_deadline(Instant::now(), Some([8; 16]), future).await;
        assert!(!polled);
        assert!(
            matches!(result, Err(StorageNodeError::Timeout { request_id: Some(id) }) if id == [8; 16])
        );
    }

    #[test]
    fn application_rejections_remain_typed() {
        assert!(matches!(
            application_error(D1Error::RequestIndexHistory, 7, 3, [1; 16]),
            StorageNodeError::Corrupt(_)
        ));
        assert!(matches!(
            application_error(D1Error::RequestIndexUnavailable, 7, 3, [1; 16]),
            StorageNodeError::Unavailable(_)
        ));
        assert!(matches!(
            application_error(
                D1Error::Conflict {
                    expected: 1,
                    current: 2
                },
                7,
                3,
                [1; 16]
            ),
            StorageNodeError::Conflict {
                expected: 1,
                current: 2
            }
        ));
        assert!(matches!(
            application_error(
                D1Error::Fenced {
                    requested: 3,
                    current: 4
                },
                7,
                3,
                [1; 16]
            ),
            StorageNodeError::Fenced {
                shard: 7,
                epoch: 3,
                current: 4
            }
        ));
        assert!(
            matches!(application_error(D1Error::IdempotencyConflict, 7, 3, [1; 16]), StorageNodeError::IdempotencyConflict(id) if id == [1; 16])
        );
        assert!(matches!(
            application_error(D1Error::DedupCapacity, 7, 3, [1; 16]),
            StorageNodeError::ResourceExhausted(_)
        ));
    }

    // A single-thread runtime makes accidental executor blocking observable.
    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_runtime_and_stopped_runtime_are_bounded() {
        let cluster = d2_raft::Cluster::new();
        let node = cluster.start(1).await.unwrap();
        node.raft
            .initialize(std::collections::BTreeMap::from([(
                1,
                openraft::BasicNode::default(),
            )]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
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
            .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 3 }))
            .await
            .unwrap();
        let adapter = ReplicatedStorageNode::new(&node, 7, Duration::from_secs(1)).unwrap();
        assert!(matches!(
            adapter.read_view(),
            Err(StorageNodeError::Unsupported(_))
        ));
        assert!(ReplicatedStorageNode::new(&node, 7, Duration::ZERO).is_err());
        let worker = adapter.clone();
        let view = run_blocking(move || worker.read_view())
            .await
            .unwrap()
            .unwrap();
        assert_eq!((view.shard, view.epoch, view.sequence), (7, 3, 0));
        assert!(matches!(
            adapter.admin().export(),
            Err(StorageNodeError::Unsupported(_))
        ));
        assert!(matches!(
            adapter.admin().set_epoch(4),
            Err(StorageNodeError::Unsupported(_))
        ));
        cluster.shutdown(&node).await;
        assert!(matches!(
            run_blocking(move || adapter.read_view()).await.unwrap(),
            Err(StorageNodeError::Unavailable(_))
        ));
    }

    #[tokio::test]
    async fn admission_and_timeout_preserve_identity() {
        let cluster = d2_raft::Cluster::new();
        let node = cluster.start(1).await.unwrap();
        let adapter = ReplicatedStorageNode::new(&node, 7, Duration::from_millis(20)).unwrap();
        let reserved = adapter
            .admission
            .clone()
            .acquire_many_owned(32)
            .await
            .unwrap();
        let worker = adapter.clone();
        assert!(matches!(
            run_blocking(move || worker.dispatch(None, async { Ok(()) }))
                .await
                .unwrap(),
            Err(StorageNodeError::ResourceExhausted(_))
        ));
        drop(reserved);
        let worker = adapter.clone();
        let error = run_blocking(move || {
            worker.dispatch(Some([9; 16]), async {
                std::future::pending::<()>().await;
                Ok(())
            })
        })
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            matches!(error, StorageNodeError::Timeout { request_id: Some(id) } if id == [9; 16])
        );
        cluster.shutdown(&node).await;
    }
}
