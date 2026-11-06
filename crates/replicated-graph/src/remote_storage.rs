//! Synchronous graph adapter for the trusted-network plaintext TCP service.
//!
//! Construct on a live Tokio runtime and invoke graph work through `run_blocking`.
//! Snapshots are bounded full copies; all data reads use the pinned copy. Each
//! read checks the selected replica's applied epoch, not a fresh quorum fence.
//! Thus concurrent epoch changes have the same check/read race as the in-process
//! adapter. No leader discovery, automatic retry, or administrative RPC is used.
use std::cell::Cell;
use std::future::Future;
use std::net::SocketAddr;
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use d2_raft::{D1Result, D1WriteRequest, D1WriteStatus};
use rocksdb_sql_table::{
    Epoch, Export, KeyRange, KeyValue, ReadView, RequestId, ScanCursor, ScanLimit, ScanPage,
    Sequence, ShardId, StorageAdmin, StorageNode, StorageNodeError, WriteOp, WriteRequest,
    WriteResult, WriteStatus, MAX_VIEW_BYTES, MAX_VIEW_ROWS,
};
use tokio::runtime::Handle;
use tokio::sync::Semaphore;

use crate::remote::{self, RemoteError};
use crate::{application_error, poll_before_deadline, Result, BLOCKING_SCOPE};

/// One explicitly selected remote replica and shard. Clones share a maximum of
/// 32 outstanding operations. A timeout or transport loss on a write retains its
/// original request ID in `Timeout`; it does not prove that the write aborted.
#[derive(Clone)]
pub struct RemoteStorageNode {
    endpoint: SocketAddr,
    shard: ShardId,
    deadline: Duration,
    runtime: Handle,
    admission: Arc<Semaphore>,
}

impl RemoteStorageNode {
    /// Capture the runtime that dispatches TCP I/O. The endpoint must serve the
    /// requested shard; construction performs no I/O and never activates D1.
    pub fn new(endpoint: SocketAddr, shard: ShardId, deadline: Duration) -> Result<Self> {
        if deadline.is_zero()
            || deadline > remote::MAX_DEADLINE
            || Instant::now().checked_add(deadline).is_none()
        {
            return Err(StorageNodeError::InvalidArgument("invalid deadline".into()));
        }
        let runtime = Handle::try_current().map_err(|_| {
            StorageNodeError::InvalidArgument("construct remote adapter on a runtime".into())
        })?;
        Ok(Self {
            endpoint,
            shard,
            deadline,
            runtime,
            admission: Arc::new(Semaphore::new(32)),
        })
    }

    fn dispatch<T, F>(&self, request_id: Option<RequestId>, future: F) -> Result<T>
    where
        T: Send + 'static,
        F: Future<Output = Result<T>> + Send + 'static,
    {
        self.check_execution_context()?;
        let end = Instant::now()
            .checked_add(self.deadline)
            .ok_or_else(|| StorageNodeError::InvalidArgument("deadline overflow".into()))?;
        let permit = self.admission.clone().try_acquire_owned().map_err(|_| {
            StorageNodeError::ResourceExhausted(
                "32 remote adapter operations already in flight".into(),
            )
        })?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let task = self.runtime.spawn(async move {
            let _permit = permit;
            let result = poll_before_deadline(end, request_id, future).await;
            let _ = sender.send(result);
        });
        match receiver.recv_timeout(end.saturating_duration_since(Instant::now())) {
            Ok(result) if Instant::now() < end => result,
            Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {
                task.abort();
                Err(StorageNodeError::Timeout { request_id })
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => match request_id {
                Some(_) => Err(StorageNodeError::Timeout { request_id }),
                None => Err(StorageNodeError::Unavailable(
                    "remote adapter runtime stopped".into(),
                )),
            },
        }
    }

    fn check_view(&self, view: &ReadView) -> Result<()> {
        if view.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(
                "view belongs to a different shard".into(),
            ));
        }
        let (endpoint, shard, deadline, epoch) =
            (self.endpoint, self.shard, self.deadline, view.epoch);
        self.dispatch(None, async move {
            let (current_shard, current, _) = remote::metadata(endpoint, shard, deadline)
                .await
                .map_err(|e| map_error(e, shard, epoch, None))?;
            if current_shard != shard {
                return Err(StorageNodeError::Corrupt(
                    "remote metadata shard mismatch".into(),
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

fn map_error(
    error: RemoteError,
    shard: ShardId,
    epoch: Epoch,
    id: Option<RequestId>,
) -> StorageNodeError {
    match error {
        RemoteError::NotLeader { leader } => StorageNodeError::NotLeader { leader },
        RemoteError::Application(error) => {
            application_error(error, shard, epoch, id.unwrap_or([0; 16]))
        }
        RemoteError::InvalidArgument => {
            StorageNodeError::InvalidArgument("invalid remote request".into())
        }
        RemoteError::ResourceExhausted => {
            StorageNodeError::ResourceExhausted("remote service limit exceeded".into())
        }
        RemoteError::Timeout { .. } => StorageNodeError::Timeout { request_id: id },
        RemoteError::Unavailable | RemoteError::Protocol if id.is_some() => {
            StorageNodeError::Timeout { request_id: id }
        }
        RemoteError::Unavailable => {
            StorageNodeError::Unavailable("remote service unavailable".into())
        }
        RemoteError::Protocol => {
            StorageNodeError::Corrupt("invalid remote protocol response".into())
        }
    }
}

impl StorageNode for RemoteStorageNode {
    fn check_execution_context(&self) -> Result<()> {
        if Handle::try_current().is_ok() && !BLOCKING_SCOPE.with(Cell::get) {
            return Err(StorageNodeError::Unsupported(
                "synchronous remote calls on a Tokio worker; use run_blocking".into(),
            ));
        }
        Ok(())
    }

    fn read_view(&self) -> Result<ReadView> {
        let (endpoint, shard, deadline) = (self.endpoint, self.shard, self.deadline);
        self.dispatch(None, async move {
            let snapshot = remote::snapshot(endpoint, shard, deadline)
                .await
                .map_err(|e| map_error(e, shard, 0, None))?;
            if snapshot.shard != shard {
                return Err(StorageNodeError::Corrupt(
                    "remote snapshot shard mismatch".into(),
                ));
            }
            let bytes = snapshot.entries.iter().fold(0usize, |n, (key, value)| {
                n.saturating_add(key.len()).saturating_add(value.len())
            });
            if snapshot.entries.len() > MAX_VIEW_ROWS || bytes > MAX_VIEW_BYTES {
                return Err(StorageNodeError::ResourceExhausted(
                    "remote snapshot limit exceeded".into(),
                ));
            }
            if snapshot
                .entries
                .windows(2)
                .any(|pair| pair[0].0 >= pair[1].0)
            {
                return Err(StorageNodeError::Corrupt(
                    "remote snapshot keys not strictly ordered".into(),
                ));
            }
            ReadView::from_entries(
                snapshot.shard,
                snapshot.epoch,
                snapshot.sequence,
                snapshot.entries.into_iter().collect(),
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
        self.check_execution_context()?;
        if request.shard != self.shard {
            return Err(StorageNodeError::InvalidArgument(
                "request belongs to a different shard".into(),
            ));
        }
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
        let (endpoint, shard, deadline, epoch, id) = (
            self.endpoint,
            self.shard,
            self.deadline,
            request.epoch,
            request.request_id,
        );
        let request = D1WriteRequest {
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
        };
        self.dispatch(Some(id), async move {
            match remote::write(endpoint, request, deadline)
                .await
                .map_err(|e| map_error(e, shard, epoch, Some(id)))?
            {
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
                _ => Err(StorageNodeError::Timeout {
                    request_id: Some(id),
                }),
            }
        })
    }

    fn admin(&self) -> &dyn StorageAdmin {
        self
    }
}

impl StorageAdmin for RemoteStorageNode {
    fn export(&self) -> Result<Export> {
        Err(StorageNodeError::Unsupported("remote export".into()))
    }
    fn import(&self, _: Export) -> Result<()> {
        Err(StorageNodeError::Unsupported("remote import".into()))
    }
    fn set_epoch(&self, _: Epoch) -> Result<()> {
        Err(StorageNodeError::Unsupported(
            "remote epoch management requires explicit D1V1 CAS".into(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run_blocking;

    fn endpoint() -> SocketAddr {
        "127.0.0.1:1".parse().unwrap()
    }

    #[test]
    fn constructor_requires_runtime() {
        assert!(matches!(
            RemoteStorageNode::new(endpoint(), 7, Duration::from_secs(1)),
            Err(StorageNodeError::InvalidArgument(_))
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn runtime_guard_limits_and_timeout_identity() {
        let adapter = RemoteStorageNode::new(endpoint(), 7, Duration::from_millis(20)).unwrap();
        assert!(RemoteStorageNode::new(endpoint(), 7, Duration::ZERO).is_err());
        assert!(RemoteStorageNode::new(endpoint(), 7, Duration::from_secs(61)).is_err());
        assert!(matches!(
            adapter.read_view(),
            Err(StorageNodeError::Unsupported(_))
        ));
        let held = adapter
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
        drop(held);
        let worker = adapter.clone();
        let result = run_blocking(move || {
            worker.dispatch(Some([9; 16]), std::future::pending::<Result<()>>())
        })
        .await
        .unwrap();
        assert!(
            matches!(result, Err(StorageNodeError::Timeout { request_id: Some(id) }) if id == [9;16])
        );
        assert!(matches!(
            adapter.admin().export(),
            Err(StorageNodeError::Unsupported(_))
        ));
        assert!(matches!(
            adapter.admin().set_epoch(4),
            Err(StorageNodeError::Unsupported(_))
        ));
    }

    #[test]
    fn application_errors_are_typed_and_unknown_writes_keep_identity() {
        let id = [4; 16];
        assert!(
            matches!(map_error(RemoteError::Application(d2_raft::D1Error::IdempotencyConflict),7,3,Some(id)), StorageNodeError::IdempotencyConflict(actual) if actual == id)
        );
        assert!(matches!(
            map_error(
                RemoteError::Application(d2_raft::D1Error::Conflict {
                    expected: 1,
                    current: 2
                }),
                7,
                3,
                Some(id)
            ),
            StorageNodeError::Conflict {
                expected: 1,
                current: 2
            }
        ));
        assert!(matches!(
            map_error(
                RemoteError::Application(d2_raft::D1Error::Fenced {
                    requested: 3,
                    current: 4
                }),
                7,
                3,
                Some(id)
            ),
            StorageNodeError::Fenced {
                shard: 7,
                epoch: 3,
                current: 4
            }
        ));
        for error in [
            RemoteError::Unavailable,
            RemoteError::Protocol,
            RemoteError::Timeout { request_id: None },
        ] {
            assert!(
                matches!(map_error(error,7,3,Some(id)), StorageNodeError::Timeout { request_id: Some(actual) } if actual == id)
            );
        }
        assert!(matches!(
            map_error(RemoteError::NotLeader { leader: Some(2) }, 7, 3, Some(id)),
            StorageNodeError::NotLeader { leader: Some(2) }
        ));
    }

    #[test]
    fn stopped_runtime_does_not_hang_or_lose_identity() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let adapter = {
            let _entered = runtime.enter();
            RemoteStorageNode::new(endpoint(), 7, Duration::from_millis(50)).unwrap()
        };
        drop(runtime);
        assert!(
            matches!(adapter.dispatch(Some([8;16]), async { Ok(()) }), Err(StorageNodeError::Timeout { request_id: Some(id) }) if id == [8;16])
        );
        assert!(matches!(
            adapter.dispatch(None, async { Ok(()) }),
            Err(StorageNodeError::Unavailable(_))
        ));
    }
}
