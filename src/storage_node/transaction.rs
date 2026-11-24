//! In-process write staging. Only commit enters the durable write path.
use super::*;
use crate::CancellationToken;
use std::time::Instant;

/// Caller-selected shard, fencing epoch, write identity, and cooperative controls.
/// The batch limits apply across the entire lifetime of the transaction.
pub struct TransactionOptions {
    pub shard: ShardId,
    pub epoch: Epoch,
    pub request_id: RequestId,
    pub deadline: Instant,
    pub cancellation: CancellationToken,
}

impl TransactionOptions {
    fn check(&self) -> std::result::Result<(), TransactionError> {
        if self.cancellation.is_cancelled() {
            return Err(TransactionError::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(TransactionError::Deadline);
        }
        Ok(())
    }
}

/// A rejected admission is definitive. A failure after native write submission
/// carries the request identity because publication may have happened.
#[derive(Debug, Error)]
pub enum TransactionError {
    #[error(transparent)]
    Node(#[from] StorageNodeError),
    #[error("transaction cancelled")]
    Cancelled,
    #[error("transaction deadline exceeded")]
    Deadline,
    #[error("transaction already aborted by a stage failure")]
    Aborted,
    #[error("unknown commit outcome for {request_id:?}: {source}")]
    UnknownOutcome {
        request_id: RequestId,
        source: StorageNodeError,
    },
}

/// Opaque byte writes, ordered by successful stage calls. Dropping or aborting
/// discards all staged operations. Any stage error invalidates the handle.
pub trait StorageTransaction {
    fn stage(&mut self, operation: &WriteOp) -> std::result::Result<(), TransactionError>;
    fn commit(self: Box<Self>) -> std::result::Result<WriteResult, TransactionError>;
    fn abort(self: Box<Self>);
}

struct LocalTransaction<'a> {
    node: &'a LocalStorageNode,
    options: TransactionOptions,
    sequence: Sequence,
    staged: Option<Vec<WriteOp>>,
    bytes: usize,
}

pub(super) fn begin(
    node: &LocalStorageNode,
    options: TransactionOptions,
) -> std::result::Result<Box<dyn StorageTransaction + '_>, TransactionError> {
    options.check()?;
    if options.shard != node.shard {
        return Err(StorageNodeError::InvalidArgument("wrong transaction shard".into()).into());
    }
    let _guard = node.lock()?;
    let current = node.epoch()?;
    if options.epoch != current {
        return Err(StorageNodeError::Fenced {
            shard: node.shard,
            epoch: options.epoch,
            current,
        }
        .into());
    }
    let sequence = node.sequence()?;
    options.check()?;
    Ok(Box::new(LocalTransaction {
        node,
        options,
        sequence,
        staged: Some(Vec::new()),
        bytes: 0,
    }))
}

impl StorageTransaction for LocalTransaction<'_> {
    fn stage(&mut self, operation: &WriteOp) -> std::result::Result<(), TransactionError> {
        // Take the buffer first. Every rejection discards the accepted prefix.
        let mut staged = self.staged.take().ok_or(TransactionError::Aborted)?;
        self.options.check()?;
        self.bytes = self.bytes.saturating_add(operation.payload_bytes());
        if staged.len() >= MAX_BATCH_OPERATIONS || self.bytes > MAX_BATCH_BYTES {
            return Err(
                StorageNodeError::ResourceExhausted("transaction batch limit".into()).into(),
            );
        }
        staged.push(operation.clone());
        self.staged = Some(staged);
        Ok(())
    }

    fn commit(self: Box<Self>) -> std::result::Result<WriteResult, TransactionError> {
        let operations = self.staged.ok_or(TransactionError::Aborted)?;
        self.options.check()?;
        let request = WriteRequest {
            shard: self.options.shard,
            epoch: self.options.epoch,
            request_id: self.options.request_id,
            operations,
        };
        let canonical = self.node.validate_write(&request)?;
        let guard = self.node.lock()?;
        self.options.check()?;
        let (batch, result) =
            self.node
                .prepare_write(&guard, &request, Some(self.sequence), canonical)?;
        self.options.check()?;
        if let Some(batch) = batch {
            self.node
                .write_batch(batch)
                .map_err(|source| TransactionError::UnknownOutcome {
                    request_id: request.request_id,
                    source,
                })?;
        }
        Ok(result)
    }

    fn abort(self: Box<Self>) {}
}
