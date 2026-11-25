//! A handle binds the original leader adapter, epoch and committed sequence.
//! There is no replicated state until the one conditional write at commit.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

impl ReplicatedStorageNode {
    pub(super) fn begin_transaction(
        &self,
        options: TransactionOptions,
    ) -> std::result::Result<Box<dyn StorageTransaction + '_>, TransactionError> {
        options.check()?;
        let shard = options.shard;
        let epoch = options.epoch;
        if shard != self.shard {
            return Err(StorageNodeError::InvalidArgument("wrong transaction shard".into()).into());
        }
        let raft = self.raft.clone();
        let state = self.state.clone();
        let end = options.deadline.min(self.dispatch_deadline()?);
        let sequence = self.dispatch_until(None, end, async move {
            leader_barrier(&raft).await?;
            checked_sequence(&state, shard, epoch).await
        });
        options.check()?;
        let sequence = sequence?;
        Ok(options.stage_writes(move |request, options| {
            self.commit_transaction(request, sequence, options)
        }))
    }

    fn commit_transaction(
        &self,
        request: WriteRequest,
        sequence: Sequence,
        options: TransactionOptions,
    ) -> std::result::Result<WriteResult, TransactionError> {
        let id = request.request_id;
        let request = prepare_write(self.shard, request, Some(sequence))?;
        let deadline = options.deadline;
        let end = deadline.min(self.dispatch_deadline()?);
        let raft = self.raft.clone();
        // The caller and runtime can race at timeout. Only the winner can
        // either submit the write or definitively close the submission window.
        let closed = Arc::new(AtomicBool::new(false));
        let submission = closed.clone();
        let result = self.dispatch_until(Some(id), end, async move {
            if let Err(error) = options.check() {
                return Ok(Err(error));
            }
            if submission.swap(true, Ordering::AcqRel) {
                return Err(StorageNodeError::Timeout {
                    request_id: Some(id),
                });
            }
            let result = replicate_write(raft, request).await;
            Ok(result.map_err(|source| match source {
                StorageNodeError::Unavailable(_) | StorageNodeError::Corrupt(_) => {
                    TransactionError::UnknownOutcome {
                        request_id: id,
                        source,
                    }
                }
                other => TransactionError::Node(other),
            }))
        });
        match result {
            Ok(outcome) => outcome,
            Err(source) if closed.swap(true, Ordering::AcqRel) => {
                Err(TransactionError::UnknownOutcome {
                    request_id: id,
                    source,
                })
            }
            Err(StorageNodeError::Timeout { .. }) if Instant::now() >= deadline => {
                Err(TransactionError::Deadline)
            }
            Err(other) => Err(other.into()),
        }
    }
}
