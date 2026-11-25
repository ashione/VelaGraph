//! Staging is private to the calling process. Only commit sends a write RPC.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

impl RemoteStorageNode {
    pub(super) fn begin_transaction(
        &self,
        options: TransactionOptions,
    ) -> std::result::Result<Box<dyn StorageTransaction + '_>, TransactionError> {
        options.check()?;
        let (shard, epoch) = (options.shard, options.epoch);
        if shard != self.shard {
            return Err(StorageNodeError::InvalidArgument("wrong transaction shard".into()).into());
        }
        let end = self.operation_deadline().min(options.deadline);
        let client = self.client(end);
        let sequence = self.dispatch_until(None, end, async move {
            let mut client = client.await.map_err(|e| map_error(e, shard, epoch, None))?;
            client
                .begin_transaction(shard, epoch)
                .await
                .map_err(|e| map_error(e, shard, epoch, None))?
                .ok_or_else(|| StorageNodeError::Unsupported("remote transaction admission".into()))
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
        let request = crate::prepare_write(self.shard, request, Some(sequence))?;
        let (shard, epoch, id) = (request.shard, request.epoch, request.request_id);
        let end = self.operation_deadline().min(options.deadline);
        let operations = request.operations.len();
        let client = self.client(end);
        let controls = Arc::new(options);
        let worker = controls.clone();
        let closed = Arc::new(AtomicBool::new(false));
        let submission = closed.clone();
        let result = self.dispatch_until(Some(id), end, async move {
            let mut client = client.await.map_err(|e| map_error(e, shard, epoch, None))?;
            // Claim immediately before tonic sends, after connection, admission
            // and bounded encoding. Once closed by the caller, no late RPC can start.
            let reply = client
                .transaction_write(request, move || {
                    if worker.check().is_err()
                        || Instant::now() >= end
                        || submission.swap(true, Ordering::AcqRel)
                    {
                        return Err(RemoteError::Timeout {
                            request_id: Some(id),
                        });
                    }
                    Ok(())
                })
                .await
                .map_err(|e| map_error(e, shard, epoch, Some(id)))?;
            match reply {
                Ok(result) => {
                    let result = write_result(result, id)?;
                    if Some(result.sequence) != sequence.checked_add(1)
                        || result.applied_operations != operations
                    {
                        return Err(StorageNodeError::Corrupt(
                            "unexpected remote transaction result".into(),
                        ));
                    }
                    Ok(Ok(result))
                }
                Err(error) => {
                    let source = map_error(error, shard, epoch, Some(id));
                    match source {
                        StorageNodeError::Timeout { .. }
                        | StorageNodeError::Unavailable(_)
                        | StorageNodeError::Corrupt(_) => Err(source),
                        other => Ok(Err(TransactionError::Node(other))),
                    }
                }
            }
        });
        match result {
            Ok(outcome) => outcome,
            Err(source) if closed.swap(true, Ordering::AcqRel) => {
                Err(TransactionError::UnknownOutcome {
                    request_id: id,
                    source,
                })
            }
            Err(source) => {
                controls.check()?;
                Err(source.into())
            }
        }
    }
}
