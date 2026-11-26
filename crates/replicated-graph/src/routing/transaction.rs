//! The catalog selects a shard for opaque writes. A successful begin pins
//! the adapter and holds one resolver operation slot until the handle is consumed.
use super::*;
use velagraph::{
    CancellationToken, StorageTransaction, TransactionError, TransactionOptions, WriteOp,
};

impl RouteResolver {
    /// Begin a single-shard byte transaction for the placement of `partition`.
    /// The timeout covers metadata lookup, leader selection, staging and commit.
    /// Only read-only admission can try another configured replica. Once a handle
    /// exists, commit uses its original adapter, epoch, sequence and request ID.
    pub fn begin_v1(
        &self,
        partition: u32,
        request_id: RequestId,
        timeout: Duration,
        cancellation: CancellationToken,
    ) -> Result<Box<dyn StorageTransaction>> {
        let op = self.start(timeout)?;
        let check = || {
            if cancellation.is_cancelled() {
                return Err(RouteError::from(TransactionError::Cancelled));
            }
            if Instant::now() >= op.end {
                return Err(RouteError::from(TransactionError::Deadline));
            }
            Ok(())
        };
        check()?;
        let placement = self.placement(partition, &op);
        check()?;
        let (placement, _) = placement?;
        let inner = select_read(&placement.replicas, &op, None, |id| {
            check()?;
            let options = TransactionOptions {
                shard: placement.shard,
                epoch: placement.epoch,
                request_id,
                deadline: op.end,
                cancellation: cancellation.clone(),
            };
            match self.adapter(&placement, id, &op) {
                Adapter::Local(node) => node.begin_transaction(options),
                Adapter::Remote(node) => node.begin_transaction(options),
                Adapter::Frozen(_) => {
                    Err(StorageNodeError::Unsupported("frozen transaction target".into()).into())
                }
            }
            .map_err(RouteError::from)
        });
        check()?;
        Ok(Box::new(RoutedTransaction {
            inner: inner?,
            _operation: op,
        }))
    }
}

struct RoutedTransaction {
    inner: Box<dyn StorageTransaction>,
    _operation: Arc<Operation>,
}

impl StorageTransaction for RoutedTransaction {
    fn stage(&mut self, operation: &WriteOp) -> std::result::Result<(), TransactionError> {
        self.inner.stage(operation)
    }

    fn commit(self: Box<Self>) -> std::result::Result<WriteResult, TransactionError> {
        self.inner.commit()
    }

    fn abort(self: Box<Self>) {
        self.inner.abort()
    }
}
