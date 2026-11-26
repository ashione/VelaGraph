//! A handle binds the original leader adapter, epoch and committed sequence.
//! There is no replicated state until the one conditional write at commit.
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

impl ReplicatedStorageNode {
    pub(crate) fn begin_transaction(
        self,
        options: TransactionOptions,
    ) -> std::result::Result<Box<dyn StorageTransaction>, TransactionError> {
        options.check()?;
        let shard = options.shard;
        let epoch = options.epoch;
        let id = options.request_id;
        if shard != self.shard {
            return Err(StorageNodeError::InvalidArgument("wrong transaction shard".into()).into());
        }
        let raft = self.raft.clone();
        let state = self.state.clone();
        let end = options.deadline.min(self.dispatch_deadline()?);
        let sequence = self.dispatch_until(None, end, async move {
            leader_barrier(&raft).await?;
            state
                .d1_unmanaged_write_sequence(shard, epoch)
                .await
                .map_err(|e| application_error(e, shard, epoch, id))
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
        #[cfg(test)]
        let gate = self.before_transaction_handoff.clone();
        let result = self.dispatch_until(Some(id), end, async move {
            if let Err(error) = options.check() {
                return Ok(Err(error));
            }
            #[cfg(test)]
            if let Some(gate) = gate {
                gate();
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn caller_deadline_before_handoff_discards_the_batch() -> anyhow::Result<()> {
        let cluster = d2_raft::Cluster::new();
        let node = cluster.start(1).await?;
        node.raft
            .initialize(std::collections::BTreeMap::from([(
                1,
                openraft::BasicNode::default(),
            )]))
            .await?;
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .state(openraft::ServerState::Leader, "single voter elected")
            .await?;
        node.raft
            .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 3 }))
            .await?
            .data
            .d1
            .expect("D1 application response")
            .expect("D1 activation accepted");

        let mut writer = ReplicatedStorageNode::new(&node, 7, Duration::from_secs(10))?;
        let reader = writer.clone();
        let entered = Arc::new(tokio::sync::Notify::new());
        let worker_entered = entered.clone();
        let (release, gate) = mpsc::channel();
        let gate = std::sync::Mutex::new(gate);
        writer.before_transaction_handoff = Some(Arc::new(move || {
            // Hold this poll across the caller timeout, even if dispatch aborts
            // the task. On release, the worker must observe the closed window.
            worker_entered.notify_one();
            gate.lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(10))
                .unwrap();
        }));
        let caller = tokio::spawn(run_blocking(move || -> anyhow::Result<()> {
            let mut tx = writer.begin_v1(TransactionOptions {
                shard: 7,
                epoch: 3,
                request_id: [78; 16],
                cancellation: Default::default(),
                deadline: Instant::now() + Duration::from_secs(2),
            })?;
            tx.stage(&WriteOp::put(b"first", [1]))?;
            tx.stage(&WriteOp::put(b"second", [2]))?;
            assert!(matches!(tx.commit(), Err(TransactionError::Deadline)));
            Ok(())
        }));
        tokio::time::timeout(Duration::from_secs(5), entered.notified()).await?;
        caller.await???;
        release.send(())?;
        // Dispatch drops its permit only after the in-flight worker exits.
        let drained =
            tokio::time::timeout(Duration::from_secs(5), reader.admission.acquire_many(32))
                .await??;
        drop(drained);
        run_blocking(move || -> anyhow::Result<()> {
            let view = reader.read_view()?;
            assert_eq!(view.sequence, 0);
            assert_eq!(reader.get(b"first", &view)?, None);
            assert_eq!(reader.get(b"second", &view)?, None);
            Ok(())
        })
        .await??;
        cluster.shutdown(&node).await;
        Ok(())
    }
}
