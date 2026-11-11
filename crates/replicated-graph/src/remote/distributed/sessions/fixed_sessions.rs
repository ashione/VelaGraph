//! Fixed sessions share the observed process-wide permits and fragment engine.
use super::*;
use crate::remote::distributed::fixed::{
    CaptureCost, FixedDescriptor, FixedFragmentOutput, FixedFragmentRequest, FixedOpenRequest,
    FixedOpened,
};
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
struct Ledger {
    next_sequence: u64,
    used: Cost,
}
pub(super) struct FixedSession {
    descriptor: FixedDescriptor,
    pub(super) node: query::FrozenNode,
    ledger: Mutex<Ledger>,
    failed: AtomicBool,
    _slot: tokio::sync::SemaphorePermit<'static>,
    _bytes: tokio::sync::SemaphorePermit<'static>,
}
impl Sessions {
    #[allow(clippy::too_many_arguments)]
    pub(in crate::remote) async fn open_fixed(
        &self,
        state: &StateMachine,
        identity: RemoteIdentity,
        shard: u64,
        request: FixedOpenRequest,
        end: Instant,
        lease_end: Instant,
    ) -> Result<FixedOpened, RemoteError> {
        let end = end.min(lease_end);
        check(end).map_err(|_| RemoteError::Timeout { request_id: None })?;
        request.validate(shard)?;
        self.expire();
        let slot = SESSION_SLOTS
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        // Reserve the entire permitted capture peak before constructing a view.
        // Previous nodes retain their actual payload, so gateway remaining limits
        // bound their retained payload plus this node's Put/Delete replay peak.
        let mut bytes = SESSION_BYTES
            .try_acquire_many(request.capture_limit.bytes as u32)
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let (snapshot, retained_proof) = state
            .certified_terminal_read(
                shard,
                request.target.epoch,
                request.target.mode,
                request.capture_limit.clone(),
                &request.proof,
            )
            .await
            .map_err(RemoteError::Application)?;
        check(end).map_err(|_| RemoteError::Timeout { request_id: None })?;
        if retained_proof != request.proof
            || snapshot.shard != shard
            || snapshot.epoch != request.target.epoch
            || snapshot.baseline_sequence != 0
            || snapshot.applied_prefix < request.proof.version()
            || snapshot.version != request.proof.version()
            || snapshot.authority.mode != request.proof.authority()
            || snapshot.authority.shard != request.proof.cohort().coordinator_shard
            || snapshot.cohort.as_ref() != Some(request.proof.cohort())
        {
            return Err(RemoteError::Protocol);
        }
        let capture_cost = CaptureCost {
            rows: snapshot.entries.len(),
            bytes: snapshot
                .entries
                .iter()
                .try_fold(0usize, |sum, (key, value)| {
                    sum.checked_add(key.len())?.checked_add(value.len())
                })
                .ok_or(RemoteError::ResourceExhausted)?,
            replay_operations: snapshot.replay_operations,
        };
        if capture_cost.rows > request.capture_limit.rows
            || capture_cost.bytes > request.capture_limit.bytes
            || capture_cost.replay_operations > request.capture_limit.operations
        {
            return Err(RemoteError::Protocol);
        }
        let descriptor = FixedDescriptor {
            stamp: QueryStamp {
                identity,
                shard,
                epoch: snapshot.epoch,
                sequence: snapshot.local_sequence,
            },
            incarnation: self.incarnation,
            token: token()?,
            authority: snapshot.authority,
            cohort: request.proof.cohort().clone(),
            version: request.proof.version(),
            mode: request.target.mode,
        };
        let view = ReadView::from_entries(
            shard,
            snapshot.epoch,
            snapshot.local_sequence,
            snapshot.entries,
        )
        .map_err(|_| RemoteError::ResourceExhausted)?;
        if let Some(unused) = bytes.split(request.capture_limit.bytes - capture_cost.bytes) {
            drop(unused);
        }
        check(end).map_err(|_| RemoteError::Timeout { request_id: None })?;
        let session = Arc::new(FixedSession {
            descriptor: descriptor.clone(),
            node: query::FrozenNode {
                view,
                end: lease_end.into_std(),
            },
            ledger: Mutex::new(Ledger::default()),
            failed: AtomicBool::new(false),
            _slot: slot,
            _bytes: bytes,
        });
        let mut values = self.fixed_values.lock().unwrap();
        if values.contains_key(&descriptor.token) {
            return Err(RemoteError::Unavailable);
        }
        values.insert(descriptor.token, session);
        Ok(FixedOpened {
            descriptor,
            retained_proof,
            capture_cost,
        })
    }

    fn lookup_fixed(
        &self,
        identity: RemoteIdentity,
        shard: u64,
        descriptor: &FixedDescriptor,
    ) -> Result<Arc<FixedSession>, RemoteError> {
        self.expire();
        if descriptor.stamp.identity != identity || descriptor.stamp.shard != shard {
            return Err(RemoteError::InvalidArgument);
        }
        if descriptor.incarnation != self.incarnation {
            return Err(RemoteError::SessionUnavailable);
        }
        let session = self
            .fixed_values
            .lock()
            .unwrap()
            .get(&descriptor.token)
            .cloned()
            .ok_or(RemoteError::SessionUnavailable)?;
        if &session.descriptor != descriptor {
            return Err(RemoteError::InvalidArgument);
        }
        if session.failed.load(Ordering::Acquire) {
            return Err(RemoteError::SessionUnavailable);
        }
        Ok(session)
    }
    pub(in crate::remote) fn close_fixed(
        &self,
        identity: RemoteIdentity,
        shard: u64,
        descriptor: &FixedDescriptor,
    ) -> Result<(), RemoteError> {
        // A poisoned session must still be closable. Exact descriptor matching
        // protects all other entries; close never changes a worker's ownership.
        if descriptor.stamp.identity != identity || descriptor.stamp.shard != shard {
            return Err(RemoteError::InvalidArgument);
        }
        if descriptor.incarnation != self.incarnation {
            return Err(RemoteError::SessionUnavailable);
        }
        let mut values = self.fixed_values.lock().unwrap();
        let session = values
            .get(&descriptor.token)
            .ok_or(RemoteError::SessionUnavailable)?;
        if &session.descriptor != descriptor {
            return Err(RemoteError::InvalidArgument);
        }
        session.failed.store(true, Ordering::Release);
        values.remove(&descriptor.token);
        Ok(())
    }
    pub(in crate::remote) async fn fragment_fixed(
        &self,
        state: &StateMachine,
        identity: RemoteIdentity,
        shard: u64,
        request: FixedFragmentRequest,
        end: Instant,
    ) -> Result<Reply, RemoteError> {
        request.fragment.validate()?;
        // The M1 grammar cannot mint scan, job, filter or projection work.
        if !matches!(
            request.fragment,
            Fragment::Exists(_)
                | Fragment::Out(_)
                | Fragment::In(_)
                | Fragment::ResolveIncoming { .. }
                | Fragment::VerifyReverse(_)
        ) || u64::from(request.fragment.partition()) != shard
            || request.remaining.bytes > CEILING.bytes
            || request.remaining.work > CEILING.work
        {
            return Err(RemoteError::InvalidArgument);
        }
        let session = self.lookup_fixed(identity, shard, &request.descriptor)?;
        validate_mode(state, &session.descriptor).await?;
        let end = end.min(Instant::from_std(session.node.end));
        let permit = FRAGMENT_SLOTS
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let expected = request.descriptor.clone();
        let result = tokio::task::spawn_blocking(move || {
            // Both permits and the full view stay alive after caller cancellation.
            let _permit = permit;
            let Ok(mut ledger) = session.ledger.try_lock() else {
                session.failed.store(true, Ordering::Release);
                return Err(QueryFailure::ResourceExhausted);
            };
            if session.failed.load(Ordering::Acquire)
                || request.request_sequence != ledger.next_sequence
            {
                session.failed.store(true, Ordering::Release);
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
            ledger.next_sequence = ledger
                .next_sequence
                .checked_add(1)
                .ok_or(QueryFailure::ResourceExhausted)?;
            let ceiling = Cost {
                bytes: request
                    .remaining
                    .bytes
                    .min(CEILING.bytes.saturating_sub(ledger.used.bytes)),
                work: request
                    .remaining
                    .work
                    .min(CEILING.work.saturating_sub(ledger.used.work)),
            };
            let node = MeteredNode {
                frozen: query::FrozenNode {
                    view: session.node.view.clone(),
                    end: end.into_std(),
                },
                cost: Arc::new(Mutex::new(Cost::default())),
                ceiling,
            };
            let run = || {
                check(end)?;
                node.charge(512, 1).map_err(query::storage_error)?;
                let value = run_fragment(&node, shard, request.fragment)?;
                let mut output = FixedFragmentOutput {
                    descriptor: session.descriptor.clone(),
                    request_sequence: request.request_sequence,
                    cost: CEILING,
                    value,
                };
                let bytes = bounded_encode(&output, end)?;
                node.charge(bytes.len().saturating_mul(2), 0)
                    .map_err(query::storage_error)?;
                output.cost = *node.cost.lock().unwrap();
                check(end)?;
                if session.failed.load(Ordering::Acquire) {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                }
                Ok(output)
            };
            let result = run();
            let cost = *node.cost.lock().unwrap();
            ledger.used.bytes = ledger.used.bytes.saturating_add(cost.bytes);
            ledger.used.work = ledger.used.work.saturating_add(cost.work);
            if result.is_err() {
                session.failed.store(true, Ordering::Release);
            }
            result
        })
        .await
        .map_err(|_| RemoteError::Unavailable)?;
        validate_mode(state, &expected).await?;
        check(end).map_err(|_| RemoteError::Timeout { request_id: None })?;
        Ok(Reply::FixedQueryFragment(result))
    }
}
async fn validate_mode(
    state: &StateMachine,
    descriptor: &FixedDescriptor,
) -> Result<(), RemoteError> {
    let mode = state
        .d1_managed_mode(descriptor.stamp.shard, descriptor.stamp.epoch)
        .await
        .map_err(|_| RemoteError::SessionUnavailable)?;
    if mode.is_none_or(|mode| mode.mode_id != descriptor.mode || mode.activated_sequence != 0) {
        return Err(RemoteError::SessionUnavailable);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2_raft::{
        participant_writes::Authority,
        transaction_decisions::{Cohort, Command, Decision, Journal, Member, Participant},
        versioned_reads::{ReadLimit, Target},
        WriteOp,
    };
    use openraft::{storage::RaftStateMachine, CommittedLeaderId, Entry, EntryPayload, LogId};

    struct Fixture {
        state: Arc<StateMachine>,
        journal: Journal,
        index: u64,
        sequence: u64,
    }
    fn authority() -> Authority {
        Authority {
            shard: 9,
            mode: [9; 16],
        }
    }
    fn cohort() -> Cohort {
        Cohort {
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: vec![Member {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
            }],
            profile: Default::default(),
        }
    }
    fn identity() -> RemoteIdentity {
        RemoteIdentity { group: 7, node: 1 }
    }
    impl Fixture {
        async fn apply(&mut self, command: D1Command) {
            self.index += 1;
            self.state
                .apply([Entry {
                    log_id: LogId::new(CommittedLeaderId::new(1, 1), self.index),
                    payload: EntryPayload::Normal(LogCommand::D1V1(command)),
                }])
                .await
                .unwrap()[0]
                .d1
                .as_ref()
                .unwrap()
                .as_ref()
                .unwrap();
        }
        async fn new() -> Self {
            let mut journal = Journal::new([9; 16]).unwrap();
            journal
                .apply(Command::BindCohort { cohort: cohort() })
                .unwrap();
            let mut f = Self {
                state: Arc::new(StateMachine::default()),
                journal,
                index: 0,
                sequence: 0,
            };
            f.apply(D1Command::Activate { shard: 7, epoch: 1 }).await;
            f.apply(D1Command::EnableManagedWrites {
                shard: 7,
                epoch: 1,
                expected_sequence: 0,
                mode_id: [7; 16],
            })
            .await;
            f.apply(D1Command::EnableParticipant {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                authority: authority(),
            })
            .await;
            f.apply(D1Command::EnableVersionedReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: f.journal.clone(),
            })
            .await;
            f
        }
        fn request(&self, version: u64) -> FixedOpenRequest {
            FixedOpenRequest {
                target: Target {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                },
                proof: self.journal.terminal_prefix(version).unwrap(),
                capture_limit: ReadLimit {
                    rows: 100,
                    bytes: 4096,
                    operations: 100,
                },
                ttl_millis: 10_000,
                acquire_timeout_millis: 5000,
            }
        }
        async fn commit(&mut self, operations: Vec<WriteOp>) {
            let id = [self.sequence as u8 + 1; 16];
            let participant = Participant {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
                intent: [1; 32],
            };
            self.journal
                .apply(Command::Begin {
                    id,
                    participants: vec![participant.clone()],
                })
                .unwrap();
            self.apply(D1Command::PrepareParticipant {
                authority: authority(),
                participant: participant.clone(),
                request: D1WriteRequest {
                    shard: 7,
                    epoch: 1,
                    request_id: id,
                    expected_sequence: Some(self.sequence),
                    operations,
                },
            })
            .await;
            self.journal
                .apply(Command::Prepared { id, participant })
                .unwrap();
            self.journal
                .apply(Command::Decide {
                    id,
                    decision: Decision::Commit,
                })
                .unwrap();
            self.sequence += 1;
            self.apply(D1Command::AdvanceVisibility {
                authority: authority(),
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                ordinal: self.sequence,
                journal: self.journal.clone(),
            })
            .await;
        }
        async fn open(&self, sessions: &Sessions, version: u64) -> FixedOpened {
            let end = Instant::now() + Duration::from_secs(5);
            sessions
                .open_fixed(&self.state, identity(), 7, self.request(version), end, end)
                .await
                .unwrap()
        }
    }
    fn fragment(descriptor: &FixedDescriptor, request_sequence: u64) -> FixedFragmentRequest {
        FixedFragmentRequest {
            descriptor: descriptor.clone(),
            request_sequence,
            fragment: Fragment::Exists(VertexId {
                partition: 7,
                id: 123,
            }),
            remaining: CEILING,
        }
    }

    #[tokio::test]
    async fn fixed_v0_and_old_view_are_certified_once_and_do_not_follow_later_writes() {
        let _fixture = TEST_SESSION_FIXTURES.lock().await;
        let mut f = Fixture::new().await;
        let sessions = Sessions::new().unwrap();
        let v0 = f.open(&sessions, 0).await;
        assert_eq!(v0.capture_cost.rows, 0);
        assert_eq!(v0.descriptor.stamp.sequence, 0);
        f.commit(vec![WriteOp::put(b"key", b"first")]).await;
        let v1 = f.open(&sessions, 1).await;
        let held = sessions
            .lookup_fixed(identity(), 7, &v1.descriptor)
            .unwrap();
        f.commit(vec![WriteOp::put(b"key", b"later")]).await;
        assert_eq!(held.node.view.get(b"key"), Some(b"first".to_vec()));
        assert_eq!(v1.retained_proof, f.journal.terminal_prefix(1).unwrap());
        let old = f.open(&sessions, 1).await;
        assert_eq!(old.capture_cost, v1.capture_cost);
        assert_eq!(old.descriptor.stamp.sequence, 1);
        assert_eq!(
            sessions
                .lookup_fixed(identity(), 7, &v0.descriptor)
                .unwrap()
                .node
                .view
                .get(b"key"),
            None
        );
        for mutate in 0..6 {
            let mut wrong = v1.descriptor.clone();
            match mutate {
                0 => wrong.version = 0,
                1 => wrong.authority.mode = [1; 16],
                2 => wrong.cohort.coordinator_epoch += 1,
                3 => wrong.mode = [1; 16],
                4 => wrong.stamp.sequence += 1,
                _ => {
                    wrong.cohort.profile =
                        d2_raft::transaction_decisions::CapacityProfile::ByteBound16MiBV1
                }
            }
            assert!(matches!(
                sessions.lookup_fixed(identity(), 7, &wrong),
                Err(RemoteError::InvalidArgument)
            ));
        }
        let mut wrong = f.request(1);
        let mut foreign = Journal::new([8; 16]).unwrap();
        foreign
            .apply(Command::BindCohort { cohort: cohort() })
            .unwrap();
        wrong.proof = foreign.terminal_prefix(0).unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        assert!(matches!(
            sessions
                .open_fixed(&f.state, identity(), 7, wrong, end, end)
                .await,
            Err(RemoteError::Application(D1Error::VisibilityHistory))
        ));
    }

    #[tokio::test]
    async fn put_then_delete_capture_peak_cannot_hide_behind_small_final_view() {
        let _fixture = TEST_SESSION_FIXTURES.lock().await;
        let mut f = Fixture::new().await;
        f.commit(vec![
            WriteOp::put(b"key", vec![1; 2048]),
            WriteOp::delete(b"key"),
        ])
        .await;
        let sessions = Sessions::new().unwrap();
        let slots = SESSION_SLOTS.available_permits();
        let bytes = SESSION_BYTES.available_permits();
        let mut request = f.request(1);
        request.capture_limit.bytes = 2048;
        let end = Instant::now() + Duration::from_secs(5);
        assert!(matches!(
            sessions
                .open_fixed(&f.state, identity(), 7, request, end, end)
                .await,
            Err(RemoteError::Application(D1Error::VisibilityReadLimit))
        ));
        assert!(sessions.fixed_values.lock().unwrap().is_empty());
        assert_eq!(SESSION_SLOTS.available_permits(), slots);
        assert_eq!(SESSION_BYTES.available_permits(), bytes);
        let opened = f.open(&sessions, 1).await;
        assert_eq!(opened.capture_cost.rows, 0);
        assert_eq!(opened.capture_cost.bytes, 0);
        assert_eq!(opened.capture_cost.replay_operations, 2);
        sessions
            .close_fixed(identity(), 7, &opened.descriptor)
            .unwrap();
        let mut exact = f.request(1);
        exact.capture_limit = ReadLimit {
            rows: 1,
            bytes: 2051,
            operations: 2,
        };
        let opened = sessions
            .open_fixed(&f.state, identity(), 7, exact.clone(), end, end)
            .await
            .unwrap();
        sessions
            .close_fixed(identity(), 7, &opened.descriptor)
            .unwrap();
        for below in 0..2 {
            let mut request = exact.clone();
            if below == 0 {
                request.capture_limit.bytes -= 1;
            } else {
                request.capture_limit.operations -= 1;
            }
            assert!(matches!(
                sessions
                    .open_fixed(&f.state, identity(), 7, request, end, end)
                    .await,
                Err(RemoteError::Application(D1Error::VisibilityReadLimit))
            ));
        }
    }

    #[tokio::test]
    async fn fixed_sequence_replay_failure_and_budget_do_not_reset_ledger() {
        let _fixture = TEST_SESSION_FIXTURES.lock().await;
        let f = Fixture::new().await;
        let sessions = Sessions::new().unwrap();
        let opened = f.open(&sessions, 0).await;
        let held = sessions
            .lookup_fixed(identity(), 7, &opened.descriptor)
            .unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        assert!(matches!(
            sessions
                .fragment_fixed(
                    &f.state,
                    identity(),
                    7,
                    fragment(&opened.descriptor, 0),
                    end
                )
                .await,
            Ok(Reply::FixedQueryFragment(Ok(_)))
        ));
        let used = held.ledger.lock().unwrap().used;
        assert!(used.work > 0 && used.bytes > 0);
        assert!(matches!(
            sessions
                .fragment_fixed(
                    &f.state,
                    identity(),
                    7,
                    fragment(&opened.descriptor, 0),
                    end
                )
                .await,
            Ok(Reply::FixedQueryFragment(Err(
                QueryFailure::IncompleteOrCorrupt
            )))
        ));
        assert_eq!(held.ledger.lock().unwrap().used.work, used.work);
        assert!(matches!(
            sessions.lookup_fixed(identity(), 7, &opened.descriptor),
            Err(RemoteError::SessionUnavailable)
        ));
        sessions
            .close_fixed(identity(), 7, &opened.descriptor)
            .unwrap();

        for sequence in [0, 1] {
            let opened = f.open(&sessions, 0).await;
            let held = sessions
                .lookup_fixed(identity(), 7, &opened.descriptor)
                .unwrap();
            let mut request = fragment(&opened.descriptor, sequence);
            request.remaining.work = 0;
            assert!(matches!(
                sessions
                    .fragment_fixed(&f.state, identity(), 7, request, end)
                    .await,
                Ok(Reply::FixedQueryFragment(Err(_)))
            ));
            assert!(held.failed.load(Ordering::Acquire));
            if sequence == 0 {
                assert!(held.ledger.lock().unwrap().used.work > 0);
            }
            sessions
                .close_fixed(identity(), 7, &opened.descriptor)
                .unwrap();
        }
    }

    #[tokio::test]
    async fn observed_and_fixed_sessions_exhaust_one_common_slot_and_byte_pool() {
        let _fixture = TEST_SESSION_FIXTURES.lock().await;
        let f = Fixture::new().await;
        let sessions = Sessions::new().unwrap();
        let before = SESSION_SLOTS.available_permits();
        let end = Instant::now() + Duration::from_secs(5);
        let observed = sessions.open(&f.state, identity(), 7, end).await.unwrap();
        let mut fixed = Vec::new();
        for _ in 1..before {
            fixed.push(f.open(&sessions, 0).await.descriptor);
        }
        assert_eq!(SESSION_SLOTS.available_permits(), 0);
        assert!(matches!(
            sessions
                .open_fixed(&f.state, identity(), 7, f.request(0), end, end)
                .await,
            Err(RemoteError::ResourceExhausted)
        ));
        assert!(matches!(
            sessions.open(&f.state, identity(), 7, end).await,
            Err(RemoteError::ResourceExhausted)
        ));
        for descriptor in fixed {
            sessions.close_fixed(identity(), 7, &descriptor).unwrap();
        }
        sessions.close(identity(), 7, &observed).unwrap();
        assert_eq!(SESSION_SLOTS.available_permits(), before);
        let free_bytes = SESSION_BYTES.available_permits();
        let reserved = SESSION_BYTES
            .try_acquire_many(free_bytes as u32 - 4095)
            .unwrap();
        assert!(matches!(
            sessions
                .open_fixed(&f.state, identity(), 7, f.request(0), end, end)
                .await,
            Err(RemoteError::ResourceExhausted)
        ));
        assert_eq!(SESSION_SLOTS.available_permits(), before);
        drop(reserved);
        assert_eq!(SESSION_BYTES.available_permits(), free_bytes);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shared_slots_and_bytes_stay_with_cancelled_worker_after_close() {
        let _fixture = TEST_SESSION_FIXTURES.lock().await;
        let mut f = Fixture::new().await;
        f.commit(vec![WriteOp::put(b"key", b"value")]).await;
        let sessions = Sessions::new().unwrap();
        let before_slots = SESSION_SLOTS.available_permits();
        let before_bytes = SESSION_BYTES.available_permits();
        let opened = f.open(&sessions, 1).await;
        let end = Instant::now() + Duration::from_secs(5);
        let observed = sessions.open(&f.state, identity(), 7, end).await.unwrap();
        assert_eq!(SESSION_SLOTS.available_permits(), before_slots - 2);
        assert_eq!(
            SESSION_BYTES.available_permits(),
            before_bytes - opened.capture_cost.bytes * 2
        );
        let held = sessions
            .lookup_fixed(identity(), 7, &opened.descriptor)
            .unwrap();
        let weak = Arc::downgrade(&held);
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _held = held;
            let _ = started_tx.send(());
            release_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            drop(_held);
            let _ = done_tx.send(());
        });
        started_rx.await.unwrap();
        worker.abort();
        sessions
            .close_fixed(identity(), 7, &opened.descriptor)
            .unwrap();
        sessions.close(identity(), 7, &observed).unwrap();
        assert_eq!(SESSION_SLOTS.available_permits(), before_slots - 1);
        assert_eq!(
            SESSION_BYTES.available_permits(),
            before_bytes - opened.capture_cost.bytes
        );
        assert!(weak.upgrade().is_some());
        release_tx.send(()).unwrap();
        done_rx.await.unwrap();
        assert!(weak.upgrade().is_none());
        assert_eq!(SESSION_SLOTS.available_permits(), before_slots);
        assert_eq!(SESSION_BYTES.available_permits(), before_bytes);
    }

    struct SmallGraph {
        writes: Vec<WriteOp>,
        source: Vec<u8>,
        target: Vec<u8>,
        canonical: Vec<u8>,
        outgoing: Vec<u8>,
        reverse: Vec<u8>,
        hop: EdgeHop,
    }
    fn small_graph() -> SmallGraph {
        // Generate record/index bytes using the existing production codec and
        // prepared batches, without inventing another encoding or writing a DB.
        fn store(entries: &BTreeMap<Vec<u8>, Vec<u8>>) -> AppendOnlyStore<query::FrozenNode> {
            AppendOnlyStore::new(
                query::FrozenNode {
                    view: ReadView::from_entries(7, 1, 0, entries.clone()).unwrap(),
                    end: std::time::Instant::now() + Duration::from_secs(5),
                },
                7,
                7,
            )
        }
        fn insert(
            entries: &mut BTreeMap<Vec<u8>, Vec<u8>>,
            batch: rocksdb_sql_table::experimental_graph::PreparedBatch,
        ) -> Vec<Vec<u8>> {
            let (request, _) = batch.into_conditional_request();
            request
                .operations
                .into_iter()
                .map(|operation| {
                    let rocksdb_sql_table::WriteOp::Put { key, value } = operation else {
                        panic!("creation fixture unexpectedly deletes a record");
                    };
                    assert!(entries.insert(key.clone(), value).is_none());
                    key
                })
                .collect()
        }
        let mut entries = BTreeMap::new();
        let batch = store(&entries).prepare_vertex(&Vertex::new(1)).unwrap();
        let source = insert(&mut entries, batch).pop().unwrap();
        let batch = store(&entries).prepare_vertex(&Vertex::new(2)).unwrap();
        let target = insert(&mut entries, batch).pop().unwrap();
        let edge = CrossEdge {
            id: 10,
            from: VertexRef {
                partition: 7,
                id: 1,
            },
            to: VertexRef {
                partition: 7,
                id: 2,
            },
            label: "knows".into(),
            properties: BTreeMap::new(),
        };
        let batch = store(&entries).prepare_source(&edge).unwrap();
        let keys = insert(&mut entries, batch);
        assert_eq!(keys.len(), 2);
        let batch = store(&entries).prepare_reverse(&edge).unwrap();
        let reverse = insert(&mut entries, batch).pop().unwrap();
        SmallGraph {
            writes: entries
                .into_iter()
                .map(|(key, value)| WriteOp::put(key, value))
                .collect(),
            source,
            target,
            canonical: keys[0].clone(),
            outgoing: keys[1].clone(),
            reverse,
            hop: EdgeHop {
                id: edge.id,
                from: VertexId {
                    partition: 7,
                    id: 1,
                },
                to: VertexId {
                    partition: 7,
                    id: 2,
                },
                label: edge.label,
            },
        }
    }
    fn incoming_fragment(graph: &SmallGraph) -> Fragment {
        Fragment::ResolveIncoming {
            target: graph.hop.to,
            edge: IncomingEdge {
                id: graph.hop.id,
                partition: 7,
                label: graph.hop.label.clone(),
            },
        }
    }
    async fn assert_fixed_corruption(graph: &SmallGraph, corruption: WriteOp, operation: Fragment) {
        let mut f = Fixture::new().await;
        // Fault injection is a trusted low-level, certified D1 fixture. It is
        // not a supported graph mutation, imported baseline or capacity proof.
        f.commit(graph.writes.clone()).await;
        let sessions = Sessions::new().unwrap();
        let original = f.open(&sessions, 1).await;
        let end = Instant::now() + Duration::from_secs(5);
        let request = |descriptor: &FixedDescriptor, sequence| FixedFragmentRequest {
            descriptor: descriptor.clone(),
            request_sequence: sequence,
            fragment: operation.clone(),
            remaining: CEILING,
        };
        assert!(matches!(
            sessions
                .fragment_fixed(
                    &f.state,
                    identity(),
                    7,
                    request(&original.descriptor, 0),
                    end
                )
                .await,
            Ok(Reply::FixedQueryFragment(Ok(_)))
        ));
        f.commit(vec![corruption]).await;
        let broken = f.open(&sessions, 2).await;
        let held = sessions
            .lookup_fixed(identity(), 7, &broken.descriptor)
            .unwrap();
        assert!(matches!(
            sessions
                .fragment_fixed(&f.state, identity(), 7, request(&broken.descriptor, 0), end)
                .await,
            Ok(Reply::FixedQueryFragment(Err(
                QueryFailure::IncompleteOrCorrupt
            )))
        ));
        let consumed = held.ledger.lock().unwrap().used;
        assert!(consumed.bytes > 0 && consumed.work > 0);
        assert!(held.failed.load(Ordering::Acquire));
        assert!(matches!(
            sessions
                .fragment_fixed(&f.state, identity(), 7, request(&broken.descriptor, 1), end)
                .await,
            Err(RemoteError::SessionUnavailable)
        ));
        assert_eq!(held.ledger.lock().unwrap().used.work, consumed.work);
        assert_eq!(held.ledger.lock().unwrap().used.bytes, consumed.bytes);
        // The earlier descriptor still reads its one original immutable view,
        // even though the latest certified V now contains a broken graph.
        assert!(matches!(
            sessions
                .fragment_fixed(
                    &f.state,
                    identity(),
                    7,
                    request(&original.descriptor, 1),
                    end
                )
                .await,
            Ok(Reply::FixedQueryFragment(Ok(_)))
        ));
        sessions
            .close_fixed(identity(), 7, &broken.descriptor)
            .unwrap();
        sessions
            .close_fixed(identity(), 7, &original.descriptor)
            .unwrap();
    }

    #[tokio::test]
    async fn fixed_canonical_and_outgoing_corruption_fail_closed_without_ledger_reuse() {
        let _fixture = TEST_SESSION_FIXTURES.lock().await;
        let graph = small_graph();
        for operation in [
            WriteOp::delete(&graph.canonical),
            WriteOp::put(&graph.canonical, [0]),
        ] {
            assert_fixed_corruption(&graph, operation, Fragment::Out(graph.hop.from)).await;
        }
        assert_fixed_corruption(
            &graph,
            WriteOp::delete(&graph.outgoing),
            incoming_fragment(&graph),
        )
        .await;
        assert_fixed_corruption(
            &graph,
            WriteOp::put(&graph.outgoing, [1]),
            Fragment::Out(graph.hop.from),
        )
        .await;
    }

    #[tokio::test]
    async fn fixed_reverse_corruption_fails_closed_without_ledger_reuse() {
        let _fixture = TEST_SESSION_FIXTURES.lock().await;
        let graph = small_graph();
        for operation in [
            WriteOp::delete(&graph.reverse),
            WriteOp::put(&graph.reverse, [1]),
        ] {
            assert_fixed_corruption(
                &graph,
                operation,
                Fragment::VerifyReverse(graph.hop.clone()),
            )
            .await;
        }
        assert_fixed_corruption(
            &graph,
            WriteOp::put(&graph.reverse, [1]),
            Fragment::In(graph.hop.to),
        )
        .await;
    }

    #[tokio::test]
    async fn fixed_endpoint_corruption_fails_closed_without_ledger_reuse() {
        let _fixture = TEST_SESSION_FIXTURES.lock().await;
        let graph = small_graph();
        for operation in [
            WriteOp::delete(&graph.source),
            WriteOp::put(&graph.source, [0]),
        ] {
            assert_fixed_corruption(&graph, operation, incoming_fragment(&graph)).await;
        }
        for operation in [
            WriteOp::delete(&graph.target),
            WriteOp::put(&graph.target, [0]),
        ] {
            assert_fixed_corruption(
                &graph,
                operation,
                Fragment::VerifyReverse(graph.hop.clone()),
            )
            .await;
        }
    }

    #[tokio::test]
    async fn fixed_expiry_wrong_partition_and_epoch_fail_without_reopen() {
        let _fixture = TEST_SESSION_FIXTURES.lock().await;
        let mut f = Fixture::new().await;
        let sessions = Sessions::new().unwrap();
        let opened = f.open(&sessions, 0).await;
        let end = Instant::now() + Duration::from_secs(5);
        let mut wrong = fragment(&opened.descriptor, 0);
        wrong.fragment = Fragment::Exists(VertexId {
            partition: 8,
            id: 1,
        });
        assert!(matches!(
            sessions
                .fragment_fixed(&f.state, identity(), 7, wrong, end)
                .await,
            Err(RemoteError::InvalidArgument)
        ));
        let expired = f.open(&sessions, 0).await;
        {
            let mut values = sessions.fixed_values.lock().unwrap();
            Arc::get_mut(values.get_mut(&expired.descriptor.token).unwrap())
                .unwrap()
                .node
                .end = std::time::Instant::now() - Duration::from_millis(1);
        }
        assert!(matches!(
            sessions.lookup_fixed(identity(), 7, &expired.descriptor),
            Err(RemoteError::SessionUnavailable)
        ));
        // A replacement process/state cannot validate an old descriptor.
        let restarted = Sessions::new().unwrap();
        assert!(matches!(
            restarted.lookup_fixed(identity(), 7, &opened.descriptor),
            Err(RemoteError::SessionUnavailable)
        ));
        f.state = Arc::new(StateMachine::default());
        f.apply(D1Command::Activate { shard: 7, epoch: 2 }).await;
        assert!(matches!(
            sessions
                .fragment_fixed(
                    &f.state,
                    identity(),
                    7,
                    fragment(&opened.descriptor, 0),
                    end
                )
                .await,
            Err(RemoteError::SessionUnavailable)
        ));
    }
}
