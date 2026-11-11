//! Ordinary durable member apply consumes compact observed history and exact raw
//! projections. Full incoming compatibility commands are explicit wire inputs,
//! never an excuse to materialize the retained local Journal.
use super::*;
use crate::transaction_decisions::{
    checkpoint::LookupKey, checkpoint::LookupResult, RequestRecord, Transaction,
};
use crate::versioned_reads::MemberHistory;

impl MemberHistory for event_ledger::MemberOverlay<'_, '_, '_> {
    fn transaction(&self, id: [u8; 16]) -> Result<Option<Transaction>, D1Error> {
        match self
            .lookup(LookupKey::Transaction(id))
            .map_err(ledger_read_error)?
        {
            LookupResult::Transaction(tx) => Ok(tx),
            _ => Err(D1Error::EventLedgerHistory),
        }
    }
    fn request_for_transaction(&self, id: [u8; 16]) -> Result<Option<RequestRecord>, D1Error> {
        event_ledger::MemberOverlay::request_for_transaction(self, id).map_err(ledger_read_error)
    }
    fn transaction_at(&self, ordinal: u64) -> Result<Option<Transaction>, D1Error> {
        event_ledger::MemberOverlay::transaction_at(self, ordinal)
            .map(Some)
            .map_err(ledger_read_error)
    }
}

/// One command-local point projection from the exact borrowed immutable overlay.
/// Its private fields can only be populated by the original complete lookup.
/// This never crosses an accepted suffix change or state publication.
pub(super) struct ProjectedTransaction<'a> {
    history: &'a dyn MemberHistory,
    transaction: Transaction,
}
impl<'a> ProjectedTransaction<'a> {
    pub(super) fn new(
        history: &'a event_ledger::MemberOverlay<'_, '_, '_>,
        ordinal: u64,
    ) -> Result<Self, D1Error> {
        let transaction = history.transaction_at(ordinal).map_err(ledger_read_error)?;
        Ok(Self {
            history,
            transaction,
        })
    }
}
impl MemberHistory for ProjectedTransaction<'_> {
    fn transaction(&self, id: [u8; 16]) -> Result<Option<Transaction>, D1Error> {
        self.history.transaction(id)
    }
    fn request_for_transaction(&self, id: [u8; 16]) -> Result<Option<RequestRecord>, D1Error> {
        self.history.request_for_transaction(id)
    }
    fn transaction_at(&self, ordinal: u64) -> Result<Option<Transaction>, D1Error> {
        if ordinal == self.transaction.ordinal {
            Ok(Some(self.transaction.clone()))
        } else {
            self.history.transaction_at(ordinal)
        }
    }
}

fn history_error(error: anyhow::Error) -> D1Error {
    if error.is::<rocksdb::Error>() || error.is::<std::io::Error>() {
        D1Error::EventLedgerUnavailable
    } else {
        D1Error::VisibilityHistory
    }
}
fn storage_failure(error: &D1Error) -> bool {
    matches!(
        error,
        D1Error::EventLedgerUnavailable
            | D1Error::EventLedgerHistory
            | D1Error::DecisionIndexUnavailable
            | D1Error::DecisionIndexHistory
            | D1Error::RequestIndexUnavailable
            | D1Error::RequestIndexHistory
    )
}
fn member_identity(
    d1: &D1State,
    authority: &crate::participant_writes::Authority,
    shard: u64,
    epoch: u64,
    mode_id: [u8; 16],
) -> Result<(), D1Error> {
    d1.check_shard(shard)?;
    d1.check_epoch(epoch)?;
    if d1.managed.as_ref().is_none_or(|m| m.mode_id != mode_id) {
        return Err(D1Error::ManagedModeConflict);
    }
    if d1
        .participant
        .as_ref()
        .is_none_or(|p| &p.authority != authority)
    {
        return Err(D1Error::ParticipantAuthority);
    }
    Ok(())
}

impl StateMachine {
    pub(super) fn verified_member<'db, 'cut>(
        &'db self,
        state: &'cut StateMachineData,
    ) -> Result<Option<event_ledger::VerifiedArchive<'db, 'cut>>, D1Error> {
        self.verify_member_archive(state, event_ledger::verify_active)
    }

    pub(super) fn verified_member_apply<'db, 'cut>(
        &'db self,
        state: &'cut StateMachineData,
    ) -> Result<Option<(event_ledger::VerifiedArchive<'db, 'cut>, Vec<Transaction>)>, D1Error> {
        self.verify_member_archive(state, event_ledger::verify_member_apply)
    }

    fn verify_member_archive<'db, 'cut, T>(
        &'db self,
        state: &'cut StateMachineData,
        verify: impl FnOnce(
            &'db rocksdb::DB,
            &'cut event_ledger::PublishedArchiveCut,
        ) -> Result<T, D1Error>,
    ) -> Result<Option<T>, D1Error> {
        let Some(visibility) = state.d1.as_ref().and_then(|d1| d1.visibility.as_ref()) else {
            return Ok(None);
        };
        let Some(checkpoint) = visibility.journal.active_arc() else {
            return Ok(None);
        };
        let storage = self.storage.as_ref().ok_or(D1Error::EventLedgerHistory)?;
        let cut = self
            .archive_cut(state)?
            .ok_or(D1Error::EventLedgerHistory)?;
        if !Arc::ptr_eq(checkpoint, cut.checkpoint_arc())
            || state.last_applied_log != cut.descriptor().applied
            || cut.descriptor().domain.role != event_ledger::Role::Member
        {
            return Err(D1Error::EventLedgerHistory);
        }
        #[cfg(test)]
        self.member_verifications.fetch_add(1, Ordering::Relaxed);
        verify(&storage.db, cut).map(Some)
    }

    pub(super) fn apply_member_entries<I>(
        &self,
        sm: &mut StateMachineData,
        entries: I,
    ) -> Result<Vec<ApplyResult>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>>,
    {
        let fail = |error: anyhow::Error| write_error(std::io::Error::other(error.to_string()));
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| fail(anyhow::anyhow!("active member lacks disk")))?;
        let (verified, verified_tail) = self
            .verified_member_apply(sm)
            .map_err(|e| fail(anyhow::anyhow!("member archive: {e:?}")))?
            .ok_or_else(|| fail(anyhow::anyhow!("active member lacks publication cut")))?;
        let old_a = sm.d1.as_ref().unwrap().visibility.as_ref().unwrap().applied;
        let domain = &verified.descriptor().domain;
        let identity = crate::decision_segments::CheckpointIdentity {
            role: crate::decision_segments::Role::Member,
            authority: domain.authority.clone(),
            shard: domain.shard,
            epoch: domain.epoch,
            mode: domain.mode,
            cohort: domain.cohort.clone(),
        };
        let tail_key =
            crate::decision_segments::checkpoint_tail_key(&identity, old_a).map_err(fail)?;
        let tail_bytes = tail_key
            .as_deref()
            .map(|key| verified.get(key))
            .transpose()
            .map_err(fail)?
            .flatten();
        let tail_records =
            if old_a.is_multiple_of(crate::terminal_prefix::SEGMENT_TRANSACTIONS as u64) {
                Vec::new()
            } else if old_a == verified.terminal_through() {
                verified_tail
            } else {
                // Invalid in-memory A must follow the original projection and
                // tail-check error path, after the original key/get operations.
                let first = old_a / crate::terminal_prefix::SEGMENT_TRANSACTIONS as u64
                    * crate::terminal_prefix::SEGMENT_TRANSACTIONS as u64
                    + 1;
                verified.transactions(first, old_a, &[]).map_err(fail)?
            };
        let checked_tail = crate::decision_segments::check_checkpoint_tail(
            &identity,
            old_a,
            sm.last_applied_log,
            verified.decision_manifest(),
            verified.decision_manifest(),
            tail_bytes.as_deref(),
            &tail_records,
        )
        .map_err(fail)?;
        let mut next = ApplyDelta::new(sm);
        let mut suffix = verified.member_overlay();
        let mut out = Vec::new();
        let mut batch = WriteBatch::default();
        for entry in entries {
            next.last_applied_log = Some(entry.log_id);
            let mut result = ApplyResult {
                value: None,
                d1: None,
            };
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Membership(membership) => {
                    next.last_membership = StoredMembership::new(Some(entry.log_id), membership)
                }
                EntryPayload::Normal(LogCommand::Noop) => {}
                EntryPayload::Normal(LogCommand::TxnBatch(_)) => {
                    result.d1 = Some(Err(D1Error::LegacyWriteDisabled))
                }
                EntryPayload::Normal(LogCommand::D1V1(command)) => {
                    let answer = self.apply_member_command(
                        sm,
                        &mut next,
                        &mut batch,
                        &verified,
                        &mut suffix,
                        command,
                    );
                    if let Err(error) = &answer {
                        if storage_failure(error) {
                            return Err(fail(anyhow::anyhow!("member evidence failed: {error:?}")));
                        }
                    }
                    result.d1 = Some(answer);
                }
            }
            out.push(result);
        }
        let d1 = next
            .d1
            .as_ref()
            .ok_or_else(|| fail(anyhow::anyhow!("member disappeared")))?;
        let visibility = d1
            .visibility
            .as_ref()
            .ok_or_else(|| fail(anyhow::anyhow!("member role changed")))?;
        let checkpoint = visibility
            .journal
            .active_arc()
            .ok_or_else(|| fail(anyhow::anyhow!("member lost active checkpoint")))?;
        suffix
            .validate_candidate(&verified, checkpoint)
            .map_err(fail)?;
        d1.validate_capacity_activation(next.last_applied_log)
            .map_err(fail)?;
        let staged = event_ledger::stage_active(
            &mut batch,
            &verified,
            checkpoint,
            suffix.commands(),
            next.last_applied_log,
        )
        .map_err(fail)?;
        let metadata = state_codec::active_member_metadata_bytes(
            d1,
            staged.descriptor_for(checkpoint).map_err(fail)?,
            checkpoint,
        )
        .map_err(fail)?;
        let terminals = staged
            .member_terminals(&verified, suffix.commands(), d1)
            .map_err(fail)?;
        let manifest = crate::decision_segments::stage_member_checkpoint(
            &mut batch,
            &checked_tail,
            &terminals,
            next.last_applied_log,
        )
        .map_err(fail)?;
        let cut = Arc::new(
            staged
                .published_member_cut(d1, checkpoint, &metadata, &manifest)
                .map_err(fail)?,
        );
        let manifest = manifest.into_bytes();
        batch.put(D1_KEY, &metadata);
        put_dedup(&mut batch, &d1.dedup)?;
        batch.put(
            APPLIED_KEY,
            serde_json::to_vec(&next.last_applied_log).map_err(write_error)?,
        );
        batch.put(
            MEMBERSHIP_KEY,
            serde_json::to_vec(&next.last_membership).map_err(write_error)?,
        );
        storage.write_sync(batch)?;
        next.archive_cut = Some(cut);
        *storage.metadata.lock().unwrap() = Some(metadata);
        *storage.decision_manifest.lock().unwrap() = Some(manifest);
        storage.legacy_d1.store(false, Ordering::Relaxed);
        storage.request_indexed.store(false, Ordering::Relaxed);
        storage.decision_indexed.store(true, Ordering::Relaxed);
        #[cfg(test)]
        if let Some(gate) = self.apply_publish_gate.lock().unwrap().take() {
            let _ = gate.entered.send(());
            gate.release
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("test must release member publication gate");
        }
        next.publish(sm);
        Ok(out)
    }

    fn apply_member_command<'operation, 'db, 'cut>(
        &self,
        base: &StateMachineData,
        next: &mut ApplyDelta,
        batch: &mut WriteBatch,
        archive: &'operation event_ledger::VerifiedArchive<'db, 'cut>,
        suffix: &mut event_ledger::MemberOverlay<'operation, 'db, 'cut>,
        command: D1Command,
    ) -> Result<D1Result, D1Error> {
        // Per-command copy-on-write staging: an invalid final page event or
        // local identity conflict cannot leak an accepted prefix into the batch.
        let mut candidate = next.d1.as_ref().ok_or(D1Error::NotActivated)?.clone();
        suffix
            .validate_candidate(
                archive,
                candidate
                    .visibility
                    .as_ref()
                    .ok_or(D1Error::VisibilityNotEnabled)?
                    .journal
                    .active_arc()
                    .ok_or(D1Error::VisibilityHistory)?,
            )
            .map_err(ledger_read_error)?;
        let mut accepted = suffix.clone();
        let (answer, request) = match command {
            D1Command::SyncHistory {
                authority,
                shard,
                epoch,
                mode_id,
                page,
            } => {
                member_identity(&candidate, &authority, shard, epoch, mode_id)?;
                accepted = accepted.merge_history_page(&page).map_err(history_error)?;
                candidate.visibility.as_mut().unwrap().journal =
                    Coordinator::Active(Arc::clone(accepted.checkpoint_arc()));
                candidate
                    .validate_member_history_changes(
                        &accepted,
                        &accepted.commands()[suffix.commands().len()..],
                    )
                    .map_err(|error| {
                        if storage_failure(&error) {
                            error
                        } else {
                            D1Error::VisibilityHistory
                        }
                    })?;
                (
                    D1Result::HistorySynced {
                        cursor: accepted.head().map_err(ledger_read_error)?,
                        applied_prefix: candidate.visibility.as_ref().unwrap().applied,
                    },
                    None,
                )
            }
            D1Command::AdvanceRetained {
                authority,
                shard,
                epoch,
                mode_id,
                ordinal,
                cut,
            } => {
                member_identity(&candidate, &authority, shard, epoch, mode_id)?;
                if !accepted
                    .prove_resolved(&cut, ordinal)
                    .map_err(history_error)?
                {
                    return Err(D1Error::VisibilityUnresolved);
                }
                // Keep the first projection before the common observation and
                // ordinal checks, preserving their existing error precedence.
                let projected = ProjectedTransaction::new(&accepted, ordinal)?;
                candidate.advance_member_transaction(
                    projected.transaction.clone(),
                    &projected,
                    base.d1.as_ref(),
                )?
            }
            D1Command::PrepareRetained(prepare) => {
                candidate.check_retained_prepare(&prepare)?;
                let target = accepted
                    .target_at_cut(&prepare.cut, prepare.request.request_id)
                    .map_err(history_error)?;
                let answer = candidate.prepare_retained_with_evidence(
                    &prepare,
                    target,
                    base.d1.as_ref(),
                    &accepted,
                )?;
                (answer, None)
            }
            D1Command::PrepareParticipant {
                authority,
                participant,
                request,
            } => {
                let answer = candidate.prepare_participant_with_evidence(
                    &authority,
                    participant,
                    request,
                    base.d1.as_ref(),
                    &accepted,
                )?;
                (answer, None)
            }
            D1Command::AdvanceVisibility {
                authority,
                shard,
                epoch,
                mode_id,
                ordinal,
                journal,
            } => {
                member_identity(&candidate, &authority, shard, epoch, mode_id)?;
                candidate.check_cohort(&journal)?;
                if ordinal == 0 || ordinal > journal.resolved_prefix() {
                    return Err(D1Error::VisibilityUnresolved);
                }
                let visibility = candidate.visibility.as_ref().unwrap();
                let old_a = visibility.applied;
                if ordinal > old_a
                    && (journal.events().len() as u64) < visibility.journal.event_count()
                {
                    return Err(D1Error::VisibilityHistory);
                }
                let extended = accepted
                    .merge_compatibility_journal(&journal)
                    .map_err(history_error)?;
                if ordinal <= old_a {
                    // The compatibility retry checks both histories but retains
                    // the previous observation, including when incoming is later.
                    return Ok(D1Result::Visibility {
                        applied_prefix: old_a,
                    });
                }
                accepted = extended;
                candidate.visibility.as_mut().unwrap().journal =
                    Coordinator::Active(Arc::clone(accepted.checkpoint_arc()));
                candidate
                    .validate_member_history_changes(
                        &accepted,
                        &accepted.commands()[suffix.commands().len()..],
                    )
                    .map_err(|error| {
                        if storage_failure(&error) {
                            error
                        } else {
                            D1Error::VisibilityHistory
                        }
                    })?;
                let tx = accepted
                    .transaction_at(ordinal)
                    .map_err(ledger_read_error)?;
                candidate.advance_member_transaction(tx, &accepted, base.d1.as_ref())?
            }
            other => return apply_d1(base, next, batch, other),
        };
        next.d1 = Some(candidate);
        *suffix = accepted;
        if let Some(request) = request {
            apply_operations(base, next, batch, request.operations);
        }
        Ok(answer)
    }
}

#[cfg(test)]
mod retained_prepare_atomicity {
    use super::*;
    use crate::participant_writes::{Authority, Status};
    use crate::retained_prepare::RetainedPrepare;
    use crate::transaction_decisions::{Cohort, Command, Journal, Member, Participant};

    fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
        Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(LogCommand::D1V1(command)),
        }
    }
    #[tokio::test]
    async fn failed_sync_cannot_publish_strict_prepare_or_observed_suffix() {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        let authority = Authority {
            shard: 9,
            mode: [9; 16],
        };
        let cohort = Cohort {
            coordinator_shard: 9,
            coordinator_epoch: 1,
            profile: Default::default(),
            members: vec![Member {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
            }],
        };
        let mut journal = Journal::new([9; 16]).unwrap();
        journal.apply(Command::BindCohort { cohort }).unwrap();
        let commands = vec![
            D1Command::Activate { shard: 7, epoch: 1 },
            D1Command::EnableManagedWrites {
                shard: 7,
                epoch: 1,
                expected_sequence: 0,
                mode_id: [7; 16],
            },
            D1Command::EnableParticipant {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                authority: authority.clone(),
            },
            D1Command::EnableVersionedReads {
                shard: 7,
                epoch: 1,
                mode_id: [7; 16],
                journal: journal.clone(),
            },
        ];
        for result in sm
            .apply(
                commands
                    .into_iter()
                    .enumerate()
                    .map(|(i, c)| entry(i as u64 + 1, c)),
            )
            .await
            .unwrap()
        {
            result.d1.unwrap().unwrap();
        }
        sm.forbid_coordinator_journal(true);
        let base = journal.history_cursor().unwrap();
        let participant = Participant {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
            intent: [3; 32],
        };
        journal
            .apply(Command::Begin {
                id: [2; 16],
                participants: vec![participant.clone()],
            })
            .unwrap();
        let cut = journal.history_cursor().unwrap();
        let prepare = RetainedPrepare {
            version: 1,
            authority: authority.clone(),
            participant,
            request: crate::D1WriteRequest {
                shard: 7,
                epoch: 1,
                request_id: [2; 16],
                expected_sequence: Some(0),
                operations: vec![WriteOp::put("x", "value")],
            },
            cut,
        };
        let commands = vec![
            entry(
                5,
                D1Command::SyncHistory {
                    authority,
                    shard: 7,
                    epoch: 1,
                    mode_id: [7; 16],
                    page: Box::new(journal.history_page(&base, None).unwrap()),
                },
            ),
            entry(6, D1Command::PrepareRetained(Box::new(prepare))),
        ];
        let disk = |sm: &StateMachine| {
            sm.storage
                .as_ref()
                .unwrap()
                .db
                .iterator(rocksdb::IteratorMode::Start)
                .map(|row| {
                    let (k, v) = row.unwrap();
                    (k.to_vec(), v.to_vec())
                })
                .collect::<Vec<_>>()
        };
        let before = disk(&sm);
        let before_state = sm.state.read().await.d1.clone();
        let before_marker = sm.applied().await;
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm.apply(commands.clone()).await.is_err());
        assert_eq!(disk(&sm), before);
        assert_eq!(sm.state.read().await.d1, before_state);
        assert_eq!(sm.applied().await, before_marker);
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        sm.forbid_coordinator_journal(true);
        assert_eq!(disk(&sm), before);
        assert_eq!(
            sm.participant_history_status(7, 1, [7; 16]).await.unwrap(),
            (base, 0)
        );
        let results = sm.apply(commands).await.unwrap();
        assert!(results[0].d1.as_ref().unwrap().is_ok());
        assert_eq!(
            results[1].d1,
            Some(Ok(D1Result::Participant(Status::Prepared)))
        );
        assert_eq!(sm.get_raw(b"x").await, None);
    }
}
