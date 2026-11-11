//! Narrow AdvanceRetained projection reuse checks. The legacy two-projection
//! path below is an independent test oracle, not a production switch.
use super::*;
use crate::d1::D1State;
use crate::state_machine::member_apply::ProjectedTransaction;
use crate::transaction_decisions::Transaction;
use crate::versioned_reads::MemberHistory;

type AdvanceResult = Result<(D1State, D1Result, Option<crate::D1WriteRequest>), D1Error>;

fn projected_advance(
    before: &D1State,
    overlay: &event_ledger::MemberOverlay<'_, '_, '_>,
    ordinal: u64,
    reuse: bool,
) -> AdvanceResult {
    let mut candidate = before.clone();
    // Both alternatives start after the unchanged frozen-cut proof. Keep the
    // first projection before the common observation and ordinal checks.
    let result = if reuse {
        let projected = ProjectedTransaction::new(overlay, ordinal)?;
        let tx = projected.transaction_at(ordinal)?.unwrap();
        candidate.advance_member_transaction(tx, &projected, Some(before))?
    } else {
        let tx = overlay.transaction_at(ordinal).map_err(ledger_read_error)?;
        candidate.advance_member_transaction(tx, overlay, Some(before))?
    };
    Ok((candidate, result.0, result.1))
}

async fn confirmed_journal(sm: &mut Arc<StateMachine>, profile: CapacityProfile) -> Journal {
    let mut journal = empty(profile);
    if !profile.is_legacy() {
        let own = sm
            .participant_capacity_grant(7, 1, [7; 16], &authority(), &cohort(profile))
            .await
            .unwrap();
        journal
            .apply(Command::ConfirmCohortCapacity {
                cohort: cohort(profile),
                grants: vec![
                    own,
                    MemberCapacityGrant {
                        member: cohort(profile).members[1].clone(),
                        activation: CapacityActivation {
                            nonce: [18; 32],
                            log_id: LogId::new(CommittedLeaderId::new(1, 1), 4),
                        },
                    },
                ],
            })
            .unwrap();
        sync_all(sm, &journal).await;
    }
    journal
}

fn legacy_advance(ordinal: u64, journal: &Journal) -> D1Command {
    D1Command::AdvanceVisibility {
        authority: authority(),
        shard: 7,
        epoch: 1,
        mode_id: [7; 16],
        ordinal,
        journal: journal.clone(),
    }
}

#[tokio::test]
async fn retained_projection_reuses_only_same_ordinal() {
    let (_dir, sm, cut) = status_fixture(64).await;
    let state = sm.state.read().await;
    let verified = sm.verified_member(&state).unwrap().unwrap();
    let overlay = verified.member_overlay();
    assert!(overlay.prove_resolved(&cut, 33).unwrap());
    let before = event_ledger::raw_walks();
    let projected = ProjectedTransaction::new(&overlay, 33).unwrap();
    assert_eq!(event_ledger::raw_walks() - before, 1);
    let expected = status_journal(64).transaction_at(33).unwrap().clone();
    for _ in 0..3 {
        let before = event_ledger::raw_walks();
        assert_eq!(
            projected.transaction_at(33).unwrap(),
            Some(expected.clone())
        );
        assert_eq!(event_ledger::raw_walks() - before, 0);
    }
    for ordinal in [1, 32, 34, 64, 0, 65, u64::MAX] {
        let expected = MemberHistory::transaction_at(&overlay, ordinal);
        let before = event_ledger::raw_walks();
        assert_eq!(projected.transaction_at(ordinal), expected, "{ordinal}");
        assert_eq!(event_ledger::raw_walks() - before, u64::from(ordinal != 0));
    }
    let before = event_ledger::raw_walks();
    assert_eq!(
        projected.transaction(expected.id).unwrap(),
        Some(expected.clone())
    );
    assert_eq!(event_ledger::raw_walks() - before, 1);
    let before = event_ledger::raw_walks();
    assert_eq!(
        projected.request_for_transaction(expected.id).unwrap(),
        None
    );
    assert_eq!(event_ledger::raw_walks() - before, 1);
}

#[tokio::test]
async fn retained_projection_keeps_transaction_equality_and_error_order() {
    let (_dir, sm, _cut) = status_fixture(4).await;
    let state = sm.state.read().await;
    let verified = sm.verified_member(&state).unwrap().unwrap();
    let overlay = verified.member_overlay();
    let original = state.d1.as_ref().unwrap();
    for ordinal in [0, 1, 2, 3, 4, 5, u64::MAX] {
        assert_eq!(
            projected_advance(original, &overlay, ordinal, true),
            projected_advance(original, &overlay, ordinal, false),
            "ordinal={ordinal}",
        );
    }
    assert!(matches!(
        projected_advance(original, &overlay, 4, true),
        Err(D1Error::VisibilityGap)
    ));
    for absent in [0, 5, u64::MAX] {
        let mut invalid = original.clone();
        invalid.visibility = None;
        // Missing/invalid point evidence still fails before observation checks.
        assert_eq!(
            projected_advance(&invalid, &overlay, absent, true),
            projected_advance(&invalid, &overlay, absent, false),
        );
        assert!(matches!(
            projected_advance(&invalid, &overlay, absent, true),
            Err(D1Error::EventLedgerHistory)
        ));
    }
    let mut invalid = original.clone();
    invalid.visibility = None;
    assert!(matches!(
        projected_advance(&invalid, &overlay, 3, true),
        Err(D1Error::VisibilityNotEnabled)
    ));
    let projected = ProjectedTransaction::new(&overlay, 3).unwrap();
    let tx = projected.transaction_at(3).unwrap().unwrap();
    let mut changed = Vec::<Transaction>::new();
    let mut bad = tx.clone();
    bad.id = id(250);
    changed.push(bad);
    let mut bad = tx.clone();
    bad.participants[0].intent[0] ^= 1;
    changed.push(bad);
    let mut bad = tx.clone();
    bad.prepared[0] = true;
    changed.push(bad);
    let mut bad = tx.clone();
    bad.decision = Some(Decision::Commit);
    changed.push(bad);
    for bad in changed {
        let mut cached = original.clone();
        let mut legacy = original.clone();
        let actual = cached.advance_member_transaction(bad.clone(), &projected, Some(original));
        assert_eq!(
            actual,
            legacy.advance_member_transaction(bad, &overlay, Some(original)),
        );
        assert_eq!(actual, Err(D1Error::VisibilityHistory));
        assert_eq!(cached, *original);
        assert_eq!(legacy, *original);
    }
}

#[tokio::test]
async fn retained_projection_preserves_frozen_cut_and_old_retry() {
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, profile).await;
        let mut journal = confirmed_journal(&mut sm, profile).await;
        journal.apply(begin(1)).unwrap();
        let unresolved = journal.history_cursor().unwrap();
        journal.apply(abort(1)).unwrap();
        let first = journal.history_cursor().unwrap();
        journal.apply(begin(2)).unwrap();
        let observed = sync_all(&mut sm, &journal).await;
        assert_eq!(
            apply(&mut sm, advance(1, &first)).await.unwrap(),
            D1Result::Visibility { applied_prefix: 1 }
        );
        assert_eq!(
            apply(&mut sm, advance(1, &unresolved)).await,
            Err(D1Error::VisibilityUnresolved)
        );
        assert_eq!(
            apply(&mut sm, advance(2, &observed)).await,
            Err(D1Error::VisibilityUnresolved)
        );
        assert_eq!(
            apply(&mut sm, advance(0, &first)).await,
            Err(D1Error::VisibilityUnresolved)
        );
        let mut forged = first.clone();
        forged.root[0] ^= 1;
        assert_eq!(
            apply(&mut sm, advance(1, &forged)).await,
            Err(D1Error::VisibilityHistory)
        );
        let before = event_ledger::raw_walks();
        assert_eq!(
            apply(&mut sm, advance(1, &first)).await.unwrap(),
            D1Result::Visibility { applied_prefix: 1 }
        );
        // Verification, frozen-cut proof, first point projection. Retry stages
        // no terminal change; the generic equality check adds no fourth walk.
        assert_eq!(event_ledger::raw_walks() - before, 3);
        let expected = disk(&sm);
        drop(sm);
        let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert_eq!(disk(&sm), expected);
        assert_eq!(status(&sm).await, (observed, 1));
    }
}

#[tokio::test]
async fn retained_projection_same_batch_successor_matches_legacy() {
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let control_dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        let mut control = Arc::new(StateMachine::open(control_dir.path()).unwrap());
        setup(&mut sm, profile).await;
        setup(&mut control, profile).await;
        let mut journal = confirmed_journal(&mut sm, profile).await;
        let control_journal = confirmed_journal(&mut control, profile).await;
        assert_eq!(journal, control_journal);
        let base = status(&sm).await.0;
        for command in [begin(1), abort(1), begin(2), abort(2), begin(3)] {
            journal.apply(command).unwrap();
        }
        let page = journal.history_page(&base, None).unwrap();
        let cut = page.cut.clone();
        let invalid_events = vec![begin(200), begin(0)];
        let invalid_end = base.extend(&invalid_events).unwrap();
        let invalid_page = HistoryPage {
            version: base.version,
            base: base.clone(),
            end: invalid_end.clone(),
            cut: invalid_end,
            events: invalid_events,
        };
        invalid_page.validate().unwrap();
        let first = sm.applied().await.unwrap().index + 1;
        let actual = sm
            .apply([
                entry(first, sync(invalid_page)),
                entry(first + 1, sync(page.clone())),
                entry(first + 2, advance(1, &cut)),
                entry(first + 3, advance(2, &cut)),
            ])
            .await
            .unwrap();
        assert!(actual[0].d1.as_ref().unwrap().is_err());
        assert!(actual[1..].iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
        let expected = control
            .apply([
                blank(first),
                entry(first + 1, sync(page)),
                entry(first + 2, legacy_advance(1, &journal)),
                entry(first + 3, legacy_advance(2, &journal)),
            ])
            .await
            .unwrap();
        assert_eq!(&actual[1..], &expected[1..]);
        assert_eq!(disk(&sm), disk(&control));
        assert_eq!(sm.state.read().await.d1, control.state.read().await.d1);
        assert_eq!(
            sm.participant_status(7, 1, [7; 16], id(200)).await.unwrap(),
            Status::Unknown
        );
        let expected = disk(&sm);
        drop(sm);
        let sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        assert_eq!(disk(&sm), expected);
        assert_eq!(status(&sm).await, (cut, 2));
    }
}

#[tokio::test]
async fn retained_projection_removes_one_walk_per_command_not_per_batch() {
    let (_dir, mut sm, cut) = status_fixture(64).await;
    let next = sm.applied().await.unwrap().index + 1;
    let before = event_ledger::raw_walks();
    let result = sm
        .apply([
            entry(next, advance(33, &cut)),
            entry(next + 1, advance(34, &cut)),
        ])
        .await
        .unwrap();
    assert!(result.iter().all(|r| r.d1.as_ref().unwrap().is_ok()));
    // One initial verification + two frozen proofs + two original point
    // projections + one new terminal projection. The old path needed eight.
    assert_eq!(event_ledger::raw_walks() - before, 6);
    assert_eq!(status(&sm).await.1, 34);
}

#[tokio::test]
async fn retained_projection_failed_sync_preserves_commit_and_reopen() {
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, profile).await;
        let journal = observed_commit(&mut sm, profile).await;
        let cut = journal.history_cursor().unwrap();
        let expected = disk(&sm);
        let d1 = sm.state.read().await.d1.clone();
        let marker = sm.applied().await;
        let next = marker.unwrap().index + 1;
        sm.storage
            .as_ref()
            .unwrap()
            .fail_next_write
            .store(true, Ordering::Relaxed);
        assert!(sm.apply([entry(next, advance(1, &cut))]).await.is_err());
        assert_eq!(disk(&sm), expected);
        assert_eq!(sm.state.read().await.d1, d1);
        assert_eq!(sm.applied().await, marker);
        drop(sm);
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        sm.forbid_coordinator_journal(true);
        assert_eq!(disk(&sm), expected);
        assert_eq!(sm.get_raw(b"x").await, None);
        assert_eq!(
            apply(&mut sm, advance(1, &cut)).await.unwrap(),
            D1Result::Visibility { applied_prefix: 1 }
        );
        assert_eq!(sm.get_raw(b"x").await, Some(b"committed".to_vec()));
        let d1 = sm.state.read().await.d1.clone();
        assert_eq!(
            apply(&mut sm, advance(1, &cut)).await.unwrap(),
            D1Result::Visibility { applied_prefix: 1 }
        );
        assert_eq!(sm.state.read().await.d1, d1);
    }
}

#[tokio::test]
async fn retained_projection_keeps_corrupt_tail_fail_closed() {
    for damage in ["raw", "derived", "metadata"] {
        let (_dir, mut sm, cut) = status_fixture(64).await;
        let state = sm.state.read().await.d1.clone();
        let marker = sm.applied().await;
        let db = &sm.storage.as_ref().unwrap().db;
        match damage {
            "raw" => {
                let rows = rows(&sm, event_ledger::PREFIX);
                assert!(rows.len() > 1);
                db.put(&rows.last().unwrap().0, b"{}").unwrap();
            }
            "derived" => {
                db.put(crate::decision_segments::MANIFEST_KEY, b"{}")
                    .unwrap();
            }
            _ => {
                db.put(D1_KEY, b"{}").unwrap();
            }
        }
        let damaged = disk(&sm);
        // Even an old retry must reject damage after its target; no cached result
        // can be minted before current complete authoritative verification.
        assert!(
            sm.apply([entry(marker.unwrap().index + 1, advance(1, &cut))])
                .await
                .is_err(),
            "{damage}"
        );
        assert_eq!(disk(&sm), damaged);
        assert_eq!(sm.state.read().await.d1, state);
        assert_eq!(sm.applied().await, marker);
    }
}

#[tokio::test]
#[ignore = "local paired point-projection benchmark; run explicitly with --release --ignored --nocapture"]
async fn retained_projection_benchmark() {
    use std::time::Instant;
    for count in [1, 2, 32, 64, 256, 512, 1024] {
        let (_dir, sm, cut) = status_fixture(count).await;
        let state = sm.state.read().await;
        let verified = sm.verified_member(&state).unwrap().unwrap();
        let overlay = verified.member_overlay();
        let ordinal = count / 2 + 1;
        assert!(overlay.prove_resolved(&cut, ordinal).unwrap());
        let expected =
            projected_advance(state.d1.as_ref().unwrap(), &overlay, ordinal, false).unwrap();
        for sample in 0..9 {
            for mode in if sample % 2 == 0 { [0, 1] } else { [1, 0] } {
                let iterations = 20;
                let before = event_ledger::raw_walks();
                let started = Instant::now();
                for _ in 0..iterations {
                    let actual =
                        projected_advance(state.d1.as_ref().unwrap(), &overlay, ordinal, mode == 1)
                            .unwrap();
                    assert_eq!(actual, expected);
                }
                let elapsed = started.elapsed();
                let walks = event_ledger::raw_walks() - before;
                assert_eq!(walks, iterations * if mode == 0 { 2 } else { 1 });
                println!("retained_projection transactions={count} mode={mode} sample={sample} iterations={iterations} walks={walks} elapsed_ns={}",elapsed.as_nanos());
            }
        }
    }
}
