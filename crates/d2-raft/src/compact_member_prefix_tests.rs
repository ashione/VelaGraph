//! Actual public active-member reads under the full-Journal tripwire.
use super::*;

fn limit() -> crate::versioned_reads::ReadLimit {
    crate::versioned_reads::ReadLimit {
        rows: 100,
        bytes: 4096,
        operations: 10000,
    }
}

async fn committed() -> (
    tempfile::TempDir,
    Arc<StateMachine>,
    crate::terminal_prefix::TerminalPrefix,
) {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, CapacityProfile::LegacyCountV1).await;
    let journal = observed_commit(&mut sm, CapacityProfile::LegacyCountV1).await;
    apply(&mut sm, advance(1, &journal.history_cursor().unwrap()))
        .await
        .unwrap();
    let prefix = journal.terminal_prefix(1).unwrap();
    drop(journal);
    (dir, sm, prefix)
}

#[tokio::test]
async fn compact_terminal_proof_does_not_construct_journal() {
    let (_dir, sm, expected) = committed().await;
    assert_eq!(
        no_member_journal(sm.participant_terminal_prefix(7, 1, [7; 16], &expected))
            .await
            .unwrap(),
        expected
    );
}

#[tokio::test]
async fn compact_terminal_read_does_not_construct_journal() {
    let (_dir, sm, expected) = committed().await;
    let (snapshot, proof) =
        no_member_journal(sm.certified_terminal_read(7, 1, [7; 16], limit(), &expected))
            .await
            .unwrap();
    assert_eq!(proof, expected);
    assert_eq!(
        snapshot.entries.get(b"x".as_slice()).map(Vec::as_slice),
        Some(b"committed".as_slice())
    );
}

async fn check(sm: &StateMachine, expected: &crate::terminal_prefix::TerminalPrefix) {
    let walks = event_ledger::raw_walks();
    let proof = no_member_journal(sm.participant_terminal_prefix(7, 1, [7; 16], expected))
        .await
        .unwrap();
    assert_eq!(&proof, expected);
    assert_eq!(event_ledger::raw_walks() - walks, 1);
    let walks = event_ledger::raw_walks();
    let (snapshot, proof) =
        no_member_journal(sm.certified_terminal_read(7, 1, [7; 16], limit(), expected))
            .await
            .unwrap();
    assert_eq!(&proof, expected);
    assert_eq!(event_ledger::raw_walks() - walks, 1);
    assert_eq!(
        snapshot.entries.get(b"x".as_slice()).map(Vec::as_slice),
        if expected.version() == 0 {
            None
        } else {
            Some(b"committed".as_slice())
        }
    );
}

async fn history_fixture(
    profile: CapacityProfile,
) -> (tempfile::TempDir, Arc<StateMachine>, Journal) {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, profile).await;
    let mut journal = observed_commit(&mut sm, profile).await;
    for n in 2..=65 {
        journal
            .apply(Command::Begin {
                id: id(n),
                participants: vec![descriptor(if n % 2 == 0 { 7 } else { 8 })],
            })
            .unwrap();
        journal.apply(abort(n)).unwrap();
    }
    let cut = sync_all(&mut sm, &journal).await;
    for ordinal in 1..=33 {
        apply(&mut sm, advance(ordinal, &cut)).await.unwrap();
    }
    // R=65 while A=33: observed decisions must not become applied evidence.
    check(&sm, &journal.terminal_prefix(33).unwrap()).await;
    assert!(matches!(
        no_member_journal(sm.participant_terminal_prefix(
            7,
            1,
            [7; 16],
            &journal.terminal_prefix(34).unwrap()
        ))
        .await,
        Err(D1Error::VisibilityNotApplied)
    ));
    for ordinal in 34..=65 {
        apply(&mut sm, advance(ordinal, &cut)).await.unwrap();
    }
    journal.apply(begin(66)).unwrap();
    journal.apply(begin(67)).unwrap();
    journal.apply(abort(67)).unwrap();
    sync_all(&mut sm, &journal).await;
    // Decision 67 does not resolve the hole at 66. A=R=65, N=67.
    (dir, sm, journal)
}

#[tokio::test]
async fn compact_terminal_profiles_segments_old_versions_holes_and_reopen() {
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let (dir, sm, journal) = history_fixture(profile).await;
        let proofs: Vec<_> = (0..=65)
            .map(|v| journal.terminal_prefix(v).unwrap())
            .collect();
        drop(journal);
        for proof in &proofs {
            check(&sm, proof).await;
        }
        drop(sm);
        let sm = StateMachine::open(dir.path()).unwrap();
        for v in [0, 1, 31, 32, 33, 63, 64, 65] {
            check(&sm, &proofs[v]).await;
        }
    }
}

#[tokio::test]
async fn compact_terminal_rejects_suffix_corruption_and_never_repairs() {
    for profile in [
        CapacityProfile::LegacyCountV1,
        CapacityProfile::ByteBound16MiBV1,
    ] {
        let (dir, sm, journal) = history_fixture(profile).await;
        let expected = journal.terminal_prefix(1).unwrap();
        drop(journal);
        drop(sm);
        let sm = StateMachine::open(dir.path()).unwrap();
        check(&sm, &expected).await;
        let db = &sm.storage.as_ref().unwrap().db;
        let raw = rows(&sm, event_ledger::PREFIX);
        let index = rows(&sm, crate::decision_segments::PREFIX);
        assert!(index.len() >= 3);
        // Last raw row includes history beyond V=1; last index row starts at 65.
        let keys = [
            raw.last().unwrap().0.clone(),
            index.last().unwrap().0.clone(),
            D1_KEY.to_vec(),
            APPLIED_KEY.to_vec(),
            crate::decision_segments::MANIFEST_KEY.to_vec(),
        ];
        for (case, key) in keys.iter().enumerate() {
            let value = db.get(key).unwrap().unwrap();
            db.put(key, b"{}").unwrap();
            let corrupted = disk(&sm);
            let proof =
                no_member_journal(sm.participant_terminal_prefix(7, 1, [7; 16], &expected)).await;
            let read =
                no_member_journal(sm.certified_terminal_read(7, 1, [7; 16], limit(), &expected))
                    .await;
            if matches!(case, 1 | 4) {
                assert!(
                    matches!(proof, Err(D1Error::DecisionIndexHistory)),
                    "{proof:?}"
                );
                assert!(
                    matches!(read, Err(D1Error::DecisionIndexHistory)),
                    "{read:?}"
                );
            } else {
                assert!(
                    matches!(proof, Err(D1Error::EventLedgerHistory)),
                    "{proof:?}"
                );
                assert!(matches!(read, Err(D1Error::EventLedgerHistory)), "{read:?}");
            }
            assert_eq!(disk(&sm), corrupted);
            db.put(key, &value).unwrap();
            check(&sm, &expected).await;
        }
        for key in [&raw.last().unwrap().0, &index.last().unwrap().0] {
            let value = db.get(key).unwrap().unwrap();
            db.delete(key).unwrap();
            let corrupted = disk(&sm);
            assert!(
                no_member_journal(sm.participant_terminal_prefix(7, 1, [7; 16], &expected))
                    .await
                    .is_err()
            );
            assert!(no_member_journal(sm.certified_terminal_read(
                7,
                1,
                [7; 16],
                limit(),
                &expected
            ))
            .await
            .is_err());
            assert_eq!(disk(&sm), corrupted);
            db.put(key, value).unwrap();
        }
        for prefix in [event_ledger::PREFIX, crate::decision_segments::PREFIX] {
            let mut extra = prefix.to_vec();
            extra.extend_from_slice(b"zz-extra");
            db.put(&extra, b"{}").unwrap();
            let corrupted = disk(&sm);
            assert!(
                no_member_journal(sm.participant_terminal_prefix(7, 1, [7; 16], &expected))
                    .await
                    .is_err()
            );
            assert!(no_member_journal(sm.certified_terminal_read(
                7,
                1,
                [7; 16],
                limit(),
                &expected
            ))
            .await
            .is_err());
            assert_eq!(disk(&sm), corrupted);
            db.delete(extra).unwrap();
        }
        drop(sm);
        let sm = StateMachine::open(dir.path()).unwrap();
        check(&sm, &expected).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compact_terminal_read_retains_guard_through_snapshot() {
    let (_dir, sm, expected) = committed().await;
    let (entered, ready) = tokio::sync::oneshot::channel();
    let (release, gate) = std::sync::mpsc::channel();
    *sm.terminal_read_gate.lock().unwrap() = Some(ApplyPublishGate {
        entered,
        release: gate,
    });
    let reader = sm.clone();
    let requested = expected.clone();
    let read = tokio::spawn(async move {
        no_member_journal(reader.certified_terminal_read(7, 1, [7; 16], limit(), &requested)).await
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), ready)
        .await
        .unwrap()
        .unwrap();
    assert!(
        sm.state.try_write().is_err(),
        "public read must hold the lock after verification"
    );
    let writer = sm.clone();
    let (waiting, queued) = tokio::sync::oneshot::channel();
    let write = tokio::spawn(async move {
        use std::future::Future;
        let mut lock = Box::pin(writer.state.write());
        std::future::poll_fn(|cx| {
            assert!(
                lock.as_mut().poll(cx).is_pending(),
                "publication must wait for snapshot capture"
            );
            std::task::Poll::Ready(())
        })
        .await;
        waiting.send(()).unwrap();
        // A publication could invalidate identity after releasing the read.
        lock.await.d1.as_mut().unwrap().epoch = 2;
    });
    tokio::time::timeout(std::time::Duration::from_secs(5), queued)
        .await
        .unwrap()
        .unwrap();
    release.send(()).unwrap();
    let (snapshot, proof) = read.await.unwrap().unwrap();
    assert_eq!(proof, expected);
    assert_eq!(
        snapshot.entries.get(b"x".as_slice()).map(Vec::as_slice),
        Some(b"committed".as_slice())
    );
    write.await.unwrap();
    assert_eq!(sm.state.read().await.d1.as_ref().unwrap().epoch, 2);
}

#[tokio::test]
async fn compact_terminal_preserves_identity_version_baseline_and_proof_errors() {
    let (_dir, sm, expected) = committed().await;
    for (shard, epoch, mode) in [(8, 1, [7; 16]), (7, 2, [7; 16]), (7, 1, [8; 16])] {
        let expected_error = {
            let state = sm.state.read().await;
            let d1 = state.d1.as_ref().unwrap();
            d1.check_shard(shard)
                .and_then(|_| d1.check_epoch(epoch))
                .and_then(|_| {
                    if d1.managed.as_ref().unwrap().mode_id == mode {
                        Ok(())
                    } else {
                        Err(D1Error::ManagedModeConflict)
                    }
                })
                .unwrap_err()
        };
        assert_eq!(
            no_member_journal(sm.participant_terminal_prefix(shard, epoch, mode, &expected))
                .await
                .unwrap_err(),
            expected_error
        );
        assert_eq!(
            no_member_journal(sm.certified_terminal_read(shard, epoch, mode, limit(), &expected))
                .await
                .unwrap_err(),
            expected_error
        );
    }
    let mut later = empty(CapacityProfile::LegacyCountV1);
    for n in 1..=2 {
        later.apply(begin(n)).unwrap();
        later.apply(abort(n)).unwrap();
    }
    let future = later.terminal_prefix(2).unwrap();
    let wrong = later.terminal_prefix(1).unwrap(); // Abort differs from the real Commit.
    drop(later);
    for (proof, error) in [
        (&future, D1Error::VisibilityNotApplied),
        (&wrong, D1Error::VisibilityHistory),
    ] {
        assert_eq!(
            no_member_journal(sm.participant_terminal_prefix(7, 1, [7; 16], proof))
                .await
                .unwrap_err(),
            error
        );
        assert_eq!(
            no_member_journal(sm.certified_terminal_read(7, 1, [7; 16], limit(), proof))
                .await
                .unwrap_err(),
            error
        );
    }
    sm.state
        .write()
        .await
        .d1
        .as_mut()
        .unwrap()
        .visibility
        .as_mut()
        .unwrap()
        .baseline_sequence = 1;
    assert_eq!(
        no_member_journal(sm.participant_terminal_prefix(7, 1, [7; 16], &expected))
            .await
            .unwrap_err(),
        D1Error::VisibilityHistory
    );
    assert_eq!(
        no_member_journal(sm.certified_terminal_read(7, 1, [7; 16], limit(), &expected))
            .await
            .unwrap_err(),
        D1Error::VisibilityHistory
    );
}

#[tokio::test]
async fn compact_terminal_keeps_full_in_memory_fallback() {
    let mut sm = Arc::new(StateMachine::default());
    let journal = empty(CapacityProfile::LegacyCountV1);
    for command in [
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
            authority: authority(),
        },
        D1Command::EnableVersionedReads {
            shard: 7,
            epoch: 1,
            mode_id: [7; 16],
            journal: journal.clone(),
        },
    ] {
        apply(&mut sm, command).await.unwrap();
    }
    let expected = journal.terminal_prefix(0).unwrap();
    assert!(sm
        .state
        .read()
        .await
        .d1
        .as_ref()
        .unwrap()
        .visibility
        .as_ref()
        .unwrap()
        .journal
        .full()
        .is_some());
    assert_eq!(
        sm.participant_terminal_prefix(7, 1, [7; 16], &expected)
            .await
            .unwrap(),
        expected
    );
    let (snapshot, proof) = sm
        .certified_terminal_read(7, 1, [7; 16], limit(), &expected)
        .await
        .unwrap();
    assert!(snapshot.entries.is_empty());
    assert_eq!(proof, expected);
}
