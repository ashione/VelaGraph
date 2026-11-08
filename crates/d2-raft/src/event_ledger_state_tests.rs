//! Cross-layer regression tests reuse the fixed-cohort fixture in the parent.
use super::*;

fn ledger_rows(sm: &StateMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .iterator(IteratorMode::Start)
        .map(Result::unwrap)
        .filter(|(key, _)| key.starts_with(event_ledger::PREFIX))
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect()
}
fn disk(sm: &StateMachine) -> Vec<(Vec<u8>, Vec<u8>)> {
    sm.storage
        .as_ref()
        .unwrap()
        .db
        .iterator(IteratorMode::Start)
        .map(Result::unwrap)
        .map(|(key, value)| (key.to_vec(), value.to_vec()))
        .collect()
}
fn metadata(sm: &StateMachine) -> serde_json::Value {
    serde_json::from_slice(
        &sm.storage
            .as_ref()
            .unwrap()
            .db
            .get(D1_KEY)
            .unwrap()
            .unwrap(),
    )
    .unwrap()
}
fn sync(page: crate::transaction_decisions::HistoryPage) -> D1Command {
    D1Command::SyncHistory {
        authority: authority(),
        shard: 7,
        epoch: 1,
        mode_id: [7; 16],
        page: Box::new(page),
    }
}
fn retained(ordinal: u64, cut: &crate::transaction_decisions::HistoryCursor) -> D1Command {
    D1Command::AdvanceRetained {
        authority: authority(),
        shard: 7,
        epoch: 1,
        mode_id: [7; 16],
        ordinal,
        cut: cut.clone(),
    }
}

#[tokio::test]
async fn journal_less_authority_and_multi_entry_overlay_keep_holes_and_old_versions() {
    let dir = tempfile::tempdir().unwrap();
    let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut sm, false).await;
    // 1 binding + 32 Begins + 31 Abort events. A terminal maximum is not the prefix.
    apply(
        &mut sm,
        (1..=32)
            .map(|n| coordinator(begin(n)))
            .chain((2..=32).map(|n| coordinator(decide(n))))
            .collect(),
    )
    .await;
    assert_eq!(
        sm.coordinator_journal(9, 1, [9; 16])
            .await
            .unwrap()
            .resolved_prefix(),
        0
    );
    let header = metadata(&sm);
    assert_eq!(header["version"], 16);
    assert!(header.get("state").is_none() && header.get("coordinator").is_none());
    assert_eq!(header["ledger"]["event_count"], 64);
    assert_eq!(ledger_rows(&sm).len(), 2);
    let sealed = ledger_rows(&sm);
    let before = disk(&sm);
    fail_sync(&sm);
    let index = sm.applied().await.unwrap().index + 1;
    assert!(sm
        .apply([
            entry(index, coordinator(decide(1))),
            entry(index + 1, coordinator(begin(33))),
            entry(index + 2, coordinator(decide(33)))
        ])
        .await
        .is_err());
    assert_eq!(disk(&sm), before);
    apply(
        &mut sm,
        vec![
            coordinator(decide(1)),
            coordinator(begin(33)),
            coordinator(decide(33)),
        ],
    )
    .await;
    assert_eq!(&ledger_rows(&sm)[..2], sealed.as_slice());
    let expected = sm.coordinator_journal(9, 1, [9; 16]).await.unwrap();
    assert_eq!(expected.resolved_prefix(), 33);
    let old = expected.terminal_prefix(1).unwrap();
    assert_eq!(prefix(&sm, 1, false).await, old);
    drop(sm);
    let sm = StateMachine::open(dir.path()).unwrap();
    assert_eq!(
        sm.coordinator_journal(9, 1, [9; 16]).await.unwrap(),
        expected
    );
    assert_eq!(prefix(&sm, 1, false).await, old);
    assert_eq!(metadata(&sm)["ledger"]["event_count"], 67);
}

#[tokio::test]
async fn incremental_member_history_and_application_publish_one_atomic_cut() {
    let dir = tempfile::tempdir().unwrap();
    let mut member = Arc::new(StateMachine::open(dir.path()).unwrap());
    setup(&mut member, true).await;
    let source = journal(33);
    let cut = source.history_cursor().unwrap();
    let mut cursor = journal(0).history_cursor().unwrap();
    let mut commands = Vec::new();
    while cursor != cut {
        let page = source.history_page(&cursor, Some(&cut)).unwrap();
        cursor = page.end.clone();
        commands.push(sync(page));
    }
    // Observed history alone must not consume a decision or make a read visible.
    apply(&mut member, commands.clone()).await;
    let status = member
        .participant_history_status(7, 1, [7; 16])
        .await
        .unwrap();
    assert_eq!(status, (cut.clone(), 0));
    assert_eq!(
        prefix(&member, 0, true).await,
        source.terminal_prefix(0).unwrap()
    );
    assert_eq!(metadata(&member)["version"], 12);
    assert!(metadata(&member)["visibility"].get("journal").is_none());
    assert!(member
        .participant_read_at(7, 1, [7; 16], 1, limit())
        .await
        .is_err());
    // Duplicate overlapping pages followed by sequential application share the
    // same unpublished candidate, never a stale reconstruction per Raft entry.
    commands.extend((1..=33).map(|v| retained(v, &cut)));
    let before = disk(&member);
    fail_sync(&member);
    let index = member.applied().await.unwrap().index + 1;
    assert!(member
        .apply(
            commands
                .clone()
                .into_iter()
                .enumerate()
                .map(|(n, c)| entry(index + n as u64, c))
        )
        .await
        .is_err());
    assert_eq!(disk(&member), before);
    apply(&mut member, commands).await;
    let old = member
        .participant_read_at(7, 1, [7; 16], 1, limit())
        .await
        .unwrap();
    let raw = ledger_rows(&member);
    assert_eq!(raw.len(), 3);
    drop(member);
    let mut member = Arc::new(StateMachine::open(dir.path()).unwrap());
    assert_eq!(
        member
            .participant_history_status(7, 1, [7; 16])
            .await
            .unwrap(),
        (cut.clone(), 33)
    );
    assert_eq!(
        member
            .participant_read_at(7, 1, [7; 16], 1, limit())
            .await
            .unwrap(),
        old
    );
    apply(&mut member, vec![retained(1, &cut), retained(33, &cut)]).await;
    assert_eq!(ledger_rows(&member), raw);
}

#[tokio::test]
async fn raw_corruption_blocks_source_reads_apply_snapshot_build_and_reopen_without_fallback() {
    for member in [false, true] {
        for case in 0..5 {
            let dir = tempfile::tempdir().unwrap();
            let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
            setup(&mut sm, member).await;
            if member {
                apply(
                    &mut sm,
                    (1..=33).map(|v| advance(v, &journal(33))).collect(),
                )
                .await;
            } else {
                append(&mut sm, 1, 33).await;
            }
            // Keep a valid complete archived snapshot: it is deliberately NOT
            // a repair source for ordinary open, read or apply.
            let image = sm.build_snapshot().await.unwrap();
            let rows = ledger_rows(&sm);
            let db = &sm.storage.as_ref().unwrap().db;
            match case {
                0 => db.delete(&rows[0].0).unwrap(),
                1 => db.put(&rows[1].0, b"bad").unwrap(),
                2 => db
                    .put([event_ledger::PREFIX, b"orphan"].concat(), &rows[0].1)
                    .unwrap(),
                3 => {
                    let mut row: serde_json::Value = serde_json::from_slice(&rows[2].1).unwrap();
                    row["after_root"][0] = serde_json::json!(255);
                    db.put(&rows[2].0, serde_json::to_vec(&row).unwrap())
                        .unwrap();
                }
                4 => {
                    let mut header = metadata(&sm);
                    header["ledger"]["head_root"][0] =
                        serde_json::json!(header["ledger"]["head_root"][0].as_u64().unwrap() ^ 1);
                    db.put(D1_KEY, serde_json::to_vec(&header).unwrap())
                        .unwrap();
                }
                _ => unreachable!(),
            }
            let before = disk(&sm);
            let marker = sm.applied().await;
            if member {
                assert_eq!(
                    sm.participant_history_status(7, 1, [7; 16]).await,
                    Err(D1Error::EventLedgerHistory)
                );
            } else {
                assert_eq!(
                    sm.coordinator_journal(9, 1, [9; 16]).await,
                    Err(D1Error::EventLedgerHistory)
                );
                assert_eq!(
                    sm.coordinator_history_page(
                        9,
                        1,
                        [9; 16],
                        &journal(0).history_cursor().unwrap(),
                        None
                    )
                    .await,
                    Err(D1Error::EventLedgerHistory)
                );
            }
            assert!(sm.build_snapshot().await.is_err());
            let command = if member {
                advance(33, &journal(33))
            } else {
                coordinator(decide(33))
            };
            assert!(sm
                .apply([entry(marker.unwrap().index + 1, command)])
                .await
                .is_err());
            assert_eq!(sm.applied().await, marker);
            assert_eq!(disk(&sm), before);
            // Explicit snapshot replacement validates the local successor, then
            // rebuilds one closed generation, including removal of orphan rows.
            sm.install_snapshot(&image.meta, image.snapshot)
                .await
                .unwrap();
            assert_eq!(ledger_rows(&sm), rows);
            // Damage again to prove reopen does not fall back to that snapshot.
            sm.storage.as_ref().unwrap().db.delete(&rows[0].0).unwrap();
            drop(sm);
            assert!(StateMachine::open(dir.path()).is_err());
        }
    }
}

#[tokio::test]
async fn published_v9_v10_migrate_atomically_and_reject_mixed_authorities() {
    for member in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        setup(&mut sm, member).await;
        if member {
            apply(&mut sm, vec![advance(1, &journal(1))]).await;
        } else {
            append(&mut sm, 1, 1).await;
        }
        let state = sm.state.read().await;
        let full = sm.load_disk_state(&state).unwrap().unwrap().state;
        let bytes = serde_json::to_vec(&serde_json::json!({"version":if member {10}else{9},
            "state":full.stage()}))
        .unwrap();
        let mut legacy_index = WriteBatch::default();
        if !member {
            crate::request_index::stage(
                &mut legacy_index,
                None,
                Some(&full),
                state.last_applied_log,
                false,
                true,
            )
            .unwrap();
        }
        drop(state);
        let db = &sm.storage.as_ref().unwrap().db;
        legacy_index.put(D1_KEY, &bytes);
        db.write(legacy_index).unwrap();
        drop(sm);
        assert!(StateMachine::open(dir.path()).is_err()); // dual authority
        {
            let db = DB::open_default(dir.path()).unwrap();
            let mut batch = WriteBatch::default();
            batch.delete_range(event_ledger::PREFIX, b"sm/ledger0");
            db.write(batch).unwrap();
        }
        let mut sm = Arc::new(StateMachine::open(dir.path()).unwrap());
        let old = prefix(&sm, 1, member).await;
        let before = disk(&sm);
        let command = if member {
            advance(1, &journal(1))
        } else {
            coordinator(decide(1))
        };
        let index = sm.applied().await.unwrap().index + 1;
        fail_sync(&sm);
        assert!(sm.apply([entry(index, command.clone())]).await.is_err());
        assert_eq!(disk(&sm), before);
        apply(&mut sm, vec![command]).await;
        assert_eq!(metadata(&sm)["version"], if member { 12 } else { 16 });
        assert_eq!(prefix(&sm, 1, member).await, old);
        assert_eq!(ledger_rows(&sm).len(), 1);
        drop(sm);
        let sm = StateMachine::open(dir.path()).unwrap();
        assert_eq!(prefix(&sm, 1, member).await, old);
    }
}
