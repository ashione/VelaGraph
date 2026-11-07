use super::*;
use crate::{D1ManagedStatus, D1WriteRequest};
fn entry(index: u64, command: D1Command) -> Entry<TypeConfig> {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(LogCommand::D1V1(command)),
    }
}
fn request(id: u8, expected: u64) -> D1WriteRequest {
    D1WriteRequest {
        shard: 7,
        epoch: 3,
        request_id: [id; 16],
        expected_sequence: Some(expected),
        operations: vec![WriteOp::put(b"key", [id])],
    }
}
fn enable() -> D1Command {
    D1Command::EnableManagedWrites {
        shard: 7,
        epoch: 3,
        expected_sequence: 1,
        mode_id: [9; 16],
    }
}
fn managed(id: u8, expected: u64) -> D1Command {
    D1Command::ManagedWrite {
        mode_id: [9; 16],
        request: request(id, expected),
    }
}

#[tokio::test]
async fn managed_activation_write_failure_reopen_snapshot_and_retry() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut sm = Arc::new(StateMachine::open(dir.path())?);
    sm.apply([
        entry(1, D1Command::Activate { shard: 7, epoch: 3 }),
        entry(2, D1Command::Write(request(1, 0))),
    ])
    .await?;
    let old = sm.storage.as_ref().unwrap().db.get(D1_KEY)?.unwrap();
    sm.storage
        .as_ref()
        .unwrap()
        .fail_next_write
        .store(true, Ordering::Relaxed);
    assert!(sm.apply([entry(3, enable())]).await.is_err());
    assert_eq!(sm.d1_managed_mode(7, 3).await.unwrap(), None);
    assert_eq!(sm.storage.as_ref().unwrap().db.get(D1_KEY)?.unwrap(), old);
    sm.apply([entry(3, enable())]).await?;
    let header = sm.storage.as_ref().unwrap().db.get(D1_KEY)?.unwrap();
    let parsed: D1MetadataV4 = serde_json::from_slice(&header)?;
    assert_eq!(parsed.version, 4);
    assert_eq!(header.as_slice(), br#"{"version":4,"shard":7,"epoch":3,"sequence":1,"dedup_bytes_limit":null,"managed":{"mode_id":[9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9],"activated_sequence":1}}"#);
    assert!(serde_json::from_slice::<D1MetadataV3>(&header).is_err());
    assert!(serde_json::from_slice::<D1State>(&header).is_err());
    sm.storage
        .as_ref()
        .unwrap()
        .fail_next_write
        .store(true, Ordering::Relaxed);
    assert!(sm.apply([entry(4, managed(2, 1))]).await.is_err());
    assert_eq!(sm.get_raw(b"key").await, Some(vec![1]));
    assert_eq!(
        sm.d1_managed_status(7, 3, [9; 16], [2; 16]).await.unwrap(),
        D1ManagedStatus::Unknown
    );
    sm.apply([entry(4, managed(2, 1))]).await?;
    let snapshot = sm.build_snapshot().await?;
    let decoded: SnapshotV1 = serde_json::from_slice(snapshot.snapshot.get_ref())?;
    assert_eq!(decoded.d1.version, 3);
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path())?);
    let target_dir = tempfile::tempdir()?;
    let mut target = Arc::new(StateMachine::open(target_dir.path())?);
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await?;
    drop(target);
    let mut target = Arc::new(StateMachine::open(target_dir.path())?);
    for state in [&mut sm, &mut target] {
        assert_eq!(
            state
                .d1_managed_status(7, 3, [9; 16], [2; 16])
                .await
                .unwrap(),
            D1ManagedStatus::Committed { sequence: 2 }
        );
        let results = state
            .apply([
                entry(5, D1Command::Write(request(3, 2))),
                entry(6, managed(1, 0)),
                entry(7, managed(2, 1)),
                entry(8, managed(3, 2)),
            ])
            .await?;
        assert_eq!(results[0].d1, Some(Err(D1Error::ManagedWriteRequired)));
        assert_eq!(results[1].d1, Some(Err(D1Error::IdempotencyConflict)));
        assert!(matches!(
            results[2].d1,
            Some(Ok(D1Result::ManagedWrite {
                sequence: 2,
                status: D1WriteStatus::Duplicate,
                ..
            }))
        ));
        assert!(matches!(
            results[3].d1,
            Some(Ok(D1Result::ManagedWrite {
                sequence: 3,
                status: D1WriteStatus::Committed,
                ..
            }))
        ));
        assert_eq!(state.get_raw(b"key").await, Some(vec![3]));
        assert_eq!(state.d1_metadata().await, Some((7, 3, 3)));
        let legacy = Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 9),
            payload: EntryPayload::Normal(LogCommand::put(b"key", b"bypass")),
        };
        assert_eq!(
            state.apply([legacy]).await?[0].d1,
            Some(Err(D1Error::LegacyWriteDisabled))
        );
        assert_eq!(state.get_raw(b"key").await, Some(vec![3]));
    }
    Ok(())
}

#[tokio::test]
async fn managed_snapshots_cannot_remove_fence_or_rewrite_committed_history() -> anyhow::Result<()>
{
    let mut sm = Arc::new(StateMachine::default());
    sm.apply([
        entry(1, D1Command::Activate { shard: 7, epoch: 3 }),
        entry(2, D1Command::Write(request(1, 0))),
        entry(3, enable()),
        entry(4, managed(2, 1)),
    ])
    .await?;
    for change in 0..5 {
        let snapshot = sm.build_snapshot().await?;
        let mut decoded: SnapshotV1 = serde_json::from_slice(snapshot.snapshot.get_ref())?;
        match change {
            0 => {
                decoded.d1.version = 1;
                decoded.d1.managed = None;
            }
            1 => decoded.d1.managed.as_mut().unwrap().mode_id = [8; 16],
            2 => {
                let mut record = serde_json::to_value(&decoded.d1.dedup[1])?;
                record["request"]["request_id"] = serde_json::to_value([7; 16])?;
                decoded.d1.dedup[1] = serde_json::from_value(record)?;
            }
            3 => decoded.d1.epoch = 2,
            _ => decoded.entries[0].1 = vec![99],
        }
        // All are syntactically valid snapshots with a later applied marker.
        // Epoch case also adjusts retained request epochs so decode validates.
        if change == 3 {
            for r in &mut decoded.d1.dedup {
                let mut record = serde_json::to_value(&*r)?;
                record["request"]["epoch"] = 2.into();
                *r = serde_json::from_value(record)?;
            }
        }
        decoded.d1.validate()?;
        let mut meta = snapshot.meta;
        meta.last_log_id = Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 100));
        let bytes = serde_json::to_vec(&decoded)?;
        assert!(sm
            .install_snapshot(&meta, Box::new(std::io::Cursor::new(bytes)))
            .await
            .is_err());
        assert_eq!(
            sm.d1_managed_status(7, 3, [9; 16], [2; 16]).await.unwrap(),
            D1ManagedStatus::Committed { sequence: 2 }
        );
    }
    Ok(())
}

#[tokio::test]
async fn managed_snapshot_applies_a_valid_new_commit_suffix() -> anyhow::Result<()> {
    let mut source = Arc::new(StateMachine::default());
    source
        .apply([
            entry(1, D1Command::Activate { shard: 7, epoch: 3 }),
            entry(2, D1Command::Write(request(1, 0))),
            entry(3, enable()),
            entry(4, managed(2, 1)),
        ])
        .await?;
    let first = source.build_snapshot().await?;
    let mut target = Arc::new(StateMachine::default());
    target.install_snapshot(&first.meta, first.snapshot).await?;
    let mut third = request(3, 2);
    third.operations = vec![
        WriteOp::Delete {
            key: b"key".to_vec(),
        },
        WriteOp::put(b"other", b"new"),
    ];
    source
        .apply([entry(
            5,
            D1Command::ManagedWrite {
                mode_id: [9; 16],
                request: third,
            },
        )])
        .await?;
    let latest = source.build_snapshot().await?;
    target
        .install_snapshot(&latest.meta, latest.snapshot)
        .await?;
    assert_eq!(target.get_raw(b"key").await, None);
    assert_eq!(target.get_raw(b"other").await, Some(b"new".to_vec()));
    assert_eq!(
        target
            .d1_managed_status(7, 3, [9; 16], [3; 16])
            .await
            .unwrap(),
        D1ManagedStatus::Committed { sequence: 3 }
    );
    Ok(())
}

#[tokio::test]
async fn managed_fresh_restore_and_reopen_reject_inconsistent_data() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut source = Arc::new(StateMachine::open(dir.path())?);
    source
        .apply([
            entry(1, D1Command::Activate { shard: 7, epoch: 3 }),
            entry(2, D1Command::Write(request(1, 0))),
            entry(3, enable()),
            entry(4, managed(2, 1)),
        ])
        .await?;
    let snapshot = source.build_snapshot().await?;
    let mut decoded: SnapshotV1 = serde_json::from_slice(snapshot.snapshot.get_ref())?;
    decoded.entries[0].1 = vec![99];
    let mut fresh = Arc::new(StateMachine::default());
    assert!(fresh
        .install_snapshot(
            &snapshot.meta,
            Box::new(Cursor::new(serde_json::to_vec(&decoded)?))
        )
        .await
        .is_err());
    assert_eq!(fresh.d1_metadata().await, None);
    source
        .storage
        .as_ref()
        .unwrap()
        .db
        .put(data_key(b"key"), [99])?;
    drop(source);
    assert!(StateMachine::open(dir.path()).is_err());
    Ok(())
}

#[tokio::test]
async fn activation_rejects_untracked_baseline_and_accepts_epoch_extended_history(
) -> anyhow::Result<()> {
    let mut source = Arc::new(StateMachine::default());
    source
        .apply([
            entry(1, D1Command::Activate { shard: 7, epoch: 3 }),
            entry(2, D1Command::Write(request(1, 0))),
        ])
        .await?;
    // Deliberate pre-upgrade corruption, accessible only inside this test module.
    source
        .state
        .write()
        .await
        .data
        .insert(b"untracked".to_vec(), vec![99]);
    assert_eq!(
        source.apply([entry(3, enable())]).await?[0].d1,
        Some(Err(D1Error::UnverifiedWriteHistory))
    );
    assert_eq!(source.d1_managed_mode(7, 3).await.unwrap(), None);
    source
        .state
        .write()
        .await
        .data
        .remove(b"untracked".as_slice());
    source
        .apply([
            entry(4, enable()),
            entry(
                5,
                D1Command::SetEpoch {
                    shard: 7,
                    expected_epoch: 3,
                    new_epoch: 4,
                },
            ),
        ])
        .await?;
    let mut updated = request(2, 1);
    updated.epoch = 4;
    source
        .apply([entry(
            6,
            D1Command::ManagedWrite {
                mode_id: [9; 16],
                request: updated.clone(),
            },
        )])
        .await?;
    let snapshot = source.build_snapshot().await?;
    let mut fresh = Arc::new(StateMachine::default());
    fresh
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await?;
    assert_eq!(
        fresh.d1_managed_mode(7, 4).await.unwrap().unwrap().mode_id,
        [9; 16]
    );
    let duplicate = fresh
        .apply([entry(
            7,
            D1Command::ManagedWrite {
                mode_id: [9; 16],
                request: updated,
            },
        )])
        .await?;
    assert!(matches!(
        duplicate[0].d1,
        Some(Ok(D1Result::ManagedWrite {
            sequence: 2,
            status: D1WriteStatus::Duplicate,
            ..
        }))
    ));
    Ok(())
}
