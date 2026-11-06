//! D1 application semantics, independent of the synchronous local D1 trait.
use std::io::Cursor;
use std::sync::Arc;

use d2_raft::{
    D1Command, D1Error, D1Result, D1WriteRequest, D1WriteStatus, LogCommand, StateMachine, WriteOp,
};
use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

fn request(id: u8, expected: Option<u64>) -> D1WriteRequest {
    D1WriteRequest {
        shard: 7,
        epoch: 3,
        request_id: [id; 16],
        expected_sequence: expected,
        operations: vec![
            WriteOp::put(b"\xff", [id]),
            WriteOp::put(b"remove", b"v"),
            WriteOp::delete(b"remove"),
        ],
    }
}

fn entry(index: u64, command: LogCommand) -> Entry<d2_raft::TypeConfig> {
    Entry {
        log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Normal(command),
    }
}

async fn apply(
    sm: &mut Arc<StateMachine>,
    index: u64,
    command: D1Command,
) -> Result<D1Result, D1Error> {
    let results = sm
        .apply([entry(index, LogCommand::D1V1(command))])
        .await
        .unwrap();
    assert_eq!(sm.applied().await.unwrap().index, index);
    results[0].d1.clone().unwrap()
}

async fn activate(sm: &mut Arc<StateMachine>) {
    assert_eq!(
        apply(sm, 1, D1Command::Activate { shard: 7, epoch: 3 }).await,
        Ok(D1Result::Activated { shard: 7, epoch: 3 })
    );
}

fn committed(sequence: u64, status: D1WriteStatus) -> Result<D1Result, D1Error> {
    Ok(D1Result::Write {
        sequence,
        status,
        applied_operations: 3,
    })
}

#[tokio::test]
async fn retries_conditions_and_rejections_preserve_atomic_state_across_reopen(
) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut sm = Arc::new(StateMachine::open(dir.path())?);
    activate(&mut sm).await;
    let first = request(1, Some(0));
    assert_eq!(
        apply(&mut sm, 2, D1Command::Write(first.clone())).await,
        committed(1, D1WriteStatus::Committed)
    );
    assert_eq!(
        apply(&mut sm, 3, D1Command::Write(request(2, None))).await,
        committed(2, D1WriteStatus::Committed)
    );
    assert_eq!(
        apply(&mut sm, 4, D1Command::Write(first.clone())).await,
        committed(1, D1WriteStatus::Duplicate)
    );
    let data = sm.scan_raw().await;
    let mut changed_condition = first.clone();
    changed_condition.expected_sequence = None;
    let mut changed_operations = first.clone();
    changed_operations.operations.push(WriteOp::delete(b"\xff"));
    let mut stale_epoch = first.clone();
    stale_epoch.epoch = 2;
    let mut future_epoch = first.clone();
    future_epoch.epoch = 4;
    let mut wrong_shard = first.clone();
    wrong_shard.shard = 8;
    let mut empty = request(3, None);
    empty.operations.clear();
    let rejected = [
        (changed_condition, D1Error::IdempotencyConflict),
        (changed_operations, D1Error::IdempotencyConflict),
        (
            stale_epoch,
            D1Error::Fenced {
                requested: 2,
                current: 3,
            },
        ),
        (
            future_epoch,
            D1Error::Fenced {
                requested: 4,
                current: 3,
            },
        ),
        (
            wrong_shard,
            D1Error::WrongShard {
                requested: 8,
                current: 7,
            },
        ),
        (empty, D1Error::EmptyBatch),
        (
            request(3, Some(0)),
            D1Error::Conflict {
                expected: 0,
                current: 2,
            },
        ),
        (
            request(3, Some(9)),
            D1Error::Conflict {
                expected: 9,
                current: 2,
            },
        ),
    ];
    for (offset, (request, error)) in rejected.into_iter().enumerate() {
        assert_eq!(
            apply(&mut sm, 5 + offset as u64, D1Command::Write(request)).await,
            Err(error)
        );
        assert_eq!(sm.scan_raw().await, data);
        assert_eq!(sm.d1_metadata().await, Some((7, 3, 2)));
    }
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path())?);
    assert_eq!(sm.applied().await.unwrap().index, 12);
    assert_eq!(sm.scan_raw().await, data);
    assert_eq!(
        apply(&mut sm, 13, D1Command::Write(first)).await,
        committed(1, D1WriteStatus::Duplicate)
    );
    // Rejected request 3 was not consumed, even across a restart.
    assert_eq!(
        apply(&mut sm, 14, D1Command::Write(request(3, Some(2)))).await,
        committed(3, D1WriteStatus::Committed)
    );
    assert_eq!(sm.get_raw(b"\xff").await, Some(vec![3]));
    assert_eq!(sm.get_raw(b"remove").await, None);
    Ok(())
}

#[tokio::test]
async fn epoch_fencing_is_replicated_and_never_resets_sequence_or_dedup() {
    let mut sm = Arc::new(StateMachine::default());
    activate(&mut sm).await;
    let original = request(1, Some(0));
    apply(&mut sm, 2, D1Command::Write(original.clone()))
        .await
        .unwrap();
    let advance = D1Command::SetEpoch {
        shard: 7,
        expected_epoch: 3,
        new_epoch: 4,
    };
    assert_eq!(
        apply(&mut sm, 3, advance.clone()).await,
        Ok(D1Result::Epoch { epoch: 4 })
    );
    assert_eq!(
        apply(&mut sm, 4, advance).await,
        Ok(D1Result::Epoch { epoch: 4 })
    );
    assert_eq!(
        apply(&mut sm, 5, D1Command::Write(original.clone())).await,
        Err(D1Error::Fenced {
            requested: 3,
            current: 4
        })
    );
    let mut refreshed = original;
    refreshed.epoch = 4;
    assert_eq!(
        apply(&mut sm, 6, D1Command::Write(refreshed)).await,
        committed(1, D1WriteStatus::Duplicate)
    );
    assert_eq!(
        apply(
            &mut sm,
            7,
            D1Command::SetEpoch {
                shard: 7,
                expected_epoch: 3,
                new_epoch: 5
            }
        )
        .await,
        Err(D1Error::Fenced {
            requested: 3,
            current: 4
        })
    );
    assert_eq!(
        apply(
            &mut sm,
            8,
            D1Command::SetEpoch {
                shard: 7,
                expected_epoch: 4,
                new_epoch: 3
            }
        )
        .await,
        Err(D1Error::InvalidEpoch)
    );
    assert_eq!(sm.d1_metadata().await, Some((7, 4, 1)));
}

#[tokio::test]
async fn snapshot_preserves_retry_identity_and_rejects_downgrade_and_corruption(
) -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut source = Arc::new(StateMachine::default());
    activate(&mut source).await;
    let first = request(1, Some(0));
    apply(&mut source, 2, D1Command::Write(first.clone()))
        .await
        .unwrap();
    apply(&mut source, 3, D1Command::Write(request(2, None)))
        .await
        .unwrap();
    let snapshot = source.build_snapshot().await?;
    let bytes = snapshot.snapshot.into_inner();
    let json: serde_json::Value = serde_json::from_slice(&bytes)?;
    assert_eq!(json["version"], 1);
    assert_eq!(json["d1"]["sequence"], 2);
    let mut destination = Arc::new(StateMachine::open(dir.path())?);
    let mut missing_marker = snapshot.meta.clone();
    missing_marker.last_log_id = None;
    assert!(destination
        .install_snapshot(&missing_marker, Box::new(Cursor::new(bytes.clone())))
        .await
        .is_err());
    assert_eq!(destination.d1_metadata().await, None);
    destination
        .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes.clone())))
        .await?;
    drop(destination);
    let mut destination = Arc::new(StateMachine::open(dir.path())?);
    assert_eq!(destination.scan_raw().await, source.scan_raw().await);
    assert_eq!(destination.applied().await, snapshot.meta.last_log_id);
    assert_eq!(
        apply(&mut destination, 4, D1Command::Write(first.clone())).await,
        committed(1, D1WriteStatus::Duplicate)
    );
    let before = destination.scan_raw().await;
    // Reinstalling an older, otherwise valid snapshot must not undo later applies.
    assert!(destination
        .install_snapshot(&snapshot.meta, Box::new(Cursor::new(bytes.clone())))
        .await
        .is_err());
    // Give malformed payloads a current index so format validation is exercised,
    // rather than merely the stale-snapshot guard.
    let mut current_meta = snapshot.meta.clone();
    current_meta.last_log_id = destination.applied().await;
    for field in ["version", "d1_version", "dedup", "shard", "duplicate_key"] {
        let mut broken = json.clone();
        match field {
            "version" => broken["version"] = 2.into(),
            "d1_version" => broken["d1"]["version"] = 2.into(),
            "dedup" => broken["d1"]["dedup"] = serde_json::json!([]),
            "shard" => broken["d1"]["shard"] = 8.into(),
            "duplicate_key" => {
                let key = broken["entries"][0].clone();
                broken["entries"].as_array_mut().unwrap().push(key);
            }
            _ => unreachable!(),
        }
        assert!(
            destination
                .install_snapshot(
                    &current_meta,
                    Box::new(Cursor::new(serde_json::to_vec(&broken)?))
                )
                .await
                .is_err(),
            "{field}"
        );
        assert_eq!(destination.scan_raw().await, before);
        assert_eq!(destination.applied().await.unwrap().index, 4);
    }
    assert!(destination
        .install_snapshot(&current_meta, Box::new(Cursor::new(b"[]".to_vec())))
        .await
        .is_err());
    drop(destination);
    let mut destination = Arc::new(StateMachine::open(dir.path())?);
    assert_eq!(
        apply(&mut destination, 5, D1Command::Write(first)).await,
        committed(1, D1WriteStatus::Duplicate)
    );
    assert_eq!(destination.d1_metadata().await, Some((7, 3, 2)));
    Ok(())
}

#[tokio::test]
async fn legacy_logs_snapshots_and_activation_have_explicit_boundaries() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let mut sm = Arc::new(StateMachine::open(dir.path())?);
    assert_eq!(
        apply(&mut sm, 1, D1Command::Write(request(1, None))).await,
        Err(D1Error::NotActivated)
    );
    sm.apply([entry(
        2,
        serde_json::from_str(r#"{"TxnBatch":[{"Put":{"key":[107],"value":[118]}}]}"#)?,
    )])
    .await?;
    assert_eq!(
        apply(&mut sm, 3, D1Command::Activate { shard: 7, epoch: 3 }).await,
        Err(D1Error::LegacyDataPresent)
    );
    let legacy = sm.build_snapshot().await?;
    assert_eq!(legacy.snapshot.get_ref(), b"[[[107],[118]]]");
    let mut restored = Arc::new(StateMachine::default());
    restored
        .install_snapshot(&legacy.meta, legacy.snapshot)
        .await?;
    assert_eq!(restored.get_raw(b"k").await, Some(b"v".to_vec()));
    drop(sm);
    let mut sm = Arc::new(StateMachine::open(dir.path())?);
    assert_eq!(sm.d1_metadata().await, None);
    sm.apply([entry(4, LogCommand::delete(b"k"))]).await?;
    apply(&mut sm, 5, D1Command::Activate { shard: 7, epoch: 3 })
        .await
        .unwrap();
    let results = sm
        .apply([
            entry(6, LogCommand::put(b"k", b"bypass")),
            entry(7, LogCommand::Noop),
        ])
        .await?;
    assert_eq!(results[0].d1, Some(Err(D1Error::LegacyWriteDisabled)));
    assert_eq!(results[1].d1, None);
    assert_eq!(sm.get_raw(b"k").await, None);
    assert_eq!(sm.d1_metadata().await, Some((7, 3, 0)));
    assert_eq!(
        apply(&mut sm, 8, D1Command::Activate { shard: 8, epoch: 3 }).await,
        Err(D1Error::WrongShard {
            requested: 8,
            current: 7
        })
    );
    Ok(())
}

#[test]
fn command_and_result_golden_vectors() {
    assert_eq!(
        serde_json::to_string(&LogCommand::D1V1(D1Command::Activate {
            shard: 7,
            epoch: 3
        }))
        .unwrap(),
        r#"{"D1V1":{"Activate":{"shard":7,"epoch":3}}}"#
    );
    let legacy: d2_raft::ApplyResult = serde_json::from_str(r#"{"value":null}"#).unwrap();
    assert_eq!(legacy.d1, None);
    assert_eq!(serde_json::to_string(&legacy).unwrap(), r#"{"value":null}"#);
}

#[tokio::test]
async fn mixed_apply_batch_matches_individual_log_entries() -> anyhow::Result<()> {
    let mut individual = Arc::new(StateMachine::default());
    let dir = tempfile::tempdir()?;
    let mut batched = Arc::new(StateMachine::open(dir.path())?);
    let first = request(1, Some(0));
    let mut fresh_epoch = request(2, Some(1));
    fresh_epoch.epoch = 4;
    let commands = [
        D1Command::Activate { shard: 7, epoch: 3 },
        D1Command::Write(first.clone()),
        D1Command::Write(request(2, Some(0))),
        D1Command::Write(first.clone()),
        D1Command::SetEpoch {
            shard: 7,
            expected_epoch: 3,
            new_epoch: 4,
        },
        D1Command::Write(first),
        D1Command::Write(fresh_epoch),
    ];
    let entries: Vec<_> = commands
        .into_iter()
        .enumerate()
        .map(|(i, command)| entry(i as u64 + 1, LogCommand::D1V1(command)))
        .collect();
    let batch_results = batched.apply(entries.clone()).await?;
    let mut individual_results = Vec::new();
    for entry in entries {
        individual_results.extend(individual.apply([entry]).await?);
    }
    assert_eq!(batch_results, individual_results);
    assert_eq!(batched.scan_raw().await, individual.scan_raw().await);
    assert_eq!(batched.d1_metadata().await, Some((7, 4, 2)));
    assert_eq!(batched.applied().await, individual.applied().await);
    let a = batched.build_snapshot().await?;
    let b = individual.build_snapshot().await?;
    assert_eq!(a.snapshot.get_ref(), b.snapshot.get_ref());
    drop(batched);
    let reopened = StateMachine::open(dir.path())?;
    assert_eq!(reopened.scan_raw().await, individual.scan_raw().await);
    assert_eq!(reopened.d1_metadata().await, Some((7, 4, 2)));
    Ok(())
}

#[tokio::test]
async fn activated_snapshot_golden_vector() -> anyhow::Result<()> {
    let mut sm = Arc::new(StateMachine::default());
    activate(&mut sm).await;
    let snapshot = sm.build_snapshot().await?;
    assert_eq!(snapshot.snapshot.get_ref(), br#"{"version":1,"entries":[],"d1":{"version":1,"shard":7,"epoch":3,"sequence":0,"dedup":[]}}"#);
    Ok(())
}

#[tokio::test]
async fn corrupt_durable_metadata_fails_reopen() -> anyhow::Result<()> {
    for corruption in ["version", "dedup", "marker"] {
        let dir = tempfile::tempdir()?;
        let mut sm = Arc::new(StateMachine::open(dir.path())?);
        activate(&mut sm).await;
        apply(&mut sm, 2, D1Command::Write(request(1, Some(0))))
            .await
            .unwrap();
        drop(sm);
        let db = rocksdb::DB::open_default(dir.path())?;
        if corruption == "marker" {
            db.delete(b"sm/applied")?;
        } else {
            let mut value: serde_json::Value = serde_json::from_slice(&db.get(b"sm/d1")?.unwrap())?;
            if corruption == "version" {
                value["version"] = 2.into();
            } else {
                value["dedup"] = serde_json::json!([]);
            }
            db.put(b"sm/d1", serde_json::to_vec(&value)?)?;
        }
        drop(db);
        assert!(StateMachine::open(dir.path()).is_err(), "{corruption}");
    }
    Ok(())
}
