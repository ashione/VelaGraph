use velagraph::{
    KeyRange, KeyValue, LocalStorageNode, ScanLimit, StorageNode, StorageNodeError, WriteOp,
    WriteRequest, WriteStatus,
};

fn node(dir: &tempfile::TempDir) -> LocalStorageNode {
    LocalStorageNode::open(dir.path().join("node"), 7).expect("open local storage node")
}

fn request(epoch: u64, id: u8, operations: Vec<WriteOp>) -> WriteRequest {
    WriteRequest {
        shard: 7,
        epoch,
        request_id: [id; 16],
        operations,
    }
}

#[test]
fn missing_key_and_empty_scan_are_not_errors() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    let view = node.read_view().unwrap();

    assert_eq!(view.shard, 7);
    assert_eq!(view.epoch, 0);
    assert_eq!(view.sequence, 0);
    assert_eq!(node.get(b"missing", &view).unwrap(), None);
    assert_eq!(
        node.scan(KeyRange::all(), ScanLimit::default(), &view)
            .unwrap(),
        Vec::<KeyValue>::new()
    );
}

#[test]
fn batches_are_atomic_idempotent_and_support_opaque_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);

    let first = node
        .batch_write(request(
            0,
            1,
            vec![
                WriteOp::put(b"a".to_vec(), vec![0, 1, 2]),
                WriteOp::delete(b"b".to_vec()),
            ],
        ))
        .unwrap();
    assert_eq!(first.status, WriteStatus::Committed);
    assert_eq!(first.sequence, 1);
    assert_eq!(first.applied_operations, 2);

    let second = node
        .batch_write(request(
            0,
            2,
            vec![
                WriteOp::put(vec![0xff, 0x00], vec![0, 1, 2]),
                WriteOp::put(Vec::new(), b"empty-key".to_vec()),
            ],
        ))
        .unwrap();
    assert_eq!(second.sequence, 2);

    let view = node.read_view().unwrap();
    assert_eq!(node.get(b"a", &view).unwrap(), Some(vec![0, 1, 2]));
    assert_eq!(node.get(b"b", &view).unwrap(), None);
    assert_eq!(node.get(&[0xff, 0x00], &view).unwrap(), Some(vec![0, 1, 2]));
    assert_eq!(node.get(b"", &view).unwrap(), Some(b"empty-key".to_vec()));

    let duplicate = node
        .batch_write(request(
            0,
            1,
            vec![
                WriteOp::put(b"a".to_vec(), vec![0, 1, 2]),
                WriteOp::delete(b"b".to_vec()),
            ],
        ))
        .unwrap();
    assert_eq!(duplicate.status, WriteStatus::Duplicate);
    assert_eq!(duplicate.sequence, 1);
    assert_eq!(duplicate.applied_operations, 2);

    let conflict = node.batch_write(request(
        0,
        1,
        vec![WriteOp::put(b"a".to_vec(), b"different".to_vec())],
    ));
    assert!(matches!(
        conflict,
        Err(StorageNodeError::IdempotencyConflict(id)) if id == [1; 16]
    ));
    let view = node.read_view().unwrap();
    assert_eq!(view.sequence, 2);
    assert_eq!(node.get(b"a", &view).unwrap(), Some(vec![0, 1, 2]));
}

#[test]
fn invalid_batches_write_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);

    assert!(matches!(
        node.batch_write(request(0, 1, Vec::new())),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    let too_many = (0..10_001)
        .map(|index| WriteOp::put(format!("k{index}").into_bytes(), b"value".to_vec()))
        .collect();
    assert!(matches!(
        node.batch_write(request(0, 2, too_many)),
        Err(StorageNodeError::ResourceExhausted(_))
    ));

    let view = node.read_view().unwrap();
    assert_eq!(view.sequence, 0);
    assert_eq!(node.get(b"k0", &view).unwrap(), None);
}

#[test]
fn scan_respects_range_and_limits() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    node.batch_write(request(
        0,
        1,
        vec![
            WriteOp::put(b"a".to_vec(), b"1".to_vec()),
            WriteOp::put(b"b".to_vec(), b"2".to_vec()),
            WriteOp::put(b"c".to_vec(), b"3".to_vec()),
        ],
    ))
    .unwrap();
    let view = node.read_view().unwrap();

    let range = KeyRange::new(b"a".to_vec(), b"c".to_vec()).unwrap();
    assert_eq!(
        node.scan(range.clone(), ScanLimit::default(), &view)
            .unwrap(),
        vec![
            KeyValue {
                key: b"a".to_vec(),
                value: b"1".to_vec()
            },
            KeyValue {
                key: b"b".to_vec(),
                value: b"2".to_vec()
            }
        ]
    );

    assert!(matches!(
        node.scan(range.clone(), ScanLimit::new(1, 1024).unwrap(), &view),
        Err(StorageNodeError::ResourceExhausted(_))
    ));
    assert!(matches!(
        node.scan(range, ScanLimit::new(100, 1).unwrap(), &view),
        Err(StorageNodeError::ResourceExhausted(_))
    ));
    let two_rows = KeyRange::new(b"a".to_vec(), b"c".to_vec()).unwrap();
    let exact_rows = node
        .scan(two_rows.clone(), ScanLimit::new(2, 1024).unwrap(), &view)
        .unwrap();
    assert_eq!(exact_rows.len(), 2);
    let exact_bytes = node
        .scan(two_rows, ScanLimit::new(100, 4).unwrap(), &view)
        .unwrap();
    assert_eq!(exact_bytes.len(), 2);
    assert_eq!(
        node.scan(
            KeyRange::new(b"b".to_vec(), b"b".to_vec()).unwrap(),
            ScanLimit::default(),
            &view
        )
        .unwrap(),
        Vec::<KeyValue>::new()
    );
    assert!(matches!(
        KeyRange::new(b"d".to_vec(), b"a".to_vec()),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    assert_eq!(
        node.scan(
            KeyRange::new(b"d".to_vec(), b"e".to_vec()).unwrap(),
            ScanLimit::default(),
            &view
        )
        .unwrap(),
        Vec::<KeyValue>::new()
    );
}

#[test]
fn read_view_is_stable_across_writes() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    let before = node.read_view().unwrap();

    node.batch_write(request(
        0,
        1,
        vec![WriteOp::put(b"k".to_vec(), b"v".to_vec())],
    ))
    .unwrap();

    assert_eq!(node.get(b"k", &before).unwrap(), None);
    assert_eq!(node.read_view().unwrap().sequence, 1);
    assert_eq!(
        node.get(b"k", &node.read_view().unwrap()).unwrap(),
        Some(b"v".to_vec())
    );
}

#[test]
fn epoch_fences_old_writers_and_old_views() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    let old_view = node.read_view().unwrap();
    node.admin().set_epoch(1).unwrap();

    assert!(matches!(
        node.batch_write(request(
            0,
            2,
            vec![WriteOp::put(b"k".to_vec(), b"v".to_vec())]
        )),
        Err(StorageNodeError::Fenced { .. })
    ));
    assert!(matches!(
        node.get(b"k", &old_view),
        Err(StorageNodeError::Fenced { .. })
    ));
    assert!(matches!(
        node.admin().set_epoch(0),
        Err(StorageNodeError::InvalidArgument(_))
    ));

    let result = node
        .batch_write(request(
            1,
            3,
            vec![WriteOp::put(b"k".to_vec(), b"v".to_vec())],
        ))
        .unwrap();
    assert_eq!(result.status, WriteStatus::Committed);
    assert_eq!(node.read_view().unwrap().epoch, 1);
}

#[test]
fn export_import_preserves_entries_epoch_and_sequence() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = node(&source_dir);
    source.admin().set_epoch(2).unwrap();
    source
        .batch_write(request(
            2,
            1,
            vec![
                WriteOp::put(b"a".to_vec(), b"1".to_vec()),
                WriteOp::put(vec![0xff], vec![1, 2, 3]),
            ],
        ))
        .unwrap();
    let export = source.admin().export().unwrap();
    assert_eq!(export.sequence, 1);
    assert_eq!(export.entries.len(), 2);

    let target_dir = tempfile::tempdir().unwrap();
    let target = node(&target_dir);
    target.admin().set_epoch(2).unwrap();
    target.admin().import(export.clone()).unwrap();
    assert_eq!(target.admin().export().unwrap(), export);
    let view = target.read_view().unwrap();
    assert_eq!(view.epoch, 2);
    assert_eq!(view.sequence, 1);

    let before = target.admin().export().unwrap();
    assert!(matches!(
        target.admin().import(export),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    assert_eq!(target.admin().export().unwrap(), before);
}

#[test]
fn failed_import_leaves_target_empty() {
    let dir = tempfile::tempdir().unwrap();
    let target = node(&dir);
    let export = velagraph::Export {
        shard: 7,
        epoch: 0,
        sequence: 1,
        entries: vec![
            KeyValue {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            },
            KeyValue {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
        ],
    };

    assert!(matches!(
        target.admin().import(export),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    let view = target.read_view().unwrap();
    assert_eq!(view.epoch, 0);
    assert_eq!(view.sequence, 0);
    assert_eq!(
        target
            .scan(KeyRange::all(), ScanLimit::default(), &view)
            .unwrap(),
        Vec::<KeyValue>::new()
    );
}

#[test]
fn shard_mismatch_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    let view = node.read_view().unwrap();

    assert!(matches!(
        node.batch_write(WriteRequest {
            shard: 8,
            epoch: 0,
            request_id: [9; 16],
            operations: vec![WriteOp::put(b"k".to_vec(), b"v".to_vec())],
        }),
        Err(StorageNodeError::InvalidArgument(_))
    ));

    let other_dir = tempfile::tempdir().unwrap();
    let other = LocalStorageNode::open(other_dir.path().join("node"), 8).unwrap();
    let other_view = other.read_view().unwrap();
    assert!(matches!(
        node.get(b"k", &other_view),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    assert!(matches!(
        node.scan(KeyRange::all(), ScanLimit::default(), &other_view),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    assert_eq!(view.sequence, 0);
}

#[test]
fn data_and_dedup_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let first = node(&dir);
    first
        .batch_write(request(
            0,
            1,
            vec![WriteOp::put(b"k".to_vec(), b"v".to_vec())],
        ))
        .unwrap();
    drop(first);

    let reopened = node(&dir);
    let view = reopened.read_view().unwrap();
    assert_eq!(view.sequence, 1);
    assert_eq!(reopened.get(b"k", &view).unwrap(), Some(b"v".to_vec()));

    let duplicate = reopened
        .batch_write(request(
            0,
            1,
            vec![WriteOp::put(b"k".to_vec(), b"v".to_vec())],
        ))
        .unwrap();
    assert_eq!(duplicate.status, WriteStatus::Duplicate);
    assert_eq!(duplicate.sequence, 1);
}

#[test]
fn import_rejects_epoch_rollback_and_non_pristine_target() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    node.admin().set_epoch(2).unwrap();

    let old_epoch = velagraph::Export {
        shard: 7,
        epoch: 1,
        sequence: 0,
        entries: Vec::new(),
    };
    assert!(matches!(
        node.admin().import(old_epoch),
        Err(StorageNodeError::InvalidArgument(_))
    ));

    node.batch_write(request(
        2,
        1,
        vec![WriteOp::put(b"temporary".to_vec(), b"value".to_vec())],
    ))
    .unwrap();
    node.batch_write(request(2, 2, vec![WriteOp::delete(b"temporary".to_vec())]))
        .unwrap();
    assert_eq!(node.read_view().unwrap().sequence, 2);

    let non_pristine = velagraph::Export {
        shard: 7,
        epoch: 2,
        sequence: 3,
        entries: vec![KeyValue {
            key: b"new".to_vec(),
            value: b"value".to_vec(),
        }],
    };
    assert!(matches!(
        node.admin().import(non_pristine),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    assert_eq!(node.read_view().unwrap().sequence, 2);
}

#[test]
fn empty_but_advanced_export_imports() {
    let source_dir = tempfile::tempdir().unwrap();
    let source = node(&source_dir);
    source
        .batch_write(request(
            0,
            1,
            vec![WriteOp::put(b"temporary".to_vec(), b"value".to_vec())],
        ))
        .unwrap();
    source
        .batch_write(request(0, 2, vec![WriteOp::delete(b"temporary".to_vec())]))
        .unwrap();
    let export = source.admin().export().unwrap();
    assert_eq!(export.sequence, 2);
    assert!(export.entries.is_empty());

    let target_dir = tempfile::tempdir().unwrap();
    let target = node(&target_dir);
    target.admin().import(export.clone()).unwrap();
    assert_eq!(target.admin().export().unwrap(), export);
    let view = target.read_view().unwrap();
    assert_eq!(view.sequence, 2);
    assert_eq!(node_get(&target, b"temporary", &view), None);
}

#[test]
fn reopening_with_a_different_shard_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let first = node(&dir);
    drop(first);

    let reopened = LocalStorageNode::open(dir.path().join("node"), 8);
    assert!(matches!(reopened, Err(StorageNodeError::Corrupt(_))));
}

fn node_get(node: &LocalStorageNode, key: &[u8], view: &velagraph::ReadView) -> Option<Vec<u8>> {
    node.get(key, view).unwrap()
}

#[test]
fn conditional_batches_conflict_atomically_and_can_be_retried() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    let initial = request(0, 1, vec![WriteOp::put(b"a", b"old")]);
    node.batch_write(initial).unwrap();
    let batch = request(
        0,
        2,
        vec![WriteOp::delete(b"a"), WriteOp::put(b"b", b"new")],
    );
    let before = node.admin().export().unwrap();
    for expected in [0, 2, u64::MAX] {
        assert!(matches!(
            node.batch_write_conditional(batch.clone(), Some(expected)),
            Err(StorageNodeError::Conflict { expected: e, current: 1 }) if e == expected
        ));
        assert_eq!(node.admin().export().unwrap(), before);
    }
    // Failed conditions did not consume the request id or advance sequence.
    let result = node.batch_write_conditional(batch, Some(1)).unwrap();
    assert_eq!(result.status, WriteStatus::Committed);
    assert_eq!(result.sequence, 2);
    assert_eq!(result.applied_operations, 2);
    let view = node.read_view().unwrap();
    assert_eq!(node.get(b"a", &view).unwrap(), None);
    assert_eq!(node.get(b"b", &view).unwrap(), Some(b"new".to_vec()));
}

#[test]
fn conditional_dedup_precedes_sequence_check_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let first = node(&dir);
    let batch = request(0, 1, vec![WriteOp::put(b"a", b"first")]);
    first
        .batch_write_conditional(batch.clone(), Some(0))
        .unwrap();
    first
        .batch_write(request(0, 2, vec![WriteOp::put(b"a", b"later")]))
        .unwrap();
    drop(first);
    let node = node(&dir);
    let before = node.admin().export().unwrap();
    let result = node
        .batch_write_conditional(batch.clone(), Some(0))
        .unwrap();
    assert_eq!(result.status, WriteStatus::Duplicate);
    assert_eq!(result.sequence, 1);
    assert_eq!(result.applied_operations, 1);
    assert_eq!(node.admin().export().unwrap(), before);
    // The condition is part of request identity, including Some versus None.
    for expected in [Some(1), Some(2), None] {
        assert!(matches!(
            node.batch_write_conditional(batch.clone(), expected),
            Err(StorageNodeError::IdempotencyConflict(_))
        ));
        assert_eq!(node.admin().export().unwrap(), before);
    }
    let different = request(0, 1, vec![WriteOp::delete(b"a")]);
    assert!(matches!(
        node.batch_write_conditional(different, Some(0)),
        Err(StorageNodeError::IdempotencyConflict(_))
    ));
    // Ownership fencing remains ahead of deduplication, even for exact retries.
    node.admin().set_epoch(1).unwrap();
    assert!(matches!(
        node.batch_write_conditional(batch, Some(0)),
        Err(StorageNodeError::Fenced {
            epoch: 0,
            current: 1,
            ..
        })
    ));
    assert_eq!(node.read_view().unwrap().sequence, 2);
}

#[test]
fn unconditional_and_conditional_request_identity_are_distinct() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    let batch = request(0, 1, vec![WriteOp::put(b"a", b"value")]);
    node.batch_write(batch.clone()).unwrap();
    assert_eq!(
        node.batch_write_conditional(batch.clone(), None)
            .unwrap()
            .status,
        WriteStatus::Duplicate
    );
    assert!(matches!(
        node.batch_write_conditional(batch, Some(0)),
        Err(StorageNodeError::IdempotencyConflict(_))
    ));
    assert_eq!(node.read_view().unwrap().sequence, 1);
}

#[test]
fn legacy_dedup_metadata_without_condition_remains_readable() {
    let dir = tempfile::tempdir().unwrap();
    // Fixture uses the pre-conditional on-disk JSON schema verbatim.
    let first = node(&dir);
    drop(first);
    let batch = request(0, 1, vec![WriteOp::put(b"k", b"v")]);
    let canonical = serde_json::to_vec(&batch.operations).unwrap();
    let old_record = serde_json::to_vec(&serde_json::json!({
        "operations": canonical,
        "sequence": 1,
        "applied_operations": 1,
    }))
    .unwrap();
    {
        let db = rocksdb::DB::open_cf(
            &rocksdb::Options::default(),
            dir.path().join("node"),
            ["data", "meta"],
        )
        .unwrap();
        let mut key = b"dedup/".to_vec();
        key.extend_from_slice(&[1; 16]);
        let mut write = rocksdb::WriteBatch::default();
        write.put_cf(db.cf_handle("data").unwrap(), b"k", b"v");
        write.put_cf(
            db.cf_handle("meta").unwrap(),
            b"sequence",
            1_u64.to_be_bytes(),
        );
        write.put_cf(db.cf_handle("meta").unwrap(), key, old_record);
        db.write(write).unwrap();
    }
    let node = node(&dir);
    let result = node.batch_write(batch.clone()).unwrap();
    assert_eq!(result.status, WriteStatus::Duplicate);
    assert_eq!(result.sequence, 1);
    assert!(matches!(
        node.batch_write_conditional(batch, Some(0)),
        Err(StorageNodeError::IdempotencyConflict(_))
    ));
    assert_eq!(node.read_view().unwrap().sequence, 1);
}

#[test]
fn conditional_sequence_overflow_and_invalid_requests_write_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    let before = node.admin().export().unwrap();
    assert!(matches!(
        node.batch_write_conditional(request(0, 1, vec![]), Some(0)),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    let mut wrong_shard = request(0, 2, vec![WriteOp::put(b"k", b"v")]);
    wrong_shard.shard = 8;
    assert!(matches!(
        node.batch_write_conditional(wrong_shard, Some(0)),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    assert_eq!(node.admin().export().unwrap(), before);
    node.admin()
        .import(velagraph::Export {
            shard: 7,
            epoch: 0,
            sequence: u64::MAX,
            entries: vec![],
        })
        .unwrap();
    let before = node.admin().export().unwrap();
    assert!(matches!(
        node.batch_write_conditional(
            request(0, 3, vec![WriteOp::put(b"k", b"v")]),
            Some(u64::MAX)
        ),
        Err(StorageNodeError::ResourceExhausted(_))
    ));
    assert_eq!(node.admin().export().unwrap(), before);
}

#[test]
fn simultaneous_conditional_writes_have_one_atomic_winner() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    for round in 0..16_u8 {
        let expected = node.read_view().unwrap().sequence;
        let gate = std::sync::Barrier::new(2);
        let results = std::thread::scope(|scope| {
            let run = |id| {
                gate.wait();
                node.batch_write_conditional(
                    request(0, id, vec![WriteOp::put(b"key", vec![id])]),
                    Some(expected),
                )
            };
            let first = scope.spawn(move || run(round * 2));
            let second = scope.spawn(move || run(round * 2 + 1));
            [first.join().unwrap(), second.join().unwrap()]
        });
        let mut winner = None;
        let mut conflicts = 0;
        for (index, result) in results.into_iter().enumerate() {
            match result {
                Ok(result) => {
                    assert_eq!(result.status, WriteStatus::Committed);
                    assert_eq!(result.sequence, expected + 1);
                    assert!(winner.replace(round * 2 + index as u8).is_none());
                }
                Err(StorageNodeError::Conflict {
                    expected: seen,
                    current,
                }) => {
                    assert_eq!(seen, expected);
                    assert_eq!(current, expected + 1);
                    conflicts += 1;
                }
                result => panic!("unexpected conditional outcome: {result:?}"),
            }
        }
        assert_eq!(conflicts, 1);
        let view = node.read_view().unwrap();
        assert_eq!(view.sequence, expected + 1);
        assert_eq!(
            node.get(b"key", &view).unwrap(),
            Some(vec![winner.unwrap()])
        );
    }
}
