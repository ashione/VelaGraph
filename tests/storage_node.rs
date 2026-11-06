use rocksdb_sql_table::{
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
    assert_eq!(duplicate.applied_operations, 0);

    let conflict = node.batch_write(request(
        0,
        1,
        vec![WriteOp::put(b"a".to_vec(), b"different".to_vec())],
    ));
    assert!(matches!(
        conflict,
        Err(StorageNodeError::IdempotencyConflict(id)) if id == [1; 16]
    ));
    assert_eq!(node.read_view().unwrap().sequence, 2);
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
    assert_eq!(node.get(b"good", &view).unwrap(), None);
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
    assert_eq!(target.read_view().unwrap().sequence, 1);

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
    let export = rocksdb_sql_table::Export {
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
    assert_eq!(
        target
            .scan(
                KeyRange::all(),
                ScanLimit::default(),
                &target.read_view().unwrap()
            )
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
fn import_rejects_epoch_and_sequence_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let node = node(&dir);
    node.admin().set_epoch(2).unwrap();

    let old_epoch = rocksdb_sql_table::Export {
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
    assert_eq!(
        node.scan(
            KeyRange::all(),
            ScanLimit::default(),
            &node.read_view().unwrap()
        )
        .unwrap(),
        Vec::<KeyValue>::new()
    );

    let old_sequence = rocksdb_sql_table::Export {
        shard: 7,
        epoch: 2,
        sequence: 1,
        entries: Vec::new(),
    };
    assert!(matches!(
        node.admin().import(old_sequence),
        Err(StorageNodeError::InvalidArgument(_))
    ));
}
