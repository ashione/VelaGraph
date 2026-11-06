use std::collections::BTreeMap;

use rocksdb_sql_table::{
    KeyRange, LocalStorageNode, ReadView, ScanLimit, StorageNode, StorageNodeError, WriteOp,
    WriteRequest,
};

fn view(entries: &[(&[u8], &[u8])]) -> ReadView {
    ReadView::from_entries(
        7,
        0,
        1,
        entries
            .iter()
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect(),
    )
    .unwrap()
}

#[test]
fn pages_are_exclusive_bounded_and_exact_at_row_and_byte_boundaries() {
    let view = view(&[(b"", b""), (b"a", b"1"), (b"aa", b"2"), (b"b", b"3")]);
    for max_rows in 1..=5 {
        for max_bytes in 3..=9 {
            let limit = ScanLimit::new(max_rows, max_bytes).unwrap();
            let mut cursor = None;
            let mut all = Vec::new();
            loop {
                let page = view
                    .scan_page(KeyRange::all(), limit, cursor.as_ref())
                    .unwrap();
                assert!(page.rows.len() <= max_rows);
                assert!(
                    page.rows
                        .iter()
                        .map(|r| r.key.len() + r.value.len())
                        .sum::<usize>()
                        <= max_bytes
                );
                assert!(!page.rows.is_empty());
                all.extend(page.rows);
                cursor = page.next_cursor;
                if cursor.is_none() {
                    break;
                }
                assert!(all.len() < 4, "no empty terminal page at exact boundary");
            }
            assert_eq!(
                all,
                view.scan(KeyRange::all(), ScanLimit::default()).unwrap()
            );
        }
    }
    // The original scan remains all-or-error, never a first-page success.
    assert!(matches!(
        view.scan(KeyRange::all(), ScanLimit::new(1, 100).unwrap()),
        Err(StorageNodeError::ResourceExhausted(_))
    ));
}

#[test]
fn half_open_ranges_mixed_labels_and_empty_results() {
    let view = view(&[
        (b"out/1/A/1", b"x"),
        (b"out/1/A/2", b"x"),
        (b"out/1/B/1", b"x"),
        (b"out/2/A/1", b"x"),
    ]);
    let range = KeyRange::new(b"out/1/".to_vec(), b"out/2/".to_vec()).unwrap();
    let limit = ScanLimit::new(2, 100).unwrap();
    let first = view.scan_page(range.clone(), limit, None).unwrap();
    let second = view
        .scan_page(range, limit, first.next_cursor.as_ref())
        .unwrap();
    assert_eq!(first.rows.len(), 2);
    assert_eq!(second.rows[0].key, b"out/1/B/1");
    assert_eq!(second.rows.len(), 1);
    assert!(second.next_cursor.is_none());
    for range in [
        KeyRange::new(b"z".to_vec(), b"z".to_vec()).unwrap(),
        KeyRange {
            start: b"z".to_vec(),
            end: None,
        },
    ] {
        let page = view.scan_page(range, limit, None).unwrap();
        assert!(page.rows.is_empty());
        assert!(page.next_cursor.is_none());
    }
    let empty = ReadView::from_entries(7, 0, 0, BTreeMap::new()).unwrap();
    assert!(empty
        .scan_page(KeyRange::all(), limit, None)
        .unwrap()
        .next_cursor
        .is_none());
}

#[test]
fn oversized_rows_and_invalid_limits_never_return_nonprogressing_pages() {
    let view = view(&[(b"a", b""), (b"b", b"too large")]);
    let limit = ScanLimit::new(10, 1).unwrap();
    let first = view.scan_page(KeyRange::all(), limit, None).unwrap();
    assert_eq!(first.rows.len(), 1);
    let cursor = first.next_cursor.unwrap();
    assert!(matches!(
        view.scan_page(KeyRange::all(), limit, Some(&cursor)),
        Err(StorageNodeError::ResourceExhausted(_))
    ));
    let resumed = view
        .scan_page(KeyRange::all(), ScanLimit::default(), Some(&cursor))
        .unwrap();
    assert_eq!(resumed.rows[0].key, b"b");
    assert!(resumed.next_cursor.is_none());
    for limit in [
        ScanLimit {
            max_rows: 0,
            max_bytes: 1,
        },
        ScanLimit {
            max_rows: 1,
            max_bytes: 0,
        },
    ] {
        assert!(matches!(
            view.scan_page(KeyRange::all(), limit, None),
            Err(StorageNodeError::InvalidArgument(_))
        ));
    }
    let huge = ReadView::from_entries(
        7,
        0,
        1,
        BTreeMap::from([(b"large".to_vec(), b"row".to_vec())]),
    )
    .unwrap();
    assert!(matches!(
        huge.scan_page(KeyRange::all(), limit, None),
        Err(StorageNodeError::ResourceExhausted(_))
    ));
}

#[test]
fn cursor_binds_snapshot_range_and_metadata_but_allows_clones_and_replay() {
    let v = view(&[(b"a", b"1"), (b"b", b"2")]);
    let limit = ScanLimit::new(1, 100).unwrap();
    let cursor = v
        .scan_page(KeyRange::all(), limit, None)
        .unwrap()
        .next_cursor
        .unwrap();
    for _ in 0..2 {
        assert_eq!(
            v.clone()
                .scan_page(KeyRange::all(), limit, Some(&cursor))
                .unwrap()
                .rows[0]
                .key,
            b"b"
        );
    }
    let other = view(&[(b"a", b"1"), (b"b", b"2")]);
    assert!(matches!(
        other.scan_page(KeyRange::all(), limit, Some(&cursor)),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    assert!(matches!(
        v.scan_page(
            KeyRange::new(b"a".to_vec(), b"c".to_vec()).unwrap(),
            limit,
            Some(&cursor)
        ),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    for field in 0..3 {
        let mut changed = v.clone();
        match field {
            0 => changed.shard += 1,
            1 => changed.epoch += 1,
            _ => changed.sequence += 1,
        };
        assert!(matches!(
            changed.scan_page(KeyRange::all(), limit, Some(&cursor)),
            Err(StorageNodeError::InvalidArgument(_))
        ));
    }
}

#[test]
fn local_pages_keep_snapshot_and_check_epoch_on_every_page() {
    let dir = tempfile::tempdir().unwrap();
    let node = LocalStorageNode::open(dir.path(), 7).unwrap();
    node.batch_write(WriteRequest {
        shard: 7,
        epoch: 0,
        request_id: [1; 16],
        operations: vec![WriteOp::put(b"a", b"1"), WriteOp::put(b"b", b"2")],
    })
    .unwrap();
    let view = node.read_view().unwrap();
    let limit = ScanLimit::new(1, 100).unwrap();
    let cursor = node
        .scan_page(KeyRange::all(), limit, &view, None)
        .unwrap()
        .next_cursor
        .unwrap();
    node.batch_write(WriteRequest {
        shard: 7,
        epoch: 0,
        request_id: [2; 16],
        operations: vec![WriteOp::delete(b"b"), WriteOp::put(b"c", b"3")],
    })
    .unwrap();
    let page = node
        .scan_page(KeyRange::all(), limit, &view, Some(&cursor))
        .unwrap();
    assert_eq!(page.rows[0].key, b"b");
    assert!(page.next_cursor.is_none());
    node.admin().set_epoch(1).unwrap();
    assert!(matches!(
        node.scan_page(KeyRange::all(), limit, &view, Some(&cursor)),
        Err(StorageNodeError::Fenced { .. })
    ));
}

#[test]
fn legacy_adapter_defaults_to_unsupported_and_invalid_ranges_are_rejected() {
    use rocksdb_sql_table::{KeyValue, StorageAdmin, WriteResult};
    struct Legacy(LocalStorageNode);
    type Result<T> = std::result::Result<T, StorageNodeError>;
    impl StorageNode for Legacy {
        fn read_view(&self) -> Result<ReadView> {
            self.0.read_view()
        }
        fn get(&self, key: &[u8], view: &ReadView) -> Result<Option<Vec<u8>>> {
            self.0.get(key, view)
        }
        fn scan(
            &self,
            range: KeyRange,
            limit: ScanLimit,
            view: &ReadView,
        ) -> Result<Vec<KeyValue>> {
            self.0.scan(range, limit, view)
        }
        fn batch_write(&self, request: WriteRequest) -> Result<WriteResult> {
            self.0.batch_write(request)
        }
        fn admin(&self) -> &dyn StorageAdmin {
            self.0.admin()
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let legacy = Legacy(LocalStorageNode::open(dir.path(), 7).unwrap());
    let view = legacy.read_view().unwrap();
    assert!(matches!(
        legacy.scan_page(KeyRange::all(), ScanLimit::default(), &view, None),
        Err(StorageNodeError::Unsupported(_))
    ));
    assert!(matches!(
        view.scan_page(
            KeyRange {
                start: b"z".to_vec(),
                end: Some(b"a".to_vec())
            },
            ScanLimit::default(),
            None
        ),
        Err(StorageNodeError::InvalidArgument(_))
    ));
    let wrong_shard = ReadView::from_entries(8, 0, 0, BTreeMap::new()).unwrap();
    assert!(matches!(
        legacy
            .0
            .scan_page(KeyRange::all(), ScanLimit::default(), &wrong_shard, None),
        Err(StorageNodeError::InvalidArgument(_))
    ));
}
