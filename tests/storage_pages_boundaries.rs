use std::collections::BTreeMap;
use velagraph::{
    KeyRange, LocalStorageNode, ReadView, ScanLimit, StorageNode, StorageNodeError, WriteOp,
    WriteRequest,
};

#[test]
fn exhaustive_binary_ranges_budgets_and_variable_page_limits() {
    let keys = vec![
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![127],
        vec![128],
        vec![255],
        vec![255, 0],
        vec![255, 255],
    ];
    let entries: BTreeMap<_, _> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| (k.clone(), vec![i as u8; i % 4]))
        .collect();
    let view = ReadView::from_entries(7, 0, 1, entries.clone()).unwrap();
    let mut endpoints = keys.clone();
    endpoints.push(vec![255, 255, 255]);
    for start in &endpoints {
        for end in endpoints.iter().map(Some).chain(std::iter::once(None)) {
            if end.is_some_and(|e| e < start) {
                continue;
            }
            let range = KeyRange {
                start: start.clone(),
                end: end.cloned(),
            };
            let expected: Vec<_> = entries
                .iter()
                .filter(|(k, _)| *k >= start && end.is_none_or(|e| *k < e))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            for row_budget in 1..=4 {
                for bytes_budget in 1..=9 {
                    let mut cursor = None;
                    let mut observed = Vec::new();
                    for iteration in 0..=expected.len() {
                        let bytes = bytes_budget + iteration % 3;
                        let limit = ScanLimit::new(row_budget, bytes).unwrap();
                        let page = match view.scan_page(range.clone(), limit, cursor.as_ref()) {
                            Ok(p) => p,
                            Err(StorageNodeError::ResourceExhausted(_)) => {
                                let (k, v) = &expected[observed.len()];
                                assert!(k.len() + v.len() > bytes);
                                view.scan_page(
                                    range.clone(),
                                    ScanLimit::new(row_budget, usize::MAX).unwrap(),
                                    cursor.as_ref(),
                                )
                                .unwrap()
                            }
                            Err(e) => panic!("unexpected error: {e}"),
                        };
                        assert!(page.rows.len() <= row_budget);
                        assert!(page.next_cursor.is_none() || !page.rows.is_empty());
                        observed.extend(page.rows.into_iter().map(|r| (r.key, r.value)));
                        cursor = page.next_cursor;
                        if cursor.is_none() {
                            break;
                        }
                    }
                    assert!(cursor.is_none(), "failed to terminate");
                    assert_eq!(observed, expected);
                }
            }
        }
    }
}

#[test]
fn cloned_snapshot_and_cursor_are_send_sync_and_replay_concurrently() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<ReadView>();
    assert_send_sync::<velagraph::ScanCursor>();
    let view =
        ReadView::from_entries(7, 0, 1, (0u8..100).map(|k| (vec![k], vec![k])).collect()).unwrap();
    let cloned = view.clone();
    let cursor = view
        .scan_page(KeyRange::all(), ScanLimit::new(1, 2).unwrap(), None)
        .unwrap()
        .next_cursor
        .unwrap();
    drop(view);
    std::thread::scope(|scope| {
        for _ in 0..8 {
            let v = &cloned;
            let c = &cursor;
            scope.spawn(move || {
                let page = v
                    .scan_page(
                        KeyRange::all(),
                        ScanLimit::new(usize::MAX, usize::MAX).unwrap(),
                        Some(c),
                    )
                    .unwrap();
                assert_eq!(page.rows.len(), 99);
                assert_eq!(page.rows.first().unwrap().key, vec![1]);
                assert_eq!(page.rows.last().unwrap().key, vec![99]);
                assert!(page.next_cursor.is_none());
            });
        }
    });
}

#[test]
fn local_real_concurrent_writes_do_not_change_pages() {
    let dir = tempfile::tempdir().unwrap();
    let node = LocalStorageNode::open(dir.path(), 7).unwrap();
    node.batch_write(WriteRequest {
        shard: 7,
        epoch: 0,
        request_id: [1; 16],
        operations: (0u8..100).map(|k| WriteOp::put([k], [k])).collect(),
    })
    .unwrap();
    let view = node.read_view().unwrap();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            for k in 0u8..100 {
                node.batch_write(WriteRequest {
                    shard: 7,
                    epoch: 0,
                    request_id: [k.wrapping_add(2); 16],
                    operations: vec![WriteOp::delete([k])],
                })
                .unwrap();
            }
        });
        let mut cursor = None;
        let mut all = Vec::new();
        loop {
            let page = node
                .scan_page(
                    KeyRange::all(),
                    ScanLimit::new(1, 2).unwrap(),
                    &view,
                    cursor.as_ref(),
                )
                .unwrap();
            all.extend(page.rows.into_iter().map(|r| r.key[0]));
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(all, (0u8..100).collect::<Vec<_>>());
    });
}
