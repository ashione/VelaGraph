use std::time::{Duration, Instant};
use velagraph::{
    CancellationToken, LocalStorageNode, StorageNode, StorageNodeError, TransactionError,
    TransactionOptions, WriteOp,
};

fn options(id: u8) -> TransactionOptions {
    TransactionOptions {
        shard: 7,
        epoch: 0,
        request_id: [id; 16],
        deadline: Instant::now() + Duration::from_secs(60),
        cancellation: CancellationToken::default(),
    }
}

#[test]
fn readers_observe_only_one_winning_batch() {
    let dir = tempfile::tempdir().unwrap();
    let local = LocalStorageNode::open(dir.path(), 7).unwrap();
    let node: &dyn StorageNode = &local;
    let before = node.read_view().unwrap();
    let mut winner = node.begin_v1(options(1)).unwrap();
    let mut stale = node.begin_v1(options(2)).unwrap();
    let mut aborted = node.begin_v1(options(3)).unwrap();
    for (tx, key) in [
        (&mut winner, b"a"),
        (&mut stale, b"s"),
        (&mut aborted, b"x"),
    ] {
        tx.stage(&WriteOp::put(key, b"first")).unwrap();
    }
    winner.stage(&WriteOp::put(b"b", b"second")).unwrap();
    winner.stage(&WriteOp::delete(b"a")).unwrap();
    aborted.abort();
    assert_eq!(node.read_view().unwrap().row_count(), 0);
    let result = winner.commit().unwrap();
    assert_eq!((result.sequence, result.applied_operations), (1, 3));
    assert!(matches!(
        stale.commit(),
        Err(TransactionError::Node(StorageNodeError::Conflict {
            expected: 0,
            current: 1
        }))
    ));
    assert_eq!(node.get(b"b", &before).unwrap(), None);
    let after = node.read_view().unwrap();
    assert_eq!((after.sequence, after.row_count()), (1, 1));
    assert_eq!(
        node.get(b"b", &after).unwrap().as_deref(),
        Some(b"second".as_slice())
    );
}

#[test]
fn failed_stage_invalidates_the_entire_prefix_for_both_limits() {
    let dir = tempfile::tempdir().unwrap();
    let local = LocalStorageNode::open(dir.path(), 7).unwrap();
    let node: &dyn StorageNode = &local;
    for byte_limit in [false, true] {
        let mut tx = node.begin_v1(options(1)).unwrap();
        if byte_limit {
            tx.stage(&WriteOp::put(b"", vec![0; 16 * 1024 * 1024]))
                .unwrap();
        } else {
            for _ in 0..10_000 {
                tx.stage(&WriteOp::put(b"", b"")).unwrap();
            }
        }
        assert!(matches!(
            tx.stage(&WriteOp::delete(b"x")),
            Err(TransactionError::Node(StorageNodeError::ResourceExhausted(
                _
            )))
        ));
        assert!(matches!(
            tx.stage(&WriteOp::delete(b"")),
            Err(TransactionError::Aborted)
        ));
        assert!(matches!(tx.commit(), Err(TransactionError::Aborted)));
        let view = node.read_view().unwrap();
        assert_eq!((view.sequence, view.row_count()), (0, 0));
    }
}

#[test]
fn cancel_deadline_and_epoch_fence_discard_staged_updates() {
    let dir = tempfile::tempdir().unwrap();
    let local = LocalStorageNode::open(dir.path(), 7).unwrap();
    let node: &dyn StorageNode = &local;
    let expired = TransactionOptions {
        deadline: Instant::now(),
        ..options(1)
    };
    assert!(matches!(
        node.begin_v1(expired),
        Err(TransactionError::Deadline)
    ));
    let opts = options(2);
    let cancel = opts.cancellation.clone();
    let mut tx = node.begin_v1(opts).unwrap();
    tx.stage(&WriteOp::put(b"a", b"cancel")).unwrap();
    cancel.cancel();
    assert!(matches!(tx.commit(), Err(TransactionError::Cancelled)));
    let mut tx = node.begin_v1(options(3)).unwrap();
    tx.stage(&WriteOp::put(b"a", b"fenced")).unwrap();
    node.admin().set_epoch(1).unwrap();
    assert!(matches!(
        tx.commit(),
        Err(TransactionError::Node(StorageNodeError::Fenced { .. }))
    ));
    let view = node.read_view().unwrap();
    assert_eq!((view.sequence, view.row_count()), (0, 0));
}

#[test]
fn process_exit_with_staged_writes_preserves_only_committed_batch() {
    const CHILD_PATH: &str = "VELAGRAPH_D1_TRANSACTION_TEST_CHILD_PATH";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let local = LocalStorageNode::open(path, 7).unwrap();
        let node: &dyn StorageNode = &local;
        let mut committed = node.begin_v1(options(1)).unwrap();
        committed.stage(&WriteOp::put(b"a", b"kept")).unwrap();
        committed.commit().unwrap();
        let mut pending = node.begin_v1(options(2)).unwrap();
        pending.stage(&WriteOp::delete(b"a")).unwrap();
        pending.stage(&WriteOp::put(b"b", b"pending")).unwrap();
        std::process::exit(42);
    }
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "process_exit_with_staged_writes_preserves_only_committed_batch",
        ])
        .env(CHILD_PATH, dir.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(42));
    let node = LocalStorageNode::open(dir.path(), 7).unwrap();
    let view = node.read_view().unwrap();
    assert_eq!((view.sequence, view.row_count()), (1, 1));
    assert_eq!(
        node.get(b"a", &view).unwrap().as_deref(),
        Some(b"kept".as_slice())
    );
}
