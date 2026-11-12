//! Contract tests for the opt-in, append-only graph/v2 experiment.
use std::{collections::BTreeMap, sync::Arc};
use velagraph::{
    experimental_graph::{AppendOnlyStore, CrossEdge, Direction, ExperimentalError, VertexRef},
    Edge, GraphRecordStore, KeyRange, KeyValue, LocalStorageNode, ReadView, ScanLimit,
    StorageAdmin, StorageNode, StorageNodeError, Value, Vertex, WriteOp, WriteRequest, WriteResult,
};

#[derive(Clone)]
struct Shared(Arc<LocalStorageNode>);
type StorageResult<T> = Result<T, StorageNodeError>;
impl StorageNode for Shared {
    fn read_view(&self) -> StorageResult<ReadView> {
        self.0.read_view()
    }
    fn get(&self, key: &[u8], view: &ReadView) -> StorageResult<Option<Vec<u8>>> {
        self.0.get(key, view)
    }
    fn scan(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
    ) -> StorageResult<Vec<KeyValue>> {
        self.0.scan(range, limit, view)
    }
    fn batch_write(&self, request: WriteRequest) -> StorageResult<WriteResult> {
        self.0.batch_write(request)
    }
    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        sequence: Option<u64>,
    ) -> StorageResult<WriteResult> {
        self.0.batch_write_conditional(request, sequence)
    }
    fn admin(&self) -> &dyn StorageAdmin {
        self.0.admin()
    }
}
fn node(path: &std::path::Path) -> Shared {
    Shared(Arc::new(LocalStorageNode::open(path, 7).unwrap()))
}
fn edge(from: u32, to: u32) -> CrossEdge {
    CrossEdge {
        id: 7,
        from: VertexRef {
            partition: from,
            id: 1,
        },
        to: VertexRef {
            partition: to,
            id: 2,
        },
        label: "K".into(),
        properties: BTreeMap::new(),
    }
}
fn vertex(store: &AppendOnlyStore<Shared>, node: &Shared, id: u64) {
    store
        .prepare_vertex(&Vertex::new(id))
        .unwrap()
        .submit(node)
        .unwrap();
}
fn raw(node: &Shared, operations: Vec<WriteOp>) {
    node.batch_write(WriteRequest {
        shard: 7,
        epoch: 0,
        request_id: [99; 16],
        operations,
    })
    .unwrap();
}
fn hex(s: &str) -> Vec<u8> {
    s.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|p| u8::from_str_radix(std::str::from_utf8(p).unwrap(), 16).unwrap())
        .collect()
}
fn index(id: u64, label: &[u8]) -> Vec<u8> {
    let mut k = hex("67726170682f76322f702f000000012f6f75742f00000000000000012f");
    k.extend(label);
    k.push(b'/');
    k.extend(1u32.to_be_bytes());
    k.extend(id.to_be_bytes());
    k
}

#[test]
fn persisted_keys_and_envelopes_match_golden_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 0x01020304);
    let b = AppendOnlyStore::new(n.clone(), 7, 0x05060708);
    let mut v = Vertex::new(1);
    v.labels.push("A".into());
    v.properties.insert("x".into(), Value::Integer(1));
    a.prepare_vertex(&v).unwrap().submit(&n).unwrap();
    vertex(&b, &n, 2);
    let mut e = edge(0x01020304, 0x05060708);
    e.properties.insert("x".into(), Value::Boolean(true));
    a.prepare_source(&e)
        .unwrap()
        .combine(b.prepare_reverse(&e).unwrap())
        .unwrap()
        .submit(&n)
        .unwrap();
    let view = n.read_view().unwrap();
    assert_eq!(
        n.get(
            &hex("67726170682f76322f702f010203042f7665727465782f0000000000000001"),
            &view
        )
        .unwrap(),
        Some(hex(
            "080210011d040302012214080110011a0e08011201411a070a017812021002"
        ))
    );
    assert_eq!(
        n.get(
            &hex("67726170682f76322f702f010203042f656467652f0000000000000007"),
            &view
        )
        .unwrap(),
        Some(hex(
            "080210021d080706052218080110021a1208071001180222014b2a070a017812022801"
        ))
    );
    for key in [
        "67726170682f76322f702f010203042f6f75742f00000000000000012f4b2f010203040000000000000007",
        "67726170682f76322f702f050607082f696e2f00000000000000022f4b2f010203040000000000000007",
    ] {
        assert_eq!(n.get(&hex(key), &view).unwrap(), Some(vec![]));
    }
}

#[test]
fn combined_batch_reopens_and_exact_retries_do_not_advance_sequence() {
    let dir = tempfile::tempdir().unwrap();
    let retry;
    {
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        let b = AppendOnlyStore::new(n.clone(), 7, 2);
        vertex(&a, &n, 1);
        vertex(&b, &n, 2);
        let e = edge(1, 2);
        retry = a
            .prepare_source(&e)
            .unwrap()
            .combine(b.prepare_reverse(&e).unwrap())
            .unwrap();
        let before = n.read_view().unwrap().sequence;
        retry.submit(&n).unwrap();
        retry.clone().submit(&n).unwrap();
        assert_eq!(n.read_view().unwrap().sequence, before + 1);
        assert!(matches!(
            a.prepare_vertex(&Vertex::new(1)),
            Err(ExperimentalError::AlreadyExists)
        ));
        assert!(matches!(
            a.prepare_source(&e),
            Err(ExperimentalError::AlreadyExists)
        ));
        assert!(matches!(
            b.prepare_reverse(&e),
            Err(ExperimentalError::AlreadyExists)
        ));
    }
    let n = node(dir.path());
    let sequence = n.read_view().unwrap().sequence;
    retry.submit(&n).unwrap();
    assert_eq!(n.read_view().unwrap().sequence, sequence);
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    assert_eq!(a.get_edge(7).unwrap(), Some(edge(1, 2)));
    b.verify_reverse(&edge(1, 2)).unwrap();
    assert_eq!(
        a.observed_index(1, Direction::Out).unwrap()[0].edge,
        edge(1, 2).identity()
    );
    assert_eq!(
        b.observed_index(2, Direction::In).unwrap()[0].edge,
        edge(1, 2).identity()
    );
}

#[test]
fn source_only_commit_is_visible_but_reverse_verification_is_incomplete() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    vertex(&a, &n, 1);
    vertex(&b, &n, 2);
    let e = edge(1, 2);
    a.prepare_source(&e).unwrap().submit(&n).unwrap();
    assert_eq!(a.get_edge(7).unwrap(), Some(e.clone()));
    assert!(b.observed_index(2, Direction::In).unwrap().is_empty());
    assert!(
        matches!(b.verify_reverse(&e),Err(ExperimentalError::Incomplete(r)) if r==e.identity())
    );
}

#[test]
fn missing_local_endpoint_and_preparation_do_not_write() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    let e = edge(1, 2);
    let before = n.admin().export().unwrap();
    assert!(matches!(a.prepare_source(&e),Err(ExperimentalError::MissingEndpoint(v)) if v==e.from));
    assert!(matches!(b.prepare_reverse(&e),Err(ExperimentalError::MissingEndpoint(v)) if v==e.to));
    a.prepare_vertex(&Vertex::new(1)).unwrap();
    let after = n.admin().export().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.sequence, after.sequence);
}

#[test]
fn v1_reads_and_cascade_deletion_are_isolated_from_v2() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let v1 = GraphRecordStore::new(n.clone(), 7, 1);
    let v2 = AppendOnlyStore::new(n.clone(), 7, 1);
    for id in [1, 2] {
        vertex(&v2, &n, id);
        assert_eq!(v1.get_vertex(id).unwrap(), None);
        v1.create_vertex(&Vertex::new(id)).unwrap();
    }
    let e = edge(1, 1);
    v2.prepare_source(&e)
        .unwrap()
        .combine(v2.prepare_reverse(&e).unwrap())
        .unwrap()
        .submit(&n)
        .unwrap();
    assert_eq!(v1.get_edge(7).unwrap(), None);
    v1.create_edge(&Edge::new(7, 1, 2, "K")).unwrap();
    let before = n
        .admin()
        .export()
        .unwrap()
        .entries
        .into_iter()
        .filter(|r| r.key.starts_with(b"graph/v2/"))
        .collect::<Vec<_>>();
    v1.delete_vertex(1).unwrap();
    assert_eq!(v1.get_edge(7).unwrap(), None);
    assert_eq!(v2.get_edge(7).unwrap(), Some(e.clone()));
    v2.verify_reverse(&e).unwrap();
    let after = n
        .admin()
        .export()
        .unwrap()
        .entries
        .into_iter()
        .filter(|r| r.key.starts_with(b"graph/v2/"))
        .collect::<Vec<_>>();
    assert_eq!(before, after);
}

#[test]
fn malformed_index_entries_fail_closed() {
    for (key, value) in [
        (index(7, b"K"), vec![1]),
        (index(7, &[255]), vec![]),
        (index(7, b"K")[..30].to_vec(), vec![]),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        vertex(&a, &n, 1);
        raw(&n, vec![WriteOp::put(key, value)]);
        assert!(matches!(
            a.observed_index(1, Direction::Out),
            Err(ExperimentalError::Corrupt(_))
        ));
    }
}

#[test]
fn index_row_limit_is_error_not_partial_success() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    vertex(&a, &n, 1);
    raw(
        &n,
        (0..1001)
            .map(|id| WriteOp::put(index(id, b"K"), vec![]))
            .collect(),
    );
    assert!(matches!(
        a.observed_index(1, Direction::Out),
        Err(ExperimentalError::Storage(
            StorageNodeError::ResourceExhausted(_)
        ))
    ));
}

#[test]
fn index_byte_limit_is_error_not_partial_success() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    vertex(&a, &n, 1);
    raw(&n, vec![WriteOp::put(index(7, b"K"), vec![0; 1024 * 1024])]);
    assert!(matches!(
        a.observed_index(1, Direction::Out),
        Err(ExperimentalError::Storage(
            StorageNodeError::ResourceExhausted(_)
        ))
    ));
}

#[test]
fn stale_preparation_cannot_overwrite_an_immutable_vertex() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let original = Vertex::new(1);
    let mut changed = original.clone();
    changed.labels.push("Changed".into());
    let first = a.prepare_vertex(&original).unwrap();
    let second = a.prepare_vertex(&changed).unwrap();
    first.submit(&n).unwrap();
    let before = n.admin().export().unwrap();
    assert!(matches!(
        second.submit(&n),
        Err(ExperimentalError::Storage(_))
    ));
    let after = n.admin().export().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.sequence, after.sequence);
    assert_eq!(a.get_vertex(1).unwrap(), Some(original));
}

#[test]
fn combination_rejects_different_validation_sequences_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let first = a.prepare_vertex(&Vertex::new(1)).unwrap();
    vertex(&a, &n, 2);
    let second = a.prepare_vertex(&Vertex::new(3)).unwrap();
    let before = n.admin().export().unwrap();
    assert!(matches!(
        first.combine(second),
        Err(ExperimentalError::Invalid(_))
    ));
    let after = n.admin().export().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.sequence, after.sequence);
}

#[test]
fn exactly_one_thousand_index_entries_are_complete_and_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    vertex(&a, &n, 1);
    raw(
        &n,
        (0..1000)
            .map(|id| WriteOp::put(index(id, if id % 2 == 0 { b"Z" } else { b"A" }), vec![]))
            .collect(),
    );
    let observed = a.observed_index(1, Direction::Out).unwrap();
    assert_eq!(observed.len(), 1000);
    assert_eq!(
        observed.iter().map(|r| r.edge.id).collect::<Vec<_>>(),
        (0..1000).collect::<Vec<_>>()
    );
}

#[test]
fn malformed_or_wrong_identity_vertex_envelopes_fail_closed() {
    let valid = hex("080210011d010000002208080110011a020801");
    let mut wrong_version = valid.clone();
    wrong_version[1] = 1;
    let mut wrong_kind = valid.clone();
    wrong_kind[3] = 2;
    let mut wrong_partition = valid.clone();
    wrong_partition[5] = 2;
    let mut trailing = valid.clone();
    trailing.push(0);
    let mut wrong_id = valid.clone();
    *wrong_id.last_mut().unwrap() = 2;
    let mut noncanonical_length = valid.clone();
    noncanonical_length.splice(10..11, [0x88, 0]);
    for bytes in [
        vec![],
        valid[..10].to_vec(),
        wrong_version,
        wrong_kind,
        wrong_partition,
        trailing,
        wrong_id,
        noncanonical_length,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        raw(
            &n,
            vec![WriteOp::put(
                hex("67726170682f76322f702f000000012f7665727465782f0000000000000001"),
                bytes,
            )],
        );
        assert!(a.get_vertex(1).is_err());
    }
}

#[test]
fn combination_rejects_conflicting_payloads_for_the_same_edge_key() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    vertex(&a, &n, 1);
    let original = edge(1, 2);
    let mut changed = original.clone();
    changed.to.partition = 3;
    changed.label = "Changed".into();
    changed.properties.insert("x".into(), Value::Integer(42));
    let first = a.prepare_source(&original).unwrap();
    let second = a.prepare_source(&changed).unwrap();
    let before = n.admin().export().unwrap();
    assert!(matches!(
        first.combine(second),
        Err(ExperimentalError::Invalid(_))
    ));
    let after = n.admin().export().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.sequence, after.sequence);
    assert_eq!(a.get_edge(7).unwrap(), None);
    assert!(a.observed_index(1, Direction::Out).unwrap().is_empty());
}

#[test]
fn combination_rejects_even_identical_duplicate_keys() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let first = a.prepare_vertex(&Vertex::new(1)).unwrap();
    assert!(matches!(
        first.clone().combine(first),
        Err(ExperimentalError::Invalid(_))
    ));
    assert_eq!(a.get_vertex(1).unwrap(), None);
}

#[test]
fn noncanonical_index_labels_and_foreign_outgoing_owners_fail_closed() {
    let mut foreign_owner = index(7, b"K");
    let n = foreign_owner.len();
    foreign_owner[n - 12..n - 8].copy_from_slice(&2u32.to_be_bytes());
    for key in [
        index(7, b""),
        index(7, b"bad/name"),
        index(7, b"1bad"),
        index(7, b"bad-name"),
        index(7, &[b'A'; 65]),
        foreign_owner,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        vertex(&a, &n, 1);
        raw(&n, vec![WriteOp::put(key, vec![])]);
        assert!(a.observed_index(1, Direction::Out).is_err());
    }
}

/// Inject a lost acknowledgement after the real conditional write committed.
/// Recording complete requests ensures retry identity covers bytes and condition.
struct CommitThenTimeout {
    node: Shared,
    calls: std::sync::Mutex<Vec<(WriteRequest, Option<u64>)>>,
}
impl StorageNode for CommitThenTimeout {
    fn read_view(&self) -> StorageResult<ReadView> {
        self.node.read_view()
    }
    fn get(&self, key: &[u8], view: &ReadView) -> StorageResult<Option<Vec<u8>>> {
        self.node.get(key, view)
    }
    fn scan(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
    ) -> StorageResult<Vec<KeyValue>> {
        self.node.scan(range, limit, view)
    }
    fn batch_write(&self, request: WriteRequest) -> StorageResult<WriteResult> {
        self.node.batch_write(request)
    }
    fn batch_write_conditional(
        &self,
        request: WriteRequest,
        sequence: Option<u64>,
    ) -> StorageResult<WriteResult> {
        let request_id = request.request_id;
        let mut calls = self.calls.lock().unwrap();
        calls.push((request.clone(), sequence));
        let result = self.node.batch_write_conditional(request, sequence)?;
        if calls.len() == 1 {
            Err(StorageNodeError::Timeout {
                request_id: Some(request_id),
            })
        } else {
            Ok(result)
        }
    }
    fn admin(&self) -> &dyn StorageAdmin {
        self.node.admin()
    }
}

#[test]
fn timeout_after_commit_retries_identical_request_and_sequence_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    vertex(&a, &n, 1);
    vertex(&b, &n, 2);
    let e = edge(1, 2);
    let prepared = a
        .prepare_source(&e)
        .unwrap()
        .combine(b.prepare_reverse(&e).unwrap())
        .unwrap();
    let transport = CommitThenTimeout {
        node: n.clone(),
        calls: std::sync::Mutex::new(vec![]),
    };
    assert!(
        matches!(prepared.submit(&transport), Err(ExperimentalError::Storage(StorageNodeError::Timeout { request_id: Some(id) })) if id == prepared.request_id())
    );
    let committed = n.admin().export().unwrap();
    assert_eq!(committed.sequence, prepared.sequence() + 1);
    prepared.clone().submit(&transport).unwrap();
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
    assert_eq!(calls[0].0.request_id, prepared.request_id());
    assert_eq!(calls[0].1, Some(prepared.sequence()));
    let retried = n.admin().export().unwrap();
    assert_eq!(committed.entries, retried.entries);
    assert_eq!(committed.sequence, retried.sequence);
    assert_eq!(a.get_edge(7).unwrap(), Some(e.clone()));
    b.verify_reverse(&e).unwrap();
    assert_eq!(a.observed_index(1, Direction::Out).unwrap().len(), 1);
    assert_eq!(b.observed_index(2, Direction::In).unwrap().len(), 1);
}

#[test]
fn audit_same_numeric_edge_ids_remain_partition_qualified() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    let c = AppendOnlyStore::new(n.clone(), 7, 3);
    vertex(&a, &n, 1);
    vertex(&b, &n, 1);
    vertex(&c, &n, 2);
    for (store, e) in [(&a, edge(1, 3)), (&b, edge(2, 3))] {
        store
            .prepare_source(&e)
            .unwrap()
            .combine(c.prepare_reverse(&e).unwrap())
            .unwrap()
            .submit(&n)
            .unwrap();
    }
    let refs = c.observed_index(2, Direction::In).unwrap();
    assert_eq!(refs.len(), 2);
    assert_ne!(refs[0].edge, refs[1].edge);
    assert_eq!(a.get_edge(7).unwrap(), Some(edge(1, 3)));
    assert_eq!(b.get_edge(7).unwrap(), Some(edge(2, 3)));
}

#[test]
fn audit_combined_same_shard_source_reverse_fail_atomically_on_stale_view() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    vertex(&a, &n, 1);
    vertex(&b, &n, 2);
    let e = edge(1, 2);
    let batch = a
        .prepare_source(&e)
        .unwrap()
        .combine(b.prepare_reverse(&e).unwrap())
        .unwrap();
    vertex(&a, &n, 99);
    assert!(batch.submit(&n).is_err());
    assert_eq!(a.get_edge(7).unwrap(), None);
    assert!(a.observed_index(1, Direction::Out).unwrap().is_empty());
    assert!(b.observed_index(2, Direction::In).unwrap().is_empty());
}

#[test]
fn vertex_pages_are_bounded_ordered_isolated_and_reopenable() {
    let dir = tempfile::tempdir().unwrap();
    {
        let n = node(dir.path());
        let store = AppendOnlyStore::new(n.clone(), 7, 1);
        for id in [u64::MAX, 300, 0, 1, 128, 129] {
            vertex(&store, &n, id);
        }
        vertex(&AppendOnlyStore::new(n.clone(), 7, 2), &n, 8);
        // This decorator intentionally lacks paged scans. No fallback to scan.
        assert!(matches!(
            store.vertex_page(None, ScanLimit::new(1, 1024).unwrap()),
            Err(ExperimentalError::Storage(StorageNodeError::Unsupported(_)))
        ));
    }
    let store = AppendOnlyStore::new(LocalStorageNode::open(dir.path(), 7).unwrap(), 7, 1);
    let mut after = None;
    let mut ids = vec![];
    loop {
        let page = store
            .vertex_page(after, ScanLimit::new(2, 1024).unwrap())
            .unwrap();
        assert!(page.vertices.len() <= 2);
        assert!(page.raw_bytes <= 1024);
        ids.extend(page.vertices.into_iter().map(|v| v.id));
        after = page.next_after;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(ids, vec![0, 1, 128, 129, 300, u64::MAX]);
    assert!(store
        .vertex_page(Some(u64::MAX), ScanLimit::new(1, 1024).unwrap())
        .unwrap()
        .vertices
        .is_empty());
    assert!(matches!(
        store.vertex_page(None, ScanLimit::new(1, 1).unwrap()),
        Err(ExperimentalError::Storage(
            StorageNodeError::ResourceExhausted(_)
        ))
    ));
    assert!(matches!(
        store.vertex_page(None, ScanLimit::new(129, 1024).unwrap()),
        Err(ExperimentalError::Invalid(_))
    ));
    assert!(matches!(
        store.vertex_page(None, ScanLimit::new(1, 1024 * 1024 + 65).unwrap()),
        Err(ExperimentalError::Invalid(_))
    ));
}

#[test]
fn vertex_pages_reject_corrupt_keys_envelopes_and_identity_without_partial_success() {
    let prefix = hex("67726170682f76322f702f000000012f7665727465782f");
    for case in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        {
            let n = node(dir.path());
            let store = AppendOnlyStore::new(n.clone(), 7, 1);
            vertex(&store, &n, 1);
            let mut key = prefix.clone();
            key.extend(2u64.to_be_bytes());
            let mut value = n
                .get(
                    &[prefix.clone(), 1u64.to_be_bytes().to_vec()].concat(),
                    &n.read_view().unwrap(),
                )
                .unwrap()
                .unwrap();
            match case {
                0 => key.push(0),
                1 => value[5] = 2, // wrong partition
                2 => value[0] = 0, // corrupt envelope
                4 => {
                    // malformed suffix of the preceding page's exact last key
                    key = prefix.clone();
                    key.extend(1u64.to_be_bytes());
                    key.push(0);
                }
                _ => {} // record id 1 under key id 2
            }
            raw(&n, vec![WriteOp::Put { key, value }]);
        }
        let store = AppendOnlyStore::new(LocalStorageNode::open(dir.path(), 7).unwrap(), 7, 1);
        let first = store
            .vertex_page(None, ScanLimit::new(1, 1024).unwrap())
            .unwrap();
        assert_eq!(first.vertices[0].id, 1);
        assert_eq!(first.next_after, Some(1));
        assert!(matches!(
            store.vertex_page(first.next_after, ScanLimit::new(128, 1024).unwrap()),
            Err(ExperimentalError::Corrupt(_))
        ));
        assert!(matches!(
            store.vertex_page(None, ScanLimit::new(128, 1024).unwrap()),
            Err(ExperimentalError::Corrupt(_))
        ));
    }
}

#[derive(Clone)]
struct EdgePaged {
    node: Shared,
    fault: u8,
}
impl StorageNode for EdgePaged {
    fn read_view(&self) -> StorageResult<ReadView> {
        self.node.read_view()
    }
    fn get(&self, key: &[u8], view: &ReadView) -> StorageResult<Option<Vec<u8>>> {
        self.node.get(key, view)
    }
    fn scan(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
    ) -> StorageResult<Vec<KeyValue>> {
        self.node.scan(range, limit, view)
    }
    fn scan_page(
        &self,
        range: KeyRange,
        limit: ScanLimit,
        view: &ReadView,
        cursor: Option<&velagraph::ScanCursor>,
    ) -> StorageResult<velagraph::ScanPage> {
        let limit = if self.fault == 4 {
            ScanLimit::new(3, 4096)?
        } else {
            limit
        };
        let mut page = self.node.0.scan_page(range.clone(), limit, view, cursor)?;
        if self.fault == 6 {
            let other = self.node.read_view()?;
            page.next_cursor = self
                .node
                .0
                .scan_page(range, limit, &other, None)?
                .next_cursor;
        } else if self.fault == 7 {
            let mut other = range;
            other.start.push(0);
            page.next_cursor = self.node.0.scan_page(other, limit, view, None)?.next_cursor;
        }
        match self.fault {
            1 => page.rows.reverse(),
            2 => page.rows.clear(),
            3 => {
                page.rows.pop();
            }
            5 => {
                if let Some(row) = page.rows.first_mut() {
                    row.value.resize(4096, 0);
                }
            }
            _ => {}
        }
        Ok(page)
    }
    fn batch_write(&self, request: WriteRequest) -> StorageResult<WriteResult> {
        self.node.batch_write(request)
    }
    fn admin(&self) -> &dyn StorageAdmin {
        self.node.admin()
    }
}
#[test]
fn edge_pages_validate_bounds_order_continuation_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        vertex(&a, &n, 1);
        vertex(&a, &n, 2);
        for id in [u64::MAX, 300, 0, 1, 128, 129] {
            let mut e = edge(1, 1);
            e.id = id;
            a.prepare_source(&e)
                .unwrap()
                .combine(a.prepare_reverse(&e).unwrap())
                .unwrap()
                .submit(&n)
                .unwrap();
        }
        assert!(matches!(
            a.edge_page(None, ScanLimit::new(1, 1024).unwrap()),
            Err(ExperimentalError::Storage(StorageNodeError::Unsupported(_)))
        ));
        for fault in 1..=7 {
            let store = AppendOnlyStore::new(
                EdgePaged {
                    node: n.clone(),
                    fault,
                },
                7,
                1,
            );
            assert!(
                matches!(
                    store.edge_page(None, ScanLimit::new(2, 1024).unwrap()),
                    Err(ExperimentalError::Corrupt(_))
                ),
                "fault {fault}"
            );
        }
    }
    let a = AppendOnlyStore::new(LocalStorageNode::open(dir.path(), 7).unwrap(), 7, 1);
    let mut after = None;
    let mut ids = vec![];
    loop {
        let page = a
            .edge_page(after, ScanLimit::new(2, 1024).unwrap())
            .unwrap();
        assert!(page.edges.len() <= 2 && page.raw_bytes <= 1024);
        ids.extend(page.edges.iter().map(|e| e.id));
        after = page.next_after;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(ids, [0, 1, 128, 129, 300, u64::MAX]);
    assert!(a
        .edge_page(Some(u64::MAX), ScanLimit::new(1, 1024).unwrap())
        .unwrap()
        .edges
        .is_empty());
    assert!(matches!(
        a.edge_page(None, ScanLimit::new(1, 1).unwrap()),
        Err(ExperimentalError::Storage(
            StorageNodeError::ResourceExhausted(_)
        ))
    ));
    for limit in [
        ScanLimit {
            max_rows: 0,
            max_bytes: 100,
        },
        ScanLimit {
            max_rows: 1,
            max_bytes: 0,
        },
        ScanLimit::new(129, 1024).unwrap(),
        ScanLimit::new(1, 1024 * 1024 + 65).unwrap(),
    ] {
        assert!(matches!(
            a.edge_page(None, limit),
            Err(ExperimentalError::Invalid(_))
        ));
    }
}
#[test]
fn edge_pages_reject_malformed_keys_envelopes_and_identity() {
    let prefix = hex("67726170682f76322f702f000000012f656467652f");
    for case in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        {
            let n = node(dir.path());
            let a = AppendOnlyStore::new(n.clone(), 7, 1);
            vertex(&a, &n, 1);
            let mut e = edge(1, 2);
            e.id = 1;
            a.prepare_source(&e).unwrap().submit(&n).unwrap();
            let old_key = [prefix.clone(), 1u64.to_be_bytes().to_vec()].concat();
            let mut key = [prefix.clone(), 2u64.to_be_bytes().to_vec()].concat();
            let mut value = n.get(&old_key, &n.read_view().unwrap()).unwrap().unwrap();
            match case {
                0 => key.push(0),
                1 => value[3] = 1,
                2 => value[0] = 0,
                4 => {
                    key = old_key;
                    key.push(0);
                }
                _ => {}
            }
            raw(&n, vec![WriteOp::Put { key, value }]);
        }
        let a = AppendOnlyStore::new(LocalStorageNode::open(dir.path(), 7).unwrap(), 7, 1);
        let first = a.edge_page(None, ScanLimit::new(1, 1024).unwrap()).unwrap();
        assert_eq!(first.next_after, Some(1));
        assert!(a
            .edge_page(first.next_after, ScanLimit::new(128, 1024).unwrap())
            .is_err());
        assert!(a
            .edge_page(None, ScanLimit::new(128, 1024).unwrap())
            .is_err());
    }
}

fn property_patch(value: Value) -> velagraph::experimental_graph::EdgePropertyPatch {
    velagraph::experimental_graph::EdgePropertyPatch {
        set: BTreeMap::from([("x".into(), value)]),
        ..Default::default()
    }
}

#[test]
fn edge_properties_reopen_exact_types_and_old_retries_preserve_new_values() {
    use velagraph::experimental_graph::EdgePropertyPatch;
    let dir = tempfile::tempdir().unwrap();
    let final_edge;
    let create;
    let update;
    {
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        let b = AppendOnlyStore::new(n.clone(), 7, 2);
        vertex(&a, &n, 1);
        vertex(&b, &n, 2);
        let mut e = edge(1, 2);
        e.properties.insert("remove".into(), Value::Boolean(true));
        create = a
            .prepare_source(&e)
            .unwrap()
            .combine(b.prepare_reverse(&e).unwrap())
            .unwrap();
        create.submit(&n).unwrap();
        let before = n.admin().export().unwrap();
        let patch = EdgePropertyPatch {
            set: BTreeMap::from([
                ("null".into(), Value::Null),
                ("int".into(), Value::Integer(i64::MAX)),
                ("min".into(), Value::Integer(i64::MIN)),
                ("real".into(), Value::Real(-0.0)),
                ("text".into(), Value::Text("精确".into())),
                ("bool".into(), Value::Boolean(false)),
            ]),
            remove: ["remove".into(), "absent".into()].into(),
        };
        update = a.prepare_source_properties(&e, &patch).unwrap();
        update.submit(&n).unwrap();
        let after = n.admin().export().unwrap();
        assert_eq!(after.sequence, before.sequence + 1);
        assert_eq!(before.entries.len(), after.entries.len());
        assert_eq!(
            before
                .entries
                .iter()
                .zip(&after.entries)
                .filter(|(a, b)| a != b)
                .count(),
            1
        ); // canonical Put only
        let updated = a.get_edge(7).unwrap().unwrap();
        assert_eq!(
            (&updated.from, &updated.to, &updated.label),
            (&e.from, &e.to, &e.label)
        );
        assert_eq!(updated.properties, patch.set);
        assert_eq!(
            match updated.properties["real"] {
                Value::Real(v) => v.to_bits(),
                _ => panic!(),
            },
            (-0.0f64).to_bits()
        );
        b.verify_reverse(&updated).unwrap();
        let next = a
            .prepare_source_properties(&updated, &property_patch(Value::Integer(2)))
            .unwrap();
        next.submit(&n).unwrap();
        final_edge = a.get_edge(7).unwrap().unwrap();
        create.submit(&n).unwrap();
        update.submit(&n).unwrap();
        assert_eq!(a.get_edge(7).unwrap(), Some(final_edge.clone()));
        assert_eq!(n.read_view().unwrap().sequence, after.sequence + 1);
    }
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    create.submit(&n).unwrap();
    update.submit(&n).unwrap();
    assert_eq!(a.get_edge(7).unwrap(), Some(final_edge));
}

#[test]
fn edge_property_validation_and_racing_cas_fail_without_writes() {
    use velagraph::experimental_graph::EdgePropertyPatch;
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    vertex(&a, &n, 1);
    vertex(&b, &n, 2);
    let e = edge(1, 2);
    a.prepare_source(&e)
        .unwrap()
        .combine(b.prepare_reverse(&e).unwrap())
        .unwrap()
        .submit(&n)
        .unwrap();
    let before = n.admin().export().unwrap();
    for patch in [
        EdgePropertyPatch::default(),
        EdgePropertyPatch {
            remove: ["x".into()].into(),
            ..property_patch(Value::Null)
        },
        property_patch(Value::Real(f64::NAN)),
        property_patch(Value::Real(f64::INFINITY)),
        property_patch(Value::Text("x".repeat(1024 * 1024))),
    ] {
        assert!(a.prepare_source_properties(&e, &patch).is_err());
    }
    let after = n.admin().export().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.sequence, after.sequence);
    let one = a
        .prepare_source_properties(&e, &property_patch(Value::Integer(1)))
        .unwrap();
    let two = a
        .prepare_source_properties(&e, &property_patch(Value::Integer(2)))
        .unwrap();
    one.submit(&n).unwrap();
    assert!(matches!(
        two.submit(&n),
        Err(ExperimentalError::Storage(
            StorageNodeError::Conflict { .. }
        ))
    ));
    // Explicitly stale expected source bytes cannot silently overwrite a newer record.
    assert!(matches!(
        a.prepare_source_properties(&e, &property_patch(Value::Null)),
        Err(ExperimentalError::Incomplete(_))
    ));
    let current = a.get_edge(7).unwrap().unwrap();
    let stale = a
        .prepare_source_properties(&current, &property_patch(Value::Null))
        .unwrap();
    vertex(&a, &n, 42); // unrelated same-shard writes conservatively invalidate CAS
    let before = n.admin().export().unwrap();
    assert!(matches!(
        stale.submit(&n),
        Err(ExperimentalError::Storage(
            StorageNodeError::Conflict { .. }
        ))
    ));
    assert_eq!(before.entries, n.admin().export().unwrap().entries);
}

#[test]
fn edge_property_lost_ack_dedups_and_changed_requests_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    vertex(&a, &n, 1);
    vertex(&b, &n, 2);
    let e = edge(1, 2);
    a.prepare_source(&e)
        .unwrap()
        .combine(b.prepare_reverse(&e).unwrap())
        .unwrap()
        .submit(&n)
        .unwrap();
    let prepared = a
        .prepare_source_properties(&e, &property_patch(Value::Null))
        .unwrap();
    let transport = CommitThenTimeout {
        node: n.clone(),
        calls: std::sync::Mutex::new(vec![]),
    };
    assert!(matches!(
        prepared.submit(&transport),
        Err(ExperimentalError::Storage(StorageNodeError::Timeout { .. }))
    ));
    let committed = n.admin().export().unwrap();
    prepared.submit(&transport).unwrap();
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls[0], calls[1]);
    assert_eq!(calls[0].0.operations.len(), 1);
    let (request, condition) = calls[0].clone();
    assert!(matches!(
        n.batch_write_conditional(request.clone(), condition.map(|n| n + 1)),
        Err(StorageNodeError::IdempotencyConflict(_))
    ));
    let mut changed = request;
    changed.operations = vec![WriteOp::put(b"changed".to_vec(), vec![])];
    assert!(matches!(
        n.batch_write_conditional(changed, condition),
        Err(StorageNodeError::IdempotencyConflict(_))
    ));
    assert_eq!(committed.entries, n.admin().export().unwrap().entries);
    assert_eq!(committed.sequence, n.read_view().unwrap().sequence);
}

#[test]
fn edge_property_prepared_source_is_fenced_by_invalidating_writes() {
    for key in [
        hex("67726170682f76322f702f000000012f7665727465782f0000000000000001"),
        index(7, b"K"),
        hex("67726170682f76322f702f000000012f656467652f0000000000000007"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        vertex(&a, &n, 1);
        let e = edge(1, 2);
        a.prepare_source(&e).unwrap().submit(&n).unwrap();
        let prepared = a
            .prepare_source_properties(&e, &property_patch(Value::Null))
            .unwrap();
        raw(&n, vec![WriteOp::Delete { key }]);
        let before = n.admin().export().unwrap();
        assert!(matches!(
            prepared.submit(&n),
            Err(ExperimentalError::Storage(
                StorageNodeError::Conflict { .. }
            ))
        ));
        assert!(a
            .prepare_source_properties(&e, &property_patch(Value::Null))
            .is_err());
        assert_eq!(before.entries, n.admin().export().unwrap().entries);
    }
}

#[test]
fn edge_property_prepared_bytes_are_immutable_and_epoch_fencing_precedes_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    vertex(&a, &n, 1);
    let mut e = edge(1, 2);
    e.properties.insert("x".into(), Value::Real(-0.0));
    a.prepare_source(&e).unwrap().submit(&n).unwrap();
    let mut wrong_zero = e.clone();
    wrong_zero.properties.insert("x".into(), Value::Real(0.0));
    assert!(matches!(
        a.prepare_source_properties(&wrong_zero, &property_patch(Value::Null)),
        Err(ExperimentalError::Incomplete(_))
    ));
    let mut patch = property_patch(Value::Integer(7));
    let prepared = a.prepare_source_properties(&e, &patch).unwrap();
    patch.set.insert("x".into(), Value::Integer(8));
    e.properties.insert("x".into(), Value::Integer(9));
    prepared.submit(&n).unwrap();
    assert_eq!(
        a.get_edge(7).unwrap().unwrap().properties["x"],
        Value::Integer(7)
    );
    let before = n.admin().export().unwrap();
    n.admin().set_epoch(1).unwrap();
    assert!(matches!(
        prepared.submit(&n),
        Err(ExperimentalError::Storage(StorageNodeError::Fenced { .. }))
    ));
    let after = n.admin().export().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.sequence, after.sequence);
}

#[test]
fn deleted_edges_reopen_reserve_identity_and_ignore_historical_retries() {
    use velagraph::experimental_graph::EdgeRecord;
    let dir = tempfile::tempdir().unwrap();
    let (create, update, deletion, expected);
    {
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        let b = AppendOnlyStore::new(n.clone(), 7, 2);
        vertex(&a, &n, 1);
        vertex(&b, &n, 2);
        let e = edge(1, 2);
        create = a
            .prepare_source(&e)
            .unwrap()
            .combine(b.prepare_reverse(&e).unwrap())
            .unwrap();
        create.submit(&n).unwrap();
        update = a
            .prepare_source_properties(&e, &property_patch(Value::Integer(42)))
            .unwrap();
        update.submit(&n).unwrap();
        expected = a.get_edge(7).unwrap().unwrap();
        deletion = a
            .prepare_source_delete(&expected)
            .unwrap()
            .combine(b.prepare_reverse_delete(&expected).unwrap())
            .unwrap();
        let before = n.read_view().unwrap().sequence;
        deletion.submit(&n).unwrap();
        for retry in [&create, &update, &deletion] {
            retry.submit(&n).unwrap();
        }
        assert_eq!(n.read_view().unwrap().sequence, before + 1);
        assert!(a.get_edge(7).unwrap().is_none());
        assert!(a.observed_index(1, Direction::Out).unwrap().is_empty());
        assert!(b.observed_index(2, Direction::In).unwrap().is_empty());
        match a.get_edge_record(7).unwrap().unwrap() {
            EdgeRecord::Deleted(tombstone) => {
                assert_eq!(tombstone.edge.identity(), expected.identity());
                assert_eq!(tombstone.edge.to, expected.to);
                assert_eq!(tombstone.edge.label, expected.label);
                assert!(tombstone.edge.properties.is_empty());
                assert_eq!(tombstone.operation_id, deletion.request_id());
            }
            other => panic!("expected permanent tombstone, got {other:?}"),
        }
        assert!(matches!(a.prepare_source_delete(&expected),
            Err(ExperimentalError::NotFound(id)) if id == expected.identity()));
    }
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    let before = n.read_view().unwrap().sequence;
    for retry in [&create, &update, &deletion] {
        retry.submit(&n).unwrap();
    }
    assert_eq!(n.read_view().unwrap().sequence, before);
    assert!(a.get_edge(7).unwrap().is_none());
    assert!(b.observed_index(2, Direction::In).unwrap().is_empty());
    let mut reused = expected.clone();
    reused.label = "Different".into();
    assert!(matches!(
        a.prepare_source(&reused),
        Err(ExperimentalError::AlreadyExists)
    ));
}

#[test]
fn deletion_cas_races_updates_and_rejects_residual_out_index() {
    for update_wins in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        vertex(&a, &n, 1);
        vertex(&a, &n, 2);
        let e = edge(1, 1);
        a.prepare_source(&e)
            .unwrap()
            .combine(a.prepare_reverse(&e).unwrap())
            .unwrap()
            .submit(&n)
            .unwrap();
        let deletion = a
            .prepare_source_delete(&e)
            .unwrap()
            .combine(a.prepare_reverse_delete(&e).unwrap())
            .unwrap();
        let update = a
            .prepare_source_properties(&e, &property_patch(Value::Integer(1)))
            .unwrap();
        let (winner, loser) = if update_wins {
            (&update, &deletion)
        } else {
            (&deletion, &update)
        };
        winner.submit(&n).unwrap();
        let before = n.read_view().unwrap().sequence;
        assert!(matches!(
            loser.submit(&n),
            Err(ExperimentalError::Storage(
                StorageNodeError::Conflict { .. }
            ))
        ));
        assert_eq!(n.read_view().unwrap().sequence, before);
        if update_wins {
            assert_eq!(
                a.get_edge(7).unwrap().unwrap().properties["x"],
                Value::Integer(1)
            );
        } else {
            assert!(a.get_edge(7).unwrap().is_none());
            raw(
                &n,
                vec![WriteOp::Put {
                    key: index(7, b"K"),
                    value: vec![],
                }],
            );
            assert!(
                matches!(a.get_edge(7), Err(ExperimentalError::Incomplete(id)) if id == e.identity())
            );
            assert!(
                matches!(a.get_edge_record(7), Err(ExperimentalError::Incomplete(id)) if id == e.identity())
            );
        }
    }
}

#[test]
fn record_pages_include_tombstones_and_charge_their_raw_bytes() {
    use velagraph::experimental_graph::EdgeRecord;
    let dir = tempfile::tempdir().unwrap();
    {
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        vertex(&a, &n, 1);
        vertex(&a, &n, 2);
        for id in 0..5 {
            let mut e = edge(1, 1);
            e.id = id;
            a.prepare_source(&e)
                .unwrap()
                .combine(a.prepare_reverse(&e).unwrap())
                .unwrap()
                .submit(&n)
                .unwrap();
            if id < 4 {
                a.prepare_source_delete(&e)
                    .unwrap()
                    .combine(a.prepare_reverse_delete(&e).unwrap())
                    .unwrap()
                    .submit(&n)
                    .unwrap();
            }
        }
    }
    let a = AppendOnlyStore::new(LocalStorageNode::open(dir.path(), 7).unwrap(), 7, 1);
    let mut after = None;
    let mut records = vec![];
    loop {
        let page = a
            .edge_record_page(after, ScanLimit::new(2, 1024).unwrap())
            .unwrap();
        assert!(page.records.len() <= 2);
        assert!(page.raw_bytes > 0 && page.raw_bytes <= 1024);
        records.extend(page.records);
        after = page.next_after;
        if after.is_none() {
            break;
        }
    }
    assert_eq!(records.len(), 5);
    for (id, record) in records.into_iter().enumerate() {
        match record {
            EdgeRecord::Deleted(t) => {
                assert!(id < 4);
                assert_eq!(t.edge.id, id as u64);
            }
            EdgeRecord::Live(e) => {
                assert_eq!(id, 4);
                assert_eq!(e.id, 4);
            }
        }
    }
    assert!(a
        .edge_record_page(None, ScanLimit::new(1, 1).unwrap())
        .is_err());
}

#[test]
fn tombstone_envelope_matches_golden_and_rejects_invalid_variants() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    vertex(&a, &n, 1);
    vertex(&a, &n, 2);
    let e = edge(1, 1);
    a.prepare_source(&e)
        .unwrap()
        .combine(a.prepare_reverse(&e).unwrap())
        .unwrap()
        .submit(&n)
        .unwrap();
    let deletion = a
        .prepare_source_delete(&e)
        .unwrap()
        .combine(a.prepare_reverse_delete(&e).unwrap())
        .unwrap();
    deletion.submit(&n).unwrap();
    let key = hex("67726170682f76322f702f000000012f656467652f0000000000000007");
    let mut golden = hex("080310031d01000000220f080110021a0908071001180222014b2a10");
    golden.extend(deletion.request_id());
    assert_eq!(
        n.get(&key, &n.read_view().unwrap()).unwrap(),
        Some(golden.clone())
    );
    // Old live-only page callers must fail, never silently omit tombstones.
    let paged = AppendOnlyStore::new(
        EdgePaged {
            node: n.clone(),
            fault: 0,
        },
        7,
        1,
    );
    assert!(matches!(
        paged.edge_page(None, ScanLimit::new(10, 1024).unwrap()),
        Err(ExperimentalError::Incomplete(_))
    ));
    for fault in 0..6 {
        let mut bytes = golden.clone();
        match fault {
            0 => {
                bytes.pop();
            } // truncated operation ID
            1 => bytes[1] = 2, // old version with deletion kind
            2 => bytes[3] = 2, // deletion version with live kind
            3 => {
                let i = bytes.len() - 17;
                bytes[i] = 15;
            } // wrong ID length
            4 => {
                bytes.extend([42, 0]);
            } // extra field
            5 => bytes[18] = 8, // canonical payload ID disagrees with key
            _ => unreachable!(),
        }
        n.batch_write(WriteRequest {
            shard: 7,
            epoch: 0,
            request_id: [150 + fault as u8; 16],
            operations: vec![WriteOp::Put {
                key: key.clone(),
                value: bytes,
            }],
        })
        .unwrap();
        assert!(
            a.get_edge_record(7).is_err(),
            "accepted malformed tombstone {fault}"
        );
        assert!(
            paged
                .edge_record_page(None, ScanLimit::new(10, 1024).unwrap())
                .is_err(),
            "page accepted malformed tombstone {fault}"
        );
    }
}

#[test]
fn delete_source_and_reverse_lost_ack_retry_exactly_and_conflict_on_change() {
    let source_dir = tempfile::tempdir().unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let source = node(source_dir.path());
    let target = node(target_dir.path());
    let a = AppendOnlyStore::new(source.clone(), 7, 1);
    let b = AppendOnlyStore::new(target.clone(), 7, 2);
    vertex(&a, &source, 1);
    vertex(&b, &target, 2);
    let e = edge(1, 2);
    a.prepare_source(&e).unwrap().submit(&source).unwrap();
    b.prepare_reverse(&e).unwrap().submit(&target).unwrap();
    let deletion = a.prepare_source_delete(&e).unwrap();
    let reverse = b.prepare_reverse_delete(&e).unwrap();
    let competing = a.prepare_source_delete(&e).unwrap();
    assert_ne!(competing.request_id(), deletion.request_id());
    for (prepared, n, operation_count) in [(&deletion, &source, 2), (&reverse, &target, 1)] {
        let transport = CommitThenTimeout {
            node: n.clone(),
            calls: std::sync::Mutex::new(vec![]),
        };
        assert!(matches!(prepared.submit(&transport),
            Err(ExperimentalError::Storage(StorageNodeError::Timeout { request_id: Some(id) })) if id == prepared.request_id()));
        let committed = n.admin().export().unwrap();
        prepared.submit(&transport).unwrap();
        let calls = transport.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], calls[1]);
        assert_eq!(calls[0].0.operations.len(), operation_count);
        let (request, sequence) = calls[0].clone();
        assert!(matches!(
            n.batch_write_conditional(request.clone(), sequence.map(|s| s + 1)),
            Err(StorageNodeError::IdempotencyConflict(_))
        ));
        let mut changed = request;
        changed.operations = vec![WriteOp::put(b"changed".to_vec(), vec![])];
        assert!(matches!(
            n.batch_write_conditional(changed, sequence),
            Err(StorageNodeError::IdempotencyConflict(_))
        ));
        let after = n.admin().export().unwrap();
        assert_eq!(committed.entries, after.entries);
        assert_eq!(committed.sequence, after.sequence);
    }
    let before = source.admin().export().unwrap();
    assert!(matches!(
        competing.submit(&source),
        Err(ExperimentalError::Storage(
            StorageNodeError::Conflict { .. }
        ))
    ));
    assert_eq!(before.entries, source.admin().export().unwrap().entries);
    assert_eq!(before.sequence, source.read_view().unwrap().sequence);
    assert!(a.get_edge(7).unwrap().is_none());
    assert!(b.observed_index(2, Direction::In).unwrap().is_empty());
}

#[test]
fn independent_delete_preserves_other_source_same_id_and_unsubmitted_old_cas() {
    let d = tempfile::tempdir().unwrap();
    let n = node(d.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    let c = AppendOnlyStore::new(n.clone(), 7, 3);
    vertex(&a, &n, 1);
    vertex(&b, &n, 2);
    vertex(&c, &n, 1);
    let ea = edge(1, 2);
    let ec = edge(3, 2);
    a.prepare_source(&ea)
        .unwrap()
        .combine(b.prepare_reverse(&ea).unwrap())
        .unwrap()
        .submit(&n)
        .unwrap();
    c.prepare_source(&ec)
        .unwrap()
        .combine(b.prepare_reverse(&ec).unwrap())
        .unwrap()
        .submit(&n)
        .unwrap();
    let pending = a
        .prepare_source_properties(&ea, &property_patch(Value::Integer(9)))
        .unwrap();
    let deleted = a
        .prepare_source_delete(&ea)
        .unwrap()
        .combine(b.prepare_reverse_delete(&ea).unwrap())
        .unwrap();
    deleted.submit(&n).unwrap();
    assert!(pending.submit(&n).is_err());
    assert!(a.get_edge(7).unwrap().is_none());
    assert_eq!(c.get_edge(7).unwrap(), Some(ec.clone()));
    b.verify_reverse(&ec).unwrap();
    let incoming = b.observed_index(2, Direction::In).unwrap();
    assert_eq!(incoming.len(), 1);
    assert_eq!(incoming[0].edge, ec.identity());
    match a.get_edge_record(7).unwrap().unwrap() {
        velagraph::experimental_graph::EdgeRecord::Deleted(t) => {
            b.verify_deleted_reverse(&t).unwrap()
        }
        _ => panic!(),
    }
}

#[test]
fn independent_delete_commit_then_timeout_retry_is_historical() {
    let d = tempfile::tempdir().unwrap();
    let n = node(d.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    vertex(&a, &n, 1);
    vertex(&b, &n, 2);
    let e = edge(1, 2);
    let create = a
        .prepare_source(&e)
        .unwrap()
        .combine(b.prepare_reverse(&e).unwrap())
        .unwrap();
    create.submit(&n).unwrap();
    let deletion = a
        .prepare_source_delete(&e)
        .unwrap()
        .combine(b.prepare_reverse_delete(&e).unwrap())
        .unwrap();
    let transport = CommitThenTimeout {
        node: n.clone(),
        calls: std::sync::Mutex::new(vec![]),
    };
    assert!(
        matches!(deletion.submit(&transport),Err(ExperimentalError::Storage(StorageNodeError::Timeout{request_id:Some(id)})) if id==deletion.request_id())
    );
    assert!(a.get_edge(7).unwrap().is_none());
    assert!(b.observed_index(2, Direction::In).unwrap().is_empty());
    vertex(&a, &n, 100);
    let before = n.admin().export().unwrap();
    deletion.submit(&transport).unwrap();
    create.submit(&n).unwrap();
    let after = n.admin().export().unwrap();
    assert_eq!(before.entries, after.entries);
    assert_eq!(before.sequence, after.sequence);
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[0], calls[1]);
}

#[test]
fn simultaneous_update_delete_cas_has_one_winner_and_consistent_indexes() {
    use std::sync::Barrier;
    use velagraph::experimental_graph::EdgeRecord;

    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let b = AppendOnlyStore::new(n.clone(), 7, 2);
    vertex(&a, &n, 1);
    vertex(&b, &n, 2);
    // Repeat with distinct canonical identities, without requiring the scheduler
    // to choose a particular winner or relying on sleeps for synchronization.
    for id in 100..116 {
        let mut e = edge(1, 2);
        e.id = id;
        a.prepare_source(&e)
            .unwrap()
            .combine(b.prepare_reverse(&e).unwrap())
            .unwrap()
            .submit(&n)
            .unwrap();
        let update = a
            .prepare_source_properties(&e, &property_patch(Value::Integer(42)))
            .unwrap();
        let deletion = a
            .prepare_source_delete(&e)
            .unwrap()
            .combine(b.prepare_reverse_delete(&e).unwrap())
            .unwrap();
        assert_eq!(update.sequence(), deletion.sequence());
        let before = n.read_view().unwrap().sequence;
        let barrier = Barrier::new(3);
        let (updated, deleted) = std::thread::scope(|scope| {
            let update_thread = scope.spawn(|| {
                barrier.wait();
                update.submit(&n)
            });
            let delete_thread = scope.spawn(|| {
                barrier.wait();
                deletion.submit(&n)
            });
            barrier.wait();
            (update_thread.join().unwrap(), delete_thread.join().unwrap())
        });
        assert_eq!(n.read_view().unwrap().sequence, before + 1);
        let update_won = match (updated, deleted) {
            (Ok(()), Err(ExperimentalError::Storage(StorageNodeError::Conflict { .. }))) => true,
            (Err(ExperimentalError::Storage(StorageNodeError::Conflict { .. })), Ok(())) => false,
            other => panic!("expected exactly one CAS winner and one conflict: {other:?}"),
        };
        let outgoing = a.observed_index(1, Direction::Out).unwrap();
        let incoming = b.observed_index(2, Direction::In).unwrap();
        let out_count = outgoing
            .iter()
            .filter(|row| row.edge == e.identity())
            .count();
        let in_count = incoming
            .iter()
            .filter(|row| row.edge == e.identity())
            .count();
        if update_won {
            let mut expected = e.clone();
            expected.properties.insert("x".into(), Value::Integer(42));
            assert_eq!(a.get_edge(id).unwrap(), Some(expected.clone()));
            assert!(
                matches!(a.get_edge_record(id).unwrap(), Some(EdgeRecord::Live(actual)) if actual == expected)
            );
            b.verify_reverse(&expected).unwrap();
            assert_eq!((out_count, in_count), (1, 1));
        } else {
            assert!(a.get_edge(id).unwrap().is_none());
            match a.get_edge_record(id).unwrap().unwrap() {
                EdgeRecord::Deleted(tombstone) => {
                    assert_eq!(tombstone.operation_id, deletion.request_id());
                    b.verify_deleted_reverse(&tombstone).unwrap();
                }
                other => panic!("delete winner must leave a tombstone: {other:?}"),
            }
            assert_eq!((out_count, in_count), (0, 0));
        }
    }
}

fn vertex_property_patch(value: Value) -> velagraph::experimental_graph::VertexPropertyPatch {
    velagraph::experimental_graph::VertexPropertyPatch {
        set: BTreeMap::from([("x".into(), value)]),
        ..Default::default()
    }
}

#[test]
fn vertex_properties_preserve_identity_labels_types_snapshots_and_reopen_retries() {
    use velagraph::experimental_graph::VertexPropertyPatch;
    let dir = tempfile::tempdir().unwrap();
    let identity = VertexRef {
        partition: 1,
        id: 1,
    };
    let key = hex("67726170682f76322f702f000000012f7665727465782f0000000000000001");
    let create;
    let update;
    let final_vertex;
    {
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        let b = AppendOnlyStore::new(n.clone(), 7, 2);
        let mut original = Vertex::new(1);
        original.labels = vec!["Person".into(), "Member".into()];
        original
            .properties
            .insert("remove".into(), Value::Boolean(true));
        create = a.prepare_vertex(&original).unwrap();
        create.submit(&n).unwrap();
        vertex(&b, &n, 2);
        let e = edge(1, 2);
        a.prepare_source(&e)
            .unwrap()
            .combine(b.prepare_reverse(&e).unwrap())
            .unwrap()
            .submit(&n)
            .unwrap();
        let frozen = n.read_view().unwrap();
        let frozen_bytes = n.get(&key, &frozen).unwrap();
        let before = n.admin().export().unwrap();
        let patch = VertexPropertyPatch {
            set: BTreeMap::from([
                ("null".into(), Value::Null),
                ("int".into(), Value::Integer(i64::MAX)),
                ("min".into(), Value::Integer(i64::MIN)),
                ("real".into(), Value::Real(-0.0)),
                ("text".into(), Value::Text("精确".into())),
                ("bool".into(), Value::Boolean(false)),
            ]),
            remove: ["remove".into(), "absent".into()].into(),
        };
        update = a
            .prepare_vertex_properties(identity, &original, &patch)
            .unwrap();
        assert_eq!(before.entries, n.admin().export().unwrap().entries);
        update.submit(&n).unwrap();
        let after = n.admin().export().unwrap();
        assert_eq!(after.sequence, before.sequence + 1);
        assert_eq!(before.entries.len(), after.entries.len());
        assert_eq!(
            before
                .entries
                .iter()
                .zip(&after.entries)
                .filter(|(a, b)| a != b)
                .count(),
            1
        );
        let updated = a.get_vertex(1).unwrap().unwrap();
        assert_eq!(updated.id, original.id);
        assert_eq!(updated.labels, original.labels);
        assert_eq!(updated.properties, patch.set);
        assert!(
            matches!(updated.properties["real"], Value::Real(v) if v.to_bits() == (-0.0f64).to_bits())
        );
        assert_eq!(n.get(&key, &frozen).unwrap(), frozen_bytes);
        assert_ne!(n.get(&key, &n.read_view().unwrap()).unwrap(), frozen_bytes);
        assert_eq!(a.get_edge(7).unwrap(), Some(e.clone()));
        b.verify_reverse(&e).unwrap();
        assert_eq!(a.observed_index(1, Direction::Out).unwrap().len(), 1);
        a.prepare_vertex_properties(
            identity,
            &updated,
            &vertex_property_patch(Value::Integer(2)),
        )
        .unwrap()
        .submit(&n)
        .unwrap();
        final_vertex = a.get_vertex(1).unwrap().unwrap();
        create.submit(&n).unwrap();
        update.submit(&n).unwrap();
        assert_eq!(a.get_vertex(1).unwrap(), Some(final_vertex.clone()));
        assert_eq!(n.read_view().unwrap().sequence, after.sequence + 1);
    }
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let before = n.admin().export().unwrap();
    create.submit(&n).unwrap();
    update.submit(&n).unwrap();
    assert_eq!(a.get_vertex(1).unwrap(), Some(final_vertex));
    assert_eq!(before.entries, n.admin().export().unwrap().entries);
    assert_eq!(before.sequence, n.read_view().unwrap().sequence);
}

#[test]
fn vertex_property_validation_rejects_wrong_identity_stale_bytes_and_invalid_patches() {
    use velagraph::experimental_graph::VertexPropertyPatch;
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let identity = VertexRef {
        partition: 1,
        id: 1,
    };
    let mut expected = Vertex::new(1);
    expected.labels.push("Person".into());
    expected.properties.insert("x".into(), Value::Real(-0.0));
    a.prepare_vertex(&expected).unwrap().submit(&n).unwrap();
    let before = n.admin().export().unwrap();
    let patch = vertex_property_patch(Value::Null);
    for wrong in [
        VertexRef {
            partition: 2,
            id: 1,
        },
        VertexRef {
            partition: 1,
            id: 2,
        },
    ] {
        assert!(matches!(
            a.prepare_vertex_properties(wrong, &expected, &patch),
            Err(ExperimentalError::Invalid(_))
        ));
    }
    let missing = VertexRef {
        partition: 1,
        id: 42,
    };
    assert!(
        matches!(a.prepare_vertex_properties(missing, &Vertex::new(42), &patch), Err(ExperimentalError::MissingEndpoint(v)) if v == missing)
    );
    for changed in 0..3 {
        let mut stale = expected.clone();
        match changed {
            0 => {
                stale.properties.insert("x".into(), Value::Real(0.0));
            }
            1 => stale.labels.push("Other".into()),
            _ => {
                stale.properties.insert("x".into(), Value::Integer(0));
            }
        }
        assert!(matches!(
            a.prepare_vertex_properties(identity, &stale, &patch),
            Err(ExperimentalError::Invalid(_))
        ));
    }
    for patch in [
        VertexPropertyPatch::default(),
        VertexPropertyPatch {
            remove: ["x".into()].into(),
            ..vertex_property_patch(Value::Null)
        },
    ] {
        assert!(matches!(
            a.prepare_vertex_properties(identity, &expected, &patch),
            Err(ExperimentalError::Invalid(_))
        ));
    }
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        assert!(matches!(
            a.prepare_vertex_properties(
                identity,
                &expected,
                &vertex_property_patch(Value::Real(value))
            ),
            Err(ExperimentalError::Graph(_))
        ));
    }
    assert!(matches!(
        a.prepare_vertex_properties(
            identity,
            &expected,
            &vertex_property_patch(Value::Text("x".repeat(1024 * 1024 - 1)))
        ),
        Err(ExperimentalError::Storage(
            StorageNodeError::ResourceExhausted(_)
        ))
    ));
    assert_eq!(before.entries, n.admin().export().unwrap().entries);
    assert_eq!(before.sequence, n.read_view().unwrap().sequence);
}

#[test]
fn vertex_property_cas_races_and_unrelated_writes_do_not_overwrite() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let identity = VertexRef {
        partition: 1,
        id: 1,
    };
    let expected = Vertex::new(1);
    a.prepare_vertex(&expected).unwrap().submit(&n).unwrap();
    let one = a
        .prepare_vertex_properties(
            identity,
            &expected,
            &vertex_property_patch(Value::Integer(1)),
        )
        .unwrap();
    let two = a
        .prepare_vertex_properties(
            identity,
            &expected,
            &vertex_property_patch(Value::Integer(2)),
        )
        .unwrap();
    one.submit(&n).unwrap();
    let before = n.admin().export().unwrap();
    assert!(matches!(
        two.submit(&n),
        Err(ExperimentalError::Storage(
            StorageNodeError::Conflict { .. }
        ))
    ));
    assert!(matches!(
        a.prepare_vertex_properties(identity, &expected, &vertex_property_patch(Value::Null)),
        Err(ExperimentalError::Invalid(_))
    ));
    assert_eq!(before.entries, n.admin().export().unwrap().entries);
    assert_eq!(before.sequence, n.read_view().unwrap().sequence);
    let current = a.get_vertex(1).unwrap().unwrap();
    let stale = a
        .prepare_vertex_properties(identity, &current, &vertex_property_patch(Value::Null))
        .unwrap();
    vertex(&a, &n, 42);
    let before = n.admin().export().unwrap();
    assert!(matches!(
        stale.submit(&n),
        Err(ExperimentalError::Storage(
            StorageNodeError::Conflict { .. }
        ))
    ));
    assert_eq!(before.entries, n.admin().export().unwrap().entries);
    assert_eq!(before.sequence, n.read_view().unwrap().sequence);
}

#[test]
fn vertex_property_prepared_bytes_are_immutable_and_epoch_fencing_precedes_dedup() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let identity = VertexRef {
        partition: 1,
        id: 1,
    };
    let mut expected = Vertex::new(1);
    a.prepare_vertex(&expected).unwrap().submit(&n).unwrap();
    let mut patch = vertex_property_patch(Value::Integer(7));
    let prepared = a
        .prepare_vertex_properties(identity, &expected, &patch)
        .unwrap();
    expected.labels.push("Changed".into());
    patch.set.insert("x".into(), Value::Integer(8));
    prepared.submit(&n).unwrap();
    let current = a.get_vertex(1).unwrap().unwrap();
    assert!(current.labels.is_empty());
    assert_eq!(current.properties["x"], Value::Integer(7));
    n.admin().set_epoch(1).unwrap();
    let before = n.admin().export().unwrap();
    assert!(matches!(
        prepared.submit(&n),
        Err(ExperimentalError::Storage(StorageNodeError::Fenced { .. }))
    ));
    assert_eq!(before.entries, n.admin().export().unwrap().entries);
    assert_eq!(before.sequence, n.read_view().unwrap().sequence);
}

#[test]
fn vertex_property_lost_ack_dedups_and_changed_requests_conflict() {
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let expected = Vertex::new(1);
    a.prepare_vertex(&expected).unwrap().submit(&n).unwrap();
    let prepared = a
        .prepare_vertex_properties(
            VertexRef {
                partition: 1,
                id: 1,
            },
            &expected,
            &vertex_property_patch(Value::Null),
        )
        .unwrap();
    let transport = CommitThenTimeout {
        node: n.clone(),
        calls: std::sync::Mutex::new(vec![]),
    };
    assert!(matches!(
        prepared.submit(&transport),
        Err(ExperimentalError::Storage(StorageNodeError::Timeout { .. }))
    ));
    let committed = n.admin().export().unwrap();
    prepared.submit(&transport).unwrap();
    let calls = transport.calls.lock().unwrap();
    assert_eq!(calls[0], calls[1]);
    assert_eq!(calls[0].0.operations.len(), 1);
    let (request, condition) = calls[0].clone();
    assert!(matches!(
        n.batch_write_conditional(request.clone(), condition.map(|n| n + 1)),
        Err(StorageNodeError::IdempotencyConflict(_))
    ));
    let mut changed = request;
    changed.operations = vec![WriteOp::put(b"changed".to_vec(), vec![])];
    assert!(matches!(
        n.batch_write_conditional(changed, condition),
        Err(StorageNodeError::IdempotencyConflict(_))
    ));
    assert_eq!(committed.entries, n.admin().export().unwrap().entries);
    assert_eq!(committed.sequence, n.read_view().unwrap().sequence);
}

#[test]
fn vertex_property_patch_admission_bounds_count_set_remove_and_payload_bytes() {
    use velagraph::experimental_graph::VertexPropertyPatch;
    let dir = tempfile::tempdir().unwrap();
    let n = node(dir.path());
    let a = AppendOnlyStore::new(n.clone(), 7, 1);
    let identity = VertexRef {
        partition: 1,
        id: 1,
    };
    let expected = Vertex::new(1);
    a.prepare_vertex(&expected).unwrap().submit(&n).unwrap();
    let before = n.admin().export().unwrap();
    // Removing absent keys is allowed, but still charged to the input budget.
    let mut patch = VertexPropertyPatch {
        remove: (0..10_000).map(|i| format!("key{i}")).collect(),
        ..Default::default()
    };
    a.prepare_vertex_properties(identity, &expected, &patch)
        .unwrap();
    patch.set.insert("x".into(), Value::Null);
    assert!(matches!(
        a.prepare_vertex_properties(identity, &expected, &patch),
        Err(ExperimentalError::Invalid(_))
    ));
    patch.remove.remove("key0");
    a.prepare_vertex_properties(identity, &expected, &patch)
        .unwrap();

    let mut patch = VertexPropertyPatch {
        remove: ["r".repeat(1024 * 1024)].into(),
        ..Default::default()
    };
    a.prepare_vertex_properties(identity, &expected, &patch)
        .unwrap();
    patch.remove.insert("a".into());
    assert!(matches!(
        a.prepare_vertex_properties(identity, &expected, &patch),
        Err(ExperimentalError::Invalid(_))
    ));
    for value in [
        Value::Null,
        Value::Integer(0),
        Value::Real(0.0),
        Value::Boolean(false),
    ] {
        let mut patch = VertexPropertyPatch {
            set: BTreeMap::from([("x".into(), value)]),
            remove: ["r".repeat(1024 * 1024 - 9)].into(),
        };
        a.prepare_vertex_properties(identity, &expected, &patch)
            .unwrap();
        patch.remove.insert("a".into());
        assert!(matches!(
            a.prepare_vertex_properties(identity, &expected, &patch),
            Err(ExperimentalError::Invalid(_))
        ));
    }
    let patch = VertexPropertyPatch {
        set: BTreeMap::from([("x".into(), Value::Text("精确".into()))]),
        remove: ["r".repeat(1024 * 1024 - 7)].into(),
    };
    a.prepare_vertex_properties(identity, &expected, &patch)
        .unwrap();
    let mut oversized = patch;
    oversized.remove.insert("a".into());
    assert!(matches!(
        a.prepare_vertex_properties(identity, &expected, &oversized),
        Err(ExperimentalError::Invalid(_))
    ));
    assert!(matches!(
        a.prepare_vertex_properties(
            identity,
            &expected,
            &vertex_property_patch(Value::Text("x".repeat(1024 * 1024)))
        ),
        Err(ExperimentalError::Invalid(_))
    ));
    assert_eq!(before.entries, n.admin().export().unwrap().entries);
    assert_eq!(before.sequence, n.read_view().unwrap().sequence);
}

#[test]
fn vertex_property_nonempty_noop_commits_advance_sequence_and_fence_sibling_batches() {
    use velagraph::experimental_graph::VertexPropertyPatch;
    for patch in [
        vertex_property_patch(Value::Integer(7)),
        VertexPropertyPatch {
            remove: ["absent".into()].into(),
            ..Default::default()
        },
    ] {
        let dir = tempfile::tempdir().unwrap();
        let n = node(dir.path());
        let a = AppendOnlyStore::new(n.clone(), 7, 1);
        let identity = VertexRef {
            partition: 1,
            id: 1,
        };
        let mut expected = Vertex::new(1);
        expected.properties.insert("x".into(), Value::Integer(7));
        a.prepare_vertex(&expected).unwrap().submit(&n).unwrap();
        let noop = a
            .prepare_vertex_properties(identity, &expected, &patch)
            .unwrap();
        let sibling = a
            .prepare_vertex_properties(
                identity,
                &expected,
                &vertex_property_patch(Value::Integer(8)),
            )
            .unwrap();
        let before = n.admin().export().unwrap();
        noop.submit(&n).unwrap();
        let committed = n.admin().export().unwrap();
        assert_eq!(committed.entries, before.entries);
        assert_eq!(committed.sequence, before.sequence + 1);
        assert_eq!(a.get_vertex(1).unwrap(), Some(expected));
        assert!(matches!(
            sibling.submit(&n),
            Err(ExperimentalError::Storage(
                StorageNodeError::Conflict { .. }
            ))
        ));
        // An exact retry remains idempotent even though the original no-op
        // consumed a sequence and invalidated other preparations.
        noop.submit(&n).unwrap();
        assert_eq!(n.admin().export().unwrap().entries, committed.entries);
        assert_eq!(n.read_view().unwrap().sequence, committed.sequence);
    }
}
