use super::*;
use crate::graph::{adjacency_key, codec, edge_key, vertex_key, EdgeDirection};
use crate::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Mutex,
};

struct Node {
    live: Mutex<ReadView>,
    acquisitions: AtomicUsize,
    pagination_enabled: bool,
}
impl Node {
    fn new(vertices: &[Vertex], edges: &[Edge]) -> Self {
        Self {
            live: Mutex::new(view(vertices, edges, 1)),
            acquisitions: AtomicUsize::new(0),
            pagination_enabled: true,
        }
    }
}
fn view(vertices: &[Vertex], edges: &[Edge], sequence: u64) -> ReadView {
    let mut entries: Vec<KeyValue> = vertices
        .iter()
        .map(|v| KeyValue {
            key: vertex_key(0, v.id),
            value: codec::encode_vertex(v).unwrap(),
        })
        .collect();
    for e in edges {
        entries.push(KeyValue {
            key: edge_key(0, e.id),
            value: codec::encode_edge(e).unwrap(),
        });
        for (direction, endpoint) in [(EdgeDirection::Out, e.from), (EdgeDirection::In, e.to)] {
            entries.push(KeyValue {
                key: adjacency_key(0, direction, endpoint, &e.label, e.id),
                value: vec![],
            });
        }
    }
    ReadView::from_entries(
        7,
        1,
        sequence,
        entries.into_iter().map(|e| (e.key, e.value)).collect(),
    )
    .unwrap()
}
impl StorageNode for Node {
    fn read_view(&self) -> std::result::Result<ReadView, StorageNodeError> {
        self.acquisitions.fetch_add(1, Ordering::SeqCst);
        Ok(self.live.lock().unwrap().clone())
    }
    fn get(
        &self,
        key: &[u8],
        view: &ReadView,
    ) -> std::result::Result<Option<Vec<u8>>, StorageNodeError> {
        Ok(view.get(key))
    }
    fn scan(
        &self,
        _: KeyRange,
        _: ScanLimit,
        _: &ReadView,
    ) -> std::result::Result<Vec<KeyValue>, StorageNodeError> {
        panic!("query must use pagination")
    }
    fn scan_page(
        &self,
        r: KeyRange,
        l: ScanLimit,
        v: &ReadView,
        c: Option<&ScanCursor>,
    ) -> std::result::Result<ScanPage, StorageNodeError> {
        if !self.pagination_enabled {
            return Err(StorageNodeError::Unsupported("paged scans".into()));
        }
        v.scan_page(r, l, c)
    }
    fn batch_write(&self, _: WriteRequest) -> std::result::Result<WriteResult, StorageNodeError> {
        unreachable!()
    }
    fn admin(&self) -> &dyn StorageAdmin {
        unreachable!()
    }
}
fn fixture() -> (Vec<Vertex>, Vec<Edge>) {
    let vertices = [
        (1, "Alice", Some(30)),
        (2, "Bob", Some(40)),
        (3, "Carol", None),
        (4, "Paris", None),
    ]
    .into_iter()
    .map(|(id, name, age)| {
        let mut v = Vertex::new(id);
        v.labels
            .push(if id == 4 { "city" } else { "person" }.into());
        v.properties.insert("name".into(), Value::Text(name.into()));
        if let Some(age) = age {
            v.properties.insert("age".into(), Value::Integer(age));
        }
        v
    })
    .collect();
    let mut edges = vec![
        Edge::new(100, 1, 2, "KNOWS"),
        Edge::new(101, 2, 3, "KNOWS"),
        Edge::new(102, 1, 4, "LIVES_IN"),
        Edge::new(103, 3, 1, "KNOWS"),
    ];
    edges[0]
        .properties
        .insert("weight".into(), Value::Integer(5));
    (vertices, edges)
}
#[test]
fn existing_query_corpus_matches_local_engine() {
    let (vertices, edges) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let local = GraphEngine::open(dir.path()).unwrap();
    for v in &vertices {
        local.create_vertex(v).unwrap();
    }
    for e in &edges {
        local.create_edge(e).unwrap();
    }
    let store = GraphRecordStore::new(Node::new(&vertices, &edges), 7, 0);
    // Reuse every literal query from the existing normative corpus, including
    // rejected queries. Additional formatted placeholders reject identically.
    let corpus = include_str!("../tests/gremlin.rs");
    let mut checked = 0;
    for query in corpus.split('"').filter(|s| s.starts_with("g.")) {
        let a = local.gremlin(query);
        let b = store.execute_gremlin(query);
        match (a, b) {
            (Ok(a), Ok(b)) => assert_eq!(a, b, "{query}"),
            (Err(a), Err(b)) => assert_eq!(
                std::mem::discriminant(&a),
                std::mem::discriminant(&b),
                "{query}: {a} / {b}"
            ),
            (a, b) => panic!("{query}: {a:?} / {b:?}"),
        }
        checked += 1;
    }
    assert!(checked > 40);
}
#[test]
fn paged_starts_caps_leading_limit_and_adjacency_multiplicity() {
    let vertices = (0..=10_000).map(Vertex::new).collect::<Vec<_>>();
    let edges = (0..=10_000)
        .map(|id| Edge::new(id, 0, 1, if id % 2 == 0 { "Z" } else { "A" }))
        .collect::<Vec<_>>();
    let at_cap = GraphRecordStore::new(Node::new(&vertices[..10_000], &edges[..10_000]), 7, 0);
    for q in ["g.V().count()", "g.E().count()", "g.V(0).out().count()"] {
        assert_eq!(
            at_cap.execute_gremlin(q).unwrap().rows,
            vec![GremlinRow::Count(10_000)],
            "{q}"
        );
    }
    let store = GraphRecordStore::new(Node::new(&vertices, &edges), 7, 0);
    for q in [
        "g.V().count()",
        "g.E().count()",
        "g.V(0).out().limit(1)",
        "g.V(0).out().count()",
    ] {
        assert!(
            matches!(
                store.execute_gremlin(q),
                Err(Error::TraversalLimitExceeded(_))
            ),
            "{q}"
        );
    }
    for q in ["g.V().limit(1000).count()", "g.E().limit(1000).count()"] {
        assert_eq!(
            store.execute_gremlin(q).unwrap().rows,
            vec![GremlinRow::Count(1000)]
        );
    }
    let edges = (0..1001)
        .map(|id| Edge::new(id, 0, 1, if id % 2 == 0 { "Z" } else { "A" }))
        .chain([Edge::new(1001, 0, 0, "SELF")])
        .collect::<Vec<_>>();
    let store = GraphRecordStore::new(Node::new(&vertices[..2], &edges), 7, 0);
    assert_eq!(
        store.execute_gremlin("g.V(0).both().count()").unwrap().rows,
        vec![GremlinRow::Count(1002)]
    );
    assert_eq!(
        store
            .execute_gremlin("g.V(0,0).out().count()")
            .unwrap()
            .rows,
        vec![GremlinRow::Count(2004)]
    );
    assert_eq!(
        store
            .execute_gremlin("g.V(0).out().limit(3).path()")
            .unwrap()
            .rows,
        (0..3)
            .map(|id| GremlinRow::Path(vec![
                PathElement::Vertex(0),
                PathElement::Edge(id),
                PathElement::Vertex(1)
            ]))
            .collect::<Vec<_>>()
    );
}
#[test]
fn oversized_records_error_for_scan_and_point_without_partial_success() {
    let mut big = Vertex::new(2);
    big.properties.insert(
        "big".into(),
        Value::Text("x".repeat(GRAPH_QUERY_PAGE_BYTES - 31)),
    );
    let store = GraphRecordStore::new(Node::new(&[Vertex::new(1), big], &[]), 7, 0);
    for q in ["g.V()", "g.V(2)"] {
        assert!(
            matches!(
                store.execute_gremlin(q),
                Err(Error::StorageNode(StorageNodeError::ResourceExhausted(_)))
            ),
            "{q}"
        );
    }
    assert_eq!(
        store.execute_gremlin("g.V().limit(1)").unwrap().rows,
        vec![GremlinRow::Vertex(1)]
    );
}
#[test]
fn snapshot_survives_concurrent_commit_at_every_query_checkpoint() {
    let (old_vertices, old_edges) = fixture();
    let mut new_vertices = old_vertices.clone();
    new_vertices[1]
        .properties
        .insert("name".into(), Value::Text("Changed".into()));
    let mut new_edges = old_edges.clone();
    new_edges[0].to = 3;
    for text in [
        "g.V().out().values('name')",
        "g.V(1).out().out().path()",
        "g.E().values('weight')",
    ] {
        let query = GremlinQuery::parse(text).unwrap();
        let store = GraphRecordStore::new(Node::new(&old_vertices, &old_edges), 7, 0);
        let expected = query.execute_records(&store).unwrap();
        let count = AtomicUsize::new(0);
        let pinned = store.read_view().unwrap();
        query
            .execute_on_read(&pinned, &|| {
                count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .unwrap();
        for trigger in 1..=count.load(Ordering::SeqCst) {
            *store.node().live.lock().unwrap() = view(&old_vertices, &old_edges, 1);
            let pinned = store.read_view().unwrap();
            let checks = AtomicUsize::new(0);
            let result = query
                .execute_on_read(&pinned, &|| {
                    if checks.fetch_add(1, Ordering::SeqCst) + 1 == trigger {
                        std::thread::scope(|scope| {
                            scope
                                .spawn(|| {
                                    *store.node().live.lock().unwrap() =
                                        view(&new_vertices, &new_edges, 2);
                                })
                                .join()
                                .unwrap();
                        });
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(result, expected, "{text} checkpoint {trigger}");
        }
    }
    let store = GraphRecordStore::new(Node::new(&old_vertices, &old_edges), 7, 0);
    store.execute_gremlin("g.V().out().out().count()").unwrap();
    assert_eq!(store.node().acquisitions.load(Ordering::SeqCst), 1);
}

#[test]
fn paged_corruption_and_cooperative_checks_fail_closed() {
    let (vertices, edges) = fixture();
    let store = GraphRecordStore::new(Node::new(&vertices, &edges), 7, 0);
    let pinned = store.read_view().unwrap();
    let query = GremlinQuery::parse("g.V().out().values('name')").unwrap();
    let count = AtomicUsize::new(0);
    assert!(matches!(
        query.execute_on_read(&pinned, &|| {
            if count.fetch_add(1, Ordering::SeqCst) == 20 {
                Err(Error::TraversalLimitExceeded("injected deadline".into()))
            } else {
                Ok(())
            }
        }),
        Err(Error::TraversalLimitExceeded(_))
    ));
    let corrupt = ReadView::from_entries(
        7,
        1,
        2,
        [
            (
                vertex_key(0, 1),
                codec::encode_vertex(&Vertex::new(1)).unwrap(),
            ),
            (
                vertex_key(0, 2),
                codec::encode_vertex(&Vertex::new(2)).unwrap(),
            ),
            (
                edge_key(0, 10),
                codec::encode_edge(&Edge::new(10, 2, 1, "A")).unwrap(),
            ),
            // Forged endpoint adjacency must not silently traverse unrelated edges.
            (adjacency_key(0, EdgeDirection::Out, 1, "A", 10), vec![]),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    *store.node().live.lock().unwrap() = corrupt;
    assert!(matches!(
        store.execute_gremlin("g.V(1).out()"),
        Err(Error::CorruptData(_))
    ));
    *store.node().live.lock().unwrap() = ReadView::from_entries(
        7,
        1,
        3,
        [(
            vertex_key(0, 1),
            codec::encode_vertex(&Vertex::new(2)).unwrap(),
        )]
        .into_iter()
        .collect(),
    )
    .unwrap();
    for q in ["g.V()", "g.V(1)"] {
        assert!(matches!(
            store.execute_gremlin(q),
            Err(Error::CorruptData(_))
        ));
    }
}

#[test]
fn missing_pagination_fails_without_legacy_scan_fallback() {
    let mut node = Node::new(&[Vertex::new(1)], &[]);
    node.pagination_enabled = false;
    let store = GraphRecordStore::new(node, 7, 0);
    assert!(matches!(
        store.execute_gremlin("g.V()"),
        Err(Error::StorageNode(StorageNodeError::Unsupported(_)))
    ));
    // Explicit point reads do not require the optional scan capability.
    assert_eq!(
        store.execute_gremlin("g.V(1)").unwrap().rows,
        vec![GremlinRow::Vertex(1)]
    );
    let wrong_shard = GraphRecordStore::new(Node::new(&[], &[]), 8, 0);
    assert!(matches!(
        wrong_shard.execute_gremlin("g.V()"),
        Err(Error::StorageNode(StorageNodeError::InvalidArgument(_)))
    ));
}

#[test]
fn paged_order_and_multiplicity_match_local_corpus() {
    let vertices = (0..12).map(Vertex::new).collect::<Vec<_>>();
    let edges = (0..1100)
        .map(|id| {
            Edge::new(
                id,
                (id * 7 + 3) % 12,
                (id * 11 + 1) % 12,
                ["A", "AA", "Z"][(id % 3) as usize],
            )
        })
        .collect::<Vec<_>>();
    let dir = tempfile::tempdir().unwrap();
    let local = GraphEngine::open(dir.path()).unwrap();
    for v in &vertices {
        local.create_vertex(v).unwrap();
    }
    for e in &edges {
        local.create_edge(e).unwrap();
    }
    let store = GraphRecordStore::new(Node::new(&vertices, &edges), 7, 0);
    for start in ["g.V()", "g.V(3,3,999,1)", "g.V(0)", "g.V(2)"] {
        for traversal in ["out()", "in()", "both()", "out('A')", "both('AA')"] {
            for finish in ["count()", "limit(77).path()", "limit(1).id()"] {
                let q = format!("{start}.{traversal}.{finish}");
                assert_eq!(
                    local.gremlin(&q).unwrap(),
                    store.execute_gremlin(&q).unwrap(),
                    "{q}"
                );
            }
        }
    }
}

#[test]
fn empty_partition_and_retained_sequence() {
    let store = GraphRecordStore::new(Node::new(&[Vertex::new(1)], &[]), 7, 1);
    assert_eq!(
        store.execute_gremlin("g.V().count()").unwrap().rows,
        vec![GremlinRow::Count(0)]
    );
    assert_eq!(
        store.execute_gremlin("g.V(1).count()").unwrap().rows,
        vec![GremlinRow::Count(0)]
    );
    let wrong_shard = GraphRecordStore::new(Node::new(&[], &[]), 8, 0);
    assert!(matches!(
        wrong_shard.execute_gremlin("g.V()"),
        Err(Error::StorageNode(StorageNodeError::InvalidArgument(_)))
    ));
    let store = GraphRecordStore::new(Node::new(&[Vertex::new(1)], &[]), 7, 0);
    let retained = store.read_view().unwrap();
    *store.node().live.lock().unwrap() = view(&[], &[], 2);
    assert_eq!(retained.sequence(), 1);
    assert_eq!(
        retained.execute_gremlin("g.V().count()").unwrap().rows,
        vec![GremlinRow::Count(1)]
    );
    assert_eq!(
        store.execute_gremlin("g.V().count()").unwrap().rows,
        vec![GremlinRow::Count(0)]
    );
}

struct FailingPageNode {
    inner: Node,
    calls: AtomicUsize,
    slow_view: bool,
}
impl StorageNode for FailingPageNode {
    fn read_view(&self) -> std::result::Result<ReadView, StorageNodeError> {
        if self.slow_view {
            std::thread::sleep(std::time::Duration::from_millis(5050));
        }
        self.inner.read_view()
    }
    fn get(
        &self,
        key: &[u8],
        view: &ReadView,
    ) -> std::result::Result<Option<Vec<u8>>, StorageNodeError> {
        self.inner.get(key, view)
    }
    fn scan(
        &self,
        _: KeyRange,
        _: ScanLimit,
        _: &ReadView,
    ) -> std::result::Result<Vec<KeyValue>, StorageNodeError> {
        panic!("fallback not allowed")
    }
    fn scan_page(
        &self,
        r: KeyRange,
        l: ScanLimit,
        v: &ReadView,
        c: Option<&ScanCursor>,
    ) -> std::result::Result<ScanPage, StorageNodeError> {
        assert!(l.max_rows <= 256);
        assert!(l.max_bytes <= GRAPH_QUERY_PAGE_BYTES);
        assert_eq!(v.sequence, 1);
        if self.calls.fetch_add(1, Ordering::SeqCst) == 1 {
            return Err(StorageNodeError::Fenced {
                shard: 7,
                epoch: 1,
                current: 2,
            });
        }
        self.inner.scan_page(r, l, v, c)
    }
    fn batch_write(&self, _: WriteRequest) -> std::result::Result<WriteResult, StorageNodeError> {
        unreachable!()
    }
    fn admin(&self) -> &dyn StorageAdmin {
        unreachable!()
    }
}
#[test]
fn mid_page_fence_never_returns_partial_success() {
    let vertices = (0..600).map(Vertex::new).collect::<Vec<_>>();
    let store = GraphRecordStore::new(
        FailingPageNode {
            inner: Node::new(&vertices, &[]),
            calls: AtomicUsize::new(0),
            slow_view: false,
        },
        7,
        0,
    );
    assert!(matches!(
        store.execute_gremlin("g.V().count()"),
        Err(Error::StorageNode(StorageNodeError::Fenced {
            current: 2,
            ..
        }))
    ));
    assert_eq!(store.node().calls.load(Ordering::SeqCst), 2);
    assert_eq!(store.node().inner.acquisitions.load(Ordering::SeqCst), 1);
}
#[test]
fn acquisition_time_is_in_query_deadline() {
    let store = GraphRecordStore::new(
        FailingPageNode {
            inner: Node::new(&[], &[]),
            calls: AtomicUsize::new(0),
            slow_view: true,
        },
        7,
        0,
    );
    assert!(matches!(
        store.execute_gremlin("g.V().count()"),
        Err(Error::TraversalLimitExceeded(_))
    ));
    assert_eq!(store.node().calls.load(Ordering::SeqCst), 0);
}

fn assert_query_byte_limit(result: Result<GremlinResult>, query: &str) {
    match result {
        Err(Error::TraversalLimitExceeded(message)) => {
            assert!(message.contains("byte"), "{query}: {message}");
        }
        other => panic!("expected accounted-byte rejection for {query}, got {other:?}"),
    }
}

#[test]
fn aggregate_budget_parallel_edges_and_repeated_ids_reject_on_all_entry_points() {
    // Stay below parser admission while retaining >64 MiB amplification:
    // fifty repeated 2 MiB vertices still exercise the execution byte budget.
    let mut target = Vertex::new(2);
    target
        .properties
        .insert("big".into(), Value::Text("x".repeat(2 * 1024 * 1024)));
    let vertices = vec![Vertex::new(1), target];
    let edges = (0..100)
        .map(|id| Edge::new(id, 1, 2, "TO"))
        .collect::<Vec<_>>();
    let dir = tempfile::tempdir().unwrap();
    let local = GraphEngine::open(dir.path()).unwrap();
    for vertex in &vertices {
        local.create_vertex(vertex).unwrap();
    }
    for edge in &edges {
        local.create_edge(edge).unwrap();
    }
    let store = GraphRecordStore::new(Node::new(&vertices, &edges), 7, 0);
    let retained = store.read_view().unwrap();
    let repeated = format!("g.V({}).count()", vec!["2"; 50].join(","));
    for text in [
        "g.V(1).out()",
        "g.V(1).out().limit(1)",
        "g.V(1).out().count()",
        &repeated,
    ] {
        let query = GremlinQuery::parse(text).unwrap();
        assert_query_byte_limit(query.execute(&local), text);
        assert_query_byte_limit(query.execute_records(&store), text);
        assert_query_byte_limit(query.execute_record_view(&retained), text);
    }
    let text = format!("g.V({}).limit(1).id()", vec!["2"; 50].join(","));
    let query = GremlinQuery::parse(&text).unwrap();
    let expected = GremlinResult {
        rows: vec![GremlinRow::Value(Value::Integer(2))],
    };
    assert_eq!(query.execute(&local).unwrap(), expected);
    assert_eq!(query.execute_records(&store).unwrap(), expected);
    assert_eq!(query.execute_record_view(&retained).unwrap(), expected);
}

#[test]
fn aggregate_budget_multi_page_decoded_properties_exceed_small_encoded_store() {
    // Small integer encodings hide substantial decoded BTreeMap entry costs.
    // 1100 records require multiple pages even at the 256-row maximum.
    let vertices = (0..1100)
        .map(|id| {
            let mut vertex = Vertex::new(id);
            vertex.labels.push("wide".into());
            for property in 0..1000 {
                vertex
                    .properties
                    .insert(format!("p{property:04}"), Value::Integer(property));
            }
            vertex
        })
        .collect::<Vec<_>>();
    let encoded_bytes: usize = vertices
        .iter()
        .map(|v| vertex_key(0, v.id).len() + codec::encode_vertex(v).unwrap().len())
        .sum();
    assert!(encoded_bytes < 32 * 1024 * 1024);
    let dir = tempfile::tempdir().unwrap();
    let local = GraphEngine::open(dir.path()).unwrap();
    for vertex in &vertices {
        local.create_vertex(vertex).unwrap();
    }
    let store = GraphRecordStore::new(Node::new(&vertices, &[]), 7, 0);
    let retained = store.read_view().unwrap();
    for text in ["g.V()", "g.V().count()", "g.V().hasLabel('wide').limit(1)"] {
        let query = GremlinQuery::parse(text).unwrap();
        assert_query_byte_limit(query.execute(&local), text);
        assert_query_byte_limit(query.execute_records(&store), text);
        assert_query_byte_limit(query.execute_record_view(&retained), text);
    }
    let query = GremlinQuery::parse("g.V().limit(1).values('p0000')").unwrap();
    let expected = GremlinResult {
        rows: vec![GremlinRow::Value(Value::Integer(0))],
    };
    assert_eq!(query.execute(&local).unwrap(), expected);
    assert_eq!(query.execute_records(&store).unwrap(), expected);
    assert_eq!(query.execute_record_view(&retained).unwrap(), expected);
}

fn check_exact_point_budget(
    backend: &impl crate::gremlin::QueryReadBackend,
    text: &str,
    expected: &GremlinResult,
) {
    use crate::query_memory::Budget;
    let query = GremlinQuery::parse(text).unwrap();
    let measured = Budget::new(16 * 1024 * 1024);
    assert_eq!(
        &query
            .execute_on_read_budget(backend, &|| Ok(()), &measured)
            .unwrap(),
        expected
    );
    assert_eq!(measured.used(), 0);
    let exact = Budget::new(measured.peak());
    assert_eq!(
        &query
            .execute_on_read_budget(backend, &|| Ok(()), &exact)
            .unwrap(),
        expected
    );
    assert_eq!(exact.used(), 0);
    assert_eq!(exact.peak(), measured.peak());
    let insufficient = Budget::new(measured.peak() - 1);
    assert_query_byte_limit(
        query.execute_on_read_budget(backend, &|| Ok(()), &insufficient),
        text,
    );
    assert_eq!(
        insufficient.used(),
        0,
        "failed query leaked reservations: {text}"
    );
    assert_eq!(
        GremlinQuery::parse("g.V(1).id()")
            .unwrap()
            .execute_on_read_budget(backend, &|| Ok(()), &insufficient)
            .unwrap()
            .rows,
        vec![GremlinRow::Value(Value::Integer(1))]
    );
    assert_eq!(insufficient.used(), 0);
}

#[test]
fn aggregate_budget_exact_point_boundaries_projections_and_edge_scratch() {
    let (mut vertices, mut edges) = fixture();
    vertices[1]
        .properties
        .insert("big".into(), Value::Text("v".repeat(64 * 1024)));
    edges[0]
        .properties
        .insert("big".into(), Value::Text("e".repeat(256 * 1024)));
    let dir = tempfile::tempdir().unwrap();
    let local = GraphEngine::open(dir.path()).unwrap();
    for vertex in &vertices {
        local.create_vertex(vertex).unwrap();
    }
    for edge in &edges {
        local.create_edge(edge).unwrap();
    }
    let store = GraphRecordStore::new(Node::new(&vertices, &edges), 7, 0);
    let retained = store.read_view().unwrap();
    for text in [
        "g.V(2)",
        "g.V(2).values('big')",
        "g.V(2).label()",
        "g.V(2).path()",
        "g.E(100)",
        "g.E(100).values('big')",
        "g.E(100).label()",
        "g.E(100).path()",
    ] {
        let query = GremlinQuery::parse(text).unwrap();
        let expected = query.execute(&local).unwrap();
        assert_eq!(query.execute_records(&store).unwrap(), expected, "{text}");
        assert_eq!(
            query.execute_record_view(&retained).unwrap(),
            expected,
            "{text}"
        );
        check_exact_point_budget(&local.read_view(), text, &expected);
        check_exact_point_budget(&retained, text, &expected);
    }
    // Traversal decodes and drops a large edge even when returning only IDs or paths.
    for (text, rows) in [
        (
            "g.V(1).out('KNOWS').id()",
            vec![GremlinRow::Value(Value::Integer(2))],
        ),
        (
            "g.V(1).out('KNOWS').label()",
            vec![GremlinRow::Value(Value::Text("person".into()))],
        ),
        (
            "g.V(1).out('KNOWS').path()",
            vec![GremlinRow::Path(vec![
                PathElement::Vertex(1),
                PathElement::Edge(100),
                PathElement::Vertex(2),
            ])],
        ),
    ] {
        let query = GremlinQuery::parse(text).unwrap();
        assert_eq!(query.execute(&local).unwrap().rows, rows);
        assert_eq!(query.execute_records(&store).unwrap().rows, rows);
        assert_eq!(query.execute_record_view(&retained).unwrap().rows, rows);
    }
    fn check_edge_scratch(backend: &impl crate::gremlin::QueryReadBackend) {
        let budget = crate::query_memory::Budget::new(128 * 1024);
        assert_query_byte_limit(
            GremlinQuery::parse("g.V(1).out('KNOWS').id()")
                .unwrap()
                .execute_on_read_budget(backend, &|| Ok(()), &budget),
            "large edge scratch",
        );
        assert_eq!(budget.used(), 0);
        assert_eq!(
            GremlinQuery::parse("g.V(1).values('name')")
                .unwrap()
                .execute_on_read_budget(backend, &|| Ok(()), &budget)
                .unwrap()
                .rows,
            vec![GremlinRow::Value(Value::Text("Alice".into()))],
        );
        assert_eq!(budget.used(), 0);
    }
    check_edge_scratch(&local.read_view());
    check_edge_scratch(&retained);
}

// Regression tests from the independent accounting audit.
#[test]
fn audit_corrupt_d1_label_rejected_before_key_reconstruction() {
    use crate::query_memory::Budget;
    let mut edge = Edge::new(7, 1, 2, "ok");
    edge.label = "x".repeat(1024 * 1024);
    let mut entries = std::collections::BTreeMap::new();
    for id in [1, 2] {
        entries.insert(
            vertex_key(0, id),
            codec::encode_vertex(&Vertex::new(id)).unwrap(),
        );
    }
    entries.insert(edge_key(0, 7), codec::encode_edge(&edge).unwrap());
    entries.insert(adjacency_key(0, EdgeDirection::Out, 1, "ok", 7), Vec::new());
    let node = Node::new(&[], &[]);
    *node.live.lock().unwrap() = ReadView::from_entries(7, 1, 1, entries).unwrap();
    let store = GraphRecordStore::new(node, 7, 0);
    let view = store.read_view().unwrap();
    let budget = Budget::new(3 * 1024 * 1024);
    let error = GremlinQuery::parse("g.V(1).out()")
        .unwrap()
        .execute_on_read_budget(&view, &|| Ok(()), &budget)
        .unwrap_err();
    assert!(
        matches!(error, Error::CorruptData(ref message) if message.contains("label")),
        "{error:?}"
    );
    assert!(
        budget.peak() >= 2 * 1024 * 1024,
        "large raw and decoded label must overlap"
    );
    assert_eq!(budget.used(), 0);
}

#[test]
fn audit_local_lookahead_is_charged_for_vertices_and_edges() {
    use crate::query_memory::Budget;
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphEngine::open(dir.path()).unwrap();
    graph.create_vertex(&Vertex::new(1)).unwrap();
    let mut big = Vertex::new(2);
    big.properties
        .insert("p".into(), Value::Text("x".repeat(100_000)));
    graph.create_vertex(&big).unwrap();
    graph.create_edge(&Edge::new(1, 1, 1, "ok")).unwrap();
    let mut edge = Edge::new(2, 1, 1, "ok");
    edge.properties = big.properties.clone();
    graph.create_edge(&edge).unwrap();
    for text in ["g.V().limit(1)", "g.E().limit(1)"] {
        let budget = Budget::new(8192);
        let result = GremlinQuery::parse(text).unwrap().execute_on_read_budget(
            &graph.read_view(),
            &|| Ok(()),
            &budget,
        );
        assert!(
            matches!(result, Err(Error::TraversalLimitExceeded(_))),
            "{text}: {result:?}"
        );
        assert_eq!(budget.used(), 0, "{text}");
    }
}

fn audit_cancel_every_checkpoint(backend: &impl crate::gremlin::QueryReadBackend) {
    use crate::query_memory::Budget;
    use std::cell::Cell;
    let query = GremlinQuery::parse("g.V(1,2).both().path()").unwrap();
    let calls = Cell::new(0);
    let budget = Budget::new(16 * 1024 * 1024);
    query
        .execute_on_read_budget(
            backend,
            &|| {
                calls.set(calls.get() + 1);
                Ok(())
            },
            &budget,
        )
        .unwrap();
    let checkpoints = calls.get();
    assert!(checkpoints > 20);
    assert_eq!(budget.used(), 0);
    for cutoff in 1..=checkpoints {
        calls.set(0);
        let result = query.execute_on_read_budget(
            backend,
            &|| {
                calls.set(calls.get() + 1);
                if calls.get() == cutoff {
                    Err(Error::TraversalLimitExceeded("audit cancellation".into()))
                } else {
                    Ok(())
                }
            },
            &budget,
        );
        assert!(
            matches!(result, Err(Error::TraversalLimitExceeded(ref m)) if m == "audit cancellation"),
            "checkpoint {cutoff}: {result:?}"
        );
        assert_eq!(budget.used(), 0, "checkpoint {cutoff}");
    }
}

#[test]
fn audit_cancellation_releases_all_local_and_d1_overlap() {
    let (vertices, edges) = fixture();
    let dir = tempfile::tempdir().unwrap();
    let local = GraphEngine::open(dir.path()).unwrap();
    for vertex in &vertices {
        local.create_vertex(vertex).unwrap();
    }
    for edge in &edges {
        local.create_edge(edge).unwrap();
    }
    let store = GraphRecordStore::new(Node::new(&vertices, &edges), 7, 0);
    audit_cancel_every_checkpoint(&local.read_view());
    audit_cancel_every_checkpoint(&store.read_view().unwrap());
}
