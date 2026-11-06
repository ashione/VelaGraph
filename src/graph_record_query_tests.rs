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
