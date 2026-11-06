use rocksdb_sql_table::{Edge, Error, GraphEngine, GremlinRow, PathElement, Value, Vertex};
use tempfile::TempDir;

fn graph() -> (TempDir, GraphEngine) {
    let dir = tempfile::tempdir().expect("temp dir");
    let graph = GraphEngine::open(dir.path()).expect("open graph");
    let people = [
        (1, "Alice", Some(30)),
        (2, "Bob", Some(40)),
        (3, "Carol", None),
    ];
    for (id, name, age) in people {
        let mut vertex = Vertex::new(id);
        vertex.labels.push("person".into());
        vertex
            .properties
            .insert("name".into(), Value::Text(name.into()));
        if let Some(age) = age {
            vertex.properties.insert("age".into(), Value::Integer(age));
        }
        graph.create_vertex(&vertex).expect("create person");
    }
    let mut city = Vertex::new(4);
    city.labels.push("city".into());
    city.properties
        .insert("name".into(), Value::Text("Paris".into()));
    graph.create_vertex(&city).expect("create city");

    let edges = [
        (100, 1, 2, "KNOWS", Some(5)),
        (101, 2, 3, "KNOWS", None),
        (102, 1, 4, "LIVES_IN", None),
        (103, 3, 1, "KNOWS", None),
    ];
    for (id, from, to, label, weight) in edges {
        let mut edge = Edge::new(id, from, to, label);
        if let Some(weight) = weight {
            edge.properties
                .insert("weight".into(), Value::Integer(weight));
        }
        graph.create_edge(&edge).expect("create edge");
    }
    (dir, graph)
}

fn rows(query: &str, graph: &GraphEngine) -> Vec<GremlinRow> {
    graph.gremlin(query).expect("query succeeds").rows
}

#[test]
fn v_and_haslabel_keep_start_order() {
    let (_dir, graph) = graph();
    assert_eq!(
        rows("g.V(2,1,2).hasLabel('person')", &graph),
        vec![
            GremlinRow::Vertex(2),
            GremlinRow::Vertex(1),
            GremlinRow::Vertex(2)
        ]
    );
    assert_eq!(rows("g.V(1L)", &graph), vec![GremlinRow::Vertex(1)]);
    assert_eq!(
        rows("g.V( 2 , 1 , 2 )", &graph),
        vec![
            GremlinRow::Vertex(2),
            GremlinRow::Vertex(1),
            GremlinRow::Vertex(2)
        ]
    );
    assert_eq!(
        rows("g.V()", &graph),
        vec![
            GremlinRow::Vertex(1),
            GremlinRow::Vertex(2),
            GremlinRow::Vertex(3),
            GremlinRow::Vertex(4)
        ]
    );
    assert!(rows("g.V(99)", &graph).is_empty());
}

#[test]
fn has_values_and_labels_are_typed() {
    let (_dir, graph) = graph();
    assert_eq!(
        rows(
            "g.V().hasLabel('person').has('name','Alice').values('name')",
            &graph
        ),
        vec![GremlinRow::Value(Value::Text("Alice".into()))]
    );
    assert_eq!(
        rows("g.V().has('age', 40).id()", &graph),
        vec![GremlinRow::Value(Value::Integer(2))]
    );
    assert_eq!(
        rows("g.V(1).label()", &graph),
        vec![GremlinRow::Value(Value::Text("person".into()))]
    );
    assert!(rows("g.V().has('missing').values('name')", &graph).is_empty());
    assert_eq!(
        rows("g.V().has('missing').count()", &graph),
        vec![GremlinRow::Count(0)]
    );
}

#[test]
fn traversal_is_bounded_and_paths_are_deterministic() {
    let (_dir, graph) = graph();
    assert_eq!(
        rows("g.V(1).out('KNOWS').id()", &graph),
        vec![GremlinRow::Value(Value::Integer(2))]
    );
    assert_eq!(
        rows("g.V(1).out('KNOWS').out('KNOWS').id()", &graph),
        vec![GremlinRow::Value(Value::Integer(3))]
    );
    assert_eq!(
        rows("g.V(1).both().count()", &graph),
        vec![GremlinRow::Count(3)]
    );
    assert_eq!(
        rows("g.V(1).out().limit(1).path()", &graph),
        vec![GremlinRow::Path(vec![
            PathElement::Vertex(1),
            PathElement::Edge(100),
            PathElement::Vertex(2),
        ])]
    );
}

#[test]
fn edge_start_and_edge_properties_work() {
    let (_dir, graph) = graph();
    assert_eq!(rows("g.E(100)", &graph), vec![GremlinRow::Edge(100)]);
    assert_eq!(
        rows("g.E(100).values('weight')", &graph),
        vec![GremlinRow::Value(Value::Integer(5))]
    );
    assert_eq!(
        rows("g.E().hasLabel('KNOWS').count()", &graph),
        vec![GremlinRow::Count(3)]
    );
    assert!(rows("g.E(99)", &graph).is_empty());
}

#[test]
fn limits_are_applied_to_the_current_rows() {
    let (_dir, graph) = graph();
    assert_eq!(
        rows("g.V().limit(2).count()", &graph),
        vec![GremlinRow::Count(2)]
    );
    assert_eq!(rows("g.V().limit(2)", &graph).len(), 2);
}

#[test]
fn default_limit_caps_rows_but_not_count() {
    let (_dir, graph) = graph();
    assert_eq!(rows("g.V().values('name')", &graph).len(), 4);
    assert_eq!(rows("g.V().count()", &graph), vec![GremlinRow::Count(4)]);
}

#[test]
fn unsupported_syntax_and_limits_are_errors() {
    let (_dir, graph) = graph();
    assert!(matches!(
        graph.gremlin("g.V().repeat(out())"),
        Err(Error::UnsupportedGremlinStep(_))
    ));
    assert!(matches!(
        graph.gremlin("g.V().limit(0)"),
        Err(Error::InvalidGremlinLimit(0))
    ));
    assert!(matches!(
        graph.gremlin("g.V().limit(1001)"),
        Err(Error::InvalidGremlinLimit(1001))
    ));
    assert!(matches!(
        graph.gremlin("g.V().out().out().out().out()"),
        Err(Error::TraversalLimitExceeded(_))
    ));
    assert!(matches!(
        graph.gremlin("g.E().out()"),
        Err(Error::InvalidGremlinTraverser(_))
    ));
    assert!(matches!(
        graph.gremlin("g.V().values('name').out()"),
        Err(Error::InvalidGremlinTraverser(_))
    ));
    assert!(matches!(
        graph.gremlin("g.V(); g.V()"),
        Err(Error::GremlinParse(_))
    ));
    assert!(matches!(
        graph.gremlin("g.V().has('name', within('Alice'))"),
        Err(Error::UnsupportedGremlinArgument(_)) | Err(Error::UnsupportedGremlinValue(_))
    ));
    assert!(matches!(
        graph.gremlin("g.V().limit('2')"),
        Err(Error::GremlinParse(_))
    ));
}

#[test]
fn quoted_properties_and_duplicate_ids_survive_literals() {
    let (_dir, graph) = graph();
    let mut vertex = Vertex::new(5);
    vertex.labels.push("odd".into());
    vertex
        .properties
        .insert("name".into(), Value::Text("O'Brien, Jr.".into()));
    graph.create_vertex(&vertex).expect("create odd vertex");
    assert_eq!(
        rows(
            "g.V(5).has('name', \"O'Brien, Jr.\").values('name')",
            &graph
        ),
        vec![GremlinRow::Value(Value::Text("O'Brien, Jr.".into()))]
    );
}

#[test]
fn default_limit_caps_a_large_start_scan() {
    let dir = tempfile::tempdir().expect("temp dir");
    let graph = GraphEngine::open(dir.path()).expect("open graph");
    for id in 1..=105 {
        let mut vertex = Vertex::new(id);
        vertex.labels.push("person".into());
        vertex
            .properties
            .insert("name".into(), Value::Text(format!("person-{id}")));
        graph.create_vertex(&vertex).expect("create vertex");
    }
    assert_eq!(rows("g.V()", &graph).len(), 100);
    assert_eq!(rows("g.V().count()", &graph), vec![GremlinRow::Count(105)]);
    assert_eq!(rows("g.V().limit(3)", &graph).len(), 3);
}

#[test]
fn terminal_steps_and_mutations_are_rejected() {
    let (_dir, graph) = graph();
    for query in [
        "g.V().count().limit(1)",
        "g.V().path().count()",
        "g.V(1).outE('KNOWS')",
        "g.V(1).addV('x')",
        "g.V().next()",
        "g.V().toList()",
    ] {
        assert!(
            matches!(
                graph.gremlin(query),
                Err(Error::UnsupportedGremlinArgument(_)) | Err(Error::UnsupportedGremlinStep(_))
            ),
            "query {query} should be rejected, got {:?}",
            graph.gremlin(query)
        );
    }
}

#[test]
fn queries_see_reopened_data() {
    let dir = tempfile::tempdir().expect("temp dir");
    {
        let graph = GraphEngine::open(dir.path()).expect("open graph");
        let mut vertex = Vertex::new(7);
        vertex.labels.push("person".into());
        vertex
            .properties
            .insert("name".into(), Value::Text("Persisted".into()));
        graph.create_vertex(&vertex).expect("create vertex");
    }
    let graph = GraphEngine::open(dir.path()).expect("reopen graph");
    assert_eq!(
        rows("g.V(7).values('name')", &graph),
        vec![GremlinRow::Value(Value::Text("Persisted".into()))]
    );
}

#[test]
fn high_fanout_is_bounded_before_later_limit_or_count() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphEngine::open(dir.path()).unwrap();
    graph.create_vertex(&Vertex::new(1)).unwrap();
    graph.create_vertex(&Vertex::new(2)).unwrap();
    // Parallel edges deliberately make a large fanout without a large start scan.
    for id in 0..10_000 {
        graph
            .create_edge(&Edge::new(id, 1, 2, if id % 2 == 0 { "Z" } else { "A" }))
            .unwrap();
    }
    assert_eq!(
        graph.gremlin("g.V(1).out().count()").unwrap().rows,
        vec![GremlinRow::Count(10_000)]
    );
    assert_eq!(
        graph.gremlin("g.V(2).in().count()").unwrap().rows,
        vec![GremlinRow::Count(10_000)]
    );
    for query in ["g.V(1,1).out().count()", "g.V(1).out().in().limit(1)"] {
        assert!(
            matches!(graph.gremlin(query), Err(Error::TraversalLimitExceeded(_))),
            "{query}"
        );
    }
    graph.create_edge(&Edge::new(10_000, 1, 2, "A")).unwrap();
    for query in [
        "g.V(1).out()",
        "g.V(2).in().count()",
        "g.V(1).both().limit(1)",
    ] {
        assert!(
            matches!(graph.gremlin(query), Err(Error::TraversalLimitExceeded(_))),
            "{query}"
        );
    }
}

#[test]
fn adjacency_order_and_self_loop_dedup_survive_streaming() {
    let dir = tempfile::tempdir().unwrap();
    let graph = GraphEngine::open(dir.path()).unwrap();
    for id in 1..=3 {
        graph.create_vertex(&Vertex::new(id)).unwrap();
    }
    for edge in [
        Edge::new(30, 1, 2, "A"),
        Edge::new(10, 1, 1, "Z"),
        Edge::new(20, 3, 1, "B"),
    ] {
        graph.create_edge(&edge).unwrap();
    }
    assert_eq!(
        graph.gremlin("g.V(1).both().limit(3)").unwrap().rows,
        vec![
            GremlinRow::Vertex(1),
            GremlinRow::Vertex(3),
            GremlinRow::Vertex(2)
        ]
    );
    assert_eq!(
        graph.gremlin("g.V(1).both().count()").unwrap().rows,
        vec![GremlinRow::Count(3)]
    );
}
