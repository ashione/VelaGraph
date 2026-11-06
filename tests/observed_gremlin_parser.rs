use rocksdb_sql_table::{
    Error, GremlinQuery, GremlinStart, GremlinStep, ObservedGremlinQuery, ObservedGremlinStep,
    MAX_GREMLIN_QUERY_BYTES,
};

#[test]
fn observed_edge_steps_use_official_ast_and_leave_v0_unchanged() {
    for (name, bare, labelled) in [
        (
            "outE",
            ObservedGremlinStep::OutE(None),
            ObservedGremlinStep::OutE(Some("knows".into())),
        ),
        (
            "inE",
            ObservedGremlinStep::InE(None),
            ObservedGremlinStep::InE(Some("knows".into())),
        ),
        (
            "bothE",
            ObservedGremlinStep::BothE(None),
            ObservedGremlinStep::BothE(Some("knows".into())),
        ),
    ] {
        for (args, expected) in [
            ("", bare),
            ("'knows'", labelled.clone()),
            ("\"knows\"", labelled),
        ] {
            let text = format!("g.V(2,1).{name}({args})");
            let query = ObservedGremlinQuery::parse(&text).unwrap();
            assert_eq!(query.start(), &GremlinStart::Vertices(vec![2, 1]));
            assert_eq!(query.steps(), &[expected]);
            assert!(matches!(
                GremlinQuery::parse(&text),
                Err(Error::UnsupportedGremlinStep(_))
            ));
        }
    }
    for (name, expected) in [
        ("inV", ObservedGremlinStep::InV),
        ("outV", ObservedGremlinStep::OutV),
        ("bothV", ObservedGremlinStep::BothV),
    ] {
        let text = format!("g.E(8).{name}()");
        let query = ObservedGremlinQuery::parse(&text).unwrap();
        assert_eq!(query.start(), &GremlinStart::Edges(vec![8]));
        assert_eq!(query.steps(), &[expected]);
        assert!(matches!(
            GremlinQuery::parse(&text),
            Err(Error::UnsupportedGremlinStep(_))
        ));
        assert!(ObservedGremlinQuery::parse(&format!("g.E(8).{name}('x')")).is_err());
    }
}

#[test]
fn observed_step_arguments_are_validated() {
    for name in ["outE", "inE", "bothE"] {
        for args in [
            "'x','y'",
            "''",
            "'bad/label'",
            "'9bad'",
            "null",
            "123",
            "label",
            "['x']",
        ] {
            assert!(
                ObservedGremlinQuery::parse(&format!("g.V(1).{name}({args})")).is_err(),
                "{name}({args})"
            );
        }
        assert!(
            ObservedGremlinQuery::parse(&format!("g.V(1).{name}('{}')", "x".repeat(65))).is_err()
        );
    }
}

#[test]
fn observed_queries_share_bounds_and_terminal_rules() {
    let query =
        ObservedGremlinQuery::parse("g.V(1).outE().inV().out().inE().outV().count()").unwrap();
    assert_eq!(
        query.steps().last(),
        Some(&ObservedGremlinStep::Existing(GremlinStep::Count))
    );
    for text in [
        "g.V(1).outE().inV().out().inE().outV().both()",
        "g.V(1).outE().inE().bothE().outE()",
    ] {
        assert!(matches!(
            ObservedGremlinQuery::parse(text),
            Err(Error::TraversalLimitExceeded(_))
        ));
    }
    for text in ["g.V(1).count().outE()", "g.E(1).path().inV()"] {
        assert!(matches!(
            ObservedGremlinQuery::parse(text),
            Err(Error::UnsupportedGremlinArgument(_))
        ));
    }
    for text in [
        "g.V(1).outE().next()",
        "g.V(1).outE().toList()",
        "g.E(1).otherV()",
        "g.V(1).repeat(__.outE())",
        "g.V(1).outE();g.V()",
        "g.V(1).outE() garbage",
        "g.V(1).outE().",
        "g.withBulk(false).V(1).outE()",
    ] {
        assert!(ObservedGremlinQuery::parse(text).is_err(), "{text}");
    }
    // Endpoint-only sequences have no separate hop cap, but cannot bypass the
    // shared pre-parser structural bound. Type checks belong to execution.
    ObservedGremlinQuery::parse(&format!("g.E(1){}", ".inV()".repeat(20))).unwrap();
    assert!(matches!(
        ObservedGremlinQuery::parse(&format!("g.E(1){}", ".inV()".repeat(21))),
        Err(Error::GremlinComplexityLimit { .. })
    ));
    assert!(matches!(
        ObservedGremlinQuery::parse(&"x".repeat(MAX_GREMLIN_QUERY_BYTES + 1)),
        Err(Error::GremlinParse(_))
    ));
}

#[test]
fn observed_existing_steps_preserve_v0_ast() {
    for text in [
        "g.V()",
        "g.E(3,2).hasLabel('x').has('n',2).values('n').limit(7)",
        "g.V(1).out('x').in().both().path()",
        "g.V().id().count()",
        "g.E().label()",
    ] {
        let legacy = GremlinQuery::parse(text).unwrap();
        let observed = ObservedGremlinQuery::parse(text).unwrap();
        assert_eq!(legacy.start(), observed.start());
        assert_eq!(
            observed.steps(),
            legacy
                .steps()
                .iter()
                .cloned()
                .map(ObservedGremlinStep::Existing)
                .collect::<Vec<_>>()
        );
    }
}
