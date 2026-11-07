use rocksdb_sql_table::{
    Error, GremlinQuery, GremlinStart, GremlinStep, ObservedGremlinQuery, ObservedGremlinStep,
    MAX_OBSERVED_GREMLIN_REPEAT_TIMES,
};

fn repeat(incoming: bool, label: Option<&str>, times: usize, emit: bool) -> ObservedGremlinStep {
    ObservedGremlinStep::Repeat {
        incoming,
        label: label.map(str::to_owned),
        times,
        emit,
    }
}

#[test]
fn repeat_coalesces_official_anonymous_body_and_post_modulators() {
    for (body, incoming, label) in [
        ("__.out()", false, None),
        ("out()", false, None),
        ("__.in()", true, None),
        ("in()", true, None),
        ("__.out('reports')", false, Some("reports")),
        ("out(\"reports\")", false, Some("reports")),
        ("__.in(\"reports\")", true, Some("reports")),
        ("in('reports')", true, Some("reports")),
    ] {
        for (modulators, emit) in [
            ("times(2)", false),
            ("emit().times(2)", true),
            ("times(2).emit()", true),
        ] {
            let text = format!("g.V(2,1,2).repeat({body}).{modulators}");
            let query = ObservedGremlinQuery::parse(&text).unwrap();
            assert_eq!(query.start(), &GremlinStart::Vertices(vec![2, 1, 2]));
            assert_eq!(query.steps(), &[repeat(incoming, label, 2, emit)], "{text}");
            assert!(matches!(
                GremlinQuery::parse(&text),
                Err(Error::UnsupportedGremlinStep(_))
            ));
        }
    }
    assert_eq!(
        ObservedGremlinQuery::parse(
            " g . V ( 1 ) . repeat ( __ . in ( 'reports' ) ) . emit ( ) . times ( 2L ) "
        )
        .unwrap()
        .steps(),
        &[repeat(true, Some("reports"), 2, true)]
    );
}

#[test]
fn repeat_iteration_bounds_are_typed_and_separate_from_unrolled_hops() {
    assert_eq!(MAX_OBSERVED_GREMLIN_REPEAT_TIMES, 32);
    for times in [1, MAX_OBSERVED_GREMLIN_REPEAT_TIMES] {
        let text = format!("g.V(1).repeat(__.out()).times({times})");
        assert_eq!(
            ObservedGremlinQuery::parse(&text).unwrap().steps(),
            &[repeat(false, None, times, false)]
        );
    }
    for times in ["0", "-1", "0L"] {
        assert!(matches!(
            ObservedGremlinQuery::parse(&format!("g.V(1).repeat(__.out()).times({times})")),
            Err(Error::UnsupportedGremlinArgument(_))
        ));
    }
    for times in ["33", "33L", "9223372036854775807"] {
        assert!(matches!(
            ObservedGremlinQuery::parse(&format!("g.V(1).repeat(__.out()).times({times})")),
            Err(Error::TraversalLimitExceeded(_))
        ));
    }
    // Combined-depth admission belongs to the server coordinator. The parser
    // retains the old three-step bound on ordinary adjacency outside the repeat.
    ObservedGremlinQuery::parse("g.V(1).out().repeat(__.in()).times(32).outE().inV().in()")
        .unwrap();
    for text in [
        "g.V(1).out().repeat(__.in()).times(1).in().outE().inV().out()",
        "g.V(1).out().out().out().out()",
    ] {
        assert!(matches!(
            ObservedGremlinQuery::parse(text),
            Err(Error::TraversalLimitExceeded(_))
        ));
    }
}

#[test]
fn repeat_rejects_unbounded_pre_modulated_and_multiple_repeat_forms() {
    for text in [
        "g.V(1).repeat(__.out())",
        "g.V(1).repeat(__.out()).emit()",
        "g.V(1).repeat(__.out()).out().times(2)",
        "g.V(1).repeat(__.out()).emit().out().times(2)",
        "g.V(1).emit().repeat(__.out()).times(2)",
        "g.V(1).times(2).repeat(__.out())",
        "g.V(1).until(__.has('x')).repeat(__.out()).times(2)",
        "g.V(1).repeat(__.out()).until(__.has('x')).times(2)",
        "g.V(1).repeat(__.out()).times(2).until(__.has('x'))",
        "g.V(1).repeat(__.out()).emit(__.has('x')).times(2)",
        "g.V(1).repeat(__.out()).times(2).emit(__.has('x'))",
        "g.V(1).repeat(__.out()).emit().times(2).emit()",
        "g.V(1).repeat(__.out()).emit().emit().times(2)",
        "g.V(1).repeat(__.out()).times(2).times(3)",
        "g.V(1).repeat(__.out()).times(2).repeat(__.in()).times(2)",
        "g.V(1).repeat(__.out()).times(2).out().repeat(__.in()).times(2)",
        "g.V(1).repeat(__.out()).times(2).as('x')",
        "g.V(1).repeat(__.out()).times(2).by('x')",
        "g.V(1).repeat('x',__.out()).times(2)",
    ] {
        assert!(ObservedGremlinQuery::parse(text).is_err(), "{text}");
    }
}

#[test]
fn repeat_body_is_exactly_one_vertex_adjacency() {
    for body in [
        "__.out().in()",
        "__.out().has('x')",
        "__.out().dedup()",
        "__.out().limit(1)",
        "__.outE().inV()",
        "__.outE()",
        "__.inE()",
        "__.both()",
        "__.V(1)",
        "__.values('x')",
        "__.has('x','out()')",
        "__.local(__.out())",
        "__.repeat(__.out()).times(2)",
        "__.to(OUT)",
        "__.to(Direction.IN,'reports')",
        "__.out('a','b')",
        "__.out('')",
        "__.out('bad/label')",
        "__.in('9bad')",
        "__.out(null)",
        "__.out(1)",
        "__.out(label)",
    ] {
        let text = format!("g.V(1).repeat({body}).times(2)");
        assert!(ObservedGremlinQuery::parse(&text).is_err(), "{text}");
    }
}

#[test]
fn repeat_retains_scalar_structure_and_terminal_validation() {
    let query = ObservedGremlinQuery::parse(
        "g.V(1).hasLabel('person').repeat(__.out()).emit().times(2).dedup().values('name').limit(5).count()",
    )
    .unwrap();
    assert_eq!(
        query.steps(),
        &[
            ObservedGremlinStep::Existing(GremlinStep::HasLabel(vec!["person".into()])),
            repeat(false, None, 2, true),
            ObservedGremlinStep::Dedup,
            ObservedGremlinStep::Existing(GremlinStep::Values("name".into())),
            ObservedGremlinStep::Existing(GremlinStep::Limit(5)),
            ObservedGremlinStep::Existing(GremlinStep::Count),
        ]
    );
    // Stream typing is intentionally not the parser's job. The executor must
    // reject this shape before reads, even if a scalar projection emits no rows.
    assert_eq!(
        ObservedGremlinQuery::parse("g.V(1).values('x').repeat(__.out()).times(2)")
            .unwrap()
            .steps(),
        &[
            ObservedGremlinStep::Existing(GremlinStep::Values("x".into())),
            repeat(false, None, 2, false),
        ]
    );
    ObservedGremlinQuery::parse("g.V(1).repeat(__.in()).times(2).path()").unwrap();
    for text in [
        "g.V(1).count().repeat(__.out()).times(2)",
        "g.V(1).path().repeat(__.out()).times(2)",
        "g.V(1).repeat(__.out()).times(2).count().limit(1)",
        "g.V(1).repeat(__.out()).times(2).path().dedup()",
    ] {
        assert!(
            matches!(
                ObservedGremlinQuery::parse(text),
                Err(Error::UnsupportedGremlinArgument(_))
            ),
            "{text}"
        );
    }
}

#[test]
fn repeat_malformed_and_non_literal_forms_do_not_fall_back() {
    for text in [
        "g.V(1).repeat().times(2)",
        "g.V(1).repeat(__.out(.times(2)",
        "g.V(1).repeat(__.out()).times()",
        "g.V(1).repeat(__.out()).times(2,3)",
        "g.V(1).repeat(__.out()).times(2.0)",
        "g.V(1).repeat(__.out()).times('2')",
        "g.V(1).repeat(__.out()).times(n)",
        "g.V(1).repeat(__.out()).times(0x20)",
        "g.V(1).repeat(__.out()).times(9223372036854775808)",
        "g.V(1).repeat(__.in_()).times(2)",
        "g.V(1).repeat(g.V(2).out()).times(2)",
        "g.V(1).repeat(__.out()).times(2) garbage",
        "g.V(1).repeat(__.out()).times(2).",
        "g.V(1).repeat(__.out()).times(2).toList()",
        "g.V(1).repeat(__.out()).times(2);g.V()",
    ] {
        assert!(ObservedGremlinQuery::parse(text).is_err(), "{text}");
    }
}

#[test]
fn repeat_shares_raw_structural_admission() {
    let suffix = ").repeat(__.out()).emit().times(32)";
    let one = format!("g.V(1{suffix}");
    let marks = one.bytes().filter(|b| b"()[]{},.:;".contains(b)).count();
    let ids = vec!["1"; 65 - marks].join(",");
    let at_limit = format!("g.V({ids}{suffix}");
    assert_eq!(
        at_limit
            .bytes()
            .filter(|b| b"()[]{},.:;".contains(b))
            .count(),
        64
    );
    ObservedGremlinQuery::parse(&at_limit).unwrap();
    assert!(matches!(
        ObservedGremlinQuery::parse(&format!("g.V(1,{ids}{suffix}")),
        Err(Error::GremlinComplexityLimit {
            marks: 65,
            limit: 64
        })
    ));
}
