#[cfg(feature = "query-profiling")]
#[test]
fn repeated_bindings_do_not_render_static_query_text() {
    use velagraph::{GremlinBindings, GremlinParameters, GremlinQuery, GremlinStart, Value};
    velagraph::query_profile::begin();
    let query = GremlinQuery::parse("g.V(1).has('name','initial').out('LINK').id()")
        .unwrap()
        .prepare(GremlinParameters {
            start_ids: true,
            has_value_steps: vec![0],
        })
        .unwrap();
    assert_eq!(velagraph::query_profile::finish().prepared_text_renders, 1);
    velagraph::query_profile::begin();
    for id in [1, 20, 300] {
        let bound = query
            .bind(GremlinBindings {
                start_ids: Some(&[id]),
                values: &[Value::Text("changed".into())],
            })
            .unwrap();
        assert_eq!(bound.start(), &GremlinStart::Vertices(vec![id]));
    }
    let profile = velagraph::query_profile::finish();
    assert_eq!(profile.prepared_text_renders, 0);
}
