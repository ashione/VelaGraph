#![cfg(feature = "query-profiling")]
use velagraph::{query_profile, temporal_store::TemporalStore};
#[test]
fn records_real_query_and_preserves_error_path() {
    let dir = tempfile::tempdir().unwrap();
    let store = TemporalStore::open(dir.path()).unwrap();
    let view = store.view(Some(0), 0).unwrap();
    query_profile::begin();
    assert!(view
        .execute_gremlin("g.V(1).path()")
        .unwrap()
        .rows
        .is_empty());
    let p = query_profile::finish();
    assert_eq!(p.calls, 1);
    assert!(p.api_ns >= p.parse_ns + p.execute_ns);
    assert!(p.parse_ns > 0);
    assert!(p.execute_ns > 0);
    query_profile::begin();
    assert!(view.execute_gremlin("not Gremlin").is_err());
    let p = query_profile::finish();
    assert_eq!(p.calls, 1);
    assert_eq!(p.execute_ns, 0);
    assert_eq!(query_profile::finish().calls, 0);
}
