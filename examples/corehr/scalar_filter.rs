//! Scalar-filter expectations from fixture-owned IDs and relationships only.
//! Scalar rows use the runner's ordered `values` contract (not a set/count-only
//! substitute). The runner does not expose scalar provenance in that contract.
use super::{partition, Model};
use serde_json::{json, Value};

pub(super) fn queries(m: &Model) -> Vec<Value> {
    let mut cases = Vec::new();
    let mut add = |name: &str, query: &str, expected: Value| {
        cases.push(json!({"name":name,"partition":partition(20),"query":query,
            "client_expected":expected,"server_supported":false,
            "server_expected_error":"Unsupported"}));
    };
    // One partition, intentionally unsorted duplicate starts. Filtering must
    // retain both occurrences rather than silently deduplicate the scalar bag.
    let starts = [26_u64, 20, 26, 23];
    assert!(starts
        .iter()
        .all(|id| *id <= m.people && partition(*id) == partition(20)));
    let ids: Vec<_> = starts
        .iter()
        .filter(|id| **id == 26)
        .map(|id| json!({"Integer":id}))
        .collect();
    add(
        "scalar_is_explicit_id_bag",
        "g.V(26,20,26,23).id().is(26)",
        json!({"values":ids}),
    );
    // The input model's IDs are integers: no Integer/Real coercion is allowed.
    add(
        "scalar_is_strict_real_mismatch",
        "g.V(26,20,26,23).id().is(26.0)",
        json!({"values":[]}),
    );

    // The global client source is local-ID ordered. Cap it before projection,
    // so this exercises global routing without adding a fixture-wide scan.
    let labels: Vec<_> = (1..=m.people + m.departments)
        .take(12)
        .map(|id| {
            if id <= m.people {
                "person"
            } else {
                "department"
            }
        })
        .filter(|label| *label == "person")
        .collect();
    let mut unique = std::collections::BTreeSet::new();
    let count = labels
        .into_iter()
        .filter(|label| unique.insert(*label))
        .take(1)
        .count();
    add(
        "scalar_is_global_label_unique_count",
        "g.V().limit(12).label().is('person').dedup().limit(1).count()",
        json!({"count":count}),
    );

    // multi_department is a fixture Boolean formula, independent of the engine.
    // The first false precedes true: moving limit across is changes the result.
    let values = [23_u64, 20, 23, 20].map(|id| id.is_multiple_of(5));
    let before: Vec<_> = values
        .iter()
        .take(1)
        .filter(|value| **value)
        .map(|value| json!({"Boolean":value}))
        .collect();
    let after: Vec<_> = values
        .iter()
        .filter(|value| **value)
        .take(1)
        .map(|value| json!({"Boolean":value}))
        .collect();
    add(
        "scalar_is_limit_before_filter",
        "g.V(23,20,23,20).values('multi_department').limit(1).is(true)",
        json!({"values":before}),
    );
    add(
        "scalar_is_filter_before_limit",
        "g.V(23,20,23,20).values('multi_department').is(true).limit(1)",
        json!({"values":after}),
    );

    // corehr_fixture.rs encodes every CrossEdge with properties: BTreeMap::new().
    // Mirror that input fact here: values skips an absent key, so is(null) must
    // not manufacture Null rows even when the edge input bag has duplicates.
    let edge_properties: Vec<std::collections::BTreeMap<&str, Value>> = [20, 20]
        .iter()
        .flat_map(|id| {
            m.edges
                .iter()
                .filter(move |edge| edge.from == *id && edge.label == "member_of")
        })
        .map(|_| std::collections::BTreeMap::new())
        .collect();
    assert!(edge_properties.len() > 1);
    assert!(edge_properties
        .iter()
        .all(|properties| properties.is_empty()));
    let missing: Vec<_> = edge_properties
        .iter()
        .filter_map(|properties| properties.get("missing"))
        .filter(|value| **value == json!("Null"))
        .take(2)
        .cloned()
        .collect();
    add(
        "scalar_is_edge_missing_null",
        "g.V(20,20).outE('member_of').values('missing').is(null).dedup().limit(2)",
        json!({"values":missing}),
    );
    cases
}

#[test]
fn scalar_filter_oracles_preserve_order_types_and_rejection_contract() {
    for (people, departments) in [(2000, 50), (200000, 5000)] {
        let m = Model::new(people, departments);
        let q = queries(&m);
        assert_eq!(q.len(), 6);
        let expected = [
            json!({"values":[{"Integer":26},{"Integer":26}]}),
            json!({"values":[]}),
            json!({"count":1}),
            json!({"values":[]}),
            json!({"values":[{"Boolean":true}]}),
            json!({"values":[]}),
        ];
        for (case, expected) in q.iter().zip(expected) {
            assert_eq!(case["client_expected"], expected, "{}", case["name"]);
            assert_eq!(case["server_supported"], false);
            assert_eq!(case["server_expected_error"], "Unsupported");
            assert!(case.get("client_expect_unsupported").is_none());
            assert!(case.get("expect_error").is_none());
            assert!(case.get("client_phase").is_none());
        }
        assert_ne!(q[3]["client_expected"], q[4]["client_expected"]);
    }
}

#[test]
fn scalar_filter_matrix_is_appended_without_replacing_prior_cases() {
    let m = Model::new(2000, 50);
    let all = m.queries();
    let added = queries(&m);
    assert_eq!(all.len(), 126 + added.len());
    assert_eq!(&all[126..], added.as_slice());
    assert!(all[..126]
        .iter()
        .all(|case| !case["name"].as_str().unwrap().starts_with("scalar_is_")));
    assert!(all[..126]
        .iter()
        .any(|case| case["name"] == "client_scalar_id_dedup"));
    assert!(all[..126]
        .iter()
        .any(|case| case["name"] == "global_join_reject_path"));
}
