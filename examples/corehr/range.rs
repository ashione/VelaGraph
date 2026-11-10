//! Positional expectations derived only from fixture input bags/relationships.
use super::{partition, Model};
use serde_json::{json, Value};

pub(super) fn queries(m: &Model) -> Vec<Value> {
    let mut cases = Vec::new();
    let mut add = |name: &str, query: &str, expected: Value| {
        cases.push(json!({"name":name,"partition":partition(20),"query":query,
            "client_expected":expected,"server_supported":false,
            "server_expected_error":"Unsupported"}));
    };
    let starts = [26, 20, 26, 23];
    let vertices = |ids: Vec<u64>| json!({"vertices":ids.into_iter().map(|id| json!([partition(id),id])).collect::<Vec<_>>()});
    add(
        "range_explicit_bag",
        "g.V(26,20,26,23).range(1,3)",
        vertices(starts[1..3].to_vec()),
    );
    add(
        "range_before_dedup",
        "g.V(26,20,26,23).range(1,3).dedup()",
        vertices(vec![20, 26]),
    );
    let mut seen = std::collections::BTreeSet::new();
    let unique: Vec<_> = starts
        .into_iter()
        .filter(|id| seen.insert(*id))
        .skip(1)
        .take(2)
        .collect();
    add(
        "range_after_dedup",
        "g.V(26,20,26,23).dedup().range(1,3)",
        vertices(unique),
    );
    let values: Vec<_> = starts
        .into_iter()
        .filter(|id| *id == 26)
        .skip(1)
        .take(1)
        .map(|id| json!({"Integer":id}))
        .collect();
    add(
        "range_scalar_filter",
        "g.V(26,20,26,23).id().is(26).range(1,2)",
        json!({"values":values}),
    );
    let paths: Vec<_> = m
        .native_paths(&[20, 20], "reports_to", true, 2, true)
        .into_iter()
        .skip(1)
        .take(2)
        .map(|(_, path)| json!(path))
        .collect();
    add(
        "range_repeat_paths",
        "g.V(20,20).repeat(out('reports_to')).emit().times(2).range(1,3).path()",
        json!({"paths":paths}),
    );
    let maps: Vec<_> = starts.into_iter().take(3).skip(1).take(1).map(|id| json!({"source":[partition(id),id],"properties":{"name":[{"Text":format!("Employee-{id:06}")}]}})).collect();
    add(
        "range_limit_maps",
        "g.V(26,20,26,23).limit(3).range(1,2).valueMap('name')",
        json!({"maps":maps}),
    );
    add(
        "range_equal_empty",
        "g.V(26,20,26,23).range(2,2).count()",
        json!({"count":0}),
    );
    add(
        "range_overrun_empty",
        "g.V(26,20,26,23).range(9,10)",
        vertices(vec![]),
    );
    cases
}

#[test]
fn range_oracles_are_additive_ordered_and_server_rejected() {
    for (people, departments) in [(2000, 50), (200000, 5000)] {
        let model = Model::new(people, departments);
        let added = queries(&model);
        let all = model.queries();
        assert_eq!(added.len(), 8);
        assert_eq!(all.len(), 140);
        assert_eq!(&all[132..], added.as_slice());
        assert!(all[..132]
            .iter()
            .all(|q| !q["name"].as_str().unwrap().starts_with("range_")));
        assert_ne!(added[1]["client_expected"], added[2]["client_expected"]);
        assert_eq!(
            added[0]["client_expected"],
            json!({"vertices":[[3,20],[3,26]]})
        );
        assert_eq!(
            added[4]["client_expected"]["paths"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        for case in added {
            assert_eq!(case["server_supported"], false);
            assert_eq!(case["server_expected_error"], "Unsupported");
            for key in ["expect_error", "client_expect_unsupported", "client_phase"] {
                assert!(case.get(key).is_none());
            }
        }
    }
}

#[test]
fn existing_offset_pagination_is_admitted_without_moving_or_weakening_server_boundary() {
    for (people, departments) in [(2000, 50), (200000, 5000)] {
        let cases = Model::new(people, departments).queries();
        let index = cases
            .iter()
            .position(|q| q["name"] == "offset_pagination")
            .unwrap();
        let case = &cases[index];
        assert!(index < 132);
        assert_eq!(cases[index - 1]["name"], "recursive_repeat");
        assert_eq!(cases[index + 1]["name"], "sort");
        assert_eq!(case["query"], "g.V(1).in('reports_to').range(2,5)");
        // Input hierarchy gives manager 1 reports 2..=9, so positions 2..5
        // select employees 4, 5, 6 with their distinct qualified partitions.
        assert_eq!(
            case["client_expected"],
            json!({"vertices":[[2,4],[3,5],[1,6]]})
        );
        assert_eq!(case["server_supported"], false);
        assert_eq!(case["server_expected_error"], "Unsupported");
        assert!(case.get("expect_error").is_none());
        assert!(case.get("client_expect_unsupported").is_none());
        assert_eq!(cases[index - 1]["expect_error"], true);
        assert_eq!(cases[index + 1]["expect_error"], true);
    }
}
