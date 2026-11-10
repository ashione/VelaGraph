//! Global-filter/relationship expectations from model IDs and input edges only.
//! The two APIs intentionally have different source orders.
use super::{active, mix, partition, Model};
use serde_json::{json, Value};
use std::collections::BTreeSet;

fn source(m: &Model, server: bool) -> Vec<u64> {
    let mut ids: Vec<_> = (1..=m.people + m.departments).collect();
    if server {
        ids.sort_by_key(|id| (partition(*id), *id));
    }
    ids
}
fn unique(ids: Vec<u64>) -> Vec<u64> {
    let mut seen = BTreeSet::new();
    ids.into_iter()
        .filter(|id| seen.insert((partition(*id), *id)))
        .collect()
}
fn vertices(ids: &[u64]) -> Value {
    json!({"vertices":ids.iter().map(|id| json!([partition(*id),id])).collect::<Vec<_>>()})
}
fn maps(m: &Model, ids: &[u64]) -> Value {
    json!({"maps":ids.iter().map(|id| json!({
        "source":[partition(*id),id],
        "properties":{
            "name":[{"Text":format!("{}-{id:06}",if *id<=m.people {"Employee"} else {"Department"})}],
            "active":[{"Boolean":active(*id)}]
        }
    })).collect::<Vec<_>>()})
}

pub(super) fn queries(m: &Model) -> Vec<Value> {
    let mut cases = Vec::new();
    let mut add = |name: &str, query: String, expected: Value, client: Option<Value>| {
        let mut row = json!({"name":name,"partition":1,"query":query,"expected":expected,"server_supported":true});
        if let Some(value) = client {
            row["client_expected"] = value;
            if name.starts_with("global_repeat_") {
                row["client_phase"] = json!("repeat_global");
            }
        } else {
            row["client_expect_unsupported"] = json!(true);
        }
        cases.push(row);
    };
    let selected_people: Vec<_> = source(m, true)
        .into_iter()
        .filter(|id| *id <= m.people && active(*id))
        .take(25)
        .collect();
    add("global_scan_selected_maps",
        "g.V().hasLabel('person').has('active',true).dedup().limit(25).valueMap('name','active','missing')".into(),
        maps(m, &selected_people), None);
    add(
        "global_scan_dedup_count",
        "g.V().dedup().count()".into(),
        json!({"count":m.people + m.departments}),
        Some(json!({"count":m.people + m.departments})),
    );
    let named = m.hop(&[20], "member_of", true);
    add(
        "global_join_named_memberships",
        "g.V().has('name','Employee-000020').limit(1).out('member_of')".into(),
        vertices(&named),
        Some(vertices(&named)),
    );
    let selected = |server: bool| {
        source(m, server)
            .into_iter()
            .filter(|id| {
                *id <= m.people && active(*id) && mix(*id ^ 0x434f554e545259).is_multiple_of(5)
            })
            .take(12)
            .collect::<Vec<_>>()
    };
    let server = m.hop(&selected(true), "reports_to", true);
    let client = m.hop(&selected(false), "reports_to", true);
    add("global_prefix_dedup_manager_bag_count",
        "g.V().hasLabel('person').has('active',true).has('country','CN').dedup().limit(12).out('reports_to').count()".into(),
        json!({"count":server.len()}), Some(json!({"count":client.len()})));
    let prefix = "g.V().hasLabel('person').has('active',true).has('country','CN').limit(12).out('reports_to')";
    add(
        "global_join_filtered_manager_rows",
        prefix.into(),
        vertices(&server),
        Some(vertices(&client)),
    );
    add(
        "global_join_filtered_manager_bag_count",
        format!("{prefix}.count()"),
        json!({"count":server.len()}),
        Some(json!({"count":client.len()})),
    );
    add(
        "global_join_filtered_manager_unique_count",
        format!("{prefix}.dedup().count()"),
        json!({"count":unique(server).len()}),
        Some(json!({"count":unique(client).len()})),
    );
    let memberships = unique(m.hop(&selected(true), "member_of", true));
    add("global_join_department_maps", "g.V().hasLabel('person').has('active',true).has('country','CN').limit(12).out('member_of').dedup().valueMap('name','active','missing')".into(), maps(m,&memberships), None);
    let dept = m.people + 2;
    let members: Vec<_> = m
        .hop(&[dept], "member_of", false)
        .into_iter()
        .filter(|id| active(*id))
        .take(25)
        .collect();
    add("global_join_named_department_members", format!("g.V().has('name','Department-{dept:06}').limit(1).in('member_of').has('active',true).limit(25).valueMap('name','active','missing')"), maps(m,&members), None);
    let colleagues = unique(m.hop(&named, "member_of", false));
    add("global_join_two_hop_unique_colleagues", "g.V().has('name','Employee-000020').limit(1).out('member_of').in('member_of').dedup().count()".into(), json!({"count":colleagues.len()}), Some(json!({"count":colleagues.len()})));
    add(
        "global_join_empty_seed",
        "g.V().has('name','NoSuchEmployee').out('reports_to').count()".into(),
        json!({"count":0}),
        Some(json!({"count":0})),
    );
    // The server's first person is ID 3; choose the opposite Boolean so limit
    // before filtering has no seed while filtering before limit has one.
    let wanted = !active(3);
    for before in [true, false] {
        let select = |server: bool| {
            let people = source(m, server).into_iter().filter(|id| *id <= m.people);
            if before {
                people
                    .take(1)
                    .filter(|id| active(*id) == wanted)
                    .collect::<Vec<_>>()
            } else {
                people
                    .filter(|id| active(*id) == wanted)
                    .take(1)
                    .collect::<Vec<_>>()
            }
        };
        let query = if before {
            format!(
                "g.V().hasLabel('person').limit(1).has('active',{wanted}).out('member_of').count()"
            )
        } else {
            format!(
                "g.V().hasLabel('person').has('active',{wanted}).limit(1).out('member_of').count()"
            )
        };
        add(
            if before {
                "global_join_limit_before_filter"
            } else {
                "global_join_filter_before_limit"
            },
            query,
            json!({"count":m.hop(&select(true),"member_of",true).len()}),
            Some(json!({"count":m.hop(&select(false),"member_of",true).len()})),
        );
    }
    // Input-only breadth-first oracle, independent of the execution plan.
    let repeat = |roots: &[u64], label: &str, outgoing: bool, depth: usize, emit: bool| {
        let mut frontier = roots.to_vec();
        let mut emitted = Vec::new();
        for _ in 0..depth {
            frontier = m.hop(&frontier, label, outgoing);
            if emit {
                emitted.extend(frontier.iter().copied());
            }
        }
        if emit {
            emitted
        } else {
            frontier
        }
    };
    let start = m.people;
    let prefix = format!("g.V().has('name','Employee-{start:06}').limit(1)");
    let ancestors = repeat(&[start], "reports_to", true, 32, true);
    add(
        "global_repeat_named_ancestors",
        format!("{prefix}.repeat(__.out('reports_to')).times(32).emit()"),
        vertices(&ancestors),
        Some(vertices(&ancestors)),
    );
    let filtered: Vec<_> = ancestors
        .iter()
        .copied()
        .filter(|id| active(*id))
        .take(3)
        .collect();
    add("global_repeat_filtered_ancestor_maps", format!("{prefix}.repeat(__.out('reports_to')).emit().times(32).has('active',true).limit(3).valueMap('name','active','missing')"), maps(m,&filtered), None);
    let exact = repeat(&[start], "reports_to", true, 4, false);
    add(
        "global_repeat_exact_depth",
        format!("{prefix}.repeat(__.out('reports_to')).times(4)"),
        vertices(&exact),
        Some(vertices(&exact)),
    );
    let descendants = repeat(&[1], "reports_to", false, 3, true);
    add("global_repeat_named_descendant_count", "g.V().has('name','Employee-000001').limit(1).repeat(__.in('reports_to')).times(3).emit().count()".into(), json!({"count":descendants.len()}), Some(json!({"count":descendants.len()})));
    let roots = selected(true);
    let shared = repeat(&roots, "reports_to", true, 4, true);
    let client_shared = repeat(&selected(false), "reports_to", true, 4, true);
    let prefix = "g.V().hasLabel('person').has('active',true).has('country','CN').limit(12).repeat(__.out('reports_to')).times(4).emit()";
    add(
        "global_repeat_multi_root_bag",
        format!("{prefix}.count()"),
        json!({"count":shared.len()}),
        Some(json!({"count":client_shared.len()})),
    );
    add(
        "global_repeat_multi_root_unique",
        format!("{prefix}.dedup().count()"),
        json!({"count":unique(shared).len()}),
        Some(json!({"count":unique(client_shared).len()})),
    );
    let departments = m.hop(&[20], "member_of", true);
    let parents = unique(repeat(&departments, "parent_department", true, 4, true));
    add("global_repeat_membership_then_department_hierarchy", "g.V().has('name','Employee-000020').limit(1).out('member_of').repeat(__.out('parent_department')).times(4).emit().dedup()".into(), vertices(&parents), Some(vertices(&parents)));
    add(
        "global_repeat_missing_root",
        "g.V().has('name','NoSuchEmployee').repeat(__.out()).times(32).count()".into(),
        json!({"count":0}),
        Some(json!({"count":0})),
    );
    cases.push(
        json!({"name":"global_scan_reject_scalar_dedup","partition":1,
        "query":"g.V().hasLabel('person').limit(1).values('name').dedup().count()",
        "client_expected":{"count":usize::from(m.people > 0)},
        "server_supported":false,"server_expected_error":"Unsupported"}),
    );
    // These explicit starts are all in one partition. Derive the complete
    // first-occurrence scalar result from the input IDs, never engine output.
    let starts = [26, 20, 26, 23];
    assert!(starts.iter().all(|id| partition(*id) == partition(20)));
    let ids = unique(starts.to_vec());
    for (name, projection, values) in [
        (
            "client_scalar_name_dedup",
            "values('name')",
            ids.iter()
                .map(|id| json!({"Text":format!("Employee-{id:06}")}))
                .collect::<Vec<_>>(),
        ),
        (
            "client_scalar_id_dedup",
            "id()",
            ids.iter().map(|id| json!({"Integer":id})).collect(),
        ),
        (
            "client_scalar_label_dedup",
            "label()",
            vec![json!({"Text":"person"})],
        ),
    ] {
        cases.push(json!({"name":name,"partition":partition(20),
            "query":format!("g.V(26,20,26,23).{projection}.dedup()"),
            "client_expected":{"values":values},
            "server_supported":false,"server_expected_error":"Unsupported"}));
    }
    // Global paths remain outside the coordinator subset, but are supported by
    // fixed-version v3. Derive the client path from source order and input edges.
    let first = source(m, false)
        .into_iter()
        .find(|id| *id <= m.people)
        .unwrap();
    let mut seen = BTreeSet::new();
    let paths = m
        .edges
        .iter()
        .enumerate()
        .filter(|(_, edge)| edge.from == first && edge.label == "member_of")
        .filter(|(_, edge)| seen.insert((partition(edge.to), edge.to)))
        .map(|(index, edge)| {
            json!([
                ["v", partition(first), first],
                ["e", partition(first), index + 1],
                ["v", partition(edge.to), edge.to]
            ])
        })
        .collect::<Vec<_>>();
    cases.push(json!({"name":"global_join_reject_path","partition":1,
        "query":"g.V().hasLabel('person').limit(1).out('member_of').dedup().path()",
        "client_expected":{"paths":paths},
        "server_supported":false,"server_expected_error":"Unsupported"}));
    cases
}

#[test]
fn global_relationship_oracles_are_nonvacuous() {
    for (people, departments) in [(2000, 50), (200000, 5000)] {
        let m = Model::new(people, departments);
        let q = queries(&m);
        let get = |name: &str| q.iter().find(|r| r["name"] == name).unwrap();
        assert_eq!(q.len(), 26);
        for name in [
            "global_repeat_named_ancestors",
            "global_repeat_exact_depth",
            "global_repeat_named_descendant_count",
            "global_repeat_multi_root_bag",
            "global_repeat_multi_root_unique",
            "global_repeat_membership_then_department_hierarchy",
            "global_repeat_missing_root",
        ] {
            let case = get(name);
            assert_ne!(case["client_expect_unsupported"], true, "{name}");
            assert!(case.get("client_expected").is_some(), "{name}");
            assert_eq!(case["server_supported"], true, "{name}");
        }
        assert_eq!(
            get("global_repeat_filtered_ancestor_maps")["client_expect_unsupported"],
            true
        );
        // The client roots are selected in local-ID order, not the coordinator's
        // partition-first order. Recompute only from fixture IDs and edges.
        let mut frontier: Vec<_> = (1..=m.people)
            .filter(|id| active(*id) && mix(*id ^ 0x434f554e545259).is_multiple_of(5))
            .take(12)
            .collect();
        let mut emitted = Vec::new();
        for _ in 0..4 {
            frontier = m.hop(&frontier, "reports_to", true);
            emitted.extend(frontier.iter().copied());
        }
        assert_eq!(
            get("global_repeat_multi_root_bag")["client_expected"],
            json!({"count":emitted.len()})
        );
        assert_eq!(
            get("global_repeat_multi_root_unique")["client_expected"],
            json!({"count":unique(emitted).len()})
        );

        for name in [
            "global_scan_reject_scalar_dedup",
            "client_scalar_name_dedup",
            "client_scalar_id_dedup",
            "client_scalar_label_dedup",
        ] {
            let case = get(name);
            assert_ne!(case["expect_error"], true);
            assert_ne!(case["client_expect_unsupported"], true);
            assert_eq!(case["server_supported"], false);
            assert_eq!(case["server_expected_error"], "Unsupported");
        }
        assert_eq!(
            get("global_scan_reject_scalar_dedup")["client_expected"],
            json!({"count":1})
        );
        assert_eq!(
            get("client_scalar_name_dedup")["client_expected"],
            json!({"values":[
                {"Text":"Employee-000026"}, {"Text":"Employee-000020"}, {"Text":"Employee-000023"}
            ]})
        );
        assert_eq!(
            get("client_scalar_id_dedup")["client_expected"],
            json!({"values":[
                {"Integer":26}, {"Integer":20}, {"Integer":23}
            ]})
        );
        assert_eq!(
            get("client_scalar_label_dedup")["client_expected"],
            json!({"values":[{"Text":"person"}]})
        );
        assert!(
            get("global_repeat_named_ancestors")["expected"]["vertices"]
                .as_array()
                .unwrap()
                .len()
                > 3
        );
        assert!(!get("global_repeat_exact_depth")["expected"]["vertices"]
            .as_array()
            .unwrap()
            .is_empty());
        assert!(
            get("global_repeat_multi_root_bag")["expected"]["count"]
                .as_u64()
                .unwrap()
                > get("global_repeat_multi_root_unique")["expected"]["count"]
                    .as_u64()
                    .unwrap()
        );
        assert!(
            !get("global_repeat_membership_then_department_hierarchy")["expected"]["vertices"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            get("global_join_named_memberships")["expected"]["vertices"]
                .as_array()
                .unwrap()
                .len(),
            3
        );
        assert_ne!(
            get("global_join_filtered_manager_rows")["expected"],
            get("global_join_filtered_manager_rows")["client_expected"]
        );
        assert!(
            get("global_join_filtered_manager_bag_count")["expected"]["count"]
                .as_u64()
                .unwrap()
                > get("global_join_filtered_manager_unique_count")["expected"]["count"]
                    .as_u64()
                    .unwrap()
        );
        assert_eq!(
            get("global_join_limit_before_filter")["expected"]["count"],
            0
        );
        assert!(
            get("global_join_filter_before_limit")["expected"]["count"]
                .as_u64()
                .unwrap()
                > 0
        );
        for name in [
            "global_join_department_maps",
            "global_join_named_department_members",
        ] {
            let rows = get(name)["expected"]["maps"].as_array().unwrap();
            assert!(!rows.is_empty());
            assert!(rows
                .iter()
                .all(|r| r["properties"].get("missing").is_none()));
        }
        assert_eq!(
            q.iter().filter(|r| r["server_supported"] == true).count(),
            21
        );
    }
}
