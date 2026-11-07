//! Batch relationship expectations derived only from the input edge list.
//! Explicit V(ids) starts are all qualified by the request's one start partition.
use super::{active, partition, Model};
use serde_json::{json, Value};
use std::collections::BTreeSet;

fn hop(model: &Model, roots: &[u64], label: &str, out: bool) -> Vec<u64> {
    roots
        .iter()
        .flat_map(|root| {
            model
                .edges
                .iter()
                .filter(move |edge| {
                    edge.label == label
                        && if out {
                            edge.from == *root
                        } else {
                            edge.to == *root
                        }
                })
                .map(move |edge| if out { edge.to } else { edge.from })
        })
        .collect()
}

fn unique(ids: &[u64]) -> Vec<u64> {
    let mut seen = BTreeSet::new();
    ids.iter()
        .copied()
        .filter(|id| seen.insert((partition(*id), *id)))
        .collect()
}

pub fn queries(model: &Model) -> Vec<Value> {
    let mut cases = Vec::new();
    let roots = [20, 23, 26, 20];
    let departments = hop(model, &roots, "member_of", true);
    let vertices = departments
        .iter()
        .map(|id| json!([partition(*id), id]))
        .collect::<Vec<_>>();
    cases.push(json!({"name":"batch_people_membership_bag","partition":partition(20),
        "query":"g.V(20,23,26,20).out('member_of')","expected":{"vertices":vertices},"server_supported":true}));
    let department_roots = [model.people + 1, model.people + 4, model.people + 1];
    let members = hop(model, &department_roots, "member_of", false);
    let department_query = format!(
        "g.V({},{},{})",
        department_roots[0], department_roots[1], department_roots[2]
    );
    for (name, query, count, dedup) in [
        (
            "batch_people_membership_count",
            "g.V(20,23,26,20).out('member_of').count()".into(),
            departments.len(),
            false,
        ),
        (
            "batch_people_unique_departments",
            "g.V(20,23,26,20).out('member_of').dedup().count()".into(),
            unique(&departments).len(),
            true,
        ),
        (
            "batch_department_members_bag_count",
            format!("{department_query}.in('member_of').count()"),
            members.len(),
            false,
        ),
        (
            "batch_department_unique_members",
            format!("{department_query}.in('member_of').dedup().count()"),
            unique(&members).len(),
            true,
        ),
        (
            "batch_department_active_unique_members",
            format!("{department_query}.in('member_of').dedup().has('active',true).count()"),
            unique(&members).iter().filter(|id| active(**id)).count(),
            true,
        ),
    ] {
        let mut case = json!({"name":name,"partition":if name.starts_with("batch_people") {partition(20)} else {partition(department_roots[0])},
            "query":query,"expected":{"count":count},"server_supported":true});
        if dedup {
            case["client_expect_unsupported"] = json!(true);
        }
        cases.push(case);
    }
    // 21 exists, but in partition(21), not partition(20). This API must not
    // silently resolve a bare ID across shards or conflate qualified identities.
    let only_20 = hop(model, &[20], "member_of", true);
    cases.push(json!({"name":"batch_start_partition_boundary","partition":partition(20),
        "query":"g.V(20,21).out('member_of').count()","expected":{"count":only_20.len()},"server_supported":true}));
    cases
}

#[test]
fn batch_oracles_have_cross_shard_duplicates_and_filters() {
    for (people, departments) in [(2000, 50), (200000, 5000)] {
        let model = Model::new(people, departments);
        let rows = queries(&model);
        let get = |name: &str| rows.iter().find(|row| row["name"] == name).unwrap();
        assert_eq!(rows.len(), 7);
        assert_eq!(get("batch_people_membership_count")["expected"]["count"], 8);
        assert_eq!(
            get("batch_start_partition_boundary")["expected"]["count"],
            3
        );
        let memberships = get("batch_people_membership_bag")["expected"]["vertices"]
            .as_array()
            .unwrap();
        assert!(memberships.iter().any(|v| v[0] != partition(20)));
        let bag = get("batch_department_members_bag_count")["expected"]["count"]
            .as_u64()
            .unwrap();
        let distinct = get("batch_department_unique_members")["expected"]["count"]
            .as_u64()
            .unwrap();
        let filtered = get("batch_department_active_unique_members")["expected"]["count"]
            .as_u64()
            .unwrap();
        assert!(bag > distinct && distinct > filtered && filtered > 0);
        // All explicit roots in each batch are intentionally in one partition.
        assert_eq!(partition(20), partition(23));
        assert_eq!(partition(23), partition(26));
        assert_eq!(partition(people + 1), partition(people + 4));
    }
}
