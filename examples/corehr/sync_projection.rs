//! Relationship-list-only oracle for terminal multi-field CoreHR queries.
use super::*;
fn properties(m: &Model, id: u64) -> Value {
    let name = format!(
        "{}-{id:06}",
        if id <= m.people {
            "Employee"
        } else {
            "Department"
        }
    );
    let mut map = serde_json::Map::from_iter([
        ("name".into(), json!([{"Text":name}])),
        ("active".into(), json!([{"Boolean":active(id)}])),
        ("nullable".into(), json!(["Null"])),
    ]);
    if id <= m.people {
        map.insert(
            "multi_department".into(),
            json!([{"Boolean":id.is_multiple_of(5)}]),
        );
    }
    json!({"source":[partition(id),id],"properties":map})
}
pub(super) fn queries(m: &Model) -> Vec<Value> {
    let mut result = Vec::new();
    let mut add = |name: &str, start: u64, text: String, ids: Vec<u64>| {
        assert!(!ids.is_empty());
        result.push(
            json!({"name":name,"partition":partition(start),"query":text,
            "expected":{"maps":ids.into_iter().map(|id| properties(m,id)).collect::<Vec<_>>()},
            "server_supported":true}),
        );
    };
    let projection = ".valueMap('name','active','multi_department','nullable','missing')";
    add(
        "project_batch_people",
        20,
        format!("g.V(20,23,26,20){projection}"),
        vec![20, 23, 26, 20],
    );
    let departments = m.hop(&[20, 23, 26, 20], "member_of", true);
    add(
        "project_batch_departments",
        20,
        format!("g.V(20,23,26,20).out('member_of'){projection}"),
        departments.clone(),
    );
    let mut seen = std::collections::BTreeSet::new();
    let unique = departments
        .into_iter()
        .filter(|id| seen.insert(*id))
        .collect();
    add(
        "project_unique_departments",
        20,
        format!("g.V(20,23,26,20).out('member_of').dedup(){projection}"),
        unique,
    );
    let d = m.people + 2;
    let members = m.hop(&[d], "member_of", false);
    add(
        "project_active_department_members",
        d,
        format!("g.V({d}).in('member_of').has('active',true).limit(25){projection}"),
        members
            .into_iter()
            .filter(|id| active(*id))
            .take(25)
            .collect(),
    );
    let colleagues = m.hop(&m.hop(&[20], "member_of", true), "member_of", false);
    let mut seen = std::collections::BTreeSet::new();
    let unique = colleagues
        .into_iter()
        .filter(|id| seen.insert(*id))
        .take(25)
        .collect();
    add(
        "project_unique_colleagues",
        20,
        format!("g.V(20).out('member_of').in('member_of').dedup().limit(25){projection}"),
        unique,
    );
    // Empty streams and empty maps are distinct: one row survives per input
    // vertex even when none of the selected properties exists.
    result.push(
        json!({"name":"project_missing_properties_bag","partition":partition(20),
        "query":"g.V(20,23,20).valueMap('missing')",
        "expected":{"maps":[
            {"source":[partition(20),20],"properties":{}},
            {"source":[partition(23),23],"properties":{}},
            {"source":[partition(20),20],"properties":{}}
        ]},"server_supported":true}),
    );
    result.push(
        json!({"name":"project_empty_stream","partition":partition(20),
        "query":"g.V(20).has('name','NoSuchEmployee').valueMap('name','nullable','missing')",
        "expected":{"maps":[]},"server_supported":true}),
    );
    result
}
#[test]
fn batch_projection_oracle_preserves_bags_and_omits_missing_properties() {
    for (people, departments) in [(2000, 50), (200000, 5000)] {
        let m = Model::new(people, departments);
        let q = queries(&m);
        assert_eq!(q.len(), 7);
        for case in &q {
            assert_ne!(case["client_expect_unsupported"], true);
            assert_eq!(case["server_supported"], true);
            assert!(case["expected"]["maps"].is_array());
        }
        let maps = q[0]["expected"]["maps"].as_array().unwrap();
        assert_eq!(maps.len(), 4);
        assert_eq!(maps[0], maps[3]);
        assert_eq!(
            maps,
            &vec![
                properties(&m, 20),
                properties(&m, 23),
                properties(&m, 26),
                properties(&m, 20)
            ]
        );
        assert!(maps
            .iter()
            .all(|row| row["properties"]["nullable"] == json!(["Null"])));
        assert_eq!(q[5]["expected"]["maps"].as_array().unwrap().len(), 3);
        assert_eq!(q[5]["expected"]["maps"][0], q[5]["expected"]["maps"][2]);
        assert!(q[5]["expected"]["maps"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["properties"] == json!({})));
        assert_eq!(q[6]["expected"], json!({"maps":[]}));
        assert!(maps
            .iter()
            .all(|row| row["properties"].get("missing").is_none()));
        let departments = q[1]["expected"]["maps"].as_array().unwrap();
        assert_eq!(departments.len(), 8);
        assert!(departments
            .iter()
            .all(|row| row["properties"].get("multi_department").is_none()));
        assert!(q[2]["expected"]["maps"].as_array().unwrap().len() < departments.len());
    }
}
