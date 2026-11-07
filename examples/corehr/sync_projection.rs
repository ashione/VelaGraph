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
            "server_supported":true,"client_expect_unsupported":true}),
        );
    };
    let projection = ".valueMap('name','active','multi_department','missing')";
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
    result
}
#[test]
fn batch_projection_oracle_preserves_bags_and_omits_missing_properties() {
    for (people, departments) in [(2000, 50), (200000, 5000)] {
        let m = Model::new(people, departments);
        let q = queries(&m);
        assert_eq!(q.len(), 5);
        let maps = q[0]["expected"]["maps"].as_array().unwrap();
        assert_eq!(maps.len(), 4);
        assert_eq!(maps[0], maps[3]);
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
