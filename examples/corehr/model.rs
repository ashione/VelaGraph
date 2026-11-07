//! Deterministic synthetic CoreHR ground truth. No database reads in this oracle.
use serde_json::{json, Value};
pub struct Edge {
    pub from: u64,
    pub to: u64,
    pub label: &'static str,
}
pub struct Model {
    pub people: u64,
    pub departments: u64,
    pub edges: Vec<Edge>,
}
pub fn partition(id: u64) -> u32 {
    1 + (id % 3) as u32
}
pub fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
pub fn active(id: u64) -> bool {
    !mix(id ^ 0x414354495645).is_multiple_of(20)
}
impl Model {
    pub fn new(people: u64, departments: u64) -> Self {
        assert!(people >= 100 && departments >= 25);
        let mut edges = Vec::new();
        for p in 2..=people {
            edges.push(Edge {
                from: p,
                to: 1 + (p - 2) / 8,
                label: "reports_to",
            });
        }
        for d in 2..=departments {
            edges.push(Edge {
                from: people + d,
                to: people + 1 + (d - 2) / 5,
                label: "parent_department",
            });
        }
        for p in 1..=people {
            // 40% preferential cohort selects the hot 10%; the rest selects all departments.
            let h = mix(p);
            let d = if p % 5 < 2 {
                1 + h % (departments / 10)
            } else {
                1 + h % departments
            };
            let count = 1 + u64::from(p % 5 == 0) + u64::from(p % 20 == 0);
            for k in 0..count {
                edges.push(Edge {
                    from: p,
                    to: people + 1 + (d - 1 + k * (departments / 3 + 1)) % departments,
                    label: "member_of",
                });
            }
        }
        Self {
            people,
            departments,
            edges,
        }
    }
    fn hop(&self, ids: &[u64], label: &str, out: bool) -> Vec<u64> {
        ids.iter()
            .flat_map(|id| {
                self.edges
                    .iter()
                    .filter(move |e| {
                        e.label == label && if out { e.from == *id } else { e.to == *id }
                    })
                    .map(move |e| if out { e.to } else { e.from })
            })
            .collect()
    }
    // Relationship-list oracle only: no engine/parser/remote calls. Each path
    // carries every qualified edge and vertex; emission appends whole layers.
    fn native_paths(
        &self,
        ids: &[u64],
        label: &str,
        out: bool,
        depth: usize,
        emit: bool,
    ) -> Vec<(u64, Vec<Value>)> {
        let mut frontier: Vec<_> = ids
            .iter()
            .map(|id| (*id, vec![json!(["v", partition(*id), id])]))
            .collect();
        let mut emitted = Vec::new();
        for _ in 0..depth {
            let mut next = Vec::new();
            for (id, path) in frontier {
                for (idx, e) in self.edges.iter().enumerate().filter(|(_, e)| {
                    e.label == label && if out { e.from == id } else { e.to == id }
                }) {
                    let dest = if out { e.to } else { e.from };
                    let mut p = path.clone();
                    p.push(json!(["e", partition(e.from), idx + 1]));
                    p.push(json!(["v", partition(dest), dest]));
                    next.push((dest, p));
                }
            }
            if emit {
                emitted.extend(next.iter().cloned());
            }
            frontier = next;
            if frontier.is_empty() {
                break;
            }
        }
        if emit {
            emitted
        } else {
            frontier
        }
    }
    pub fn queries(&self) -> Vec<Value> {
        let mut q = Vec::new();
        let mut add = |name: &str, start: u64, steps: &str, ids: Vec<u64>, server: bool| {
            q.push(json!({"name":name,"partition":partition(start),"query":format!("g.V({start}){steps}"),"expected":{"vertices":ids.iter().map(|id|json!([partition(*id),id])).collect::<Vec<_>>()},"server_supported":server}));
        };
        for p in [1, 20, self.people / 2, self.people] {
            add(
                &format!("manager_{p}"),
                p,
                ".out('reports_to')",
                self.hop(&[p], "reports_to", true),
                true,
            );
            add(
                &format!("direct_reports_{p}"),
                p,
                ".in('reports_to')",
                self.hop(&[p], "reports_to", false),
                false,
            );
            add(
                &format!("departments_{p}"),
                p,
                ".out('member_of')",
                self.hop(&[p], "member_of", true),
                true,
            );
        }
        let p = self.people;
        let ancestors = self.hop(
            &self.hop(&self.hop(&[p], "reports_to", true), "reports_to", true),
            "reports_to",
            true,
        );
        add(
            "manager_chain_three_hops",
            p,
            ".out('reports_to').out('reports_to').out('reports_to')",
            ancestors,
            true,
        );
        add(
            "reports_two_hops",
            1,
            ".in('reports_to').in('reports_to')",
            self.hop(&self.hop(&[1], "reports_to", false), "reports_to", false),
            false,
        );
        let d = self.people + self.departments;
        add(
            "department_ancestors_three_hops",
            d,
            ".out('parent_department').out('parent_department').out('parent_department')",
            self.hop(
                &self.hop(
                    &self.hop(&[d], "parent_department", true),
                    "parent_department",
                    true,
                ),
                "parent_department",
                true,
            ),
            true,
        );
        let d = self.people + 2;
        add(
            "department_descendants_two_hops",
            d,
            ".in('parent_department').in('parent_department')",
            self.hop(
                &self.hop(&[d], "parent_department", false),
                "parent_department",
                false,
            ),
            false,
        );
        add(
            "department_members",
            d,
            ".in('member_of').limit(1000)",
            self.hop(&[d], "member_of", false),
            false,
        );
        add(
            "active_department_members",
            d,
            ".in('member_of').has('active',true).limit(1000)",
            self.hop(&[d], "member_of", false)
                .into_iter()
                .filter(|id| active(*id))
                .collect(),
            false,
        );
        add(
            "colleagues_via_department",
            20,
            ".out('member_of').in('member_of').limit(25)",
            self.hop(&self.hop(&[20], "member_of", true), "member_of", false)
                .into_iter()
                .take(25)
                .collect(),
            false,
        );
        add(
            "department_parent_join",
            20,
            ".out('member_of').out('parent_department')",
            self.hop(
                &self.hop(&[20], "member_of", true),
                "parent_department",
                true,
            ),
            true,
        );
        add(
            "missing_person",
            self.people + self.departments + 1,
            "",
            Vec::new(),
            true,
        );
        q.push(json!({"name":"direct_report_count","partition":partition(1),"query":"g.V(1).in('reports_to').count()","expected":{"count":8},"server_supported":false}));
        q.push(json!({"name":"filtered_join_count","partition":partition(20),"query":"g.V(20).out('member_of').hasLabel('department').count()","expected":{"count":3},"server_supported":true}));
        q.push(json!({"name":"person_name_projection","partition":partition(20),"query":"g.V(20).values('name')","expected":{"values":[{"Text":"Employee-000020"}]},"server_supported":true}));
        let duplicated = self.hop(&[20, 20], "member_of", true);
        q.push(json!({"name":"duplicate_start_bag","partition":partition(20),"query":"g.V(20,20).out('member_of')","expected":{"vertices":duplicated.iter().map(|id|json!([partition(*id),id])).collect::<Vec<_>>()},"server_supported":true}));
        let colleagues = self.hop(&self.hop(&[20], "member_of", true), "member_of", false);
        assert_eq!(colleagues.iter().filter(|id| **id == 20).count(), 3);
        q.push(json!({"name":"colleagues_bag_count","partition":partition(20),"query":"g.V(20).out('member_of').in('member_of').count()","expected":{"count":colleagues.len()},"server_supported":false}));
        // Independent relationship-list oracle: first occurrence of a qualified
        // endpoint survives, preserving its complete path and stream position.
        let mut seen = std::collections::BTreeSet::new();
        let unique: Vec<_> = colleagues
            .iter()
            .copied()
            .filter(|id| seen.insert((partition(*id), *id)))
            .collect();
        assert!(unique.len() < colleagues.len());
        let verts = |ids: &[u64]| {
            json!({"vertices": ids.iter()
            .map(|id| json!([partition(*id), id])).collect::<Vec<_>>()})
        };
        let join = "g.V(20).out('member_of').in('member_of')";
        q.push(json!({"name":"dedup_join","partition":partition(20),"query":format!("{join}.dedup().limit(1000)"),"expected":verts(&unique),"server_supported":true}));
        q.push(json!({"name":"unique_colleague_count","partition":partition(20),"query":format!("{join}.dedup().count()"),"expected":{"count":unique.len()},"server_supported":true}));
        q.push(json!({"name":"active_unique_colleague_count","partition":partition(20),"query":format!("{join}.dedup().has('active',true).count()"),"expected":{"count":unique.iter().filter(|id| active(**id)).count()},"server_supported":true}));
        // Choose the first duplicate prefix so limit/dedup order is observably
        // different for both smoke and full fixtures, not a vacuous prefix.
        let mut seen = std::collections::BTreeSet::new();
        let boundary = colleagues.iter().position(|id| !seen.insert(*id)).unwrap() + 1;
        let mut seen = std::collections::BTreeSet::new();
        let before: Vec<_> = colleagues[..boundary]
            .iter()
            .copied()
            .filter(|id| seen.insert(*id))
            .collect();
        let after: Vec<_> = unique.iter().copied().take(boundary).collect();
        assert_ne!(before, after);
        q.push(json!({"name":"limit_before_dedup","partition":partition(20),"query":format!("{join}.limit({boundary}).dedup()"),"expected":verts(&before),"server_supported":true}));
        q.push(json!({"name":"limit_after_dedup","partition":partition(20),"query":format!("{join}.dedup().limit({boundary})"),"expected":verts(&after),"server_supported":true}));
        q.push(json!({"name":"duplicate_starts_dedup","partition":partition(20),"query":"g.V(20,20).dedup().dedup().count()","expected":{"count":1},"server_supported":true}));
        q.push(json!({"name":"duplicate_memberships_unique_count","partition":partition(20),"query":"g.V(20,20).out('member_of').dedup().count()","expected":{"count":3},"server_supported":true}));
        q.push(json!({"name":"empty_dedup_count","partition":partition(self.people),"query":format!("g.V({}).in('reports_to').dedup().count()",self.people),"expected":{"count":0},"server_supported":true}));
        let mut seen = std::collections::BTreeSet::new();
        let mut paths = Vec::new();
        for (out_idx, out) in self
            .edges
            .iter()
            .enumerate()
            .filter(|(_, e)| e.from == 20 && e.label == "member_of")
        {
            for (in_idx, incoming) in self
                .edges
                .iter()
                .enumerate()
                .filter(|(_, e)| e.to == out.to && e.label == "member_of")
            {
                if seen.insert((partition(incoming.from), incoming.from)) {
                    paths.push(json!([
                        ["v", partition(20), 20],
                        ["e", partition(20), out_idx + 1],
                        ["v", partition(out.to), out.to],
                        ["e", partition(incoming.from), in_idx + 1],
                        ["v", partition(incoming.from), incoming.from]
                    ]));
                }
            }
        }
        assert_eq!(paths.len(), unique.len());
        q.push(json!({"name":"dedup_first_path","partition":partition(20),"query":format!("{join}.dedup().limit(1000).path()"),"expected":{"paths":paths},"server_supported":true}));
        for (name, query) in [
            ("recursive_repeat", "g.V(1).repeat(in('reports_to')).emit()"),
            ("offset_pagination", "g.V(1).in('reports_to').range(2,5)"),
            ("sort", "g.V(1).in('reports_to').order()"),
            ("predicate", "g.V(1).has('level',gt(2))"),
        ] {
            q.push(json!({"name":name,"partition":partition(1),"query":query,"expect_error":true}));
        }
        q.push(json!({"name":"global_count","partition":1,"query":"g.V().count()","expected":{"count":self.people+self.departments},"server_supported":true}));
        // Two public APIs intentionally use different qualified ordering.
        // Derive both from IDs and model properties, never database output.
        let client_order: Vec<_> = (1..=self.people + self.departments).collect();
        let mut server_order = client_order.clone();
        server_order.sort_by_key(|id| (partition(*id), *id));
        let is_cn = |id: u64| mix(id ^ 0x434f554e545259).is_multiple_of(5);
        let mut add_count = |name: &str, query: &str, server: usize, client: usize| {
            q.push(json!({"name":name,"partition":1,"query":query,"expected":{"count":server},"client_expected":{"count":client},"server_supported":true}));
        };
        for (name, query, count) in [
            (
                "global_people_count",
                "g.V().hasLabel('person').count()",
                self.people as usize,
            ),
            (
                "global_departments_count",
                "g.V().hasLabel('department').count()",
                self.departments as usize,
            ),
            (
                "global_active_people_count",
                "g.V().hasLabel('person').has('active',true).count()",
                (1..=self.people).filter(|id| active(*id)).count(),
            ),
            (
                "global_inactive_people_count",
                "g.V().hasLabel('person').has('active',false).count()",
                (1..=self.people).filter(|id| !active(*id)).count(),
            ),
            (
                "global_active_departments_count",
                "g.V().hasLabel('department').has('active',true).count()",
                (self.people + 1..=self.people + self.departments)
                    .filter(|id| active(*id))
                    .count(),
            ),
            (
                "global_multi_department_count",
                "g.V().has('multi_department',true).count()",
                (self.people / 5) as usize,
            ),
            (
                "global_cn_people_count",
                "g.V().hasLabel('person').has('country','CN').count()",
                (1..=self.people).filter(|id| is_cn(*id)).count(),
            ),
            (
                "global_missing_label_count",
                "g.V().hasLabel('not_present').count()",
                0,
            ),
            ("global_zero_limit_count", "g.V().limit(0).count()", 0),
        ] {
            add_count(name, query, count, count);
        }
        let before_server = server_order
            .iter()
            .take(300)
            .filter(|id| **id > self.people)
            .count();
        let before_client = client_order
            .iter()
            .take(300)
            .filter(|id| **id > self.people)
            .count();
        add_count(
            "global_limit_before_department_count",
            "g.V().limit(300).hasLabel('department').count()",
            before_server,
            before_client,
        );
        add_count(
            "global_limit_after_department_count",
            "g.V().hasLabel('department').limit(300).count()",
            self.departments.min(300) as usize,
            self.departments.min(300) as usize,
        );
        let mut add_list = |name: &str, query: &str, server: Vec<u64>, client: Vec<u64>| {
            q.push(json!({"name":name,"partition":1,"query":query,"expected":verts(&server),"client_expected":verts(&client),"server_supported":true}));
        };
        add_list(
            "global_default_prefix",
            "g.V()",
            server_order.iter().copied().take(100).collect(),
            client_order.iter().copied().take(100).collect(),
        );
        add_list(
            "global_page_prefix",
            "g.V().limit(300)",
            server_order.iter().copied().take(300).collect(),
            client_order.iter().copied().take(300).collect(),
        );
        add_list(
            "global_active_prefix",
            "g.V().has('active',true).limit(300)",
            server_order
                .iter()
                .copied()
                .filter(|id| active(*id))
                .take(300)
                .collect(),
            client_order
                .iter()
                .copied()
                .filter(|id| active(*id))
                .take(300)
                .collect(),
        );
        add_list(
            "global_cn_departments",
            "g.V().hasLabel('department').has('country','CN').limit(1000)",
            server_order
                .iter()
                .copied()
                .filter(|id| *id > self.people && is_cn(*id))
                .take(1000)
                .collect(),
            client_order
                .iter()
                .copied()
                .filter(|id| *id > self.people && is_cn(*id))
                .take(1000)
                .collect(),
        );
        let mut native = |name: &str,
                          start: u64,
                          query: String,
                          paths: Vec<(u64, Vec<Value>)>,
                          shape: &str| {
            let expected = match shape {
                "count" => json!({"count":paths.len()}),
                "path" => json!({"paths":paths.iter().map(|(_,p)|p).collect::<Vec<_>>()}),
                _ => verts(&paths.iter().map(|(id, _)| *id).collect::<Vec<_>>()),
            };
            q.push(json!({"name":name,"partition":partition(start),"query":query,"expected":expected,"server_supported":true,"client_expect_unsupported":true}));
        };
        for (kind, start, label) in [
            ("person", self.people, "reports_to"),
            (
                "department",
                self.people + self.departments,
                "parent_department",
            ),
        ] {
            let ancestors = self.native_paths(&[start], label, true, 32, true);
            let depth = ancestors.len();
            assert!((1..=32).contains(&depth));
            native(
                &format!("native_{kind}_ancestors_emit32"),
                start,
                format!("g.V({start}).repeat(__.out('{label}')).emit().times(32).limit(1000)"),
                ancestors.clone(),
                "vertices",
            );
            native(
                &format!("native_{kind}_ancestors_first_paths"),
                start,
                format!(
                    "g.V({start}).repeat(__.out('{label}')).times(32).emit().limit(1000).path()"
                ),
                ancestors,
                "path",
            );
            native(
                &format!("native_{kind}_exact_root"),
                start,
                format!("g.V({start}).repeat(__.out('{label}')).times({depth})"),
                self.native_paths(&[start], label, true, depth, false),
                "vertices",
            );
            native(
                &format!("native_{kind}_past_root"),
                start,
                format!(
                    "g.V({start}).repeat(__.out('{label}')).times({})",
                    depth + 1
                ),
                Vec::new(),
                "vertices",
            );
            native(
                &format!("native_{kind}_exact32_empty"),
                start,
                format!("g.V({start}).repeat(__.out('{label}')).times(32).count()"),
                Vec::new(),
                "count",
            );
        }
        let descendants = self.native_paths(&[1], "reports_to", false, 2, false);
        native(
            "native_person_descendants_two",
            1,
            "g.V(1).repeat(__.in('reports_to')).times(2).limit(1000)".into(),
            descendants,
            "vertices",
        );
        let descendants = self.native_paths(&[1], "reports_to", false, 2, true);
        native(
            "native_person_descendants_emit_two",
            1,
            "g.V(1).repeat(__.in('reports_to')).emit().times(2).limit(1000)".into(),
            descendants.clone(),
            "vertices",
        );
        native(
            "native_person_active_descendants_count",
            1,
            "g.V(1).repeat(__.in('reports_to')).emit().times(2).has('active',true).count()".into(),
            descendants
                .into_iter()
                .filter(|(id, _)| active(*id))
                .collect(),
            "count",
        );
        native(
            "native_person_descendants_emit_three_count",
            1,
            "g.V(1).repeat(__.in('reports_to')).times(3).emit().count()".into(),
            self.native_paths(&[1], "reports_to", false, 3, true),
            "count",
        );
        let d = self.people + 2;
        native(
            "native_department_descendants_two",
            d,
            format!("g.V({d}).repeat(__.in('parent_department')).times(2).limit(1000)"),
            self.native_paths(&[d], "parent_department", false, 2, false),
            "vertices",
        );
        native(
            "native_department_descendants_emit_two_count",
            d,
            format!("g.V({d}).repeat(__.in('parent_department')).emit().times(2).count()"),
            self.native_paths(&[d], "parent_department", false, 2, true),
            "count",
        );
        let p = self.people;
        let duplicates = self.native_paths(&[p, p], "reports_to", true, 32, true);
        native(
            "native_duplicate_ancestor_bag_count",
            p,
            format!("g.V({p},{p}).repeat(__.out('reports_to')).emit().times(32).count()"),
            duplicates.clone(),
            "count",
        );
        let mut seen = std::collections::BTreeSet::new();
        native("native_duplicate_ancestor_dedup_paths",p,format!("g.V({p},{p}).repeat(__.out('reports_to')).emit().times(32).dedup().limit(1000).path()"),duplicates.into_iter().filter(|(id,_)|seen.insert((partition(*id),*id))).collect(),"path");
        for (name, query, error, client_direct) in [
            (
                "native_broad_descendants_budget",
                "g.V(1).repeat(__.in('reports_to')).emit().times(32).count()",
                "ResourceExhausted",
                true,
            ),
            (
                "native_broad_limit_cannot_rescue",
                "g.V(1).repeat(__.in('reports_to')).emit().times(32).limit(1)",
                "ResourceExhausted",
                true,
            ),
            (
                "native_zero_depth",
                "g.V(1).repeat(__.out('reports_to')).times(0)",
                "Unsupported",
                false,
            ),
            (
                "native_depth33",
                "g.V(1).repeat(__.out('reports_to')).times(33)",
                "ResourceExhausted",
                false,
            ),
            (
                "native_combined_depth33",
                "g.V(1).out('reports_to').repeat(__.out('reports_to')).times(32)",
                "ResourceExhausted",
                true,
            ),
        ] {
            q.push(json!({"name":name,"partition":partition(1),"query":query,"expect_error":true,"client_expect_unsupported":client_direct,"server_supported":false,"server_expected_error":error}));
        }
        for case in &mut q {
            if case["name"] == "global_zero_limit_count" {
                // Official parser accepts only limits 1..=1000. Internal
                // exhausted counters do not expand the user-facing grammar.
                case["server_supported"] = json!(false);
                case["server_expected_error"] = json!("InvalidArgument");
                case["expect_error"] = json!(true);
                case.as_object_mut().unwrap().remove("expected");
                case.as_object_mut().unwrap().remove("client_expected");
            }
            if case["query"].as_str().unwrap().contains(".dedup()") {
                case["client_expect_unsupported"] = json!(true);
            }
            let name = case["name"].as_str().unwrap();
            if name.starts_with("direct_reports_")
                || [
                    "reports_two_hops",
                    "department_descendants_two_hops",
                    "department_members",
                    "active_department_members",
                    "colleagues_via_department",
                    "direct_report_count",
                    "colleagues_bag_count",
                ]
                .contains(&name)
            {
                case["server_supported"] = json!(true);
            }
        }
        q
    }
}
#[test]
fn fixture_invariants() {
    let m = Model::new(200, 50);
    assert_eq!(m.edges.len(), 199 + 49 + 250);
    for p in 1..=200 {
        let members = m.hop(&[p], "member_of", true);
        assert_eq!(
            members.len(),
            1 + usize::from(p % 5 == 0) + usize::from(p % 20 == 0)
        );
        assert!(members.iter().all(|d| *d > 200 && *d <= 250));
    }
    assert!(m
        .edges
        .iter()
        .filter(|e| e.label != "member_of")
        .all(|e| e.to < e.from));
}

#[test]
fn dedup_oracle_is_nonvacuous() {
    let m = Model::new(2000, 50);
    let q = m.queries();
    assert_eq!(q.len(), 78);
    let get = |name: &str| q.iter().find(|v| v["name"] == name).unwrap();
    assert!(
        get("unique_colleague_count")["expected"]["count"]
            .as_u64()
            .unwrap()
            < get("colleagues_bag_count")["expected"]["count"]
                .as_u64()
                .unwrap()
    );
    assert_ne!(
        get("limit_before_dedup")["expected"],
        get("limit_after_dedup")["expected"]
    );
    let vertices = get("dedup_join")["expected"]["vertices"]
        .as_array()
        .unwrap();
    let paths = get("dedup_first_path")["expected"]["paths"]
        .as_array()
        .unwrap();
    assert_eq!(vertices.len(), paths.len());
    for (v, p) in vertices.iter().zip(paths) {
        let last = p.as_array().unwrap().last().unwrap();
        assert_eq!(json!([last[1], last[2]]), *v);
    }
}

#[test]
fn global_oracles_use_distinct_orders_and_real_properties() {
    let m = Model::new(2000, 50);
    let q = m.queries();
    let get = |name: &str| q.iter().find(|v| v["name"] == name).unwrap();
    assert_ne!(
        get("global_page_prefix")["expected"],
        get("global_page_prefix")["client_expected"]
    );
    assert_eq!(
        get("global_page_prefix")["expected"]["vertices"][0],
        json!([1, 3])
    );
    assert_eq!(
        get("global_page_prefix")["client_expected"]["vertices"][0],
        json!([2, 1])
    );
    assert_ne!(
        get("global_limit_before_department_count")["expected"],
        get("global_limit_after_department_count")["expected"]
    );
    assert_eq!(
        get("global_active_people_count")["expected"]["count"]
            .as_u64()
            .unwrap()
            + get("global_inactive_people_count")["expected"]["count"]
                .as_u64()
                .unwrap(),
        m.people
    );
}

#[test]
fn native_hierarchy_oracle_preserves_levels_and_paths() {
    let m = Model::new(2000, 50);
    let q = m.queries();
    let get = |name: &str| q.iter().find(|v| v["name"] == name).unwrap();
    assert_eq!(
        get("native_person_ancestors_emit32")["partition"],
        json!(partition(m.people))
    );
    assert_eq!(
        get("native_person_descendants_two")["partition"],
        json!(partition(1))
    );
    assert_eq!(
        get("native_department_descendants_two")["partition"],
        json!(partition(m.people + 2))
    );
    assert_eq!(
        get("native_person_descendants_emit_three_count")["expected"]["count"],
        json!(584)
    );
    assert_eq!(
        get("native_person_descendants_two")["expected"]["vertices"]
            .as_array()
            .unwrap()
            .len(),
        64
    );
    let a = get("native_person_ancestors_emit32")["expected"]["vertices"]
        .as_array()
        .unwrap();
    assert!(a.len() > 3);
    assert_eq!(
        get("native_duplicate_ancestor_bag_count")["expected"]["count"],
        json!(2 * a.len())
    );
    let paths = get("native_duplicate_ancestor_dedup_paths")["expected"]["paths"]
        .as_array()
        .unwrap();
    assert_eq!(paths.len(), a.len());
    for (i, p) in paths.iter().enumerate() {
        assert_eq!(p.as_array().unwrap().len(), 1 + 2 * (i + 1));
    }
}
