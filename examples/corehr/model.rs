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
        q.push(json!({"name":"global_count","partition":1,"query":"g.V().count()","expect_error":self.people+self.departments>10000,"expected":{"count":self.people+self.departments}}));
        for case in &mut q {
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
    assert_eq!(q.len(), 40);
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
