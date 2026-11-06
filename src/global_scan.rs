//! Bounded scans over a caller-pinned vector of partition read views.
//!
//! This is not a global snapshot or cross-partition edge traversal. The caller
//! owns placement validation, snapshot acquisition, and the end-to-end deadline.
use std::time::Instant;

use crate::gremlin::QueryReadBackend;
use crate::query_memory::{self, Accounted, AccountedVec, Budget};
use crate::{
    Error, GraphRecordReadView, GremlinQuery, GremlinRow, GremlinStart, GremlinStep, Result,
    StorageNode, DEFAULT_GREMLIN_LIMIT, GREMLIN_DEADLINE, MAX_GREMLIN_ACCOUNTED_BYTES,
    MAX_GREMLIN_ROWS,
};

/// A row's partition is part of its identity; local IDs need not be unique.
#[derive(Debug, Clone, PartialEq)]
pub struct QualifiedGremlinRow {
    pub partition: u32,
    pub row: GremlinRow,
}

/// Count is global and therefore has no fabricated originating partition.
#[derive(Debug, Clone, PartialEq)]
pub enum GlobalScanOutput {
    Rows(Vec<QualifiedGremlinRow>),
    Count(u64),
}

/// Validated global scans with order-preserving filters, projections and limits.
#[derive(Debug, Clone)]
pub struct GlobalScanPlan {
    vertices: bool,
    limit: Option<usize>,
    steps: Vec<GremlinStep>,
    explicit_limit: bool,
}

impl GlobalScanPlan {
    /// Parse with the official Gremlin parser and reject unsupported global plans
    /// before the caller performs any catalog or data I/O.
    pub fn parse(query: &str) -> Result<Self> {
        let parsed = GremlinQuery::parse(query)?;
        let vertices = match parsed.start() {
            GremlinStart::Vertices(ids) if ids.is_empty() => true,
            GremlinStart::Edges(ids) if ids.is_empty() => false,
            _ => return Err(unsupported()),
        };
        for step in parsed.steps() {
            if !matches!(
                step,
                GremlinStep::HasLabel(_)
                    | GremlinStep::Has { .. }
                    | GremlinStep::Values(_)
                    | GremlinStep::Label
                    | GremlinStep::Limit(_)
                    | GremlinStep::Count
            ) {
                return Err(unsupported());
            }
        }
        // Only an immediately leading limit can bound candidate acquisition.
        let limit = match parsed.steps().first() {
            Some(GremlinStep::Limit(n)) => Some(*n),
            _ => None,
        };
        Ok(Self {
            vertices,
            limit,
            explicit_limit: parsed
                .steps()
                .iter()
                .any(|s| matches!(s, GremlinStep::Limit(_))),
            steps: parsed.steps().to_vec(),
        })
    }

    /// Execute against retained views using one query-owned 64 MiB budget.
    ///
    /// The caller must pair each view with its actual partition and reuse one
    /// pinned shard sequence for partitions on the same shard. Duplicate
    /// partitions are rejected. `check` must enforce the absolute deadline that
    /// began before snapshot acquisition; it is also called for empty input.
    /// This helper additionally enforces the ordinary five-second execution
    /// deadline. Snapshot storage and caller-owned input are outside its logical
    /// accounting budget, as in the existing single-partition query contract.
    pub fn execute<S: StorageNode>(
        &self,
        views: &[(u32, GraphRecordReadView<'_, S>)],
        check: &dyn Fn() -> Result<()>,
    ) -> Result<GlobalScanOutput> {
        self.execute_budget(views, check, &Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES))
    }

    fn execute_budget<S: StorageNode>(
        &self,
        views: &[(u32, GraphRecordReadView<'_, S>)],
        check: &dyn Fn() -> Result<()>,
        budget: &Budget,
    ) -> Result<GlobalScanOutput> {
        let deadline = Instant::now() + GREMLIN_DEADLINE;
        let check = || {
            check()?;
            if Instant::now() >= deadline {
                return Err(Error::TraversalLimitExceeded(
                    "global scan deadline exceeded".into(),
                ));
            }
            Ok(())
        };
        check()?;
        // No allocation for duplicate checking; callers separately cap fanout.
        for (i, (partition, _)) in views.iter().enumerate() {
            check()?;
            if views[..i].iter().any(|(p, _)| p == partition) {
                return Err(Error::UnsupportedGremlinArgument(
                    "duplicate scan partition".into(),
                ));
            }
        }
        let _control = budget.reserve(query_memory::CONTROL_BYTES)?;
        // With a leading limit only the current top N and one partition's N
        // candidates can coexist. Otherwise retain at most the global row cap.
        let capacity = self.limit.map_or(MAX_GREMLIN_ROWS, |n| n * 2);
        let _slots = budget.reserve(query_memory::mul(
            capacity,
            std::mem::size_of::<(u64, u32)>(),
        )?)?;
        let mut ids = Vec::with_capacity(capacity);
        for (partition, view) in views {
            check()?;
            let scan_limit = self.limit.unwrap_or(MAX_GREMLIN_ROWS - ids.len());
            // A zero remaining allowance still needs a one-record probe to
            // distinguish an empty remaining partition from cap overflow.
            let scan_limit = scan_limit.max(1);
            if self.vertices {
                let (records, truncated) =
                    view.scan_vertices_checked(scan_limit, &mut || check(), budget)?;
                if self.limit.is_none()
                    && (truncated || records.len() > MAX_GREMLIN_ROWS - ids.len())
                {
                    return Err(row_cap());
                }
                for record in records {
                    check()?;
                    ids.push((record.id, *partition));
                }
            } else {
                let (records, truncated) =
                    view.scan_edges_checked(scan_limit, &mut || check(), budget)?;
                if self.limit.is_none()
                    && (truncated || records.len() > MAX_GREMLIN_ROWS - ids.len())
                {
                    return Err(row_cap());
                }
                for record in records {
                    check()?;
                    ids.push((record.id, *partition));
                }
            }
            if let Some(n) = self.limit {
                ids.sort_unstable();
                ids.truncate(n);
            }
            check()?;
        }
        ids.sort_unstable();
        check()?;
        let mut rows = AccountedVec::new(budget);
        for (id, partition) in ids {
            check()?;
            rows.push((
                partition,
                Accounted::new(
                    if self.vertices {
                        GremlinRow::Vertex(id)
                    } else {
                        GremlinRow::Edge(id)
                    },
                    budget.reserve(0)?,
                ),
            ))?;
        }
        for step in &self.steps {
            check()?;
            match step {
                GremlinStep::Limit(n) => rows.truncate(*n),
                GremlinStep::Count => {
                    check()?;
                    return Ok(GlobalScanOutput::Count(rows.len() as u64));
                }
                _ => {
                    let mut output = AccountedVec::new(budget);
                    for (partition, row) in rows {
                        check()?;
                        let view = &views
                            .iter()
                            .find(|(p, _)| *p == partition)
                            .expect("qualified candidate has a pinned view")
                            .1;
                        if let Some(row) = crate::gremlin::apply_global_element_step(
                            view, row, step, &check, budget,
                        )? {
                            output.push((partition, row))?;
                        }
                    }
                    rows = output;
                }
            }
        }
        if !self.explicit_limit {
            rows.truncate(DEFAULT_GREMLIN_LIMIT);
        }
        // Keep input slots and every payload charged until the final output is
        // complete, including payloads moved out of the accounted iterator.
        let _output_slots = budget.reserve(query_memory::mul(
            rows.len(),
            std::mem::size_of::<QualifiedGremlinRow>(),
        )?)?;
        let mut payload_charges = AccountedVec::new(budget);
        let mut output = Vec::with_capacity(rows.len());
        for (partition, row) in rows {
            check()?;
            payload_charges.push(row.charge)?;
            output.push(QualifiedGremlinRow {
                partition,
                row: row.value,
            });
        }
        check()?;
        Ok(GlobalScanOutput::Rows(output))
    }
}

fn unsupported() -> Error {
    Error::UnsupportedGremlinStep(
        "global scan supports V()/E(), hasLabel/has, values/label, limit, and terminal count()"
            .into(),
    )
}
fn row_cap() -> Error {
    Error::TraversalLimitExceeded("global scan row cap exceeded; add a leading limit".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn plans_only_the_documented_subset() {
        for text in [
            "g.V()",
            "g.E()",
            "g.V().limit(2)",
            "g.E().count()",
            "g.V().limit(2).count()",
        ] {
            GlobalScanPlan::parse(text).unwrap();
        }
        for text in ["g.V(1)", "g.E(1)", "g.V().out()", "g.V().id()"] {
            assert!(matches!(
                GlobalScanPlan::parse(text),
                Err(Error::UnsupportedGremlinStep(_))
            ));
        }
    }
    struct Node(crate::ReadView);
    impl StorageNode for Node {
        fn admin(&self) -> &dyn crate::StorageAdmin {
            unreachable!()
        }
        fn read_view(&self) -> std::result::Result<crate::ReadView, crate::StorageNodeError> {
            Ok(self.0.clone())
        }
        fn get(
            &self,
            key: &[u8],
            view: &crate::ReadView,
        ) -> std::result::Result<Option<Vec<u8>>, crate::StorageNodeError> {
            Ok(view.get(key))
        }
        fn scan(
            &self,
            _: crate::KeyRange,
            _: crate::ScanLimit,
            _: &crate::ReadView,
        ) -> std::result::Result<Vec<crate::KeyValue>, crate::StorageNodeError> {
            panic!("must page")
        }
        fn scan_page(
            &self,
            range: crate::KeyRange,
            limit: crate::ScanLimit,
            view: &crate::ReadView,
            cursor: Option<&crate::ScanCursor>,
        ) -> std::result::Result<crate::ScanPage, crate::StorageNodeError> {
            view.scan_page(range, limit, cursor)
        }
        fn batch_write(
            &self,
            _: crate::WriteRequest,
        ) -> std::result::Result<crate::WriteResult, crate::StorageNodeError> {
            unreachable!()
        }
    }
    fn store(partition: u32, ids: &[u64]) -> crate::GraphRecordStore<Node> {
        let entries = ids
            .iter()
            .map(|id| {
                let vertex = crate::Vertex {
                    id: *id,
                    labels: vec!["V".into()],
                    properties: std::collections::BTreeMap::new(),
                };
                (
                    crate::graph::vertex_key(partition, *id),
                    crate::graph::codec::encode_vertex(&vertex).unwrap(),
                )
            })
            .collect();
        crate::GraphRecordStore::new(
            Node(crate::ReadView::from_entries(7, 1, 1, entries).unwrap()),
            7,
            partition,
        )
    }
    fn rich_store(partition: u32, count: u64) -> crate::GraphRecordStore<Node> {
        let entries = (1..=count)
            .map(|id| {
                let mut vertex = crate::Vertex::new(id);
                vertex.labels = vec!["first".into(), "person".into()];
                if id % 2 == 0 {
                    vertex
                        .properties
                        .insert("x".into(), crate::Value::Integer(partition.into()));
                    vertex.properties.insert("null".into(), crate::Value::Null);
                    vertex
                        .properties
                        .insert("text".into(), crate::Value::Text("1".into()));
                    vertex
                        .properties
                        .insert("flag".into(), crate::Value::Boolean(true));
                }
                (
                    crate::graph::vertex_key(partition, id),
                    crate::graph::codec::encode_vertex(&vertex).unwrap(),
                )
            })
            .collect();
        crate::GraphRecordStore::new(
            Node(crate::ReadView::from_entries(7, 1, 1, entries).unwrap()),
            7,
            partition,
        )
    }
    #[test]
    fn filters_projections_preserve_origin_order_and_step_position() {
        let a = rich_store(1, 240);
        let b = rich_store(2, 240);
        let views = [(2, b.read_view().unwrap()), (1, a.read_view().unwrap())];
        let run = |q| {
            GlobalScanPlan::parse(q)
                .unwrap()
                .execute(&views, &|| Ok(()))
                .unwrap()
        };
        assert_eq!(
            run("g.V().limit(2).has('x').count()"),
            GlobalScanOutput::Count(0)
        );
        assert_eq!(
            run("g.V().has('x').limit(2).count()"),
            GlobalScanOutput::Count(2)
        );
        assert_eq!(
            run("g.V().hasLabel('person').has('x').count()"),
            GlobalScanOutput::Count(240)
        );
        assert_eq!(
            run("g.V().values('x').count()"),
            GlobalScanOutput::Count(240)
        );
        assert_eq!(
            run("g.V().has('x',1).count()"),
            GlobalScanOutput::Count(120)
        );
        assert_eq!(
            run("g.V().has('x','1').count()"),
            GlobalScanOutput::Count(0)
        );
        assert_eq!(
            run("g.V().has('x',1.0).count()"),
            GlobalScanOutput::Count(0)
        );
        assert_eq!(
            run("g.V().has('null').count()"),
            GlobalScanOutput::Count(240)
        );
        assert_eq!(
            run("g.V().values('missing').has('x').count()"),
            GlobalScanOutput::Count(0)
        );
        assert_eq!(
            run("g.V().has('null',null).has('flag',true).has('text','1').count()"),
            GlobalScanOutput::Count(240)
        );
        assert_eq!(
            run("g.V().values('missing').count()"),
            GlobalScanOutput::Count(0)
        );
        assert_eq!(
            run("g.V().values('x').limit(2).limit(1)"),
            GlobalScanOutput::Rows(vec![QualifiedGremlinRow {
                partition: 1,
                row: GremlinRow::Value(crate::Value::Integer(1))
            }])
        );
        assert_eq!(
            run("g.V().limit(1).label()"),
            GlobalScanOutput::Rows(vec![QualifiedGremlinRow {
                partition: 1,
                row: GremlinRow::Value(crate::Value::Text("first".into()))
            }])
        );
        let GlobalScanOutput::Rows(rows) = run("g.V().has('x').values('x')") else {
            panic!("rows")
        };
        assert_eq!(rows.len(), 100);
        assert!(matches!(
            GlobalScanPlan::parse("g.V().label().has('x')")
                .unwrap()
                .execute(&views, &|| Ok(())),
            Err(Error::InvalidGremlinTraverser(_))
        ));
    }
    #[test]
    fn late_filters_and_limits_do_not_waive_candidate_cap() {
        let a = rich_store(1, 6000);
        let b = rich_store(2, 6000);
        let views = [(1, a.read_view().unwrap()), (2, b.read_view().unwrap())];
        for q in [
            "g.V().has('missing').limit(1)",
            "g.V().values('missing').count()",
            "g.V().hasLabel('absent').count()",
        ] {
            assert!(
                matches!(
                    GlobalScanPlan::parse(q)
                        .unwrap()
                        .execute(&views, &|| Ok(())),
                    Err(Error::TraversalLimitExceeded(_))
                ),
                "{q}"
            );
        }
        assert_eq!(
            GlobalScanPlan::parse("g.V().limit(1).has('missing').count()")
                .unwrap()
                .execute(&views, &|| Ok(()))
                .unwrap(),
            GlobalScanOutput::Count(0)
        );
    }
    #[test]
    fn projection_budget_and_deadline_release_all_charges() {
        let a = rich_store(1, 200);
        let b = rich_store(2, 200);
        let views = [(1, a.read_view().unwrap()), (2, b.read_view().unwrap())];
        let plan = GlobalScanPlan::parse("g.V().has('text').values('text').limit(200)").unwrap();
        let budget = Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES);
        plan.execute_budget(&views, &|| Ok(()), &budget).unwrap();
        assert_eq!(budget.used(), 0);
        let exact = Budget::new(budget.peak());
        plan.execute_budget(&views, &|| Ok(()), &exact).unwrap();
        assert_eq!(exact.used(), 0);
        // Page sizes adapt to remaining budget; peak-minus-one need not fail.
        let short = Budget::new(query_memory::CONTROL_BYTES);
        assert!(matches!(
            plan.execute_budget(&views, &|| Ok(()), &short),
            Err(Error::TraversalLimitExceeded(_))
        ));
        assert_eq!(short.used(), 0);
        let checks = std::cell::Cell::new(0);
        assert!(plan
            .execute_budget(
                &views,
                &|| {
                    checks.set(checks.get() + 1);
                    if checks.get() == 1500 {
                        Err(row_cap())
                    } else {
                        Ok(())
                    }
                },
                &budget
            )
            .is_err());
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn projected_payloads_share_one_budget_across_partitions() {
        let make = |partition| {
            let entries = (1..=20)
                .map(|id| {
                    let mut vertex = crate::Vertex::new(id);
                    vertex
                        .properties
                        .insert("x".into(), crate::Value::Text("x".repeat(16 * 1024)));
                    (
                        crate::graph::vertex_key(partition, id),
                        crate::graph::codec::encode_vertex(&vertex).unwrap(),
                    )
                })
                .collect();
            crate::GraphRecordStore::new(
                Node(crate::ReadView::from_entries(7, 1, 1, entries).unwrap()),
                7,
                partition,
            )
        };
        let a = make(1);
        let b = make(2);
        let plan = GlobalScanPlan::parse("g.V().values('x')").unwrap();
        let budget = Budget::new(750 * 1024);
        for (partition, store) in [(1, &a), (2, &b)] {
            plan.execute_budget(
                &[(partition, store.read_view().unwrap())],
                &|| Ok(()),
                &budget,
            )
            .unwrap();
            assert_eq!(budget.used(), 0);
        }
        let both = [(1, a.read_view().unwrap()), (2, b.read_view().unwrap())];
        // Acquisition and plain ID output fit; accumulating values across both
        // partitions must fail, even though each partition fits by itself.
        GlobalScanPlan::parse("g.V()")
            .unwrap()
            .execute_budget(&both, &|| Ok(()), &budget)
            .unwrap();
        assert!(matches!(
            plan.execute_budget(&both, &|| Ok(()), &budget),
            Err(Error::TraversalLimitExceeded(_))
        ));
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn global_order_limit_count_and_partition_identity() {
        let a = store(10, &[1, 7]);
        let b = store(20, &[1, 2]);
        let views = [(20, b.read_view().unwrap()), (10, a.read_view().unwrap())];
        let GlobalScanOutput::Rows(rows) = GlobalScanPlan::parse("g.V()")
            .unwrap()
            .execute(&views, &|| Ok(()))
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(
            rows.iter()
                .map(|r| (r.partition, r.row.clone()))
                .collect::<Vec<_>>(),
            vec![
                (10, GremlinRow::Vertex(1)),
                (20, GremlinRow::Vertex(1)),
                (20, GremlinRow::Vertex(2)),
                (10, GremlinRow::Vertex(7))
            ]
        );
        assert_eq!(
            GlobalScanPlan::parse("g.V().count()")
                .unwrap()
                .execute(&views, &|| Ok(()))
                .unwrap(),
            GlobalScanOutput::Count(4)
        );
        assert_eq!(
            GlobalScanPlan::parse("g.V().limit(2).count()")
                .unwrap()
                .execute(&views, &|| Ok(()))
                .unwrap(),
            GlobalScanOutput::Count(2)
        );
    }
    #[test]
    fn global_cap_is_not_per_partition_and_limit_bounds_it() {
        let ids: Vec<_> = (0..6000).collect();
        let a = store(10, &ids);
        let b = store(20, &ids);
        let views = [(10, a.read_view().unwrap()), (20, b.read_view().unwrap())];
        assert!(matches!(
            GlobalScanPlan::parse("g.V().count()")
                .unwrap()
                .execute(&views, &|| Ok(())),
            Err(Error::TraversalLimitExceeded(_))
        ));
        assert_eq!(
            GlobalScanPlan::parse("g.V().limit(1).count()")
                .unwrap()
                .execute(&views, &|| Ok(()))
                .unwrap(),
            GlobalScanOutput::Count(1)
        );
    }
    #[test]
    fn default_cap_does_not_change_count_and_checks_run_on_empty_input() {
        let ids: Vec<_> = (0..80).collect();
        let a = store(10, &ids);
        let b = store(20, &ids);
        let views = [(10, a.read_view().unwrap()), (20, b.read_view().unwrap())];
        let GlobalScanOutput::Rows(rows) = GlobalScanPlan::parse("g.V()")
            .unwrap()
            .execute(&views, &|| Ok(()))
            .unwrap()
        else {
            panic!("rows")
        };
        assert_eq!(rows.len(), 100);
        let plan = GlobalScanPlan::parse("g.V().count()").unwrap();
        assert_eq!(
            plan.execute(&views, &|| Ok(())).unwrap(),
            GlobalScanOutput::Count(160)
        );
        assert_eq!(
            plan.execute::<Node>(&[], &|| Ok(())).unwrap(),
            GlobalScanOutput::Count(0)
        );
        assert!(plan.execute::<Node>(&[], &|| Err(row_cap())).is_err());
        let duplicate = [(10, a.read_view().unwrap()), (10, a.read_view().unwrap())];
        assert!(plan.execute(&duplicate, &|| Ok(())).is_err());
    }
    #[test]
    fn shared_budget_releases_after_success_and_failure() {
        let a = store(10, &[1, 7]);
        let b = store(20, &[1, 2]);
        let views = [(10, a.read_view().unwrap()), (20, b.read_view().unwrap())];
        let plan = GlobalScanPlan::parse("g.V().limit(2)").unwrap();
        let budget = Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES);
        plan.execute_budget(&views, &|| Ok(()), &budget).unwrap();
        assert_eq!(budget.used(), 0);
        let required = budget.peak();
        assert!(required > query_memory::CONTROL_BYTES);
        let tiny = Budget::new(query_memory::CONTROL_BYTES);
        assert!(matches!(
            plan.execute_budget(&views, &|| Ok(()), &tiny),
            Err(Error::TraversalLimitExceeded(_))
        ));
        assert_eq!(tiny.used(), 0);
        let injected = Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES);
        let calls = std::cell::Cell::new(0);
        assert!(plan
            .execute_budget(
                &views,
                &|| {
                    calls.set(calls.get() + 1);
                    if calls.get() == 10 {
                        Err(row_cap())
                    } else {
                        Ok(())
                    }
                },
                &injected
            )
            .is_err());
        assert_eq!(injected.used(), 0);
    }
    #[test]
    fn audit_exact_cap_with_trailing_empty_and_one_more() {
        let ids: Vec<_> = (0..10000).collect();
        let a = store(10, &ids);
        let empty = store(20, &[]);
        let one = store(30, &[u64::MAX]);
        let plan = GlobalScanPlan::parse("g.V().count()").unwrap();
        assert_eq!(
            plan.execute(
                &[
                    (10, a.read_view().unwrap()),
                    (20, empty.read_view().unwrap())
                ],
                &|| Ok(())
            )
            .unwrap(),
            GlobalScanOutput::Count(10000)
        );
        assert!(matches!(
            plan.execute(
                &[
                    (10, a.read_view().unwrap()),
                    (20, empty.read_view().unwrap()),
                    (30, one.read_view().unwrap())
                ],
                &|| Ok(())
            ),
            Err(Error::TraversalLimitExceeded(_))
        ));
    }
    #[test]
    fn audit_limit_bounds_and_extreme_id_order() {
        for q in [
            "g.V().limit(0)",
            "g.E().limit(-1)",
            "g.V().limit(1001)",
            "g.V().count().limit(1)",
        ] {
            assert!(GlobalScanPlan::parse(q).is_err(), "{q}");
        }
        let a = store(u32::MAX, &[0, u64::MAX]);
        let b = store(0, &[0, u64::MAX]);
        let output = GlobalScanPlan::parse("g.V().limit(1000)")
            .unwrap()
            .execute(
                &[
                    (u32::MAX, a.read_view().unwrap()),
                    (0, b.read_view().unwrap()),
                ],
                &|| Ok(()),
            )
            .unwrap();
        let GlobalScanOutput::Rows(rows) = output else {
            panic!("rows")
        };
        assert_eq!(
            rows.iter().map(|r| r.partition).collect::<Vec<_>>(),
            vec![0, u32::MAX, 0, u32::MAX]
        );
        assert_eq!(rows[0].row, GremlinRow::Vertex(0));
        assert_eq!(rows[3].row, GremlinRow::Vertex(u64::MAX));
    }
}
