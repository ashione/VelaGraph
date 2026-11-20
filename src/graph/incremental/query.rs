use super::*;
use crate::gremlin::QueryReadBackend;
use crate::query_memory::{Accounted, AccountedVec, Budget};
use std::{borrow::Cow, cell::Cell};
struct Backend<'v, 'g> {
    view: &'v IncrementalReadView<'g>,
    deadline: Instant,
    operations: Cell<usize>,
    bytes: Cell<usize>,
    limits: IncrementalQueryLimits,
}
impl Backend<'_, '_> {
    fn charge(&self, operations: usize, bytes: usize) -> Result<()> {
        let n = self
            .operations
            .get()
            .checked_add(operations)
            .ok_or_else(|| limit("query work"))?;
        let b = self
            .bytes
            .get()
            .checked_add(bytes)
            .ok_or_else(|| limit("query work"))?;
        if n > self.limits.max_operations || b > self.limits.max_work_bytes {
            return Err(Error::TraversalLimitExceeded(
                "incremental query work budget".into(),
            ));
        }
        self.operations.set(n);
        self.bytes.set(b);
        // Pure accounting: the executor brackets point reads and result return
        // with this backend's deadline callback. Polling the same clock for each
        // counter update duplicates those checkpoints without adding an I/O fence.
        Ok(())
    }
    fn check(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            return Err(Error::TraversalLimitExceeded(
                "incremental query deadline".into(),
            ));
        }
        Ok(())
    }
}
impl QueryReadBackend for Backend<'_, '_> {
    fn get_vertex(&self, id: u64, budget: &Budget) -> Result<Option<Accounted<Vertex>>> {
        Ok(self
            .query_vertex(id, budget)?
            .map(|v| v.map(Cow::into_owned)))
    }
    fn get_edge(&self, id: u64, budget: &Budget) -> Result<Option<Accounted<Edge>>> {
        Ok(self.query_edge(id, budget)?.map(|e| e.map(Cow::into_owned)))
    }
    fn query_vertex(&self, id: u64, budget: &Budget) -> Result<Option<Accounted<Cow<'_, Vertex>>>> {
        self.charge(1, POINT_WORK_BYTES)?;
        self.view
            .state()
            .vertices
            .get(id)
            .map(|v| {
                #[cfg(feature = "query-profiling")]
                crate::query_profile::read_event(
                    crate::query_profile::ReadEvent::IncrementalVertex,
                );
                let b = v.record.bytes;
                let v = &v.record.value;
                self.charge(0, b)?;
                Ok(Accounted::new(Cow::Borrowed(v), budget.reserve(b)?))
            })
            .transpose()
    }
    fn query_edge(&self, id: u64, budget: &Budget) -> Result<Option<Accounted<Cow<'_, Edge>>>> {
        self.charge(1, POINT_WORK_BYTES)?;
        self.view
            .state()
            .edges
            .get(id)
            .map(|e| {
                #[cfg(feature = "query-profiling")]
                crate::query_profile::read_event(crate::query_profile::ReadEvent::IncrementalEdge);
                let b = e.bytes;
                let e = &e.value;
                self.charge(0, b)?;
                Ok(Accounted::new(Cow::Borrowed(e), budget.reserve(b)?))
            })
            .transpose()
    }
    fn scan_vertices_checked(
        &self,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
        budget: &Budget,
    ) -> Result<(AccountedVec<Accounted<Vertex>>, bool)> {
        check()?;
        let take = limit
            .min(crate::MAX_GREMLIN_ROWS)
            .min(self.view.state().vertex_count);
        let _scratch = budget.reserve(SHARDS * SCAN_HEAP_ENTRY_FEE + take * POINT_WORK_BYTES)?;
        let ids = self.view.state().vertices.sorted_ids(take);
        let mut out = AccountedVec::new(budget);
        for id in ids {
            check()?;
            if let Some(v) = self.get_vertex(id, budget)? {
                out.push(v)?
            }
        }
        check()?;
        Ok((out, take < self.view.state().vertex_count))
    }
    fn scan_edges_checked(
        &self,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
        budget: &Budget,
    ) -> Result<(AccountedVec<Accounted<Edge>>, bool)> {
        check()?;
        let take = limit
            .min(crate::MAX_GREMLIN_ROWS)
            .min(self.view.state().edge_count);
        let _scratch = budget.reserve(SHARDS * SCAN_HEAP_ENTRY_FEE + take * POINT_WORK_BYTES)?;
        let ids = self.view.state().edges.sorted_ids(take);
        let mut out = AccountedVec::new(budget);
        for id in ids {
            check()?;
            if let Some(e) = self.get_edge(id, budget)? {
                out.push(e)?
            }
        }
        check()?;
        Ok((out, take < self.view.state().edge_count))
    }
    fn visit_adjacent_edges(
        &self,
        id: u64,
        incoming: bool,
        label: Option<&str>,
        check: &mut impl FnMut() -> Result<()>,
        visit: &mut impl FnMut(Accounted<Edge>) -> Result<()>,
        budget: &Budget,
    ) -> Result<()> {
        self.visit_query_edges(
            id,
            incoming,
            label,
            check,
            &mut |e| visit(e.map(Cow::into_owned)),
            budget,
        )
    }
    fn visit_query_edges<'a>(
        &'a self,
        id: u64,
        incoming: bool,
        label: Option<&str>,
        check: &mut impl FnMut() -> Result<()>,
        visit: &mut impl FnMut(Accounted<Cow<'a, Edge>>) -> Result<()>,
        budget: &Budget,
    ) -> Result<()> {
        check()?;
        self.charge(1, POINT_WORK_BYTES)?;
        if let Some(label) = label {
            validate_label(label)?
        }
        let vertex = self
            .view
            .state()
            .vertices
            .get(id)
            .ok_or(Error::VertexNotFound(id))?;
        let adj = if incoming {
            &vertex.incoming
        } else {
            &vertex.outgoing
        };
        for id in adj.ids() {
            let e = self
                .view
                .state()
                .edges
                .get(id)
                .ok_or_else(|| bad("query adjacency"))?;
            check()?;
            self.charge(1, POINT_WORK_BYTES)?;
            #[cfg(feature = "query-profiling")]
            crate::query_profile::read_event(crate::query_profile::ReadEvent::IncrementalCandidate);
            if label.is_some_and(|l| l != e.label) {
                continue;
            }
            let b = e.bytes;
            let e = &e.value;
            self.charge(0, b)?;
            visit(Accounted::new(Cow::Borrowed(e), budget.reserve(b)?))?;
        }
        check()
    }
}
impl IncrementalReadView<'_> {
    /// Execute against one view with cumulative work/result limits and cancellation.
    pub fn execute_query_batch(
        &self,
        queries: &[GremlinQuery],
        control: &crate::ExecutionControl,
        limits: crate::QueryBatchLimits,
    ) -> Result<Vec<GremlinResult>> {
        control.check()?;
        limits.validate(queries.len())?;
        let backend = Backend {
            view: self,
            deadline: Instant::now() + crate::GREMLIN_DEADLINE,
            operations: Cell::new(0),
            bytes: Cell::new(0),
            limits: IncrementalQueryLimits {
                max_operations: limits.max_work_operations,
                max_work_bytes: limits.max_work_bytes,
            },
        };
        let budget = Budget::new(limits.max_live_bytes);
        crate::execution::execute_batch(
            queries,
            &backend,
            &|| {
                control.check()?;
                backend.check()
            },
            limits,
            &budget,
        )
    }
    /// Parse official Gremlin and execute it against this fixed view.
    pub fn execute_gremlin(&self, text: &str) -> Result<GremlinResult> {
        self.execute_query(&GremlinQuery::parse(text)?)
    }
    /// Execute a parsed query with fresh default query budgets.
    pub fn execute_query(&self, query: &GremlinQuery) -> Result<GremlinResult> {
        self.execute_query_with_limits(query, IncrementalQueryLimits::default())
    }
    /// Bind checked parameters and execute against this view.
    pub fn execute_prepared(
        &self,
        query: &PreparedGremlinQuery,
        bindings: GremlinBindings<'_>,
    ) -> Result<GremlinResult> {
        self.execute_query(&query.bind(bindings)?)
    }
    /// Execute with caller-tightened work limits and the existing shared
    /// Gremlin row/hop/live-memory limits and cooperative deadline.
    pub fn execute_query_with_limits(
        &self,
        query: &GremlinQuery,
        limits: IncrementalQueryLimits,
    ) -> Result<GremlinResult> {
        let max = IncrementalQueryLimits::default();
        if limits.max_operations == 0
            || limits.max_operations > max.max_operations
            || limits.max_work_bytes == 0
            || limits.max_work_bytes > max.max_work_bytes
        {
            return Err(Error::TraversalLimitExceeded(
                "invalid incremental query limits".into(),
            ));
        }
        #[cfg(feature = "query-profiling")]
        crate::query_profile::read_event(crate::query_profile::ReadEvent::IncrementalQuery);
        let backend = Backend {
            view: self,
            deadline: Instant::now() + crate::GREMLIN_DEADLINE,
            operations: Cell::new(0),
            bytes: Cell::new(0),
            limits,
        };
        let budget = Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES);
        query.execute_on_read_budget(&backend, &|| backend.check(), &budget)
    }
}

#[cfg(test)]
mod deadline_tests {
    use super::*;
    fn seeded() -> IncrementalGraph {
        IncrementalGraph::from_records(
            "deadline",
            IncrementalMode::Cow,
            IncrementalLimits::default(),
            vec![Vertex::new(1), Vertex::new(2), Vertex::new(3)],
            vec![Edge::new(1, 1, 2, "x"), Edge::new(2, 1, 3, "x")],
            vec![],
        )
        .unwrap()
    }
    #[test]
    fn expired_execution_rejects_hits_misses_empty_filters_and_scans() {
        let graph = seeded();
        let view = graph.read().unwrap();
        for text in [
            "g.V(1)",
            "g.V(99)",
            "g.V(1).has('missing',1)",
            "g.V().limit(1)",
            "g.E().limit(1)",
        ] {
            let backend = Backend {
                view: &view,
                deadline: Instant::now(),
                operations: Cell::new(0),
                bytes: Cell::new(0),
                limits: IncrementalQueryLimits::default(),
            };
            assert!(GremlinQuery::parse(text)
                .unwrap()
                .execute_on_read_budget(
                    &backend,
                    &|| backend.check(),
                    &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES)
                )
                .is_err());
        }
    }
    #[test]
    fn cancellation_is_checked_on_empty_and_filtered_adjacency() {
        let graph = seeded();
        let view = graph.read().unwrap();
        let backend = Backend {
            view: &view,
            deadline: Instant::now() + Duration::from_secs(1),
            operations: Cell::new(0),
            bytes: Cell::new(0),
            limits: IncrementalQueryLimits::default(),
        };
        for (id, label) in [(3, None), (1, Some("different"))] {
            assert!(backend
                .visit_query_edges(
                    id,
                    false,
                    label,
                    &mut || Err(Error::TraversalLimitExceeded("cancelled".into())),
                    &mut |_| panic!("cancelled visitor ran"),
                    &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES)
                )
                .is_err());
        }
    }
    #[test]
    fn cancellation_after_last_visitor_cannot_return_success() {
        let graph = seeded();
        let view = graph.read().unwrap();
        let backend = Backend {
            view: &view,
            deadline: Instant::now() + Duration::from_secs(1),
            operations: Cell::new(0),
            bytes: Cell::new(0),
            limits: IncrementalQueryLimits::default(),
        };
        let cancelled = Cell::new(false);
        let result = backend.visit_query_edges(
            2,
            true,
            None,
            &mut || {
                if cancelled.get() {
                    Err(Error::TraversalLimitExceeded("cancelled".into()))
                } else {
                    Ok(())
                }
            },
            &mut |_| {
                cancelled.set(true);
                Ok(())
            },
            &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
        );
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod batch_control_tests {
    use super::*;
    #[test]
    fn cancel_during_second_query_discards_first_result_and_releases_budget() {
        let graph = IncrementalGraph::from_records(
            "batch-cancel",
            IncrementalMode::Cow,
            IncrementalLimits::default(),
            vec![Vertex::new(1), Vertex::new(2)],
            vec![],
            vec![],
        )
        .unwrap();
        let view = graph.read().unwrap();
        let backend = Backend {
            view: &view,
            deadline: Instant::now() + Duration::from_secs(30),
            operations: Cell::new(0),
            bytes: Cell::new(0),
            limits: IncrementalQueryLimits::default(),
        };
        let budget = Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES);
        let queries = [
            GremlinQuery::parse("g.V(1)").unwrap(),
            GremlinQuery::parse("g.V(2)").unwrap(),
        ];
        let result = crate::execution::execute_batch(
            &queries,
            &backend,
            &|| {
                if backend.operations.get() >= 2 {
                    Err(Error::ExecutionCancelled)
                } else {
                    Ok(())
                }
            },
            crate::QueryBatchLimits::default(),
            &budget,
        );
        assert!(matches!(result, Err(Error::ExecutionCancelled)));
        assert_eq!(backend.operations.get(), 2);
        assert_eq!(budget.used(), 0);
        let control = crate::ExecutionControl::new(
            crate::CancellationToken::default(),
            Duration::from_secs(30),
        )
        .unwrap();
        let result = view
            .execute_query_batch(&queries, &control, crate::QueryBatchLimits::default())
            .unwrap();
        assert_eq!(result[0].rows, vec![crate::GremlinRow::Vertex(1)]);
        assert_eq!(result[1].rows, vec![crate::GremlinRow::Vertex(2)]);
    }
}
