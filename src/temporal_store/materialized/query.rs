use super::*;
use crate::gremlin::QueryReadBackend;
use std::cell::Cell;

struct Backend<'a, 'g> {
    view: &'a TemporalReadModelView<'g>,
    deadline: Instant,
    used: Cell<usize>,
    bytes: Cell<usize>,
    limits: MaterializedQueryLimits,
}
impl Backend<'_, '_> {
    fn check(&self) -> crate::Result<()> {
        if Instant::now() >= self.deadline {
            Err(crate::Error::TraversalLimitExceeded(WORK_MESSAGE.into()))
        } else {
            Ok(())
        }
    }
    fn charge(&self, bytes: usize) -> crate::Result<()> {
        self.check()?;
        let n = self.used.get() + 1;
        let b = self
            .bytes
            .get()
            .checked_add(bytes)
            .ok_or_else(|| crate::Error::TraversalLimitExceeded(WORK_MESSAGE.into()))?;
        if n > self.limits.max_operations || b > self.limits.max_work_bytes {
            return Err(crate::Error::TraversalLimitExceeded(WORK_MESSAGE.into()));
        }
        self.used.set(n);
        self.bytes.set(b);
        Ok(())
    }
    fn copy_work(&self, bytes: usize) -> crate::Result<()> {
        self.check()?;
        let total = self.bytes.get().checked_add(bytes).ok_or_else(work_error)?;
        if total > self.limits.max_work_bytes {
            return Err(work_error());
        }
        self.bytes.set(total);
        Ok(())
    }
    fn visible(&self, slot: u32) -> crate::Result<bool> {
        #[cfg(feature = "query-profiling")]
        crate::query_profile::read_event(crate::query_profile::ReadEvent::MaterializedVertexCheck);
        self.charge(std::mem::size_of::<Piece>())?;
        Ok(self.view.graph.vertices[slot as usize]
            .at(self.view.time)
            .is_some())
    }
    fn copy_vertex(&self, p: u32, budget: &Budget) -> crate::Result<Accounted<Vertex>> {
        #[cfg(feature = "query-profiling")]
        crate::query_profile::read_event(crate::query_profile::ReadEvent::MaterializedVertexClone);
        let cached = &self.view.graph.payloads[p as usize];
        let Record::Vertex(v) = &cached.record else {
            return Err(crate::Error::TemporalStore(Box::new(
                TemporalStoreError::Corrupt,
            )));
        };
        self.copy_work(cached.bytes)?;
        let charge = budget.reserve(cached.bytes)?;
        Ok(Accounted::new(v.clone(), charge))
    }
    fn copy_edge(&self, p: u32, budget: &Budget) -> crate::Result<Accounted<Edge>> {
        #[cfg(feature = "query-profiling")]
        crate::query_profile::read_event(crate::query_profile::ReadEvent::MaterializedEdgeClone);
        let cached = &self.view.graph.payloads[p as usize];
        let Record::Edge(e) = &cached.record else {
            return Err(crate::Error::TemporalStore(Box::new(
                TemporalStoreError::Corrupt,
            )));
        };
        self.copy_work(cached.bytes)?;
        let charge = budget.reserve(cached.bytes)?;
        Ok(Accounted::new(e.clone(), charge))
    }
}
impl QueryReadBackend for Backend<'_, '_> {
    fn get_vertex(&self, id: u64, budget: &Budget) -> crate::Result<Option<Accounted<Vertex>>> {
        self.charge(std::mem::size_of::<Piece>())?;
        let g = self.view.graph;
        let Some(slot) = g.vertex_index.get(&id) else {
            return Ok(None);
        };
        g.vertices[*slot as usize]
            .at(self.view.time)
            .map(|p| self.copy_vertex(p.payload, budget))
            .transpose()
    }
    fn get_edge(&self, id: u64, budget: &Budget) -> crate::Result<Option<Accounted<Edge>>> {
        self.charge(std::mem::size_of::<Piece>())?;
        let g = self.view.graph;
        let Some(slot) = g.edge_index.get(&id) else {
            return Ok(None);
        };
        let Some(piece) = g.edges[*slot as usize].at(self.view.time) else {
            return Ok(None);
        };
        let Record::Edge(e) = &g.payloads[piece.payload as usize].record else {
            return Err(crate::Error::TemporalStore(Box::new(
                TemporalStoreError::Corrupt,
            )));
        };
        if !self.visible(g.vertex_index[&e.from])? || !self.visible(g.vertex_index[&e.to])? {
            return Ok(None);
        }
        Ok(Some(self.copy_edge(piece.payload, budget)?))
    }
    fn scan_vertices_checked(
        &self,
        _: usize,
        _: &mut impl FnMut() -> crate::Result<()>,
        _: &Budget,
    ) -> crate::Result<(AccountedVec<Accounted<Vertex>>, bool)> {
        Err(unsupported())
    }
    fn scan_edges_checked(
        &self,
        _: usize,
        _: &mut impl FnMut() -> crate::Result<()>,
        _: &Budget,
    ) -> crate::Result<(AccountedVec<Accounted<Edge>>, bool)> {
        Err(unsupported())
    }
    fn visit_adjacent_edges(
        &self,
        id: u64,
        incoming: bool,
        label: Option<&str>,
        check: &mut impl FnMut() -> crate::Result<()>,
        visit: &mut impl FnMut(Accounted<Edge>) -> crate::Result<()>,
        budget: &Budget,
    ) -> crate::Result<()> {
        self.check()?;
        let g = self.view.graph;
        let Some(slot) = g.vertex_index.get(&id) else {
            return Ok(());
        };
        let rows = if incoming {
            &g.incoming[*slot as usize]
        } else {
            &g.outgoing[*slot as usize]
        };
        for index in rows {
            #[cfg(feature = "query-profiling")]
            crate::query_profile::read_event(
                crate::query_profile::ReadEvent::MaterializedCandidate,
            );
            self.charge(ROW_WORK_BYTES)?;
            check()?;
            let row = g.rows[*index as usize];
            if !row.contains(self.view.time)
                || label.is_some_and(|l| l != g.labels[row.label as usize])
            {
                continue;
            }
            if !self.visible(row.from)? || !self.visible(row.to)? {
                continue;
            }
            visit(self.copy_edge(row.payload, budget)?)?;
        }
        Ok(())
    }
}

fn work_error() -> crate::Error {
    crate::Error::TraversalLimitExceeded(WORK_MESSAGE.into())
}
fn unsupported() -> crate::Error {
    crate::Error::UnsupportedGremlinArgument(GLOBAL_MESSAGE.into())
}
impl MaterializedQueryLimits {
    fn validate(self) -> crate::Result<()> {
        if self.max_operations == 0
            || self.max_operations > MAX_QUERY_OPERATIONS
            || self.max_work_bytes == 0
            || self.max_work_bytes > MAX_READ_BYTES
        {
            return Err(work_error());
        }
        Ok(())
    }
}
impl TemporalReadModelView<'_> {
    /// Parse and execute standard Gremlin at this fixed V/T.
    /// Prefer a reused parsed or prepared query for repeated execution.
    pub fn execute_gremlin(&self, text: &str) -> crate::Result<GremlinResult> {
        self.execute_gremlin_with_limits(text, MaterializedQueryLimits::default())
    }
    /// Parse and execute with caller-tightened materialized work limits.
    pub fn execute_gremlin_with_limits(
        &self,
        text: &str,
        limits: MaterializedQueryLimits,
    ) -> crate::Result<GremlinResult> {
        #[cfg(feature = "query-profiling")]
        let _api = crate::query_profile::Timer::start(crate::query_profile::Phase::Api);
        limits.validate()?;
        let query = {
            #[cfg(feature = "query-profiling")]
            let _parse = crate::query_profile::Timer::start(crate::query_profile::Phase::Parse);
            GremlinQuery::parse(text)?
        };
        self.execute(&query, limits)
    }
    /// Execute an existing parsed plan with fresh query budgets and deadline.
    pub fn execute_query(&self, query: &GremlinQuery) -> crate::Result<GremlinResult> {
        self.execute_query_with_limits(query, MaterializedQueryLimits::default())
    }
    /// Execute a parsed plan with caller-tightened materialized work limits.
    pub fn execute_query_with_limits(
        &self,
        query: &GremlinQuery,
        limits: MaterializedQueryLimits,
    ) -> crate::Result<GremlinResult> {
        #[cfg(feature = "query-profiling")]
        let _api = crate::query_profile::Timer::start(crate::query_profile::Phase::Api);
        limits.validate()?;
        self.execute(query, limits)
    }
    /// Bind typed arguments and execute without reparsing the template.
    pub fn execute_prepared(
        &self,
        query: &PreparedGremlinQuery,
        bindings: GremlinBindings<'_>,
    ) -> crate::Result<GremlinResult> {
        self.execute_prepared_with_limits(query, bindings, MaterializedQueryLimits::default())
    }
    /// Bind and execute with caller-tightened materialized work limits.
    pub fn execute_prepared_with_limits(
        &self,
        query: &PreparedGremlinQuery,
        bindings: GremlinBindings<'_>,
        limits: MaterializedQueryLimits,
    ) -> crate::Result<GremlinResult> {
        #[cfg(feature = "query-profiling")]
        let _api = crate::query_profile::Timer::start(crate::query_profile::Phase::Api);
        limits.validate()?;
        let bound = query.bind(bindings)?;
        self.execute(&bound, limits)
    }
    fn execute(
        &self,
        query: &GremlinQuery,
        limits: MaterializedQueryLimits,
    ) -> crate::Result<GremlinResult> {
        match query.start() {
            GremlinStart::Vertices(ids) | GremlinStart::Edges(ids) if ids.is_empty() => {
                return Err(unsupported())
            }
            _ => {}
        }
        let budget = Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES);
        let backend = Backend {
            view: self,
            deadline: Instant::now() + crate::GREMLIN_DEADLINE,
            used: Cell::new(0),
            bytes: Cell::new(0),
            limits,
        };
        #[cfg(feature = "query-profiling")]
        let _execute = crate::query_profile::Timer::start(crate::query_profile::Phase::Execute);
        query.execute_on_read_budget(&backend, &|| backend.check(), &budget)
    }
}
