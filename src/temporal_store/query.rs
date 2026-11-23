//! Indexed temporal Gremlin. No global scan fallback.
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use super::query_memo::RecordMemo;
use super::*;
use crate::gremlin::QueryReadBackend;
use crate::query_memory::{Accounted, AccountedVec, Budget, Reclaim};
use crate::{
    Error, GremlinBindings, GremlinQuery, GremlinResult, GremlinStart, PreparedGremlinQuery,
};

impl TemporalStoreView<'_> {
    /// Execute official Gremlin point queries and out/in/both at this V/T.
    /// Global starts reject before reads; adjacency uses a persistent candidate index.
    pub fn execute_gremlin(&self, text: &str) -> crate::Result<GremlinResult> {
        #[cfg(feature = "query-profiling")]
        let _api = crate::query_profile::Timer::start(crate::query_profile::Phase::Api);
        // Syntax plans and revision prefixes are reused; each execution has fresh
        // V/T checks, endpoint validation, work limits and live-memory charges.
        // A poisoned optimization cache is treated as a miss; query/storage errors
        // still propagate through the unchanged execution path below.
        let cached = self
            .plan_cache
            .lock()
            .ok()
            .and_then(|mut cache| cache.get(text));
        let query = match cached {
            Some(query) => {
                #[cfg(feature = "query-profiling")]
                crate::query_profile::cache_hit();
                query
            }
            None => {
                let query = {
                    #[cfg(feature = "query-profiling")]
                    let _parse =
                        crate::query_profile::Timer::start(crate::query_profile::Phase::Parse);
                    Arc::new(GremlinQuery::parse(text)?)
                };
                // Parsing and all execution happen outside the cache lock.
                if let Ok(mut cache) = self.plan_cache.lock() {
                    cache.insert(text, Arc::clone(&query));
                }
                query
            }
        };
        self.execute_query_inner(&query)
    }

    /// Execute an immutable parsed plan with fresh mode checks and budgets at this view's V/T.
    pub fn execute_query(&self, query: &GremlinQuery) -> crate::Result<GremlinResult> {
        #[cfg(feature = "query-profiling")]
        let _api = crate::query_profile::Timer::start(crate::query_profile::Phase::Api);
        self.execute_query_inner(query)
    }

    /// Bind typed arguments without reparsing, then execute with a fresh budget.
    pub fn execute_prepared(
        &self,
        query: &PreparedGremlinQuery,
        bindings: GremlinBindings<'_>,
    ) -> crate::Result<GremlinResult> {
        #[cfg(feature = "query-profiling")]
        let _api = crate::query_profile::Timer::start(crate::query_profile::Phase::Api);
        let bound = query.bind(bindings)?;
        self.execute_query_inner(&bound)
    }

    /// Execute one fixed-V/T batch, sharing revision work, memo and live budgets.
    /// Existing temporal revision ceilings remain stricter than caller limits.
    pub fn execute_query_batch(
        &self,
        queries: &[GremlinQuery],
        control: &crate::ExecutionControl,
        limits: crate::QueryBatchLimits,
    ) -> crate::Result<Vec<GremlinResult>> {
        control.check()?;
        limits.validate(queries.len())?;
        for query in queries {
            if matches!(query.start(), GremlinStart::Vertices(ids)|GremlinStart::Edges(ids) if ids.is_empty())
            {
                return Err(Error::UnsupportedGremlinArgument(
                    "temporal global start requires an index".into(),
                ));
            }
        }
        let budget = Budget::new(limits.max_live_bytes);
        let mut backend = Backend::new(self, &budget);
        backend.control = Some(control.clone());
        backend.limits = limits;
        crate::execution::execute_batch(queries, &backend, &|| backend.check(), limits, &budget)
    }

    fn execute_query_inner(&self, query: &GremlinQuery) -> crate::Result<GremlinResult> {
        match query.start() {
            GremlinStart::Vertices(ids) | GremlinStart::Edges(ids) if ids.is_empty() => {
                return Err(Error::UnsupportedGremlinArgument(
                    "temporal global start requires an index".into(),
                ))
            }
            _ => {}
        }
        let budget = Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES);
        let backend = Backend::new(self, &budget);
        {
            #[cfg(feature = "query-profiling")]
            let _execute = crate::query_profile::Timer::start(crate::query_profile::Phase::Execute);
            query.execute_on_read_budget(&backend, &|| backend.check(), &budget)
        }
    }
}
struct Backend<'a, 'db> {
    view: &'a TemporalStoreView<'db>,
    deadline: Instant,
    control: Option<crate::ExecutionControl>,
    limits: crate::QueryBatchLimits,
    rows: Cell<usize>,
    bytes: Cell<usize>,
    memo: Rc<RecordMemo>,
    // Separate from adjacency iteration, whose visitor resolves records.
    record_cursor: RefCell<Option<rocksdb::DBRawIterator<'a>>>,
    // True only after observing the first key beyond the previous identity.
    record_lookahead: Cell<bool>,
    adjacency_cursor: RefCell<Option<rocksdb::DBRawIterator<'a>>>,
}
fn error(error: TemporalStoreError) -> Error {
    Error::TemporalStore(Box::new(error))
}
impl<'a, 'db> Backend<'a, 'db> {
    fn new(view: &'a TemporalStoreView<'db>, budget: &Budget) -> Self {
        let memo = Rc::new(RecordMemo::default());
        let reclaim: Rc<dyn Reclaim> = memo.clone();
        budget.set_reclaimer(Rc::downgrade(&reclaim));
        Self {
            view,
            deadline: Instant::now() + crate::GREMLIN_DEADLINE,
            control: None,
            limits: crate::QueryBatchLimits::default(),
            rows: Cell::new(0),
            bytes: Cell::new(0),
            memo,
            record_cursor: RefCell::new(None),
            record_lookahead: Cell::new(false),
            adjacency_cursor: RefCell::new(None),
        }
    }
    fn check(&self) -> crate::Result<()> {
        if let Some(control) = &self.control {
            control.check()?;
        }
        if Instant::now() >= self.deadline {
            return Err(Error::TraversalLimitExceeded(
                "temporal query deadline".into(),
            ));
        }
        Ok(())
    }
    fn charge(&self, bytes: usize) -> crate::Result<()> {
        self.charge_many(1, bytes)
    }
    fn charge_many(&self, count: usize, bytes: usize) -> crate::Result<()> {
        let rows = self
            .rows
            .get()
            .checked_add(count)
            .ok_or_else(|| error(TemporalStoreError::Limit))?;
        let bytes = self
            .bytes
            .get()
            .checked_add(bytes)
            .ok_or_else(|| error(TemporalStoreError::Limit))?;
        if rows > MAX_READ_REVISIONS.min(self.limits.max_work_operations)
            || bytes > MAX_READ_BYTES.min(self.limits.max_work_bytes)
        {
            return Err(error(TemporalStoreError::Limit));
        }
        self.rows.set(rows);
        self.bytes.set(bytes);
        Ok(())
    }
    fn record<T>(
        &self,
        identity: Key,
        budget: &Budget,
        decode: fn(&[u8], &Budget) -> crate::Result<Accounted<T>>,
    ) -> crate::Result<Option<Accounted<T>>> {
        self.check()?;
        #[cfg(feature = "query-profiling")]
        {
            use crate::query_profile::{read_event, ReadEvent};
            read_event(ReadEvent::Read);
        }
        if let Some(block) = self.view.revision_cache.get(
            identity,
            self.view.version,
            self.view.snapshot.sequence_number(),
        ) {
            if let Some(hit) = block.lookup(self.view.valid_time) {
                // If any prefix admission might fail, replay storage in original
                // row order so competing memory/work errors keep their precedence.
                let fits = self.rows.get().checked_add(hit.rows).is_some_and(|rows| {
                    rows <= MAX_READ_REVISIONS.min(self.limits.max_work_operations)
                }) && self
                    .bytes
                    .get()
                    .checked_add(hit.bytes)
                    .is_some_and(|bytes| bytes <= MAX_READ_BYTES.min(self.limits.max_work_bytes))
                    && hit.peak_bytes <= budget.available();
                if fits {
                    self.charge_many(hit.rows, hit.bytes)?;
                    // Reproduce the largest transient raw-row reservation before
                    // decoding the selected payload under its original raw charge.
                    drop(budget.reserve(hit.peak_bytes)?);
                    let _raw = budget.reserve(hit.selected_bytes)?;
                    self.check()?;
                    return hit
                        .payload
                        .map(|bytes| {
                            #[cfg(feature = "query-profiling")]
                            crate::query_profile::read_event(
                                crate::query_profile::ReadEvent::Decode,
                            );
                            decode(bytes, budget)
                        })
                        .transpose();
                }
            }
        }
        let mut builder = self.view.revision_cache.builder();
        let start = revision_key_bytes(identity, self.view.version, 0);
        let prefix = &start[..IDENTITY_PREFIX_LEN];
        let view: &'a TemporalStoreView<'db> = self.view;
        let mut slot = self.record_cursor.borrow_mut();
        let cursor = slot.get_or_insert_with(|| {
            #[cfg(feature = "query-profiling")]
            crate::query_profile::read_event(crate::query_profile::ReadEvent::Iterator);
            view.snapshot.raw_iterator()
        });
        let reuse_lookahead = self.record_lookahead.replace(false)
            && cursor
                .key()
                .is_some_and(|key| key.starts_with(prefix) && key >= start.as_slice());
        if reuse_lookahead {
            #[cfg(feature = "query-profiling")]
            crate::query_profile::read_event(crate::query_profile::ReadEvent::LookaheadHit);
        } else {
            cursor.seek(start.as_slice());
            #[cfg(feature = "query-profiling")]
            crate::query_profile::read_event(crate::query_profile::ReadEvent::Seek);
        }
        loop {
            self.check()?;
            // Check status before treating an invalid cursor as absence.
            cursor.status()?;
            let Some((key, value)) = cursor.item() else {
                break;
            };
            if !key.starts_with(prefix) {
                self.record_lookahead.set(true);
                break;
            }
            self.charge(key.len() + value.len())?;
            let _raw = budget.reserve(key.len() + value.len())?;
            let (valid, payload) = decode_revision(key, value, self.view.version).map_err(error)?;
            revision_cache::capture(&mut builder, valid, value);
            if valid.from <= self.view.valid_time
                && valid.to.is_none_or(|to| self.view.valid_time < to)
            {
                self.check()?;
                let result = payload
                    .map(|bytes| {
                        #[cfg(feature = "query-profiling")]
                        crate::query_profile::read_event(crate::query_profile::ReadEvent::Decode);
                        decode(bytes, budget)
                    })
                    .transpose()?;
                if let Some(builder) = builder {
                    self.view.revision_cache.insert(
                        identity,
                        self.view.version,
                        self.view.snapshot.sequence_number(),
                        builder,
                        false,
                    );
                }
                return Ok(result);
            }
            cursor.next();
        }
        if let Some(builder) = builder {
            self.view.revision_cache.insert(
                identity,
                self.view.version,
                self.view.snapshot.sequence_number(),
                builder,
                true,
            );
        }
        Ok(None)
    }
}
impl<'a, 'db> QueryReadBackend for Backend<'a, 'db> {
    fn get_vertex(&self, id: u64, budget: &Budget) -> crate::Result<Option<Accounted<Vertex>>> {
        self.check()?;
        let cached = self.memo.vertices.borrow_mut().take(id);
        if let Some(vertex) = cached {
            #[cfg(feature = "query-profiling")]
            crate::query_profile::read_event(crate::query_profile::ReadEvent::CacheHit);
            self.memo.vertices.borrow_mut().retain(id, &vertex, budget);
            return Ok(vertex);
        }
        let Some(vertex) = self.record(Key::Vertex(id), budget, codec::decode_vertex_accounted)?
        else {
            self.memo.vertices.borrow_mut().retain(id, &None, budget);
            return Ok(None);
        };
        if vertex.id != id {
            return Err(error(TemporalStoreError::Corrupt));
        }
        validate_vertex(&vertex)?;
        let vertex = Some(vertex);
        self.memo.vertices.borrow_mut().retain(id, &vertex, budget);
        Ok(vertex)
    }
    fn get_edge(&self, id: u64, budget: &Budget) -> crate::Result<Option<Accounted<Edge>>> {
        self.check()?;
        let cached = self.memo.edges.borrow_mut().take(id);
        if let Some(edge) = cached {
            #[cfg(feature = "query-profiling")]
            crate::query_profile::read_event(crate::query_profile::ReadEvent::CacheHit);
            self.memo.edges.borrow_mut().retain(id, &edge, budget);
            return Ok(edge);
        }
        let Some(edge) = self.record(Key::Edge(id), budget, codec::decode_edge_accounted)? else {
            self.memo.edges.borrow_mut().retain(id, &None, budget);
            return Ok(None);
        };
        if edge.id != id {
            return Err(error(TemporalStoreError::Corrupt));
        }
        validate_edge(&edge)?;
        if self.get_vertex(edge.from, budget)?.is_none()
            || self.get_vertex(edge.to, budget)?.is_none()
        {
            self.memo.edges.borrow_mut().retain(id, &None, budget);
            return Ok(None);
        }
        let edge = Some(edge);
        self.memo.edges.borrow_mut().retain(id, &edge, budget);
        Ok(edge)
    }
    fn scan_vertices_checked(
        &self,
        _limit: usize,
        _check: &mut impl FnMut() -> crate::Result<()>,
        _budget: &Budget,
    ) -> crate::Result<(AccountedVec<Accounted<Vertex>>, bool)> {
        Err(Error::UnsupportedGremlinArgument(
            "temporal global start requires an index".into(),
        ))
    }
    fn scan_edges_checked(
        &self,
        _limit: usize,
        _check: &mut impl FnMut() -> crate::Result<()>,
        _budget: &Budget,
    ) -> crate::Result<(AccountedVec<Accounted<Edge>>, bool)> {
        Err(Error::UnsupportedGremlinArgument(
            "temporal global start requires an index".into(),
        ))
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
        let prefix = adjacency_prefix(id, incoming);
        let head = self.view.snapshot_head;
        let view: &'a TemporalStoreView<'db> = self.view;
        // Take the cursor out before invoking a visitor. A nested scan can then
        // use a separate temporary cursor instead of panicking on a RefCell borrow.
        let mut cursor = self
            .adjacency_cursor
            .borrow_mut()
            .take()
            .unwrap_or_else(|| {
                #[cfg(feature = "query-profiling")]
                crate::query_profile::read_event(
                    crate::query_profile::ReadEvent::AdjacencyIterator,
                );
                view.snapshot.raw_iterator()
            });
        let result = (|| -> crate::Result<()> {
            cursor.seek(&prefix);
            loop {
                self.check()?;
                check()?;
                cursor.status()?;
                let Some((key, value)) = cursor.item() else {
                    break;
                };
                if !key.starts_with(&prefix) {
                    break;
                }
                #[cfg(feature = "query-profiling")]
                crate::query_profile::read_event(
                    crate::query_profile::ReadEvent::AdjacencyCandidate,
                );
                self.charge(key.len() + value.len())?;
                let _raw = budget.reserve(key.len() + value.len())?;
                if key.len() != prefix.len() + 8 || value.len() != 8 {
                    return Err(error(TemporalStoreError::Corrupt));
                }
                let first = decode_version(Some(value)).map_err(error)?;
                if first == 0 || first > head {
                    return Err(error(TemporalStoreError::Corrupt));
                }
                if first > self.view.version {
                    cursor.next();
                    continue;
                }
                let edge_id = u64::from_be_bytes(
                    key[prefix.len()..]
                        .try_into()
                        .map_err(|_| error(TemporalStoreError::Corrupt))?,
                );
                let Some(edge) = self.get_edge(edge_id, budget)? else {
                    cursor.next();
                    continue;
                };
                if (if incoming { edge.to } else { edge.from }) == id
                    && label.is_none_or(|label| edge.label == label)
                {
                    visit(edge)?;
                }
                cursor.next();
            }
            Ok(())
        })();
        *self.adjacency_cursor.borrow_mut() = Some(cursor);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GremlinRow, PathElement, Value};
    fn vertex(id: u64, from: i64, to: Option<i64>, name: &str) -> TemporalMutation {
        let mut vertex = Vertex::new(id);
        vertex.labels.push("Node".into());
        vertex
            .properties
            .insert("name".into(), Value::Text(name.into()));
        TemporalMutation::Vertex {
            valid: Interval { from, to },
            vertex,
        }
    }
    #[test]
    fn official_queries_follow_fixed_coordinates_after_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = TemporalStore::open(dir.path()).unwrap();
            store
                .commit(&[
                    vertex(1, 0, None, "original"),
                    vertex(2, 0, None, "endpoint"),
                    TemporalMutation::Edge {
                        valid: Interval { from: 0, to: None },
                        edge: Edge::new(7, 1, 2, "LINK"),
                    },
                ])
                .unwrap();
            let before = store.view(None, 7).unwrap();
            store
                .commit(&[
                    vertex(1, 5, Some(10), "corrected"),
                    vertex(1, 20, None, "future"),
                ])
                .unwrap();
            assert_eq!(
                before
                    .execute_gremlin("g.V(1).values('name')")
                    .unwrap()
                    .rows,
                vec![GremlinRow::Value(Value::Text("original".into()))]
            );
            store
                .commit(&[TemporalMutation::Absent {
                    valid: Interval {
                        from: 6,
                        to: Some(9),
                    },
                    key: Key::Vertex(2),
                }])
                .unwrap();
        }
        let store = TemporalStore::open(dir.path()).unwrap();
        for (v, t, name) in [
            (1, 7, "original"),
            (2, 7, "corrected"),
            (3, 10, "original"),
            (3, 20, "future"),
        ] {
            let view = store.view(Some(v), t).unwrap();
            assert_eq!(
                view.execute_gremlin("g.V(1).hasLabel('Node').has('name').values('name')")
                    .unwrap()
                    .rows,
                vec![GremlinRow::Value(Value::Text(name.into()))]
            );
        }
        assert_eq!(
            store
                .view(Some(2), 7)
                .unwrap()
                .execute_gremlin("g.E(7).count()")
                .unwrap()
                .rows,
            vec![GremlinRow::Count(1)]
        );
        assert_eq!(
            store
                .view(Some(3), 7)
                .unwrap()
                .execute_gremlin("g.E(7).count()")
                .unwrap()
                .rows,
            vec![GremlinRow::Count(0)]
        );
        assert_eq!(
            store
                .view(Some(3), 9)
                .unwrap()
                .execute_gremlin("g.E(7).label()")
                .unwrap()
                .rows,
            vec![GremlinRow::Value(Value::Text("LINK".into()))]
        );
        let view = store.view(Some(2), 7).unwrap();
        assert_eq!(
            view.execute_gremlin("g.V(1,1,999).path()").unwrap().rows,
            vec![
                GremlinRow::Path(vec![PathElement::Vertex(1)]),
                GremlinRow::Path(vec![PathElement::Vertex(1)])
            ]
        );
        assert_eq!(
            view.execute_gremlin("g.V(1,2).limit(1).id()").unwrap().rows,
            vec![GremlinRow::Value(Value::Integer(1))]
        );
    }
    #[test]
    fn unsupported_forms_reject_even_for_empty_graphs_or_missing_ids() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        let view = store.view(None, 0).unwrap();
        for text in ["g.V()", "g.E().limit(1)"] {
            assert!(
                matches!(
                    view.execute_gremlin(text),
                    Err(Error::UnsupportedGremlinArgument(_) | Error::UnsupportedGremlinStep(_))
                ),
                "{text}"
            );
        }
        assert!(view.execute_gremlin("not a query").is_err());
        assert_eq!(
            view.execute_gremlin("g.V(999).count()").unwrap().rows,
            vec![GremlinRow::Count(0)]
        );
    }
    #[test]
    fn corrupt_payload_reached_through_lookahead_still_errors() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store.commit(&[vertex(1, 0, Some(1), "expired")]).unwrap();
        store
            .db
            .put(
                revision_key(Key::Vertex(2), 1, 0),
                encode_assertion(
                    Interval { from: 0, to: None },
                    Some(b"invalid graph payload"),
                )
                .unwrap(),
            )
            .unwrap();
        assert!(store
            .view(None, 2)
            .unwrap()
            .execute_gremlin("g.V(1,2).hasLabel('missing').count()")
            .is_err());
    }
    #[cfg(feature = "query-profiling")]
    #[test]
    fn lookahead_can_cross_record_kinds_without_skipping_endpoint_checks() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store
            .commit(&[
                vertex(1, 0, None, "one"),
                vertex(2, 0, None, "two"),
                TemporalMutation::Edge {
                    valid: Interval { from: 0, to: None },
                    edge: Edge::new(7, 1, 2, "LINK"),
                },
            ])
            .unwrap();
        let view = store.view(None, 0).unwrap();
        let budget = Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES);
        let backend = Backend::new(&view, &budget);
        crate::query_profile::begin();
        assert!(backend.get_vertex(9, &budget).unwrap().is_none());
        assert_eq!(backend.get_edge(7, &budget).unwrap().unwrap().id, 7);
        let stats = crate::query_profile::finish();
        assert_eq!(stats.record_reads, 4);
        assert_eq!(stats.record_seeks, 3);
        assert_eq!(stats.record_lookahead_hits, 1);
        drop(backend);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn adjacency_visitor_reentry_and_error_preserve_cursor_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        let valid = Interval { from: 0, to: None };
        store
            .commit(&[
                vertex(1, 0, None, "one"),
                vertex(2, 0, None, "two"),
                vertex(3, 0, None, "three"),
                TemporalMutation::Edge {
                    valid,
                    edge: Edge::new(7, 1, 3, "LINK"),
                },
                TemporalMutation::Edge {
                    valid,
                    edge: Edge::new(8, 2, 3, "LINK"),
                },
                TemporalMutation::Edge {
                    valid,
                    edge: Edge::new(9, 1, 2, "LINK"),
                },
            ])
            .unwrap();
        let view = store.view(None, 0).unwrap();
        let budget = Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES);
        let backend = Backend::new(&view, &budget);
        let mut seen = Vec::new();
        backend
            .visit_adjacent_edges(
                1,
                false,
                None,
                &mut || Ok(()),
                &mut |edge| {
                    seen.push(edge.id);
                    if edge.id != 7 {
                        return Ok(());
                    }
                    backend.visit_adjacent_edges(
                        2,
                        false,
                        None,
                        &mut || Ok(()),
                        &mut |edge| {
                            seen.push(edge.id);
                            Ok(())
                        },
                        &budget,
                    )
                },
                &budget,
            )
            .unwrap();
        assert_eq!(seen, vec![7, 8, 9]);
        assert!(backend
            .visit_adjacent_edges(
                1,
                false,
                None,
                &mut || Ok(()),
                &mut |_| Err(Error::UnsupportedGremlinArgument(
                    "visitor rejection".into()
                )),
                &budget
            )
            .is_err());
        let mut after_error = Vec::new();
        backend
            .visit_adjacent_edges(
                2,
                false,
                None,
                &mut || Ok(()),
                &mut |edge| {
                    after_error.push(edge.id);
                    Ok(())
                },
                &budget,
            )
            .unwrap();
        assert_eq!(after_error, vec![8]);
        drop(backend);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn leading_limit_leaves_later_corrupt_record_unread() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store
            .commit(&[vertex(1, 0, None, "n"), vertex(2, 0, None, "n")])
            .unwrap();
        store
            .db
            .put(revision_key(Key::Vertex(2), 1, 0), b"invalid assertion")
            .unwrap();
        let view = store.view(None, 0).unwrap();
        assert_eq!(
            view.execute_gremlin("g.V(1,2).limit(1).id()").unwrap().rows,
            vec![GremlinRow::Value(Value::Integer(1))]
        );
        assert!(view.execute_gremlin("g.V(1,2).limit(2).id()").is_err());
    }
    #[test]
    fn optional_memo_yields_to_live_results_and_never_retains_errors() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store.commit(&[vertex(1, 0, None, "n")]).unwrap();
        let view = store.view(None, 0).unwrap();
        let budget = Budget::new(2048);
        let backend = Backend::new(&view, &budget);
        let first = backend.get_vertex(1, &budget).unwrap().unwrap();
        let bytes = first.accounted_bytes();
        let second = backend.get_vertex(1, &budget).unwrap().unwrap();
        assert_eq!(first.value, second.value);
        assert!(budget.used() >= bytes * 3); // two caller records plus memo
        drop(first);
        drop(second);
        assert!(budget.used() > 0);
        let full = budget.reserve(2048).unwrap(); // optional records are evicted
        assert_eq!(budget.used(), 2048);
        assert!(backend.get_vertex(1, &budget).is_err());
        drop(full);
        assert!(backend.get_vertex(1, &budget).unwrap().is_some());
        // Even a retained hit checks the original deadline.
        let expired = Backend {
            deadline: Instant::now(),
            ..backend
        };
        assert!(matches!(
            expired.get_vertex(1, &budget),
            Err(Error::TraversalLimitExceeded(_))
        ));
        drop(expired);
        assert_eq!(budget.used(), 0);
    }
    #[test]
    fn memory_deadline_and_cumulative_history_limits_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store.commit(&[vertex(1, 0, None, "n")]).unwrap();
        let view = store.view(None, 0).unwrap();
        let backend = Backend {
            view: &view,
            deadline: Instant::now() + crate::GREMLIN_DEADLINE,
            control: None,
            limits: crate::QueryBatchLimits::default(),
            rows: Cell::new(0),
            bytes: Cell::new(0),
            memo: Default::default(),
            record_cursor: RefCell::new(None),
            record_lookahead: Cell::new(false),
            adjacency_cursor: RefCell::new(None),
        };
        let budget = Budget::new(1);
        assert!(backend.get_vertex(1, &budget).is_err());
        assert_eq!(budget.used(), 0);
        let budget = Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES);
        backend.rows.set(MAX_READ_REVISIONS);
        assert!(
            matches!(backend.get_vertex(1,&budget),Err(Error::TemporalStore(e)) if matches!(*e,TemporalStoreError::Limit))
        );
        assert_eq!(budget.used(), 0);
        let expired = Backend {
            view: &view,
            deadline: Instant::now(),
            control: None,
            limits: crate::QueryBatchLimits::default(),
            rows: Cell::new(0),
            bytes: Cell::new(0),
            memo: Default::default(),
            record_cursor: RefCell::new(None),
            record_lookahead: Cell::new(false),
            adjacency_cursor: RefCell::new(None),
        };
        assert!(matches!(
            expired.get_vertex(999, &budget),
            Err(Error::TraversalLimitExceeded(_))
        ));
    }
    #[test]
    fn actual_identity_and_endpoint_chains_share_one_query_work_budget() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        let mut batch = WriteBatch::default();
        for id in [1, 2] {
            let payload = codec::encode_vertex(&Vertex::new(id)).unwrap();
            for v in 1..=2048 {
                let valid = Interval {
                    from: 0,
                    to: if v == 1 { None } else { Some(1) },
                };
                batch.put(
                    revision_key(Key::Vertex(id), v, 0),
                    encode_assertion(valid, Some(&payload)).unwrap(),
                );
            }
        }
        let payload = codec::encode_edge(&Edge::new(7, 1, 2, "LINK")).unwrap();
        batch.put(
            revision_key(Key::Edge(7), 2048, 0),
            encode_assertion(Interval { from: 0, to: None }, Some(&payload)).unwrap(),
        );
        batch.put(HEAD_KEY, 2048u64.to_be_bytes());
        store.db.write(batch).unwrap();
        let view = store.view(None, 2).unwrap();
        // 2,048 actual revision reads per vertex, exactly 4,096 in total.
        assert_eq!(
            view.execute_gremlin("g.V(1,2).count()").unwrap().rows,
            vec![GremlinRow::Count(2)]
        );
        // The edge itself plus both endpoints exceeds the SAME query budget.
        assert!(
            matches!(view.execute_gremlin("g.E(7).count()"), Err(Error::TemporalStore(e)) if matches!(*e, TemporalStoreError::Limit))
        );
        let budget = Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES);
        let backend = Backend {
            view: &view,
            deadline: Instant::now() + crate::GREMLIN_DEADLINE,
            control: None,
            limits: crate::QueryBatchLimits::default(),
            rows: Cell::new(0),
            bytes: Cell::new(0),
            memo: Default::default(),
            record_cursor: RefCell::new(None),
            record_lookahead: Cell::new(false),
            adjacency_cursor: RefCell::new(None),
        };
        let result = GremlinQuery::parse("g.V(1,2).count()")
            .unwrap()
            .execute_on_read_budget(&backend, &|| backend.check(), &budget)
            .unwrap();
        assert_eq!(result.rows, vec![GremlinRow::Count(2)]);
        drop(backend);
        assert_eq!(budget.used(), 0);
    }

    #[test]
    fn canonical_corruption_cannot_be_filtered_into_empty_success() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store.commit(&[vertex(1, 0, None, "n")]).unwrap();
        store
            .db
            .put(
                revision_key(Key::Vertex(1), 1, 0),
                encode_assertion(
                    Interval { from: 0, to: None },
                    Some(&codec::encode_vertex(&Vertex::new(2)).unwrap()),
                )
                .unwrap(),
            )
            .unwrap();
        let view = store.view(None, 0).unwrap();
        assert!(
            matches!(view.execute_gremlin("g.V(1).hasLabel('NoMatch').count()"),Err(Error::TemporalStore(e)) if matches!(*e,TemporalStoreError::Corrupt))
        );
    }
}

#[cfg(test)]
mod adjacency_tests {
    use super::*;
    use crate::GremlinRow;
    fn valid(from: i64, to: Option<i64>) -> Interval {
        Interval { from, to }
    }
    fn ids(view: &TemporalStoreView<'_>, id: u64, direction: &str) -> Vec<u64> {
        view.execute_gremlin(&format!("g.V({id}).{direction}().id()"))
            .unwrap()
            .rows
            .into_iter()
            .map(|row| match row {
                GremlinRow::Value(crate::Value::Integer(id)) => id as u64,
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }
    #[test]
    fn indexed_hops_reopen_corrections_deletions_and_fixed_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = TemporalStore::open(dir.path()).unwrap();
            store
                .commit(
                    &(1..=4)
                        .map(|id| TemporalMutation::Vertex {
                            valid: valid(i64::MIN, None),
                            vertex: Vertex::new(id),
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            store
                .commit(&[TemporalMutation::Edge {
                    valid: valid(0, None),
                    edge: Edge::new(10, 1, 2, "A"),
                }])
                .unwrap();
            let old = store.view(None, 5).unwrap();
            store
                .commit(&[TemporalMutation::Edge {
                    valid: valid(5, Some(10)),
                    edge: Edge::new(10, 3, 4, "B"),
                }])
                .unwrap();
            assert_eq!(ids(&old, 1, "out"), vec![2]);
            assert!(ids(&old, 3, "out").is_empty());
            store
                .commit(&[
                    TemporalMutation::Absent {
                        valid: valid(7, Some(8)),
                        key: Key::Vertex(4),
                    },
                    TemporalMutation::Absent {
                        valid: valid(8, Some(9)),
                        key: Key::Edge(10),
                    },
                ])
                .unwrap();
        }
        let store = TemporalStore::open(dir.path()).unwrap();
        for t in [0, 4, 10, i64::MAX] {
            let view = store.view(None, t).unwrap();
            assert_eq!(ids(&view, 1, "out"), vec![2]);
            assert_eq!(ids(&view, 2, "in"), vec![1]);
            assert!(ids(&view, 3, "both").is_empty());
        }
        for t in [5, 6, 9] {
            let view = store.view(None, t).unwrap();
            assert!(ids(&view, 1, "out").is_empty());
            assert_eq!(ids(&view, 3, "out"), vec![4]);
            assert_eq!(ids(&view, 4, "both"), vec![3]);
            assert_eq!(
                view.execute_gremlin("g.V(3).out('B').in('B').path()")
                    .unwrap()
                    .rows
                    .len(),
                1
            );
            assert!(view
                .execute_gremlin("g.V(3).out('A')")
                .unwrap()
                .rows
                .is_empty());
        }
        for t in [7, 8] {
            assert!(ids(&store.view(None, t).unwrap(), 3, "out").is_empty());
        }
        assert!(ids(&store.view(None, -1).unwrap(), 1, "out").is_empty());
    }
    #[test]
    fn random_dense_graph_oracle_survives_reopen() {
        // Independent dense time-cell state, not the production history selector.
        let dir = tempfile::tempdir().unwrap();
        let mut vertices = vec![vec![true; 6]; 9];
        let mut edges = vec![vec![None; 12]; 9];
        let mut oracle = Vec::new();
        let mut seed = 728193u64;
        {
            let store = TemporalStore::open(dir.path()).unwrap();
            store
                .commit(
                    &(0..6)
                        .map(|id| TemporalMutation::Vertex {
                            valid: valid(0, None),
                            vertex: Vertex::new(id),
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap();
            oracle.push((vertices.clone(), edges.clone()));
            for n in 0..48 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let a = ((seed >> 8) % 8) as usize;
                let b = a + 1 + ((seed >> 16) as usize % (9 - a));
                let id = ((seed >> 24) % 12) as usize;
                let from = (seed >> 32) % 6;
                let to = (seed >> 40) % 6;
                let mutation = if n % 7 == 0 {
                    for row in &mut vertices[a..b] {
                        row[id % 6] = false;
                    }
                    TemporalMutation::Absent {
                        valid: valid(a as i64, Some(b as i64)),
                        key: Key::Vertex((id % 6) as u64),
                    }
                } else if n % 5 == 0 {
                    for row in &mut edges[a..b] {
                        row[id] = None;
                    }
                    TemporalMutation::Absent {
                        valid: valid(a as i64, Some(b as i64)),
                        key: Key::Edge(id as u64),
                    }
                } else {
                    for row in &mut edges[a..b] {
                        row[id] = Some((from, to));
                    }
                    TemporalMutation::Edge {
                        valid: valid(a as i64, Some(b as i64)),
                        edge: Edge::new(id as u64, from, to, "R"),
                    }
                };
                store.commit(&[mutation]).unwrap();
                oracle.push((vertices.clone(), edges.clone()));
            }
        }
        let store = TemporalStore::open(dir.path()).unwrap();
        for (v, (vertices, edges)) in oracle.iter().enumerate() {
            for t in 0..9 {
                let view = store.view(Some(v as u64 + 1), t as i64).unwrap();
                for id in 0..6 {
                    for direction in ["out", "in", "both"] {
                        let mut expected = Vec::new();
                        if vertices[t][id] {
                            for pair in edges[t].iter().flatten() {
                                if !vertices[t][pair.0 as usize] || !vertices[t][pair.1 as usize] {
                                    continue;
                                }
                                if direction != "in" && pair.0 == id as u64 {
                                    expected.push(pair.1);
                                } else if direction != "out" && pair.1 == id as u64 {
                                    expected.push(pair.0);
                                }
                            }
                        }
                        assert_eq!(
                            ids(&view, id as u64, direction),
                            expected,
                            "V={} T={t} id={id} direction={direction}",
                            v + 1
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod adjacency_limits {
    use super::*;
    #[test]
    fn golden_adjacency_and_old_format_refusal() {
        let mut expected = b"graph/temporal/v2/adjacency/".to_vec();
        expected.extend([1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 9]);
        assert_eq!(adjacency_key(7, true, 9), expected);
        let dir = tempfile::tempdir().unwrap();
        {
            let mut options = Options::default();
            options.create_if_missing(true);
            let db = DB::open(&options, dir.path()).unwrap();
            db.put(b"graph/temporal/v1/format", [1]).unwrap();
            db.put(b"graph/temporal/v1/head", 0u64.to_be_bytes())
                .unwrap();
        }
        assert!(matches!(
            TemporalStore::open(dir.path()),
            Err(TemporalStoreError::IncompatibleStore)
        ));
        let db = DB::open(&Options::default(), dir.path()).unwrap();
        assert!(db.get(FORMAT_KEY).unwrap().is_none());
        assert_eq!(db.iterator(IteratorMode::Start).count(), 2);
    }
    #[test]
    fn adjacency_corruption_and_candidate_work_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store
            .commit(&[TemporalMutation::Vertex {
                valid: Interval { from: 0, to: None },
                vertex: Vertex::new(1),
            }])
            .unwrap();
        let key = adjacency_key(1, false, 7);
        for bytes in [
            vec![],
            0u64.to_be_bytes().to_vec(),
            2u64.to_be_bytes().to_vec(),
        ] {
            store.db.put(&key, bytes).unwrap();
            assert!(
                matches!(store.view(None,0).unwrap().execute_gremlin("g.V(1).out()"),Err(Error::TemporalStore(e)) if matches!(*e,TemporalStoreError::Corrupt))
            );
        }
        store.db.put(&key, 1u64.to_be_bytes()).unwrap();
        let view = store.view(None, 0).unwrap();
        let backend = Backend {
            view: &view,
            deadline: Instant::now() + crate::GREMLIN_DEADLINE,
            control: None,
            limits: crate::QueryBatchLimits::default(),
            rows: Cell::new(MAX_READ_REVISIONS),
            bytes: Cell::new(0),
            memo: Default::default(),
            record_cursor: RefCell::new(None),
            record_lookahead: Cell::new(false),
            adjacency_cursor: RefCell::new(None),
        };
        let result = backend.visit_adjacent_edges(
            1,
            false,
            None,
            &mut || Ok(()),
            &mut |_| panic!("must not visit"),
            &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
        );
        assert!(
            matches!(result,Err(Error::TemporalStore(e)) if matches!(*e,TemporalStoreError::Limit))
        );
    }
}

#[cfg(test)]
mod adjacency_atomicity {
    use super::*;
    #[test]
    fn disjoint_batch_candidates_and_failed_batch_are_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        let interval = Interval { from: 0, to: None };
        store
            .commit(
                &(1..=3)
                    .map(|id| TemporalMutation::Vertex {
                        valid: interval,
                        vertex: Vertex::new(id),
                    })
                    .collect::<Vec<_>>(),
            )
            .unwrap();
        store
            .commit(&[
                TemporalMutation::Edge {
                    valid: Interval {
                        from: 0,
                        to: Some(5),
                    },
                    edge: Edge::new(1, 1, 2, "R"),
                },
                TemporalMutation::Edge {
                    valid: Interval { from: 5, to: None },
                    edge: Edge::new(1, 1, 3, "R"),
                },
            ])
            .unwrap();
        assert_eq!(
            store.db.get(adjacency_key(1, false, 1)).unwrap().unwrap(),
            2u64.to_be_bytes()
        );
        for (t, target) in [(0, 2), (5, 3)] {
            assert_eq!(
                store
                    .view(None, t)
                    .unwrap()
                    .execute_gremlin("g.V(1).out().id()")
                    .unwrap()
                    .rows,
                vec![crate::GremlinRow::Value(crate::Value::Integer(target))]
            );
        }
        assert!(store
            .commit(&[
                TemporalMutation::Edge {
                    valid: interval,
                    edge: Edge::new(9, 3, 1, "R")
                },
                TemporalMutation::Absent {
                    valid: Interval {
                        from: 1,
                        to: Some(1)
                    },
                    key: Key::Edge(8)
                },
            ])
            .is_err());
        assert_eq!(store.view(None, 0).unwrap().version(), 2);
        assert!(store.db.get(adjacency_key(3, false, 9)).unwrap().is_none());
    }
    #[test]
    #[ignore = "manual timing, not a performance assertion"]
    fn temporal_adjacency_timing() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        let interval = Interval { from: 0, to: None };
        let start = Instant::now();
        let mut mutations = (1..=33)
            .map(|id| TemporalMutation::Vertex {
                valid: interval,
                vertex: Vertex::new(id),
            })
            .collect::<Vec<_>>();
        mutations.extend((2..=33).map(|id| TemporalMutation::Edge {
            valid: interval,
            edge: Edge::new(id, 1, id, "R"),
        }));
        store.commit(&mutations).unwrap();
        eprintln!("65-record synced batch: {:?}", start.elapsed());
        let view = store.view(None, 0).unwrap();
        let query = GremlinQuery::parse("g.V(1).out().count()").unwrap();
        let mut timings = Vec::new();
        for _ in 0..101 {
            let backend = Backend {
                view: &view,
                deadline: Instant::now() + crate::GREMLIN_DEADLINE,
                control: None,
                limits: crate::QueryBatchLimits::default(),
                rows: Cell::new(0),
                bytes: Cell::new(0),
                memo: Default::default(),
                record_cursor: RefCell::new(None),
                record_lookahead: Cell::new(false),
                adjacency_cursor: RefCell::new(None),
            };
            let start = Instant::now();
            assert_eq!(
                query
                    .execute_on_read_budget(
                        &backend,
                        &|| backend.check(),
                        &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES)
                    )
                    .unwrap()
                    .rows,
                vec![crate::GremlinRow::Count(32)]
            );
            timings.push(start.elapsed().as_micros());
        }
        timings.remove(0);
        timings.sort();
        eprintln!(
            "warm 32-edge out/count, parsed once, 100 iterations debug: p50={}us p95={}us max={}us",
            timings[49], timings[94], timings[99]
        );
    }
}
