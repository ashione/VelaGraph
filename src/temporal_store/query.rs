//! Explicit-ID Gremlin on durable temporal point reads. No global scan fallback.
use std::cell::Cell;
use std::time::Instant;

use super::*;
use crate::gremlin::QueryReadBackend;
use crate::query_memory::{Accounted, AccountedVec, Budget};
use crate::{Error, GremlinQuery, GremlinResult, GremlinStart, GremlinStep};

impl TemporalStoreView<'_> {
    /// Execute the existing official Gremlin point-query subset at this V/T.
    /// Global starts and adjacency reject before any reads, even on empty input.
    pub fn execute_gremlin(&self, text: &str) -> crate::Result<GremlinResult> {
        let query = GremlinQuery::parse(text)?;
        match query.start() {
            GremlinStart::Vertices(ids) | GremlinStart::Edges(ids) if ids.is_empty() => {
                return Err(Error::UnsupportedGremlinArgument(
                    "temporal global start requires an index".into(),
                ))
            }
            _ => {}
        }
        for step in query.steps() {
            if matches!(
                step,
                GremlinStep::Out(_) | GremlinStep::In(_) | GremlinStep::Both(_)
            ) {
                return Err(Error::UnsupportedGremlinStep(
                    "temporal adjacency requires an index".into(),
                ));
            }
        }
        let backend = Backend {
            view: self,
            deadline: Instant::now() + crate::GREMLIN_DEADLINE,
            rows: Cell::new(0),
            bytes: Cell::new(0),
        };
        query.execute_on_read_budget(
            &backend,
            &|| backend.check(),
            &Budget::new(crate::MAX_GREMLIN_ACCOUNTED_BYTES),
        )
    }
}
struct Backend<'a, 'db> {
    view: &'a TemporalStoreView<'db>,
    deadline: Instant,
    rows: Cell<usize>,
    bytes: Cell<usize>,
}
fn error(error: TemporalStoreError) -> Error {
    Error::TemporalStore(Box::new(error))
}
impl Backend<'_, '_> {
    fn check(&self) -> crate::Result<()> {
        if Instant::now() >= self.deadline {
            return Err(Error::TraversalLimitExceeded(
                "temporal query deadline".into(),
            ));
        }
        Ok(())
    }
    fn record<T>(
        &self,
        identity: Key,
        budget: &Budget,
        decode: fn(&[u8], &Budget) -> crate::Result<Accounted<T>>,
    ) -> crate::Result<Option<Accounted<T>>> {
        self.check()?;
        let prefix = identity_prefix(identity);
        let start = revision_key(identity, self.view.version, 0);
        for row in self
            .view
            .snapshot
            .iterator(IteratorMode::From(&start, Direction::Forward))
        {
            self.check()?;
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            let rows = self
                .rows
                .get()
                .checked_add(1)
                .ok_or_else(|| error(TemporalStoreError::Limit))?;
            let bytes = self
                .bytes
                .get()
                .checked_add(key.len() + value.len())
                .ok_or_else(|| error(TemporalStoreError::Limit))?;
            if rows > MAX_READ_REVISIONS || bytes > MAX_READ_BYTES {
                return Err(error(TemporalStoreError::Limit));
            }
            self.rows.set(rows);
            self.bytes.set(bytes);
            let _raw = budget.reserve(key.len() + value.len())?;
            let (valid, payload) =
                decode_revision(&key, &value, self.view.version).map_err(error)?;
            if valid.from <= self.view.valid_time
                && valid.to.is_none_or(|to| self.view.valid_time < to)
            {
                self.check()?;
                return payload.map(|bytes| decode(bytes, budget)).transpose();
            }
        }
        Ok(None)
    }
}
impl QueryReadBackend for Backend<'_, '_> {
    fn get_vertex(&self, id: u64, budget: &Budget) -> crate::Result<Option<Accounted<Vertex>>> {
        let Some(vertex) = self.record(Key::Vertex(id), budget, codec::decode_vertex_accounted)?
        else {
            return Ok(None);
        };
        if vertex.id != id {
            return Err(error(TemporalStoreError::Corrupt));
        }
        validate_vertex(&vertex)?;
        Ok(Some(vertex))
    }
    fn get_edge(&self, id: u64, budget: &Budget) -> crate::Result<Option<Accounted<Edge>>> {
        let Some(edge) = self.record(Key::Edge(id), budget, codec::decode_edge_accounted)? else {
            return Ok(None);
        };
        if edge.id != id {
            return Err(error(TemporalStoreError::Corrupt));
        }
        validate_edge(&edge)?;
        if self.get_vertex(edge.from, budget)?.is_none()
            || self.get_vertex(edge.to, budget)?.is_none()
        {
            return Ok(None);
        }
        Ok(Some(edge))
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
        _id: u64,
        _incoming: bool,
        _label: Option<&str>,
        _check: &mut impl FnMut() -> crate::Result<()>,
        _visit: &mut impl FnMut(Accounted<Edge>) -> crate::Result<()>,
        _budget: &Budget,
    ) -> crate::Result<()> {
        Err(Error::UnsupportedGremlinStep(
            "temporal adjacency requires an index".into(),
        ))
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
        for text in [
            "g.V()",
            "g.E().limit(1)",
            "g.V(999).out('LINK')",
            "g.V(999).in()",
            "g.V(999).both()",
        ] {
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
    fn memory_deadline_and_cumulative_history_limits_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let store = TemporalStore::open(dir.path()).unwrap();
        store.commit(&[vertex(1, 0, None, "n")]).unwrap();
        let view = store.view(None, 0).unwrap();
        let backend = Backend {
            view: &view,
            deadline: Instant::now() + crate::GREMLIN_DEADLINE,
            rows: Cell::new(0),
            bytes: Cell::new(0),
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
            rows: Cell::new(0),
            bytes: Cell::new(0),
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
            rows: Cell::new(0),
            bytes: Cell::new(0),
        };
        let result = GremlinQuery::parse("g.V(1,2).count()")
            .unwrap()
            .execute_on_read_budget(&backend, &|| backend.check(), &budget)
            .unwrap();
        assert_eq!(result.rows, vec![GremlinRow::Count(2)]);
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
