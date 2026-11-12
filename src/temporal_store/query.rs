//! Indexed temporal Gremlin. No global scan fallback.
use std::cell::Cell;
use std::time::Instant;

use super::*;
use crate::gremlin::QueryReadBackend;
use crate::query_memory::{Accounted, AccountedVec, Budget};
use crate::{Error, GremlinQuery, GremlinResult, GremlinStart};

impl TemporalStoreView<'_> {
    /// Execute official Gremlin point queries and out/in/both at this V/T.
    /// Global starts reject before reads; adjacency uses a persistent candidate index.
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
    fn charge(&self, bytes: usize) -> crate::Result<()> {
        let rows = self
            .rows
            .get()
            .checked_add(1)
            .ok_or_else(|| error(TemporalStoreError::Limit))?;
        let bytes = self
            .bytes
            .get()
            .checked_add(bytes)
            .ok_or_else(|| error(TemporalStoreError::Limit))?;
        if rows > MAX_READ_REVISIONS || bytes > MAX_READ_BYTES {
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
            self.charge(key.len() + value.len())?;
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
        id: u64,
        incoming: bool,
        label: Option<&str>,
        check: &mut impl FnMut() -> crate::Result<()>,
        visit: &mut impl FnMut(Accounted<Edge>) -> crate::Result<()>,
        budget: &Budget,
    ) -> crate::Result<()> {
        let prefix = adjacency_prefix(id, incoming);
        let head = decode_version(self.view.snapshot.get(HEAD_KEY)?.as_deref()).map_err(error)?;
        for row in self
            .view
            .snapshot
            .iterator(IteratorMode::From(&prefix, Direction::Forward))
        {
            self.check()?;
            check()?;
            let (key, value) = row?;
            if !key.starts_with(&prefix) {
                break;
            }
            self.charge(key.len() + value.len())?;
            let _raw = budget.reserve(key.len() + value.len())?;
            if key.len() != prefix.len() + 8 || value.len() != 8 {
                return Err(error(TemporalStoreError::Corrupt));
            }
            let first = decode_version(Some(&value)).map_err(error)?;
            if first == 0 || first > head {
                return Err(error(TemporalStoreError::Corrupt));
            }
            if first > self.view.version {
                continue;
            }
            let edge_id = u64::from_be_bytes(
                key[prefix.len()..]
                    .try_into()
                    .map_err(|_| error(TemporalStoreError::Corrupt))?,
            );
            let Some(edge) = self.get_edge(edge_id, budget)? else {
                continue;
            };
            if (if incoming { edge.to } else { edge.from }) == id
                && label.is_none_or(|label| edge.label == label)
            {
                visit(edge)?;
            }
        }
        Ok(())
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
            rows: Cell::new(MAX_READ_REVISIONS),
            bytes: Cell::new(0),
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
                rows: Cell::new(0),
                bytes: Cell::new(0),
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
