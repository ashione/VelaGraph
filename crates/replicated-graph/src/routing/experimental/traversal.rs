//! Bounded, explicitly incomplete observations using the existing Gremlin parser.
use super::*;
use rocksdb_sql_table::{GremlinQuery, GremlinStart, GremlinStep, Value};

/// The value is an observation, never a certified complete traversal result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservedPathElement {
    Vertex(VertexRef),
    Edge(EdgeRef),
}
/// Ordered row in an explicitly unverified observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ObservedRow {
    Vertex(VertexRef),
    Path(Vec<ObservedPathElement>),
    Count(u64),
}
/// Results plus the frozen snapshot vector, never a global consistency proof.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedTraversal {
    pub rows: Vec<ObservedRow>,
    pub completeness: AdjacencyCompleteness,
    pub metadata_sequence: u64,
    pub snapshots: Vec<ShardReadStamp>,
}
/// Cumulative logical work admission; charges are never refunded during a query.
/// Snapshot storage, parser allocations and bounded primitive-read scratch are
/// excluded. This is deliberately not a heap/RSS bound.
struct Work {
    bytes: usize,
    visits: usize,
}
impl Work {
    fn charge(&mut self, bytes: usize) -> ExperimentalResult<()> {
        self.bytes = self.bytes.saturating_add(bytes);
        if self.bytes > 64 * 1024 * 1024 {
            return Err(limit("v2 traversal cumulative logical bytes exceed 64 MiB"));
        }
        Ok(())
    }
    fn visit(&mut self) -> ExperimentalResult<()> {
        self.visits += 1;
        if self.visits > 10_000 {
            return Err(limit("v2 traversal exceeds 10000 work units"));
        }
        Ok(())
    }
    fn vertex(&mut self, v: &Vertex) -> ExperimentalResult<()> {
        self.charge(128 + v.labels.iter().map(String::len).sum::<usize>())?;
        for (k, v) in &v.properties {
            self.charge(
                128 + k.len()
                    + match v {
                        Value::Text(s) => s.len(),
                        _ => 8,
                    },
            )?;
        }
        Ok(())
    }
}
fn limit(message: &str) -> ExperimentalRouteError {
    ExperimentalRouteError::Query(rocksdb_sql_table::Error::TraversalLimitExceeded(
        message.into(),
    ))
}
struct Row {
    vertex: VertexRef,
    path: Vec<ObservedPathElement>,
}
fn push(rows: &mut Vec<Row>, row: Row, work: &mut Work) -> ExperimentalResult<()> {
    if rows.len() >= 10_000 {
        return Err(limit("v2 traversal row cap exceeded"));
    }
    work.charge(128 + row.path.len() * std::mem::size_of::<ObservedPathElement>())?;
    rows.push(row);
    Ok(())
}
impl ExperimentalReadSession {
    /// Executes against this session's original catalog, shard views and deadline.
    /// Local start IDs belong to `start_partition`; subsequent hops are qualified.
    /// Missing explicit starts produce no row. Empty V() scans every frozen
    /// catalog partition in (local ID, partition ID) order, ignoring start_partition.
    pub fn observed_gremlin(
        &self,
        start_partition: u32,
        text: &str,
    ) -> ExperimentalResult<ObservedTraversal> {
        self.op.check(None)?;
        let query = GremlinQuery::parse(text).map_err(ExperimentalRouteError::Query)?;
        self.op.check(None)?;
        let GremlinStart::Vertices(ids) = query.start() else {
            return Err(ExperimentalRouteError::Unsupported("v2 requires a V start"));
        };
        for step in query.steps() {
            if !matches!(
                step,
                GremlinStep::Out(_)
                    | GremlinStep::In(_)
                    | GremlinStep::Both(_)
                    | GremlinStep::HasLabel(_)
                    | GremlinStep::Has { .. }
                    | GremlinStep::Path
                    | GremlinStep::Limit(_)
                    | GremlinStep::Count
            ) {
                return Err(ExperimentalRouteError::Unsupported(
                    "v2 step outside observed traversal subset",
                ));
            }
        }
        // Explicit IDs retain partition validation even when all are absent.
        if !ids.is_empty() {
            self.store(start_partition)?;
        }
        let mut work = Work {
            bytes: 4096,
            visits: 0,
        };
        let mut rows = Vec::new();
        for id in ids {
            self.op.check(None)?;
            if matches!(query.steps().first(), Some(GremlinStep::Limit(n)) if rows.len() >= *n) {
                break;
            }
            work.visit()?;
            let r = VertexRef {
                partition: start_partition,
                id: *id,
            };
            if let Some(v) = self.get_vertex(r)? {
                work.vertex(&v)?;
                push(
                    &mut rows,
                    Row {
                        vertex: r,
                        path: vec![ObservedPathElement::Vertex(r)],
                    },
                    &mut work,
                )?;
            }
        }
        if ids.is_empty() {
            let leading = match query.steps().first() {
                Some(GremlinStep::Limit(n)) => Some(*n),
                _ => None,
            };
            for partition in self.placements.keys() {
                let store = self.store(*partition)?;
                let mut after = None;
                let mut acquired = 0usize;
                loop {
                    self.op.check(None)?;
                    let remaining = leading.map_or(128, |n| n.saturating_sub(acquired).min(128));
                    if remaining == 0 {
                        break;
                    }
                    // One bounded decoded page may overlap already admitted rows.
                    // Raw page and decoded payload are charged cumulatively before retention.
                    let page =
                        store.vertex_page(after, ScanLimit::new(remaining, 1024 * 1024 + 64)?)?;
                    work.charge(page.raw_bytes + page.vertices.len() * 128)?;
                    for vertex in page.vertices {
                        self.op.check(None)?;
                        work.visit()?;
                        work.vertex(&vertex)?;
                        let r = VertexRef {
                            partition: *partition,
                            id: vertex.id,
                        };
                        push(
                            &mut rows,
                            Row {
                                vertex: r,
                                path: vec![ObservedPathElement::Vertex(r)],
                            },
                            &mut work,
                        )?;
                        acquired += 1;
                    }
                    if let Some(n) = leading {
                        rows.sort_unstable_by_key(|row| (row.vertex.id, row.vertex.partition));
                        rows.truncate(n);
                    }
                    after = page.next_after;
                    if after.is_none() {
                        break;
                    }
                }
            }
            rows.sort_unstable_by_key(|row| (row.vertex.id, row.vertex.partition));
        }
        for step in query.steps() {
            self.op.check(None)?;
            match step {
                GremlinStep::Limit(n) => rows.truncate(*n),
                GremlinStep::HasLabel(_) | GremlinStep::Has { .. } => {
                    let mut filtered = Vec::new();
                    for row in rows {
                        self.op.check(None)?;
                        work.visit()?;
                        let v = self
                            .get_vertex(row.vertex)?
                            .ok_or(ExperimentalError::MissingEndpoint(row.vertex))?;
                        work.vertex(&v)?;
                        let matches = match step {
                            GremlinStep::HasLabel(labels) => {
                                v.labels.iter().any(|l| labels.contains(l))
                            }
                            GremlinStep::Has { key, value } => match value {
                                Some(value) => v.properties.get(key) == Some(value),
                                None => v.properties.contains_key(key),
                            },
                            _ => unreachable!("filter step"),
                        };
                        if matches {
                            push(&mut filtered, row, &mut work)?;
                        }
                    }
                    rows = filtered;
                }
                GremlinStep::Out(label) | GremlinStep::In(label) | GremlinStep::Both(label) => {
                    let mut next = Vec::new();
                    for row in rows {
                        self.op.check(None)?;
                        let dirs: &[Direction] = match step {
                            GremlinStep::Out(_) => &[Direction::Out],
                            GremlinStep::In(_) => &[Direction::In],
                            _ => &[Direction::Out, Direction::In],
                        };
                        let mut ordered = BTreeMap::new();
                        for direction in dirs {
                            work.visit()?;
                            let observed = self.observed_adjacency(row.vertex, *direction)?;
                            for edge in observed.edges {
                                self.op.check(None)?;
                                work.visit()?;
                                work.charge(256 + edge.label.len())?;
                                for (key, value) in &edge.properties {
                                    work.charge(
                                        128 + key.len()
                                            + match value {
                                                Value::Text(s) => s.len(),
                                                _ => 8,
                                            },
                                    )?;
                                }
                                if label.as_ref().is_some_and(|l| l != &edge.label) {
                                    continue;
                                }
                                let target = if *direction == Direction::Out {
                                    edge.to
                                } else {
                                    edge.from
                                };
                                // Deduplicate only the same qualified edge (self-loop), not targets.
                                ordered.insert((edge.id, edge.from.partition), target);
                            }
                        }
                        for ((id, partition), target) in ordered {
                            self.op.check(None)?;
                            let mut path = row.path.clone();
                            path.push(ObservedPathElement::Edge(EdgeRef { partition, id }));
                            path.push(ObservedPathElement::Vertex(target));
                            push(
                                &mut next,
                                Row {
                                    vertex: target,
                                    path,
                                },
                                &mut work,
                            )?;
                        }
                    }
                    rows = next;
                }
                GremlinStep::Path | GremlinStep::Count => {}
                _ => unreachable!("validated supported subset"),
            }
        }
        let count = matches!(query.steps().last(), Some(GremlinStep::Count));
        let path = matches!(query.steps().last(), Some(GremlinStep::Path));
        if !count
            && !query
                .steps()
                .iter()
                .any(|s| matches!(s, GremlinStep::Limit(_)))
        {
            rows.truncate(100);
        }
        work.charge(rows.len() * 128 + self.snapshots.len() * 64)?;
        let output = if count {
            vec![ObservedRow::Count(rows.len() as u64)]
        } else {
            rows.into_iter()
                .map(|r| {
                    if path {
                        ObservedRow::Path(r.path)
                    } else {
                        ObservedRow::Vertex(r.vertex)
                    }
                })
                .collect()
        };
        self.op.check(None)?;
        Ok(ObservedTraversal {
            rows: output,
            completeness: AdjacencyCompleteness::Unverified,
            metadata_sequence: self.metadata_sequence,
            snapshots: self.snapshots.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cumulative_budget_is_shared_and_never_refunded() {
        let mut work = Work {
            bytes: 0,
            visits: 0,
        };
        work.charge(64 * 1024 * 1024).unwrap();
        assert!(matches!(
            work.charge(1),
            Err(ExperimentalRouteError::Query(
                rocksdb_sql_table::Error::TraversalLimitExceeded(_)
            ))
        ));
        let mut work = Work {
            bytes: 0,
            visits: 9999,
        };
        work.visit().unwrap();
        assert!(work.visit().is_err());
    }
    #[tokio::test]
    async fn empty_global_catalog_is_an_unverified_empty_observation() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let session = ExperimentalReadSession {
            metadata: Placement {
                shard: 1,
                group: 1,
                epoch: 1,
                replicas: vec![1],
            },
            metadata_sequence: 42,
            snapshots: vec![],
            placements: BTreeMap::new(),
            nodes: BTreeMap::new(),
            op: Arc::new(Operation {
                end: Instant::now() + Duration::from_secs(60),
                _permit: semaphore.acquire_owned().await.unwrap(),
            }),
        };
        let output = session
            .observed_gremlin(999, "g.V().has('missing').count()")
            .unwrap();
        assert_eq!(output.rows, vec![ObservedRow::Count(0)]);
        assert_eq!(output.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(output.metadata_sequence, 42);
        assert!(output.snapshots.is_empty());
        assert!(session
            .observed_gremlin(999, "g.V().path()")
            .unwrap()
            .rows
            .is_empty());
        assert!(matches!(
            session.observed_gremlin(999, "g.V(1)"),
            Err(ExperimentalRouteError::Route(RouteError::MissingPartition(
                999
            )))
        ));
        assert!(matches!(
            session.observed_gremlin(999, "g.V().values('x')"),
            Err(ExperimentalRouteError::Unsupported(_))
        ));
    }
    #[tokio::test]
    async fn expired_session_does_not_restart_deadline_even_for_empty_count() {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(1));
        let session = ExperimentalReadSession {
            metadata: Placement {
                shard: 1,
                group: 1,
                epoch: 1,
                replicas: vec![1],
            },
            metadata_sequence: 0,
            snapshots: vec![],
            placements: BTreeMap::new(),
            nodes: BTreeMap::new(),
            op: Arc::new(Operation {
                end: Instant::now() - Duration::from_secs(1),
                _permit: semaphore.acquire_owned().await.unwrap(),
            }),
        };
        assert!(matches!(
            session.observed_gremlin(1, "g.V(7).count()"),
            Err(ExperimentalRouteError::Route(RouteError::Storage(
                StorageNodeError::Timeout { .. }
            )))
        ));
    }
}
