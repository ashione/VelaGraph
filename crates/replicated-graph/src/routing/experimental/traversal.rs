//! Bounded, explicitly incomplete observations using the existing Gremlin parser.
use super::*;
mod edges;
pub(crate) use edges::{validate_steps as validate_traversal, TraversalRead};
pub use edges::{ObservedElementRef, ObservedRowV3, ObservedTraversalV3};
use velagraph::{GremlinQuery, GremlinStart, GremlinStep, Value};

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
/// Version 2 observation rows add scalar projections without weakening the
/// original `ObservedRow: Eq` contract. Value equality is `Value::PartialEq`:
/// signed zeros compare equal; caller-constructed NaN is not self-equal.
/// Stored graph values reject NaN and infinity at write and decode boundaries.
#[derive(Clone, Debug, PartialEq)]
pub enum ObservedRowV2 {
    Vertex(VertexRef),
    Path(Vec<ObservedPathElement>),
    Count(u64),
    /// The qualified vertex that supplied this value. Repeated traversers remain
    /// repeated rows even when their source and value are identical.
    Value {
        source: VertexRef,
        value: Value,
    },
}
/// Additive projection-capable result. This is still an unverified observation,
/// not a globally consistent or complete result. No storage format is changed.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservedTraversalV2 {
    pub rows: Vec<ObservedRowV2>,
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
    ExperimentalRouteError::Query(velagraph::Error::TraversalLimitExceeded(message.into()))
}
struct Row {
    vertex: VertexRef,
    path: Vec<ObservedPathElement>,
    value: Option<Value>,
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
        let result = self.execute_observed(start_partition, text, false)?;
        let output = ObservedTraversal {
            rows: result
                .rows
                .into_iter()
                .map(|row| match row {
                    ObservedRowV2::Vertex(v) => ObservedRow::Vertex(v),
                    ObservedRowV2::Path(p) => ObservedRow::Path(p),
                    ObservedRowV2::Count(n) => ObservedRow::Count(n),
                    ObservedRowV2::Value { .. } => {
                        unreachable!("legacy entrypoint rejects projections")
                    }
                })
                .collect(),
            completeness: result.completeness,
            metadata_sequence: result.metadata_sequence,
            snapshots: result.snapshots,
        };
        self.op.check(None)?;
        Ok(output)
    }

    /// Projection-capable version of `observed_gremlin`, sharing the same frozen
    /// session and budgets. `values(key)` drops missing properties but retains
    /// explicit Null; `label()` returns the first stored label or empty text.
    /// Only limit/count may follow a scalar projection, including on empty input.
    pub fn observed_gremlin_v2(
        &self,
        start_partition: u32,
        text: &str,
    ) -> ExperimentalResult<ObservedTraversalV2> {
        self.execute_observed(start_partition, text, true)
    }

    fn execute_observed(
        &self,
        start_partition: u32,
        text: &str,
        projections: bool,
    ) -> ExperimentalResult<ObservedTraversalV2> {
        self.op.check(None)?;
        let query = GremlinQuery::parse(text).map_err(ExperimentalRouteError::Query)?;
        self.op.check(None)?;
        let GremlinStart::Vertices(ids) = query.start() else {
            return Err(ExperimentalRouteError::Unsupported("v2 requires a V start"));
        };
        let mut scalar = false;
        for step in query.steps() {
            if scalar && !matches!(step, GremlinStep::Limit(_) | GremlinStep::Count) {
                return Err(ExperimentalRouteError::Query(
                    velagraph::Error::UnsupportedGremlinArgument(
                        "only limit/count may follow a v2 scalar projection".into(),
                    ),
                ));
            }
            if projections && matches!(step, GremlinStep::Values(_) | GremlinStep::Label) {
                scalar = true;
                continue;
            }
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
                        value: None,
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
                                value: None,
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
                                    value: None,
                                },
                                &mut work,
                            )?;
                        }
                    }
                    rows = next;
                }
                GremlinStep::Values(_) | GremlinStep::Label => {
                    let mut projected = Vec::new();
                    for mut row in rows {
                        self.op.check(None)?;
                        work.visit()?;
                        let mut vertex = self
                            .get_vertex(row.vertex)?
                            .ok_or(ExperimentalError::MissingEndpoint(row.vertex))?;
                        work.vertex(&vertex)?;
                        let value = match step {
                            GremlinStep::Values(key) => vertex.properties.remove(key),
                            GremlinStep::Label => Some(Value::Text(
                                vertex.labels.into_iter().next().unwrap_or_default(),
                            )),
                            _ => unreachable!("projection step"),
                        };
                        if let Some(value) = value {
                            work.charge(match &value {
                                Value::Text(s) => s.len(),
                                _ => 8,
                            })?;
                            row.value = Some(value);
                            row.path.clear();
                            push(&mut projected, row, &mut work)?;
                        }
                    }
                    rows = projected;
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
            vec![ObservedRowV2::Count(rows.len() as u64)]
        } else {
            rows.into_iter()
                .map(|r| {
                    if let Some(value) = r.value {
                        ObservedRowV2::Value {
                            source: r.vertex,
                            value,
                        }
                    } else if path {
                        ObservedRowV2::Path(r.path)
                    } else {
                        ObservedRowV2::Vertex(r.vertex)
                    }
                })
                .collect()
        };
        self.op.check(None)?;
        Ok(ObservedTraversalV2 {
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
    fn projection_equality_follows_value_without_claiming_eq() {
        let source = VertexRef {
            partition: 1,
            id: 7,
        };
        let row = |x| ObservedRowV2::Value {
            source,
            value: Value::Real(x),
        };
        assert_eq!(row(-0.0), row(0.0));
        let nan = row(f64::NAN);
        assert_ne!(nan, nan.clone());
    }
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
                velagraph::Error::TraversalLimitExceeded(_)
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
        let edge_output = session
            .observed_gremlin_v3(999, "g.E().has('missing').count()")
            .unwrap();
        assert_eq!(edge_output.rows, vec![ObservedRowV3::Count(0)]);
        assert_eq!(edge_output.completeness, AdjacencyCompleteness::Unverified);
        assert_eq!(edge_output.metadata_sequence, 42);
        assert!(session.observed_gremlin_v3(999, "g.E().out()").is_err());
        assert!(session.observed_gremlin_v3(999, "g.E(1)").is_err());
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
        for q in [
            "g.E().count()",
            "g.E(1).label()",
            "g.V().count()",
            "g.V().outE().inV().count()",
            "g.E().bothV().count()",
        ] {
            assert!(matches!(
                session.observed_gremlin_v3(1, q),
                Err(ExperimentalRouteError::Route(RouteError::Storage(
                    StorageNodeError::Timeout { .. }
                )))
            ));
        }
        assert!(matches!(
            session.observed_gremlin_v2(1, "g.V().values('x').count()"),
            Err(ExperimentalRouteError::Route(RouteError::Storage(
                StorageNodeError::Timeout { .. }
            )))
        ));
        assert!(matches!(
            session.observed_gremlin(1, "g.V(7).count()"),
            Err(ExperimentalRouteError::Route(RouteError::Storage(
                StorageNodeError::Timeout { .. }
            )))
        ));
    }
}
