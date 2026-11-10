//! Additive edge observations; no global completeness or consistency guarantee.
use super::*;

/// Qualified provenance for either kind of graph element.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservedElementRef {
    Vertex(VertexRef),
    Edge(EdgeRef),
}
/// Version 3 adds edge starts and element-qualified scalar provenance.
#[derive(Clone, Debug, PartialEq)]
pub enum ObservedRowV3 {
    Vertex(VertexRef),
    Edge(EdgeRef),
    Path(Vec<ObservedPathElement>),
    Count(u64),
    Value {
        source: ObservedElementRef,
        value: Value,
    },
}
/// A frozen vector of shard observations, not a globally consistent snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct ObservedTraversalV3 {
    pub rows: Vec<ObservedRowV3>,
    pub completeness: AdjacencyCompleteness,
    pub metadata_sequence: u64,
    pub snapshots: Vec<ShardReadStamp>,
}
struct ElementRow {
    element: ObservedElementRef,
    path: Vec<ObservedPathElement>,
    value: Option<Value>,
}
impl ObservedElementRef {
    fn path_element(self) -> ObservedPathElement {
        match self {
            Self::Vertex(v) => ObservedPathElement::Vertex(v),
            Self::Edge(e) => ObservedPathElement::Edge(e),
        }
    }
    fn order(self) -> (u64, u32) {
        match self {
            Self::Vertex(v) => (v.id, v.partition),
            Self::Edge(e) => (e.id, e.partition),
        }
    }
}
impl Work {
    fn edge(&mut self, edge: &CrossEdge) -> ExperimentalResult<()> {
        self.charge(256 + edge.label.len())?;
        for (key, value) in &edge.properties {
            self.charge(
                128 + key.len()
                    + match value {
                        Value::Text(s) => s.len(),
                        _ => 8,
                    },
            )?;
        }
        Ok(())
    }
}
fn push_element(
    rows: &mut Vec<ElementRow>,
    row: ElementRow,
    work: &mut Work,
) -> ExperimentalResult<()> {
    if rows.len() >= 10_000 {
        return Err(limit("v3 element row cap exceeded"));
    }
    work.charge(128 + row.path.len() * std::mem::size_of::<ObservedPathElement>())?;
    rows.push(row);
    Ok(())
}
fn start_row(element: ObservedElementRef) -> ElementRow {
    ElementRow {
        element,
        path: vec![element.path_element()],
        value: None,
    }
}
/// Validate stream types before acquisition, including empty streams. No runtime
/// row can turn an invalid vertex/edge transition into an apparent success.
pub(crate) fn validate_steps(
    query: &rocksdb_sql_table::ObservedGremlinQuery,
) -> ExperimentalResult<()> {
    use rocksdb_sql_table::ObservedGremlinStep as Step;
    let mut edge_stream = matches!(query.start(), GremlinStart::Edges(_));
    let mut scalar = false;
    for step in query.steps() {
        if scalar
            && !matches!(
                step,
                Step::Existing(GremlinStep::Limit(_) | GremlinStep::Count)
            )
        {
            // Preserve the error variant of the former v3 -> v2 vertex dispatch.
            if matches!(query.start(), GremlinStart::Vertices(_)) {
                return Err(ExperimentalRouteError::Query(
                    rocksdb_sql_table::Error::UnsupportedGremlinArgument(
                        "only limit/count may follow a v2 scalar projection".into(),
                    ),
                ));
            }
            return Err(ExperimentalRouteError::Unsupported(
                "only limit/count may follow a v3 scalar projection",
            ));
        }
        match step {
            Step::Repeat { .. } => {
                return Err(ExperimentalRouteError::Unsupported(
                    "repeat is only supported by the server coordinator",
                ));
            }
            Step::OutE(_) | Step::InE(_) | Step::BothE(_) => {
                if edge_stream {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 adjacency requires vertices",
                    ));
                }
                edge_stream = true;
            }
            Step::InV | Step::OutV | Step::BothV => {
                if !edge_stream {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 endpoints require edges",
                    ));
                }
                edge_stream = false;
            }
            Step::Existing(GremlinStep::Out(_) | GremlinStep::In(_) | GremlinStep::Both(_)) => {
                if edge_stream {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 adjacency requires vertices",
                    ));
                }
            }
            Step::Existing(GremlinStep::Values(_) | GremlinStep::Label | GremlinStep::Id) => {
                scalar = true
            }
            Step::Existing(
                GremlinStep::HasLabel(_)
                | GremlinStep::Has { .. }
                | GremlinStep::Limit(_)
                | GremlinStep::Path
                | GremlinStep::Count,
            ) => {}
            _ => {
                return Err(ExperimentalRouteError::Unsupported(
                    "v3 step outside observed traversal subset",
                ))
            }
        }
    }
    Ok(())
}
/// Private execution-only read boundary. Providers own their consistency proof;
/// the shared executor returns rows, never a snapshot/certification envelope.
pub(crate) trait TraversalRead {
    type Node: StorageNode;
    fn check(&self) -> ExperimentalResult<()>;
    fn partitions(&self) -> Vec<u32>;
    fn metadata_bytes(&self) -> usize;
    fn store(&self, partition: u32) -> ExperimentalResult<AppendOnlyStore<Self::Node>>;
    fn get_vertex(&self, vertex: VertexRef) -> ExperimentalResult<Option<Vertex>>;
    fn get_edge(&self, edge: EdgeRef) -> ExperimentalResult<Option<CrossEdge>>;
    fn verified_edge_record(&self, edge: EdgeRef) -> ExperimentalResult<Option<EdgeRecord>>;
    fn observed_adjacency(
        &self,
        vertex: VertexRef,
        direction: Direction,
    ) -> ExperimentalResult<Vec<CrossEdge>>;

    fn execute_traversal(
        &self,
        start_partition: u32,
        text: &str,
    ) -> ExperimentalResult<Vec<ObservedRowV3>> {
        use rocksdb_sql_table::{ObservedGremlinQuery, ObservedGremlinStep as Step};
        self.check()?;
        let query = ObservedGremlinQuery::parse(text).map_err(ExperimentalRouteError::Query)?;
        self.check()?;
        validate_steps(&query)?;
        let (edge_start, ids) = match query.start() {
            GremlinStart::Edges(ids) => (true, ids),
            GremlinStart::Vertices(ids) => (false, ids),
        };
        if !ids.is_empty() {
            self.store(start_partition)?;
        }
        let mut work = Work {
            bytes: 4096,
            visits: 0,
        };
        let mut rows = Vec::new();
        let leading = match query.steps().first() {
            Some(Step::Existing(GremlinStep::Limit(n))) => Some(*n),
            _ => None,
        };
        for id in ids {
            self.check()?;
            if leading.is_some_and(|n| rows.len() >= n) {
                break;
            }
            work.visit()?;
            let element = if edge_start {
                let r = EdgeRef {
                    partition: start_partition,
                    id: *id,
                };
                let Some(edge) = self.get_edge(r)? else {
                    continue;
                };
                work.edge(&edge)?;
                ObservedElementRef::Edge(r)
            } else {
                let r = VertexRef {
                    partition: start_partition,
                    id: *id,
                };
                let Some(vertex) = self.get_vertex(r)? else {
                    continue;
                };
                work.vertex(&vertex)?;
                ObservedElementRef::Vertex(r)
            };
            push_element(&mut rows, start_row(element), &mut work)?;
        }
        if ids.is_empty() {
            for partition in self.partitions() {
                let store = self.store(partition)?;
                let mut after = None;
                let mut acquired = 0usize;
                loop {
                    self.check()?;
                    let remaining = leading.map_or(128, |n| n.saturating_sub(acquired).min(128));
                    if remaining == 0 {
                        break;
                    }
                    let scan_limit = ScanLimit::new(remaining, 1024 * 1024 + 64)?;
                    if edge_start {
                        let page = store.edge_record_page(after, scan_limit)?;
                        work.charge(page.raw_bytes + page.records.len() * 128)?;
                        for candidate in page.records {
                            self.check()?;
                            work.visit()?;
                            work.edge(candidate.edge())?;
                            let r = candidate.identity();
                            let record = self
                                .verified_edge_record(r)?
                                .ok_or(ExperimentalError::Incomplete(r))?;
                            work.edge(record.edge())?;
                            if record != candidate {
                                return Err(ExperimentalError::Incomplete(r).into());
                            }
                            // Deleted candidates still consume raw bytes, work and
                            // their page cursor. Only verified live edges count
                            // towards a leading limit, so tombstone pages cannot
                            // truncate the requested live prefix.
                            if matches!(record, EdgeRecord::Deleted(_)) {
                                continue;
                            }
                            push_element(
                                &mut rows,
                                start_row(ObservedElementRef::Edge(r)),
                                &mut work,
                            )?;
                            acquired += 1;
                        }
                        after = page.next_after;
                    } else {
                        let page = store.vertex_page(after, scan_limit)?;
                        work.charge(page.raw_bytes + page.vertices.len() * 128)?;
                        for vertex in page.vertices {
                            self.check()?;
                            work.visit()?;
                            work.vertex(&vertex)?;
                            let r = VertexRef {
                                partition,
                                id: vertex.id,
                            };
                            push_element(
                                &mut rows,
                                start_row(ObservedElementRef::Vertex(r)),
                                &mut work,
                            )?;
                            acquired += 1;
                        }
                        after = page.next_after;
                    }
                    if let Some(n) = leading {
                        rows.sort_unstable_by_key(|r| r.element.order());
                        rows.truncate(n);
                    }
                    if after.is_none() {
                        break;
                    }
                }
            }
            rows.sort_unstable_by_key(|r| r.element.order());
        }
        for step in query.steps() {
            self.check()?;
            match step {
                Step::Existing(GremlinStep::Limit(n)) => rows.truncate(*n),
                Step::Existing(GremlinStep::Path | GremlinStep::Count) => {}
                Step::OutE(label)
                | Step::InE(label)
                | Step::BothE(label)
                | Step::Existing(
                    GremlinStep::Out(label) | GremlinStep::In(label) | GremlinStep::Both(label),
                ) => {
                    let edge_output = matches!(step, Step::OutE(_) | Step::InE(_) | Step::BothE(_));
                    let dirs: &[Direction] = match step {
                        Step::OutE(_) | Step::Existing(GremlinStep::Out(_)) => &[Direction::Out],
                        Step::InE(_) | Step::Existing(GremlinStep::In(_)) => &[Direction::In],
                        _ => &[Direction::Out, Direction::In],
                    };
                    let mut next = Vec::new();
                    for row in rows {
                        self.check()?;
                        let ObservedElementRef::Vertex(vertex) = row.element else {
                            unreachable!("validated vertex stream")
                        };
                        let mut ordered = BTreeMap::new();
                        for direction in dirs {
                            work.visit()?;
                            let observed = self.observed_adjacency(vertex, *direction)?;
                            for edge in observed {
                                self.check()?;
                                work.visit()?;
                                work.edge(&edge)?;
                                if label.as_ref().is_some_and(|label| label != &edge.label) {
                                    continue;
                                }
                                let target = if *direction == Direction::Out {
                                    edge.to
                                } else {
                                    edge.from
                                };
                                // Qualified edge identity only. Parallel edges and input bag
                                // multiplicity survive; a loop in both directions collapses.
                                ordered.insert((edge.id, edge.from.partition), target);
                            }
                        }
                        for ((id, partition), target) in ordered {
                            self.check()?;
                            let edge = EdgeRef { partition, id };
                            let mut path = row.path.clone();
                            path.push(ObservedPathElement::Edge(edge));
                            let element = if edge_output {
                                ObservedElementRef::Edge(edge)
                            } else {
                                path.push(ObservedPathElement::Vertex(target));
                                ObservedElementRef::Vertex(target)
                            };
                            push_element(
                                &mut next,
                                ElementRow {
                                    element,
                                    path,
                                    value: None,
                                },
                                &mut work,
                            )?;
                        }
                    }
                    rows = next;
                }
                Step::InV | Step::OutV | Step::BothV => {
                    let mut next = Vec::new();
                    for row in rows {
                        self.check()?;
                        work.visit()?;
                        let ObservedElementRef::Edge(r) = row.element else {
                            unreachable!("validated edge stream")
                        };
                        let edge = self.get_edge(r)?.ok_or(ExperimentalError::Incomplete(r))?;
                        work.edge(&edge)?;
                        let endpoints: &[VertexRef] = match step {
                            Step::InV => std::slice::from_ref(&edge.to),
                            Step::OutV => std::slice::from_ref(&edge.from),
                            _ => &[edge.from, edge.to],
                        };
                        for vertex in endpoints {
                            self.check()?;
                            let mut path = row.path.clone();
                            path.push(ObservedPathElement::Vertex(*vertex));
                            push_element(
                                &mut next,
                                ElementRow {
                                    element: ObservedElementRef::Vertex(*vertex),
                                    path,
                                    value: None,
                                },
                                &mut work,
                            )?;
                        }
                    }
                    rows = next;
                }
                Step::Existing(
                    step @ (GremlinStep::HasLabel(_)
                    | GremlinStep::Has { .. }
                    | GremlinStep::Values(_)
                    | GremlinStep::Label
                    | GremlinStep::Id),
                ) => {
                    let mut next = Vec::new();
                    for mut row in rows {
                        self.check()?;
                        work.visit()?;
                        let (labels, mut properties) = match row.element {
                            ObservedElementRef::Edge(r) => {
                                let edge =
                                    self.get_edge(r)?.ok_or(ExperimentalError::Incomplete(r))?;
                                work.edge(&edge)?;
                                (vec![edge.label], edge.properties)
                            }
                            ObservedElementRef::Vertex(r) => {
                                let vertex = self
                                    .get_vertex(r)?
                                    .ok_or(ExperimentalError::MissingEndpoint(r))?;
                                work.vertex(&vertex)?;
                                (vertex.labels, vertex.properties)
                            }
                        };
                        let keep = match step {
                            GremlinStep::HasLabel(wanted) => {
                                labels.iter().any(|label| wanted.contains(label))
                            }
                            GremlinStep::Has { key, value } => match value {
                                Some(value) => properties.get(key) == Some(value),
                                None => properties.contains_key(key),
                            },
                            GremlinStep::Values(key) => {
                                row.value = properties.remove(key);
                                row.value.is_some()
                            }
                            GremlinStep::Id => {
                                row.value = Some(Value::Integer(
                                    i64::try_from(row.element.order().0).map_err(|_| {
                                        ExperimentalRouteError::Query(
                                            rocksdb_sql_table::Error::UnsupportedGremlinValue(
                                                "id exceeds i64".into(),
                                            ),
                                        )
                                    })?,
                                ));
                                true
                            }
                            GremlinStep::Label => {
                                row.value = Some(Value::Text(
                                    labels.into_iter().next().unwrap_or_default(),
                                ));
                                true
                            }
                            _ => unreachable!("filter/projection"),
                        };
                        if keep {
                            if let Some(value) = &row.value {
                                work.charge(match value {
                                    Value::Text(s) => s.len(),
                                    _ => 8,
                                })?;
                                row.path.clear();
                            }
                            push_element(&mut next, row, &mut work)?;
                        }
                    }
                    rows = next;
                }
                _ => unreachable!("validated observed subset"),
            }
        }
        let count = matches!(
            query.steps().last(),
            Some(Step::Existing(GremlinStep::Count))
        );
        let path = matches!(
            query.steps().last(),
            Some(Step::Existing(GremlinStep::Path))
        );
        if !count
            && !query
                .steps()
                .iter()
                .any(|s| matches!(s, Step::Existing(GremlinStep::Limit(_))))
        {
            rows.truncate(100);
        }
        work.charge(rows.len() * 128 + self.metadata_bytes())?;
        let rows = if count {
            vec![ObservedRowV3::Count(rows.len() as u64)]
        } else {
            rows.into_iter()
                .map(|r| {
                    if let Some(value) = r.value {
                        ObservedRowV3::Value {
                            source: r.element,
                            value,
                        }
                    } else if path {
                        ObservedRowV3::Path(r.path)
                    } else {
                        match r.element {
                            ObservedElementRef::Vertex(v) => ObservedRowV3::Vertex(v),
                            ObservedElementRef::Edge(e) => ObservedRowV3::Edge(e),
                        }
                    }
                })
                .collect()
        };
        self.check()?;
        Ok(rows)
    }
}

impl TraversalRead for ExperimentalReadSession {
    type Node = PinnedNode;
    fn check(&self) -> ExperimentalResult<()> {
        self.op.check(None).map_err(Into::into)
    }
    fn partitions(&self) -> Vec<u32> {
        self.placements.keys().copied().collect()
    }
    fn metadata_bytes(&self) -> usize {
        self.snapshots.len() * 64
    }
    fn store(&self, partition: u32) -> ExperimentalResult<AppendOnlyStore<Self::Node>> {
        ExperimentalReadSession::store(self, partition)
    }
    fn get_vertex(&self, vertex: VertexRef) -> ExperimentalResult<Option<Vertex>> {
        ExperimentalReadSession::get_vertex(self, vertex)
    }
    fn get_edge(&self, edge: EdgeRef) -> ExperimentalResult<Option<CrossEdge>> {
        ExperimentalReadSession::get_edge(self, edge)
    }
    fn verified_edge_record(&self, edge: EdgeRef) -> ExperimentalResult<Option<EdgeRecord>> {
        ExperimentalReadSession::verified_edge_record(self, edge)
    }
    fn observed_adjacency(
        &self,
        vertex: VertexRef,
        direction: Direction,
    ) -> ExperimentalResult<Vec<CrossEdge>> {
        ExperimentalReadSession::observed_adjacency(self, vertex, direction).map(|a| a.edges)
    }
}

impl ExperimentalReadSession {
    /// Existing observed API and metadata remain weak per-shard observations.
    /// Shared row execution does not relabel or certify this session.
    pub fn observed_gremlin_v3(
        &self,
        start_partition: u32,
        text: &str,
    ) -> ExperimentalResult<ObservedTraversalV3> {
        let rows = self.execute_traversal(start_partition, text)?;
        Ok(ObservedTraversalV3 {
            rows,
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
    fn client_observed_executor_rejects_server_only_repeat_before_reads() {
        for text in [
            "g.V(1).repeat(__.out()).times(2)",
            "g.V().repeat(__.in()).emit().times(32)",
            "g.E(1).repeat(__.out()).times(1)",
        ] {
            let query = rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap();
            assert!(matches!(
                validate_steps(&query),
                Err(ExperimentalRouteError::Unsupported(_))
            ));
        }
        let scalar = rocksdb_sql_table::ObservedGremlinQuery::parse(
            "g.V(1).values('x').repeat(__.out()).times(2)",
        )
        .unwrap();
        assert!(validate_steps(&scalar).is_err());
    }
}
