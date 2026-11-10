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
    /// Terminal selected vertex properties, with singleton-list Gremlin values.
    PropertyMap {
        source: VertexRef,
        properties: BTreeMap<String, Vec<Value>>,
    },
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
    admit_element(rows.len(), row.path.len(), work)?;
    rows.push(row);
    Ok(())
}
fn admit_element(rows: usize, path_len: usize, work: &mut Work) -> ExperimentalResult<()> {
    if rows >= 10_000 {
        return Err(limit("v3 element row cap exceeded"));
    }
    work.charge(128 + path_len * std::mem::size_of::<ObservedPathElement>())
}
fn start_row(element: ObservedElementRef) -> ElementRow {
    ElementRow {
        element,
        path: vec![element.path_element()],
        value: None,
    }
}
// Non-text keys are allocation-free. Text keys use their own set so duplicate
// lookups borrow str and unique payloads are charged before cloning.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum DedupKey {
    Vertex(u64, u32),
    Edge(u64, u32),
    Null,
    Boolean(bool),
    Integer(i64),
    Real(u64),
}
fn dedup_rows(
    rows: Vec<ElementRow>,
    work: &mut Work,
    mut check: impl FnMut() -> ExperimentalResult<()>,
) -> ExperimentalResult<Vec<ElementRow>> {
    let mut seen = std::collections::BTreeSet::new();
    let mut text_seen = std::collections::BTreeSet::<String>::new();
    let mut next = Vec::new();
    for row in rows {
        check()?;
        work.visit()?;
        let fresh = if let Some(Value::Text(text)) = &row.value {
            if text_seen.contains(text.as_str()) {
                false
            } else {
                work.charge(128 + text.len())?;
                text_seen.insert(text.clone());
                true
            }
        } else {
            let key = match &row.value {
                None => match row.element {
                    ObservedElementRef::Vertex(v) => DedupKey::Vertex(v.id, v.partition),
                    ObservedElementRef::Edge(e) => DedupKey::Edge(e.id, e.partition),
                },
                Some(Value::Null) => DedupKey::Null,
                Some(Value::Boolean(v)) => DedupKey::Boolean(*v),
                Some(Value::Integer(v)) => DedupKey::Integer(*v),
                // Records admit only finite reals. Preserve strict Value equality,
                // including equal signed zeros, without coercing Integer to Real.
                Some(Value::Real(v)) => DedupKey::Real(if *v == 0.0 { 0 } else { v.to_bits() }),
                Some(Value::Text(_)) => unreachable!("text handled above"),
            };
            if seen.contains(&key) {
                false
            } else {
                work.charge(128)?;
                seen.insert(key);
                true
            }
        };
        if fresh {
            // Keep the first complete row/provenance, never set iteration order.
            // Charges are cumulative; removed duplicates do not refund budgets.
            push_element(&mut next, row, work)?;
        }
    }
    Ok(next)
}
// Filtering moves existing rows without cloning values or provenance. The
// cumulative work and retained-row charges apply even when later steps discard
// these rows; an empty result never suppresses an upstream failure.
fn select_rows(
    rows: Vec<ElementRow>,
    mut keep: impl FnMut(usize, &ElementRow) -> bool,
    work: &mut Work,
    mut check: impl FnMut() -> ExperimentalResult<()>,
) -> ExperimentalResult<Vec<ElementRow>> {
    let mut next = Vec::new();
    for (index, row) in rows.into_iter().enumerate() {
        check()?;
        work.visit()?;
        if keep(index, &row) {
            push_element(&mut next, row, work)?;
        }
    }
    Ok(next)
}

fn filter_scalars(
    rows: Vec<ElementRow>,
    wanted: &Value,
    work: &mut Work,
    check: impl FnMut() -> ExperimentalResult<()>,
) -> ExperimentalResult<Vec<ElementRow>> {
    select_rows(
        rows,
        |_, row| row.value.as_ref() == Some(wanted),
        work,
        check,
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamKind {
    Vertex,
    Edge,
    Scalar,
}

/// Validate stream types before acquisition, including empty streams. No runtime
/// row can turn an invalid vertex/edge transition into an apparent success.
pub(crate) fn validate_steps(
    query: &rocksdb_sql_table::ObservedGremlinQuery,
) -> ExperimentalResult<()> {
    use rocksdb_sql_table::ObservedGremlinStep as Step;
    let mut stream = match query.start() {
        GremlinStart::Vertices(_) => StreamKind::Vertex,
        GremlinStart::Edges(_) => StreamKind::Edge,
    };
    let mut depth = 0usize;
    for (index, step) in query.steps().iter().enumerate() {
        if stream == StreamKind::Scalar
            && !matches!(
                step,
                Step::Is(_)
                    | Step::Range { .. }
                    | Step::Dedup
                    | Step::Existing(GremlinStep::Limit(_) | GremlinStep::Count)
            )
        {
            // Preserve the error variant of the former v3 -> v2 vertex dispatch.
            if matches!(query.start(), GremlinStart::Vertices(_)) {
                return Err(ExperimentalRouteError::Query(
                    rocksdb_sql_table::Error::UnsupportedGremlinArgument(
                        "only is/dedup/limit/range/count may follow a v2 scalar projection".into(),
                    ),
                ));
            }
            return Err(ExperimentalRouteError::Unsupported(
                "only is/dedup/limit/range/count may follow a v3 scalar projection",
            ));
        }
        match step {
            Step::ValueMap(keys) => {
                if stream != StreamKind::Vertex || index + 1 != query.steps().len() {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 valueMap requires a terminal vertex stream",
                    ));
                }
                crate::selected_properties::validate_keys(keys).map_err(|_| {
                    ExperimentalRouteError::Query(
                        rocksdb_sql_table::Error::UnsupportedGremlinArgument(
                            "valueMap requires one to eight distinct bounded keys".into(),
                        ),
                    )
                })?;
            }
            Step::Is(_) => {
                if stream != StreamKind::Scalar {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 is requires a scalar stream",
                    ));
                }
            }
            Step::Dedup | Step::Range { .. } => {}
            Step::Repeat { times, .. } => {
                if stream != StreamKind::Vertex {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 repeat requires vertices",
                    ));
                }
                depth += times;
            }
            Step::OutE(_) | Step::InE(_) | Step::BothE(_) => {
                if stream != StreamKind::Vertex {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 adjacency requires vertices",
                    ));
                }
                stream = StreamKind::Edge;
                depth += 1;
            }
            Step::InV | Step::OutV | Step::BothV => {
                if stream != StreamKind::Edge {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 endpoints require edges",
                    ));
                }
                stream = StreamKind::Vertex;
            }
            Step::Existing(GremlinStep::Out(_) | GremlinStep::In(_) | GremlinStep::Both(_)) => {
                if stream != StreamKind::Vertex {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 adjacency requires vertices",
                    ));
                }
                depth += 1;
            }
            Step::Existing(GremlinStep::Values(_) | GremlinStep::Label | GremlinStep::Id) => {
                stream = StreamKind::Scalar
            }
            Step::Existing(
                GremlinStep::HasLabel(_)
                | GremlinStep::Has { .. }
                | GremlinStep::Limit(_)
                | GremlinStep::Path
                | GremlinStep::Count,
            ) => {}
        }
    }
    if depth > rocksdb_sql_table::MAX_OBSERVED_GREMLIN_REPEAT_TIMES {
        return Err(limit("v3 combined traversal depth exceeds 32"));
    }
    Ok(())
}

// Ordinary and repeated adjacency use one implementation and the same provider,
// deadline and cumulative work counter. No query-text or AST rewriting occurs.
fn expand_rows<R: TraversalRead + ?Sized>(
    read: &R,
    rows: Vec<ElementRow>,
    dirs: &[Direction],
    label: Option<&str>,
    edge_output: bool,
    work: &mut Work,
) -> ExperimentalResult<Vec<ElementRow>> {
    let mut next = Vec::new();
    for row in rows {
        read.check()?;
        let ObservedElementRef::Vertex(vertex) = row.element else {
            unreachable!("validated vertex stream")
        };
        let mut ordered = BTreeMap::new();
        for direction in dirs {
            work.visit()?;
            let observed = read.observed_adjacency(vertex, *direction)?;
            for edge in observed {
                read.check()?;
                work.visit()?;
                work.edge(&edge)?;
                if label.is_some_and(|label| label != edge.label) {
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
            read.check()?;
            let edge = EdgeRef { partition, id };
            // Admit the complete row before cloning the growing path.
            admit_element(
                next.len(),
                row.path.len() + if edge_output { 1 } else { 2 },
                work,
            )?;
            let mut path = row.path.clone();
            path.push(ObservedPathElement::Edge(edge));
            let element = if edge_output {
                ObservedElementRef::Edge(edge)
            } else {
                path.push(ObservedPathElement::Vertex(target));
                ObservedElementRef::Vertex(target)
            };
            next.push(ElementRow {
                element,
                path,
                value: None,
            });
        }
    }
    Ok(next)
}

fn repeat_rows<R: TraversalRead + ?Sized>(
    read: &R,
    mut rows: Vec<ElementRow>,
    incoming: bool,
    label: Option<&str>,
    times: usize,
    emit: bool,
    work: &mut Work,
) -> ExperimentalResult<Vec<ElementRow>> {
    let dirs = [if incoming {
        Direction::In
    } else {
        Direction::Out
    }];
    let mut emitted = Vec::new();
    for depth in 1..=times {
        read.check()?;
        work.visit()?;
        rows = expand_rows(read, rows, &dirs, label, false, work)?;
        if rows.is_empty() {
            break;
        }
        if emit {
            if depth == times {
                // The last frontier has no next iteration. Move it once, while
                // preserving the same cumulative emitted-retention charge.
                for row in rows {
                    read.check()?;
                    push_element(&mut emitted, row, work)?;
                }
                return Ok(emitted);
            }
            for row in &rows {
                read.check()?;
                admit_element(emitted.len(), row.path.len(), work)?;
                emitted.push(ElementRow {
                    element: row.element,
                    path: row.path.clone(),
                    value: None,
                });
            }
        }
    }
    Ok(if emit { emitted } else { rows })
}

fn project_maps<R: TraversalRead + ?Sized>(
    read: &R,
    rows: Vec<ElementRow>,
    keys: &[String],
    work: &mut Work,
) -> ExperimentalResult<Vec<ObservedRowV3>> {
    read.check()?;
    // The terminal projection runs after the ordinary default result cap,
    // but every upstream traversal/integrity check has already completed.
    // Admit backing storage and every map before retaining singleton lists.
    work.charge(rows.len() * std::mem::size_of::<ObservedRowV3>())?;
    let mut output = Vec::with_capacity(rows.len());
    for row in rows {
        read.check()?;
        work.visit()?;
        let ObservedElementRef::Vertex(source) = row.element else {
            unreachable!("validated vertex map stream")
        };
        let mut vertex = read
            .get_vertex(source)?
            .ok_or(ExperimentalError::MissingEndpoint(source))?;
        work.vertex(&vertex)?;
        vertex.properties.retain(|key, _| keys.contains(key));
        let bytes =
            crate::selected_properties::properties_bytes(&vertex.properties).map_err(|error| {
                match error {
                    crate::remote::query::QueryFailure::ResourceExhausted => {
                        limit("v3 selected property map exceeds 2 KiB")
                    }
                    _ => ExperimentalRouteError::Query(rocksdb_sql_table::Error::CorruptData(
                        "invalid selected property map".into(),
                    )),
                }
            })?;
        work.charge(bytes + 128 * vertex.properties.len() + 256)?;
        output.push(ObservedRowV3::PropertyMap {
            source,
            properties: vertex
                .properties
                .into_iter()
                .map(|(key, value)| (key, vec![value]))
                .collect(),
        });
    }
    Ok(output)
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
                Step::Is(value) => {
                    rows = filter_scalars(rows, value, &mut work, || self.check())?;
                }
                Step::Range { low, high } => {
                    rows = select_rows(
                        rows,
                        |index, _| (*low..*high).contains(&index),
                        &mut work,
                        || self.check(),
                    )?;
                }
                Step::Dedup => rows = dedup_rows(rows, &mut work, || self.check())?,
                Step::Repeat {
                    incoming,
                    label,
                    times,
                    emit,
                } => {
                    rows = repeat_rows(
                        self,
                        rows,
                        *incoming,
                        label.as_deref(),
                        *times,
                        *emit,
                        &mut work,
                    )?;
                }
                Step::Existing(GremlinStep::Limit(n)) => rows.truncate(*n),
                Step::Existing(GremlinStep::Path | GremlinStep::Count) | Step::ValueMap(_) => {}
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
                    rows = expand_rows(self, rows, dirs, label.as_deref(), edge_output, &mut work)?;
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
            && !query.steps().iter().any(|s| {
                matches!(
                    s,
                    Step::Existing(GremlinStep::Limit(_)) | Step::Range { .. }
                )
            })
        {
            rows.truncate(100);
        }
        work.charge(rows.len() * 128 + self.metadata_bytes())?;
        let rows = if let Some(Step::ValueMap(keys)) = query.steps().last() {
            project_maps(self, rows, keys, &mut work)?
        } else if count {
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
    fn dedup_preflights_element_and_scalar_streams() {
        for text in [
            "g.V().dedup().out().dedup().path()",
            "g.E().dedup().bothV().dedup().count()",
            "g.V(999).outE().dedup().id()",
            "g.V(999).values('missing').dedup()",
            "g.E(999).label().dedup().dedup().count()",
            "g.V(999).id().limit(2).dedup().limit(1)",
        ] {
            let query = rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap();
            validate_steps(&query).unwrap();
        }
        for text in [
            "g.V(999).values('missing').dedup().out()",
            "g.E(999).label().dedup().hasLabel('x')",
            "g.V(999).id().dedup().path()",
        ] {
            let query = rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap();
            assert!(validate_steps(&query).is_err(), "{text}");
        }
        for text in [
            "g.V().dedup('a')",
            "g.V().dedup(local)",
            "g.V().dedup().by('name')",
            "g.V().path().dedup()",
        ] {
            assert!(
                rocksdb_sql_table::ObservedGremlinQuery::parse(text).is_err(),
                "{text}"
            );
        }
    }

    #[test]
    fn dedup_charges_duplicates_retention_and_cancellation() {
        let row = || {
            start_row(ObservedElementRef::Vertex(VertexRef {
                partition: 1,
                id: 7,
            }))
        };
        let mut work = Work {
            visits: 9_998,
            bytes: 0,
        };
        let result = dedup_rows(vec![row(), row()], &mut work, || Ok(())).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(work.visits, 10_000);
        assert_eq!(work.bytes, 256 + std::mem::size_of::<ObservedPathElement>());
        assert!(matches!(
            dedup_rows(vec![row()], &mut work, || Ok(())),
            Err(ExperimentalRouteError::Query(
                rocksdb_sql_table::Error::TraversalLimitExceeded(_)
            ))
        ));
        let mut work = Work {
            visits: 0,
            bytes: 64 * 1024 * 1024 - 1,
        };
        assert!(dedup_rows(vec![row()], &mut work, || Ok(())).is_err());
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        assert!(dedup_rows(vec![row()], &mut work, || Err(limit("expired"))).is_err());
        assert_eq!(work.visits, 0);
    }

    fn scalar_row(id: u64, value: Value) -> ElementRow {
        ElementRow {
            element: ObservedElementRef::Vertex(VertexRef { partition: 1, id }),
            path: Vec::new(),
            value: Some(value),
        }
    }

    #[test]
    fn range_preflights_entire_stream_even_when_empty() {
        for text in [
            "g.V(999).range(0,0).outE().range(0,2).bothV().path()",
            "g.E(999).range(1,2).label().range(0,1).is('x').dedup().count()",
            "g.V().repeat(out()).times(2).range(1,3).valueMap('x')",
        ] {
            validate_steps(&rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap()).unwrap();
        }
        for text in [
            "g.V(999).range(0,0).inV()",
            "g.E(999).range(0,0).out()",
            "g.V(999).id().range(0,0).path()",
            "g.V(999).values('missing').range(0,0).has('x')",
            "g.V(999).range(0,0).repeat(out()).times(32).out()",
        ] {
            assert!(
                validate_steps(&rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap())
                    .is_err(),
                "accepted {text}"
            );
        }
    }

    #[test]
    fn range_selection_preserves_order_bags_qualified_identity_and_owned_values() {
        let sources = [
            ObservedElementRef::Vertex(VertexRef {
                partition: 7,
                id: 9,
            }),
            ObservedElementRef::Edge(EdgeRef {
                partition: 3,
                id: 1,
            }),
            ObservedElementRef::Vertex(VertexRef {
                partition: 2,
                id: 9,
            }),
            ObservedElementRef::Vertex(VertexRef {
                partition: 7,
                id: 9,
            }),
        ];
        // Exercise the shared selection primitive over all endpoint pairs,
        // including empty ranges and ranges beyond the complete input bag.
        for low in 0..=6 {
            for high in low..=6 {
                let mut work = Work {
                    visits: 0,
                    bytes: 0,
                };
                let rows = sources
                    .iter()
                    .map(|source| ElementRow {
                        element: *source,
                        path: vec![source.path_element()],
                        value: Some(Value::Text("保留".into())),
                    })
                    .collect();
                let selected = select_rows(
                    rows,
                    |index, _| (low..high).contains(&index),
                    &mut work,
                    || Ok(()),
                )
                .unwrap();
                let expected = &sources[low.min(sources.len())..high.min(sources.len())];
                assert_eq!(
                    selected.iter().map(|row| row.element).collect::<Vec<_>>(),
                    expected
                );
                for row in &selected {
                    assert_eq!(row.path, [row.element.path_element()]);
                    assert_eq!(row.value, Some(Value::Text("保留".into())));
                }
                assert_eq!(work.visits, sources.len());
                assert_eq!(
                    work.bytes,
                    expected.len() * (128 + std::mem::size_of::<ObservedPathElement>())
                );
            }
        }
    }

    #[test]
    fn range_selection_checks_discarded_suffix_work_deadline_and_retention() {
        let rows = || (0..4).map(|id| scalar_row(id, Value::Null)).collect();
        for bounds in [0..0, 0..1, 8..9] {
            let mut work = Work {
                visits: 9_997,
                bytes: 0,
            };
            assert!(select_rows(
                rows(),
                |index, _| bounds.contains(&index),
                &mut work,
                || Ok(())
            )
            .is_err());
            assert_eq!(work.visits, 10_001);
        }
        // Cancellation after the selected prefix still rejects, rather than
        // returning a prefix before inspecting the rest of this bounded step.
        let mut checks = 0;
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        assert!(select_rows(
            rows(),
            |index, _| index == 0,
            &mut work,
            || {
                checks += 1;
                if checks == 3 {
                    Err(limit("expired"))
                } else {
                    Ok(())
                }
            }
        )
        .is_err());
        assert_eq!(work.visits, 2);
        let mut work = Work {
            visits: 0,
            bytes: 64 * 1024 * 1024 - 127,
        };
        assert!(select_rows(rows(), |index, _| index == 1, &mut work, || Ok(())).is_err());
        assert_eq!(work.visits, 2);
        assert!(work.bytes > 64 * 1024 * 1024);
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        assert!(select_rows(Vec::new(), |_, _| true, &mut work, || Ok(()))
            .unwrap()
            .is_empty());
        assert_eq!((work.visits, work.bytes), (0, 0));
    }

    #[test]
    fn scalar_filter_preflights_entire_stream() {
        for text in [
            "g.V(999).values('missing').is(null).dedup().count()",
            "g.E(999).id().is(1).limit(1).is(1)",
            "g.V().repeat(out()).times(2).label().is('same')",
        ] {
            validate_steps(&rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap()).unwrap();
        }
        for text in [
            "g.V(999).is(1)",
            "g.E(999).is(1)",
            "g.V(999).id().is(1).path()",
            "g.V(999).id().is(1).out()",
            "g.E(999).label().is('same').inV()",
            "g.V(999).values('missing').is(null).valueMap('x')",
            "g.V(999).id().is(1).values('x')",
        ] {
            assert!(
                validate_steps(&rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap())
                    .is_err(),
                "accepted {text}"
            );
        }
    }

    #[test]
    fn scalar_filter_keeps_strict_bags_provenance_and_signed_zero() {
        let values = [
            Value::Null,
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Integer(1),
            Value::Real(1.0),
            Value::Text("1".into()),
            Value::Real(-0.0),
            Value::Real(0.0),
            Value::Integer(0),
            Value::Text("é".into()),
            Value::Text("e\u{301}".into()),
            Value::Text("同值".into()),
            Value::Null,
        ];
        for wanted in &values {
            let expected: Vec<_> = values
                .iter()
                .enumerate()
                .filter(|(_, value)| *value == wanted)
                .map(|(id, value)| (id as u64, value.clone()))
                .collect();
            let rows = values
                .iter()
                .enumerate()
                .map(|(id, value)| scalar_row(id as u64, value.clone()))
                .collect();
            let mut work = Work {
                visits: 0,
                bytes: 0,
            };
            let actual = filter_scalars(rows, wanted, &mut work, || Ok(())).unwrap();
            assert_eq!(work.visits, values.len());
            assert_eq!(work.bytes, expected.len() * 128);
            let actual: Vec<_> = actual
                .into_iter()
                .map(|row| (row.element.order().0, row.value.unwrap()))
                .collect();
            assert_eq!(actual, expected);
            if *wanted == Value::Real(0.0) {
                let Value::Real(zero) = actual[0].1 else {
                    panic!("expected real")
                };
                assert_eq!(zero.to_bits(), (-0.0f64).to_bits());
            }
        }
    }

    #[test]
    fn scalar_filter_charges_rejected_inputs_and_retention_before_allocation() {
        let mut work = Work {
            visits: 9_999,
            bytes: 0,
        };
        assert!(filter_scalars(
            vec![scalar_row(1, Value::Null), scalar_row(2, Value::Null)],
            &Value::Integer(1),
            &mut work,
            || Ok(())
        )
        .is_err());
        let mut work = Work {
            visits: 0,
            bytes: 64 * 1024 * 1024 - 127,
        };
        assert!(filter_scalars(
            vec![scalar_row(1, Value::Null)],
            &Value::Null,
            &mut work,
            || Ok(())
        )
        .is_err());
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        assert!(filter_scalars(
            vec![scalar_row(1, Value::Null)],
            &Value::Null,
            &mut work,
            || Err(limit("expired"))
        )
        .is_err());
        assert_eq!((work.visits, work.bytes), (0, 0));
    }

    #[test]
    fn scalar_dedup_matches_strict_linear_oracle_and_first_provenance() {
        let values = [
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Integer(0),
            Value::Integer(1),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Real(-0.0),
            Value::Real(0.0),
            Value::Real(1.0),
            Value::Real(f64::MIN_POSITIVE),
            Value::Real(f64::MAX),
            Value::Text(String::new()),
            Value::Text("1".into()),
            Value::Text("同值".into()),
            Value::Text("é".into()),
            Value::Text("e\u{301}".into()),
        ];
        let mut seed = 37u64;
        for _ in 0..64 {
            let mut expected: Vec<(ObservedElementRef, Value)> = Vec::new();
            let mut rows = Vec::new();
            for id in 0..80 {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                let value = values[(seed >> 32) as usize % values.len()].clone();
                let row = scalar_row(id, value.clone());
                // Independent linear equality oracle, not production key encoding.
                if !expected.iter().any(|(_, previous)| *previous == value) {
                    expected.push((row.element, value));
                }
                rows.push(row);
            }
            let mut work = Work {
                visits: 0,
                bytes: 0,
            };
            let actual: Vec<_> = dedup_rows(rows, &mut work, || Ok(()))
                .unwrap()
                .into_iter()
                .map(|row| (row.element, row.value.unwrap()))
                .collect();
            assert_eq!(actual, expected);
            assert_eq!(work.visits, 80);
        }
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        let actual = dedup_rows(
            vec![
                scalar_row(4, Value::Real(-0.0)),
                scalar_row(5, Value::Real(0.0)),
            ],
            &mut work,
            || Ok(()),
        )
        .unwrap();
        assert_eq!(actual.len(), 1);
        let Some(Value::Real(zero)) = actual[0].value else {
            panic!("expected real")
        };
        assert_eq!(zero.to_bits(), (-0.0f64).to_bits());
    }

    #[test]
    fn scalar_dedup_charges_unique_text_before_retention_and_every_input() {
        let value = Value::Text("同值".into());
        let cost = 128 + "同值".len() + 128;
        let mut work = Work {
            visits: 9_998,
            bytes: 64 * 1024 * 1024 - cost,
        };
        let rows = dedup_rows(
            vec![scalar_row(1, value.clone()), scalar_row(2, value.clone())],
            &mut work,
            || Ok(()),
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(work.visits, 10_000);
        assert_eq!(work.bytes, 64 * 1024 * 1024);
        assert!(dedup_rows(vec![scalar_row(3, value.clone())], &mut work, || Ok(())).is_err());
        let mut work = Work {
            visits: 0,
            bytes: 64 * 1024 * 1024 - 128,
        };
        assert!(dedup_rows(vec![scalar_row(1, value.clone())], &mut work, || Ok(())).is_err());
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        assert!(
            dedup_rows(vec![scalar_row(1, value)], &mut work, || Err(limit(
                "expired"
            )))
            .is_err()
        );
        assert_eq!(work.visits, 0);
        assert_eq!(work.bytes, 0);
    }

    #[test]
    fn repeat_preflights_full_stream_and_combined_depth() {
        for text in [
            "g.V(1).repeat(__.out()).times(2)",
            "g.V().repeat(__.in()).emit().times(32)",
            "g.E(1).bothV().repeat(__.out()).times(32).path()",
            "g.V(1).outE().inV().repeat(__.in()).times(31).dedup().id()",
        ] {
            let query = rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap();
            validate_steps(&query).unwrap();
        }
        for text in [
            "g.E(999).repeat(__.out()).times(1)",
            "g.V(999).outE().repeat(__.out()).times(1)",
            "g.V(999).values('x').repeat(__.out()).times(2)",
            "g.V(999).repeat(__.in()).times(32).out()",
            "g.V(999).outE().inV().repeat(__.out()).times(32)",
        ] {
            let query = rocksdb_sql_table::ObservedGremlinQuery::parse(text).unwrap();
            assert!(validate_steps(&query).is_err(), "{text}");
        }
        for text in [
            "g.V(999).repeat(__.out()).times(0)",
            "g.V(999).repeat(__.out()).times(33)",
            "g.V(999).repeat(__.both()).times(2)",
            "g.V(999).repeat(__.out().dedup()).times(2)",
            "g.V(999).emit().repeat(__.out()).times(2)",
            "g.V(999).repeat(__.out()).until(has('x'))",
            "g.V(999).repeat(__.out()).times(1).repeat(__.in()).times(1)",
        ] {
            assert!(
                rocksdb_sql_table::ObservedGremlinQuery::parse(text).is_err(),
                "{text}"
            );
        }
    }
    struct LoopRead {
        checks: std::cell::Cell<usize>,
        fail_after: usize,
    }
    impl TraversalRead for LoopRead {
        type Node = PinnedNode;
        fn check(&self) -> ExperimentalResult<()> {
            let checked = self.checks.get();
            self.checks.set(checked + 1);
            if checked >= self.fail_after {
                Err(limit("expired"))
            } else {
                Ok(())
            }
        }
        fn partitions(&self) -> Vec<u32> {
            unreachable!()
        }
        fn metadata_bytes(&self) -> usize {
            unreachable!()
        }
        fn store(&self, _: u32) -> ExperimentalResult<AppendOnlyStore<Self::Node>> {
            unreachable!()
        }
        fn get_vertex(&self, vertex: VertexRef) -> ExperimentalResult<Option<Vertex>> {
            Ok(Some(Vertex {
                id: vertex.id,
                labels: Vec::new(),
                properties: BTreeMap::from([("x".into(), Value::Text("value".into()))]),
            }))
        }
        fn get_edge(&self, _: EdgeRef) -> ExperimentalResult<Option<CrossEdge>> {
            unreachable!()
        }
        fn verified_edge_record(&self, _: EdgeRef) -> ExperimentalResult<Option<EdgeRecord>> {
            unreachable!()
        }
        fn observed_adjacency(
            &self,
            vertex: VertexRef,
            _: Direction,
        ) -> ExperimentalResult<Vec<CrossEdge>> {
            Ok(vec![CrossEdge {
                id: 11,
                from: vertex,
                to: vertex,
                label: "K".into(),
                properties: BTreeMap::new(),
            }])
        }
    }

    #[test]
    fn selected_maps_charge_backing_rows_properties_and_deadlines_before_retention() {
        let read = LoopRead {
            checks: std::cell::Cell::new(0),
            fail_after: usize::MAX,
        };
        let row = || {
            start_row(ObservedElementRef::Vertex(VertexRef {
                partition: 7,
                id: 1,
            }))
        };
        let keys = vec!["x".to_string()];
        let backing = std::mem::size_of::<ObservedRowV3>();
        let vertex_bytes = 128 + 128 + 1 + 5;
        let flat_bytes = serde_json::to_vec(&BTreeMap::from([("x", Value::Text("value".into()))]))
            .unwrap()
            .len();
        let retained = flat_bytes + 128 + 256;
        let mut work = Work {
            bytes: 0,
            visits: 0,
        };
        assert_eq!(
            project_maps(&read, vec![row()], &keys, &mut work)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            (work.bytes, work.visits),
            (backing + vertex_bytes + retained, 1)
        );
        for remaining in [backing - 1, backing + vertex_bytes + retained - 1] {
            let mut work = Work {
                bytes: 64 * 1024 * 1024 - remaining,
                visits: 0,
            };
            assert!(project_maps(&read, vec![row()], &keys, &mut work).is_err());
            assert!(work.bytes > 64 * 1024 * 1024);
        }
        let mut work = Work {
            bytes: 0,
            visits: 10_000,
        };
        assert!(project_maps(&read, vec![row()], &keys, &mut work).is_err());
        assert_eq!(work.bytes, backing);
        let expired = LoopRead {
            checks: std::cell::Cell::new(0),
            fail_after: 0,
        };
        let mut work = Work {
            bytes: 0,
            visits: 0,
        };
        assert!(project_maps(&expired, vec![row()], &keys, &mut work).is_err());
        assert_eq!((work.bytes, work.visits), (0, 0));
    }

    #[test]
    fn repeat_keeps_paths_and_cumulative_round_emission_and_deadline_budgets() {
        let vertex = VertexRef {
            partition: 7,
            id: 3,
        };
        let read = LoopRead {
            checks: std::cell::Cell::new(0),
            fail_after: usize::MAX,
        };
        for emit in [false, true] {
            let mut work = Work {
                visits: 0,
                bytes: 0,
            };
            let rows = repeat_rows(
                &read,
                vec![start_row(ObservedElementRef::Vertex(vertex))],
                false,
                None,
                32,
                emit,
                &mut work,
            )
            .unwrap();
            assert_eq!(work.visits, 96); // one round, one adjacency and one edge per depth
            assert_eq!(rows.len(), if emit { 32 } else { 1 });
            let mut expected_bytes = 0;
            for depth in 1..=32 {
                let row_bytes = 128 + (2 * depth + 1) * std::mem::size_of::<ObservedPathElement>();
                expected_bytes += 257 + row_bytes + if emit { row_bytes } else { 0 };
            }
            assert_eq!(work.bytes, expected_bytes);
            for (index, row) in rows.iter().enumerate() {
                let depth = if emit { index + 1 } else { 32 };
                let mut expected = vec![ObservedPathElement::Vertex(vertex)];
                for _ in 0..depth {
                    expected.extend([
                        ObservedPathElement::Edge(EdgeRef {
                            partition: 7,
                            id: 11,
                        }),
                        ObservedPathElement::Vertex(vertex),
                    ]);
                }
                assert_eq!(row.path, expected);
            }
        }
        let mut work = Work {
            visits: 9_998,
            bytes: 0,
        };
        assert!(repeat_rows(
            &read,
            vec![start_row(ObservedElementRef::Vertex(vertex))],
            false,
            None,
            2,
            true,
            &mut work
        )
        .is_err());
        let mut work = Work {
            visits: 0,
            bytes: 64 * 1024 * 1024 - 257,
        };
        assert!(repeat_rows(
            &read,
            vec![start_row(ObservedElementRef::Vertex(vertex))],
            false,
            None,
            2,
            true,
            &mut work
        )
        .is_err());
        // A full first frontier fits exactly, but retaining the first emitted
        // copy must fail before its clone and cannot return an early prefix.
        let row_bytes = 128 + 3 * std::mem::size_of::<ObservedPathElement>();
        let mut work = Work {
            visits: 0,
            bytes: 64 * 1024 * 1024 - 257 - row_bytes,
        };
        assert!(repeat_rows(
            &read,
            vec![start_row(ObservedElementRef::Vertex(vertex))],
            false,
            None,
            2,
            true,
            &mut work
        )
        .is_err());
        assert_eq!(work.visits, 3);
        assert_eq!(work.bytes, 64 * 1024 * 1024 + row_bytes);
        for fail_after in [0, 4, 10, 50] {
            let read = LoopRead {
                checks: std::cell::Cell::new(0),
                fail_after,
            };
            let mut work = Work {
                visits: 0,
                bytes: 0,
            };
            assert!(repeat_rows(
                &read,
                vec![start_row(ObservedElementRef::Vertex(vertex))],
                false,
                None,
                32,
                true,
                &mut work
            )
            .is_err());
        }
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        assert!(
            repeat_rows(&read, Vec::new(), false, None, 32, true, &mut work)
                .unwrap()
                .is_empty()
        );
        assert_eq!((work.visits, work.bytes), (1, 0));
        assert!(admit_element(10_000, 1, &mut work).is_err());
    }
}
