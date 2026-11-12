//! Private executor over the existing official-grammar AST. No wire builders,
//! text matching, tokenization, parser alternatives, or weak-read fallback.
use super::*;
use crate::remote::query::QualifiedElement;
use velagraph::experimental_graph::Direction;
use velagraph::{
    GremlinStart, GremlinStep, ObservedGremlinQuery, ObservedGremlinStep, DEFAULT_GREMLIN_LIMIT,
};

struct Plan {
    start: QualifiedElement,
    steps: Vec<ObservedGremlinStep>,
    adjacency: Option<Direction>,
    output: Output,
    maximum: usize,
    explicit_limit: bool,
}
#[derive(Clone, Copy)]
enum Output {
    Element,
    Value,
    Id,
    Label,
    Count,
}
fn error(failure: QueryFailure) -> Error {
    Error::Query(failure)
}
fn parse_error(cause: velagraph::Error) -> Error {
    error(crate::remote::query::graph_error(cause))
}
fn parse(query: &Query) -> Result<Plan> {
    let parsed = ObservedGremlinQuery::parse(&query.text).map_err(parse_error)?;
    let (start, mut vertex) = match parsed.start() {
        GremlinStart::Vertices(ids) if ids.len() == 1 => (
            QualifiedElement::Vertex {
                partition: query.partition,
                id: ids[0],
            },
            true,
        ),
        GremlinStart::Edges(ids) if ids.len() == 1 => (
            QualifiedElement::Edge {
                partition: query.partition,
                id: ids[0],
            },
            false,
        ),
        _ => return Err(error(QueryFailure::Unsupported)),
    };
    let mut adjacency = None;
    let mut output = Output::Element;
    let mut maximum = 1;
    let mut explicit_limit = false;
    for (index, step) in parsed.steps().iter().enumerate() {
        match step {
            ObservedGremlinStep::OutE(_) | ObservedGremlinStep::InE(_) => {
                if !vertex || !matches!(output, Output::Element) {
                    return Err(error(QueryFailure::InvalidArgument));
                }
                if index != 0 {
                    return Err(error(QueryFailure::Unsupported));
                }
                adjacency = Some(if matches!(step, ObservedGremlinStep::OutE(_)) {
                    Direction::Out
                } else {
                    Direction::In
                });
                vertex = false;
                maximum = 1000;
            }
            ObservedGremlinStep::Existing(GremlinStep::Limit(n)) => {
                maximum = maximum.min(*n);
                explicit_limit = true;
            }
            ObservedGremlinStep::Existing(GremlinStep::Count) => output = Output::Count,
            ObservedGremlinStep::Existing(
                step @ (GremlinStep::Values(_) | GremlinStep::Id | GremlinStep::Label),
            ) => {
                if !matches!(output, Output::Element) {
                    return Err(error(QueryFailure::InvalidArgument));
                }
                output = match step {
                    GremlinStep::Values(_) => Output::Value,
                    GremlinStep::Id => Output::Id,
                    _ => Output::Label,
                };
            }
            _ => return Err(error(QueryFailure::Unsupported)),
        }
    }
    Ok(Plan {
        start,
        steps: parsed.steps().to_vec(),
        adjacency,
        output,
        maximum,
        explicit_limit,
    })
}
// Elements and AST instructions are internal execution plumbing, never decoded
// from a remote typed read operation.
enum Item {
    Vertex(VertexRef, Vertex),
    Edge(CrossEdge),
    Value(QualifiedElement, Value),
    Count(u64),
}
impl Item {
    fn source(&self) -> QualifiedElement {
        match self {
            Self::Vertex(v, _) => QualifiedElement::Vertex {
                partition: v.partition,
                id: v.id,
            },
            Self::Edge(e) => QualifiedElement::Edge {
                partition: e.from.partition,
                id: e.id,
            },
            _ => unreachable!("preflight rejects non-element projection"),
        }
    }
}
fn project(item: Item, step: &GremlinStep) -> Result<Option<Item>> {
    let source = item.source();
    let value = match step {
        GremlinStep::Values(key) => match item {
            Item::Vertex(_, mut v) => v.properties.remove(key),
            Item::Edge(mut e) => e.properties.remove(key),
            _ => unreachable!("preflight rejects scalar projection"),
        },
        GremlinStep::Id => {
            let id = match source {
                QualifiedElement::Vertex { id, .. } | QualifiedElement::Edge { id, .. } => id,
            };
            Some(Value::Integer(
                i64::try_from(id).map_err(|_| error(QueryFailure::Unsupported))?,
            ))
        }
        GremlinStep::Label => Some(Value::Text(match item {
            Item::Vertex(_, v) => v.labels.into_iter().next().unwrap_or_default(),
            Item::Edge(e) => e.label,
            _ => unreachable!("preflight rejects scalar projection"),
        })),
        _ => unreachable!("only admitted projections reach executor"),
    };
    Ok(value.map(|value| Item::Value(source, value)))
}
pub(super) fn execute(snapshot: &GraphSnapshot, query: &Query, end: Instant) -> Result<QueryData> {
    before_deadline(end)?;
    let plan = parse(query)?; // Validate the whole AST before touching even an empty stream.
    before_deadline(end)?;
    let mut items = match plan.start {
        QualifiedElement::Vertex { partition, id } => {
            let vertex = VertexRef { partition, id };
            snapshot
                .vertex(vertex)?
                .map(|v| Item::Vertex(vertex, v))
                .into_iter()
                .collect::<Vec<_>>()
        }
        QualifiedElement::Edge { partition, id } => snapshot
            .edge(EdgeRef { partition, id })?
            .map(Item::Edge)
            .into_iter()
            .collect(),
    };
    for step in &plan.steps {
        before_deadline(end)?;
        match step {
            ObservedGremlinStep::OutE(label) | ObservedGremlinStep::InE(label) => {
                let mut next = Vec::new();
                for item in items {
                    let Item::Vertex(vertex, _) = item else {
                        unreachable!("preflight checks vertex adjacency");
                    };
                    let direction = plan.adjacency.expect("preflight recorded adjacency");
                    let mut edges = snapshot.observed_adjacency(vertex, direction)?;
                    edges.sort_by_key(|edge| (edge.id, edge.from.partition));
                    for edge in edges {
                        before_deadline(end)?;
                        if label.as_ref().is_none_or(|label| *label == edge.label) {
                            next.push(Item::Edge(edge));
                        }
                    }
                }
                items = next;
            }
            ObservedGremlinStep::Existing(GremlinStep::Limit(n)) => items.truncate(*n),
            ObservedGremlinStep::Existing(GremlinStep::Count) => {
                items = vec![Item::Count(items.len() as u64)]
            }
            ObservedGremlinStep::Existing(
                step @ (GremlinStep::Values(_) | GremlinStep::Id | GremlinStep::Label),
            ) => {
                let mut next = Vec::new();
                for item in items {
                    before_deadline(end)?;
                    if let Some(value) = project(item, step)? {
                        next.push(value);
                    }
                }
                items = next;
            }
            _ => unreachable!("only admitted AST reaches execution"),
        }
    }
    if !plan.explicit_limit && !matches!(plan.output, Output::Count) {
        items.truncate(DEFAULT_GREMLIN_LIMIT);
    }
    let mut rows = Vec::with_capacity(items.len());
    let mut bytes = 0usize;
    for item in items {
        before_deadline(end)?;
        let row = match item {
            Item::Vertex(v, _) => QueryRow::Element(QualifiedElement::Vertex {
                partition: v.partition,
                id: v.id,
            }),
            Item::Edge(e) => QueryRow::Element(QualifiedElement::Edge {
                partition: e.from.partition,
                id: e.id,
            }),
            Item::Value(source, value) => QueryRow::Value {
                source: Some(source),
                value,
            },
            Item::Count(n) => QueryRow::Count(n),
        };
        bytes = bytes.saturating_add(
            128 + match &row {
                QueryRow::Value {
                    value: Value::Text(s),
                    ..
                } => s.len(),
                _ => 8,
            },
        );
        if bytes > MAX_RESPONSE_BYTES {
            return Err(Error::Capacity);
        }
        rows.push(row);
    }
    snapshot.version()?;
    before_deadline(end)?;
    Ok(QueryData {
        rows,
        completeness: QueryCompleteness::Unverified,
    })
}
// Binding/token/version/query echoes are checked by the caller. This validates
// row shape and qualified identity, not the truth of an unauthenticated peer's data.
pub(super) fn valid_output(query: &Query, data: &QueryData, binding: &Binding) -> bool {
    let member = |p: u32| {
        binding
            .cohort
            .members
            .iter()
            .any(|m| m.shard == u64::from(p))
    };
    if !member(query.partition) || data.completeness != QueryCompleteness::Unverified {
        return false;
    }
    let Ok(plan) = parse(query) else {
        return false;
    };
    let bound = if plan.explicit_limit {
        plan.maximum
    } else {
        plan.maximum.min(DEFAULT_GREMLIN_LIMIT)
    };
    if matches!(plan.output, Output::Count) {
        return matches!(data.rows.as_slice(),[QueryRow::Count(n)] if *n <= plan.maximum as u64);
    }
    if data.rows.len() > bound {
        return false;
    }
    let valid_source = |source: QualifiedElement| match (plan.adjacency, source) {
        (None, _) => source == plan.start,
        (Some(Direction::Out), QualifiedElement::Edge { partition, .. }) => {
            partition == query.partition
        }
        (Some(Direction::In), QualifiedElement::Edge { partition, .. }) => member(partition),
        _ => false,
    };
    let mut ids = BTreeSet::new();
    for row in &data.rows {
        let source = match (plan.output, row) {
            (Output::Element, QueryRow::Element(source)) => *source,
            (
                Output::Value | Output::Label | Output::Id,
                QueryRow::Value {
                    source: Some(source),
                    value,
                },
            ) => {
                if matches!(value,Value::Real(n) if !n.is_finite()) {
                    return false;
                }
                match plan.output {
                    Output::Label if !matches!(value, Value::Text(_)) => return false,
                    Output::Id => {
                        let id = match source {
                            QualifiedElement::Vertex { id, .. }
                            | QualifiedElement::Edge { id, .. } => *id,
                        };
                        if i64::try_from(id).ok().map(Value::Integer).as_ref() != Some(value) {
                            return false;
                        }
                    }
                    _ => {}
                }
                *source
            }
            _ => return false,
        };
        let key = match source {
            QualifiedElement::Vertex { partition, id }
            | QualifiedElement::Edge { partition, id } => (partition, id),
        };
        if !valid_source(source) || !ids.insert(key) {
            return false;
        }
    }
    true
}

// Reuse the established result shapes without weakening this protocol's strict
// fields or changing the existing remote query protocol's deserializer.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
enum WireElement {
    Vertex { partition: u32, id: u64 },
    Edge { partition: u32, id: u64 },
}
impl From<WireElement> for QualifiedElement {
    fn from(value: WireElement) -> Self {
        match value {
            WireElement::Vertex { partition, id } => Self::Vertex { partition, id },
            WireElement::Edge { partition, id } => Self::Edge { partition, id },
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
enum WireRow {
    Element(WireElement),
    PropertyMap {
        source: WireElement,
        properties: BTreeMap<String, Vec<Value>>,
    },
    Path(Vec<WireElement>),
    Value {
        source: Option<WireElement>,
        value: Value,
    },
    Count(u64),
}
pub(super) fn strict_rows<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<QueryRow>, D::Error> {
    Vec::<WireRow>::deserialize(deserializer).map(|rows| {
        rows.into_iter()
            .map(|row| match row {
                WireRow::Element(element) => QueryRow::Element(element.into()),
                WireRow::PropertyMap { source, properties } => QueryRow::PropertyMap {
                    source: source.into(),
                    properties,
                },
                WireRow::Path(elements) => {
                    QueryRow::Path(elements.into_iter().map(Into::into).collect())
                }
                WireRow::Value { source, value } => QueryRow::Value {
                    source: source.map(Into::into),
                    value,
                },
                WireRow::Count(n) => QueryRow::Count(n),
            })
            .collect()
    })
}

// The managed provider is private and contains no mutable route/catalog handle.
// Its stores and cross-edge validation all derive from this one proven session.
struct FixedRead<'a> {
    snapshot: &'a GraphSnapshot,
    end: Instant,
}
use crate::routing::experimental::{
    ExperimentalResult, ExperimentalRouteError, ObservedElementRef, ObservedPathElement,
    ObservedRowV3, TraversalRead,
};
use velagraph::experimental_graph::{AppendOnlyStore, EdgeRecord, ExperimentalError};
fn provider_error(cause: managed_graph::Error) -> ExperimentalRouteError {
    use velagraph::StorageNodeError;
    match cause {
        managed_graph::Error::Graph(e) => ExperimentalRouteError::Graph(e),
        managed_graph::Error::Storage(e) => e.into(),
        managed_graph::Error::Capacity => {
            StorageNodeError::ResourceExhausted("fixed read".into()).into()
        }
        managed_graph::Error::Identity => {
            ExperimentalRouteError::Unsupported("partition outside fixed cohort")
        }
        managed_graph::Error::Snapshot(cohort_reads::Error::Expired) => {
            StorageNodeError::Timeout { request_id: None }.into()
        }
        _ => StorageNodeError::Unavailable("fixed session unavailable".into()).into(),
    }
}
fn traversal_error(cause: ExperimentalRouteError) -> Error {
    use crate::routing::RouteError;
    error(match cause {
        ExperimentalRouteError::Query(e) => crate::remote::query::graph_error(e),
        ExperimentalRouteError::Unsupported(_) => QueryFailure::Unsupported,
        ExperimentalRouteError::Route(RouteError::Storage(e))
        | ExperimentalRouteError::Graph(ExperimentalError::Storage(e)) => {
            crate::remote::query::storage_error(e)
        }
        ExperimentalRouteError::Graph(ExperimentalError::Invalid(_)) => {
            QueryFailure::InvalidArgument
        }
        ExperimentalRouteError::Graph(_) => QueryFailure::IncompleteOrCorrupt,
        _ => QueryFailure::Unavailable,
    })
}
impl TraversalRead for FixedRead<'_> {
    type Node = crate::remote::query::FrozenNode;
    fn check(&self) -> ExperimentalResult<()> {
        if Instant::now() >= self.end {
            return Err(velagraph::StorageNodeError::Timeout { request_id: None }.into());
        }
        self.snapshot.version().map_err(provider_error)?;
        Ok(())
    }
    fn partitions(&self) -> Vec<u32> {
        self.snapshot.partitions()
    }
    fn metadata_bytes(&self) -> usize {
        self.snapshot.partitions().len() * 64
    }
    fn store(&self, partition: u32) -> ExperimentalResult<AppendOnlyStore<Self::Node>> {
        self.check()?;
        self.snapshot.store(partition).map_err(provider_error)
    }
    fn get_vertex(&self, vertex: VertexRef) -> ExperimentalResult<Option<Vertex>> {
        self.check()?;
        self.snapshot.vertex(vertex).map_err(provider_error)
    }
    fn get_edge(&self, edge: EdgeRef) -> ExperimentalResult<Option<CrossEdge>> {
        self.check()?;
        self.snapshot.edge(edge).map_err(provider_error)
    }
    fn verified_edge_record(&self, edge: EdgeRef) -> ExperimentalResult<Option<EdgeRecord>> {
        self.check()?;
        self.snapshot
            .verified_edge_record(edge)
            .map_err(provider_error)
    }
    fn observed_adjacency(
        &self,
        vertex: VertexRef,
        direction: Direction,
    ) -> ExperimentalResult<Vec<CrossEdge>> {
        self.check()?;
        self.snapshot
            .observed_adjacency(vertex, direction)
            .map_err(provider_error)
    }
}
fn qualified(element: ObservedElementRef) -> QualifiedElement {
    match element {
        ObservedElementRef::Vertex(v) => QualifiedElement::Vertex {
            partition: v.partition,
            id: v.id,
        },
        ObservedElementRef::Edge(e) => QualifiedElement::Edge {
            partition: e.partition,
            id: e.id,
        },
    }
}
pub(super) fn execute_traversal(
    snapshot: &GraphSnapshot,
    query: &Query,
    end: Instant,
) -> Result<QueryData> {
    let rows = FixedRead { snapshot, end }
        .execute_traversal(query.partition, &query.text)
        .map_err(traversal_error)?;
    let data = QueryData {
        rows: rows
            .into_iter()
            .map(|row| match row {
                ObservedRowV3::Vertex(v) => {
                    QueryRow::Element(qualified(ObservedElementRef::Vertex(v)))
                }
                ObservedRowV3::Edge(e) => QueryRow::Element(qualified(ObservedElementRef::Edge(e))),
                ObservedRowV3::Count(n) => QueryRow::Count(n),
                ObservedRowV3::PropertyMap { source, properties } => QueryRow::PropertyMap {
                    source: qualified(ObservedElementRef::Vertex(source)),
                    properties,
                },
                ObservedRowV3::Value { source, value } => QueryRow::Value {
                    source: Some(qualified(source)),
                    value,
                },
                ObservedRowV3::Path(elements) => QueryRow::Path(
                    elements
                        .into_iter()
                        .map(|e| match e {
                            ObservedPathElement::Vertex(v) => {
                                qualified(ObservedElementRef::Vertex(v))
                            }
                            ObservedPathElement::Edge(e) => qualified(ObservedElementRef::Edge(e)),
                        })
                        .collect(),
                ),
            })
            .collect(),
        completeness: QueryCompleteness::Unverified,
    };
    // Full result admission happens before any page can be emitted. Paging has
    // separate complete-envelope admission, including echoes and tokens.
    encode(&data, MAX_RESPONSE_BYTES)?;
    snapshot.version()?;
    before_deadline(end)?;
    Ok(data)
}
// Upper bounds follow the admitted AST's order. A limit before expansion does
// not incorrectly cap the final stream; both directions can contribute 2,000
// adjacency candidates per input before the common 10,000-row ceiling.
fn traversal_maximum(query: &ObservedGremlinQuery) -> usize {
    use velagraph::ObservedGremlinStep as Step;
    let ids = match query.start() {
        GremlinStart::Vertices(ids) | GremlinStart::Edges(ids) => ids,
    };
    let mut maximum = if ids.is_empty() { 10_000 } else { ids.len() };
    for step in query.steps() {
        let factor = match step {
            Step::Repeat { times, emit, .. } => {
                let mut frontier = maximum;
                let mut emitted = 0usize;
                for _ in 0..*times {
                    frontier = frontier.saturating_mul(1000).min(10_000);
                    emitted = emitted.saturating_add(frontier).min(10_000);
                }
                maximum = if *emit { emitted } else { frontier };
                1
            }
            Step::BothE(_) | Step::Existing(GremlinStep::Both(_)) => 2000,
            Step::OutE(_)
            | Step::InE(_)
            | Step::Existing(GremlinStep::Out(_) | GremlinStep::In(_)) => 1000,
            Step::BothV => 2,
            Step::Existing(GremlinStep::Limit(n)) => {
                maximum = maximum.min(*n);
                1
            }
            Step::Range { low, high } => {
                maximum = maximum.min(*high).saturating_sub(*low);
                1
            }
            _ => 1,
        };
        maximum = maximum.saturating_mul(factor).min(10_000);
    }
    maximum
}
pub(super) fn valid_traversal_total(query: &Query, total: u32) -> bool {
    let Ok(parsed) = ObservedGremlinQuery::parse(&query.text) else {
        return false;
    };
    if crate::routing::experimental::validate_traversal(&parsed).is_err() {
        return false;
    }
    if matches!(
        parsed.steps().last(),
        Some(ObservedGremlinStep::Existing(GremlinStep::Count))
    ) {
        return total == 1;
    }
    let explicit = parsed.steps().iter().any(|s| {
        matches!(
            s,
            ObservedGremlinStep::Existing(GremlinStep::Limit(_))
                | ObservedGremlinStep::Range { .. }
        )
    });
    let maximum = traversal_maximum(&parsed);
    total as usize
        <= if explicit {
            maximum
        } else {
            maximum.min(DEFAULT_GREMLIN_LIMIT)
        }
}
// At most 32 variants, one per post-emit depth. Each variant retains exact
// element kinds and source-ownership constraints through the complete suffix.
#[derive(Clone)]
struct TraversalPathShape {
    elements: Vec<bool>,
    owners: Vec<(usize, Vec<usize>)>,
}
impl TraversalPathShape {
    fn adjacency(&mut self, incoming: bool) {
        let n = self.elements.len();
        self.owners
            .push((n, vec![if incoming { n + 1 } else { n - 1 }]));
        self.elements.extend([true, false]);
    }
}

/// Checks shapes/provenance in whole results or page slices. Bag duplicates are
/// legal; this cannot authenticate a trusted peer or prove graph completeness.
pub(super) fn valid_traversal_output(query: &Query, data: &QueryData, binding: &Binding) -> bool {
    use velagraph::ObservedGremlinStep as Step;
    let member = |partition: u32| {
        binding
            .cohort
            .members
            .iter()
            .any(|m| m.shard == u64::from(partition))
    };
    if !member(query.partition) || data.completeness != QueryCompleteness::Unverified {
        return false;
    }
    let Ok(parsed) = ObservedGremlinQuery::parse(&query.text) else {
        return false;
    };
    if crate::routing::experimental::validate_traversal(&parsed).is_err() {
        return false;
    }
    let (mut edge, ids) = match parsed.start() {
        GremlinStart::Vertices(ids) => (false, ids),
        GremlinStart::Edges(ids) => (true, ids),
    };
    let mut shapes = vec![TraversalPathShape {
        elements: vec![edge],
        owners: Vec::new(),
    }];
    let mut scalar = false;
    let mut map_keys = None;
    let mut label = false;
    let mut id_projection = false;
    let mut count = false;
    let mut path = false;
    for step in parsed.steps() {
        match step {
            Step::OutE(_) | Step::InE(_) | Step::BothE(_) => {
                for shape in &mut shapes {
                    if matches!(step, Step::OutE(_)) {
                        shape
                            .owners
                            .push((shape.elements.len(), vec![shape.elements.len() - 1]));
                    }
                    shape.elements.push(true);
                }
                edge = true;
            }
            Step::InV | Step::OutV | Step::BothV => {
                for shape in &mut shapes {
                    if matches!(step, Step::OutV) {
                        shape
                            .owners
                            .push((shape.elements.len() - 1, vec![shape.elements.len()]));
                    }
                    shape.elements.push(false);
                }
                edge = false;
            }
            Step::Existing(GremlinStep::Out(_) | GremlinStep::In(_) | GremlinStep::Both(_)) => {
                for shape in &mut shapes {
                    let n = shape.elements.len();
                    let endpoints = match step {
                        Step::Existing(GremlinStep::Out(_)) => vec![n - 1],
                        Step::Existing(GremlinStep::In(_)) => vec![n + 1],
                        _ => vec![n - 1, n + 1],
                    };
                    shape.owners.push((n, endpoints));
                    shape.elements.extend([true, false]);
                }
            }
            Step::Repeat {
                incoming,
                times,
                emit,
                ..
            } => {
                let mut expanded = Vec::new();
                for mut shape in shapes {
                    for depth in 1..=*times {
                        shape.adjacency(*incoming);
                        if *emit && depth != *times {
                            expanded.push(shape.clone());
                        }
                    }
                    expanded.push(shape);
                }
                shapes = expanded;
            }
            Step::ValueMap(keys) => map_keys = Some(keys),
            Step::Existing(GremlinStep::Values(_)) => scalar = true,
            Step::Existing(GremlinStep::Id) => {
                scalar = true;
                id_projection = true;
            }
            Step::Existing(GremlinStep::Label) => {
                scalar = true;
                label = true;
            }
            Step::Existing(GremlinStep::Count) => count = true,
            Step::Existing(GremlinStep::Path) => path = true,
            _ => {}
        }
    }
    if count {
        return matches!(data.rows.as_slice(), [QueryRow::Count(n)] if *n <= traversal_maximum(&parsed) as u64);
    }
    if !valid_traversal_total(query, data.rows.len() as u32) {
        return false;
    }
    let valid_element = |element: &QualifiedElement, expected_edge: bool| match element {
        QualifiedElement::Vertex { partition, .. } => !expected_edge && member(*partition),
        QualifiedElement::Edge { partition, .. } => expected_edge && member(*partition),
    };
    let valid_start = |element: &QualifiedElement| {
        let (partition, id) = match element {
            QualifiedElement::Vertex { partition, id }
            | QualifiedElement::Edge { partition, id } => (*partition, *id),
        };
        ids.is_empty() || (partition == query.partition && ids.contains(&id))
    };
    let partition = |element: &QualifiedElement| match element {
        QualifiedElement::Vertex { partition, .. } | QualifiedElement::Edge { partition, .. } => {
            *partition
        }
    };
    data.rows.iter().all(|row| match row {
        QueryRow::PropertyMap { source, properties } if map_keys.is_some() => {
            valid_element(source, false)
                && (shapes[0].elements.len() != 1 || valid_start(source))
                && crate::selected_properties::valid_map(properties, map_keys.unwrap())
        }
        QueryRow::Element(e) if !scalar && !path && map_keys.is_none() => {
            valid_element(e, edge) && (shapes[0].elements.len() != 1 || valid_start(e))
        }
        QueryRow::Value {
            source: Some(e),
            value,
        } if scalar => {
            valid_element(e, edge)
                && (shapes[0].elements.len() != 1 || valid_start(e))
                && !matches!(value, Value::Real(v) if !v.is_finite())
                && (!label || matches!(value, Value::Text(_)))
                && (!id_projection || {
                    let id = match e {
                        QualifiedElement::Vertex { id, .. } | QualifiedElement::Edge { id, .. } => {
                            *id
                        }
                    };
                    matches!(value, Value::Integer(n) if i64::try_from(id).ok() == Some(*n))
                })
        }
        QueryRow::Path(elements) if path => {
            elements.first().is_some_and(valid_start)
                && shapes.iter().any(|shape| {
                    elements.len() == shape.elements.len()
                        && elements
                            .iter()
                            .zip(&shape.elements)
                            .all(|(e, edge)| valid_element(e, *edge))
                        && shape.owners.iter().all(|(edge, vertices)| {
                            vertices
                                .iter()
                                .any(|v| partition(&elements[*edge]) == partition(&elements[*v]))
                        })
                })
        }
        _ => false,
    })
}
