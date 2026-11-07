//! Private executor over the existing official-grammar AST. No wire builders,
//! text matching, tokenization, parser alternatives, or weak-read fallback.
use super::*;
use crate::remote::query::QualifiedElement;
use rocksdb_sql_table::experimental_graph::Direction;
use rocksdb_sql_table::{
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
fn parse_error(cause: rocksdb_sql_table::Error) -> Error {
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
                WireRow::Value { source, value } => QueryRow::Value {
                    source: source.map(Into::into),
                    value,
                },
                WireRow::Count(n) => QueryRow::Count(n),
            })
            .collect()
    })
}
