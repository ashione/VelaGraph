//! Standard Gremlin v0 parsing and execution.
//!
//! The text is parsed by [`gremlin_rs`], which is generated from Apache
//! TinkerPop's official `Gremlin.g4` grammar. This module only walks that parse
//! tree and maps the supported subset onto [`GraphEngine`]; it does not define
//! a second grammar or a dialect.
//!
//! The normative subset, semantics, limits, and errors live in
//! `docs/QUERY_SPEC.md`.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use gremlin_rs::{parse_query, ParseTree};

use crate::error::{Error, Result};
use crate::graph::{Edge, GraphEngine, Vertex};
use crate::value::Value;

/// Maximum number of `out`/`in`/`both` steps in one query.
pub const MAX_GREMLIN_EDGE_STEPS: usize = 3;
/// Maximum rows materialized before an explicit `limit` is reached.
pub const MAX_GREMLIN_ROWS: usize = 10_000;
/// Default result cap when a query has no explicit `limit`.
pub const DEFAULT_GREMLIN_LIMIT: usize = 100;
/// Maximum explicit `limit(n)`.
pub const MAX_GREMLIN_LIMIT: usize = 1_000;
/// Maximum accepted query text.
pub const MAX_GREMLIN_QUERY_BYTES: usize = 16 * 1024;
/// Wall-clock deadline for one query.
pub const GREMLIN_DEADLINE: Duration = Duration::from_secs(5);

// Rule numbers from the generated parser. They are part of the pinned
// `gremlin-rs` 0.1.0 grammar integration, not a replacement grammar.
const RULE_TRAVERSAL_SOURCE: usize = 3;
const RULE_ROOT_TRAVERSAL: usize = 5;
const RULE_SPAWN: usize = 14;
const RULE_SPAWN_E: usize = 17;
const RULE_SPAWN_V: usize = 18;
const RULE_CHAIN: usize = 26;
const RULE_METHOD: usize = 29;

const RULE_METHOD_BOTH: usize = 44;
const RULE_METHOD_COUNT: usize = 59;
const RULE_METHOD_HAS: usize = 79;
const RULE_METHOD_HAS_LABEL: usize = 82;
const RULE_METHOD_ID: usize = 85;
const RULE_METHOD_IN: usize = 87;
const RULE_METHOD_LABEL: usize = 95;
const RULE_METHOD_LIMIT: usize = 97;
const RULE_METHOD_OUT: usize = 117;
const RULE_METHOD_PATH: usize = 121;
const RULE_METHOD_VALUES: usize = 161;

/// Where a traversal starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GremlinStart {
    /// `V()` or `V(ids...)`. An empty vector means all vertices.
    Vertices(Vec<u64>),
    /// `E()` or `E(ids...)`. An empty vector means all edges.
    Edges(Vec<u64>),
}

/// One supported Gremlin step.
#[derive(Debug, Clone, PartialEq)]
pub enum GremlinStep {
    HasLabel(Vec<String>),
    Has { key: String, value: Option<Value> },
    Out(Option<String>),
    In(Option<String>),
    Both(Option<String>),
    Values(String),
    Id,
    Label,
    Limit(usize),
    Path,
    Count,
}

/// A validated Gremlin v0 query.
#[derive(Debug, Clone, PartialEq)]
pub struct GremlinQuery {
    start: GremlinStart,
    steps: Vec<GremlinStep>,
    explicit_limit: bool,
}

impl GremlinQuery {
    /// Parse and validate one Gremlin traversal.
    pub fn parse(input: &str) -> Result<Self> {
        if input.len() > MAX_GREMLIN_QUERY_BYTES {
            return Err(Error::GremlinParse(format!(
                "query text is {} bytes; the limit is {MAX_GREMLIN_QUERY_BYTES}",
                input.len()
            )));
        }
        if contains_unquoted_semicolon(input) {
            return Err(Error::GremlinParse(
                "multiple Gremlin traversals are not supported".into(),
            ));
        }
        let tree = parse_query(input).map_err(|error| Error::GremlinParse(error.to_string()))?;
        // `parse_query` can recover by stopping before invalid trailing input
        // instead of reporting a syntax error. Verify that the returned tree
        // covers the entire query before executing anything.
        if normalize_gremlin_text(input) != normalize_gremlin_text(&tree.text()) {
            return Err(Error::GremlinParse(
                "unrecognized or trailing input after the Gremlin traversal".into(),
            ));
        }

        // `parse_query` accepts a single `query`; require exactly one
        // `rootTraversal` child instead of accepting expression wrappers.
        let roots: Vec<&ParseTree> = tree
            .children()
            .iter()
            .filter(|child| rule_index(child) == Some(RULE_ROOT_TRAVERSAL))
            .collect();
        if roots.len() != 1 {
            return Err(Error::GremlinParse(
                "expected exactly one Gremlin traversal".into(),
            ));
        }
        let root = roots[0];
        // Terminal traversals (`next()`, `toList()`, ...) are siblings of the
        // root traversal in the generated grammar, not methods in its chain.
        // Reject them rather than executing only the part before the dot.
        for child in tree.children() {
            if rule_index(child) == Some(RULE_ROOT_TRAVERSAL) || child.text() == "." {
                continue;
            }
            return Err(Error::UnsupportedGremlinStep(child.text()));
        }
        if tree.children().iter().any(|child| child.text() == ".") {
            return Err(Error::GremlinParse(
                "trailing tokens after the Gremlin traversal".into(),
            ));
        }

        // The parse tree puts `g` (`traversal_source`), the spawn method
        // (`V`/`E`), and the chained traversal side by side under
        // `root_traversal`; the spawn node is not nested in the source node.
        root.children()
            .iter()
            .find(|child| rule_index(child) == Some(RULE_TRAVERSAL_SOURCE))
            .ok_or_else(|| Error::UnsupportedGremlinStep("g".into()))?;
        let spawn = root
            .children()
            .iter()
            .find(|child| rule_index(child) == Some(RULE_SPAWN))
            .ok_or_else(|| Error::UnsupportedGremlinStep("g".into()))?;
        // Reject terminal traversals such as `next()`/`toList()` and any
        // other root-level construct instead of silently running only `V()`.
        for child in root.children() {
            match rule_index(child) {
                Some(RULE_TRAVERSAL_SOURCE | RULE_SPAWN | RULE_CHAIN) => {}
                None if child.text() == "." => {}
                _ => return Err(Error::UnsupportedGremlinStep(child.text())),
            }
        }
        let source = root
            .children()
            .iter()
            .find(|child| rule_index(child) == Some(RULE_TRAVERSAL_SOURCE))
            .expect("source checked above");
        if source
            .children()
            .iter()
            .any(|child| rule_index(child).is_some())
        {
            return Err(Error::UnsupportedGremlinStep(source.text()));
        }
        let start_node = spawn
            .children()
            .iter()
            .find(|child| matches!(rule_index(child), Some(RULE_SPAWN_V | RULE_SPAWN_E)))
            .ok_or_else(|| Error::UnsupportedGremlinStep("g".into()))?;
        let start_rule = rule_index(start_node).expect("start node is a rule");
        let (start_name, start_args) = method_parts(start_node)?;
        let start = match start_rule {
            RULE_SPAWN_V => parse_start_ids("V", &start_name, &start_args, true)?,
            RULE_SPAWN_E => parse_start_ids("E", &start_name, &start_args, false)?,
            _ => unreachable!("rule matched above"),
        };

        let mut method_nodes = Vec::new();
        for child in root.children() {
            if rule_index(child) == Some(RULE_CHAIN) {
                collect_chain(child, &mut method_nodes)?;
            }
        }
        let mut steps = Vec::with_capacity(method_nodes.len());
        for node in method_nodes {
            steps.push(parse_step(node)?);
        }

        let edge_steps = steps
            .iter()
            .filter(|step| {
                matches!(
                    step,
                    GremlinStep::Out(_) | GremlinStep::In(_) | GremlinStep::Both(_)
                )
            })
            .count();
        if edge_steps > MAX_GREMLIN_EDGE_STEPS {
            return Err(Error::TraversalLimitExceeded(format!(
                "query has {edge_steps} edge steps; the limit is {MAX_GREMLIN_EDGE_STEPS}"
            )));
        }
        if let Some((index, _)) = steps.iter().enumerate().find(|(index, step)| {
            matches!(step, GremlinStep::Path | GremlinStep::Count) && *index + 1 != steps.len()
        }) {
            return Err(Error::UnsupportedGremlinArgument(format!(
                "{} must be the last step",
                match &steps[index] {
                    GremlinStep::Path => "path",
                    GremlinStep::Count => "count",
                    _ => unreachable!(),
                }
            )));
        }
        let explicit_limit = steps
            .iter()
            .any(|step| matches!(step, GremlinStep::Limit(_)));

        Ok(Self {
            start,
            steps,
            explicit_limit,
        })
    }

    /// The traversal start, after parsing `V`/`E` and their ids.
    pub fn start(&self) -> &GremlinStart {
        &self.start
    }

    /// The validated step sequence, in execution order.
    pub fn steps(&self) -> &[GremlinStep] {
        &self.steps
    }

    /// Execute this query against `graph`.
    pub fn execute(&self, graph: &GraphEngine) -> Result<GremlinResult> {
        let deadline = Instant::now() + GREMLIN_DEADLINE;
        self.execute_checked(graph, &|| {
            if Instant::now() >= deadline {
                Err(Error::TraversalLimitExceeded(
                    "query deadline exceeded".into(),
                ))
            } else {
                Ok(())
            }
        })
    }

    fn execute_checked(
        &self,
        graph: &GraphEngine,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<GremlinResult> {
        check()?;
        // A leading `limit` can be pushed into the start scan, so a large
        // graph does not fail the row cap before an explicit bound applies.
        let start_limit = match self.steps.first() {
            Some(GremlinStep::Limit(limit)) => Some(*limit),
            _ => None,
        };
        let mut rows = self.start_rows(graph, start_limit, check)?;
        enforce_row_cap(rows.len())?;

        for step in &self.steps {
            check()?;
            rows = apply_step(graph, rows, step, check)?;
            check()?;
            enforce_row_cap(rows.len())?;
        }

        // The default cap is a result cap, not a traversal cutoff; `count`
        // already observed the pre-cap rows it is supposed to count.
        if !self.explicit_limit
            && !rows.iter().any(|row| matches!(row, Row::Count(_)))
            && rows.len() > DEFAULT_GREMLIN_LIMIT
        {
            rows.truncate(DEFAULT_GREMLIN_LIMIT);
        }

        let rows = rows
            .into_iter()
            .map(|row| {
                check()?;
                Ok(row.into_row())
            })
            .collect::<Result<Vec<_>>>()?;
        check()?;
        Ok(GremlinResult { rows })
    }

    fn start_rows(
        &self,
        graph: &GraphEngine,
        limit: Option<usize>,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<Vec<Row>> {
        match &self.start {
            GremlinStart::Vertices(ids) if ids.is_empty() => {
                let scan_limit = limit.unwrap_or(MAX_GREMLIN_ROWS);
                let (vertices, truncated) =
                    graph.scan_vertices_checked(scan_limit, &mut || check())?;
                if truncated && limit.is_none() {
                    return Err(Error::TraversalLimitExceeded(format!(
                        "more than {MAX_GREMLIN_ROWS} vertices matched; add an explicit limit"
                    )));
                }
                Ok(vertices
                    .into_iter()
                    .map(|vertex| {
                        check()?;
                        let id = vertex.id;
                        Ok(Row::Element(
                            Element::Vertex(vertex),
                            vec![PathElement::Vertex(id)],
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?)
            }
            GremlinStart::Vertices(ids) => {
                let mut rows = Vec::with_capacity(ids.len().min(MAX_GREMLIN_ROWS));
                for id in ids {
                    check()?;
                    if let Some(vertex) = graph.get_vertex(*id)? {
                        check()?;
                        enforce_row_cap(rows.len() + 1)?;
                        rows.push(Row::Element(
                            Element::Vertex(vertex),
                            vec![PathElement::Vertex(*id)],
                        ));
                    }
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        break;
                    }
                }
                Ok(rows)
            }
            GremlinStart::Edges(ids) if ids.is_empty() => {
                let scan_limit = limit.unwrap_or(MAX_GREMLIN_ROWS);
                let (edges, truncated) = graph.scan_edges_checked(scan_limit, &mut || check())?;
                if truncated && limit.is_none() {
                    return Err(Error::TraversalLimitExceeded(format!(
                        "more than {MAX_GREMLIN_ROWS} edges matched; add an explicit limit"
                    )));
                }
                Ok(edges
                    .into_iter()
                    .map(|edge| {
                        check()?;
                        let id = edge.id;
                        Ok(Row::Element(
                            Element::Edge(edge),
                            vec![PathElement::Edge(id)],
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?)
            }
            GremlinStart::Edges(ids) => {
                let mut rows = Vec::with_capacity(ids.len().min(MAX_GREMLIN_ROWS));
                for id in ids {
                    check()?;
                    if let Some(edge) = graph.get_edge(*id)? {
                        check()?;
                        enforce_row_cap(rows.len() + 1)?;
                        rows.push(Row::Element(
                            Element::Edge(edge),
                            vec![PathElement::Edge(*id)],
                        ));
                    }
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        break;
                    }
                }
                Ok(rows)
            }
        }
    }
}

/// Query result: ordered rows.
#[derive(Debug, Clone, PartialEq)]
pub struct GremlinResult {
    pub rows: Vec<GremlinRow>,
}

/// One result row. These are the row shapes allowed by `QUERY_SPEC.md`.
#[derive(Debug, Clone, PartialEq)]
pub enum GremlinRow {
    Vertex(u64),
    Edge(u64),
    Value(Value),
    Path(Vec<PathElement>),
    Count(u64),
}

/// A vertex or edge id in a traversal path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathElement {
    Vertex(u64),
    Edge(u64),
}

#[derive(Debug, Clone, PartialEq)]
enum Element {
    Vertex(Vertex),
    Edge(Edge),
}

#[derive(Debug, Clone, PartialEq)]
enum Row {
    Element(Element, Vec<PathElement>),
    Value(Value),
    Path(Vec<PathElement>),
    Count(u64),
}

impl Row {
    fn into_row(self) -> GremlinRow {
        match self {
            Row::Element(Element::Vertex(vertex), _) => GremlinRow::Vertex(vertex.id),
            Row::Element(Element::Edge(edge), _) => GremlinRow::Edge(edge.id),
            Row::Value(value) => GremlinRow::Value(value),
            Row::Path(path) => GremlinRow::Path(path),
            Row::Count(count) => GremlinRow::Count(count),
        }
    }
}

fn apply_step(
    graph: &GraphEngine,
    rows: Vec<Row>,
    step: &GremlinStep,
    check: &dyn Fn() -> Result<()>,
) -> Result<Vec<Row>> {
    check()?;
    match step {
        GremlinStep::HasLabel(labels) => rows
            .into_iter()
            .map(|row| {
                check()?;
                match row {
                    Row::Element(element, path) => {
                        let matches = match &element {
                            Element::Vertex(vertex) => {
                                vertex.labels.iter().any(|label| labels.contains(label))
                            }
                            Element::Edge(edge) => labels.contains(&edge.label),
                        };
                        Ok(matches.then_some(Row::Element(element, path)))
                    }
                    _ => Err(invalid_traverser("hasLabel", "a vertex or edge")),
                }
            })
            .filter_map(Result::transpose)
            .collect(),
        GremlinStep::Has { key, value } => rows
            .into_iter()
            .map(|row| {
                check()?;
                match row {
                    Row::Element(element, path) => {
                        let properties = match &element {
                            Element::Vertex(vertex) => &vertex.properties,
                            Element::Edge(edge) => &edge.properties,
                        };
                        let matches = match value {
                            Some(expected) => properties.get(key) == Some(expected),
                            None => properties.contains_key(key),
                        };
                        Ok(matches.then_some(Row::Element(element, path)))
                    }
                    _ => Err(invalid_traverser("has", "a vertex or edge")),
                }
            })
            .filter_map(Result::transpose)
            .collect(),
        GremlinStep::Out(label) => expand(graph, rows, Direction::Out, label.as_deref(), check),
        GremlinStep::In(label) => expand(graph, rows, Direction::In, label.as_deref(), check),
        GremlinStep::Both(label) => expand(graph, rows, Direction::Both, label.as_deref(), check),
        GremlinStep::Values(key) => {
            let mut next = Vec::new();
            for row in rows {
                check()?;
                let Row::Element(element, _) = row else {
                    return Err(invalid_traverser("values", "a vertex or edge"));
                };
                let properties = match &element {
                    Element::Vertex(vertex) => &vertex.properties,
                    Element::Edge(edge) => &edge.properties,
                };
                if let Some(value) = properties.get(key) {
                    next.push(Row::Value(value.clone()));
                }
            }
            Ok(next)
        }
        GremlinStep::Id => rows
            .into_iter()
            .map(|row| {
                check()?;
                match row {
                    Row::Element(element, _) => {
                        let id = match element {
                            Element::Vertex(vertex) => vertex.id,
                            Element::Edge(edge) => edge.id,
                        };
                        let id = i64::try_from(id).map_err(|_| {
                            Error::UnsupportedGremlinValue(format!(
                                "id {id} does not fit the v0 integer row type"
                            ))
                        })?;
                        Ok(Row::Value(Value::Integer(id)))
                    }
                    _ => Err(invalid_traverser("id", "a vertex or edge")),
                }
            })
            .collect(),
        GremlinStep::Label => rows
            .into_iter()
            .map(|row| {
                check()?;
                match row {
                    Row::Element(element, _) => {
                        let label = match element {
                            Element::Vertex(vertex) => {
                                vertex.labels.first().cloned().unwrap_or_default()
                            }
                            Element::Edge(edge) => edge.label,
                        };
                        Ok(Row::Value(Value::Text(label)))
                    }
                    _ => Err(invalid_traverser("label", "a vertex or edge")),
                }
            })
            .collect(),
        GremlinStep::Limit(limit) => {
            let mut rows = rows;
            rows.truncate(*limit);
            Ok(rows)
        }
        GremlinStep::Path => rows
            .into_iter()
            .map(|row| {
                check()?;
                match row {
                    Row::Element(_, path) => Ok(Row::Path(path)),
                    _ => Err(invalid_traverser("path", "a vertex or edge")),
                }
            })
            .collect(),
        GremlinStep::Count => Ok(vec![Row::Count(rows.len() as u64)]),
    }
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Out,
    In,
    Both,
}

fn expand(
    graph: &GraphEngine,
    rows: Vec<Row>,
    direction: Direction,
    label: Option<&str>,
    check: &dyn Fn() -> Result<()>,
) -> Result<Vec<Row>> {
    let mut next = Vec::new();
    for row in rows {
        check()?;
        let Row::Element(Element::Vertex(vertex), path) = row else {
            return Err(invalid_traverser(
                "out/in/both",
                "a vertex (not an edge or value)",
            ));
        };
        // Adjacency keys are label-first, not edge-id-first. Keep only bounded
        // output rows, ordered by edge id, rather than materializing all edges.
        // The map also collapses a self-loop seen in both directions.
        let mut adjacent = BTreeMap::new();
        let mut visit = |edge: Edge| -> Result<()> {
            check()?;
            if adjacent.contains_key(&edge.id) {
                return Ok(());
            }
            let target_id = match direction {
                Direction::Out => edge.to,
                Direction::In => edge.from,
                Direction::Both if edge.from == vertex.id => edge.to,
                Direction::Both => edge.from,
            };
            let Some(target) = graph.get_vertex(target_id)? else {
                return Ok(());
            };
            check()?;
            enforce_row_cap(next.len() + adjacent.len() + 1)?;
            let mut next_path = path.clone();
            next_path.push(PathElement::Edge(edge.id));
            next_path.push(PathElement::Vertex(target_id));
            adjacent.insert(edge.id, Row::Element(Element::Vertex(target), next_path));
            Ok(())
        };
        if matches!(direction, Direction::Out | Direction::Both) {
            graph.visit_adjacent_edges(vertex.id, false, label, &mut || check(), &mut visit)?;
        }
        if matches!(direction, Direction::In | Direction::Both) {
            graph.visit_adjacent_edges(vertex.id, true, label, &mut || check(), &mut visit)?;
        }
        for (_, row) in adjacent {
            check()?;
            next.push(row);
        }
    }
    check()?;
    Ok(next)
}

fn invalid_traverser(step: &str, expected: &str) -> Error {
    Error::InvalidGremlinTraverser(format!("`{step}` requires {expected}"))
}

fn enforce_row_cap(rows: usize) -> Result<()> {
    if rows > MAX_GREMLIN_ROWS {
        return Err(Error::TraversalLimitExceeded(format!(
            "query produced {rows} rows; the limit is {MAX_GREMLIN_ROWS}"
        )));
    }
    Ok(())
}

fn parse_start_ids(
    expected: &str,
    name: &str,
    args: &[String],
    vertices: bool,
) -> Result<GremlinStart> {
    if name != expected {
        return Err(Error::UnsupportedGremlinStep(name.to_string()));
    }
    let mut ids = Vec::with_capacity(args.len());
    for arg in args {
        let value = parse_literal(arg)?;
        let Value::Integer(id) = value else {
            return Err(Error::UnsupportedGremlinArgument(format!(
                "{expected}() ids must be integers"
            )));
        };
        if id < 0 {
            return Err(Error::UnsupportedGremlinArgument(format!(
                "{expected}() ids must be non-negative"
            )));
        }
        ids.push(id as u64);
    }
    Ok(if vertices {
        GremlinStart::Vertices(ids)
    } else {
        GremlinStart::Edges(ids)
    })
}

fn parse_step(node: &ParseTree) -> Result<GremlinStep> {
    let children = node.children();
    let specific = children
        .iter()
        .find(|child| rule_index(child).is_some_and(|index| index >= 30))
        .ok_or_else(|| Error::UnsupportedGremlinStep(node.text()))?;
    let rule = rule_index(specific).expect("specific rule has an index");
    let (name, args) = method_parts(specific)?;
    match rule {
        RULE_METHOD_HAS_LABEL => {
            if args.is_empty() {
                return Err(Error::UnsupportedGremlinArgument(
                    "hasLabel requires at least one label".into(),
                ));
            }
            let labels = args
                .iter()
                .map(|arg| parse_text_literal(arg))
                .collect::<Result<Vec<_>>>()?;
            for label in &labels {
                validate_label(label)?;
            }
            Ok(GremlinStep::HasLabel(labels))
        }
        RULE_METHOD_HAS => {
            let key = args
                .first()
                .ok_or_else(|| {
                    Error::UnsupportedGremlinArgument("has requires a property key".into())
                })
                .and_then(|arg| parse_text_literal(arg))?;
            let value = match args.get(1) {
                Some(arg) => Some(parse_literal(arg)?),
                None => None,
            };
            if args.len() > 2 {
                return Err(Error::UnsupportedGremlinArgument(
                    "has accepts only a key and an optional value".into(),
                ));
            }
            Ok(GremlinStep::Has { key, value })
        }
        RULE_METHOD_OUT => Ok(GremlinStep::Out(parse_optional_label("out", &args)?)),
        RULE_METHOD_IN => Ok(GremlinStep::In(parse_optional_label("in", &args)?)),
        RULE_METHOD_BOTH => Ok(GremlinStep::Both(parse_optional_label("both", &args)?)),
        RULE_METHOD_VALUES => {
            if args.len() != 1 {
                return Err(Error::UnsupportedGremlinArgument(
                    "values requires exactly one property key".into(),
                ));
            }
            Ok(GremlinStep::Values(parse_text_literal(args[0].as_str())?))
        }
        RULE_METHOD_ID => {
            require_no_args("id", &args)?;
            Ok(GremlinStep::Id)
        }
        RULE_METHOD_LABEL => {
            require_no_args("label", &args)?;
            Ok(GremlinStep::Label)
        }
        RULE_METHOD_LIMIT => {
            if args.len() != 1 {
                return Err(Error::UnsupportedGremlinArgument(
                    "limit requires exactly one integer".into(),
                ));
            }
            let Value::Integer(limit) = parse_literal(args[0].as_str())? else {
                return Err(Error::UnsupportedGremlinArgument(
                    "limit requires an integer literal".into(),
                ));
            };
            if !(1..=MAX_GREMLIN_LIMIT as i64).contains(&limit) {
                return Err(Error::InvalidGremlinLimit(limit));
            }
            Ok(GremlinStep::Limit(limit as usize))
        }
        RULE_METHOD_PATH => {
            require_no_args("path", &args)?;
            Ok(GremlinStep::Path)
        }
        RULE_METHOD_COUNT => {
            require_no_args("count", &args)?;
            Ok(GremlinStep::Count)
        }
        _ => Err(Error::UnsupportedGremlinStep(name)),
    }
}

fn parse_optional_label(step: &str, args: &[String]) -> Result<Option<String>> {
    if args.len() > 1 {
        return Err(Error::UnsupportedGremlinArgument(format!(
            "{step} accepts at most one label in v0"
        )));
    }
    let label = match args.first() {
        Some(arg) => Some(parse_text_literal(arg)?),
        None => None,
    };
    if let Some(label) = &label {
        validate_label(label)?;
    }
    Ok(label)
}

fn require_no_args(step: &str, args: &[String]) -> Result<()> {
    if args.is_empty() {
        Ok(())
    } else {
        Err(Error::UnsupportedGremlinArgument(format!(
            "{step} takes no arguments"
        )))
    }
}

fn method_parts(node: &ParseTree) -> Result<(String, Vec<String>)> {
    let text = node.text();
    let (name, rest) = text.split_once('(').ok_or_else(|| {
        Error::UnsupportedGremlinArgument(format!("cannot read method from `{text}`"))
    })?;
    let args = rest.strip_suffix(')').ok_or_else(|| {
        Error::UnsupportedGremlinArgument(format!("unterminated argument list in `{text}`"))
    })?;
    let name = name.trim().to_string();
    let args = split_args(args)?;
    Ok((name, args))
}

fn split_args(input: &str) -> Result<Vec<String>> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(Vec::new());
    }
    let mut args = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut escaped = false;
    let mut depth = 0usize;
    for (index, ch) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match quote {
            Some(_delimiter) if ch == '\\' => escaped = true,
            Some(delimiter) if ch == delimiter => quote = None,
            Some(_) => {}
            None => match ch {
                '\'' | '"' => quote = Some(ch),
                '(' | '[' | '{' => depth += 1,
                ')' | ']' | '}' => {
                    depth = depth.checked_sub(1).ok_or_else(|| {
                        Error::UnsupportedGremlinArgument("unbalanced argument delimiters".into())
                    })?;
                }
                ',' if depth == 0 => {
                    let arg = input[start..index].trim();
                    if arg.is_empty() {
                        return Err(Error::UnsupportedGremlinArgument("empty argument".into()));
                    }
                    args.push(arg.to_string());
                    start = index + ch.len_utf8();
                }
                _ => {}
            },
        }
    }
    if quote.is_some() || depth != 0 {
        return Err(Error::UnsupportedGremlinArgument(
            "unterminated argument".into(),
        ));
    }
    let arg = input[start..].trim();
    if arg.is_empty() {
        return Err(Error::UnsupportedGremlinArgument("empty argument".into()));
    }
    args.push(arg.to_string());
    Ok(args)
}

fn parse_text_literal(input: &str) -> Result<String> {
    match parse_literal(input)? {
        Value::Text(text) => Ok(text),
        _ => Err(Error::UnsupportedGremlinArgument(format!(
            "expected a string literal, got `{input}`"
        ))),
    }
}

fn parse_literal(input: &str) -> Result<Value> {
    let input = input.trim();
    if input.is_empty() {
        return Err(Error::UnsupportedGremlinValue("empty literal".into()));
    }
    if let Some(rest) = input.strip_prefix('\'') {
        return parse_string(rest, '\'');
    }
    if let Some(rest) = input.strip_prefix('"') {
        return parse_string(rest, '"');
    }
    match input {
        "true" => return Ok(Value::Boolean(true)),
        "false" => return Ok(Value::Boolean(false)),
        "null" => return Ok(Value::Null),
        _ => {}
    }
    if input.starts_with('[') || input.starts_with('{') {
        return Err(Error::UnsupportedGremlinValue(format!(
            "collection literal `{input}` is not supported in v0"
        )));
    }
    if let Some(integer) = input.strip_suffix('L').or_else(|| input.strip_suffix('l')) {
        return integer
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|_| Error::UnsupportedGremlinValue(input.into()));
    }
    if input.contains(['.', 'e', 'E']) {
        let value: f64 = input
            .parse()
            .map_err(|_| Error::UnsupportedGremlinValue(input.into()))?;
        if !value.is_finite() {
            return Err(Error::UnsupportedGremlinValue(format!(
                "non-finite number `{input}`"
            )));
        }
        Ok(Value::Real(value))
    } else {
        input
            .parse::<i64>()
            .map(Value::Integer)
            .map_err(|_| Error::UnsupportedGremlinValue(input.into()))
    }
}

fn parse_string(input: &str, delimiter: char) -> Result<Value> {
    let body = input
        .strip_suffix(delimiter)
        .ok_or_else(|| Error::UnsupportedGremlinValue(input.into()))?;
    let mut text = String::with_capacity(body.len());
    let mut chars = body.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            text.push(ch);
            continue;
        }
        let escaped = chars
            .next()
            .ok_or_else(|| Error::UnsupportedGremlinValue(input.into()))?;
        match escaped {
            'n' => text.push('\n'),
            'r' => text.push('\r'),
            't' => text.push('\t'),
            '\\' => text.push('\\'),
            '\'' => text.push('\''),
            '"' => text.push('"'),
            other => {
                text.push('\\');
                text.push(other);
            }
        }
    }
    Ok(Value::Text(text))
}

fn collect_chain<'a>(node: &'a ParseTree, out: &mut Vec<&'a ParseTree>) -> Result<()> {
    for child in node.children() {
        match rule_index(child) {
            Some(RULE_CHAIN) => collect_chain(child, out)?,
            Some(RULE_METHOD) => out.push(child),
            None if child.text() == "." => {}
            _ => return Err(Error::UnsupportedGremlinStep(child.text())),
        }
    }
    Ok(())
}

fn normalize_gremlin_text(input: &str) -> String {
    let mut normalized = String::with_capacity(input.len());
    let mut quote = None;
    let mut escaped = false;
    for ch in input.chars() {
        if escaped {
            normalized.push(ch);
            escaped = false;
            continue;
        }
        match quote {
            Some(_delimiter) if ch == '\\' => {
                normalized.push(ch);
                escaped = true;
            }
            Some(delimiter) if ch == delimiter => {
                normalized.push(ch);
                quote = None;
            }
            Some(_) => normalized.push(ch),
            None => match ch {
                '\'' | '"' => {
                    quote = Some(ch);
                    normalized.push(ch);
                }
                ch if ch.is_whitespace() => {}
                ch => normalized.push(ch),
            },
        }
    }
    normalized
}

fn contains_unquoted_semicolon(input: &str) -> bool {
    let mut quote = None;
    let mut escaped = false;
    for ch in input.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        match quote {
            Some(_delimiter) if ch == '\\' => escaped = true,
            Some(delimiter) if ch == delimiter => quote = None,
            Some(_) => {}
            None => match ch {
                '\'' | '"' => quote = Some(ch),
                ';' => return true,
                _ => {}
            },
        }
    }
    false
}

fn rule_index(node: &ParseTree) -> Option<usize> {
    match node {
        ParseTree::Rule(rule) => Some(rule.context().rule_index()),
        ParseTree::Terminal(_) | ParseTree::Error(_) => None,
    }
}

fn validate_label(label: &str) -> Result<()> {
    let valid_length = !label.is_empty() && label.len() <= 64;
    let valid_bytes = label.bytes().enumerate().all(|(index, byte)| {
        byte.is_ascii_alphabetic() || byte == b'_' || (index > 0 && byte.is_ascii_digit())
    });
    if valid_length && valid_bytes {
        Ok(())
    } else {
        Err(Error::InvalidGraphLabel(label.to_string()))
    }
}

impl GraphEngine {
    /// Parse and execute one standard Gremlin v0 traversal.
    pub fn gremlin(&self, query: &str) -> Result<GremlinResult> {
        GremlinQuery::parse(query)?.execute(self)
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn deadline_failure_at_every_checkpoint_never_returns_partial_success() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        let mut vertex = Vertex::new(1);
        vertex.labels.push("person".into());
        vertex
            .properties
            .insert("name".into(), Value::Text("A".into()));
        graph.create_vertex(&vertex).unwrap();
        graph.create_edge(&Edge::new(1, 1, 1, "LOOP")).unwrap();
        for text in [
            "g.V()",
            "g.E()",
            "g.V(1).both().path()",
            "g.V(1).out().count()",
            "g.V(1).hasLabel('person').has('name').values('name')",
            "g.E(1).id()",
            "g.V(1).label()",
            "g.V(1).limit(1)",
        ] {
            let query = GremlinQuery::parse(text).unwrap();
            let calls = Cell::new(0);
            query
                .execute_checked(&graph, &|| {
                    calls.set(calls.get() + 1);
                    Ok(())
                })
                .unwrap();
            // Fail every checkpoint, including final-step and final result checks.
            // No sleep or dependence on thread scheduling / wall-clock timing.
            for fail_at in 1..=calls.get() {
                let current = Cell::new(0);
                let result = query.execute_checked(&graph, &|| {
                    current.set(current.get() + 1);
                    if current.get() == fail_at {
                        Err(Error::TraversalLimitExceeded("injected deadline".into()))
                    } else {
                        Ok(())
                    }
                });
                assert!(
                    matches!(result, Err(Error::TraversalLimitExceeded(_))),
                    "{text}, checkpoint {fail_at}"
                );
                assert_eq!(
                    current.get(),
                    fail_at,
                    "execution continued after cancellation"
                );
            }
        }
    }

    #[test]
    fn explicit_id_start_checks_cap_before_appending() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        graph.create_vertex(&Vertex::new(1)).unwrap();
        let query = GremlinQuery {
            start: GremlinStart::Vertices(vec![1; MAX_GREMLIN_ROWS + 1]),
            steps: vec![],
            explicit_limit: false,
        };
        assert!(matches!(
            query.execute(&graph),
            Err(Error::TraversalLimitExceeded(_))
        ));
        let bounded = GremlinQuery {
            steps: vec![GremlinStep::Limit(1)],
            explicit_limit: true,
            ..query
        };
        assert_eq!(
            bounded.execute(&graph).unwrap().rows,
            vec![GremlinRow::Vertex(1)]
        );
    }
}
