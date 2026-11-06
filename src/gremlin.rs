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
use crate::query_memory::{self, Accounted, AccountedVec, Budget};
use crate::value::Value;

/// Query-only reads from one stable sequence. Implementations must never mix
/// live reads into a view, silently truncate an internal cap, or collect an
/// unbounded adjacency list. This is not a distributed storage-node contract.
pub(crate) trait QueryReadBackend {
    fn get_vertex(&self, id: u64, budget: &Budget) -> Result<Option<Accounted<Vertex>>>;
    fn get_edge(&self, id: u64, budget: &Budget) -> Result<Option<Accounted<Edge>>>;
    fn scan_vertices_checked(
        &self,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
        budget: &Budget,
    ) -> Result<(AccountedVec<Accounted<Vertex>>, bool)>;
    fn scan_edges_checked(
        &self,
        limit: usize,
        check: &mut impl FnMut() -> Result<()>,
        budget: &Budget,
    ) -> Result<(AccountedVec<Accounted<Edge>>, bool)>;
    fn visit_adjacent_edges(
        &self,
        vertex_id: u64,
        incoming: bool,
        label: Option<&str>,
        check: &mut impl FnMut() -> Result<()>,
        visit: &mut impl FnMut(Accounted<Edge>) -> Result<()>,
        budget: &Budget,
    ) -> Result<()>;
}

/// Default per-execution logical byte budget; not an allocator or RSS limit.
pub const MAX_GREMLIN_ACCOUNTED_BYTES: usize = 64 * 1024 * 1024;

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

    /// Execute this query against one snapshot of `graph`.
    /// Concurrent commits become visible only to subsequent executions.
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
        let view = graph.read_view();
        self.execute_on_read(&view, check)
    }

    /// Execute against one newly acquired D1 graph view. Its acquisition, including
    /// a replicated leader barrier, is included in the cooperative deadline.
    pub fn execute_records<S: crate::StorageNode>(
        &self,
        graph: &crate::GraphRecordStore<S>,
    ) -> Result<GremlinResult> {
        let deadline = Instant::now() + GREMLIN_DEADLINE;
        let view = graph.read_view()?;
        self.execute_on_read(&view, &|| check_deadline(deadline))
    }

    /// Execute against a retained D1 view without acquiring another read barrier.
    /// The originating adapter still enforces execution context and epoch fencing.
    pub fn execute_record_view<S: crate::StorageNode>(
        &self,
        view: &crate::GraphRecordReadView<'_, S>,
    ) -> Result<GremlinResult> {
        let deadline = Instant::now() + GREMLIN_DEADLINE;
        self.execute_on_read(view, &|| check_deadline(deadline))
    }

    // The backend represents one stable read view for the entire execution.
    fn execute_on_read(
        &self,
        graph: &impl QueryReadBackend,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<GremlinResult> {
        self.execute_on_read_budget(graph, check, &Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES))
    }

    pub(crate) fn execute_on_read_budget(
        &self,
        graph: &impl QueryReadBackend,
        check: &dyn Fn() -> Result<()>,
        budget: &Budget,
    ) -> Result<GremlinResult> {
        let _control = budget.reserve(query_memory::CONTROL_BYTES)?;
        check()?;
        // A leading `limit` can be pushed into the start scan, so a large
        // graph does not fail the row cap before an explicit bound applies.
        let start_limit = match self.steps.first() {
            Some(GremlinStep::Limit(limit)) => Some(*limit),
            _ => None,
        };
        let mut rows = self.start_rows(graph, start_limit, check, budget)?;
        enforce_row_cap(rows.len())?;

        for step in &self.steps {
            check()?;
            rows = apply_step(graph, rows, step, check, budget)?;
            check()?;
            enforce_row_cap(rows.len())?;
        }

        // The default cap is a result cap, not a traversal cutoff; `count`
        // already observed the pre-cap rows it is supposed to count.
        if !self.explicit_limit
            && !rows.iter().any(|row| matches!(row.value, Row::Count(_)))
            && rows.len() > DEFAULT_GREMLIN_LIMIT
        {
            rows.truncate(DEFAULT_GREMLIN_LIMIT);
        }

        // The input iterator keeps its container charge while the output Vec
        // grows; payload reservations move with the rows, never cloning values.
        let mut output = AccountedVec::new(budget);
        let mut charges = AccountedVec::new(budget);
        for row in rows {
            check()?;
            let Accounted { value, mut charge } = row;
            let value = value.into_row();
            let bytes = result_payload_bytes(&value)?;
            charge.shrink(bytes);
            output.push(value)?;
            charges.push(charge)?;
        }
        check()?;
        let (rows, _slots) = output.into_parts();
        // Successful return transfers payload ownership out of the query budget.
        Ok(GremlinResult { rows })
    }

    fn start_rows(
        &self,
        graph: &impl QueryReadBackend,
        limit: Option<usize>,
        check: &dyn Fn() -> Result<()>,
        budget: &Budget,
    ) -> Result<AccountedVec<Accounted<Row>>> {
        let mut rows = AccountedVec::new(budget);
        match &self.start {
            GremlinStart::Vertices(ids) if ids.is_empty() => {
                let (vertices, truncated) = graph.scan_vertices_checked(
                    limit.unwrap_or(MAX_GREMLIN_ROWS),
                    &mut || check(),
                    budget,
                )?;
                if truncated && limit.is_none() {
                    return Err(Error::TraversalLimitExceeded(
                        "vertex row cap exceeded; add an explicit limit".into(),
                    ));
                }
                for vertex in vertices {
                    check()?;
                    rows.push(vertex_row(vertex, budget)?)?;
                }
            }
            GremlinStart::Vertices(ids) => {
                for id in ids {
                    check()?;
                    if let Some(vertex) = graph.get_vertex(*id, budget)? {
                        check()?;
                        enforce_row_cap(rows.len() + 1)?;
                        rows.push(vertex_row(vertex, budget)?)?;
                    }
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        break;
                    }
                }
            }
            GremlinStart::Edges(ids) if ids.is_empty() => {
                let (edges, truncated) = graph.scan_edges_checked(
                    limit.unwrap_or(MAX_GREMLIN_ROWS),
                    &mut || check(),
                    budget,
                )?;
                if truncated && limit.is_none() {
                    return Err(Error::TraversalLimitExceeded(
                        "edge row cap exceeded; add an explicit limit".into(),
                    ));
                }
                for edge in edges {
                    check()?;
                    rows.push(edge_row(edge)?)?;
                }
            }
            GremlinStart::Edges(ids) => {
                for id in ids {
                    check()?;
                    if let Some(edge) = graph.get_edge(*id, budget)? {
                        check()?;
                        enforce_row_cap(rows.len() + 1)?;
                        rows.push(edge_row(edge)?)?;
                    }
                    if limit.is_some_and(|limit| rows.len() >= limit) {
                        break;
                    }
                }
            }
        }
        Ok(rows)
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

fn vertex_row(mut vertex: Accounted<Vertex>, _budget: &Budget) -> Result<Accounted<Row>> {
    vertex.charge.grow(query_memory::PATH_ITEM)?;
    let id = vertex.id;
    Ok(vertex.map(|v| Row::Element(Element::Vertex(v), vec![PathElement::Vertex(id)])))
}
fn edge_row(mut edge: Accounted<Edge>) -> Result<Accounted<Row>> {
    edge.charge.grow(query_memory::PATH_ITEM)?;
    let id = edge.id;
    Ok(edge.map(|e| Row::Element(Element::Edge(e), vec![PathElement::Edge(id)])))
}
fn value_bytes(value: &Value) -> usize {
    match value {
        Value::Text(s) => s.len(),
        _ => 0,
    }
}
fn result_payload_bytes(row: &GremlinRow) -> Result<usize> {
    match row {
        GremlinRow::Value(v) => Ok(value_bytes(v)),
        GremlinRow::Path(p) => query_memory::mul(p.len(), query_memory::PATH_ITEM),
        _ => Ok(0),
    }
}
fn apply_step(
    graph: &impl QueryReadBackend,
    rows: AccountedVec<Accounted<Row>>,
    step: &GremlinStep,
    check: &dyn Fn() -> Result<()>,
    budget: &Budget,
) -> Result<AccountedVec<Accounted<Row>>> {
    check()?;
    match step {
        GremlinStep::Out(label) => {
            return expand(graph, rows, Direction::Out, label.as_deref(), check, budget)
        }
        GremlinStep::In(label) => {
            return expand(graph, rows, Direction::In, label.as_deref(), check, budget)
        }
        GremlinStep::Both(label) => {
            return expand(
                graph,
                rows,
                Direction::Both,
                label.as_deref(),
                check,
                budget,
            )
        }
        GremlinStep::Limit(limit) => {
            let mut rows = rows;
            rows.truncate(*limit);
            return Ok(rows);
        }
        GremlinStep::Count => {
            let count = rows.len() as u64;
            drop(rows);
            let mut output = AccountedVec::new(budget);
            output.push(Accounted::new(Row::Count(count), budget.reserve(0)?))?;
            return Ok(output);
        }
        _ => {}
    }
    let mut output = AccountedVec::new(budget);
    for row in rows {
        check()?;
        let Accounted {
            value: row,
            mut charge,
        } = row;
        let Row::Element(element, path) = row else {
            return Err(invalid_traverser(
                "has/values/id/label/path",
                "a vertex or edge",
            ));
        };
        let next = match step {
            GremlinStep::HasLabel(labels) => {
                let matches = match &element {
                    Element::Vertex(v) => v.labels.iter().any(|l| labels.contains(l)),
                    Element::Edge(e) => labels.contains(&e.label),
                };
                matches.then_some(Row::Element(element, path))
            }
            GremlinStep::Has { key, value } => {
                let properties = match &element {
                    Element::Vertex(v) => &v.properties,
                    Element::Edge(e) => &e.properties,
                };
                let matches = match value {
                    Some(v) => properties.get(key) == Some(v),
                    None => properties.contains_key(key),
                };
                matches.then_some(Row::Element(element, path))
            }
            GremlinStep::Values(key) => {
                // Move the value out; never temporarily duplicate a large Text.
                let mut properties = match element {
                    Element::Vertex(v) => v.properties,
                    Element::Edge(e) => e.properties,
                };
                let value = properties.remove(key);
                drop(properties);
                drop(path);
                charge.shrink(value.as_ref().map_or(0, value_bytes));
                value.map(Row::Value)
            }
            GremlinStep::Id => {
                let id = match element {
                    Element::Vertex(v) => v.id,
                    Element::Edge(e) => e.id,
                };
                let id = i64::try_from(id).map_err(|_| {
                    Error::UnsupportedGremlinValue(format!(
                        "id {id} does not fit the v0 integer row type"
                    ))
                })?;
                drop(path);
                charge.shrink(0);
                Some(Row::Value(Value::Integer(id)))
            }
            GremlinStep::Label => {
                let label = match element {
                    Element::Vertex(v) => v.labels.into_iter().next().unwrap_or_default(),
                    Element::Edge(e) => e.label,
                };
                drop(path);
                charge.shrink(label.len());
                Some(Row::Value(Value::Text(label)))
            }
            GremlinStep::Path => {
                drop(element);
                charge.shrink(query_memory::mul(path.len(), query_memory::PATH_ITEM)?);
                Some(Row::Path(path))
            }
            _ => unreachable!("handled above"),
        };
        if let Some(value) = next {
            output.push(Accounted::new(value, charge))?;
        }
    }
    Ok(output)
}

#[derive(Debug, Clone, Copy)]
enum Direction {
    Out,
    In,
    Both,
}

fn expand(
    graph: &impl QueryReadBackend,
    rows: AccountedVec<Accounted<Row>>,
    direction: Direction,
    label: Option<&str>,
    check: &dyn Fn() -> Result<()>,
    budget: &Budget,
) -> Result<AccountedVec<Accounted<Row>>> {
    let mut next = AccountedVec::new(budget);
    for source in rows {
        check()?;
        let Row::Element(Element::Vertex(vertex), path) = &source.value else {
            return Err(invalid_traverser(
                "out/in/both",
                "a vertex (not an edge or value)",
            ));
        };
        // Input, current source, adjacent map and next rows all keep their own
        // reservations alive throughout this overlap.
        let mut adjacent = BTreeMap::new();
        let mut visit = |edge: Accounted<Edge>| -> Result<()> {
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
            let Some(mut target) = graph.get_vertex(target_id, budget)? else {
                return Ok(());
            };
            check()?;
            enforce_row_cap(next.len() + adjacent.len() + 1)?;
            let len = query_memory::add(path.len(), 2)?;
            target
                .charge
                .grow(query_memory::mul(len, query_memory::PATH_ITEM)?)?;
            let map_slot = budget.reserve(query_memory::MAP_ENTRY)?;
            let mut next_path = Vec::with_capacity(len);
            next_path.extend_from_slice(path);
            next_path.push(PathElement::Edge(edge.id));
            next_path.push(PathElement::Vertex(target_id));
            let row = target.map(|v| Row::Element(Element::Vertex(v), next_path));
            adjacent.insert(edge.id, (row, map_slot));
            Ok(())
        };
        if matches!(direction, Direction::Out | Direction::Both) {
            graph.visit_adjacent_edges(
                vertex.id,
                false,
                label,
                &mut || check(),
                &mut visit,
                budget,
            )?;
        }
        if matches!(direction, Direction::In | Direction::Both) {
            graph.visit_adjacent_edges(
                vertex.id,
                true,
                label,
                &mut || check(),
                &mut visit,
                budget,
            )?;
        }
        // BTreeMap's allocator node packing is excluded; the defined per-entry
        // charge moves out and is released only after the output slot is reserved.
        for (_, (row, _map_slot)) in adjacent {
            check()?;
            next.push(row)?;
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

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    use std::cell::Cell;
    use std::sync::Barrier;

    fn populate(graph: &GraphEngine) {
        for id in 1..=3 {
            let mut vertex = Vertex::new(id);
            vertex.labels.push("old".into());
            vertex
                .properties
                .insert("version".into(), Value::Integer(0));
            graph.create_vertex(&vertex).unwrap();
        }
        for (id, from, to, label) in [(9, 1, 2, "A"), (3, 1, 3, "Z"), (5, 2, 3, "A")] {
            graph.create_edge(&Edge::new(id, from, to, label)).unwrap();
        }
    }

    fn replace_graph(graph: &GraphEngine) {
        let mut transaction = graph.begin().unwrap();
        for id in 1..=3 {
            transaction.delete_vertex(id).unwrap();
        }
        for id in 1..=4 {
            let mut vertex = Vertex::new(id);
            vertex.labels.push("new".into());
            vertex
                .properties
                .insert("version".into(), Value::Integer(1));
            transaction.create_vertex(&vertex).unwrap();
        }
        transaction.create_edge(&Edge::new(3, 1, 4, "NEW")).unwrap();
        transaction.commit().unwrap();
    }

    #[test]
    fn old_view_keeps_points_scans_adjacency_and_properties_at_one_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        populate(&graph);
        let view = graph.read_view();
        let sequence = view.sequence();
        let texts = [
            "g.V().values('version')",
            "g.V(3,1,1,99).label()",
            "g.E().label()",
            "g.E(9,3,9,99).id()",
            "g.V(1).out().path()",
            "g.V(3).in('A').id()",
            "g.V(2).both().id()",
            "g.V().limit(2).values('version')",
            "g.E().limit(2).id()",
            "g.V().hasLabel('old').has('version',0).count()",
            "g.V(1).out().out().values('version')",
        ];
        let queries: Vec<_> = texts
            .iter()
            .map(|text| GremlinQuery::parse(text).unwrap())
            .collect();
        let before: Vec<_> = queries
            .iter()
            .map(|q| q.execute_on_read(&view, &|| Ok(())).unwrap())
            .collect();
        std::thread::scope(|scope| scope.spawn(|| replace_graph(&graph)).join().unwrap());
        assert_eq!(view.sequence(), sequence);
        assert!(graph.read_view().sequence() > sequence);
        for (query, expected) in queries.iter().zip(before) {
            assert_eq!(query.execute_on_read(&view, &|| Ok(())).unwrap(), expected);
            assert_ne!(query.execute(&graph).unwrap(), expected);
        }
    }

    #[test]
    fn concurrent_commit_at_every_execution_checkpoint_cannot_mix_snapshots() {
        // Synchronize the writer before continuing at each checkpoint; neither
        // sleeps nor a lucky thread schedule can make this test pass.
        for text in [
            "g.V().values('version')",
            "g.E().label()",
            "g.V(3,1,1).label()",
            "g.E(9,3,9).label()",
            "g.V(1).out().out().values('version')",
            "g.V(3).in('A').values('version')",
            "g.V(2).both().path()",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let graph = GraphEngine::open(dir.path()).unwrap();
            populate(&graph);
            let query = GremlinQuery::parse(text).unwrap();
            let calls = Cell::new(0);
            let expected = query
                .execute_checked(&graph, &|| {
                    calls.set(calls.get() + 1);
                    Ok(())
                })
                .unwrap();
            // Checkpoint 1 precedes snapshot acquisition. All later checkpoints
            // must observe the originally pinned sequence despite this commit.
            for pause_at in 2..=calls.get() {
                let current = Cell::new(0);
                let barrier = Barrier::new(2);
                let actual = std::thread::scope(|scope| {
                    let writer = scope.spawn(|| {
                        barrier.wait();
                        replace_graph(&graph);
                        barrier.wait();
                    });
                    let result = query.execute_checked(&graph, &|| {
                        current.set(current.get() + 1);
                        if current.get() == pause_at {
                            barrier.wait();
                            barrier.wait();
                        }
                        Ok(())
                    });
                    writer.join().unwrap();
                    result.unwrap()
                });
                assert_eq!(actual, expected, "{text} at checkpoint {pause_at}");
                assert_ne!(query.execute(&graph).unwrap(), expected);
                for id in 1..=4 {
                    graph.delete_vertex(id).unwrap();
                }
                populate(&graph);
            }
        }
    }
}

fn check_deadline(deadline: Instant) -> Result<()> {
    if Instant::now() >= deadline {
        Err(Error::TraversalLimitExceeded(
            "query deadline exceeded".into(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
#[path = "graph_record_query_tests.rs"]
mod record_query_tests;

#[cfg(test)]
mod memory_overlap_tests {
    use super::*;

    #[test]
    fn individually_fitting_input_and_output_sets_cannot_bypass_overlap() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        for id in [1, 2] {
            let mut vertex = Vertex::new(id);
            vertex
                .properties
                .insert("p".into(), Value::Text("x".repeat(4096)));
            graph.create_vertex(&vertex).unwrap();
        }
        graph.create_edge(&Edge::new(1, 1, 2, "E")).unwrap();
        let view = graph.read_view();
        let mut individually = 0;
        for text in ["g.V(1,1)", "g.V(2,2)"] {
            let budget = Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES);
            GremlinQuery::parse(text)
                .unwrap()
                .execute_on_read_budget(&view, &|| Ok(()), &budget)
                .unwrap();
            individually = individually.max(budget.peak());
            assert_eq!(budget.used(), 0);
        }
        let query = GremlinQuery::parse("g.V(1,1).out() ").unwrap();
        let tight = Budget::new(individually);
        assert!(matches!(
            query.execute_on_read_budget(&view, &|| Ok(()), &tight),
            Err(Error::TraversalLimitExceeded(_))
        ));
        assert_eq!(tight.used(), 0);
        let roomy = Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES);
        assert_eq!(
            query
                .execute_on_read_budget(&view, &|| Ok(()), &roomy)
                .unwrap()
                .rows,
            vec![GremlinRow::Vertex(2), GremlinRow::Vertex(2)]
        );
        assert!(roomy.peak() > individually);
        assert_eq!(roomy.used(), 0);
    }
    #[test]
    #[ignore = "manual accounting high-water smoke benchmark"]
    fn accounted_high_water_smoke() {
        let dir = tempfile::tempdir().unwrap();
        let graph = GraphEngine::open(dir.path()).unwrap();
        for id in 0..10_000 {
            let mut vertex = Vertex::new(id);
            vertex.labels.push("V".into());
            vertex
                .properties
                .insert("n".into(), Value::Integer(id as i64));
            graph.create_vertex(&vertex).unwrap();
        }
        for id in 0..8 {
            graph.create_edge(&Edge::new(id, 0, 0, "E")).unwrap();
        }
        let mut short = Vertex::new(10_000);
        for id in 0..1000 {
            short.properties.insert(format!("p{id}"), Value::Null);
        }
        graph.create_vertex(&short).unwrap();
        let mut big = Vertex::new(10_001);
        big.properties
            .insert("big".into(), Value::Text("x".repeat(1024 * 1024)));
        graph.create_vertex(&big).unwrap();
        for id in 100..200 {
            graph.create_edge(&Edge::new(id, 1, 10_001, "BIG")).unwrap();
        }
        for text in [
            "g.V().limit(100).values('n')",
            "g.V().limit(1000).values('n')",
            "g.V(0).out().out().out().path()",
            "g.V(10000).count()",
            "g.V(1).out().count()",
        ] {
            let budget = Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES);
            let result = GremlinQuery::parse(text).unwrap().execute_on_read_budget(
                &graph.read_view(),
                &|| Ok(()),
                &budget,
            );
            println!(
                "{text}: peak_accounted_bytes={} rejected={}",
                budget.peak(),
                result.is_err()
            );
            assert_eq!(budget.used(), 0);
        }
        // Remove the extra benchmark records so the scan is exactly at row cap.
        graph.delete_vertex(10_000).unwrap();
        graph.delete_vertex(10_001).unwrap();
        let budget = Budget::new(MAX_GREMLIN_ACCOUNTED_BYTES);
        GremlinQuery::parse("g.V().count()")
            .unwrap()
            .execute_on_read_budget(&graph.read_view(), &|| Ok(()), &budget)
            .unwrap();
        println!("g.V().count(): peak_accounted_bytes={}", budget.peak());
        assert_eq!(budget.used(), 0);
    }
}
