//! Bounded graph traversal on top of the K1 adjacency indexes.
//!
//! This is the execution foundation for K2. It deliberately has no Cypher
//! parser yet: callers describe a start vertex, direction, optional edge label,
//! hop limit, and path limit. A later parser/planner can target this executor.

use std::collections::{HashSet, VecDeque};

use crate::error::{Error, Result};
use crate::graph::{Edge, GraphEngine};

/// Maximum supported traversal depth. Keeping this small prevents accidental
/// full-graph walks before the query layer has cost-based limits.
pub const MAX_TRAVERSAL_HOPS: u8 = 3;

/// Edge direction followed during traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraversalDirection {
    Out,
    In,
    Both,
}

/// Bounded traversal request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraversalOptions {
    pub max_hops: u8,
    pub direction: TraversalDirection,
    pub label: Option<String>,
    pub limit: usize,
}

impl Default for TraversalOptions {
    fn default() -> Self {
        Self {
            max_hops: 1,
            direction: TraversalDirection::Out,
            label: None,
            limit: 100,
        }
    }
}

/// One path from the start vertex. `vertices[0]` is the start, and
/// `edges[i]` connects `vertices[i]` to `vertices[i + 1]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraversalPath {
    pub vertices: Vec<u64>,
    pub edges: Vec<u64>,
}

/// Traversal result. `truncated` is true when the path limit stopped a longer
/// exploration; callers must not treat the result as complete in that case.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraversalResult {
    pub paths: Vec<TraversalPath>,
    pub truncated: bool,
}

impl GraphEngine {
    /// Breadth-first traversal from `start`, returning paths of 1..=max_hops.
    ///
    /// A vertex is visited at most once per request, so cycles terminate and
    /// the result contains at most one path to each reachable vertex. Edge
    /// order is deterministic: edge id ascending.
    pub fn traverse(&self, start: u64, options: &TraversalOptions) -> Result<TraversalResult> {
        validate_options(options)?;
        if self.get_vertex(start)?.is_none() {
            return Err(Error::VertexNotFound(start));
        }

        let mut visited = HashSet::new();
        visited.insert(start);
        let mut queue = VecDeque::new();
        queue.push_back(TraversalPath {
            vertices: vec![start],
            edges: Vec::new(),
        });

        let mut paths = Vec::new();
        let mut truncated = false;
        'outer: while let Some(path) = queue.pop_front() {
            if path.edges.len() >= usize::from(options.max_hops) {
                continue;
            }
            let current = *path.vertices.last().expect("path has a current vertex");
            let edges = self.edges_for_traversal(current, options)?;
            for edge in edges {
                let next = next_vertex(&edge, current, options.direction);
                if !visited.insert(next) {
                    continue;
                }
                let mut next_path = path.clone();
                next_path.vertices.push(next);
                next_path.edges.push(edge.id);
                if paths.len() >= options.limit {
                    truncated = true;
                    break 'outer;
                }
                let expand = next_path.edges.len() < usize::from(options.max_hops);
                paths.push(next_path.clone());
                if expand {
                    queue.push_back(next_path);
                }
            }
        }
        Ok(TraversalResult { paths, truncated })
    }

    fn edges_for_traversal(&self, vertex: u64, options: &TraversalOptions) -> Result<Vec<Edge>> {
        let label = options.label.as_deref();
        let mut edges = match options.direction {
            TraversalDirection::Out => self.out_edges(vertex, label)?,
            TraversalDirection::In => self.in_edges(vertex, label)?,
            TraversalDirection::Both => {
                let mut edges = self.out_edges(vertex, label)?;
                edges.extend(self.in_edges(vertex, label)?);
                edges.sort_by_key(|edge| edge.id);
                edges.dedup_by_key(|edge| edge.id);
                edges
            }
        };
        edges.sort_by_key(|edge| edge.id);
        Ok(edges)
    }
}

fn validate_options(options: &TraversalOptions) -> Result<()> {
    if options.max_hops == 0 || options.max_hops > MAX_TRAVERSAL_HOPS {
        return Err(Error::InvalidTraversal(format!(
            "max_hops must be between 1 and {MAX_TRAVERSAL_HOPS}, got {}",
            options.max_hops
        )));
    }
    if options.limit == 0 {
        return Err(Error::InvalidTraversal("limit must be positive".into()));
    }
    if let Some(label) = &options.label {
        if label.is_empty() || label.len() > 64 {
            return Err(Error::InvalidGraphLabel(label.clone()));
        }
    }
    Ok(())
}

fn next_vertex(edge: &Edge, current: u64, direction: TraversalDirection) -> u64 {
    match direction {
        TraversalDirection::Out => edge.to,
        TraversalDirection::In => edge.from,
        TraversalDirection::Both => {
            if edge.from == current {
                edge.to
            } else {
                edge.from
            }
        }
    }
}
