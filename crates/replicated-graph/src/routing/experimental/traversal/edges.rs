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
struct EdgeRow {
    edge: EdgeRef,
    value: Option<Value>,
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
fn push_edge(rows: &mut Vec<EdgeRow>, row: EdgeRow, work: &mut Work) -> ExperimentalResult<()> {
    if rows.len() >= 10_000 {
        return Err(limit("v3 edge row cap exceeded"));
    }
    work.charge(128 + std::mem::size_of::<ObservedPathElement>())?;
    rows.push(row);
    Ok(())
}
impl ExperimentalReadSession {
    /// Adds E()/E(ids) observations to the V subset. Explicit edge IDs belong to
    /// start_partition, preserving order and duplicates. Empty E() scans the
    /// frozen catalog in (edge ID, owner partition) order. Edge traversal steps
    /// are rejected before execution, even for empty input. All output remains
    /// Unverified and uses the original session deadline, without refreshing it.
    pub fn observed_gremlin_v3(
        &self,
        start_partition: u32,
        text: &str,
    ) -> ExperimentalResult<ObservedTraversalV3> {
        self.op.check(None)?;
        let query = GremlinQuery::parse(text).map_err(ExperimentalRouteError::Query)?;
        self.op.check(None)?;
        let GremlinStart::Edges(ids) = query.start() else {
            let result = self.execute_observed(start_partition, text, true)?;
            let output = ObservedTraversalV3 {
                rows: result
                    .rows
                    .into_iter()
                    .map(|row| match row {
                        ObservedRowV2::Vertex(v) => ObservedRowV3::Vertex(v),
                        ObservedRowV2::Path(p) => ObservedRowV3::Path(p),
                        ObservedRowV2::Count(n) => ObservedRowV3::Count(n),
                        ObservedRowV2::Value { source, value } => ObservedRowV3::Value {
                            source: ObservedElementRef::Vertex(source),
                            value,
                        },
                    })
                    .collect(),
                completeness: result.completeness,
                metadata_sequence: result.metadata_sequence,
                snapshots: result.snapshots,
            };
            self.op.check(None)?;
            return Ok(output);
        };
        let mut scalar = false;
        for step in query.steps() {
            if scalar && !matches!(step, GremlinStep::Limit(_) | GremlinStep::Count) {
                return Err(ExperimentalRouteError::Unsupported(
                    "only limit/count may follow a v3 scalar projection",
                ));
            }
            match step {
                GremlinStep::Values(_) | GremlinStep::Label => scalar = true,
                GremlinStep::HasLabel(_)
                | GremlinStep::Has { .. }
                | GremlinStep::Limit(_)
                | GremlinStep::Path
                | GremlinStep::Count => {}
                _ => {
                    return Err(ExperimentalRouteError::Unsupported(
                        "v3 edge traversal step unsupported",
                    ))
                }
            }
        }
        if !ids.is_empty() {
            self.store(start_partition)?;
        }
        let mut work = Work {
            bytes: 4096,
            visits: 0,
        };
        let mut rows = Vec::new();
        let leading = match query.steps().first() {
            Some(GremlinStep::Limit(n)) => Some(*n),
            _ => None,
        };
        for id in ids {
            self.op.check(None)?;
            if leading.is_some_and(|n| rows.len() >= n) {
                break;
            }
            work.visit()?;
            let r = EdgeRef {
                partition: start_partition,
                id: *id,
            };
            if let Some(edge) = self.get_edge(r)? {
                work.edge(&edge)?;
                push_edge(
                    &mut rows,
                    EdgeRow {
                        edge: r,
                        value: None,
                    },
                    &mut work,
                )?;
            }
        }
        if ids.is_empty() {
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
                    let page =
                        store.edge_page(after, ScanLimit::new(remaining, 1024 * 1024 + 64)?)?;
                    work.charge(page.raw_bytes + page.edges.len() * 128)?;
                    for candidate in page.edges {
                        self.op.check(None)?;
                        work.visit()?;
                        work.edge(&candidate)?;
                        let r = candidate.identity();
                        let edge = self.get_edge(r)?.ok_or(ExperimentalError::Incomplete(r))?;
                        work.edge(&edge)?;
                        if edge != candidate {
                            return Err(ExperimentalError::Incomplete(r).into());
                        }
                        push_edge(
                            &mut rows,
                            EdgeRow {
                                edge: r,
                                value: None,
                            },
                            &mut work,
                        )?;
                        acquired += 1;
                    }
                    if let Some(n) = leading {
                        rows.sort_unstable_by_key(|r| (r.edge.id, r.edge.partition));
                        rows.truncate(n);
                    }
                    after = page.next_after;
                    if after.is_none() {
                        break;
                    }
                }
            }
            rows.sort_unstable_by_key(|r| (r.edge.id, r.edge.partition));
        }
        for step in query.steps() {
            self.op.check(None)?;
            match step {
                GremlinStep::Limit(n) => rows.truncate(*n),
                GremlinStep::Path | GremlinStep::Count => {}
                GremlinStep::HasLabel(_)
                | GremlinStep::Has { .. }
                | GremlinStep::Values(_)
                | GremlinStep::Label => {
                    let mut next = Vec::new();
                    for mut row in rows {
                        self.op.check(None)?;
                        work.visit()?;
                        let mut edge = self
                            .get_edge(row.edge)?
                            .ok_or(ExperimentalError::Incomplete(row.edge))?;
                        work.edge(&edge)?;
                        let keep = match step {
                            GremlinStep::HasLabel(labels) => labels.contains(&edge.label),
                            GremlinStep::Has { key, value } => match value {
                                Some(value) => edge.properties.get(key) == Some(value),
                                None => edge.properties.contains_key(key),
                            },
                            GremlinStep::Values(key) => {
                                row.value = edge.properties.remove(key);
                                row.value.is_some()
                            }
                            GremlinStep::Label => {
                                row.value = Some(Value::Text(edge.label));
                                true
                            }
                            _ => unreachable!("validated filter/projection"),
                        };
                        if keep {
                            if let Some(value) = &row.value {
                                work.charge(match value {
                                    Value::Text(s) => s.len(),
                                    _ => 8,
                                })?;
                            }
                            push_edge(&mut next, row, &mut work)?;
                        }
                    }
                    rows = next;
                }
                _ => unreachable!("validated edge subset"),
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
        let rows = if count {
            vec![ObservedRowV3::Count(rows.len() as u64)]
        } else {
            rows.into_iter()
                .map(|r| {
                    if let Some(value) = r.value {
                        ObservedRowV3::Value {
                            source: ObservedElementRef::Edge(r.edge),
                            value,
                        }
                    } else if path {
                        ObservedRowV3::Path(vec![ObservedPathElement::Edge(r.edge)])
                    } else {
                        ObservedRowV3::Edge(r.edge)
                    }
                })
                .collect()
        };
        self.op.check(None)?;
        Ok(ObservedTraversalV3 {
            rows,
            completeness: AdjacencyCompleteness::Unverified,
            metadata_sequence: self.metadata_sequence,
            snapshots: self.snapshots.clone(),
        })
    }
}
