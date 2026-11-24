use crate::{
    output::Output,
    registry::{self, Resource},
    status::{self, Failure, Result},
    wire,
};
use serde::Serialize;
use velagraph::temporal_reference::{Interval, Key};
use velagraph::temporal_store::{TemporalMutation, TemporalStore};
use velagraph::{
    ExecutionControl, GremlinBindings, GremlinParameters, GremlinQuery, IncrementalGraph,
    IncrementalMode, IncrementalMutation,
};

#[cfg(test)]
thread_local! { pub(super) static FAIL_AFTER_OPEN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
pub enum Engine {
    Memory {
        graph: IncrementalGraph,
        limits: velagraph::IncrementalLimits,
        mode: &'static str,
    },
    Temporal(TemporalStore),
}
#[derive(Serialize)]
struct Info<'a> {
    abi_version: u32,
    backend: &'static str,
    identity: Option<&'a str>,
    version: u64,
    metadata: Option<&'a [u8]>,
    mode: Option<&'a str>,
    max_graph_bytes: Option<usize>,
    capabilities: &'static [&'static str],
    max_batch_mutations: usize,
    max_query_work: usize,
}
#[derive(Serialize)]
struct Batch<'a> {
    version: u64,
    metadata: Option<&'a [u8]>,
    results: Vec<velagraph::GremlinResult>,
}
const MEMORY_CAPS: &[&str] = &[
    "ordinary_graph",
    "ordered_atomic_mutations",
    "vertex_delete_cascade",
    "private_full_load",
    "checkpoint_restore",
    "prepared_queries",
    "controlled_batch_queries",
    "current_version_only",
];
const TEMPORAL_CAPS: &[&str] = &[
    "temporal_assertions",
    "wal_synced_batches",
    "reopen",
    "historical_vt",
    "prepared_queries",
    "controlled_batch_queries",
    "interval_absence_no_cascade",
];
impl Engine {
    pub fn open(q: wire::Open, c: &ExecutionControl) -> Result<Self> {
        c.check()?;
        Ok(match q {
            wire::Open::Memory {
                identity,
                mode,
                vertices,
                edges,
                metadata,
                restore,
                limits,
            } => {
                let (mode, mode_name) = match mode {
                    wire::Mode::Cow => (IncrementalMode::Cow, "cow"),
                    wire::Mode::Locked => (IncrementalMode::Locked, "locked"),
                };
                let limits = limits.unwrap_or_default().kernel();
                if vertices.len().saturating_add(edges.len()) > limits.max_records {
                    return Err(status::limit("full-load record count ceiling"));
                }
                let vertices = vertices
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<velagraph::Vertex>>();
                let edges = edges
                    .into_iter()
                    .map(Into::into)
                    .collect::<Vec<velagraph::Edge>>();
                let graph = if let Some(path) = restore {
                    if !vertices.is_empty() || !edges.is_empty() || !metadata.is_empty() {
                        return Err(status::invalid(
                            "restore cannot include initial records or metadata",
                        ));
                    }
                    IncrementalGraph::restore_with_control(path, &identity, mode, limits.clone(), c)
                        .map_err(|e| {
                            let failure = Failure::Core(e);
                            // These bytes came from an admitted checkpoint file,
                            // not the caller's JSON request. Keep limits/control/I/O
                            // typed; malformed stored records are corruption.
                            if failure.code(false) == status::INVALID {
                                Failure::Simple(status::CORRUPT, "invalid checkpoint payload")
                            } else {
                                failure
                            }
                        })?
                } else {
                    IncrementalGraph::from_records_with_control(
                        identity,
                        mode,
                        limits.clone(),
                        vertices,
                        edges,
                        metadata,
                        c,
                    )
                    .map_err(|e| match e {
                        velagraph::Error::IncrementalMemory(ref inner)
                            if matches!(
                                inner.as_ref(),
                                velagraph::IncrementalMemoryError::InvalidState(_)
                            ) =>
                        {
                            status::invalid("invalid full-load records or endpoints")
                        }
                        other => Failure::Core(other),
                    })?
                };
                Self::Memory {
                    graph,
                    limits,
                    mode: mode_name,
                }
            }
            // Opening a temporal database may initialize its synced format. Once open
            // starts, return its actual outcome even if cancellation races that I/O.
            wire::Open::Temporal { path } => {
                let store = TemporalStore::open(path)?;
                #[cfg(test)]
                if FAIL_AFTER_OPEN.with(|flag| flag.replace(false)) {
                    panic!("injected uncertain temporal initialization");
                }
                Self::Temporal(store)
            }
        })
    }
    pub fn apply(&self, q: wire::Apply, c: &ExecutionControl) -> Result<u64> {
        c.check()?;
        match self {
            Self::Memory { graph: g, .. } => {
                if q.mutations.len() > 10_000 {
                    return Err(status::limit("memory mutation count ceiling"));
                }
                let token = g.token()?;
                if token.sequence != q.expected_version {
                    return Err(velagraph::Error::TransactionConflict.into());
                }
                let mut mutations = Vec::with_capacity(q.mutations.len());
                for m in q.mutations {
                    c.check()?;
                    mutations.push(match m {
                        wire::Mutation::PutVertex { vertex, valid } => {
                            no_time(valid)?;
                            IncrementalMutation::PutVertex(vertex.into())
                        }
                        wire::Mutation::PutEdge { edge, valid } => {
                            no_time(valid)?;
                            IncrementalMutation::PutEdge(edge.into())
                        }
                        wire::Mutation::DeleteVertex { id, valid } => {
                            no_time(valid)?;
                            IncrementalMutation::DeleteVertex(id)
                        }
                        wire::Mutation::DeleteEdge { id, valid } => {
                            no_time(valid)?;
                            IncrementalMutation::DeleteEdge(id)
                        }
                    });
                }
                Ok(g.apply_with_control(
                    &token,
                    &mutations,
                    q.metadata.as_deref().unwrap_or(&[]),
                    c,
                )?
                .sequence)
            }
            Self::Temporal(g) => {
                if q.mutations.len() > 256 {
                    return Err(status::limit("temporal mutation count ceiling"));
                }
                if q.metadata.is_some() {
                    return Err(status::unsupported(
                        "temporal batches have no opaque metadata publication API",
                    ));
                }
                let mut mutations = Vec::with_capacity(q.mutations.len());
                for m in q.mutations {
                    c.check()?;
                    mutations.push(match m {
                        wire::Mutation::PutVertex { vertex, valid } => TemporalMutation::Vertex {
                            vertex: vertex.into(),
                            valid: time(valid)?,
                        },
                        wire::Mutation::PutEdge { edge, valid } => TemporalMutation::Edge {
                            edge: edge.into(),
                            valid: time(valid)?,
                        },
                        wire::Mutation::DeleteVertex { id, valid } => TemporalMutation::Absent {
                            key: Key::Vertex(id),
                            valid: time(valid)?,
                        },
                        wire::Mutation::DeleteEdge { id, valid } => TemporalMutation::Absent {
                            key: Key::Edge(id),
                            valid: time(valid)?,
                        },
                    });
                }
                Ok(g.commit_with_control(q.expected_version, &mutations, c)?)
            }
        }
    }
    pub fn query(
        &self,
        q: wire::Queries,
        c: &ExecutionControl,
        out: Output,
        packed: bool,
    ) -> Result<(u64, u64)> {
        if q.queries.is_empty() || q.queries.len() > 64 {
            return Err(status::limit("query batch must contain 1..64 queries"));
        }
        let limits = q.limits.unwrap_or_default().kernel();
        let mut queries = Vec::with_capacity(q.queries.len());
        let mut retained_plans = Vec::new();
        for query in q.queries {
            c.check()?;
            let bound = match (query.text, query.prepared) {
                (Some(text), None) => {
                    if query.start_ids.is_some() || !query.values.is_empty() {
                        return Err(status::invalid("literal queries cannot include bindings"));
                    }
                    GremlinQuery::parse(&text)?
                }
                (None, Some(id)) => {
                    let resource = registry::get(id)?;
                    let Resource::Plan(plan) = resource.as_ref() else {
                        return Err(Failure::Simple(
                            status::HANDLE,
                            "handle is not a prepared plan",
                        ));
                    };
                    let bound = plan.query.bind(GremlinBindings {
                        start_ids: query.start_ids.as_deref(),
                        values: &query.values,
                    })?;
                    retained_plans.push(resource);
                    bound
                }
                _ => {
                    return Err(status::invalid(
                        "select exactly one query text or prepared handle",
                    ))
                }
            };
            queries.push(bound);
        }
        c.check()?;
        match self {
            Self::Memory { graph: g, .. } => {
                if q.valid_time_micros.is_some() {
                    return Err(status::unsupported(
                        "ordinary memory graph has no valid-time axis",
                    ));
                }
                let view = g.read()?;
                c.check()?;
                let version = view.token().sequence;
                if q.version.is_some_and(|v| v != version) {
                    return Err(velagraph::Error::TransactionConflict.into());
                }
                let results = view.execute_query_batch(&queries, c, limits)?;
                let buffer = if packed {
                    out.encode_packed(version, Some(view.metadata()), &results, c)?
                } else {
                    out.encode(
                        &Batch {
                            version,
                            metadata: Some(view.metadata()),
                            results,
                        },
                        c,
                    )?
                };
                Ok((version, buffer))
            }
            Self::Temporal(g) => {
                let t = q
                    .valid_time_micros
                    .ok_or_else(|| status::invalid("temporal query requires valid_time_micros"))?;
                let view = g.view(q.version, t)?;
                c.check()?;
                let version = view.version();
                let results = view.execute_query_batch(&queries, c, limits)?;
                let buffer = if packed {
                    out.encode_packed(version, None, &results, c)?
                } else {
                    out.encode(
                        &Batch {
                            version,
                            metadata: None,
                            results,
                        },
                        c,
                    )?
                };
                Ok((version, buffer))
            }
        }
    }
    pub fn info(&self, c: &ExecutionControl, out: Output) -> Result<u64> {
        c.check()?;
        match self {
            Self::Memory {
                graph: g,
                limits,
                mode,
            } => {
                let v = g.read()?;
                let t = v.token();
                out.encode(
                    &Info {
                        abi_version: 1,
                        backend: "memory",
                        identity: Some(&t.identity),
                        version: t.sequence,
                        metadata: Some(v.metadata()),
                        mode: Some(mode),
                        max_graph_bytes: Some(limits.max_accounted_bytes),
                        capabilities: MEMORY_CAPS,
                        max_batch_mutations: limits.max_batch_operations,
                        max_query_work: 65_536,
                    },
                    c,
                )
            }
            Self::Temporal(g) => {
                let v = g.view(None, 0)?;
                out.encode(
                    &Info {
                        abi_version: 1,
                        backend: "temporal",
                        identity: None,
                        version: v.version(),
                        metadata: None,
                        mode: None,
                        max_graph_bytes: None,
                        capabilities: TEMPORAL_CAPS,
                        max_batch_mutations: 256,
                        max_query_work: 4096,
                    },
                    c,
                )
            }
        }
    }
    pub fn checkpoint(&self, path: &str, c: &ExecutionControl) -> Result<()> {
        match self {
            Self::Memory { graph: g, .. } => Ok(g.checkpoint_with_control(path, c)?),
            Self::Temporal(_) => Err(status::unsupported(
                "temporal checkpoint export is not exposed by this ABI",
            )),
        }
    }
}
fn no_time(t: Option<wire::Interval>) -> Result<()> {
    if t.is_some() {
        return Err(status::unsupported(
            "valid intervals require a temporal backend",
        ));
    }
    Ok(())
}
fn time(t: Option<wire::Interval>) -> Result<Interval> {
    let t = t.ok_or_else(|| status::invalid("temporal mutation requires a valid interval"))?;
    Ok(Interval {
        from: t.from,
        to: t.to,
    })
}
pub fn prepare(q: wire::Prepare, c: &ExecutionControl) -> Result<registry::Plan> {
    c.check()?;
    if q.text.len() > velagraph::MAX_GREMLIN_QUERY_BYTES {
        return Err(status::limit("query text limit"));
    }
    let permit = crate::bounds::Permit::plan(q.text.len() * 4 + 4096)?;
    let query = GremlinQuery::parse(&q.text)?.prepare(GremlinParameters {
        start_ids: q.bind_start_ids,
        has_value_steps: q.value_steps,
    })?;
    c.check()?;
    Ok(registry::Plan {
        query,
        _permit: permit,
    })
}
