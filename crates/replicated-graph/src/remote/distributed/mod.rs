//! Experimental server coordination over a caller-maintained immutable catalog.
//! No snapshot downloads, retries, global consistency, or completeness claim.
use super::*;
use query::{QualifiedElement, QueryCompleteness, QueryFailure, QueryRow, QueryStamp};
use std::collections::{BTreeMap, BTreeSet};
use velagraph::{
    GremlinStart, GremlinStep, ObservedGremlinQuery as GremlinQuery, ObservedGremlinStep as Step,
    Value,
};
mod adjacency;
pub(crate) mod fixed;
pub(crate) mod fixed_execution;
mod fixed_parallel;
#[cfg(test)]
mod typed_test_peer;
use adjacency::Direction;
pub mod consistency;
pub mod grpc;
mod hierarchy;
pub mod jobs;
mod projection;
mod scan;
mod sessions;
mod sync_projection;
pub(super) use sessions::Sessions;

const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_WORK: usize = 10_000;
const MAX_ROWS: usize = 10_000;
const MAX_WIRE: usize = query::MAX_QUERY_RESPONSE_BYTES;
const MAX_TIMEOUT: u64 = 5000;
static COORDINATORS: Semaphore = Semaphore::const_new(2);

/// One fixed selected data replica. Configuration is trusted, not discovered.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartitionRoute {
    pub partition: u32,
    pub shard: u64,
    pub epoch: u64,
    pub identity: RemoteIdentity,
    pub endpoint: SocketAddr,
}
/// Immutable startup configuration, not a read of the persisted metadata catalog.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StaticCatalog {
    pub partitions: Vec<PartitionRoute>,
}
impl StaticCatalog {
    fn validate(&self) -> Result<(), RemoteError> {
        self.validate_bounded(32, 8)
    }
    // Fixed-V cohorts have their own protocol membership ceiling. Keep the
    // observed entrypoint's independent admission contract unchanged.
    fn validate_bounded(
        &self,
        max_partitions: usize,
        max_shards: usize,
    ) -> Result<(), RemoteError> {
        if self.partitions.is_empty() || self.partitions.len() > max_partitions {
            return Err(RemoteError::InvalidArgument);
        }
        let mut partitions = BTreeMap::new();
        let mut shards = BTreeMap::new();
        let mut endpoints = BTreeMap::new();
        let mut groups = BTreeMap::new();
        for p in &self.partitions {
            if p.epoch == 0
                || p.identity.node == 0
                || p.endpoint.port() == 0
                || partitions.insert(p.partition, ()).is_some()
            {
                return Err(RemoteError::InvalidArgument);
            }
            if groups
                .insert(p.identity.group, p.shard)
                .is_some_and(|old| old != p.shard)
            {
                return Err(RemoteError::InvalidArgument);
            }
            let owner = (p.epoch, p.identity, p.endpoint);
            if shards
                .insert(p.shard, owner)
                .is_some_and(|old| old != owner)
                || endpoints
                    .insert(p.endpoint, (p.shard, p.identity))
                    .is_some_and(|old| old != (p.shard, p.identity))
            {
                return Err(RemoteError::InvalidArgument);
            }
        }
        if shards.len() > max_shards {
            return Err(RemoteError::ResourceExhausted);
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DistributedRequest {
    pub start_partition: u32,
    pub text: String,
    pub timeout_millis: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DistributedOutput {
    pub completeness: QueryCompleteness,
    pub snapshots: Vec<QueryStamp>,
    pub rows: Vec<QueryRow>,
}
pub type DistributedResponse = Result<DistributedOutput, QueryFailure>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    stamp: QueryStamp,
    incarnation: [u8; 16],
    token: [u8; 16],
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VertexId {
    partition: u32,
    id: u64,
}
impl VertexId {
    fn element(self) -> QualifiedElement {
        QualifiedElement::Vertex {
            partition: self.partition,
            id: self.id,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct EdgeHop {
    id: u64,
    from: VertexId,
    to: VertexId,
    label: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IncomingEdge {
    id: u64,
    partition: u32,
    label: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum Fragment {
    HierarchyBatch {
        partition: u32,
        fragments: Vec<Fragment>,
    },
    Scan(scan::ScanRequest),
    Exists(VertexId),
    Out(VertexId),
    In(VertexId),
    ResolveIncoming {
        target: VertexId,
        edge: IncomingEdge,
    },
    VerifyReverse(EdgeHop),
    Has {
        vertex: VertexId,
        key: String,
        value: Option<Value>,
    },
    HasLabel {
        vertex: VertexId,
        labels: Vec<String>,
    },
    Values {
        vertex: VertexId,
        key: String,
    },
    Project {
        vertex: VertexId,
        keys: Vec<String>,
    },
}
impl Fragment {
    fn validate(&self) -> Result<(), RemoteError> {
        let bytes = match self {
            Self::HierarchyBatch {
                partition,
                fragments,
            } => {
                if fragments.is_empty() || fragments.len() > jobs::BATCH_ROWS {
                    return Err(RemoteError::InvalidArgument);
                }
                for fragment in fragments {
                    if !matches!(
                        fragment,
                        Self::Exists(_)
                            | Self::Out(_)
                            | Self::In(_)
                            | Self::ResolveIncoming { .. }
                            | Self::VerifyReverse(_)
                            | Self::Project { .. }
                    ) || fragment.partition() != *partition
                    {
                        return Err(RemoteError::InvalidArgument);
                    }
                    fragment.validate()?;
                }
                serde_json::to_vec(self)
                    .map_err(|_| RemoteError::InvalidArgument)?
                    .len()
            }
            Self::Scan(request) => {
                request.validate()?;
                0
            }
            Self::Exists(_) | Self::Out(_) | Self::In(_) => 0,
            Self::ResolveIncoming { edge, .. } => {
                if edge.label.len() > 64 {
                    return Err(RemoteError::InvalidArgument);
                }
                edge.label.len()
            }
            Self::VerifyReverse(edge) => {
                if edge.label.len() > 64 {
                    return Err(RemoteError::InvalidArgument);
                }
                edge.label.len()
            }
            Self::Has { key, value, .. } => key.len().saturating_add(match value {
                Some(Value::Text(text)) => text.len(),
                _ => 8,
            }),
            Self::HasLabel { labels, .. } => {
                if labels.len() > 64 {
                    return Err(RemoteError::ResourceExhausted);
                }
                labels.iter().map(String::len).sum()
            }
            Self::Values { key, .. } => key.len(),
            Self::Project { keys, .. } => {
                projection::validate_keys(keys).map_err(|_| RemoteError::InvalidArgument)?;
                keys.iter().map(String::len).sum()
            }
        };
        if bytes > query::MAX_QUERY_TEXT_BYTES {
            return Err(RemoteError::ResourceExhausted);
        }
        Ok(())
    }
    fn partition(&self) -> u32 {
        match self {
            Self::HierarchyBatch { partition, .. } => *partition,
            Self::Scan(request) => request.partition,
            Self::Exists(v) | Self::Out(v) | Self::In(v) => v.partition,
            Self::ResolveIncoming { edge, .. } => edge.partition,
            Self::VerifyReverse(e) => e.to.partition,
            Self::Has { vertex, .. }
            | Self::HasLabel { vertex, .. }
            | Self::Values { vertex, .. }
            | Self::Project { vertex, .. } => vertex.partition,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cost {
    bytes: usize,
    work: usize,
}
impl Cost {
    fn charge(&mut self, bytes: usize, work: usize, ceiling: Cost) -> Result<(), QueryFailure> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or(QueryFailure::ResourceExhausted)?;
        self.work = self
            .work
            .checked_add(work)
            .ok_or(QueryFailure::ResourceExhausted)?;
        if self.bytes > ceiling.bytes || self.work > ceiling.work {
            return Err(QueryFailure::ResourceExhausted);
        }
        Ok(())
    }
    #[cfg(test)]
    fn remaining(self) -> Self {
        Self {
            bytes: MAX_BYTES.saturating_sub(self.bytes),
            work: MAX_WORK.saturating_sub(self.work),
        }
    }
}
const CEILING: Cost = Cost {
    bytes: MAX_BYTES,
    work: MAX_WORK,
};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FragmentRequest {
    descriptor: Descriptor,
    fragment: Fragment,
    remaining: Cost,
}
#[derive(Debug, Serialize, Deserialize)]
enum FragmentValue {
    HierarchyBatch(Vec<FragmentValue>),
    Scan(scan::ScanResult),
    Bool(bool),
    Edges(Vec<EdgeHop>),
    Incoming(Vec<IncomingEdge>),
    Edge(EdgeHop),
    Value(Option<Value>),
    Projected {
        vertex: VertexId,
        properties: BTreeMap<String, Value>,
    },
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FragmentOutput {
    descriptor: Descriptor,
    cost: Cost,
    value: FragmentValue,
}

fn check(end: Instant) -> Result<(), QueryFailure> {
    if Instant::now() >= end {
        Err(QueryFailure::Timeout)
    } else {
        Ok(())
    }
}
fn remote_failure(error: RemoteError) -> QueryFailure {
    match error {
        RemoteError::Timeout { .. } => QueryFailure::Timeout,
        RemoteError::ResourceExhausted => QueryFailure::ResourceExhausted,
        RemoteError::InvalidArgument | RemoteError::Protocol => QueryFailure::IncompleteOrCorrupt,
        _ => QueryFailure::Unavailable,
    }
}
fn validate_request(request: &DistributedRequest) -> Result<(), RemoteError> {
    if request.text.len() > query::MAX_QUERY_TEXT_BYTES {
        return Err(RemoteError::ResourceExhausted);
    }
    if !(1..=MAX_TIMEOUT).contains(&request.timeout_millis) {
        return Err(RemoteError::InvalidArgument);
    }
    Ok(())
}
fn plan(request: &DistributedRequest) -> Result<GremlinQuery, QueryFailure> {
    let query = GremlinQuery::parse(&request.text).map_err(query::graph_error)?;
    if scan::is_global(&query) {
        if scan::join_at(&query)?.is_none() {
            scan::steps(&query)?;
        }
        return Ok(query);
    }
    if !matches!(query.start(), GremlinStart::Vertices(ids) if !ids.is_empty()) {
        return Err(QueryFailure::Unsupported);
    }
    hierarchy::validate_depth(&query)?;
    let mut scalar = false;
    for (index, step) in query.steps().iter().enumerate() {
        if scalar
            && !matches!(
                step,
                Step::Existing(GremlinStep::Limit(_)) | Step::Existing(GremlinStep::Count)
            )
        {
            return Err(QueryFailure::Unsupported);
        }
        match step {
            Step::ValueMap(keys) if index + 1 == query.steps().len() => {
                projection::validate_keys(keys)?;
            }
            // The official parser bounds the complete traversal to three edge
            // steps. All hops share this execution's sessions, deadline and cost.
            Step::Existing(GremlinStep::Out(_))
            | Step::Existing(GremlinStep::In(_))
            | Step::Existing(GremlinStep::Both(_))
            | Step::Dedup
            | Step::Repeat { .. } => {}
            Step::Existing(GremlinStep::Has { .. })
            | Step::Existing(GremlinStep::HasLabel(_))
            | Step::Existing(GremlinStep::Limit(_)) => {}
            Step::Existing(GremlinStep::Values(_)) => scalar = true,
            Step::Existing(GremlinStep::Path) | Step::Existing(GremlinStep::Count)
                if index + 1 == query.steps().len() => {}
            _ => return Err(QueryFailure::Unsupported),
        }
    }
    Ok(query)
}
struct Traverser {
    vertex: VertexId,
    path: Vec<QualifiedElement>,
    value: Option<Value>,
}
fn push(rows: &mut Vec<Traverser>, row: Traverser, cost: &mut Cost) -> Result<(), QueryFailure> {
    if rows.len() >= MAX_ROWS {
        return Err(QueryFailure::ResourceExhausted);
    }
    cost.charge(256 + row.path.len() * 32, 1, CEILING)?;
    rows.push(row);
    Ok(())
}
fn dedup_vertices(
    rows: Vec<Traverser>,
    cost: &mut Cost,
    end: Instant,
) -> Result<Vec<Traverser>, QueryFailure> {
    // All upstream rows have already been validated and charged.
    // Reserve a conservative tree-node cost before each insertion;
    // charge every input lookup even for duplicates. Never reset cost.
    let mut seen = BTreeSet::new();
    let mut next = Vec::new();
    for row in rows {
        check(end)?;
        cost.charge(0, 1, CEILING)?;
        let identity = (row.vertex.partition, row.vertex.id);
        if !seen.contains(&identity) {
            cost.charge(128, 0, CEILING)?;
            seen.insert(identity);
            push(&mut next, row, cost)?;
        }
    }
    Ok(next)
}
struct Execution<'a> {
    catalog: &'a StaticCatalog,
    grpc_clients: Option<&'a BTreeMap<u64, grpc::QueryClient>>,
    sessions: BTreeMap<u64, (PartitionRoute, Descriptor)>,
    fixed_sessions: Option<&'a mut BTreeMap<u64, fixed_execution::NodeSession>>,
    control: Option<fixed_execution::ExecutionControl>,
    fixed_parallelism: usize,
    end: Instant,
    cost: Cost,
    scan_cost: Cost,
    ceiling: Cost,
}
impl Execution<'_> {
    fn check(&self) -> Result<(), QueryFailure> {
        check(self.end)?;
        if let Some(control) = &self.control {
            control.check()?;
        }
        Ok(())
    }
    fn route(&self, partition: u32) -> Result<&PartitionRoute, QueryFailure> {
        self.catalog
            .partitions
            .iter()
            .find(|p| p.partition == partition)
            .ok_or(QueryFailure::UnsupportedScope)
    }
    async fn fragment(&mut self, fragment: Fragment) -> Result<FragmentValue, QueryFailure> {
        self.fragment_transport(fragment).await
    }
    async fn fragment_transport(
        &mut self,
        fragment: Fragment,
    ) -> Result<FragmentValue, QueryFailure> {
        self.check()?;
        if let Some(sessions) = &mut self.fixed_sessions {
            // The separate admitted fixed path shares all traversal/adjacency
            // semantics, but never calls an observed OpenQuery/QueryFragment.
            return fixed_execution::fragment(
                sessions,
                fragment,
                &mut self.cost,
                self.end,
                self.control.as_ref(),
            )
            .await;
        }
        let route = self.route(fragment.partition())?.clone();
        let descriptor = self
            .sessions
            .get(&route.shard)
            .ok_or(QueryFailure::Unavailable)?
            .1
            .clone();
        let (cost, ceiling) = if matches!(fragment, Fragment::Scan(_)) {
            (&mut self.scan_cost, scan::SCAN_CEILING)
        } else {
            (&mut self.cost, self.ceiling)
        };
        // Charge request encoding and transport work before allocating/sending.
        cost.charge(512, 1, ceiling)?;
        let request = FragmentRequest {
            descriptor: descriptor.clone(),
            fragment,
            remaining: Cost {
                bytes: ceiling.bytes.saturating_sub(cost.bytes),
                work: ceiling.work.saturating_sub(cost.work),
            },
        };
        let encoded = serde_json::to_vec(&request).map_err(|_| QueryFailure::InvalidArgument)?;
        cost.charge(encoded.len() * 2, 0, ceiling)?;
        let request = FragmentRequest {
            remaining: Cost {
                bytes: ceiling.bytes.saturating_sub(cost.bytes),
                work: ceiling.work.saturating_sub(cost.work),
            },
            ..request
        };
        let reply = observed_client(self.grpc_clients, &route, self.end)?
            .fragment(request, self.end.saturating_duration_since(Instant::now()))
            .await
            .map(Reply::QueryFragment)
            .map_err(remote_failure)?;
        check(self.end)?;
        let Reply::QueryFragment(response) = reply else {
            return Err(QueryFailure::IncompleteOrCorrupt);
        };
        let output = response?;
        if output.descriptor != descriptor {
            return Err(QueryFailure::IncompleteOrCorrupt);
        }
        cost.charge(output.cost.bytes, output.cost.work, ceiling)?;
        Ok(output.value)
    }
    async fn boolean(&mut self, fragment: Fragment) -> Result<bool, QueryFailure> {
        match self.fragment(fragment).await? {
            FragmentValue::Bool(value) => Ok(value),
            _ => Err(QueryFailure::IncompleteOrCorrupt),
        }
    }
    async fn incoming(
        &mut self,
        target: VertexId,
        label: Option<&str>,
    ) -> Result<Vec<EdgeHop>, QueryFailure> {
        let FragmentValue::Incoming(entries) = self.fragment(Fragment::In(target)).await? else {
            return Err(QueryFailure::IncompleteOrCorrupt);
        };
        if entries.len() > 1000
            || entries
                .windows(2)
                .any(|w| (w[0].id, w[0].partition) >= (w[1].id, w[1].partition))
        {
            return Err(QueryFailure::IncompleteOrCorrupt);
        }
        let mut edges = Vec::new();
        for entry in entries {
            let FragmentValue::Edge(edge) = self
                .fragment(Fragment::ResolveIncoming {
                    target,
                    edge: entry.clone(),
                })
                .await?
            else {
                return Err(QueryFailure::IncompleteOrCorrupt);
            };
            if edge.id != entry.id
                || edge.from.partition != entry.partition
                || edge.to != target
                || edge.label != entry.label
            {
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
            // The target scan verified this exact reverse entry and endpoint;
            // source resolution verified the canonical edge, outgoing index and
            // source endpoint. Both are fixed sessions. Filter only after all
            // those checks, without another identical target verification RPC.
            if label.is_some_and(|label| label != edge.label) {
                continue;
            }
            self.cost.charge(256 + edge.label.len(), 1, self.ceiling)?;
            edges.push(edge);
        }
        Ok(edges)
    }

    // Shared scalar projection for explicit-ID, joined and bounded scan roots.
    async fn values(
        &mut self,
        rows: Vec<Traverser>,
        key: &str,
    ) -> Result<Vec<Traverser>, QueryFailure> {
        let mut next = Vec::new();
        for mut row in rows {
            let FragmentValue::Value(value) = self
                .fragment(Fragment::Values {
                    vertex: row.vertex,
                    key: key.to_owned(),
                })
                .await?
            else {
                return Err(QueryFailure::IncompleteOrCorrupt);
            };
            if let Some(value) = value {
                row.value = Some(value);
                row.path.clear();
                push(&mut next, row, &mut self.cost)?;
            }
        }
        Ok(next)
    }
    async fn run(
        &mut self,
        request: &DistributedRequest,
        query: &GremlinQuery,
    ) -> DistributedResponse {
        self.route(request.start_partition)?;
        let join = scan::join_at(query)?;
        if scan::is_global(query) && join.is_none() {
            return self.run_scan(query).await;
        }
        let mut rows = Vec::new();
        if let Some(at) = join {
            for row in self.scan_rows(query, Some(at)).await? {
                let QueryRow::Element(QualifiedElement::Vertex { partition, id }) = row else {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                };
                let vertex = VertexId { partition, id };
                push(
                    &mut rows,
                    Traverser {
                        vertex,
                        path: vec![vertex.element()],
                        value: None,
                    },
                    &mut self.cost,
                )?;
            }
        }
        let traversal_steps = &query.steps()[join.unwrap_or(0)..];
        let GremlinStart::Vertices(ids) = query.start() else {
            return Err(QueryFailure::Unsupported);
        };
        for id in ids {
            if matches!(query.steps().first(), Some(Step::Existing(GremlinStep::Limit(n))) if rows.len() >= *n)
            {
                break;
            }
            let vertex = VertexId {
                partition: request.start_partition,
                id: *id,
            };
            if self.boolean(Fragment::Exists(vertex)).await? {
                push(
                    &mut rows,
                    Traverser {
                        vertex,
                        path: vec![vertex.element()],
                        value: None,
                    },
                    &mut self.cost,
                )?;
            }
        }
        for step in traversal_steps {
            self.check()?;
            match step {
                Step::Existing(GremlinStep::Limit(n)) => rows.truncate(*n),
                Step::Dedup => {
                    rows = dedup_vertices(rows, &mut self.cost, self.end)?;
                }
                Step::Existing(GremlinStep::Out(label))
                | Step::Existing(GremlinStep::In(label))
                | Step::Existing(GremlinStep::Both(label)) => {
                    rows = self
                        .expand(
                            rows,
                            Direction::from_step(step).ok_or(QueryFailure::Unsupported)?,
                            label.as_deref(),
                        )
                        .await?;
                }
                Step::Repeat {
                    incoming,
                    label,
                    times,
                    emit,
                } => {
                    rows = self
                        .repeat(rows, *incoming, label.as_deref(), *times, *emit)
                        .await?;
                }
                Step::Existing(GremlinStep::Has { .. })
                | Step::Existing(GremlinStep::HasLabel(_)) => {
                    let mut next = Vec::new();
                    for row in rows {
                        let fragment = match step {
                            Step::Existing(GremlinStep::Has { key, value }) => Fragment::Has {
                                vertex: row.vertex,
                                key: key.clone(),
                                value: value.clone(),
                            },
                            Step::Existing(GremlinStep::HasLabel(labels)) => Fragment::HasLabel {
                                vertex: row.vertex,
                                labels: labels.clone(),
                            },
                            _ => unreachable!(),
                        };
                        if self.boolean(fragment).await? {
                            push(&mut next, row, &mut self.cost)?;
                        }
                    }
                    rows = next;
                }
                Step::Existing(GremlinStep::Values(key)) => {
                    rows = self.values(rows, key).await?;
                }
                Step::ValueMap(_) => {}
                Step::Existing(GremlinStep::Path) | Step::Existing(GremlinStep::Count) => {}
                _ => return Err(QueryFailure::Unsupported),
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
        if !scan::is_global(query)
            && !count
            && !query
                .steps()
                .iter()
                .any(|s| matches!(s, Step::Existing(GremlinStep::Limit(_))))
        {
            rows.truncate(100);
        }
        let projected = match query.steps().last() {
            Some(Step::ValueMap(keys)) => Some(self.project_rows(&rows, keys).await?),
            _ => None,
        };
        let output = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: if let Some(sessions) = &self.fixed_sessions {
                sessions
                    .values()
                    .map(|s| s.descriptor.stamp.clone())
                    .collect()
            } else {
                self.sessions
                    .values()
                    .map(|(_, d)| d.stamp.clone())
                    .collect()
            },
            rows: if let Some(projected) = projected {
                projected
            } else if count {
                vec![QueryRow::Count(rows.len() as u64)]
            } else {
                rows.into_iter()
                    .map(|r| {
                        if let Some(value) = r.value {
                            QueryRow::Value {
                                source: Some(r.vertex.element()),
                                value,
                            }
                        } else if path {
                            QueryRow::Path(r.path)
                        } else {
                            QueryRow::Element(r.vertex.element())
                        }
                    })
                    .collect()
            },
        };
        let bytes = bounded_encode(&output, self.end)?;
        self.cost.charge(bytes.len() * 2, 0, self.ceiling)?;
        self.check()?;
        Ok(output)
    }
    async fn close(&self) {
        // Cleanup does not extend the original deadline. Fixed TTL handles cancellation.
        for (route, descriptor) in self.sessions.values() {
            if Instant::now() >= self.end {
                break;
            }
            if let Ok(client) = observed_client(self.grpc_clients, route, self.end) {
                let _ = client
                    .close(
                        descriptor.clone(),
                        self.end
                            .saturating_duration_since(Instant::now())
                            .min(Duration::from_millis(20)),
                    )
                    .await;
            }
        }
    }
}

// A missing cache is a cold typed channel, never a transport fallback.
fn observed_client(
    clients: Option<&BTreeMap<u64, grpc::QueryClient>>,
    route: &PartitionRoute,
    end: Instant,
) -> Result<grpc::QueryClient, QueryFailure> {
    match clients {
        Some(clients) => clients
            .get(&route.shard)
            .cloned()
            .ok_or(QueryFailure::Unavailable),
        None => grpc::QueryClient::connect_lazy(
            route.endpoint,
            route.identity,
            end.saturating_duration_since(Instant::now()),
        )
        .map_err(remote_failure),
    }
}

async fn execute_transport(
    catalog: &StaticCatalog,
    request: DistributedRequest,
    end: Instant,
    grpc_clients: Option<&BTreeMap<u64, grpc::QueryClient>>,
) -> DistributedResponse {
    let _permit = COORDINATORS
        .try_acquire()
        .map_err(|_| QueryFailure::ResourceExhausted)?;
    check(end)?;
    let query = plan(&request)?;
    let mut execution = Execution {
        fixed_sessions: None,
        control: None,
        fixed_parallelism: 3,
        catalog,
        grpc_clients,
        ceiling: CEILING,
        scan_cost: Cost {
            bytes: 4096,
            work: 0,
        },
        sessions: BTreeMap::new(),
        end,
        cost: Cost {
            bytes: 4096,
            work: 0,
        },
    };
    execution.route(request.start_partition)?;
    let result = async {
        // Pin each unique shard before executing any traverser.
        for route in &catalog.partitions {
            if execution.sessions.contains_key(&route.shard) {
                continue;
            }
            check(end)?;
            if scan::is_global(&query) {
                execution.scan_cost.charge(1024, 1, scan::SCAN_CEILING)?;
            } else {
                execution.cost.charge(1024, 1, execution.ceiling)?;
            }
            let remaining = end.saturating_duration_since(Instant::now());
            let reply = observed_client(grpc_clients, route, end)?
                .open(route.shard, remaining)
                .await
                .map(Reply::OpenQuery)
                .map_err(remote_failure)?;
            let Reply::OpenQuery(descriptor) = reply else {
                return Err(QueryFailure::IncompleteOrCorrupt);
            };
            if descriptor.stamp.identity != route.identity
                || descriptor.stamp.shard != route.shard
                || descriptor.stamp.epoch != route.epoch
            {
                return Err(QueryFailure::IncompleteOrCorrupt);
            }
            execution
                .sessions
                .insert(route.shard, (route.clone(), descriptor));
        }
        execution.run(&request, &query).await
    }
    .await;
    execution.close().await;
    check(end)?;
    result
}

fn bounded_encode<T: Serialize>(value: &T, end: Instant) -> Result<Vec<u8>, QueryFailure> {
    bounded_serialize(value, end, true).map(|(bytes, _)| bytes)
}
// Preserve existing logical output accounting for typed wire transports without
// constructing a second serialized payload or changing result capacity.
fn bounded_encoded_size<T: Serialize>(value: &T, end: Instant) -> Result<usize, QueryFailure> {
    bounded_serialize(value, end, false).map(|(_, size)| size)
}
fn bounded_serialize<T: Serialize>(
    value: &T,
    end: Instant,
    collect: bool,
) -> Result<(Vec<u8>, usize), QueryFailure> {
    struct Writer {
        bytes: Vec<u8>,
        size: usize,
        collect: bool,
        end: Instant,
        until_deadline_check: usize,
    }
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > MAX_WIRE.saturating_sub(self.size) {
                return Err(io::Error::other("bounded query encoding"));
            }
            // serde emits many tiny tokens. Amortize small-write checks over
            // at most 8KiB, and check before every larger individual write.
            // Keep byte admission on every write and avoid an extra buffer.
            if bytes.len() >= self.until_deadline_check {
                if Instant::now() >= self.end {
                    return Err(io::Error::other("bounded query encoding"));
                }
                self.until_deadline_check = 8192;
            }
            self.until_deadline_check = self.until_deadline_check.saturating_sub(bytes.len());
            self.size += bytes.len();
            if self.collect {
                self.bytes.extend_from_slice(bytes);
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        size: 0,
        collect,
        end,
        until_deadline_check: 0,
    };
    check(end)?;
    serde_json::to_writer(&mut writer, value).map_err(|_| {
        if Instant::now() >= end {
            QueryFailure::Timeout
        } else {
            QueryFailure::ResourceExhausted
        }
    })?;
    // The final incomplete interval must not publish an expired result.
    check(end)?;
    Ok((writer.bytes, writer.size))
}

/// Typed gRPC coordinator endpoint. No JSON listener or transport fallback.
/// Restrict this plaintext experimental service to a trusted network.
pub use grpc::GrpcCoordinatorService as CoordinatorService;

/// One typed request to the selected coordinator; no retry or local execution.
/// Use `grpc::CoordinatorClient` to reuse a channel across requests.
pub async fn query(
    endpoint: SocketAddr,
    request: DistributedRequest,
    deadline: Duration,
) -> Result<DistributedResponse, RemoteError> {
    validate_request(&request)?;
    let end = checked_deadline(
        deadline.min(Duration::from_millis(request.timeout_millis)),
        None,
    )?;
    tokio::time::timeout_at(end, async {
        let client = grpc::CoordinatorClient::connect_lazy(
            endpoint,
            end.saturating_duration_since(Instant::now()),
        )?;
        client
            .query(request, end.saturating_duration_since(Instant::now()))
            .await
    })
    .await
    .unwrap_or(Err(RemoteError::Timeout { request_id: None }))
}
fn validate_output(
    output: &DistributedOutput,
    request: &DistributedRequest,
) -> Result<(), RemoteError> {
    validate_output_bounded(output, request, 8)
}
fn validate_output_bounded(
    output: &DistributedOutput,
    request: &DistributedRequest,
    max_snapshots: usize,
) -> Result<(), RemoteError> {
    if output.completeness != QueryCompleteness::Unverified
        || output.snapshots.is_empty()
        || output.snapshots.len() > max_snapshots
        || output.rows.len() > MAX_ROWS
        || output
            .snapshots
            .iter()
            .any(|s| s.epoch == 0 || s.identity.node == 0)
        || output
            .snapshots
            .windows(2)
            .any(|w| w[0].shard >= w[1].shard)
    {
        return Err(RemoteError::Protocol);
    }
    let query = plan(request).map_err(|_| RemoteError::Protocol)?;
    let count = matches!(
        query.steps().last(),
        Some(Step::Existing(GremlinStep::Count))
    );
    let path = matches!(
        query.steps().last(),
        Some(Step::Existing(GremlinStep::Path))
    );
    let scalar = query
        .steps()
        .iter()
        .any(|s| matches!(s, Step::Existing(GremlinStep::Values(_))));
    let map_keys = match query.steps().last() {
        Some(Step::ValueMap(keys)) => Some(keys.as_slice()),
        _ => None,
    };
    let directions = hierarchy::path_directions(&query);
    let hops = directions.iter().map(Vec::len).max().unwrap_or(0);
    let joined = scan::join_at(&query)
        .map_err(|_| RemoteError::Protocol)?
        .is_some();
    if scan::is_global(&query) && !joined && !count {
        let keys = output
            .rows
            .iter()
            .map(|row| match row {
                QueryRow::Element(QualifiedElement::Vertex { partition, id })
                | QueryRow::Value {
                    source: Some(QualifiedElement::Vertex { partition, id }),
                    ..
                }
                | QueryRow::PropertyMap {
                    source: QualifiedElement::Vertex { partition, id },
                    ..
                } => Ok((*partition, *id)),
                _ => Err(RemoteError::Protocol),
            })
            .collect::<Result<Vec<_>, _>>()?;
        let cap = query
            .steps()
            .iter()
            .filter_map(|s| match s {
                Step::Existing(GremlinStep::Limit(n)) => Some(*n),
                _ => None,
            })
            .min()
            .unwrap_or(100);
        if keys.len() > cap || keys.windows(2).any(|w| w[0] >= w[1]) {
            return Err(RemoteError::Protocol);
        }
    }
    let out = hops != 0 || scan::is_global(&query);
    let valid_vertex = |v: &QualifiedElement| matches!(v, QualifiedElement::Vertex { partition, .. } if out || *partition == request.start_partition);
    if count {
        return if matches!(output.rows.as_slice(), [QueryRow::Count(n)] if *n <= if scan::is_global(&query) && !joined { scan::SCAN_CEILING.work as u64 } else { MAX_ROWS as u64 })
        {
            Ok(())
        } else {
            Err(RemoteError::Protocol)
        };
    }
    for row in &output.rows {
        let valid = match row {
            QueryRow::Element(v) => !path && !scalar && map_keys.is_none() && valid_vertex(v),
            QueryRow::PropertyMap { source, properties } => {
                valid_vertex(source) && map_keys.is_some_and(|keys| {
                    sync_projection::valid_map(properties, keys)
                })
            }
            QueryRow::Path(p) if path => {
                matches!(p.first(), Some(QualifiedElement::Vertex { partition, .. }) if *partition == request.start_partition)
                    && directions.iter().any(|directions| {
                        p.len() == 1 + 2 * directions.len()
                        && p.windows(3).step_by(2).zip(directions).all(|(hop, direction)| {
                            matches!(hop,
                                [QualifiedElement::Vertex { partition: source, .. },
                                 QualifiedElement::Edge { partition: owner, .. },
                                 QualifiedElement::Vertex { partition: target, .. }] if direction.admits_owner(*owner, *source, *target))
                        })
                    })
            }
            QueryRow::Value {
                source: Some(source),
                value,
            } => {
                scalar && valid_vertex(source) && !matches!(value, Value::Real(n) if !n.is_finite())
            }
            _ => false,
        };
        if !valid {
            return Err(RemoteError::Protocol);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn request(text: &str) -> DistributedRequest {
        DistributedRequest {
            start_partition: 1,
            text: text.into(),
            timeout_millis: 5000,
        }
    }
    fn route(partition: u32) -> PartitionRoute {
        PartitionRoute {
            partition,
            shard: 1,
            epoch: 2,
            identity: RemoteIdentity { group: 3, node: 1 },
            endpoint: "127.0.0.1:10000".parse().unwrap(),
        }
    }
    #[test]
    fn fixed_membership_is_independent_of_observed_fanout_and_lane_width() {
        let routes: Vec<_> = (1..=65)
            .map(|n| PartitionRoute {
                partition: n,
                shard: u64::from(n),
                epoch: 1,
                identity: RemoteIdentity {
                    group: u64::from(n),
                    node: 1,
                },
                endpoint: format!("127.0.0.1:{}", 10000 + n).parse().unwrap(),
            })
            .collect();
        for count in [1, 8, 9, 32, 64] {
            let catalog = StaticCatalog {
                partitions: routes[..count].to_vec(),
            };
            assert!(catalog
                .validate_bounded(fixed::MAX_MEMBERS, fixed::MAX_MEMBERS)
                .is_ok());
            assert_eq!(catalog.validate().is_ok(), count <= 8);
        }
        let mut catalog = StaticCatalog { partitions: routes };
        assert!(catalog
            .validate_bounded(fixed::MAX_MEMBERS, fixed::MAX_MEMBERS)
            .is_err());
        catalog.partitions.pop();
        catalog.partitions[63].endpoint = catalog.partitions[0].endpoint;
        assert!(catalog
            .validate_bounded(fixed::MAX_MEMBERS, fixed::MAX_MEMBERS)
            .is_err());
        assert!(fixed_parallel::validate_width(8).is_ok());
        assert!(fixed_parallel::validate_width(9).is_err());
    }
    #[test]
    fn fixed_output_membership_preserves_observed_snapshot_bound() {
        let request = request("g.V(1).count()");
        for count in [1, 8, 9, 64, 65] {
            let output = DistributedOutput {
                completeness: QueryCompleteness::Unverified,
                snapshots: (1..=count)
                    .map(|shard| QueryStamp {
                        identity: RemoteIdentity {
                            group: shard,
                            node: 1,
                        },
                        shard,
                        epoch: 1,
                        sequence: 0,
                    })
                    .collect(),
                rows: vec![QueryRow::Count(0)],
            };
            assert_eq!(validate_output(&output, &request).is_ok(), count <= 8);
            assert_eq!(
                validate_output_bounded(&output, &request, fixed::MAX_MEMBERS).is_ok(),
                count <= 64
            );
        }
    }
    #[test]
    fn static_catalog_rejects_duplicate_and_conflicting_ownership() {
        assert!(StaticCatalog {
            partitions: vec![route(1), route(2)]
        }
        .validate()
        .is_ok());
        assert!(StaticCatalog {
            partitions: vec![route(1), route(1)]
        }
        .validate()
        .is_err());
        let mut conflicting = route(2);
        conflicting.epoch += 1;
        assert!(StaticCatalog {
            partitions: vec![route(1), conflicting]
        }
        .validate()
        .is_err());
        let mut conflicting = route(2);
        conflicting.shard += 1;
        assert!(StaticCatalog {
            partitions: vec![route(1), conflicting]
        }
        .validate()
        .is_err());
    }
    #[test]
    fn plan_is_validated_before_empty_stream_can_hide_unsupported_steps() {
        for text in [
            "g.E(1)",
            "g.V(999).both('a','b')",
            "g.V(999).values('a').both()",
            "g.V(999).repeat(__.both()).times(2)",
            "g.V(999).values('a').has('a')",
            "g.V(999).id()",
            "g.V(999).values('a').dedup()",
            "g.V(999).path().dedup()",
            "g.V(999).dedup('label')",
            "g.V(999).dedup().by('name')",
            "g.V(999).dedup(local)",
            "g.V(999).outE().dedup()",
        ] {
            assert!(plan(&request(text)).is_err(), "{text}");
        }
        for text in [
            "g.V(999).both()",
            "g.V(1,1).both('a').path()",
            "g.V(1).both().in().both().path()",
            "g.V(1).both().repeat(__.in()).times(31).path()",
            "g.V(1,1).out('a').path()",
            "g.V(1).out().out().out().path()",
            "g.V(1).in().out().in().path()",
            "g.V(999).limit(1).count()",
            "g.V(1,1).dedup().in().dedup().path()",
            "g.V(1).out().in().dedup().count()",
            "g.V(1).has('a',true).out().limit(1).values('a').count()",
        ] {
            assert!(plan(&request(text)).is_ok(), "{text}");
        }
    }
    #[test]
    fn hop_admission_is_bounded_even_for_empty_or_limited_queries() {
        for text in [
            "g.V(999).both().both().both().both().count()",
            "g.V(999).both().repeat(__.in()).times(32).count()",
            "g.V(999).out().out().out().out().count()",
            "g.V(1).limit(1).out().out().out().out()",
        ] {
            assert_eq!(
                plan(&request(text)).unwrap_err(),
                QueryFailure::ResourceExhausted
            );
        }
    }

    #[test]
    fn client_validates_every_hop_and_exact_path_length() {
        let vertex = |partition| QualifiedElement::Vertex { partition, id: 1 };
        let edge = |partition| QualifiedElement::Edge { partition, id: 1 };
        let path = vec![
            vertex(1),
            edge(1),
            vertex(2),
            edge(2),
            vertex(1),
            edge(1),
            vertex(2),
        ];
        let mut output = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: vec![QueryStamp {
                identity: RemoteIdentity { group: 1, node: 1 },
                shard: 7,
                epoch: 1,
                sequence: 1,
            }],
            rows: vec![QueryRow::Path(path.clone())],
        };
        let req = request("g.V(1).out().out().out().path()");
        validate_output(&output, &req).unwrap();
        for index in 0..path.len() {
            let mut bad = path.clone();
            bad[index] = if index % 2 == 0 { edge(1) } else { vertex(1) };
            output.rows = vec![QueryRow::Path(bad)];
            assert!(validate_output(&output, &req).is_err());
        }
        for index in [1, 3, 5] {
            let mut bad = path.clone();
            bad[index] = edge(99);
            output.rows = vec![QueryRow::Path(bad)];
            assert!(validate_output(&output, &req).is_err());
        }
        for length in 0..path.len() {
            output.rows = vec![QueryRow::Path(path[..length].to_vec())];
            assert!(validate_output(&output, &req).is_err());
        }
        output.rows = vec![QueryRow::Path(path)];
        assert!(validate_output(&output, &request("g.V(1).out().out().path()")).is_err());
    }

    #[test]
    fn dedup_uses_qualified_identity_first_path_and_cumulative_budget() {
        let row = |partition, id, via| Traverser {
            vertex: VertexId { partition, id },
            path: vec![QualifiedElement::Vertex {
                partition: via,
                id: 99,
            }],
            value: None,
        };
        let end = Instant::now() + Duration::from_secs(5);
        let mut cost = Cost::default();
        let result = dedup_vertices(
            vec![row(2, 1, 3), row(1, 1, 4), row(2, 1, 5)],
            &mut cost,
            end,
        )
        .unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0].vertex,
            VertexId {
                partition: 2,
                id: 1
            }
        );
        assert_eq!(
            result[1].vertex,
            VertexId {
                partition: 1,
                id: 1
            }
        );
        assert_eq!(result[0].path, row(2, 1, 3).path);
        for (bytes, work) in [(MAX_BYTES - 127, 0), (0, MAX_WORK)] {
            let mut cost = Cost::default();
            cost.charge(bytes, work, CEILING).unwrap();
            assert!(matches!(
                dedup_vertices(vec![row(1, 1, 1)], &mut cost, end),
                Err(QueryFailure::ResourceExhausted)
            ));
        }
        assert!(matches!(
            dedup_vertices(
                vec![row(1, 1, 1)],
                &mut Cost::default(),
                Instant::now() - Duration::from_millis(1)
            ),
            Err(QueryFailure::Timeout)
        ));
    }

    #[test]
    fn global_budget_is_cumulative_and_never_refunded_by_limit_or_count() {
        let mut cost = Cost::default();
        cost.charge(MAX_BYTES - 1, MAX_WORK - 1, CEILING).unwrap();
        assert_eq!(cost.remaining().bytes, 1);
        assert!(cost.charge(2, 0, CEILING).is_err());
        let mut cost = Cost::default();
        cost.charge(0, MAX_WORK, CEILING).unwrap();
        assert!(cost.charge(0, 1, CEILING).is_err());
        let mut cost = Cost::default();
        assert!(cost.charge(usize::MAX, 0, CEILING).is_err());
    }
    #[test]
    fn bounded_output_and_expired_execution_fail_closed() {
        let expired = Instant::now() - Duration::from_millis(1);
        assert_eq!(
            bounded_encode(&vec![0; 1], expired).unwrap_err(),
            QueryFailure::Timeout
        );
        assert_eq!(
            bounded_encode(
                &"x".repeat(MAX_WIRE),
                Instant::now() + Duration::from_secs(1)
            )
            .unwrap_err(),
            QueryFailure::ResourceExhausted
        );
    }
    #[test]
    fn amortized_deadline_preserves_exact_json_and_wire_boundary() {
        let end = Instant::now() + Duration::from_secs(5);
        for length in [0, 1, 8190, 8191, 8192, 8193, 16384, MAX_WIRE - 2] {
            let value = "x".repeat(length);
            let encoded = bounded_encode(&value, end).unwrap();
            assert_eq!(encoded, serde_json::to_vec(&value).unwrap());
            assert_eq!(encoded.len(), length + 2);
            assert_eq!(bounded_encoded_size(&value, end).unwrap(), encoded.len());
        }
        for length in [MAX_WIRE - 1, MAX_WIRE, MAX_WIRE + 1] {
            assert_eq!(
                bounded_encode(&"x".repeat(length), end).unwrap_err(),
                QueryFailure::ResourceExhausted
            );
        }
        for length in [MAX_WIRE - 1, MAX_WIRE, MAX_WIRE + 1] {
            assert_eq!(
                bounded_encoded_size(&"x".repeat(length), end).unwrap_err(),
                QueryFailure::ResourceExhausted
            );
        }
        let escaped = serde_json::json!({ "unicode": "部门", "nested": ["\u{0000}".repeat(8192), "\\\"\n".repeat(8192)] });
        assert_eq!(
            bounded_encode(&escaped, end).unwrap(),
            serde_json::to_vec(&escaped).unwrap()
        );
    }

    #[test]
    fn amortized_deadline_is_independent_of_graph_result_distribution() {
        fn check_rows(rows: Vec<QueryRow>) {
            let reference = serde_json::to_vec(&rows).unwrap();
            let actual = bounded_encode(&rows, Instant::now() + Duration::from_secs(5));
            if reference.len() > MAX_WIRE {
                assert_eq!(actual.unwrap_err(), QueryFailure::ResourceExhausted);
            } else {
                let actual = actual.unwrap();
                assert_eq!(actual, reference);
                assert_eq!(
                    serde_json::from_slice::<Vec<QueryRow>>(&actual).unwrap(),
                    rows
                );
            }
        }
        let vertex = |id| QualifiedElement::Vertex {
            partition: (id % 3) as u32,
            id,
        };
        // Serialization is unaware of topology or query syntax. Exercise short
        // aggregates, sparse IDs, repeated high-fanout bags, and long paths.
        for width in [0, 1, 17, 257, 4096] {
            check_rows(vec![QueryRow::Count(width as u64)]);
            check_rows(
                (0..width)
                    .map(|id| QueryRow::Element(vertex(id as u64 * 101)))
                    .collect(),
            );
            check_rows((0..width).map(|_| QueryRow::Element(vertex(9))).collect());
            check_rows(
                (0..width)
                    .map(|id| {
                        QueryRow::Path((0..32).map(|hop| vertex((id + hop) as u64)).collect())
                    })
                    .collect(),
            );
        }
        check_rows(
            [
                Value::Null,
                Value::Integer(i64::MIN),
                Value::Real(0.125),
                Value::Boolean(true),
                Value::Text("\u{0000}\n\"部门".repeat(2048)),
            ]
            .into_iter()
            .map(|value| QueryRow::Value {
                source: Some(vertex(42)),
                value,
            })
            .collect(),
        );
        check_rows(vec![QueryRow::Count(u64::MAX)]);
    }

    #[test]
    fn amortized_deadline_checks_final_partial_interval() {
        struct ExpireAfterWriting {
            end: Instant,
        }
        impl Serialize for ExpireAfterWriting {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let result = serializer.serialize_u8(1)?;
                while Instant::now() < self.end {
                    std::thread::yield_now();
                }
                Ok(result)
            }
        }
        let end = Instant::now() + Duration::from_millis(2);
        assert_eq!(
            bounded_encode(&ExpireAfterWriting { end }, end).unwrap_err(),
            QueryFailure::Timeout
        );
    }

    #[test]
    fn amortized_deadline_propagates_serializer_error() {
        struct Refused;
        impl Serialize for Refused {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                use serde::ser::SerializeSeq;
                let mut sequence = serializer.serialize_seq(Some(2))?;
                sequence.serialize_element(&"x".repeat(8193))?;
                Err(serde::ser::Error::custom("deliberate encoding failure"))
            }
        }
        assert_eq!(
            bounded_encode(&Refused, Instant::now() + Duration::from_secs(1)).unwrap_err(),
            QueryFailure::ResourceExhausted
        );
    }

    #[test]
    fn client_validates_edge_owner_using_each_hop_direction() {
        let v = |partition| QualifiedElement::Vertex { partition, id: 1 };
        let e = |partition| QualifiedElement::Edge { partition, id: 1 };
        let mut result = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: vec![QueryStamp {
                identity: RemoteIdentity { group: 1, node: 1 },
                shard: 1,
                epoch: 1,
                sequence: 1,
            }],
            rows: vec![QueryRow::Path(vec![
                v(1),
                e(2),
                v(2),
                e(2),
                v(3),
                e(1),
                v(1),
            ])],
        };
        let req = request("g.V(1).in().out().in().path()");
        validate_output(&result, &req).unwrap();
        for slot in [1, 3, 5] {
            let QueryRow::Path(original) = &result.rows[0] else {
                panic!()
            };
            let mut bad = original.clone();
            bad[slot] = e(99);
            let good = std::mem::replace(&mut result.rows[0], QueryRow::Path(bad));
            assert!(validate_output(&result, &req).is_err());
            result.rows[0] = good;
        }
        assert!(plan(&request("g.V(999).in().out().in().out().count()")).is_err());
    }

    #[test]
    fn client_rejects_wrong_qualified_paths_and_result_shapes() {
        let stamp = QueryStamp {
            identity: RemoteIdentity { group: 1, node: 1 },
            shard: 7,
            epoch: 1,
            sequence: 1,
        };
        let source = QualifiedElement::Vertex {
            partition: 1,
            id: 1,
        };
        let target = QualifiedElement::Vertex {
            partition: 2,
            id: 1,
        };
        let mut output = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: vec![stamp],
            rows: vec![QueryRow::Path(vec![
                source,
                QualifiedElement::Edge {
                    partition: 1,
                    id: 9,
                },
                target,
            ])],
        };
        let req = request("g.V(1).out().path()");
        validate_output(&output, &req).unwrap();
        output.rows = vec![QueryRow::Path(vec![
            source,
            QualifiedElement::Edge {
                partition: 2,
                id: 9,
            },
            target,
        ])];
        assert!(validate_output(&output, &req).is_err());
        output.rows = vec![QueryRow::Count(1), QueryRow::Count(1)];
        assert!(validate_output(&output, &request("g.V(1).count()")).is_err());
        output.rows = vec![QueryRow::Value {
            source: Some(target),
            value: Value::Null,
        }];
        assert!(validate_output(&output, &request("g.V(1).values('a')")).is_err());
    }
}
