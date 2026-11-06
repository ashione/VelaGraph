//! Experimental server coordination over a caller-maintained immutable catalog.
//! No snapshot downloads, retries, global consistency, or completeness claim.
use super::*;
use query::{QualifiedElement, QueryCompleteness, QueryFailure, QueryRow, QueryStamp};
use rocksdb_sql_table::{GremlinQuery, GremlinStart, GremlinStep, Value};
use std::collections::BTreeMap;
mod sessions;
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
        if self.partitions.is_empty() || self.partitions.len() > 32 {
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
        if shards.len() > 8 {
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
enum Fragment {
    Exists(VertexId),
    Out(VertexId),
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
}
impl Fragment {
    fn validate(&self) -> Result<(), RemoteError> {
        let bytes = match self {
            Self::Exists(_) | Self::Out(_) => 0,
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
        };
        if bytes > query::MAX_QUERY_TEXT_BYTES {
            return Err(RemoteError::ResourceExhausted);
        }
        Ok(())
    }
    fn partition(&self) -> u32 {
        match self {
            Self::Exists(v) | Self::Out(v) => v.partition,
            Self::VerifyReverse(e) => e.to.partition,
            Self::Has { vertex, .. }
            | Self::HasLabel { vertex, .. }
            | Self::Values { vertex, .. } => vertex.partition,
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
    Bool(bool),
    Edges(Vec<EdgeHop>),
    Value(Option<Value>),
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
    if !matches!(query.start(), GremlinStart::Vertices(ids) if !ids.is_empty()) {
        return Err(QueryFailure::Unsupported);
    }
    let mut outs = 0;
    let mut scalar = false;
    for (index, step) in query.steps().iter().enumerate() {
        if scalar && !matches!(step, GremlinStep::Limit(_) | GremlinStep::Count) {
            return Err(QueryFailure::Unsupported);
        }
        match step {
            GremlinStep::Out(_) => {
                outs += 1;
                if outs > 1 {
                    return Err(QueryFailure::Unsupported);
                }
            }
            GremlinStep::Has { .. } | GremlinStep::HasLabel(_) | GremlinStep::Limit(_) => {}
            GremlinStep::Values(_) => scalar = true,
            GremlinStep::Path | GremlinStep::Count if index + 1 == query.steps().len() => {}
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
struct Execution<'a> {
    catalog: &'a StaticCatalog,
    sessions: BTreeMap<u64, (PartitionRoute, Descriptor)>,
    end: Instant,
    cost: Cost,
}
impl Execution<'_> {
    fn route(&self, partition: u32) -> Result<&PartitionRoute, QueryFailure> {
        self.catalog
            .partitions
            .iter()
            .find(|p| p.partition == partition)
            .ok_or(QueryFailure::UnsupportedScope)
    }
    async fn fragment(&mut self, fragment: Fragment) -> Result<FragmentValue, QueryFailure> {
        check(self.end)?;
        let route = self.route(fragment.partition())?.clone();
        let descriptor = self
            .sessions
            .get(&route.shard)
            .ok_or(QueryFailure::Unavailable)?
            .1
            .clone();
        // Charge request encoding and transport work before allocating/sending.
        self.cost.charge(512, 1, CEILING)?;
        let request = FragmentRequest {
            descriptor: descriptor.clone(),
            fragment,
            remaining: self.cost.remaining(),
        };
        let encoded = serde_json::to_vec(&request).map_err(|_| QueryFailure::InvalidArgument)?;
        self.cost.charge(encoded.len() * 2, 0, CEILING)?;
        let request = FragmentRequest {
            remaining: self.cost.remaining(),
            ..request
        };
        let reply = rpc(
            route.endpoint,
            Some(route.identity),
            Operation::QueryFragment(request),
            self.end.saturating_duration_since(Instant::now()),
            None,
        )
        .await
        .map_err(remote_failure)?;
        check(self.end)?;
        let Reply::QueryFragment(response) = reply else {
            return Err(QueryFailure::IncompleteOrCorrupt);
        };
        let output = response?;
        if output.descriptor != descriptor {
            return Err(QueryFailure::IncompleteOrCorrupt);
        }
        self.cost
            .charge(output.cost.bytes, output.cost.work, CEILING)?;
        Ok(output.value)
    }
    async fn boolean(&mut self, fragment: Fragment) -> Result<bool, QueryFailure> {
        match self.fragment(fragment).await? {
            FragmentValue::Bool(value) => Ok(value),
            _ => Err(QueryFailure::IncompleteOrCorrupt),
        }
    }
    async fn run(
        &mut self,
        request: &DistributedRequest,
        query: &GremlinQuery,
    ) -> DistributedResponse {
        self.route(request.start_partition)?;
        let mut rows = Vec::new();
        let GremlinStart::Vertices(ids) = query.start() else {
            return Err(QueryFailure::Unsupported);
        };
        for id in ids {
            if matches!(query.steps().first(), Some(GremlinStep::Limit(n)) if rows.len() >= *n) {
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
        for step in query.steps() {
            check(self.end)?;
            match step {
                GremlinStep::Limit(n) => rows.truncate(*n),
                GremlinStep::Out(label) => {
                    let mut next = Vec::new();
                    for row in rows {
                        let FragmentValue::Edges(edges) =
                            self.fragment(Fragment::Out(row.vertex)).await?
                        else {
                            return Err(QueryFailure::IncompleteOrCorrupt);
                        };
                        if edges.len() > 1000
                            || edges.windows(2).any(|w| {
                                (w[0].id, w[0].from.partition) >= (w[1].id, w[1].from.partition)
                            })
                        {
                            return Err(QueryFailure::IncompleteOrCorrupt);
                        }
                        for edge in edges {
                            if edge.from != row.vertex {
                                return Err(QueryFailure::IncompleteOrCorrupt);
                            }
                            // Verify every observed edge, even if the label subsequently drops it.
                            if !self.boolean(Fragment::VerifyReverse(edge.clone())).await? {
                                return Err(QueryFailure::IncompleteOrCorrupt);
                            }
                            if label.as_ref().is_some_and(|l| l != &edge.label) {
                                continue;
                            }
                            self.cost.charge(row.path.len() * 32, 0, CEILING)?;
                            let mut path = row.path.clone();
                            path.push(QualifiedElement::Edge {
                                partition: edge.from.partition,
                                id: edge.id,
                            });
                            path.push(edge.to.element());
                            push(
                                &mut next,
                                Traverser {
                                    vertex: edge.to,
                                    path,
                                    value: None,
                                },
                                &mut self.cost,
                            )?;
                        }
                    }
                    rows = next;
                }
                GremlinStep::Has { .. } | GremlinStep::HasLabel(_) => {
                    let mut next = Vec::new();
                    for row in rows {
                        let fragment = match step {
                            GremlinStep::Has { key, value } => Fragment::Has {
                                vertex: row.vertex,
                                key: key.clone(),
                                value: value.clone(),
                            },
                            GremlinStep::HasLabel(labels) => Fragment::HasLabel {
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
                GremlinStep::Values(key) => {
                    let mut next = Vec::new();
                    for mut row in rows {
                        let FragmentValue::Value(value) = self
                            .fragment(Fragment::Values {
                                vertex: row.vertex,
                                key: key.clone(),
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
                    rows = next;
                }
                GremlinStep::Path | GremlinStep::Count => {}
                _ => return Err(QueryFailure::Unsupported),
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
        let output = DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: self
                .sessions
                .values()
                .map(|(_, d)| d.stamp.clone())
                .collect(),
            rows: if count {
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
        self.cost.charge(bytes.len() * 2, 0, CEILING)?;
        check(self.end)?;
        Ok(output)
    }
    async fn close(&self) {
        // Cleanup does not extend the original deadline. Fixed TTL handles cancellation.
        for (route, descriptor) in self.sessions.values() {
            if Instant::now() >= self.end {
                break;
            }
            let _ = rpc(
                route.endpoint,
                Some(route.identity),
                Operation::CloseQuery {
                    descriptor: descriptor.clone(),
                },
                self.end
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(20)),
                None,
            )
            .await;
        }
    }
}

async fn execute(
    catalog: &StaticCatalog,
    request: DistributedRequest,
    end: Instant,
) -> DistributedResponse {
    let _permit = COORDINATORS
        .try_acquire()
        .map_err(|_| QueryFailure::ResourceExhausted)?;
    check(end)?;
    let query = plan(&request)?;
    let mut execution = Execution {
        catalog,
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
            execution.cost.charge(1024, 1, CEILING)?;
            let remaining = end.saturating_duration_since(Instant::now());
            let reply = rpc(
                route.endpoint,
                Some(route.identity),
                Operation::OpenQuery {
                    shard: route.shard,
                    timeout_millis: remaining.as_millis().min(MAX_TIMEOUT as u128) as u64,
                },
                remaining,
                None,
            )
            .await
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
    struct Writer {
        bytes: Vec<u8>,
        end: Instant,
    }
    impl io::Write for Writer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if Instant::now() >= self.end || bytes.len() > MAX_WIRE.saturating_sub(self.bytes.len())
            {
                return Err(io::Error::other("bounded query encoding"));
            }
            self.bytes.extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Writer {
        bytes: Vec::new(),
        end,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| {
        if Instant::now() >= end {
            QueryFailure::Timeout
        } else {
            QueryFailure::ResourceExhausted
        }
    })?;
    Ok(writer.bytes)
}

/// Dedicated coordinator endpoint; it cannot mutate graph state or choose endpoints from a request.
/// All callers must explicitly restrict this plaintext service to a trusted network.
pub struct CoordinatorService {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl CoordinatorService {
    pub async fn start(
        bind: SocketAddr,
        catalog: StaticCatalog,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        catalog.validate()?;
        checked_deadline(deadline, None)?;
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let address = listener
            .local_addr()
            .map_err(|_| RemoteError::Unavailable)?;
        let (stop, mut stopped) = watch::channel(false);
        let catalog = Arc::new(catalog);
        let task = tokio::spawn(async move {
            let mut handlers = JoinSet::new();
            let admission = Arc::new(Semaphore::new(2));
            loop {
                let accepted = tokio::select! { _ = stopped.changed() => break, _ = handlers.join_next(), if !handlers.is_empty() => continue, value = listener.accept() => value };
                let Ok((mut socket, _)) = accepted else {
                    break;
                };
                let Ok(permit) = admission.clone().try_acquire_owned() else {
                    continue;
                };
                let catalog = catalog.clone();
                handlers.spawn(async move {
                    let _permit = permit;
                    let end = Instant::now() + deadline.min(Duration::from_millis(MAX_TIMEOUT));
                    let work = async {
                        let request: DistributedRequest = read_frame_limited(
                            &mut socket,
                            end,
                            query::MAX_QUERY_TEXT_BYTES + 1024,
                        )
                        .await?;
                        validate_request(&request)?;
                        let end =
                            end.min(Instant::now() + Duration::from_millis(request.timeout_millis));
                        let response = execute(&catalog, request, end).await;
                        let bytes = bounded_encode(&response, end)
                            .map_err(|_| RemoteError::ResourceExhausted)?;
                        tokio::time::timeout_at(end, async {
                            socket.write_u32(bytes.len() as u32).await?;
                            socket.write_all(&bytes).await
                        })
                        .await
                        .map_err(|_| RemoteError::Timeout { request_id: None })?
                        .map_err(|_| RemoteError::Unavailable)
                    };
                    let _ = tokio::time::timeout_at(end, work).await;
                });
            }
            handlers.abort_all();
            while handlers.join_next().await.is_some() {}
        });
        Ok(Self {
            address,
            stop,
            task: Some(task),
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for CoordinatorService {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

/// One request to the selected coordinator. No client-side graph execution or fallback.
pub async fn query(
    endpoint: SocketAddr,
    request: DistributedRequest,
    deadline: Duration,
) -> Result<DistributedResponse, RemoteError> {
    validate_request(&request)?;
    let end = checked_deadline(deadline, None)?;
    let _permit = CLIENT_ADMISSION
        .try_acquire()
        .map_err(|_| RemoteError::ResourceExhausted)?;
    tokio::time::timeout_at(end, async {
        let mut socket = TcpStream::connect(endpoint)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        write_frame(&mut socket, &request, end).await?;
        let result: DistributedResponse = read_frame_limited(&mut socket, end, MAX_WIRE).await?;
        if let Ok(output) = &result {
            validate_output(output, &request)?;
        }
        if Instant::now() >= end {
            return Err(RemoteError::Timeout { request_id: None });
        }
        Ok(result)
    })
    .await
    .unwrap_or(Err(RemoteError::Timeout { request_id: None }))
}
fn validate_output(
    output: &DistributedOutput,
    request: &DistributedRequest,
) -> Result<(), RemoteError> {
    if output.completeness != QueryCompleteness::Unverified
        || output.snapshots.is_empty()
        || output.snapshots.len() > 8
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
    let count = matches!(query.steps().last(), Some(GremlinStep::Count));
    let path = matches!(query.steps().last(), Some(GremlinStep::Path));
    let scalar = query
        .steps()
        .iter()
        .any(|s| matches!(s, GremlinStep::Values(_)));
    let out = query
        .steps()
        .iter()
        .any(|s| matches!(s, GremlinStep::Out(_)));
    let valid_vertex = |v: &QualifiedElement| matches!(v, QualifiedElement::Vertex { partition, .. } if out || *partition == request.start_partition);
    if count {
        return if matches!(output.rows.as_slice(), [QueryRow::Count(n)] if *n <= MAX_ROWS as u64) {
            Ok(())
        } else {
            Err(RemoteError::Protocol)
        };
    }
    for row in &output.rows {
        let valid = match row {
            QueryRow::Element(v) => !path && !scalar && valid_vertex(v),
            QueryRow::Path(p) if path => match p.as_slice() {
                [QualifiedElement::Vertex { partition, .. }] => {
                    !out && *partition == request.start_partition
                }
                [QualifiedElement::Vertex {
                    partition: source, ..
                }, QualifiedElement::Edge {
                    partition: owner, ..
                }, QualifiedElement::Vertex { .. }] => {
                    out && *source == request.start_partition && owner == source
                }
                _ => false,
            },
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
            "g.V()",
            "g.E(1)",
            "g.V(999).out().out()",
            "g.V(999).in()",
            "g.V(999).values('a').has('a')",
            "g.V(999).id()",
        ] {
            assert!(plan(&request(text)).is_err(), "{text}");
        }
        for text in [
            "g.V(1,1).out('a').path()",
            "g.V(999).limit(1).count()",
            "g.V(1).has('a',true).out().limit(1).values('a').count()",
        ] {
            assert!(plan(&request(text)).is_ok(), "{text}");
        }
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
