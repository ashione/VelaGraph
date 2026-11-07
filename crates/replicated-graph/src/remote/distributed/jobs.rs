//! Incremental native count jobs. Progress is not a partial query result.
use super::*;
use std::sync::Mutex;

pub(super) const LEASE_MILLIS: u64 = 120_000;
pub(super) const BATCH_ROWS: usize = 128;
// One adjacency is bounded to 1,000 entries. Sixteen frontiers therefore
// cannot exceed the 16,384-entry work-page bound, even before label filtering.
const ADVANCE_ROWS: usize = 16;
pub(super) const JOB_CEILING: Cost = Cost {
    bytes: 1024 * 1024 * 1024,
    work: 8_000_000,
};
const MAX_KEYS: usize = 250_000;
const MAX_MASS: u64 = 1_000_000_000;
pub(super) const MAX_PAGE_EDGES: usize = 16_384;
static JOB_SLOTS: Semaphore = Semaphore::const_new(2);
static RETAINED_SLOTS: Semaphore = Semaphore::const_new(128);

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Request {
    Start { start_partition: u32, text: String },
    Advance { token: [u8; 16], sequence: u64 },
    Cancel { token: [u8; 16] },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Output {
    Progress {
        token: [u8; 16],
        next_sequence: u64,
        complete: bool,
        count: Option<u64>,
        completeness: QueryCompleteness,
        snapshots: Vec<QueryStamp>,
    },
    Cancelled,
}
pub type Response = Result<Output, QueryFailure>;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Envelope {
    pub hierarchy_job: Request,
}

struct Plan {
    ids: Vec<u64>,
    incoming: bool,
    label: Option<String>,
    times: usize,
    emit: bool,
    dedup: bool,
}
fn parse(text: &str) -> Result<Plan, QueryFailure> {
    let query = GremlinQuery::parse(text).map_err(query::graph_error)?;
    hierarchy::validate_depth(&query)?;
    let GremlinStart::Vertices(ids) = query.start() else {
        return Err(QueryFailure::Unsupported);
    };
    if ids.is_empty() {
        return Err(QueryFailure::Unsupported);
    }
    let (repeat, dedup) = match query.steps() {
        [repeat, Step::Existing(GremlinStep::Count)] => (repeat, false),
        [repeat, Step::Dedup, Step::Existing(GremlinStep::Count)] => (repeat, true),
        _ => return Err(QueryFailure::Unsupported),
    };
    let Step::Repeat {
        incoming,
        label,
        times,
        emit,
    } = repeat
    else {
        return Err(QueryFailure::Unsupported);
    };
    Ok(Plan {
        ids: ids.clone(),
        incoming: *incoming,
        label: label.clone(),
        times: *times,
        emit: *emit,
        dedup,
    })
}
// Only emit().dedup().count() retains every emitted identity. Share its
// membership and both depth bags, rather than charging the same key twice.
// Alternating slots avoid an unbounded reset pass at depth transitions.
const SCAN_ROWS: usize = 128;
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct IdentityState {
    weights: [u64; 2],
    seen: bool,
}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Compact {
    identities: BTreeMap<VertexId, IdentityState>,
    slot: usize,
    cursor: Option<VertexId>,
    current_keys: usize,
    next_keys: usize,
    next_mass: u64,
    total: u64,
}
impl Compact {
    fn seed(&mut self, vertex: VertexId, cost: &mut Cost) -> Result<(), QueryFailure> {
        let old = self.identities.get(&vertex).copied().unwrap_or_default();
        let additional = usize::from(!self.identities.contains_key(&vertex));
        if self.identities.len() + additional > MAX_KEYS {
            return Err(QueryFailure::ResourceExhausted);
        }
        let weight = checked(old.weights[self.slot], 1)?;
        let mut charged = *cost;
        charged.charge(128 * additional, 1, JOB_CEILING)?;
        let state = self.identities.entry(vertex).or_default();
        self.current_keys += usize::from(state.weights[self.slot] == 0);
        state.weights[self.slot] = weight;
        *cost = charged;
        Ok(())
    }
    fn accept(
        &mut self,
        vertex: VertexId,
        weight: u64,
        cost: &mut Cost,
    ) -> Result<(), QueryFailure> {
        let old = self.identities.get(&vertex).copied().unwrap_or_default();
        let additional = usize::from(!self.identities.contains_key(&vertex));
        if self.identities.len() + additional > MAX_KEYS {
            return Err(QueryFailure::ResourceExhausted);
        }
        let next = 1 - self.slot;
        let weight_sum = checked(old.weights[next], weight)?;
        let mass = checked(self.next_mass, weight)?;
        let total = checked(self.total, u64::from(!old.seen))?;
        let mut charged = *cost;
        charged.charge(128 * additional, 1, JOB_CEILING)?;
        let state = self.identities.entry(vertex).or_default();
        self.next_keys += usize::from(state.weights[next] == 0 && weight_sum != 0);
        state.weights[next] = weight_sum;
        state.seen = true;
        self.next_mass = mass;
        self.total = total;
        *cost = charged;
        Ok(())
    }
    fn take(
        &mut self,
        cost: &mut Cost,
        end: Instant,
    ) -> Result<Vec<(VertexId, u64)>, QueryFailure> {
        use std::ops::Bound::{Excluded, Unbounded};
        let lower = self.cursor.map_or(Unbounded, Excluded);
        let mut current = Vec::new();
        let mut cursor = self.cursor;
        let mut scanned = 0;
        // Historical identities are bounded work too. New next-depth keys on
        // either side of this cursor cannot enter the current bag.
        for (vertex, state) in self.identities.range((lower, Unbounded)).take(SCAN_ROWS) {
            check(end)?;
            scanned += 1;
            cursor = Some(*vertex);
            if state.weights[self.slot] != 0 {
                current.push((*vertex, state.weights[self.slot]));
                if current.len() == ADVANCE_ROWS || current.len() == self.current_keys {
                    break;
                }
            }
        }
        let mut charged = *cost;
        charged.charge(0, scanned, JOB_CEILING)?;
        for (vertex, _) in &current {
            self.identities.get_mut(vertex).unwrap().weights[self.slot] = 0;
        }
        self.current_keys -= current.len();
        self.cursor = cursor;
        *cost = charged;
        Ok(current)
    }
    fn next_depth(&mut self) {
        debug_assert_eq!(self.current_keys, 0);
        self.slot = 1 - self.slot;
        self.cursor = None;
        self.current_keys = self.next_keys;
        self.next_keys = 0;
        self.next_mass = 0;
    }
}
struct Job {
    catalog: Arc<StaticCatalog>,
    sessions: BTreeMap<u64, (PartitionRoute, Descriptor)>,
    expires: Instant,
    cost: Cost,
    plan: Plan,
    compact: Option<Compact>,
    frontier: BTreeMap<VertexId, u64>,
    next: BTreeMap<VertexId, u64>,
    seen: BTreeSet<VertexId>,
    depth: usize,
    total: u64,
    next_mass: u64,
    initialized: bool,
    sequence: u64,
    last: Option<(u64, Output)>,
    completed: bool,
    _permit: Option<tokio::sync::SemaphorePermit<'static>>,
    _retained: tokio::sync::SemaphorePermit<'static>,
}
struct Entry {
    expires: Instant,
    job: Arc<tokio::sync::Mutex<Job>>,
}
pub(super) struct Jobs {
    values: Mutex<BTreeMap<[u8; 16], Entry>>,
}
impl Jobs {
    pub(super) fn new() -> Self {
        Self {
            values: Mutex::new(BTreeMap::new()),
        }
    }
    pub(super) fn expire(&self) {
        self.values
            .lock()
            .unwrap()
            .retain(|_, v| Instant::now() < v.expires);
    }
    pub(super) async fn handle(
        &self,
        catalog: Arc<StaticCatalog>,
        request: Request,
        end: Instant,
    ) -> Response {
        self.expire();
        match request {
            Request::Start {
                start_partition,
                text,
            } => {
                if text.len() > query::MAX_QUERY_TEXT_BYTES {
                    return Err(QueryFailure::ResourceExhausted);
                }
                let plan = parse(&text)?;
                let retained = RETAINED_SLOTS
                    .try_acquire()
                    .map_err(|_| QueryFailure::ResourceExhausted)?;
                let permit = JOB_SLOTS
                    .try_acquire()
                    .map_err(|_| QueryFailure::ResourceExhausted)?;
                let mut token = [0; 16];
                getrandom::fill(&mut token).map_err(|_| QueryFailure::Unavailable)?;
                let expires = Instant::now() + Duration::from_millis(LEASE_MILLIS);
                let mut job = Job {
                    catalog,
                    sessions: BTreeMap::new(),
                    expires,
                    cost: Cost::default(),
                    compact: (plan.emit && plan.dedup).then(Compact::default),
                    plan,
                    frontier: BTreeMap::new(),
                    next: BTreeMap::new(),
                    seen: BTreeSet::new(),
                    depth: 0,
                    total: 0,
                    next_mass: 0,
                    initialized: false,
                    sequence: 0,
                    last: None,
                    completed: false,
                    _permit: Some(permit),
                    _retained: retained,
                };
                let result = job.open(start_partition, end).await;
                if let Err(error) = result {
                    job.close().await;
                    return Err(error);
                }
                let output = job.output(token);
                let mut values = self.values.lock().unwrap();
                if values.contains_key(&token) {
                    return Err(QueryFailure::Unavailable);
                }
                values.insert(
                    token,
                    Entry {
                        expires,
                        job: Arc::new(tokio::sync::Mutex::new(job)),
                    },
                );
                Ok(output)
            }
            Request::Advance { token, sequence } => {
                let entry = self
                    .values
                    .lock()
                    .unwrap()
                    .get(&token)
                    .map(|v| v.job.clone())
                    .ok_or(QueryFailure::Unavailable)?;
                let mut job = entry
                    .try_lock()
                    .map_err(|_| QueryFailure::ResourceExhausted)?;
                check(job.expires)?;
                if let Some((last, output)) = &job.last {
                    if *last == sequence {
                        return Ok(output.clone());
                    }
                }
                if sequence != job.sequence || job.completed {
                    return Err(QueryFailure::InvalidArgument);
                }
                // Remove before an await can be cancelled: interrupted state is never resumed.
                self.values.lock().unwrap().remove(&token);
                match job.advance(end).await {
                    Ok(()) => {
                        job.sequence = job
                            .sequence
                            .checked_add(1)
                            .ok_or(QueryFailure::ResourceExhausted)?;
                        if job.completed {
                            job.close().await;
                            job._permit.take();
                        }
                        let output = job.output(token);
                        job.last = Some((sequence, output.clone()));
                        let expires = job.expires;
                        drop(job);
                        self.values.lock().unwrap().insert(
                            token,
                            Entry {
                                expires,
                                job: entry,
                            },
                        );
                        Ok(output)
                    }
                    Err(error) => {
                        job.close().await;
                        self.values.lock().unwrap().remove(&token);
                        Err(error)
                    }
                }
            }
            Request::Cancel { token } => {
                let entry = self
                    .values
                    .lock()
                    .unwrap()
                    .get(&token)
                    .map(|v| v.job.clone())
                    .ok_or(QueryFailure::Unavailable)?;
                let mut job = entry
                    .try_lock()
                    .map_err(|_| QueryFailure::ResourceExhausted)?;
                self.values.lock().unwrap().remove(&token);
                job.close().await;
                Ok(Output::Cancelled)
            }
        }
    }
}
impl Job {
    fn charge(&mut self, bytes: usize, work: usize) -> Result<(), QueryFailure> {
        self.cost.charge(bytes, work, JOB_CEILING)
    }
    fn output(&self, token: [u8; 16]) -> Output {
        Output::Progress {
            token,
            next_sequence: self.sequence,
            complete: self.completed,
            count: self.completed.then_some(self.total),
            completeness: QueryCompleteness::Unverified,
            snapshots: self
                .sessions
                .values()
                .map(|(_, d)| d.stamp.clone())
                .collect(),
        }
    }
    async fn open(&mut self, partition: u32, end: Instant) -> Result<(), QueryFailure> {
        if !self
            .catalog
            .partitions
            .iter()
            .any(|p| p.partition == partition)
        {
            return Err(QueryFailure::UnsupportedScope);
        }
        for id in self.plan.ids.clone() {
            if let Some(compact) = &mut self.compact {
                compact.seed(VertexId { partition, id }, &mut self.cost)?;
            } else {
                self.charge(128, 1)?;
                add(&mut self.frontier, VertexId { partition, id }, 1)?;
            }
        }
        for route in self.catalog.partitions.clone() {
            if self.sessions.contains_key(&route.shard) {
                continue;
            }
            check(end)?;
            self.charge(1024, 1)?;
            let reply = rpc(
                route.endpoint,
                Some(route.identity),
                Operation::OpenHierarchyQuery { shard: route.shard },
                end.saturating_duration_since(Instant::now()),
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
            self.sessions.insert(route.shard, (route, descriptor));
        }
        check(end)
    }
    async fn close(&mut self) {
        let execution = Execution {
            catalog: &self.catalog,
            sessions: self.sessions.clone(),
            end: Instant::now() + Duration::from_millis(200),
            cost: self.cost,
            ceiling: JOB_CEILING,
        };
        execution.close().await;
    }
    async fn batch(
        &mut self,
        fragments: Vec<Fragment>,
        end: Instant,
    ) -> Result<Vec<FragmentValue>, QueryFailure> {
        if fragments.len() > MAX_PAGE_EDGES {
            return Err(QueryFailure::ResourceExhausted);
        }
        self.charge(fragments.len() * 256, fragments.len())?;
        let n = fragments.len();
        let mut groups: BTreeMap<u32, Vec<(usize, Fragment)>> = BTreeMap::new();
        for (i, f) in fragments.into_iter().enumerate() {
            groups.entry(f.partition()).or_default().push((i, f));
        }
        let mut outputs: Vec<Option<FragmentValue>> = (0..n).map(|_| None).collect();
        for (partition, group) in groups {
            for chunk in group.chunks(BATCH_ROWS) {
                check(end)?;
                let mut execution = Execution {
                    catalog: &self.catalog,
                    sessions: self.sessions.clone(),
                    end,
                    cost: self.cost,
                    ceiling: JOB_CEILING,
                };
                let result = execution
                    .fragment(Fragment::HierarchyBatch {
                        partition,
                        fragments: chunk.iter().map(|(_, f)| f.clone()).collect(),
                    })
                    .await;
                self.cost = execution.cost;
                let FragmentValue::HierarchyBatch(values) = result? else {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                };
                if values.len() != chunk.len() {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                }
                for ((index, _), value) in chunk.iter().zip(values) {
                    outputs[*index] = Some(value);
                }
            }
        }
        outputs
            .into_iter()
            .map(|v| v.ok_or(QueryFailure::IncompleteOrCorrupt))
            .collect()
    }
    async fn advance(&mut self, end: Instant) -> Result<(), QueryFailure> {
        let end = end.min(self.expires);
        check(end)?;
        self.charge(256, 1)?;
        if !self.initialized {
            let starts: Vec<_> = if let Some(compact) = &self.compact {
                compact.identities.keys().copied().collect()
            } else {
                self.frontier.keys().copied().collect()
            };
            let values = self
                .batch(starts.iter().copied().map(Fragment::Exists).collect(), end)
                .await?;
            for (vertex, value) in starts.into_iter().zip(values) {
                match value {
                    FragmentValue::Bool(true) => {}
                    FragmentValue::Bool(false) => {
                        if let Some(compact) = &mut self.compact {
                            compact.identities.remove(&vertex);
                            compact.current_keys -= 1;
                        } else {
                            self.frontier.remove(&vertex);
                        }
                    }
                    _ => return Err(QueryFailure::IncompleteOrCorrupt),
                }
            }
            self.initialized = true;
        }
        let current = if let Some(compact) = &mut self.compact {
            compact.take(&mut self.cost, end)?
        } else {
            let mut current = Vec::new();
            for _ in 0..ADVANCE_ROWS {
                if let Some(row) = self.frontier.pop_first() {
                    current.push(row);
                } else {
                    break;
                }
            }
            current
        };
        let leaves = current
            .iter()
            .map(|(v, _)| {
                if self.plan.incoming {
                    Fragment::In(*v)
                } else {
                    Fragment::Out(*v)
                }
            })
            .collect();
        let observations = self.batch(leaves, end).await?;
        let mut pending = Vec::new();
        let mut weights = Vec::new();
        let mut outgoing = Vec::new();
        for ((vertex, weight), value) in current.into_iter().zip(observations) {
            match value {
                FragmentValue::Incoming(entries) if self.plan.incoming => {
                    if entries.len() > 1000
                        || entries
                            .windows(2)
                            .any(|w| (w[0].id, w[0].partition) >= (w[1].id, w[1].partition))
                    {
                        return Err(QueryFailure::IncompleteOrCorrupt);
                    }
                    for edge in entries {
                        if pending.len() >= MAX_PAGE_EDGES {
                            return Err(QueryFailure::ResourceExhausted);
                        }
                        self.charge(256 + edge.label.len(), 1)?;
                        weights.push((vertex, weight, edge.clone()));
                        pending.push(Fragment::ResolveIncoming {
                            target: vertex,
                            edge,
                        });
                    }
                }
                FragmentValue::Edges(edges) if !self.plan.incoming => {
                    if edges.len() > 1000 || edges.windows(2).any(|w| w[0].id >= w[1].id) {
                        return Err(QueryFailure::IncompleteOrCorrupt);
                    }
                    for edge in edges {
                        if edge.from != vertex {
                            return Err(QueryFailure::IncompleteOrCorrupt);
                        }
                        if pending.len() >= MAX_PAGE_EDGES {
                            return Err(QueryFailure::ResourceExhausted);
                        }
                        self.charge(256 + edge.label.len(), 1)?;
                        pending.push(Fragment::VerifyReverse(edge.clone()));
                        outgoing.push((weight, edge));
                    }
                }
                _ => return Err(QueryFailure::IncompleteOrCorrupt),
            }
        }
        let verified = self.batch(pending, end).await?;
        if self.plan.incoming {
            for ((target, weight, observed), value) in weights.into_iter().zip(verified) {
                check(end)?;
                let FragmentValue::Edge(edge) = value else {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                };
                if edge.to != target
                    || edge.id != observed.id
                    || edge.from.partition != observed.partition
                    || edge.label != observed.label
                {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                }
                self.accept(edge.from, weight, &edge.label)?;
            }
        } else {
            for ((weight, edge), value) in outgoing.into_iter().zip(verified) {
                check(end)?;
                if !matches!(value, FragmentValue::Bool(true)) {
                    return Err(QueryFailure::IncompleteOrCorrupt);
                }
                self.accept(edge.to, weight, &edge.label)?;
            }
        }
        if let Some(compact) = &mut self.compact {
            if compact.current_keys == 0 {
                self.depth += 1;
                if self.depth == self.plan.times || compact.next_keys == 0 {
                    self.completed = true;
                    // Retained replay needs only total and snapshot stamps.
                    self.compact = None;
                } else {
                    compact.next_depth();
                }
            }
        } else if self.frontier.is_empty() {
            self.depth += 1;
            if self.depth == self.plan.times || self.next.is_empty() {
                if !self.plan.emit {
                    self.total = if self.plan.dedup {
                        self.next.len() as u64
                    } else {
                        sum(&self.next)?
                    };
                }
                self.frontier.clear();
                self.next.clear();
                self.seen.clear();
                self.completed = true;
            } else {
                self.frontier = std::mem::take(&mut self.next);
                self.next_mass = 0;
            }
        }
        check(end)
    }
    fn accept(&mut self, vertex: VertexId, weight: u64, label: &str) -> Result<(), QueryFailure> {
        if self
            .plan
            .label
            .as_ref()
            .is_some_and(|expected| expected != label)
        {
            return Ok(());
        }
        if let Some(compact) = &mut self.compact {
            compact.accept(vertex, weight, &mut self.cost)?;
            self.total = compact.total;
            return Ok(());
        }
        let additional = usize::from(!self.next.contains_key(&vertex))
            + usize::from(self.plan.emit && self.plan.dedup && !self.seen.contains(&vertex));
        if self.frontier.len() + self.next.len() + self.seen.len() + additional > MAX_KEYS {
            return Err(QueryFailure::ResourceExhausted);
        }
        self.charge(128 * additional, 1)?;
        self.next_mass = checked(self.next_mass, weight)?;
        add(&mut self.next, vertex, weight)?;
        if self.plan.emit {
            if self.plan.dedup {
                if self.seen.insert(vertex) {
                    self.total = checked(self.total, 1)?;
                }
            } else {
                self.total = checked(self.total, weight)?;
            }
        }
        Ok(())
    }
}
fn checked(a: u64, b: u64) -> Result<u64, QueryFailure> {
    a.checked_add(b)
        .filter(|n| *n <= MAX_MASS)
        .ok_or(QueryFailure::ResourceExhausted)
}
fn add(
    map: &mut BTreeMap<VertexId, u64>,
    vertex: VertexId,
    weight: u64,
) -> Result<(), QueryFailure> {
    if !map.contains_key(&vertex) && map.len() >= MAX_KEYS {
        return Err(QueryFailure::ResourceExhausted);
    }
    let old = map.get(&vertex).copied().unwrap_or(0);
    map.insert(vertex, checked(old, weight)?);
    Ok(())
}
fn sum(map: &BTreeMap<VertexId, u64>) -> Result<u64, QueryFailure> {
    map.values().try_fold(0, |a, b| checked(a, *b))
}

/// Advances server-held computation only; the client never expands graph edges.
pub async fn request(
    endpoint: SocketAddr,
    request: Request,
    deadline: Duration,
) -> Result<Response, RemoteError> {
    if matches!(&request, Request::Start {text,..} if text.len()>query::MAX_QUERY_TEXT_BYTES) {
        return Err(RemoteError::ResourceExhausted);
    }
    let end = checked_deadline(deadline.min(Duration::from_millis(MAX_TIMEOUT)), None)?;
    let _permit = CLIENT_ADMISSION
        .try_acquire()
        .map_err(|_| RemoteError::ResourceExhausted)?;
    tokio::time::timeout_at(end, async {
        let mut socket = TcpStream::connect(endpoint)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        write_frame(
            &mut socket,
            &Envelope {
                hierarchy_job: request.clone(),
            },
            end,
        )
        .await?;
        let response: Response = read_frame_limited(&mut socket, end, MAX_WIRE).await?;
        if let Ok(output) = &response {
            validate_output(output, &request)?;
        }
        Ok(response)
    })
    .await
    .unwrap_or(Err(RemoteError::Timeout { request_id: None }))
}
fn validate_output(output: &Output, request: &Request) -> Result<(), RemoteError> {
    match (output, request) {
        (Output::Cancelled, Request::Cancel { .. }) => Ok(()),
        (
            Output::Progress {
                token,
                next_sequence,
                complete,
                count,
                completeness,
                snapshots,
            },
            request,
        ) => {
            if *completeness != QueryCompleteness::Unverified
                || *complete != count.is_some()
                || count.is_some_and(|n| n > MAX_MASS)
                || snapshots.is_empty()
                || snapshots.len() > 8
                || snapshots.windows(2).any(|w| w[0].shard >= w[1].shard)
            {
                return Err(RemoteError::Protocol);
            }
            let mut shards = BTreeSet::new();
            for stamp in snapshots {
                if stamp.epoch == 0 || stamp.identity.node == 0 || !shards.insert(stamp.shard) {
                    return Err(RemoteError::Protocol);
                }
            }
            match request {
                Request::Start { .. } if *next_sequence == 0 && !complete => Ok(()),
                Request::Advance {
                    token: expected,
                    sequence,
                } if token == expected && sequence.checked_add(1) == Some(*next_sequence) => Ok(()),
                _ => Err(RemoteError::Protocol),
            }
        }
        _ => Err(RemoteError::Protocol),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn vertex(id: usize) -> VertexId {
        // Neighboring logical vertices deliberately share the numeric ID.
        VertexId {
            partition: (id % 2) as u32,
            id: (id / 2) as u64,
        }
    }
    #[test]
    fn compact_depth_bags_match_explicit_paths_with_cycles_parallel_edges_and_reconvergence() {
        for seed in 0..50usize {
            let n = 23;
            let edges: Vec<Vec<usize>> = (0..n)
                .map(|v| vec![v, (v + seed + 1) % n, (v + seed + 1) % n, (v * 7 + 3) % n])
                .collect();
            let mut compact = Compact::default();
            let mut cost = Cost::default();
            let mut paths = vec![0, 0, 1];
            let mut emitted = BTreeSet::new();
            for v in &paths {
                compact.seed(vertex(*v), &mut cost).unwrap();
            }
            for depth in 0..5 {
                let old_paths = paths;
                paths = old_paths.iter().flat_map(|v| edges[*v].clone()).collect();
                emitted.extend(paths.iter().copied().map(vertex));
                let end = Instant::now() + Duration::from_secs(30);
                while compact.current_keys != 0 {
                    let page = compact.take(&mut cost, end).unwrap();
                    assert!(page.len() <= ADVANCE_ROWS);
                    for (v, weight) in page {
                        let index = v.id as usize * 2 + v.partition as usize;
                        for target in &edges[index] {
                            compact.accept(vertex(*target), weight, &mut cost).unwrap();
                        }
                    }
                }
                assert_eq!(
                    compact.total,
                    emitted.len() as u64,
                    "seed {seed}, depth {depth}"
                );
                assert_eq!(compact.next_mass, paths.len() as u64);
                compact.next_depth();
                let mut oracle = BTreeMap::new();
                for v in &paths {
                    *oracle.entry(vertex(*v)).or_insert(0u64) += 1;
                }
                let actual: BTreeMap<_, _> = compact
                    .identities
                    .iter()
                    .filter(|(_, state)| state.weights[compact.slot] != 0)
                    .map(|(v, state)| (*v, state.weights[compact.slot]))
                    .collect();
                assert_eq!(actual, oracle);
                assert_eq!(compact.current_keys, oracle.len());
                assert!(compact
                    .identities
                    .values()
                    .all(|s| s.weights[1 - compact.slot] == 0));
            }
        }
    }
    #[test]
    fn compact_sparse_scan_is_incremental_metered_and_failure_atomic() {
        let mut compact = Compact::default();
        for id in 0..1000 {
            compact.identities.insert(
                VertexId { partition: 0, id },
                IdentityState {
                    seen: true,
                    ..IdentityState::default()
                },
            );
        }
        let mut cost = Cost::default();
        let last = VertexId {
            partition: 0,
            id: 999,
        };
        compact.seed(last, &mut cost).unwrap();
        let end = Instant::now() + Duration::from_secs(30);
        let work = cost.work;
        assert!(compact.take(&mut cost, end).unwrap().is_empty());
        assert_eq!(cost.work - work, SCAN_ROWS);
        assert_eq!(compact.cursor.unwrap().id, SCAN_ROWS as u64 - 1);
        assert_eq!(compact.current_keys, 1);
        let before = compact.clone();
        cost.work = JOB_CEILING.work;
        let charged = cost;
        assert_eq!(
            compact.take(&mut cost, end),
            Err(QueryFailure::ResourceExhausted)
        );
        assert_eq!(compact, before);
        assert_eq!((cost.bytes, cost.work), (charged.bytes, charged.work));
        cost.work = 0;
        assert!(compact
            .take(&mut cost, Instant::now() - Duration::from_secs(1))
            .is_err());
        assert_eq!(compact, before);
        let mut rows = Vec::new();
        while compact.current_keys != 0 {
            rows.extend(compact.take(&mut cost, end).unwrap());
        }
        assert_eq!(rows, vec![(last, 1)]);
        assert_eq!(cost.work, 1000 - SCAN_ROWS);
        assert_eq!(compact.next_keys, 0);
    }
    #[test]
    fn compact_identity_cap_shares_frontiers_and_seen_without_bypassing_mass_or_cost() {
        // Includes conservative logical overhead for B-tree nodes and allocator.
        assert!(std::mem::size_of::<(VertexId, IdentityState)>() * 2 <= 128);
        let mut compact = Compact::default();
        let mut cost = Cost::default();
        for id in 0..MAX_KEYS {
            compact.seed(vertex(id), &mut cost).unwrap();
        }
        // At exactly the key cap an existing current key can also be emitted
        // into the next depth. No second allocation is charged.
        let bytes = cost.bytes;
        for id in 0..MAX_KEYS {
            compact.accept(vertex(id), 1, &mut cost).unwrap();
        }
        assert_eq!(compact.total, MAX_KEYS as u64);
        assert_eq!(compact.next_keys, MAX_KEYS);
        let before_cap = compact.clone();
        let charged_cap = cost;
        assert_eq!(
            compact.accept(vertex(MAX_KEYS), 1, &mut cost),
            Err(QueryFailure::ResourceExhausted)
        );
        assert_eq!(compact, before_cap);
        assert_eq!(
            (cost.bytes, cost.work),
            (charged_cap.bytes, charged_cap.work)
        );
        compact
            .accept(vertex(0), MAX_MASS - MAX_KEYS as u64, &mut cost)
            .unwrap();
        assert_eq!(compact.identities.len(), MAX_KEYS);
        assert_eq!(compact.next_keys, MAX_KEYS);
        assert_eq!(cost.bytes, bytes);
        let before = compact.clone();
        let charged = cost;
        for (v, weight) in [(vertex(MAX_KEYS), 1), (vertex(1), 1), (vertex(0), 1)] {
            assert_eq!(
                compact.accept(v, weight, &mut cost),
                Err(QueryFailure::ResourceExhausted)
            );
            assert_eq!(compact, before);
            assert_eq!((cost.bytes, cost.work), (charged.bytes, charged.work));
        }
        assert_eq!(
            compact.seed(vertex(MAX_KEYS), &mut cost),
            Err(QueryFailure::ResourceExhausted)
        );
        assert_eq!(compact, before);
        // Budget failure must not insert an identity or increment totals.
        for ceiling in [
            Cost {
                bytes: JOB_CEILING.bytes,
                work: 0,
            },
            Cost {
                bytes: 0,
                work: JOB_CEILING.work,
            },
        ] {
            let mut fresh = Compact::default();
            let mut exhausted = ceiling;
            assert_eq!(
                fresh.accept(vertex(0), 1, &mut exhausted),
                Err(QueryFailure::ResourceExhausted)
            );
            assert_eq!(fresh, Compact::default());
            assert_eq!(
                (exhausted.bytes, exhausted.work),
                (ceiling.bytes, ceiling.work)
            );
        }
    }
    #[test]
    fn advance_grain_fits_maximum_adjacencies_without_a_larger_page_budget() {
        // Regression for 125 hot departments, whose unrelated membership
        // observations must still be validated before label filtering.
        let frontier: Vec<_> = (0..125).collect();
        let pages: Vec<_> = frontier.chunks(ADVANCE_ROWS).collect();
        assert_eq!(pages.iter().map(|p| p.len()).sum::<usize>(), 125);
        assert!(pages.iter().all(|p| p.len() * 1000 <= MAX_PAGE_EDGES));
    }
    #[test]
    fn admission_is_count_only_and_weights_are_bounded_qualified_bags() {
        for text in [
            "g.V(1).repeat(__.in('reports_to')).emit().times(32).count()",
            "g.V(1,1).repeat(__.out()).times(32).dedup().count()",
        ] {
            assert!(parse(text).is_ok(), "{text}");
        }
        for text in [
            "g.V().repeat(__.in()).times(2).count()",
            "g.V(1).out().repeat(__.in()).times(2).count()",
            "g.V(1).repeat(__.in()).times(2).limit(1).count()",
            "g.V(1).repeat(__.in()).times(2).path()",
            "g.V(1).repeat(__.in()).times(0).count()",
            "g.V(1).repeat(__.in()).times(33).count()",
        ] {
            assert!(parse(text).is_err(), "{text}");
        }
        let mut map = BTreeMap::new();
        add(
            &mut map,
            VertexId {
                partition: 1,
                id: 7,
            },
            3,
        )
        .unwrap();
        add(
            &mut map,
            VertexId {
                partition: 1,
                id: 7,
            },
            4,
        )
        .unwrap();
        add(
            &mut map,
            VertexId {
                partition: 2,
                id: 7,
            },
            5,
        )
        .unwrap();
        assert_eq!(map.len(), 2);
        assert_eq!(sum(&map).unwrap(), 12);
        assert_eq!(checked(MAX_MASS, 1), Err(QueryFailure::ResourceExhausted));
        assert_eq!(checked(u64::MAX, 1), Err(QueryFailure::ResourceExhausted));
        assert_eq!(
            add(
                &mut map,
                VertexId {
                    partition: 1,
                    id: 7
                },
                MAX_MASS
            ),
            Err(QueryFailure::ResourceExhausted)
        );
    }
    #[tokio::test]
    async fn weighted_frontiers_match_path_bags_in_cycles_and_reconvergence() {
        for seed in 0..50usize {
            let n = 7;
            let edges: Vec<Vec<usize>> = (0..n)
                .map(|v| {
                    (0..n)
                        .filter(|u| (v * 19 + u * 13 + seed) % 11 < 3)
                        .flat_map(|u| std::iter::repeat_n(u, 1 + (u + v + seed) % 2))
                        .collect()
                })
                .collect();
            for emit in [false, true] {
                for dedup in [false, true] {
                    let plan = Plan {
                        ids: vec![0, 0],
                        incoming: false,
                        label: None,
                        times: 5,
                        emit,
                        dedup,
                    };
                    let permit = JOB_SLOTS.acquire().await.unwrap();
                    let mut job = Job {
                        catalog: Arc::new(StaticCatalog { partitions: vec![] }),
                        sessions: BTreeMap::new(),
                        expires: Instant::now() + Duration::from_secs(1),
                        cost: Cost::default(),
                        compact: None,
                        plan,
                        frontier: BTreeMap::new(),
                        next: BTreeMap::new(),
                        seen: BTreeSet::new(),
                        depth: 0,
                        total: 0,
                        next_mass: 0,
                        initialized: true,
                        sequence: 0,
                        last: None,
                        completed: false,
                        _permit: Some(permit),
                        _retained: RETAINED_SLOTS.acquire().await.unwrap(),
                    };
                    let vertex = |id| VertexId {
                        partition: (id % 2) as u32,
                        id: id as u64,
                    };
                    add(&mut job.frontier, vertex(0), 2).unwrap();
                    let mut paths = vec![0, 0];
                    let mut emitted = Vec::new();
                    for _ in 0..5 {
                        paths = paths.iter().flat_map(|v| edges[*v].clone()).collect();
                        emitted.extend(paths.clone());
                        for (v, weight) in std::mem::take(&mut job.frontier) {
                            for u in &edges[v.id as usize] {
                                job.accept(vertex(*u), weight, "x").unwrap();
                            }
                        }
                        job.frontier = std::mem::take(&mut job.next);
                        job.next_mass = 0;
                    }
                    let expected = if emit { emitted } else { paths };
                    let expected = if dedup {
                        expected.into_iter().collect::<BTreeSet<_>>().len() as u64
                    } else {
                        expected.len() as u64
                    };
                    let actual = if emit {
                        job.total
                    } else if dedup {
                        job.frontier.len() as u64
                    } else {
                        sum(&job.frontier).unwrap()
                    };
                    assert_eq!(actual, expected, "seed{seed}/emit{emit}/dedup{dedup}");
                }
            }
        }
    }
    #[test]
    fn client_rejects_progress_that_looks_like_a_partial_result() {
        let token = [7; 16];
        let request = Request::Advance { token, sequence: 3 };
        let stamp = QueryStamp {
            identity: RemoteIdentity { group: 1, node: 1 },
            shard: 1,
            epoch: 1,
            sequence: 5,
        };
        let make = |complete, count| Output::Progress {
            token,
            next_sequence: 4,
            complete,
            count,
            completeness: QueryCompleteness::Unverified,
            snapshots: vec![stamp.clone()],
        };
        assert!(validate_output(&make(false, None), &request).is_ok());
        assert!(validate_output(&make(true, Some(42)), &request).is_ok());
        for output in [
            make(false, Some(42)),
            make(true, None),
            make(true, Some(MAX_MASS + 1)),
            Output::Cancelled,
        ] {
            assert!(validate_output(&output, &request).is_err());
        }
        assert!(validate_output(
            &make(false, None),
            &Request::Advance {
                token: [0; 16],
                sequence: 3
            }
        )
        .is_err());
        assert!(
            validate_output(&make(false, None), &Request::Advance { token, sequence: 4 }).is_err()
        );
        assert!(validate_output(&make(false, None), &Request::Cancel { token }).is_err());
        assert!(validate_output(
            &make(false, None),
            &Request::Start {
                start_partition: 0,
                text: String::new()
            }
        )
        .is_err());
        let mut malformed = make(false, None);
        if let Output::Progress { snapshots, .. } = &mut malformed {
            snapshots.push(stamp);
        }
        assert!(validate_output(&malformed, &request).is_err());
    }
    #[tokio::test]
    async fn cancelled_advance_cannot_resume_a_consumed_frontier() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let route = PartitionRoute {
            partition: 0,
            shard: 1,
            epoch: 1,
            identity: RemoteIdentity { group: 1, node: 1 },
            endpoint: listener.local_addr().unwrap(),
        };
        let descriptor = Descriptor {
            stamp: QueryStamp {
                identity: route.identity,
                shard: 1,
                epoch: 1,
                sequence: 1,
            },
            incarnation: [1; 16],
            token: [2; 16],
        };
        let catalog = Arc::new(StaticCatalog {
            partitions: vec![route.clone()],
        });
        let expires = Instant::now() + Duration::from_secs(5);
        let job = Job {
            catalog: catalog.clone(),
            sessions: BTreeMap::from([(1, (route, descriptor))]),
            expires,
            cost: Cost::default(),
            compact: None,
            plan: parse("g.V(1).repeat(__.out()).times(2).count()").unwrap(),
            frontier: BTreeMap::from([(
                VertexId {
                    partition: 0,
                    id: 1,
                },
                1,
            )]),
            next: BTreeMap::new(),
            seen: BTreeSet::new(),
            depth: 0,
            total: 0,
            next_mass: 0,
            initialized: true,
            sequence: 0,
            last: None,
            completed: false,
            _permit: Some(JOB_SLOTS.acquire().await.unwrap()),
            _retained: RETAINED_SLOTS.acquire().await.unwrap(),
        };
        let jobs = Arc::new(Jobs::new());
        let token = [3; 16];
        jobs.values.lock().unwrap().insert(
            token,
            Entry {
                expires,
                job: Arc::new(tokio::sync::Mutex::new(job)),
            },
        );
        let worker = jobs.clone();
        let task = tokio::spawn(async move {
            worker
                .handle(catalog, Request::Advance { token, sequence: 0 }, expires)
                .await
        });
        let (_socket, _) = listener.accept().await.unwrap();
        assert!(!jobs.values.lock().unwrap().contains_key(&token));
        task.abort();
        let _ = task.await;
        assert!(!jobs.values.lock().unwrap().contains_key(&token));
    }
}
