//! Query-bound fixed-V orchestration. No raw KV is downloaded on this path.
use super::*;
use crate::remote::{
    distributed::{fixed, fixed_execution, DistributedRequest, PartitionRoute},
    RemoteError, RemoteIdentity,
};
use d2_raft::managed_rpc::{Identity, Role};

/// Deployment-owned graph port and exact matching managed identity. The port is
/// never inferred from a managed address and never accepted from a query.
#[derive(Clone, Debug)]
pub struct LocalGraphRoute {
    pub endpoint: SocketAddr,
    pub identity: RemoteIdentity,
    pub managed_identity: Identity,
}
/// A query-result identity, distinct from both observed and materialized sessions.
/// It is not a credential or a restartable token.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalSession {
    pub incarnation: [u8; 16],
    pub token: [u8; 32],
    pub version: u64,
}
pub(super) struct Retained {
    descriptor: LocalSession,
    requested_profile: Option<LocalReadProfile>,
    pub(super) expires: Instant,
    _memory: OwnedSemaphorePermit,
    result: pages::Materialized,
    pub(super) closed: std::sync::atomic::AtomicBool,
    // Immutable session/result/page sizing: one successful HTTP frame proof is
    // valid for every subsequent fetch. Failed/expired proofs are never cached.
    http_preflighted: std::sync::atomic::AtomicBool,
    grpc_preflighted: std::sync::atomic::AtomicBool,
}
fn graph_plan(traversal: public_proto::GraphTraversal) -> Result<velagraph::ObservedGremlinQuery> {
    use public_proto::graph_step::Kind;
    use velagraph::GremlinStep;
    if traversal.version != 1 {
        return Err(Error::Query(QueryFailure::Unsupported));
    }
    let steps = traversal
        .steps
        .into_iter()
        .map(|step| {
            Ok(match step.kind.ok_or(Error::Protocol)? {
                Kind::In(hop) => GremlinStep::In(hop.label),
                Kind::Out(hop) => GremlinStep::Out(hop.label),
                Kind::Count(_) => GremlinStep::Count,
                Kind::Path(_) => GremlinStep::Path,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    velagraph::ObservedGremlinQuery::fixed_vertex_traversal(traversal.vertex_ids, steps).map_err(
        |cause| {
            Error::Query(match cause {
                velagraph::Error::TraversalLimitExceeded(_) => QueryFailure::ResourceExhausted,
                _ => QueryFailure::InvalidArgument,
            })
        },
    )
}

pub(super) fn snapshot_error(error: cohort_reads::Error) -> Error {
    managed_graph::Error::from(error).into()
}
pub(super) fn remote_error(error: RemoteError) -> Error {
    match error {
        RemoteError::ResourceExhausted
        | RemoteError::Application(d2_raft::D1Error::VisibilityReadLimit) => Error::Capacity,
        RemoteError::Application(_) => Error::Rejected,
        RemoteError::InvalidArgument | RemoteError::Protocol => Error::Identity,
        RemoteError::SessionUnavailable => Error::SessionUnavailable,
        _ => Error::Unknown,
    }
}
pub(super) fn request(query: &Query) -> DistributedRequest {
    DistributedRequest {
        start_partition: query.partition,
        text: query.text.clone(),
        timeout_millis: 5000,
    }
}
pub(super) fn validate_routes(config: &RemoteConfig, routes: &[LocalGraphRoute]) -> Result<()> {
    let cohort = &config.binding.cohort;
    if routes.is_empty()
        || routes.len() > fixed::MAX_MEMBERS
        || routes.len() != cohort.members.len()
        || routes.len() != config.members.len()
    {
        return Err(Error::Identity);
    }
    let coordinator = config.coordinator.identity();
    if coordinator.role != Role::Coordinator
        || coordinator.authority != config.binding.authority
        || coordinator.cohort != *cohort
    {
        return Err(Error::Identity);
    }
    let mut endpoints = BTreeSet::new();
    let mut identities = BTreeSet::new();
    let mut groups = BTreeSet::new();
    for ((route, client), member) in routes.iter().zip(&config.members).zip(&cohort.members) {
        let id = &route.managed_identity;
        if id != client.identity()
            || id.role != Role::Participant
            || id.authority != config.binding.authority
            || id.cohort != *cohort
            || id.target.shard != member.shard
            || id.target.epoch != member.epoch
            || id.target.mode != member.mode
            || member.shard > u64::from(u32::MAX)
            || route.identity.group != id.group
            || route.identity.node != id.node
            || id.node == 0
            || route.endpoint.port() == 0
            || !endpoints.insert(route.endpoint)
            || !identities.insert((id.group, id.node))
            || !groups.insert(id.group)
        {
            return Err(Error::Identity);
        }
    }
    Ok(())
}
pub(super) fn live(retained: &Retained, end: Instant) -> Result<()> {
    if retained.closed.load(std::sync::atomic::Ordering::Acquire)
        || Instant::now() >= retained.expires
    {
        return Err(Error::SessionUnavailable);
    }
    before_deadline(end)
}
pub(super) fn preflight(binding: &Binding, reply: Reply) -> Result<()> {
    encode(
        &Response {
            version: VERSION,
            request_id: [255; 16],
            binding: binding.clone(),
            result: Ok(reply),
        },
        MAX_RESPONSE_BYTES,
    )?;
    Ok(())
}
fn preflight_pages(
    binding: &Binding,
    profile: Option<LocalReadProfile>,
    session: &LocalSession,
    result: &pages::Materialized,
    end: Instant,
) -> Result<()> {
    for offset in (0..result.data.rows.len().max(1)).step_by(usize::from(result.size)) {
        before_deadline(end)?;
        preflight(
            binding,
            Reply::FetchLocalQuery {
                session: session.clone(),
                query: result.query.clone(),
                cursor: result.cursors.get(&offset).cloned(),
                page_size: result.size,
                page: result.page(offset),
            },
        )?;
    }
    before_deadline(end)?;
    preflight(binding, open_reply(profile, session, result))
}
// HTTP has a different preview envelope. Validate every future page before
// delivering any HTTP page, including a session originally opened over TCP.
fn preflight_http_pages(
    session: &LocalSession,
    result: &pages::Materialized,
    end: Instant,
) -> Result<()> {
    for offset in (0..result.data.rows.len().max(1)).step_by(usize::from(result.size)) {
        before_deadline(end)?;
        http_codec::encode(&Ok(Reply::FetchLocalQuery {
            session: session.clone(),
            query: result.query.clone(),
            cursor: result.cursors.get(&offset).cloned(),
            page_size: result.size,
            page: result.page(offset),
        }))?;
    }
    before_deadline(end)
}
// Prove all pages with the exact fixed-binding v2 identity envelope before any
// first page is delivered. Even a v1 open may later be fetched by a v2 client.
fn preflight_grpc_pages(
    binding: &Binding,
    session: &LocalSession,
    result: &pages::Materialized,
    end: Instant,
) -> Result<()> {
    for offset in (0..result.data.rows.len().max(1)).step_by(usize::from(result.size)) {
        before_deadline(end)?;
        let message = grpc_codec::encode_fetch(&Reply::FetchLocalQuery {
            session: session.clone(),
            query: result.query.clone(),
            cursor: result.cursors.get(&offset).cloned(),
            page_size: result.size,
            page: result.page(offset),
        })?;
        grpc_codec::preflight_identity(message, binding)?;
    }
    before_deadline(end)
}
impl Retained {
    pub(super) fn preflight_grpc(&self, binding: &Binding, end: Instant) -> Result<()> {
        live(self, end)?;
        if !self
            .grpc_preflighted
            .load(std::sync::atomic::Ordering::Acquire)
        {
            preflight_grpc_pages(binding, &self.descriptor, &self.result, end)?;
            live(self, end)?;
            self.grpc_preflighted
                .store(true, std::sync::atomic::Ordering::Release);
        }
        live(self, end)
    }
    pub(super) fn preflight_http(&self, end: Instant) -> Result<()> {
        live(self, end)?;
        if !self
            .http_preflighted
            .load(std::sync::atomic::Ordering::Acquire)
        {
            preflight_http_pages(&self.descriptor, &self.result, end)?;
            live(self, end)?;
            self.http_preflighted
                .store(true, std::sync::atomic::Ordering::Release);
        }
        live(self, end)
    }
}
fn open_reply(
    profile: Option<LocalReadProfile>,
    session: &LocalSession,
    result: &pages::Materialized,
) -> Reply {
    match profile {
        None => Reply::OpenLocalQuery {
            session: session.clone(),
            query: result.query.clone(),
            page_size: result.size,
            page: result.page(0),
        },
        Some(profile) => Reply::OpenLocalQueryWithProfile {
            profile,
            session: session.clone(),
            query: result.query.clone(),
            page_size: result.size,
            page: result.page(0),
        },
    }
}

pub(super) fn valid_page(
    query: &Query,
    cursor: Option<&PageCursor>,
    size: u16,
    page: &QueryPage,
    binding: &Binding,
) -> bool {
    fixed_execution::validate(&request(query)).is_ok()
        && pages::valid_page(query, cursor, size, page, binding)
}
// Acquisition owns cleanup and admission even when its caller is cancelled.
// An Open whose reply is lost has no trustworthy descriptor; its node TTL is
// still the final backstop. Cleanup never retries an open or a mutation.
struct CapturedLocal {
    sessions: BTreeMap<u64, fixed_execution::NodeSession>,
    work: Option<Arc<OwnedSemaphorePermit>>,
    memory: Option<OwnedSemaphorePermit>,
    execution: Option<tokio::sync::SemaphorePermit<'static>>,
    flight: Option<OwnedSemaphorePermit>,
}
impl CapturedLocal {
    fn new(
        work: Arc<OwnedSemaphorePermit>,
        memory: OwnedSemaphorePermit,
        execution: tokio::sync::SemaphorePermit<'static>,
        flight: Option<OwnedSemaphorePermit>,
    ) -> Self {
        Self {
            sessions: BTreeMap::new(),
            work: Some(work),
            memory: Some(memory),
            execution: Some(execution),
            flight,
        }
    }
    async fn close(&mut self) {
        // Separate bounded teardown budget, not an extension to acquisition.
        fixed_execution::close(
            &self.sessions,
            tokio::time::Instant::now() + Duration::from_millis(500),
        )
        .await;
        self.sessions.clear();
    }
}
impl Drop for CapturedLocal {
    fn drop(&mut self) {
        if self.sessions.is_empty() {
            return;
        }
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let sessions = std::mem::take(&mut self.sessions);
        let permits = (
            self.work.take(),
            self.memory.take(),
            self.execution.take(),
            self.flight.take(),
        );
        runtime.spawn(async move {
            let _permits = permits;
            fixed_execution::close(
                &sessions,
                tokio::time::Instant::now() + Duration::from_millis(500),
            )
            .await;
        });
    }
}

impl State {
    #[allow(clippy::too_many_arguments)]
    async fn capture_local(
        &self,
        profile: LocalReadProfile,
        version: Option<u64>,
        minimum_version: Option<u64>,
        ttl: Duration,
        end: Instant,
        capture: &mut CapturedLocal,
    ) -> Result<(u64, Instant)> {
        let routes = self
            .config
            .local_routes
            .as_ref()
            .ok_or(Error::Query(QueryFailure::Unsupported))?;
        let cut = if let Some(minimum) = minimum_version {
            cohort_reads::admit_endpoints_at_least(
                self.config.coordinator.reference(),
                self.config.binding.authority.clone(),
                &self.config.binding.cohort,
                self.config.endpoints(),
                minimum,
                ttl,
                end,
            )
            .await
        } else {
            cohort_reads::admit_endpoints(
                self.config.coordinator.reference(),
                self.config.binding.authority.clone(),
                self.config.binding.cohort.coordinator_epoch,
                self.config.endpoints(),
                version,
                ttl,
            )
            .await
        }
        .map_err(snapshot_error)?;
        before_deadline(end)?;
        if cut.cohort() != &self.config.binding.cohort
            || cut.authority() != &self.config.binding.authority
        {
            return Err(Error::Identity);
        }
        let expires = cut.expires_at();
        let acquire_end = end.min(cut.deadline());
        let limit = profile.capture_limit();
        let remaining_limit = |rows: usize, bytes: usize, ops: usize| ReadLimit {
            rows: limit.rows.saturating_sub(rows),
            bytes: limit.bytes.saturating_sub(bytes),
            operations: limit.operations.saturating_sub(ops),
        };
        let make_open = |target: Target, limit: ReadLimit| fixed::FixedOpenRequest {
            profile,
            target,
            proof: cut.proof().clone(),
            capture_limit: limit,
            ttl_millis: expires
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(60_000) as u64,
            acquire_timeout_millis: acquire_end
                .saturating_duration_since(Instant::now())
                .as_millis()
                .min(5000) as u64,
        };
        // All members' full request and worst reply must fit before opening one.
        for (route, (_, target)) in routes.iter().zip(cut.members()) {
            cut.check().map_err(snapshot_error)?;
            fixed::preflight(
                route.identity,
                &make_open(target.clone(), remaining_limit(0, 0, 0)),
                tokio::time::Instant::from_std(acquire_end),
            )
            .map_err(remote_error)?;
        }
        let (mut rows, mut bytes, mut operations) = (0usize, 0usize, 0usize);
        for (route, (_, target)) in routes.iter().zip(cut.members()) {
            cut.check().map_err(snapshot_error)?;
            before_deadline(acquire_end)?;
            let remaining = remaining_limit(rows, bytes, operations);
            // Match materialized acquisition: even a later empty member
            // cannot turn an exhausted aggregate budget into success.
            if remaining.rows == 0 || remaining.bytes == 0 || remaining.operations == 0 {
                return Err(Error::Capacity);
            }
            let transport = self.config.local_transport(route)?;
            let opened = transport
                .open(
                    make_open(target.clone(), remaining),
                    acquire_end.saturating_duration_since(Instant::now()),
                )
                .await
                .map_err(remote_error)?;
            let descriptor = &opened.descriptor;
            if descriptor.authority != *cut.authority()
                || descriptor.cohort != *cut.cohort()
                || descriptor.version != cut.version()
                || descriptor.mode != target.mode
                || descriptor.stamp.identity != route.identity
                || descriptor.stamp.shard != target.shard
                || descriptor.stamp.epoch != target.epoch
                || descriptor.incarnation == [0; 16]
                || descriptor.token == [0; 16]
            {
                return Err(Error::Identity);
            }
            // Once endpoint/descriptor identity is safe, own cleanup even
            // if subsequent proof or aggregate-cost validation fails.
            let partition = u32::try_from(target.shard).map_err(|_| Error::Identity)?;
            capture.sessions.insert(
                target.shard,
                fixed_execution::NodeSession::new(
                    PartitionRoute {
                        partition,
                        shard: target.shard,
                        epoch: target.epoch,
                        identity: route.identity,
                        endpoint: route.endpoint,
                    },
                    opened.descriptor.clone(),
                    opened.capture_cost.bytes,
                    opened.capture_cost.rows,
                )
                .map_err(remote_error)?
                .with_transport(transport),
            );
            // Compare the complete retained certificate, not a boolean or V.
            if opened.descriptor.profile != profile || opened.retained_proof != *cut.proof() {
                return Err(Error::Identity);
            }
            rows = rows
                .checked_add(opened.capture_cost.rows)
                .ok_or(Error::Capacity)?;
            bytes = bytes
                .checked_add(opened.capture_cost.bytes)
                .ok_or(Error::Capacity)?;
            operations = operations
                .checked_add(opened.capture_cost.replay_operations)
                .ok_or(Error::Capacity)?;
            if rows > limit.rows || bytes > limit.bytes || operations > limit.operations {
                return Err(Error::Capacity);
            }
        }
        cut.check().map_err(snapshot_error)?;
        before_deadline(end)?;
        Ok((cut.version(), expires))
    }

    pub(super) async fn probe_readiness(
        &self,
        work: Arc<OwnedSemaphorePermit>,
        end: Instant,
    ) -> Result<u64> {
        before_deadline(end)?;
        if self
            .jobs_stopping
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::Rejected);
        }
        let flight = self
            .readiness
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let execution = fixed_execution::admit_execution().map_err(Error::Query)?;
        self.expire();
        let memory = self
            .memory
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let mut capture = CapturedLocal::new(work, memory, execution, Some(flight));
        // This is deliberately the existing default query capture profile.
        // Capacity failure is NOT a positive readiness observation.
        let result = self
            .capture_local(
                LocalReadProfile::default(),
                None,
                None,
                MAX_TTL,
                end,
                &mut capture,
            )
            .await;
        capture.close().await;
        before_deadline(end)?;
        result.map(|(version, _)| version)
    }

    pub(super) fn get_local(&self, descriptor: &LocalSession) -> Result<Arc<Retained>> {
        self.expire();
        if descriptor.incarnation != self.incarnation {
            return Err(Error::SessionUnavailable);
        }
        let sessions = self.local_sessions.lock().unwrap();
        let found = sessions
            .get(&descriptor.token)
            .ok_or(Error::SessionUnavailable)?;
        if found.descriptor != *descriptor {
            return Err(Error::Identity);
        }
        if found.closed.load(std::sync::atomic::Ordering::Acquire) {
            return Err(Error::SessionUnavailable);
        }
        Ok(found.clone())
    }
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn open_local(
        &self,
        requested_profile: Option<LocalReadProfile>,
        version: Option<u64>,
        minimum_version: Option<u64>,
        ttl_millis: u64,
        query: Query,
        page_size: u16,
        work: Arc<OwnedSemaphorePermit>,
        end: Instant,
    ) -> Result<Reply> {
        let ttl = Duration::from_millis(ttl_millis);
        if ttl.is_zero() || ttl > MAX_TTL || !(1..=MAX_PAGE_SIZE).contains(&page_size) {
            return Err(Error::Protocol);
        }
        let req = request(&query);
        let plan = fixed_execution::parse(&req).map_err(Error::Query)?;
        let (rows, version, expires, mut capture) = self
            .capture_and_execute(
                requested_profile.unwrap_or_default(),
                version,
                minimum_version,
                ttl,
                query.partition,
                &plan,
                work,
                end,
            )
            .await?;
        let descriptor = LocalSession {
            incarnation: self.incarnation,
            token: random()?,
            version,
        };
        let work = capture.work.take().expect("capture owns admission");
        let execution = capture.execution.take().expect("capture owns execution");
        let memory = capture.memory.take().expect("capture owns memory");
        let binding = self.config.binding.clone();
        let retained = tokio::task::spawn_blocking(move || {
            // Blocking encoding cannot be canceled; hold both shared permits.
            let _work = work;
            let _execution = execution;
            let data = QueryData {
                rows,
                completeness: QueryCompleteness::Unverified,
            };
            if data.rows.len() > pages::MAX_ROWS
                || !query::valid_traversal_output(&query, &data, &binding)
            {
                return Err(Error::Capacity);
            }
            encode(&data, pages::MAX_RESULT_BYTES)?;
            let mut materialized = pages::Materialized {
                query: query.clone(),
                size: page_size,
                data,
                offsets: BTreeMap::new(),
                cursors: BTreeMap::new(),
            };
            for offset in (usize::from(page_size)..materialized.data.rows.len())
                .step_by(usize::from(page_size))
            {
                before_deadline(end)?;
                let cursor = PageCursor { token: random()? };
                if materialized
                    .offsets
                    .insert(cursor.clone(), offset)
                    .is_some()
                {
                    return Err(Error::Unknown);
                }
                materialized.cursors.insert(offset, cursor);
            }
            let retained = Arc::new(Retained {
                descriptor,
                requested_profile,
                expires,
                _memory: memory,
                result: materialized,
                closed: std::sync::atomic::AtomicBool::new(false),
                http_preflighted: std::sync::atomic::AtomicBool::new(false),
                grpc_preflighted: std::sync::atomic::AtomicBool::new(false),
            });
            preflight_pages(
                &binding,
                retained.requested_profile,
                &retained.descriptor,
                &retained.result,
                end.min(expires),
            )?;
            live(&retained, end)?;
            Ok(retained)
        })
        .await
        .map_err(|_| Error::Unknown)??;
        live(&retained, end)?;
        let reply = open_reply(
            retained.requested_profile,
            &retained.descriptor,
            &retained.result,
        );
        let mut sessions = self.local_sessions.lock().unwrap();
        if sessions.contains_key(&retained.descriptor.token) {
            return Err(Error::Unknown);
        }
        sessions.insert(retained.descriptor.token, retained);
        Ok(reply)
    }
    #[allow(clippy::too_many_arguments)]
    async fn capture_and_execute(
        &self,
        profile: LocalReadProfile,
        version: Option<u64>,
        minimum_version: Option<u64>,
        ttl: Duration,
        partition: u32,
        plan: &velagraph::ObservedGremlinQuery,
        work: Arc<OwnedSemaphorePermit>,
        end: Instant,
    ) -> Result<(Vec<QueryRow>, u64, Instant, CapturedLocal)> {
        if !self
            .config
            .binding
            .cohort
            .members
            .iter()
            .any(|m| m.shard == u64::from(partition))
        {
            return Err(Error::Identity);
        }
        // Reuse the existing process-wide observed coordinator execution gate.
        // Certification and proof preflight are part of the same admitted work.
        let execution = fixed_execution::admit_execution().map_err(Error::Query)?;
        self.expire();
        let memory = self
            .memory
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let mut capture = CapturedLocal::new(work, memory, execution, None);
        let captured = self
            .capture_local(profile, version, minimum_version, ttl, end, &mut capture)
            .await;
        let (version, expires) = match captured {
            Ok(captured) => captured,
            Err(error) => {
                // Ordinary failure preserves the synchronous cleanup contract.
                // Drop is only the fallback when this future is cancelled.
                capture.close().await;
                return Err(error);
            }
        };
        let result = fixed_execution::execute_plan(
            &mut capture.sessions,
            partition,
            plan,
            self.config.local_query_parallelism,
            tokio::time::Instant::from_std(end.min(expires)),
            None,
        )
        .await
        .map_err(Error::Query);
        capture.close().await;
        let rows = result?;
        before_deadline(end.min(expires))?;
        Ok((rows, version, expires, capture))
    }

    pub(super) async fn execute_graph(
        &self,
        request: public_proto::GraphExecuteRequest,
        work: Arc<OwnedSemaphorePermit>,
        end: Instant,
    ) -> Result<(u64, public_proto::QueryData, Instant, OwnedSemaphorePermit)> {
        use public_proto::read_selector::Kind;
        let (version, minimum) = match request
            .read
            .and_then(|read| read.kind)
            .ok_or(Error::Protocol)?
        {
            Kind::Latest(_) => (None, None),
            Kind::Exact(v) => (Some(v), None),
            Kind::Minimum(v) => (None, Some(v)),
        };
        let traversal = request.traversal.ok_or(Error::Protocol)?;
        let partition = traversal.partition;
        let plan = graph_plan(traversal)?;
        let (rows, version, expires, mut capture) = self
            .capture_and_execute(
                LocalReadProfile::default(),
                version,
                minimum,
                MAX_TTL,
                partition,
                &plan,
                work,
                end,
            )
            .await?;
        let data = QueryData {
            rows,
            completeness: QueryCompleteness::Unverified,
        };
        if !query::valid_planned_output(partition, &plan, &data, &self.config.binding) {
            return Err(Error::Query(QueryFailure::IncompleteOrCorrupt));
        }
        encode(&data, pages::MAX_RESULT_BYTES)?;
        let data = public_proto::QueryData {
            rows: data
                .rows
                .iter()
                .map(grpc_codec::out_row)
                .collect::<Result<_>>()?,
            completeness: public_proto::Completeness::Unverified as i32,
        };
        before_deadline(end.min(expires))?;
        Ok((
            version,
            data,
            end.min(expires),
            capture
                .memory
                .take()
                .expect("capture owns result reservation"),
        ))
    }

    pub(super) fn fetch_local(
        &self,
        session: LocalSession,
        query: Query,
        cursor: Option<PageCursor>,
        page_size: u16,
        end: Instant,
    ) -> Result<Reply> {
        let retained = self.get_local(&session)?;
        live(&retained, end)?;
        if retained.result.query != query || retained.result.size != page_size {
            return Err(Error::Identity);
        }
        let offset = match &cursor {
            None => 0,
            Some(cursor) => *retained.result.offsets.get(cursor).ok_or(Error::Identity)?,
        };
        let page = retained.result.page(offset);
        live(&retained, end)?;
        Ok(Reply::FetchLocalQuery {
            session,
            query,
            cursor,
            page_size,
            page,
        })
    }
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    fn traversal() -> public_proto::GraphTraversal {
        use public_proto::{graph_step::Kind, Empty, GraphHop, GraphStep};
        public_proto::GraphTraversal {
            version: 1,
            partition: 7,
            vertex_ids: vec![2, 1, 2],
            steps: vec![
                Kind::Out(GraphHop {
                    label: Some("knows".into()),
                }),
                Kind::In(GraphHop { label: None }),
                Kind::Path(Empty {}),
            ]
            .into_iter()
            .map(|kind| GraphStep { kind: Some(kind) })
            .collect(),
        }
    }
    const TRAVERSAL_TEXT: &str = "g.V(2,1,2).out('knows').in().path()";

    #[test]
    fn graph_plan_matches_ordered_text_and_rejects_invalid_shapes() {
        let input = traversal();
        let expected = velagraph::ObservedGremlinQuery::parse(TRAVERSAL_TEXT).unwrap();
        assert_eq!(graph_plan(input.clone()).unwrap(), expected);
        let mut reordered = input.clone();
        reordered.steps.swap(0, 1);
        assert_ne!(graph_plan(reordered).unwrap(), expected);
        let invalid_cases: [fn(&mut public_proto::GraphTraversal); 8] = [
            |t: &mut public_proto::GraphTraversal| t.version = 0,
            |t| t.vertex_ids.clear(),
            |t| t.vertex_ids = vec![1; 33],
            |t| t.vertex_ids[0] = u64::MAX,
            |t| t.steps[0].kind = None,
            |t| t.steps.swap(0, 2),
            |t| t.steps = vec![t.steps[0].clone(); 4],
            |t| {
                t.steps[0].kind = Some(public_proto::graph_step::Kind::Out(
                    public_proto::GraphHop {
                        label: Some("bad-label".into()),
                    },
                ))
            },
        ];
        for mutate in invalid_cases {
            let mut invalid = input.clone();
            mutate(&mut invalid);
            assert!(graph_plan(invalid).is_err());
        }
    }

    #[test]
    #[ignore = "opt-in timing; use --release --ignored --nocapture"]
    fn graph_plan_benchmark_v1() {
        use std::hint::black_box;
        let typed = traversal();
        let text = DistributedRequest {
            start_partition: 7,
            text: TRAVERSAL_TEXT.into(),
            timeout_millis: 5000,
        };
        assert_eq!(
            fixed_execution::parse(&text).unwrap(),
            graph_plan(typed.clone()).unwrap()
        );
        const N: usize = 2000;
        for warmup in [true, false] {
            let iterations = if warmup { 50 } else { N };
            let start = Instant::now();
            for _ in 0..iterations {
                black_box(fixed_execution::parse(black_box(&text)).unwrap());
            }
            let text_time = start.elapsed();
            let start = Instant::now();
            for _ in 0..iterations {
                black_box(graph_plan(black_box(typed.clone())).unwrap());
            }
            if !warmup {
                println!(
                    "graph_plan_v1 {} iterations={} text_ns/op={} typed_including_clone_ns/op={}",
                    env!("CARGO_PKG_VERSION"),
                    N,
                    text_time.as_nanos() / N as u128,
                    start.elapsed().as_nanos() / N as u128
                );
            }
        }
    }

    use d2_raft::transaction_decisions::Member;
    #[test]
    fn local_send_check_rejects_expiry_and_close_after_encoding() {
        let now = Instant::now();
        let mut retained = Retained {
            requested_profile: None,
            descriptor: LocalSession {
                incarnation: [1; 16],
                token: [1; 32],
                version: 0,
            },
            expires: now + Duration::from_secs(10),
            _memory: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            result: pages::Materialized {
                query: Query {
                    partition: 7,
                    text: "g.V(1).count()".into(),
                },
                size: 1,
                data: QueryData {
                    completeness: QueryCompleteness::Unverified,
                    rows: vec![QueryRow::Count(0)],
                },
                offsets: BTreeMap::new(),
                cursors: BTreeMap::new(),
            },
            closed: std::sync::atomic::AtomicBool::new(false),
            http_preflighted: std::sync::atomic::AtomicBool::new(false),
            grpc_preflighted: std::sync::atomic::AtomicBool::new(false),
        };
        let end = now + Duration::from_secs(20);
        live(&retained, end).unwrap();
        assert!(!retained
            .http_preflighted
            .load(std::sync::atomic::Ordering::Acquire));
        retained.preflight_http(end).unwrap();
        assert!(retained
            .http_preflighted
            .load(std::sync::atomic::Ordering::Acquire));
        retained.preflight_http(end).unwrap();
        // The encoder can complete before TTL while the awaiting task resumes
        // afterwards. The same mandatory pre-send check must reject this state.
        retained.expires = now;
        assert_eq!(live(&retained, end), Err(Error::SessionUnavailable));
        assert_eq!(retained.preflight_http(end), Err(Error::SessionUnavailable));
        retained.expires = now + Duration::from_secs(10);
        retained
            .closed
            .store(true, std::sync::atomic::Ordering::Release);
        assert_eq!(live(&retained, end), Err(Error::SessionUnavailable));
        assert_eq!(retained.preflight_http(end), Err(Error::SessionUnavailable));
    }
    #[test]
    fn seal_checks_late_full_envelope_before_ready() {
        // Synthetic serializer boundary, not a claim that M1's default-100
        // three-hop result grammar can produce this oversized property row.
        let binding = Binding {
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: Cohort {
                profile: Default::default(),
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
            },
            mapping: Mapping::PartitionEqualsShardV1,
        };
        let session = LocalSession {
            incarnation: [255; 16],
            token: [255; 32],
            version: u64::MAX,
        };
        let cursor = PageCursor { token: [255; 32] };
        let result = pages::Materialized {
            query: Query {
                partition: 7,
                text: "g.V(1)".into(),
            },
            size: 1,
            data: QueryData {
                completeness: QueryCompleteness::Unverified,
                rows: vec![
                    QueryRow::Count(1),
                    QueryRow::Value {
                        source: None,
                        value: Value::Text("x".repeat(MAX_RESPONSE_BYTES)),
                    },
                ],
            },
            offsets: [(cursor.clone(), 1)].into(),
            cursors: [(1, cursor)].into(),
        };
        preflight(
            &binding,
            Reply::OpenLocalQuery {
                session: session.clone(),
                query: result.query.clone(),
                page_size: 1,
                page: result.page(0),
            },
        )
        .unwrap();
        assert_eq!(
            preflight_pages(
                &binding,
                None,
                &session,
                &result,
                Instant::now() + Duration::from_secs(5)
            ),
            Err(Error::Capacity)
        );
    }
    #[test]
    fn explicit_profile_first_reply_uses_its_complete_larger_envelope() {
        // Synthetic serializer boundary, not supported M1 grammar output.
        let binding = Binding {
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: Cohort {
                profile: Default::default(),
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
            },
            mapping: Mapping::PartitionEqualsShardV1,
        };
        let session = LocalSession {
            incarnation: [255; 16],
            token: [255; 32],
            version: u64::MAX,
        };
        let mut result = pages::Materialized {
            query: Query {
                partition: 7,
                text: "g.V(1)".into(),
            },
            size: 1,
            data: QueryData {
                completeness: QueryCompleteness::Unverified,
                rows: vec![QueryRow::Value {
                    source: None,
                    value: Value::Text(String::new()),
                }],
            },
            offsets: BTreeMap::new(),
            cursors: BTreeMap::new(),
        };
        let frame_len = |reply| {
            serde_json::to_vec(&Response {
                version: VERSION,
                request_id: [255; 16],
                binding: binding.clone(),
                result: Ok(reply),
            })
            .unwrap()
            .len()
        };
        let old_open = frame_len(open_reply(None, &session, &result));
        let old_fetch = frame_len(Reply::FetchLocalQuery {
            session: session.clone(),
            query: result.query.clone(),
            cursor: None,
            page_size: 1,
            page: result.page(0),
        });
        let padding = MAX_RESPONSE_BYTES - old_open.max(old_fetch);
        result.data.rows = vec![QueryRow::Value {
            source: None,
            value: Value::Text("x".repeat(padding)),
        }];
        let end = Instant::now() + Duration::from_secs(5);
        preflight_pages(&binding, None, &session, &result, end).unwrap();
        for profile in [
            LocalReadProfile::Parity2MiBV1,
            LocalReadProfile::LocalRead8MiBV1,
        ] {
            assert_eq!(
                preflight_pages(&binding, Some(profile), &session, &result, end),
                Err(Error::Capacity)
            );
        }
    }
    #[test]
    fn capture_capacity_errors_do_not_become_unknown_or_identity() {
        assert_eq!(
            remote_error(RemoteError::Application(
                d2_raft::D1Error::VisibilityReadLimit
            )),
            Error::Capacity
        );
    }
}

#[cfg(test)]
mod http_boundary_tests {
    use super::*;
    #[test]
    fn http_preflight_rejects_later_page_before_first_delivery() {
        let session = LocalSession {
            incarnation: [1; 16],
            token: [2; 32],
            version: 1,
        };
        let result = pages::Materialized {
            query: Query {
                partition: 7,
                text: "g.V(1,2).valueMap('x')".into(),
            },
            size: 1,
            data: QueryData {
                completeness: QueryCompleteness::Unverified,
                rows: vec![
                    QueryRow::Count(1),
                    QueryRow::PropertyMap {
                        source: crate::remote::query::QualifiedElement::Vertex {
                            partition: 7,
                            id: 1,
                        },
                        properties: BTreeMap::from([("x".into(), vec![Value::Integer(0); 60_000])]),
                    },
                ],
            },
            offsets: BTreeMap::new(),
            cursors: BTreeMap::new(),
        };
        // A small first page alone is insufficient to admit this result.
        assert!(http_codec::encode(&Ok(open_reply(None, &session, &result))).is_ok());
        assert_eq!(
            preflight_http_pages(&session, &result, Instant::now() + Duration::from_secs(5)),
            Err(Error::Capacity)
        );
    }
}

#[cfg(test)]
mod grpc_identity_boundary_tests {
    use super::*;
    use d2_raft::transaction_decisions::Member;
    use prost::Message;

    #[test]
    fn later_page_identity_overhead_is_rejected_before_profile_first_delivery() {
        // Synthetic encoder boundary, not a claim that query/result logical
        // budgets admit this fixture. It isolates the protobuf envelope proof.
        let binding = Binding {
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: Cohort {
                profile: Default::default(),
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
            },
            mapping: Mapping::PartitionEqualsShardV1,
        };
        let session = LocalSession {
            incarnation: [1; 16],
            token: [2; 32],
            version: 1,
        };
        let cursor = PageCursor { token: [3; 32] };
        let result = |len: usize| pages::Materialized {
            query: Query {
                partition: 7,
                text: "g.V(1,2).values('x')".into(),
            },
            size: 1,
            data: QueryData {
                completeness: QueryCompleteness::Unverified,
                rows: vec![
                    QueryRow::Count(1),
                    QueryRow::Value {
                        source: None,
                        value: Value::Text("x".repeat(len)),
                    },
                ],
            },
            offsets: BTreeMap::from([(cursor.clone(), 1)]),
            cursors: BTreeMap::from([(1, cursor.clone())]),
        };
        let later = |result: &pages::Materialized| Reply::FetchLocalQuery {
            session: session.clone(),
            query: result.query.clone(),
            cursor: Some(cursor.clone()),
            page_size: 1,
            page: result.page(1),
        };
        let (mut low, mut high) = (0, MAX_RESPONSE_BYTES);
        while low < high {
            let mid = low + (high - low).div_ceil(2);
            if grpc_codec::encode_fetch(&later(&result(mid))).is_ok() {
                low = mid;
            } else {
                high = mid - 1;
            }
        }
        let result = result(low);
        let bare = grpc_codec::encode_fetch(&later(&result)).unwrap();
        assert!(bare.encoded_len() <= MAX_RESPONSE_BYTES);
        assert_eq!(
            grpc_codec::preflight_identity(bare, &binding),
            Err(Error::Capacity)
        );
        let profile = Some(LocalReadProfile::LocalRead8MiBV1);
        let first = grpc_codec::encode_open(&open_reply(profile, &session, &result)).unwrap();
        grpc_codec::preflight_identity(first, &binding).unwrap();
        let now = Instant::now();
        let retained = Retained {
            descriptor: session,
            requested_profile: profile,
            expires: now + Duration::from_secs(30),
            _memory: Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap(),
            result,
            closed: std::sync::atomic::AtomicBool::new(false),
            http_preflighted: std::sync::atomic::AtomicBool::new(false),
            grpc_preflighted: std::sync::atomic::AtomicBool::new(false),
        };
        assert_eq!(
            retained.preflight_grpc(&binding, now + Duration::from_secs(20)),
            Err(Error::Capacity)
        );
        assert!(!retained
            .grpc_preflighted
            .load(std::sync::atomic::Ordering::Acquire));
    }
}

#[cfg(test)]
mod readiness_cleanup_tests {
    use super::*;
    use crate::remote::query::QueryStamp;
    use d2_raft::transaction_decisions::Member;

    #[tokio::test]
    async fn saturated_readiness_does_not_queue_or_contact_dependencies() {
        let _fixtures = fixed_execution::lock_session_fixtures().await;
        use d2_raft::managed_rpc::Client;
        let cohort = Cohort {
            profile: Default::default(),
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: vec![Member {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
            }],
        };
        let authority = Authority {
            shard: 9,
            mode: [9; 16],
        };
        let coordinator = Client::new(
            "127.0.0.1:1".parse().unwrap(),
            Identity {
                group: 9,
                node: 1,
                target: Target {
                    shard: 9,
                    epoch: 1,
                    mode: [9; 16],
                },
                role: Role::Coordinator,
                authority: authority.clone(),
                cohort: cohort.clone(),
            },
            Duration::from_secs(5),
        )
        .unwrap();
        let state = State {
            config: RemoteConfig {
                coordinator,
                members: vec![],
                binding: Binding {
                    authority,
                    cohort,
                    mapping: Mapping::PartitionEqualsShardV1,
                },
            }
            .into(),
            incarnation: [1; 16],
            sessions: Mutex::new(BTreeMap::new()),
            local_sessions: Mutex::new(BTreeMap::new()),
            jobs: Mutex::new(BTreeMap::new()),
            job_tasks: Mutex::new(JoinSet::new()),
            job_rpc_budget: Duration::from_secs(5),
            jobs_stopping: std::sync::atomic::AtomicBool::new(false),
            memory: Arc::new(Semaphore::new(1)),
            connections: Arc::new(Semaphore::new(1)),
            submit: tokio::sync::Mutex::new(()),
            readiness: Arc::new(Semaphore::new(1)),
            timing: None,
        };
        let flight = state.readiness.clone().try_acquire_owned().unwrap();
        let work = Arc::new(state.connections.clone().try_acquire_owned().unwrap());
        assert_eq!(
            state
                .probe_readiness(work.clone(), Instant::now() + Duration::from_secs(5))
                .await,
            Err(Error::Capacity)
        );
        drop(flight);
        let memory = state.memory.clone().try_acquire_owned().unwrap();
        assert_eq!(
            state
                .probe_readiness(work.clone(), Instant::now() + Duration::from_secs(5))
                .await,
            Err(Error::Capacity)
        );
        assert_eq!(state.readiness.available_permits(), 1);
        drop(memory);
        state
            .jobs_stopping
            .store(true, std::sync::atomic::Ordering::Release);
        assert_eq!(
            state
                .probe_readiness(work, Instant::now() + Duration::from_secs(5))
                .await,
            Err(Error::Rejected)
        );
    }

    #[tokio::test]
    async fn cancelled_capture_holds_admission_until_bounded_cleanup() {
        let _fixtures = fixed_execution::lock_session_fixtures().await;
        // A listening TCP peer that never speaks gRPC forces cleanup to wait
        // for its deadline, rather than winning a connection-refused race.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let work = Arc::new(Semaphore::new(1));
        let memory = Arc::new(Semaphore::new(1));
        let flight = Arc::new(Semaphore::new(1));
        let execution = fixed_execution::admit_execution().unwrap();
        let mut capture = CapturedLocal::new(
            Arc::new(work.clone().try_acquire_owned().unwrap()),
            memory.clone().try_acquire_owned().unwrap(),
            execution,
            Some(flight.clone().try_acquire_owned().unwrap()),
        );
        let identity = RemoteIdentity { group: 7, node: 1 };
        let descriptor = fixed::FixedDescriptor {
            profile: LocalReadProfile::default(),
            stamp: QueryStamp {
                identity,
                shard: 7,
                epoch: 1,
                sequence: 0,
            },
            incarnation: [1; 16],
            token: [2; 16],
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: Cohort {
                profile: Default::default(),
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
            },
            version: 0,
            mode: [7; 16],
        };
        capture.sessions.insert(
            7,
            fixed_execution::NodeSession::new(
                PartitionRoute {
                    partition: 7,
                    shard: 7,
                    epoch: 1,
                    identity,
                    endpoint,
                },
                descriptor,
                0,
                0,
            )
            .unwrap(),
        );
        drop(capture);
        // Drop has transferred admission to cleanup before scheduling it.
        assert_eq!(work.available_permits(), 0);
        assert_eq!(memory.available_permits(), 0);
        assert_eq!(flight.available_permits(), 0);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if work.available_permits() == 1
                    && memory.available_permits() == 1
                    && flight.available_permits() == 1
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        drop(listener);
    }
}
