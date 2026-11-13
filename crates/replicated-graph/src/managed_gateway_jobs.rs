//! Bounded process-local jobs. Tasks and reservations belong to the gateway,
//! never the connection that happened to admit or poll the job.
use super::*;
use crate::remote::distributed::{fixed, fixed_execution, PartitionRoute};
use lifecycle::{Lifecycle, Phase as LifePhase, Renewal};

/// Opaque identity. A restart invalidates every handle; this is not a credential.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobHandle {
    pub incarnation: [u8; 16],
    pub token: [u8; 32],
}
/// All durations start at admission. Only the idle lease can be renewed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobPolicy {
    pub idle_millis: u64,
    pub execution_millis: u64,
    pub lifetime_millis: u64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobPhase {
    Capturing,
    Running,
    Sealing,
    Ready,
    Cancelled,
    Expired,
    Failed,
}
/// Relative live observation, not an absolute remote clock. Anchor at this RPC's
/// send time. An exact retry preserves generation and the stored server expiry;
/// remaining_millis decreases with elapsed time rather than extending that expiry.
/// This differs from node FixedLeaseGrant, anchored at its ORIGINAL RPC send.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobGrant {
    pub generation: u64,
    pub remaining_millis: u64,
}
/// No intermediate rows. Version and row count appear only after full sealing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobStatus {
    pub phase: JobPhase,
    pub version: Option<u64>,
    pub total: Option<u32>,
    pub failure: Option<Error>,
}
#[derive(Clone, PartialEq)]
struct BindingKey {
    version: Option<u64>,
    query: Query,
    page_size: u16,
    policy: JobPolicy,
}
#[derive(Clone)]
struct Node {
    route: PartitionRoute,
    descriptor: fixed::FixedDescriptor,
    expires: Instant,
}
struct Inner {
    life: Lifecycle,
    nodes: Vec<Node>,
    result: Option<Arc<pages::Materialized>>,
    version: Option<u64>,
    failure: Option<Error>,
}
pub(super) struct Job {
    handle: JobHandle,
    key: BindingKey,
    hard_end: Instant,
    execution_end: Instant,
    inner: Mutex<Inner>,
    stop: watch::Sender<bool>,
    _memory: OwnedSemaphorePermit,
}
fn millis(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}
fn grant_reply(handle: JobHandle, grant: lifecycle::Grant, now: Instant) -> Result<Reply> {
    let remaining_millis = millis(grant.expires.saturating_duration_since(now));
    if remaining_millis == 0 {
        return Err(Error::SessionUnavailable);
    }
    Ok(Reply::RenewLocalQueryJob {
        job: handle,
        grant: JobGrant {
            generation: grant.generation,
            remaining_millis,
        },
    })
}
fn life_error(error: lifecycle::Error) -> Error {
    match error {
        lifecycle::Error::Busy | lifecycle::Error::InvalidPhase => Error::JobBusy,
        lifecycle::Error::InvalidGeneration => Error::Identity,
        lifecycle::Error::InvalidPolicy => Error::Protocol,
        lifecycle::Error::Capacity => Error::Capacity,
        _ => Error::SessionUnavailable,
    }
}
fn expire_required_nodes(inner: &mut Inner, now: Instant) {
    if inner.life.phase(now) == LifePhase::Running
        && inner.nodes.iter().any(|node| now >= node.expires)
    {
        inner.life.expire(now);
    }
}
impl Job {
    pub(super) fn deadline(&self) -> Result<Instant> {
        let mut inner = self.inner.lock().unwrap();
        expire_required_nodes(&mut inner, Instant::now());
        inner.life.deadline(Instant::now()).map_err(life_error)
    }
    pub(super) fn live_ready(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.life.deadline(Instant::now()).map_err(life_error)?;
        if inner.life.phase(Instant::now()) != LifePhase::Ready {
            return Err(Error::JobBusy);
        }
        Ok(())
    }
    fn status(&self, cancel: bool) -> JobStatus {
        let mut inner = self.inner.lock().unwrap();
        let now = Instant::now();
        expire_required_nodes(&mut inner, now);
        if cancel {
            inner.life.cancel(now);
        }
        let phase = inner.life.phase(now);
        if matches!(
            phase,
            LifePhase::Cancelled | LifePhase::Expired | LifePhase::Failed
        ) {
            inner.result = None;
            let _ = self.stop.send(true);
        }
        JobStatus {
            phase: match phase {
                LifePhase::Capturing => JobPhase::Capturing,
                LifePhase::Running => JobPhase::Running,
                LifePhase::Sealing => JobPhase::Sealing,
                LifePhase::Ready => JobPhase::Ready,
                LifePhase::Cancelled => JobPhase::Cancelled,
                LifePhase::Expired => JobPhase::Expired,
                LifePhase::Failed => JobPhase::Failed,
            },
            version: (phase == LifePhase::Ready)
                .then_some(inner.version)
                .flatten(),
            total: inner.result.as_ref().map(|r| r.data.rows.len() as u32),
            failure: inner.failure.clone(),
        }
    }
    fn expire_node_lease(&self) {
        let mut inner = self.inner.lock().unwrap();
        expire_required_nodes(&mut inner, Instant::now());
    }
    fn fail(&self, error: Error) {
        let mut inner = self.inner.lock().unwrap();
        inner.life.fail(Instant::now());
        inner.failure = Some(error);
        inner.result = None;
        let _ = self.stop.send(true);
    }
}
// Cancellation of an individual control connection follows the same phase
// linearization as an unsuccessful node response, never a blanket query abort.
struct RenewalRound {
    job: Arc<Job>,
    proposed: lifecycle::Grant,
    finished: bool,
}
impl Drop for RenewalRound {
    fn drop(&mut self) {
        if !self.finished {
            let mut inner = self.job.inner.lock().unwrap();
            expire_required_nodes(&mut inner, Instant::now());
            let _ = inner
                .life
                .finish_renewal(Instant::now(), self.proposed, false);
            drop(inner);
            self.job.status(false);
        }
    }
}
struct WorkerOwner {
    job: Arc<Job>,
    completed: bool,
}
impl Drop for WorkerOwner {
    fn drop(&mut self) {
        if !self.completed {
            // A panic or supervisor cancellation cannot leave a live job without
            // an owner. Poisoned state remains fail-closed at lookup.
            if let Ok(mut inner) = self.job.inner.lock() {
                inner.life.fail(Instant::now());
                inner.failure = Some(Error::Unknown);
                inner.result = None;
                let _ = self.job.stop.send(true);
            }
        }
    }
}
pub(super) fn valid_status(status: &JobStatus) -> bool {
    match status.phase {
        JobPhase::Ready => {
            status.version.is_some()
                && status.total.is_some_and(|n| n as usize <= pages::MAX_ROWS)
                && status.failure.is_none()
        }
        JobPhase::Capturing | JobPhase::Running | JobPhase::Sealing => {
            status.version.is_none() && status.total.is_none() && status.failure.is_none()
        }
        _ => status.version.is_none() && status.total.is_none(),
    }
}
impl State {
    pub(super) fn expire_jobs(&self) {
        self.jobs.lock().unwrap().retain(|_, job| {
            job.status(false);
            Instant::now() < job.hard_end
        });
        let mut tasks = self.job_tasks.lock().unwrap();
        while tasks.try_join_next().is_some() {}
    }
    pub(super) fn cancel_jobs(&self) {
        let jobs = self.jobs.lock().unwrap();
        self.jobs_stopping
            .store(true, std::sync::atomic::Ordering::Release);
        for job in jobs.values() {
            job.status(true);
        }
    }
    pub(super) fn get_job(&self, handle: &JobHandle) -> Result<Arc<Job>> {
        if handle.incarnation != self.incarnation {
            return Err(Error::SessionUnavailable);
        }
        self.jobs
            .lock()
            .unwrap()
            .values()
            .find(|job| job.handle == *handle && Instant::now() < job.hard_end)
            .cloned()
            .ok_or(Error::SessionUnavailable)
    }
    pub(super) fn start_job(
        self: &Arc<Self>,
        operation_id: [u8; 16],
        version: Option<u64>,
        query: Query,
        page_size: u16,
        policy: JobPolicy,
    ) -> Result<Reply> {
        if self.config.local_routes.is_none() {
            return Err(Error::Query(QueryFailure::Unsupported));
        }
        if operation_id == [0; 16] || !(1..=MAX_PAGE_SIZE).contains(&page_size) {
            return Err(Error::Protocol);
        }
        fixed_execution::validate(&local::request(&query)).map_err(Error::Query)?;
        if !self
            .config
            .binding
            .cohort
            .members
            .iter()
            .any(|m| m.shard == u64::from(query.partition))
        {
            return Err(Error::Identity);
        }
        let now = Instant::now();
        let life = Lifecycle::new(
            now,
            Duration::from_millis(policy.idle_millis),
            Duration::from_millis(policy.execution_millis),
            Duration::from_millis(policy.lifetime_millis),
        )
        .map_err(life_error)?;
        let key = BindingKey {
            version,
            query,
            page_size,
            policy,
        };
        self.expire();
        let mut registry = self.jobs.lock().unwrap();
        if self
            .jobs_stopping
            .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Error::SessionUnavailable);
        }
        if let Some(job) = registry.get(&operation_id) {
            if job.key != key {
                return Err(Error::Identity);
            }
            return Ok(Reply::StartLocalQueryJob {
                operation_id,
                job: job.handle.clone(),
            });
        }
        let execution = fixed_execution::admit_execution().map_err(Error::Query)?;
        let memory = self
            .memory
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Capacity)?;
        let (stop, _) = watch::channel(false);
        let job = Arc::new(Job {
            handle: JobHandle {
                incarnation: self.incarnation,
                token: random()?,
            },
            hard_end: now + Duration::from_millis(key.policy.lifetime_millis),
            execution_end: now + Duration::from_millis(key.policy.execution_millis),
            key,
            inner: Mutex::new(Inner {
                life,
                nodes: Vec::new(),
                result: None,
                version: None,
                failure: None,
            }),
            stop,
            _memory: memory,
        });
        let reply = Reply::StartLocalQueryJob {
            operation_id,
            job: job.handle.clone(),
        };
        local::preflight(&self.config.binding, reply.clone())?;
        registry.insert(operation_id, job.clone());
        let state = self.clone();
        self.job_tasks.lock().unwrap().spawn(async move {
            let mut owner = WorkerOwner {
                job: job.clone(),
                completed: false,
            };
            if let Err(error) = state.run_job(job.clone(), Arc::new(execution)).await {
                job.fail(error);
            }
            owner.completed = true;
        });
        Ok(reply)
    }
    pub(super) fn job_status(&self, handle: JobHandle, cancel: bool) -> Result<Reply> {
        let job = self.get_job(&handle)?;
        let status = job.status(cancel);
        Ok(if cancel {
            Reply::CancelLocalQueryJob {
                job: handle,
                status,
            }
        } else {
            Reply::GetLocalQueryJob {
                job: handle,
                status,
            }
        })
    }
    pub(super) fn fetch_job(
        &self,
        handle: JobHandle,
        query: Query,
        cursor: Option<PageCursor>,
        page_size: u16,
    ) -> Result<Reply> {
        let job = self.get_job(&handle)?;
        job.live_ready()?;
        let inner = job.inner.lock().unwrap();
        let result = inner.result.as_ref().ok_or(Error::SessionUnavailable)?;
        if query != result.query || page_size != result.size {
            return Err(Error::Identity);
        }
        let offset = match &cursor {
            None => 0,
            Some(c) => *result.offsets.get(c).ok_or(Error::Identity)?,
        };
        Ok(Reply::FetchLocalQueryJob {
            job: handle,
            query,
            cursor,
            page_size,
            version: inner.version.ok_or(Error::Identity)?,
            page: result.page(offset),
        })
    }
    pub(super) async fn renew_job(
        &self,
        handle: JobHandle,
        expected: u64,
        rpc_end: Instant,
    ) -> Result<Reply> {
        let job = self.get_job(&handle)?;
        let (proposed, nodes) = {
            let mut inner = job.inner.lock().unwrap();
            expire_required_nodes(&mut inner, Instant::now());
            match inner
                .life
                .begin_renewal(Instant::now(), expected)
                .map_err(life_error)?
            {
                Renewal::Replay(grant) => return grant_reply(handle, grant, Instant::now()),
                Renewal::Begin(grant) => (
                    grant,
                    if inner.life.phase(Instant::now()) == LifePhase::Running {
                        inner.nodes.clone()
                    } else {
                        Vec::new()
                    },
                ),
            }
        };
        // A disconnected renewal must not strand an unowned pending round.
        let mut round = RenewalRound {
            job: job.clone(),
            proposed,
            finished: false,
        };
        let mut all = true;
        for node in nodes {
            let sent = Instant::now();
            let lease_millis = millis(
                proposed
                    .expires
                    .min(job.execution_end)
                    .saturating_duration_since(sent),
            );
            let end = job.deadline()?.min(sent + self.job_rpc_budget).min(rpc_end);
            let transport = self.config.fixed_transport(
                node.route.endpoint,
                node.route.identity,
                node.route.shard,
            )?;
            let grant = transport
                .renew(
                    fixed::FixedRenewRequest {
                        descriptor: node.descriptor.clone(),
                        generation: proposed.generation,
                        lease_millis,
                    },
                    end.saturating_duration_since(sent),
                )
                .await;
            match grant {
                Ok(grant)
                    if grant.descriptor == node.descriptor
                        && grant.generation == proposed.generation
                        && grant.granted_millis > 0
                        && grant.granted_millis <= lease_millis =>
                {
                    let mut inner = job.inner.lock().unwrap();
                    if let Some(n) = inner
                        .nodes
                        .iter_mut()
                        .find(|n| n.descriptor == node.descriptor)
                    {
                        n.expires = sent + Duration::from_millis(grant.granted_millis);
                    }
                }
                _ => {
                    all = false;
                    break;
                }
            }
        }
        before_deadline(rpc_end)?;
        let grant = {
            let mut inner = job.inner.lock().unwrap();
            expire_required_nodes(&mut inner, Instant::now());
            inner
                .life
                .finish_renewal(Instant::now(), proposed, all)
                .map_err(life_error)
        };
        round.finished = true;
        if grant.is_err() {
            job.status(false);
        }
        grant_reply(handle, grant?, Instant::now())
    }

    async fn run_job(
        &self,
        job: Arc<Job>,
        execution: Arc<tokio::sync::SemaphorePermit<'static>>,
    ) -> Result<()> {
        let mut sessions = BTreeMap::new();
        let result = self.capture_execute_job(&job, &mut sessions).await;
        job.expire_node_lease();
        // Sealing transition precedes cleanup: later node failures cannot alter it.
        let result = result.and_then(|(version, rows)| {
            job.inner
                .lock()
                .unwrap()
                .life
                .reads_finished(Instant::now())
                .map_err(life_error)?;
            Ok((version, rows))
        });
        fixed_execution::close(
            &sessions,
            tokio::time::Instant::from_std(Instant::now() + self.job_rpc_budget),
        )
        .await;
        job.inner.lock().unwrap().nodes.clear();
        let (version, rows) = result?;
        let binding = self.config.binding.clone();
        let worker = job.clone();
        let result = tokio::task::spawn_blocking(move || {
            // Both reservation owners survive async cancellation until encoding exits.
            let _execution = execution;
            let query = worker.key.query.clone();
            let data = QueryData {
                rows,
                completeness: QueryCompleteness::Unverified,
            };
            worker.deadline()?;
            if data.rows.len() > pages::MAX_ROWS
                || !query::valid_traversal_output(&query, &data, &binding)
            {
                return Err(Error::Capacity);
            }
            encode(&data, pages::MAX_RESULT_BYTES)?;
            let mut result = pages::Materialized {
                query,
                size: worker.key.page_size,
                data,
                offsets: BTreeMap::new(),
                cursors: BTreeMap::new(),
            };
            for offset in
                (usize::from(result.size)..result.data.rows.len()).step_by(usize::from(result.size))
            {
                worker.deadline()?;
                let cursor = PageCursor { token: random()? };
                if result.offsets.insert(cursor.clone(), offset).is_some() {
                    return Err(Error::Unknown);
                }
                result.cursors.insert(offset, cursor);
            }
            for offset in (0..result.data.rows.len().max(1)).step_by(usize::from(result.size)) {
                worker.deadline()?;
                local::preflight(
                    &binding,
                    Reply::FetchLocalQueryJob {
                        job: worker.handle.clone(),
                        query: result.query.clone(),
                        cursor: result.cursors.get(&offset).cloned(),
                        page_size: result.size,
                        version,
                        page: result.page(offset),
                    },
                )?;
            }
            worker.deadline()?;
            Ok(Arc::new(result))
        })
        .await
        .map_err(|_| Error::Unknown)??;
        let mut inner = job.inner.lock().unwrap();
        inner.life.publish(Instant::now()).map_err(life_error)?;
        inner.version = Some(version);
        inner.result = Some(result);
        Ok(())
    }
    async fn capture_execute_job(
        &self,
        job: &Arc<Job>,
        sessions: &mut BTreeMap<u64, fixed_execution::NodeSession>,
    ) -> Result<(u64, Vec<QueryRow>)> {
        let routes = self.config.local_routes.as_ref().ok_or(Error::Identity)?;
        let initial_end = job.deadline()?;
        let cut = tokio::time::timeout_at(
            tokio::time::Instant::from_std(initial_end.min(Instant::now() + self.job_rpc_budget)),
            cohort_reads::admit_endpoints(
                self.config.coordinator.reference(),
                self.config.binding.authority.clone(),
                self.config.binding.cohort.coordinator_epoch,
                self.config.endpoints(),
                job.key.version,
                initial_end.saturating_duration_since(Instant::now()),
            ),
        )
        .await
        .map_err(|_| Error::SessionUnavailable)?
        .map_err(local::snapshot_error)?;
        if cut.cohort() != &self.config.binding.cohort
            || cut.authority() != &self.config.binding.authority
        {
            return Err(Error::Identity);
        }
        let acquire_end = initial_end.min(cut.deadline());
        let remaining = |rows: usize, bytes: usize, ops: usize| ReadLimit {
            rows: fixed::CAPTURE_ROWS.saturating_sub(rows),
            bytes: fixed::CAPTURE_BYTES.saturating_sub(bytes),
            operations: fixed::CAPTURE_OPERATIONS.saturating_sub(ops),
        };
        let make_open = |target: Target, limit: ReadLimit| {
            let now = Instant::now();
            fixed::RenewableFixedOpenRequest {
                open: fixed::FixedOpenRequest {
                    profile: fixed::LocalReadProfile::Parity2MiBV1,
                    target,
                    proof: cut.proof().clone(),
                    capture_limit: limit,
                    ttl_millis: millis(
                        initial_end
                            .min(job.execution_end)
                            .saturating_duration_since(now),
                    ),
                    acquire_timeout_millis: millis(
                        acquire_end
                            .saturating_duration_since(now)
                            .min(self.job_rpc_budget),
                    )
                    .min(5000),
                },
                hard_lifetime_millis: millis(job.execution_end.saturating_duration_since(now)),
            }
        };
        for (route, (_, target)) in routes.iter().zip(cut.members()) {
            job.deadline()?;
            fixed::renewable_preflight(
                route.identity,
                &make_open(target.clone(), remaining(0, 0, 0)),
                tokio::time::Instant::from_std(acquire_end),
            )
            .map_err(local::remote_error)?;
        }
        let (mut rows, mut bytes, mut operations) = (0usize, 0usize, 0usize);
        let mut nodes = Vec::new();
        for (route, (_, target)) in routes.iter().zip(cut.members()) {
            job.deadline()?;
            cut.check().map_err(local::snapshot_error)?;
            let limit = remaining(rows, bytes, operations);
            if limit.rows == 0 || limit.bytes == 0 || limit.operations == 0 {
                return Err(Error::Capacity);
            }
            let request = make_open(target.clone(), limit);
            let requested_lease = request.open.ttl_millis;
            let requested_profile = request.open.profile;
            let sent = Instant::now();
            let transport = self.config.local_transport(route)?;
            let opened = transport
                .renewable_open(
                    request,
                    acquire_end
                        .saturating_duration_since(sent)
                        .min(self.job_rpc_budget),
                )
                .await
                .map_err(local::remote_error)?;
            let descriptor = &opened.opened.descriptor;
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
            let route = PartitionRoute {
                partition: u32::try_from(target.shard).map_err(|_| Error::Identity)?,
                shard: target.shard,
                epoch: target.epoch,
                identity: route.identity,
                endpoint: route.endpoint,
            };
            sessions.insert(
                target.shard,
                fixed_execution::NodeSession::new(
                    route.clone(),
                    descriptor.clone(),
                    opened.opened.capture_cost.bytes,
                    opened.opened.capture_cost.rows,
                )
                .with_transport(transport),
            );
            if descriptor.profile != requested_profile
                || opened.opened.retained_proof != *cut.proof()
                || opened.grant.descriptor != *descriptor
                || opened.grant.generation != 0
                || opened.grant.granted_millis == 0
                || opened.grant.granted_millis > requested_lease
            {
                return Err(Error::Identity);
            }
            nodes.push(Node {
                route,
                descriptor: descriptor.clone(),
                expires: sent + Duration::from_millis(opened.grant.granted_millis),
            });
            rows = rows
                .checked_add(opened.opened.capture_cost.rows)
                .ok_or(Error::Capacity)?;
            bytes = bytes
                .checked_add(opened.opened.capture_cost.bytes)
                .ok_or(Error::Capacity)?;
            operations = operations
                .checked_add(opened.opened.capture_cost.replay_operations)
                .ok_or(Error::Capacity)?;
            if rows > fixed::CAPTURE_ROWS
                || bytes > fixed::CAPTURE_BYTES
                || operations > fixed::CAPTURE_OPERATIONS
            {
                return Err(Error::Capacity);
            }
        }
        {
            let mut inner = job.inner.lock().unwrap();
            inner.life.captured(Instant::now()).map_err(life_error)?;
            inner.nodes = nodes;
        }
        let controlled = job.clone();
        let control = fixed_execution::ExecutionControl::new(
            Arc::new(move || {
                let mut inner = controlled.inner.lock().unwrap();
                expire_required_nodes(&mut inner, Instant::now());
                let end = inner
                    .life
                    .deadline(Instant::now())
                    .map_err(|_| QueryFailure::Timeout)?;
                let end = inner.nodes.iter().fold(end, |end, n| end.min(n.expires));
                if Instant::now() >= end {
                    return Err(QueryFailure::Timeout);
                }
                Ok(tokio::time::Instant::from_std(end))
            }),
            job.stop.subscribe(),
            self.job_rpc_budget,
        )
        .map_err(Error::Query)?;
        let rows = fixed_execution::execute_controlled(
            sessions,
            &local::request(&job.key.query),
            self.config.local_query_parallelism,
            tokio::time::Instant::from_std(job.execution_end),
            control,
        )
        .await
        .map_err(Error::Query)?;
        Ok((cut.version(), rows))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture(pool: Arc<Semaphore>, now: Instant) -> Arc<Job> {
        let (stop, _) = watch::channel(false);
        Arc::new(Job {
            handle: JobHandle {
                incarnation: [1; 16],
                token: [2; 32],
            },
            key: BindingKey {
                version: Some(7),
                query: Query {
                    partition: 7,
                    text: "g.V(1).count()".into(),
                },
                page_size: 1,
                policy: JobPolicy {
                    idle_millis: 60_000,
                    execution_millis: 120_000,
                    lifetime_millis: 300_000,
                },
            },
            hard_end: now + Duration::from_secs(300),
            execution_end: now + Duration::from_secs(120),
            inner: Mutex::new(Inner {
                life: Lifecycle::new(
                    now,
                    Duration::from_secs(60),
                    Duration::from_secs(120),
                    Duration::from_secs(300),
                )
                .unwrap(),
                nodes: vec![],
                result: None,
                version: None,
                failure: None,
            }),
            stop,
            _memory: pool.try_acquire_owned().unwrap(),
        })
    }
    fn ready(job: &Job) {
        let mut inner = job.inner.lock().unwrap();
        let now = Instant::now();
        inner.life.captured(now).unwrap();
        inner.life.reads_finished(now).unwrap();
        inner.life.publish(now).unwrap();
        inner.version = Some(7);
        inner.result = Some(Arc::new(pages::Materialized {
            query: job.key.query.clone(),
            size: 1,
            data: QueryData {
                rows: vec![QueryRow::Count(3)],
                completeness: QueryCompleteness::Unverified,
            },
            offsets: BTreeMap::new(),
            cursors: BTreeMap::new(),
        }));
    }
    #[test]
    fn terminal_records_drop_payload_but_keep_shared_slot_until_forgotten() {
        let pool = Arc::new(Semaphore::new(1));
        let job = fixture(pool.clone(), Instant::now());
        ready(&job);
        let worker = job.clone();
        assert_eq!(job.status(false).total, Some(1));
        assert_eq!(job.status(true).phase, JobPhase::Cancelled);
        assert!(job.inner.lock().unwrap().result.is_none());
        assert_eq!(job.live_ready(), Err(Error::SessionUnavailable));
        assert_eq!(pool.available_permits(), 0);
        drop(job);
        assert_eq!(pool.available_permits(), 0);
        drop(worker);
        assert_eq!(pool.available_permits(), 1);
    }
    #[test]
    fn no_intermediate_rows_and_expired_ready_never_reappears() {
        let pool = Arc::new(Semaphore::new(1));
        let job = fixture(pool, Instant::now());
        assert_eq!(job.live_ready(), Err(Error::JobBusy));
        assert_eq!(job.status(false).version, None);
        ready(&job);
        job.inner
            .lock()
            .unwrap()
            .life
            .phase(Instant::now() + Duration::from_secs(60));
        let status = job.status(false);
        assert_eq!(status.phase, JobPhase::Expired);
        assert_eq!(status.version, None);
        assert_eq!(status.total, None);
        assert_eq!(job.live_ready(), Err(Error::SessionUnavailable));
    }
    #[test]
    fn lost_renewal_connection_is_phase_aware_and_does_not_reopen_nodes() {
        for sealed in [false, true] {
            let job = fixture(Arc::new(Semaphore::new(1)), Instant::now());
            let mut inner = job.inner.lock().unwrap();
            inner.life.captured(Instant::now()).unwrap();
            let Renewal::Begin(proposed) = inner.life.begin_renewal(Instant::now(), 0).unwrap()
            else {
                panic!("new round")
            };
            if sealed {
                inner.life.reads_finished(Instant::now()).unwrap();
                inner.life.publish(Instant::now()).unwrap();
            }
            drop(inner);
            drop(RenewalRound {
                job: job.clone(),
                proposed,
                finished: false,
            });
            let mut inner = job.inner.lock().unwrap();
            assert_eq!(
                inner.life.phase(Instant::now()),
                if sealed {
                    LifePhase::Ready
                } else {
                    LifePhase::Failed
                }
            );
            if sealed {
                assert_eq!(inner.life.grant(Instant::now()).unwrap(), proposed);
            }
            assert!(inner.nodes.is_empty());
        }
    }
    #[test]
    fn exact_binding_includes_all_query_and_policy_fields() {
        let job = fixture(Arc::new(Semaphore::new(1)), Instant::now());
        for mutation in 0..5 {
            let mut changed = job.key.clone();
            match mutation {
                0 => changed.version = None,
                1 => changed.query.text.push(' '),
                2 => changed.query.partition += 1,
                3 => changed.page_size += 1,
                _ => changed.policy.execution_millis -= 1,
            }
            assert!(changed != job.key);
        }
    }
    #[test]
    fn only_expired_required_node_grants_expire_job() {
        use crate::remote::{query::QueryStamp, RemoteIdentity};
        let now = Instant::now();
        let identity = RemoteIdentity { group: 7, node: 1 };
        let node = Node {
            route: PartitionRoute {
                partition: 7,
                shard: 7,
                epoch: 1,
                identity,
                endpoint: "127.0.0.1:1".parse().unwrap(),
            },
            descriptor: fixed::FixedDescriptor {
                profile: fixed::LocalReadProfile::Parity2MiBV1,
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
                    members: vec![],
                },
                version: 7,
                mode: [7; 16],
            },
            expires: now,
        };
        for sealed in [false, true] {
            let job = fixture(Arc::new(Semaphore::new(1)), now);
            let mut inner = job.inner.lock().unwrap();
            inner.life.captured(now).unwrap();
            if sealed {
                inner.life.reads_finished(now).unwrap();
            }
            inner.nodes.push(node.clone());
            expire_required_nodes(&mut inner, now);
            assert_eq!(
                inner.life.phase(now),
                if sealed {
                    LifePhase::Sealing
                } else {
                    LifePhase::Expired
                }
            );
        }
        let job = fixture(Arc::new(Semaphore::new(1)), now);
        let mut inner = job.inner.lock().unwrap();
        inner.life.captured(now).unwrap();
        let mut live_node = node;
        live_node.expires = now + Duration::from_secs(10);
        inner.nodes.push(live_node);
        expire_required_nodes(&mut inner, now);
        inner.life.fail(now);
        assert_eq!(inner.life.phase(now), LifePhase::Failed);
    }
    #[test]
    fn abandoned_owner_fails_without_releasing_inflight_reservation() {
        let pool = Arc::new(Semaphore::new(1));
        let job = fixture(pool.clone(), Instant::now());
        drop(WorkerOwner {
            job: job.clone(),
            completed: false,
        });
        assert_eq!(job.status(false).phase, JobPhase::Failed);
        assert_eq!(pool.available_permits(), 0);
        drop(job);
        assert_eq!(pool.available_permits(), 1);
    }
    #[test]
    fn wire_status_cannot_publish_unsealed_metadata_or_oversized_result() {
        let mut status = JobStatus {
            phase: JobPhase::Running,
            version: None,
            total: None,
            failure: None,
        };
        assert!(valid_status(&status));
        status.version = Some(0);
        assert!(!valid_status(&status));
        status.phase = JobPhase::Ready;
        status.total = Some(pages::MAX_ROWS as u32 + 1);
        assert!(!valid_status(&status));
        status.total = Some(0);
        assert!(valid_status(&status));
        status.failure = Some(Error::Unknown);
        assert!(!valid_status(&status));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn whole_renewal_rpc_is_five_seconds_not_five_per_member() {
        use crate::remote::{query::QueryStamp, RemoteIdentity};
        use d2_raft::managed_rpc::{Identity, Role};
        use d2_raft::transaction_decisions::Member;
        use std::sync::atomic::{AtomicUsize, Ordering};
        let cohort = Cohort {
            profile: Default::default(),
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: vec![
                Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                },
                Member {
                    shard: 8,
                    epoch: 1,
                    mode: [8; 16],
                },
            ],
        };
        let binding = Binding {
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: cohort.clone(),
            mapping: Mapping::PartitionEqualsShardV1,
        };
        let memory = Arc::new(Semaphore::new(1));
        let job = fixture(memory.clone(), Instant::now());
        let calls = Arc::new(AtomicUsize::new(0));
        let mut responders = Vec::new();
        let mut nodes = Vec::new();
        for shard in [7, 8] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let endpoint = listener.local_addr().unwrap();
            let observed = calls.clone();
            responders.push(tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let request: serde_json::Value = read_frame(&mut socket, MAX_REQUEST_BYTES).await.unwrap();
                let renew = &request["operation"]["RenewFixedQuery"];
                let reply = serde_json::json!({"version":1,"result":{"Ok":{"RenewFixedQuery":{
                    "descriptor": renew["descriptor"], "generation": renew["generation"], "granted_millis": renew["lease_millis"]
                }}}});
                observed.fetch_add(1, Ordering::SeqCst);
                // Each response is individually inside five seconds. Their
                // serial total exceeds the entire control operation's budget.
                tokio::time::sleep(Duration::from_millis(2600)).await;
                let bytes = serde_json::to_vec(&reply).unwrap();
                let _ = write_bytes(&mut socket, &bytes).await;
            }));
            let identity = RemoteIdentity {
                group: shard,
                node: 1,
            };
            nodes.push(Node {
                route: PartitionRoute {
                    partition: shard as u32,
                    shard,
                    epoch: 1,
                    identity,
                    endpoint,
                },
                descriptor: fixed::FixedDescriptor {
                    profile: fixed::LocalReadProfile::Parity2MiBV1,
                    stamp: QueryStamp {
                        identity,
                        shard,
                        epoch: 1,
                        sequence: 0,
                    },
                    incarnation: [1; 16],
                    token: [shard as u8; 16],
                    authority: binding.authority.clone(),
                    cohort: cohort.clone(),
                    version: 7,
                    mode: [shard as u8; 16],
                },
                expires: Instant::now() + Duration::from_secs(60),
            });
        }
        {
            let mut inner = job.inner.lock().unwrap();
            inner.life.captured(Instant::now()).unwrap();
            inner.nodes = nodes;
        }
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
                authority: binding.authority.clone(),
                cohort,
            },
            Duration::from_secs(15),
        )
        .unwrap();
        let state = Arc::new(State {
            config: RemoteConfig {
                coordinator,
                binding: binding.clone(),
                members: vec![],
            }
            .into(),
            incarnation: [1; 16],
            sessions: Mutex::new(BTreeMap::new()),
            local_sessions: Mutex::new(BTreeMap::new()),
            jobs: Mutex::new(BTreeMap::from([([1; 16], job.clone())])),
            job_tasks: Mutex::new(JoinSet::new()),
            job_rpc_budget: Duration::from_secs(5),
            jobs_stopping: std::sync::atomic::AtomicBool::new(false),
            memory,
            connections: Arc::new(Semaphore::new(1)),
            submit: tokio::sync::Mutex::new(()),
            timing: None,
        });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = listener.local_addr().unwrap();
        let worker = state.clone();
        let handler = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let permit = Arc::new(worker.connections.clone().try_acquire_owned().unwrap());
            // Reproduce a gateway configured for fifteen seconds, proving the
            // new operation applies its stricter total five-second profile.
            let _ = serve(
                socket,
                worker,
                permit,
                Instant::now() + Duration::from_secs(15),
            )
            .await;
        });
        let start = Instant::now();
        assert!(request(
            endpoint,
            binding,
            Operation::RenewLocalQueryJob {
                job: job.handle.clone(),
                expected_generation: 0
            },
            Duration::from_secs(15)
        )
        .await
        .is_err());
        assert!(start.elapsed() < Duration::from_secs(7));
        handler.await.unwrap();
        for responder in responders {
            responder.await.unwrap();
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(job.status(false).phase, JobPhase::Failed);
        assert_eq!(
            job.inner
                .lock()
                .unwrap()
                .life
                .begin_renewal(Instant::now(), 0),
            Err(lifecycle::Error::Failed)
        );
    }
    #[test]
    fn submillisecond_grants_are_unavailable_not_identity_failures() {
        let now = Instant::now();
        let handle = JobHandle {
            incarnation: [1; 16],
            token: [2; 32],
        };
        assert_eq!(
            grant_reply(
                handle.clone(),
                lifecycle::Grant {
                    generation: 1,
                    expires: now + Duration::from_micros(999)
                },
                now
            ),
            Err(Error::SessionUnavailable)
        );
        assert!(grant_reply(
            handle,
            lifecycle::Grant {
                generation: 1,
                expires: now + Duration::from_millis(1)
            },
            now
        )
        .is_ok());
    }
}
