//! Narrow query-bound execution over already-certified fixed-V descriptors.
//! This does not enter the observed executor or open/replace any node session.
use super::*;
use fixed::{FixedDescriptor, FixedFragmentRequest};

/// Shared cancellation/lease observation, separate from mutable fragment ledgers.
#[derive(Clone)]
pub(crate) struct ExecutionControl {
    deadline: std::sync::Arc<dyn Fn() -> Result<Instant, QueryFailure> + Send + Sync>,
    stop: tokio::sync::watch::Receiver<bool>,
    rpc_budget: Duration,
}
impl ExecutionControl {
    pub(crate) fn new(
        deadline: std::sync::Arc<dyn Fn() -> Result<Instant, QueryFailure> + Send + Sync>,
        stop: tokio::sync::watch::Receiver<bool>,
        rpc_budget: Duration,
    ) -> Result<Self, QueryFailure> {
        if rpc_budget.is_zero() || rpc_budget > super::super::MAX_DEADLINE {
            return Err(QueryFailure::InvalidArgument);
        }
        Ok(Self {
            deadline,
            stop,
            rpc_budget,
        })
    }
    pub(super) fn check(&self) -> Result<Instant, QueryFailure> {
        if *self.stop.borrow() || self.stop.has_changed().is_err() {
            return Err(QueryFailure::Unavailable);
        }
        let end = (self.deadline)()?;
        check(end)?;
        Ok(end)
    }
    async fn cancelled(&self) {
        let mut stop = self.stop.clone();
        loop {
            let stopped = *stop.borrow();
            if stopped || stop.changed().await.is_err() {
                return;
            }
        }
    }
}

pub(crate) struct NodeSession {
    pub route: PartitionRoute,
    pub descriptor: FixedDescriptor,
    pub(super) sequence: u64,
    pub(super) capture_bytes: usize,
    pub(super) capture_rows: usize,
}
impl NodeSession {
    pub(crate) fn new(
        route: PartitionRoute,
        descriptor: FixedDescriptor,
        capture_bytes: usize,
        capture_rows: usize,
    ) -> Self {
        Self {
            route,
            descriptor,
            sequence: 0,
            capture_bytes,
            capture_rows,
        }
    }
}

pub(crate) fn admit_execution() -> Result<tokio::sync::SemaphorePermit<'static>, QueryFailure> {
    COORDINATORS
        .try_acquire()
        .map_err(|_| QueryFailure::ResourceExhausted)
}

#[cfg(test)]
pub(crate) async fn lock_session_fixtures() -> tokio::sync::MutexGuard<'static, ()> {
    sessions::TEST_SESSION_FIXTURES.lock().await
}

/// Official AST admission happens before any fixed session is allocated.
pub(crate) fn validate(request: &DistributedRequest) -> Result<(), QueryFailure> {
    parse(request).map(|_| ())
}
fn parse(request: &DistributedRequest) -> Result<GremlinQuery, QueryFailure> {
    if request.text.len() > query::MAX_QUERY_TEXT_BYTES {
        return Err(QueryFailure::ResourceExhausted);
    }
    let query = GremlinQuery::parse(&request.text).map_err(query::graph_error)?;
    if !matches!(query.start(), GremlinStart::Vertices(ids) if !ids.is_empty()) {
        return Err(QueryFailure::Unsupported);
    }
    for (index, step) in query.steps().iter().enumerate() {
        match step {
            Step::Existing(GremlinStep::Out(_) | GremlinStep::In(_)) => {}
            Step::Existing(GremlinStep::Path | GremlinStep::Count)
                if index + 1 == query.steps().len() => {}
            _ => return Err(QueryFailure::Unsupported),
        }
    }
    // Match the original explicit-ID grammar/defaults; the parser bounds hops.
    Ok(query)
}
pub(super) struct Prepared {
    route: PartitionRoute,
    request: FixedFragmentRequest,
    pub(super) cost: Cost,
}
pub(super) fn prepare(
    node: &mut NodeSession,
    fragment: Fragment,
    ceiling: Cost,
) -> Result<Prepared, QueryFailure> {
    let mut cost = Cost::default();
    cost.charge(512, 1, ceiling)?;
    let mut request = FixedFragmentRequest {
        descriptor: node.descriptor.clone(),
        request_sequence: node.sequence,
        fragment,
        remaining: Cost {
            bytes: ceiling.bytes.saturating_sub(cost.bytes),
            work: ceiling.work.saturating_sub(cost.work),
        },
    };
    let encoded = serde_json::to_vec(&request).map_err(|_| QueryFailure::InvalidArgument)?;
    cost.charge(encoded.len().saturating_mul(2), 0, ceiling)?;
    request.remaining = Cost {
        bytes: ceiling.bytes.saturating_sub(cost.bytes),
        work: ceiling.work.saturating_sub(cost.work),
    };
    // Unknown/failure ends the query. Never refund work or reopen/retry.
    node.sequence = node
        .sequence
        .checked_add(1)
        .ok_or(QueryFailure::ResourceExhausted)?;
    Ok(Prepared {
        route: node.route.clone(),
        request,
        cost,
    })
}
pub(super) async fn invoke(
    prepared: Prepared,
    end: Instant,
    control: Option<&ExecutionControl>,
) -> Result<fixed::FixedFragmentOutput, QueryFailure> {
    check(end)?;
    // Every leaf samples the current lease and bounded RPC budget at dispatch.
    // A later renewal cannot extend an already in-flight request.
    let rpc_end = match control {
        Some(control) => end
            .min(control.check()?)
            .min(Instant::now() + control.rpc_budget),
        None => end,
    };
    let expected = prepared.request.descriptor.clone();
    let sequence = prepared.request.request_sequence;
    let remaining = prepared.request.remaining;
    let output = fixed::fragment(
        prepared.route.endpoint,
        prepared.route.identity,
        prepared.request,
        rpc_end.saturating_duration_since(Instant::now()),
    )
    .await
    .map_err(remote_failure)??;
    check(rpc_end)?;
    if let Some(control) = control {
        control.check()?;
    }
    if output.descriptor != expected || output.request_sequence != sequence {
        return Err(QueryFailure::IncompleteOrCorrupt);
    }
    if output.cost.bytes > remaining.bytes || output.cost.work > remaining.work {
        return Err(QueryFailure::ResourceExhausted);
    }
    Ok(output)
}
pub(super) async fn fragment(
    sessions: &mut BTreeMap<u64, NodeSession>,
    fragment: Fragment,
    cost: &mut Cost,
    end: Instant,
    control: Option<&ExecutionControl>,
) -> Result<FragmentValue, QueryFailure> {
    check(end)?;
    let node = sessions
        .get_mut(&u64::from(fragment.partition()))
        .ok_or(QueryFailure::UnsupportedScope)?;
    let prepared = prepare(
        node,
        fragment,
        Cost {
            bytes: CEILING.bytes.saturating_sub(cost.bytes),
            work: CEILING.work.saturating_sub(cost.work),
        },
    )?;
    cost.charge(prepared.cost.bytes, prepared.cost.work, CEILING)?;
    let output = invoke(prepared, end, control).await?;
    cost.charge(output.cost.bytes, output.cost.work, CEILING)?;
    Ok(output.value)
}

pub(crate) async fn execute(
    sessions: &mut BTreeMap<u64, NodeSession>,
    request: &DistributedRequest,
    parallelism: usize,
    end: Instant,
) -> Result<Vec<QueryRow>, QueryFailure> {
    execute_inner(sessions, request, parallelism, end, None).await
}

pub(crate) async fn execute_controlled(
    sessions: &mut BTreeMap<u64, NodeSession>,
    request: &DistributedRequest,
    parallelism: usize,
    end: Instant,
    control: ExecutionControl,
) -> Result<Vec<QueryRow>, QueryFailure> {
    control.check()?;
    tokio::select! {
        biased;
        _ = control.cancelled() => Err(QueryFailure::Unavailable),
        result = execute_inner(sessions, request, parallelism, end, Some(control.clone())) => result,
    }
}

async fn execute_inner(
    sessions: &mut BTreeMap<u64, NodeSession>,
    request: &DistributedRequest,
    parallelism: usize,
    end: Instant,
    control: Option<ExecutionControl>,
) -> Result<Vec<QueryRow>, QueryFailure> {
    fixed_parallel::validate_width(parallelism)?;
    let query = parse(request)?;
    let catalog = StaticCatalog {
        partitions: sessions.values().map(|s| s.route.clone()).collect(),
    };
    catalog
        .validate_bounded(fixed::MAX_MEMBERS, fixed::MAX_MEMBERS)
        .map_err(remote_failure)?;
    let mut execution = Execution {
        grpc_clients: None,
        catalog: &catalog,
        sessions: BTreeMap::new(),
        fixed_sessions: Some(sessions),
        control,
        fixed_parallelism: parallelism,
        end,
        cost: Cost {
            bytes: 4096,
            work: 0,
        },
        scan_cost: Cost::default(),
        ceiling: CEILING,
    };
    let output = execution.run(request, &query).await?;
    validate_output_bounded(&output, request, fixed::MAX_MEMBERS).map_err(remote_failure)?;
    execution.check()?;
    Ok(output.rows)
}

pub(crate) async fn close(sessions: &BTreeMap<u64, NodeSession>, end: Instant) {
    for node in sessions.values() {
        if Instant::now() >= end {
            break;
        }
        let _ = fixed::close(
            node.route.endpoint,
            node.route.identity,
            node.descriptor.clone(),
            end.saturating_duration_since(Instant::now())
                .min(Duration::from_millis(20)),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn fixed_subset_uses_official_ast_and_rejects_before_open() {
        let request = |text: &str| DistributedRequest {
            start_partition: 7,
            text: text.into(),
            timeout_millis: 5000,
        };
        for text in ["g.V(1)", "g.V(1,1).out().in('x').path()", "g.V(9).count()"] {
            validate(&request(text)).unwrap();
        }
        for text in [
            "g.V()",
            "g.E(1)",
            "g.V(1).both()",
            "g.V(1).limit(1)",
            "g.V(1).values('x')",
            "g.V(1).repeat(out()).times(2)",
        ] {
            assert!(validate(&request(text)).is_err(), "{text}");
        }
    }
    #[tokio::test]
    async fn execution_control_observes_renewal_and_cancellation() {
        use std::sync::{Arc, Mutex};
        let (stop, receiver) = tokio::sync::watch::channel(false);
        let deadline = Arc::new(Mutex::new(Instant::now() + Duration::from_secs(1)));
        let observed = deadline.clone();
        let control = ExecutionControl::new(
            Arc::new(move || Ok(*observed.lock().unwrap())),
            receiver,
            Duration::from_secs(5),
        )
        .unwrap();
        let first = control.check().unwrap();
        *deadline.lock().unwrap() = first + Duration::from_secs(10);
        assert_eq!(control.check().unwrap(), first + Duration::from_secs(10));
        stop.send(true).unwrap();
        control.cancelled().await;
        assert_eq!(control.check(), Err(QueryFailure::Unavailable));
    }
    #[tokio::test]
    async fn execution_control_fails_when_owner_disappears() {
        use std::sync::Arc;
        let (stop, receiver) = tokio::sync::watch::channel(false);
        let control = ExecutionControl::new(
            Arc::new(|| Ok(Instant::now() + Duration::from_secs(1))),
            receiver,
            Duration::from_secs(5),
        )
        .unwrap();
        drop(stop);
        control.cancelled().await;
        assert_eq!(control.check(), Err(QueryFailure::Unavailable));
    }
    #[test]
    fn execution_control_rejects_expired_and_unbounded_rpc() {
        use std::sync::Arc;
        let (_stop, receiver) = tokio::sync::watch::channel(false);
        let deadline: Arc<dyn Fn() -> Result<Instant, QueryFailure> + Send + Sync> =
            Arc::new(|| Ok(Instant::now() - Duration::from_secs(1)));
        for budget in [Duration::ZERO, Duration::from_secs(61)] {
            assert!(matches!(
                ExecutionControl::new(deadline.clone(), receiver.clone(), budget),
                Err(QueryFailure::InvalidArgument)
            ));
        }
        let control = ExecutionControl::new(deadline, receiver, Duration::from_secs(5)).unwrap();
        assert_eq!(control.check(), Err(QueryFailure::Timeout));
    }
}
