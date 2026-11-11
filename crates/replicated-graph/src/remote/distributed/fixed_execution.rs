//! Narrow query-bound execution over already-certified fixed-V descriptors.
//! This does not enter the observed executor or open/replace any node session.
use super::*;
use fixed::{FixedDescriptor, FixedFragmentRequest};

pub(crate) struct NodeSession {
    pub route: PartitionRoute,
    pub descriptor: FixedDescriptor,
    sequence: u64,
}
impl NodeSession {
    pub(crate) fn new(route: PartitionRoute, descriptor: FixedDescriptor) -> Self {
        Self {
            route,
            descriptor,
            sequence: 0,
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
pub(super) async fn fragment(
    sessions: &mut BTreeMap<u64, NodeSession>,
    fragment: Fragment,
    cost: &mut Cost,
    end: Instant,
) -> Result<FragmentValue, QueryFailure> {
    check(end)?;
    let node = sessions
        .get_mut(&u64::from(fragment.partition()))
        .ok_or(QueryFailure::UnsupportedScope)?;
    cost.charge(512, 1, CEILING)?;
    let mut request = FixedFragmentRequest {
        descriptor: node.descriptor.clone(),
        request_sequence: node.sequence,
        fragment,
        remaining: Cost {
            bytes: MAX_BYTES.saturating_sub(cost.bytes),
            work: MAX_WORK.saturating_sub(cost.work),
        },
    };
    let encoded = serde_json::to_vec(&request).map_err(|_| QueryFailure::InvalidArgument)?;
    cost.charge(encoded.len().saturating_mul(2), 0, CEILING)?;
    request.remaining = Cost {
        bytes: MAX_BYTES.saturating_sub(cost.bytes),
        work: MAX_WORK.saturating_sub(cost.work),
    };
    let sequence = node.sequence;
    // Unknown/failure ends the query. Never refund work or reopen/retry.
    node.sequence = sequence
        .checked_add(1)
        .ok_or(QueryFailure::ResourceExhausted)?;
    let output = fixed::fragment(
        node.route.endpoint,
        node.route.identity,
        request,
        end.saturating_duration_since(Instant::now()),
    )
    .await
    .map_err(remote_failure)??;
    check(end)?;
    if output.descriptor != node.descriptor || output.request_sequence != sequence {
        return Err(QueryFailure::IncompleteOrCorrupt);
    }
    cost.charge(output.cost.bytes, output.cost.work, CEILING)?;
    Ok(output.value)
}

pub(crate) async fn execute(
    sessions: &mut BTreeMap<u64, NodeSession>,
    request: &DistributedRequest,
    end: Instant,
) -> Result<Vec<QueryRow>, QueryFailure> {
    let query = parse(request)?;
    let catalog = StaticCatalog {
        partitions: sessions.values().map(|s| s.route.clone()).collect(),
    };
    catalog.validate().map_err(remote_failure)?;
    let mut execution = Execution {
        catalog: &catalog,
        sessions: BTreeMap::new(),
        fixed_sessions: Some(sessions),
        end,
        cost: Cost {
            bytes: 4096,
            work: 0,
        },
        scan_cost: Cost::default(),
        ceiling: CEILING,
    };
    let output = execution.run(request, &query).await?;
    validate_output(&output, request).map_err(remote_failure)?;
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
}
