//! Explicit, fail-closed consistency negotiation for distributed queries.
//!
//! This is an admission contract, not an MVCC or distributed transaction
//! implementation. Legacy APIs continue to use independent shard snapshots.
use super::*;

/// The visibility guarantee required for the entire query, including job pages.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadConsistency {
    /// Each shard is frozen independently. The vector need not be a global cut.
    IndependentShardSnapshots,
    /// One complete committed global version, excluding unresolved transactions.
    /// Currently unsupported, even for a catalog containing only one shard.
    GlobalCommittedSnapshot,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Envelope {
    pub consistency_request: Request,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Request {
    version: u32,
    required: ReadConsistency,
    operation: Operation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) enum Operation {
    Query(DistributedRequest),
    Job(jobs::Request),
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Response {
    version: u32,
    result: Result<Accepted, QueryFailure>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Accepted {
    consistency: ReadConsistency,
    output: Output,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum Output {
    Query(DistributedOutput),
    Job(jobs::Output),
}

impl Request {
    pub(super) fn deadline(&self, end: Instant) -> Result<Instant, RemoteError> {
        match &self.operation {
            Operation::Query(request) => {
                validate_request(request)?;
                Ok(end.min(Instant::now() + Duration::from_millis(request.timeout_millis)))
            }
            Operation::Job(_) => Ok(end),
        }
    }
}

pub(super) async fn handle(
    catalog: Arc<StaticCatalog>,
    jobs: &jobs::Jobs,
    request: Request,
    end: Instant,
) -> Result<Vec<u8>, QueryFailure> {
    // Admission precedes session creation, job allocation or token lookup. Do
    // not infer global atomicity from shard count: writes may be staged or
    // bypass the future transaction authority even in a single-shard catalog.
    let result = if request.version != 1
        || request.required != ReadConsistency::IndependentShardSnapshots
    {
        Err(QueryFailure::Unsupported)
    } else {
        match request.operation {
            Operation::Query(request) => execute(&catalog, request, end).await.map(Output::Query),
            Operation::Job(request) => jobs.handle(catalog, request, end).await.map(Output::Job),
        }
        .map(|output| Accepted {
            consistency: ReadConsistency::IndependentShardSnapshots,
            output,
        })
    };
    bounded_encode(&Response { version: 1, result }, end)
}

/// Execute with an explicit visibility requirement. No downgrade or retry occurs.
/// Old coordinators reject this envelope rather than executing a weaker query.
pub async fn query_with_consistency(
    endpoint: SocketAddr,
    request: DistributedRequest,
    required: ReadConsistency,
    deadline: Duration,
) -> Result<DistributedResponse, RemoteError> {
    validate_request(&request)?;
    match exchange(endpoint, Operation::Query(request), required, deadline).await? {
        Ok(Output::Query(output)) => Ok(Ok(output)),
        Ok(_) => Err(RemoteError::Protocol),
        Err(error) => Ok(Err(error)),
    }
}

/// Start, advance, read or cancel a job without silently weakening consistency.
/// A strict request cannot adopt or advance a legacy independent-snapshot token.
pub async fn job_with_consistency(
    endpoint: SocketAddr,
    request: jobs::Request,
    required: ReadConsistency,
    deadline: Duration,
) -> Result<jobs::Response, RemoteError> {
    if matches!(&request, jobs::Request::Start {text,..} | jobs::Request::StartRows {text,..} if text.len() > query::MAX_QUERY_TEXT_BYTES)
    {
        return Err(RemoteError::ResourceExhausted);
    }
    match exchange(endpoint, Operation::Job(request), required, deadline).await? {
        Ok(Output::Job(output)) => Ok(Ok(output)),
        Ok(_) => Err(RemoteError::Protocol),
        Err(error) => Ok(Err(error)),
    }
}

fn validate(
    response: &Response,
    operation: &Operation,
    required: ReadConsistency,
) -> Result<(), RemoteError> {
    if response.version != 1 {
        return Err(RemoteError::Protocol);
    }
    if let Ok(accepted) = &response.result {
        // This client implements only the independent-snapshot contract. Never
        // accept an unsupported global claim from an old/misconfigured peer.
        if accepted.consistency != required
            || accepted.consistency != ReadConsistency::IndependentShardSnapshots
        {
            return Err(RemoteError::Protocol);
        }
        match (&accepted.output, operation) {
            (Output::Query(output), Operation::Query(request)) => validate_output(output, request)?,
            (Output::Job(output), Operation::Job(request)) => {
                jobs::validate_output(output, request)?
            }
            _ => return Err(RemoteError::Protocol),
        }
    }
    Ok(())
}

async fn exchange(
    endpoint: SocketAddr,
    operation: Operation,
    required: ReadConsistency,
    deadline: Duration,
) -> Result<Result<Output, QueryFailure>, RemoteError> {
    let end = checked_deadline(deadline.min(Duration::from_millis(MAX_TIMEOUT)), None)?;
    let _permit = CLIENT_ADMISSION
        .try_acquire()
        .map_err(|_| RemoteError::ResourceExhausted)?;
    tokio::time::timeout_at(end, async {
        let mut socket = TcpStream::connect(endpoint)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let envelope = Envelope {
            consistency_request: Request {
                version: 1,
                required,
                operation,
            },
        };
        write_frame(&mut socket, &envelope, end).await?;
        let response: Response = read_frame_limited(&mut socket, end, MAX_WIRE).await?;
        validate(&response, &envelope.consistency_request.operation, required)?;
        check(end).map_err(|_| RemoteError::Timeout { request_id: None })?;
        Ok(response.result.map(|accepted| accepted.output))
    })
    .await
    .unwrap_or(Err(RemoteError::Timeout { request_id: None }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn operation() -> Operation {
        Operation::Job(jobs::Request::Cancel { token: [0; 16] })
    }
    fn accepted(consistency: ReadConsistency) -> Response {
        Response {
            version: 1,
            result: Ok(Accepted {
                consistency,
                output: Output::Job(jobs::Output::Cancelled),
            }),
        }
    }
    #[test]
    fn client_rejects_downgrade_unknown_version_and_global_claim() {
        use ReadConsistency::*;
        assert!(validate(
            &accepted(IndependentShardSnapshots),
            &operation(),
            IndependentShardSnapshots
        )
        .is_ok());
        assert!(validate(
            &accepted(IndependentShardSnapshots),
            &operation(),
            GlobalCommittedSnapshot
        )
        .is_err());
        assert!(validate(
            &accepted(GlobalCommittedSnapshot),
            &operation(),
            GlobalCommittedSnapshot
        )
        .is_err());
        let mut response = accepted(IndependentShardSnapshots);
        response.version = 2;
        assert!(validate(&response, &operation(), IndependentShardSnapshots).is_err());
        response.version = 1;
        let query = Operation::Query(DistributedRequest {
            start_partition: 0,
            text: "g.V(1)".into(),
            timeout_millis: 1000,
        });
        assert!(validate(&response, &query, IndependentShardSnapshots).is_err());
    }
    #[test]
    fn legacy_and_unknown_envelopes_do_not_silently_parse() {
        let envelope = Envelope {
            consistency_request: Request {
                version: 1,
                required: ReadConsistency::GlobalCommittedSnapshot,
                operation: operation(),
            },
        };
        let json = serde_json::to_vec(&envelope).unwrap();
        assert!(serde_json::from_slice::<DistributedRequest>(&json).is_err());
        assert!(serde_json::from_slice::<jobs::Envelope>(&json).is_err());
        let mut mixed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        mixed["start_partition"] = 0.into();
        mixed["text"] = "g.V(1)".into();
        mixed["timeout_millis"] = 1000.into();
        assert!(serde_json::from_value::<Envelope>(mixed.clone()).is_err());
        assert!(serde_json::from_value::<DistributedRequest>(mixed).is_err());
        let mut mixed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        mixed["hierarchy_job"] =
            serde_json::to_value(jobs::Request::Cancel { token: [0; 16] }).unwrap();
        assert!(serde_json::from_value::<Envelope>(mixed.clone()).is_err());
        assert!(serde_json::from_value::<jobs::Envelope>(mixed).is_err());
        let json = String::from_utf8(json)
            .unwrap()
            .replace("GlobalCommittedSnapshot", "FutureMode");
        assert!(serde_json::from_str::<Envelope>(&json).is_err());
    }
    #[tokio::test]
    async fn unknown_protocol_version_is_rejected_before_job_lookup() {
        let bytes = handle(
            Arc::new(StaticCatalog { partitions: vec![] }),
            &jobs::Jobs::new(),
            Request {
                version: 2,
                required: ReadConsistency::IndependentShardSnapshots,
                operation: operation(),
            },
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        let response: Response = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(response.version, 1);
        assert!(matches!(response.result, Err(QueryFailure::Unsupported)));
    }
}
