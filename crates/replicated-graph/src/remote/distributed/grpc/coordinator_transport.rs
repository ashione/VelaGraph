//! Typed observed coordinator ingress. No managed/fixed consistency or fallback.
use super::coordinator_codec as codec;
use super::*;
use crate::remote::paged::grpc::{AdmittedService, LimitedIo};
use prost::Message;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use wire::coordinator_service_client::CoordinatorServiceClient;
use wire::coordinator_service_server::{CoordinatorService, CoordinatorServiceServer};

const INGRESS_REQUEST_LIMIT: usize = query::MAX_QUERY_TEXT_BYTES + 1024;

#[derive(Clone)]
struct CoordinatorHandler {
    catalog: Arc<StaticCatalog>,
    clients: Arc<BTreeMap<u64, QueryClient>>,
    jobs: Arc<jobs::Jobs>,
    deadline: Duration,
}
#[tonic::async_trait]
impl CoordinatorService for CoordinatorHandler {
    async fn query(
        &self,
        request: tonic::Request<wire::CoordinatorQueryRequest>,
    ) -> Result<tonic::Response<wire::CoordinatorQueryResponse>, Status> {
        let request = codec::decode_query_request(request.into_inner()).map_err(status)?;
        validate_request(&request).map_err(status)?;
        let end = Instant::now()
            + self
                .deadline
                .min(Duration::from_millis(request.timeout_millis));
        let result = tokio::time::timeout_at(
            end,
            execute_transport(&self.catalog, request, end, Some(&self.clients)),
        )
        .await
        .unwrap_or(Err(QueryFailure::Timeout));
        account_output(&result, end)?;
        let output = codec::encode_query_response(result);
        bounded_response(output, end)
    }
    async fn job(
        &self,
        request: tonic::Request<wire::CoordinatorJobRequest>,
    ) -> Result<tonic::Response<wire::CoordinatorJobResponse>, Status> {
        let (request, duration) =
            codec::decode_job_request(request.into_inner()).map_err(status)?;
        let end = Instant::now() + self.deadline.min(duration);
        // No transparent retry: timeout/cancellation can consume an Advance.
        let result =
            tokio::time::timeout_at(end, self.jobs.handle(self.catalog.clone(), request, end))
                .await
                .unwrap_or(Err(QueryFailure::Timeout));
        account_output(&result, end)?;
        let output = codec::encode_job_response(result);
        bounded_response(output, end)
    }
}
fn account_output<T: Serialize>(output: &T, end: Instant) -> Result<(), Status> {
    bounded_encoded_size(output, end)
        .map(|_| ())
        .map_err(|error| {
            status(match error {
                QueryFailure::Timeout => RemoteError::Timeout { request_id: None },
                _ => RemoteError::ResourceExhausted,
            })
        })
}
fn bounded_response<T: Message>(output: T, end: Instant) -> Result<tonic::Response<T>, Status> {
    if output.encoded_len() > MAX_WIRE {
        return Err(status(RemoteError::ResourceExhausted));
    }
    if Instant::now() >= end {
        return Err(status(RemoteError::Timeout { request_id: None }));
    }
    Ok(response(output, end))
}

/// Observed coordinator endpoint with typed gRPC on both ingress and shard routes.
/// Configuration is trusted, immutable and server-side. No request selects peers.
pub struct GrpcCoordinatorService {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    hard_stop: CancellationToken,
    task: Option<JoinHandle<()>>,
}
impl GrpcCoordinatorService {
    pub async fn start(
        bind: SocketAddr,
        catalog: StaticCatalog,
        deadline: Duration,
    ) -> Result<Self, RemoteError> {
        catalog.validate()?;
        let end = checked_deadline(deadline, None)?;
        let deadline = deadline.min(Duration::from_millis(MAX_TIMEOUT));
        let mut clients = BTreeMap::new();
        for route in &catalog.partitions {
            if let std::collections::btree_map::Entry::Vacant(entry) = clients.entry(route.shard) {
                entry.insert(
                    QueryClient::connect(
                        route.endpoint,
                        route.identity,
                        end.saturating_duration_since(Instant::now()),
                    )
                    .await?,
                );
            }
        }
        let clients = Arc::new(clients);
        let jobs = Arc::new(jobs::Jobs::with_grpc_clients(Some(clients.clone())));
        let handler = CoordinatorHandler {
            catalog: Arc::new(catalog),
            clients,
            jobs: jobs.clone(),
            deadline,
        };
        let listener = TcpListener::bind(bind)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let address = listener
            .local_addr()
            .map_err(|_| RemoteError::Unavailable)?;
        let (stop, mut stopped) = watch::channel(false);
        let hard_stop = CancellationToken::new();
        let connection_stop = hard_stop.clone();
        let task = tokio::spawn(async move {
            let cleanup = jobs.clone();
            let shutdown = async move {
                let mut interval = tokio::time::interval(Duration::from_millis(100));
                loop {
                    tokio::select! {
                        _ = stopped.changed() => break,
                        _ = interval.tick() => cleanup.expire(),
                    }
                }
            };
            let connections = Arc::new(Semaphore::new(MAX_IN_FLIGHT));
            let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener).filter_map(
                move |accepted| match accepted {
                    Err(error) => Some(Err(error)),
                    Ok(socket) => connections.clone().try_acquire_owned().ok().map(|permit| {
                        // Custom incoming bypasses Tonic's TCP_NODELAY default.
                        socket.set_nodelay(true)?;
                        Ok(LimitedIo {
                            socket,
                            _permit: permit,
                            cancelled: Box::pin(connection_stop.clone().cancelled_owned()),
                        })
                    }),
                },
            );
            let service = CoordinatorServiceServer::new(handler)
                .max_decoding_message_size(INGRESS_REQUEST_LIMIT)
                .max_encoding_message_size(MAX_WIRE);
            let _ = tonic::transport::Server::builder()
                .timeout(deadline)
                .max_concurrent_streams(Some(2))
                .add_service(AdmittedService {
                    inner: service,
                    admission: Arc::new(Semaphore::new(2)),
                    deadline,
                })
                .serve_with_incoming_shutdown(incoming, shutdown)
                .await;
            jobs.clear();
        });
        Ok(Self {
            address,
            stop,
            hard_stop,
            task: Some(task),
        })
    }
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub async fn shutdown(mut self) {
        // Close sockets so flow-controlled or idle peers cannot delay cleanup.
        self.hard_stop.cancel();
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for GrpcCoordinatorService {
    fn drop(&mut self) {
        self.hard_stop.cancel();
        let _ = self.stop.send(true);
    }
}

/// Reusable multiplexed channel; no retry, forwarding or legacy fallback.
#[derive(Clone)]
pub struct CoordinatorClient {
    client: CoordinatorServiceClient<Channel>,
}
impl CoordinatorClient {
    pub async fn connect(address: SocketAddr, deadline: Duration) -> Result<Self, RemoteError> {
        let end = checked_deadline(deadline, None)?;
        let endpoint = Channel::from_shared(format!("http://{address}"))
            .map_err(|_| RemoteError::InvalidArgument)?
            .connect_timeout(deadline)
            .buffer_size(MAX_IN_FLIGHT)
            .concurrency_limit(MAX_IN_FLIGHT);
        let channel = tokio::time::timeout_at(end, endpoint.connect())
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?
            .map_err(|_| RemoteError::Unavailable)?;
        Ok(Self {
            client: CoordinatorServiceClient::new(channel)
                .max_decoding_message_size(MAX_WIRE)
                .max_encoding_message_size(INGRESS_REQUEST_LIMIT),
        })
    }
    pub async fn query(
        &self,
        request: DistributedRequest,
        deadline: Duration,
    ) -> Result<DistributedResponse, RemoteError> {
        validate_request(&request)?;
        let end = checked_deadline(
            deadline.min(Duration::from_millis(request.timeout_millis)),
            None,
        )?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let mut request = request;
        request.timeout_millis = remaining(end)?.as_millis() as u64;
        let message = codec::encode_query_request(request.clone());
        let response = tokio::time::timeout_at(
            end,
            self.client
                .clone()
                .query(QueryClient::request(message, end)),
        )
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?
        .map_err(remote_status)?;
        let output = codec::decode_query_response(response.into_inner())?;
        account_output(&output, end).map_err(remote_status)?;
        if let Ok(value) = &output {
            validate_output(value, &request)?;
        }
        remaining(end)?;
        Ok(output)
    }
    pub async fn job(
        &self,
        request: jobs::Request,
        deadline: Duration,
    ) -> Result<jobs::Response, RemoteError> {
        if matches!(&request, jobs::Request::Start { text, .. } | jobs::Request::StartRows { text, .. }
            if text.len() > query::MAX_QUERY_TEXT_BYTES)
        {
            return Err(RemoteError::ResourceExhausted);
        }
        let end = checked_deadline(deadline.min(Duration::from_millis(MAX_TIMEOUT)), None)?;
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let message = codec::encode_job_request(request.clone(), remaining(end)?);
        let response = tokio::time::timeout_at(
            end,
            self.client.clone().job(QueryClient::request(message, end)),
        )
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?
        .map_err(remote_status)?;
        let output = codec::decode_job_response(response.into_inner())?;
        account_output(&output, end).map_err(remote_status)?;
        if let Ok(value) = &output {
            jobs::validate_output(value, &request)?;
        }
        remaining(end)?;
        Ok(output)
    }
}
fn remaining(end: Instant) -> Result<Duration, RemoteError> {
    let remaining = end.saturating_duration_since(Instant::now());
    if remaining.as_millis() == 0 {
        Err(RemoteError::Timeout { request_id: None })
    } else {
        Ok(remaining)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_protobuf_does_not_expand_observed_output_budget() {
        let end = Instant::now() + Duration::from_secs(5);
        let mut output: DistributedResponse = Ok(DistributedOutput {
            completeness: QueryCompleteness::Unverified,
            snapshots: vec![QueryStamp {
                identity: RemoteIdentity { group: 1, node: 1 },
                shard: 1,
                epoch: 1,
                sequence: 1,
            }],
            rows: Vec::new(),
        });
        for count in [1, 100, 8000] {
            output.as_mut().unwrap().rows = (0..count)
                .map(|id| QueryRow::Value {
                    source: Some(QualifiedElement::Vertex { partition: 1, id }),
                    value: Value::Text("\u{0000}".repeat(50)),
                })
                .collect();
            let old = bounded_encode(&output, end);
            let accounted = bounded_encoded_size(&output, end);
            assert_eq!(old.as_ref().map(Vec::len).map_err(Clone::clone), accounted);
            let encoded = codec::encode_query_response(output.clone());
            assert!(encoded.encoded_len() < MAX_WIRE);
            if count == 8000 {
                assert_eq!(old.unwrap_err(), QueryFailure::ResourceExhausted);
                assert_eq!(
                    account_output(&output, end).unwrap_err().code(),
                    tonic::Code::ResourceExhausted
                );
            } else {
                account_output(&output, end).unwrap();
            }
        }
    }
}
