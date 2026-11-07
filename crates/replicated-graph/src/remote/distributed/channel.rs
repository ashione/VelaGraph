//! Opt-in, sequential hierarchy channels. No retransmission or snapshot reopen.
use super::*;

pub(super) const TRANSPORT_BYTES: usize = 512;
const PROTOCOL: u32 = 1;
const MAX_REQUESTS: u64 = 1024;
const MAX_AGE: Duration = Duration::from_secs(60);
const SERVER_IDLE: Duration = Duration::from_secs(5);
const CLIENT_IDLE: Duration = Duration::from_secs(4);
const FRAME_LIMIT: usize = query::MAX_QUERY_RESPONSE_BYTES + 1024;
static SOCKETS: Semaphore = Semaphore::const_new(8);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Frame {
    version: u32,
    sequence: u64,
    timeout_millis: u64,
    request: FragmentRequest,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Answer {
    version: u32,
    sequence: u64,
    retire: bool,
    output: Result<FragmentOutput, QueryFailure>,
}

pub(super) struct Connection {
    stream: TcpStream,
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    descriptor: Descriptor,
    sequence: u64,
    expires: Instant,
    idle: Instant,
    _permit: tokio::sync::SemaphorePermit<'static>,
}
impl Connection {
    pub(super) fn live(&self) -> bool {
        Instant::now() < self.idle.min(self.expires) && self.sequence < MAX_REQUESTS
    }
    fn reusable(
        &self,
        endpoint: SocketAddr,
        identity: RemoteIdentity,
        descriptor: &Descriptor,
        end: Instant,
    ) -> bool {
        self.live()
            && self.endpoint == endpoint
            && self.identity == identity
            && &self.descriptor == descriptor
            && end <= self.expires
    }
}

pub(super) async fn call(
    endpoint: SocketAddr,
    identity: RemoteIdentity,
    request: FragmentRequest,
    end: Instant,
    previous: Option<Connection>,
) -> (Result<Reply, RemoteError>, Option<Connection>) {
    let previous = previous.filter(|c| c.reusable(endpoint, identity, &request.descriptor, end));
    let fresh_permit = if previous.is_none() {
        match SOCKETS.try_acquire() {
            Ok(permit) => Some(permit),
            // This is a transport selection before any attempt, not a retry.
            Err(_) => {
                return (
                    rpc(
                        endpoint,
                        Some(identity),
                        Operation::QueryFragment(request),
                        end.saturating_duration_since(Instant::now()),
                        None,
                    )
                    .await,
                    None,
                )
            }
        }
    } else {
        None
    };
    let Ok(_active) = CLIENT_ADMISSION.try_acquire() else {
        return (Err(RemoteError::ResourceExhausted), None);
    };
    let result = tokio::time::timeout_at(end, async move {
        let descriptor = request.descriptor.clone();
        let mut connection = if let Some(connection) = previous {
            connection
        } else {
            let expires = Instant::now() + MAX_AGE;
            let stream = TcpStream::connect(endpoint)
                .await
                .map_err(|_| RemoteError::Unavailable)?;
            stream
                .set_nodelay(true)
                .map_err(|_| RemoteError::Unavailable)?;
            Connection {
                stream,
                endpoint,
                identity,
                descriptor: descriptor.clone(),
                sequence: 0,
                expires,
                idle: Instant::now() + CLIENT_IDLE,
                _permit: fresh_permit.unwrap(),
            }
        };
        let sequence = connection.sequence;
        if sequence == 0 {
            write_frame(
                &mut connection.stream,
                &super::super::Request {
                    version: VERSION,
                    identity: Some(identity),
                    operation: Operation::OpenHierarchyChannel {
                        version: PROTOCOL,
                        request,
                    },
                },
                end,
            )
            .await?;
        } else {
            let remaining = end.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(RemoteError::Timeout { request_id: None });
            }
            let timeout_millis = remaining
                .as_millis()
                .saturating_add(u128::from(
                    !remaining.subsec_nanos().is_multiple_of(1_000_000),
                ))
                .min(MAX_TIMEOUT as u128) as u64;
            write_frame(
                &mut connection.stream,
                &Frame {
                    version: PROTOCOL,
                    sequence,
                    timeout_millis,
                    request,
                },
                end,
            )
            .await?;
        }
        let Answer {
            version,
            sequence: echoed,
            retire,
            output,
        } = read_frame_limited(&mut connection.stream, end, FRAME_LIMIT).await?;
        if version != PROTOCOL
            || echoed != sequence
            || output
                .as_ref()
                .is_ok_and(|output| output.descriptor != descriptor)
        {
            return Err(RemoteError::Protocol);
        }
        connection.sequence += 1;
        connection.idle = Instant::now() + CLIENT_IDLE;
        let keep = output.is_ok() && !retire && connection.live();
        Ok((Reply::QueryFragment(output), keep.then_some(connection)))
    })
    .await;
    match result {
        Ok(Ok((reply, connection))) => (Ok(reply), connection),
        Ok(Err(error)) => (Err(error), None),
        Err(_) => (Err(RemoteError::Timeout { request_id: None }), None),
    }
}

// Idle waiting is separate from execution. The first received byte starts one
// absolute frame/execute/write deadline; slow bodies cannot obtain a fresh one.
async fn next_frame(
    stream: &mut TcpStream,
    life: Instant,
    operation_limit: Duration,
) -> Result<(Frame, Instant), RemoteError> {
    let mut header = [0u8; 4];
    tokio::time::timeout_at(
        life.min(Instant::now() + SERVER_IDLE),
        stream.read_exact(&mut header[..1]),
    )
    .await
    .map_err(|_| RemoteError::Timeout { request_id: None })?
    .map_err(|_| RemoteError::Unavailable)?;
    let started = Instant::now();
    let end = life.min(started + operation_limit);
    let frame = tokio::time::timeout_at(end, async {
        stream
            .read_exact(&mut header[1..])
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let size = u32::from_be_bytes(header) as usize;
        if size == 0 || size > FRAME_LIMIT {
            return Err(RemoteError::ResourceExhausted);
        }
        let mut bytes = vec![0; size];
        stream
            .read_exact(&mut bytes)
            .await
            .map_err(|_| RemoteError::Unavailable)?;
        let frame: Frame = decode_frame_json(&bytes).map_err(|_| RemoteError::Protocol)?;
        if Instant::now() >= end {
            return Err(RemoteError::Timeout { request_id: None });
        }
        Ok(frame)
    })
    .await
    .map_err(|_| RemoteError::Timeout { request_id: None })??;
    if !(1..=MAX_TIMEOUT).contains(&frame.timeout_millis) {
        return Err(RemoteError::InvalidArgument);
    }
    let end = end.min(started + Duration::from_millis(frame.timeout_millis));
    Ok((frame, end))
}

pub(in crate::remote) async fn serve(
    mut stream: TcpStream,
    state: Arc<StateMachine>,
    sessions: Arc<Sessions>,
    peer: (RemoteIdentity, u64),
    version: u32,
    first: FragmentRequest,
    deadlines: (Instant, Duration),
) -> Result<(), RemoteError> {
    let (identity, shard) = peer;
    let (first_end, operation_limit) = deadlines;
    if version != PROTOCOL || !matches!(first.fragment, Fragment::HierarchyBatch { .. }) {
        return Err(RemoteError::InvalidArgument);
    }
    // first_end was fixed at accept, before decoding the initial request.
    let accepted = first_end - operation_limit;
    let operation_limit = operation_limit.min(Duration::from_millis(MAX_TIMEOUT));
    let descriptor = first.descriptor.clone();
    let lease = sessions.hierarchy_deadline(identity, shard, &descriptor)?;
    let life = lease.min(accepted + MAX_AGE);
    stream
        .set_nodelay(true)
        .map_err(|_| RemoteError::Unavailable)?;
    let mut sequence = 0;
    let mut request = first;
    let mut end = first_end.min(life).min(Instant::now() + operation_limit);
    loop {
        if Instant::now() >= end
            || request.descriptor != descriptor
            || !matches!(request.fragment, Fragment::HierarchyBatch { .. })
        {
            return Err(RemoteError::InvalidArgument);
        }
        let result = tokio::time::timeout_at(
            end,
            sessions.fragment(&state, identity, shard, request, end),
        )
        .await
        .map_err(|_| RemoteError::Timeout { request_id: None })?;
        let output = match result {
            Ok(Reply::QueryFragment(output)) => output,
            Ok(_) => Err(QueryFailure::IncompleteOrCorrupt),
            Err(error) => Err(remote_failure(error)),
        };
        let retire = output.is_err()
            || sequence + 1 >= MAX_REQUESTS
            || Instant::now() + operation_limit >= life;
        let response = Answer {
            version: PROTOCOL,
            sequence,
            retire,
            output,
        };
        tokio::time::timeout_at(end, write_frame(&mut stream, &response, end))
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })??;
        if retire {
            return Ok(());
        }
        sequence += 1;
        let (frame, next_end) = next_frame(&mut stream, life, operation_limit).await?;
        if frame.version != PROTOCOL || frame.sequence != sequence {
            return Err(RemoteError::Protocol);
        }
        request = frame.request;
        end = next_end;
    }
}
#[cfg(test)]
mod channel_tests {
    use super::*;
    use tokio::net::TcpListener;
    use tokio::sync::{oneshot, Mutex};

    // These tests share the production socket admission semaphore.
    static SERIAL: Mutex<()> = Mutex::const_new(());

    fn identity() -> RemoteIdentity {
        RemoteIdentity {
            group: u64::MAX,
            node: u64::MAX,
        }
    }
    fn descriptor() -> Descriptor {
        Descriptor {
            stamp: QueryStamp {
                identity: identity(),
                shard: u64::MAX,
                epoch: u64::MAX,
                sequence: u64::MAX,
            },
            incarnation: [255; 16],
            token: [255; 16],
        }
    }
    fn request() -> FragmentRequest {
        FragmentRequest {
            descriptor: descriptor(),
            fragment: Fragment::HierarchyBatch {
                partition: u32::MAX,
                fragments: vec![Fragment::Exists(VertexId {
                    partition: u32::MAX,
                    id: u64::MAX,
                })],
            },
            remaining: CEILING,
        }
    }
    fn answer(sequence: u64, retire: bool) -> Answer {
        Answer {
            version: PROTOCOL,
            sequence,
            retire,
            output: Ok(FragmentOutput {
                descriptor: descriptor(),
                cost: Cost::default(),
                value: FragmentValue::HierarchyBatch(vec![FragmentValue::Bool(true)]),
            }),
        }
    }
    fn end() -> Instant {
        Instant::now() + Duration::from_secs(2)
    }
    async fn listener() -> TcpListener {
        TcpListener::bind("127.0.0.1:0").await.unwrap()
    }
    async fn first(stream: &mut TcpStream) {
        let message: super::super::super::Request = read_frame(stream, end()).await.unwrap();
        assert_eq!(message.version, VERSION);
        assert_eq!(message.identity, Some(identity()));
        match message.operation {
            Operation::OpenHierarchyChannel { version, request } => {
                assert_eq!(version, PROTOCOL);
                assert_eq!(request.descriptor, descriptor());
                assert!(matches!(request.fragment, Fragment::HierarchyBatch { .. }));
            }
            _ => panic!("must perform the initial fragment in the open frame"),
        }
    }
    async fn eof(stream: &mut TcpStream) {
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), stream.read(&mut byte))
                .await
                .expect("peer did not close")
                .unwrap(),
            0
        );
    }
    async fn no_reconnect(listener: &TcpListener) {
        assert!(
            tokio::time::timeout(Duration::from_millis(75), listener.accept())
                .await
                .is_err(),
            "a dispatched operation must never reconnect or fall back to RPC"
        );
    }
    fn assert_success(reply: Result<Reply, RemoteError>) {
        match reply.unwrap() {
            Reply::QueryFragment(Ok(output)) => {
                assert_eq!(output.descriptor, descriptor());
                assert!(matches!(output.value, FragmentValue::HierarchyBatch(_)));
            }
            _ => panic!("expected a successful hierarchy fragment"),
        }
    }

    #[tokio::test]
    async fn two_fragments_reuse_one_socket_and_echo_sequence() {
        let _serial = SERIAL.lock().await;
        let listener = listener().await;
        let endpoint = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            first(&mut stream).await;
            write_frame(&mut stream, &answer(0, false), end())
                .await
                .unwrap();
            let frame: Frame = read_frame(&mut stream, end()).await.unwrap();
            assert_eq!(frame.version, PROTOCOL);
            assert_eq!(frame.sequence, 1);
            assert_eq!(frame.request.descriptor, descriptor());
            assert!((1..=MAX_TIMEOUT).contains(&frame.timeout_millis));
            write_frame(&mut stream, &answer(1, false), end())
                .await
                .unwrap();
            eof(&mut stream).await;
            no_reconnect(&listener).await;
        });
        let (reply, connection) = call(endpoint, identity(), request(), end(), None).await;
        assert_success(reply);
        assert_eq!(connection.as_ref().unwrap().sequence, 1);
        {
            let connection = connection.as_ref().unwrap();
            assert!(connection.live());
            assert!(connection.idle <= Instant::now() + CLIENT_IDLE);
            assert!(connection.expires <= Instant::now() + MAX_AGE);
            assert!(connection.reusable(endpoint, identity(), &descriptor(), end()));
            let mut wrong = descriptor();
            wrong.token[0] ^= 1;
            assert!(!connection.reusable(endpoint, identity(), &wrong, end()));
            assert!(!connection.reusable(
                endpoint,
                RemoteIdentity { group: 0, node: 0 },
                &descriptor(),
                end()
            ));
            assert!(!connection.reusable(
                endpoint,
                identity(),
                &descriptor(),
                connection.expires + Duration::from_nanos(1)
            ));
        }
        let (reply, connection) = call(endpoint, identity(), request(), end(), connection).await;
        assert_success(reply);
        assert_eq!(connection.as_ref().unwrap().sequence, 2);
        let mut connection = connection.unwrap();
        connection.sequence = MAX_REQUESTS;
        assert!(!connection.live());
        connection.sequence = 2;
        connection.idle = Instant::now();
        assert!(!connection.live());
        connection.idle = Instant::now() + CLIENT_IDLE;
        connection.expires = Instant::now();
        assert!(!connection.live());
        drop(connection);
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn explicit_retirement_reconnects_only_for_the_next_fragment() {
        let _serial = SERIAL.lock().await;
        let listener = listener().await;
        let endpoint = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                first(&mut stream).await; // Same Descriptor, no new snapshot/open-session RPC.
                write_frame(&mut stream, &answer(0, true), end())
                    .await
                    .unwrap();
                eof(&mut stream).await;
            }
            no_reconnect(&listener).await;
        });
        for _ in 0..2 {
            let (reply, connection) = call(endpoint, identity(), request(), end(), None).await;
            assert_success(reply);
            assert!(connection.is_none());
        }
        peer.await.unwrap();
    }

    #[derive(Clone, Copy, Debug)]
    enum Broken {
        Sequence,
        Version,
        Descriptor,
        Oversized,
        Truncated,
        LostAck,
        UnknownField,
    }
    async fn broken_reply(kind: Broken, stream: &mut TcpStream) {
        match kind {
            Broken::Sequence | Broken::Version | Broken::Descriptor => {
                let mut reply = answer(0, false);
                match kind {
                    Broken::Sequence => reply.sequence = 1,
                    Broken::Version => reply.version = PROTOCOL + 1,
                    Broken::Descriptor => reply.output.as_mut().unwrap().descriptor.token[0] ^= 1,
                    _ => unreachable!(),
                }
                write_frame(stream, &reply, end()).await.unwrap();
            }
            Broken::Oversized => {
                stream
                    .write_all(&((FRAME_LIMIT + 1) as u32).to_be_bytes())
                    .await
                    .unwrap();
            }
            Broken::Truncated => {
                stream.write_all(&100u32.to_be_bytes()).await.unwrap();
                stream.write_all(b"{").await.unwrap();
                stream.shutdown().await.unwrap();
            }
            Broken::LostAck => {
                stream.shutdown().await.unwrap();
            }
            Broken::UnknownField => {
                let mut reply = serde_json::to_value(answer(0, false)).unwrap();
                reply
                    .as_object_mut()
                    .unwrap()
                    .insert("unexpected".into(), true.into());
                write_frame(stream, &reply, end()).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn bad_initial_ack_fails_closed_without_retry() {
        let _serial = SERIAL.lock().await;
        for kind in [
            Broken::Sequence,
            Broken::Version,
            Broken::Descriptor,
            Broken::Oversized,
            Broken::Truncated,
            Broken::LostAck,
            Broken::UnknownField,
        ] {
            let listener = listener().await;
            let endpoint = listener.local_addr().unwrap();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                first(&mut stream).await;
                broken_reply(kind, &mut stream).await;
                eof(&mut stream).await;
                no_reconnect(&listener).await;
            });
            let (reply, connection) = call(endpoint, identity(), request(), end(), None).await;
            assert!(connection.is_none(), "{kind:?}");
            match kind {
                Broken::Sequence | Broken::Version | Broken::Descriptor | Broken::UnknownField => {
                    assert!(matches!(reply, Err(RemoteError::Protocol)), "{kind:?}")
                }
                Broken::Oversized => assert!(matches!(reply, Err(RemoteError::ResourceExhausted))),
                Broken::Truncated | Broken::LostAck => {
                    assert!(matches!(reply, Err(RemoteError::Unavailable)))
                }
            }
            peer.await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancelling_pending_reused_call_closes_socket_and_returns_its_permit() {
        let _serial = SERIAL.lock().await;
        let listener = listener().await;
        let endpoint = listener.local_addr().unwrap();
        let (received, wait_received) = oneshot::channel();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            first(&mut stream).await;
            write_frame(&mut stream, &answer(0, false), end())
                .await
                .unwrap();
            let frame: Frame = read_frame(&mut stream, end()).await.unwrap();
            assert_eq!(frame.sequence, 1);
            received.send(()).unwrap();
            eof(&mut stream).await;
            no_reconnect(&listener).await;
        });
        let (reply, connection) = call(endpoint, identity(), request(), end(), None).await;
        assert_success(reply);
        assert!(connection.is_some());
        let with_socket = SOCKETS.available_permits();
        let operation = tokio::spawn(call(endpoint, identity(), request(), end(), connection));
        wait_received.await.unwrap();
        operation.abort();
        match operation.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("cancelled operation unexpectedly completed"),
        }
        assert_eq!(SOCKETS.available_permits(), with_socket + 1);
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn query_failure_is_not_retried_or_cached() {
        let _serial = SERIAL.lock().await;
        let listener = listener().await;
        let endpoint = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            first(&mut stream).await;
            let mut response = answer(0, false);
            response.output = Err(QueryFailure::IncompleteOrCorrupt);
            write_frame(&mut stream, &response, end()).await.unwrap();
            eof(&mut stream).await;
            no_reconnect(&listener).await;
        });
        let (reply, connection) = call(endpoint, identity(), request(), end(), None).await;
        assert!(matches!(
            reply,
            Ok(Reply::QueryFragment(Err(QueryFailure::IncompleteOrCorrupt)))
        ));
        assert!(connection.is_none());
        peer.await.unwrap();
    }

    #[test]
    fn actual_wire_wrappers_fit_fixed_transport_charge_with_two_copies() {
        assert_eq!(TRANSPORT_BYTES, 512);
        let request = request();
        let payload = serde_json::to_vec(&request).unwrap().len();
        let first = super::super::super::Request {
            version: VERSION,
            identity: Some(identity()),
            operation: Operation::OpenHierarchyChannel {
                version: PROTOCOL,
                request: request.clone(),
            },
        };
        let later = Frame {
            version: PROTOCOL,
            sequence: u64::MAX,
            timeout_millis: u64::MAX,
            request,
        };
        let request_overheads = [
            serde_json::to_vec(&first).unwrap().len() - payload + 4,
            serde_json::to_vec(&later).unwrap().len() - payload + 4,
        ];
        for sequence in [0, MAX_REQUESTS - 1, u64::MAX] {
            // false is the longer boolean encoding, and all identity/stamp integers
            // and descriptor byte values in the payload are their maximum widths.
            let response = answer(sequence, false);
            let output_bytes = serde_json::to_vec(response.output.as_ref().unwrap())
                .unwrap()
                .len();
            let success_overhead = serde_json::to_vec(&response).unwrap().len() - output_bytes + 4;
            let failures = [
                QueryFailure::Parse,
                QueryFailure::Unsupported,
                QueryFailure::InvalidArgument,
                QueryFailure::ResourceExhausted,
                QueryFailure::Timeout,
                QueryFailure::UnsupportedScope,
                QueryFailure::IncompleteOrCorrupt,
                QueryFailure::Unavailable,
            ];
            let mut reply_overheads = vec![success_overhead];
            for failure in failures {
                // Error replies have no separately charged FragmentOutput: count
                // the ENTIRE compact answer, including the typed failure payload.
                let error = Answer {
                    version: PROTOCOL,
                    sequence,
                    retire: false,
                    output: Err(failure),
                };
                reply_overheads.push(serde_json::to_vec(&error).unwrap().len() + 4);
            }
            for request_overhead in request_overheads {
                for reply_overhead in &reply_overheads {
                    let total = 2 * (request_overhead + reply_overhead);
                    assert!(total <= TRANSPORT_BYTES,
                        "request wrapper {request_overhead}, reply wrapper {reply_overhead}, two copies {total}");
                }
            }
        }
    }

    #[tokio::test]
    async fn next_frame_body_cannot_extend_deadline_started_by_first_byte() {
        let listener = listener().await;
        let endpoint = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(endpoint).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let frame = Frame {
            version: PROTOCOL,
            sequence: 1,
            timeout_millis: MAX_TIMEOUT,
            request: request(),
        };
        let bytes = serde_json::to_vec(&frame).unwrap();
        let header = (bytes.len() as u32).to_be_bytes();
        let (started, ready) = oneshot::channel();
        let reader = tokio::spawn(async move {
            started.send(()).unwrap();
            next_frame(
                &mut server,
                Instant::now() + Duration::from_secs(3),
                Duration::from_millis(500),
            )
            .await
        });
        ready.await.unwrap();
        client.write_all(&header[..1]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        client.write_all(&header[1..]).await.unwrap();
        // A broken implementation restarting the 500ms timer after the header
        // would still be pending at this 650ms outer limit.
        let result = tokio::time::timeout(Duration::from_millis(350), reader)
            .await
            .expect("body incorrectly received a fresh operation deadline")
            .unwrap();
        assert!(matches!(result, Err(RemoteError::Timeout { .. })));
    }
}
