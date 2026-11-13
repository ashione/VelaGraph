//! Bounded generic metadata RPC session. No epoch cache or automatic replay.
use super::{
    operations_grpc::{codec, wire},
    RemoteError, RemoteIdentity, CLIENT_ADMISSION, MAX_DEADLINE, MAX_IN_FLIGHT, VERSION,
};
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Channel, Streaming};

type Metadata = (u64, u64, u64);
type Result<T> = std::result::Result<T, RemoteError>;
#[derive(Debug)]
struct Command {
    shard: u64,
    end: Instant,
    reply: oneshot::Sender<Result<Metadata>>,
}
struct Pending {
    shard: u64,
    end: Instant,
    reply: oneshot::Sender<Result<Metadata>>,
}
struct Owner {
    channel: Channel,
    identity: RemoteIdentity,
    session: Mutex<Option<mpsc::Sender<Command>>>,
}
/// Clones share one lazily established stream, never cached metadata.
#[derive(Clone)]
pub struct Client {
    owner: Arc<Owner>,
}
impl Client {
    /// Does not establish a connection or issue RPCs. Tonic owns the lazy
    /// Channel worker; the Metadata session actor starts on the first call.
    pub fn new(endpoint: std::net::SocketAddr, identity: RemoteIdentity) -> Self {
        let channel = Channel::from_shared(format!("http://{endpoint}"))
            .expect("socket address is a valid URI")
            .connect_lazy();
        Self {
            owner: Arc::new(Owner {
                channel,
                identity,
                session: Mutex::new(None),
            }),
        }
    }
    pub async fn metadata(&self, shard: u64, end: Instant) -> Result<Metadata> {
        let now = Instant::now();
        if end <= now {
            return Err(timeout());
        }
        if end - now > MAX_DEADLINE {
            return Err(RemoteError::InvalidArgument);
        }
        let _permit = CLIENT_ADMISSION
            .try_acquire()
            .map_err(|_| RemoteError::ResourceExhausted)?;
        let tx = {
            let mut session = self
                .owner
                .session
                .lock()
                .map_err(|_| RemoteError::Unavailable)?;
            if session.as_ref().is_none_or(|s| s.is_closed()) {
                let (tx, rx) = mpsc::channel(MAX_IN_FLIGHT);
                tokio::spawn(actor(self.owner.channel.clone(), self.owner.identity, rx));
                *session = Some(tx);
            }
            session.as_ref().unwrap().clone()
        };
        let (reply, receive) = oneshot::channel();
        tx.try_send(Command { shard, end, reply })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => RemoteError::ResourceExhausted,
                mpsc::error::TrySendError::Closed(_) => RemoteError::Unavailable,
            })?;
        let value = tokio::time::timeout_at(end, receive)
            .await
            .map_err(|_| timeout())?
            .map_err(|_| RemoteError::Unavailable)??;
        if Instant::now() >= end {
            return Err(timeout());
        }
        Ok(value)
    }
}
fn timeout() -> RemoteError {
    RemoteError::Timeout { request_id: None }
}
type Connecting = Pin<
    Box<
        dyn Future<
                Output = std::result::Result<
                    tonic::Response<Streaming<wire::MetadataResponse>>,
                    tonic::Status,
                >,
            > + Send,
    >,
>;
async fn actor(channel: Channel, identity: RemoteIdentity, mut commands: mpsc::Receiver<Command>) {
    // Consume and validate the first command before creating any stream. An
    // already-ended initial caller must not leave an empty handshake cached.
    let Some(first) = commands.recv().await else {
        return;
    };
    let millis = first
        .end
        .saturating_duration_since(Instant::now())
        .as_millis() as u64;
    if first.reply.is_closed() || millis == 0 {
        commands.close();
        let _ = first.reply.send(Err(timeout()));
        return;
    }
    let (outbound, receiver) = mpsc::channel(MAX_IN_FLIGHT);
    let initial = wire::MetadataRequest {
        version: VERSION,
        id: 1,
        identity: Some(wire::Identity {
            group: identity.group,
            node: identity.node,
        }),
        shard: first.shard,
        timeout_millis: millis,
    };
    // Fresh bounded channel is empty; failure still closes rather than replaying.
    if outbound.try_send(initial).is_err() {
        commands.close();
        let _ = first.reply.send(Err(RemoteError::Unavailable));
        return;
    }

    let mut connecting: Option<Connecting> = Some(Box::pin(async move {
        wire::metadata_service_client::MetadataServiceClient::new(channel)
            .max_encoding_message_size(super::operations_grpc::bounded::METADATA_MESSAGE_BYTES)
            .max_decoding_message_size(super::operations_grpc::bounded::METADATA_MESSAGE_BYTES)
            .exchange(ReceiverStream::new(receiver))
            .await
    }));
    let mut incoming: Option<Streaming<wire::MetadataResponse>> = None;
    let mut pending = BTreeMap::from([(
        1,
        Pending {
            shard: first.shard,
            end: first.end,
            reply: first.reply,
        },
    )]);
    let mut last_sent = 1u64;
    let failure = loop {
        let now = Instant::now();
        pending.retain(|_, p| {
            if p.reply.is_closed() {
                return false;
            }
            if p.end <= now {
                let (dummy, _) = oneshot::channel();
                let reply = std::mem::replace(&mut p.reply, dummy);
                let _ = reply.send(Err(timeout()));
                return false;
            }
            true
        });
        if commands.is_closed() || (last_sent > 0 && pending.is_empty() && connecting.is_some()) {
            break RemoteError::Unavailable;
        }
        let earliest = pending.values().map(|p| p.end).min();
        tokio::select! {
            command=commands.recv()=> {
                let Some(command)=command else {break RemoteError::Unavailable;};
                if command.reply.is_closed(){continue;}
                if command.end<=Instant::now(){let _=command.reply.send(Err(timeout()));continue;}
                if pending.len()>=MAX_IN_FLIGHT {let _=command.reply.send(Err(RemoteError::ResourceExhausted));continue;}
                let Some(id)=last_sent.checked_add(1) else {commands.close();let _=command.reply.send(Err(RemoteError::Protocol));break RemoteError::Protocol;};
                let millis=command.end.saturating_duration_since(Instant::now()).as_millis() as u64;
                if millis==0 {let _=command.reply.send(Err(timeout()));continue;}
                let message=wire::MetadataRequest{version:VERSION,id,identity:Some(wire::Identity{group:identity.group,node:identity.node}),shard:command.shard,timeout_millis:millis};
                if outbound.try_send(message).is_err(){commands.close();let _=command.reply.send(Err(RemoteError::Unavailable));break RemoteError::Unavailable;}
                last_sent=id;
                pending.insert(id,Pending{shard:command.shard,end:command.end,reply:command.reply});
            },
            result=async { match connecting.as_mut(){Some(f)=>f.await,None=>std::future::pending().await} }=>{
                connecting=None;
                match result {Ok(response)=>incoming=Some(response.into_inner()),Err(status)=>break super::paged::grpc::remote_status(status)}
            },
            result=async {match incoming.as_mut(){Some(stream)=>stream.message().await,None=>std::future::pending().await}}=>{
                let response=match result {Ok(Some(v))=>v,Ok(None)=>break RemoteError::Unavailable,Err(status)=>break super::paged::grpc::remote_status(status)};
                if response.version!=VERSION || response.id==0 || response.id>last_sent {break RemoteError::Protocol;}
                let Some(p)=pending.remove(&response.id) else {continue;}; // ended IDs need no tombstones
                if p.reply.is_closed(){continue;}
                if Instant::now()>=p.end {let _=p.reply.send(Err(timeout()));continue;}
                let value=match response.result {
                    Some(wire::metadata_response::Result::Metadata(m)) if m.shard==p.shard && m.epoch!=0=>Ok((m.shard,m.epoch,m.sequence)),
                    Some(wire::metadata_response::Result::Error(e))=>codec::unfailure(e).and_then(Err),
                    _=>Err(RemoteError::Protocol),
                };
                let protocol=matches!(value,Err(RemoteError::Protocol));
                if protocol {commands.close();}
                let _=p.reply.send(value);
                if protocol {break RemoteError::Protocol;}
            },
            _=async {match earliest {Some(end)=>tokio::time::sleep_until(end).await,None=>std::future::pending().await}}=>{},
        }
    };
    commands.close();
    for (_, p) in pending {
        let _ = p.reply.send(Err(failure.clone()));
    }
    // Closing the receiver fails any queued calls; none are replayed on a new stream.
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tonic::{Request, Response, Status};
    #[derive(Clone)]
    struct Mock {
        streams: Arc<AtomicUsize>,
        requests: Arc<AtomicUsize>,
        live: Arc<AtomicUsize>,
        behavior: u8,
    }
    struct Guard(Arc<AtomicUsize>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }
    #[tonic::async_trait]
    impl wire::metadata_service_server::MetadataService for Mock {
        type ExchangeStream = ReceiverStream<std::result::Result<wire::MetadataResponse, Status>>;
        async fn exchange(
            &self,
            request: Request<Streaming<wire::MetadataRequest>>,
        ) -> std::result::Result<Response<Self::ExchangeStream>, Status> {
            let number = self.streams.fetch_add(1, Ordering::SeqCst);
            if self.behavior == 4 {
                return Err(Status::resource_exhausted("stream capacity"));
            }
            self.live.fetch_add(1, Ordering::SeqCst);
            let guard = Guard(self.live.clone());
            let mut incoming = request.into_inner();
            let (tx, rx) = mpsc::channel(16);
            let mock = self.clone();
            tokio::spawn(async move {
                let _guard = guard;
                while let Ok(Some(v)) = incoming.message().await {
                    let sequence = mock.requests.fetch_add(1, Ordering::SeqCst) as u64;
                    if mock.behavior == 5 {
                        let _ = tx
                            .send(Err(Status::resource_exhausted("response capacity")))
                            .await;
                        break;
                    }
                    if mock.behavior == 6 {
                        continue;
                    }
                    if mock.behavior == 2 && number == 0 {
                        break;
                    }
                    let reply = wire::MetadataResponse {
                        version: 1,
                        id: if mock.behavior == 3 { v.id + 10 } else { v.id },
                        result: Some(wire::metadata_response::Result::Metadata(wire::Metadata {
                            shard: v.shard,
                            epoch: 1,
                            sequence,
                        })),
                    };
                    if mock.behavior == 1 && v.id == 1 {
                        let tx = tx.clone();
                        tokio::spawn(async move {
                            tokio::time::sleep(std::time::Duration::from_millis(80)).await;
                            let _ = tx.send(Ok(reply)).await;
                        });
                    } else if tx.send(Ok(reply)).await.is_err() {
                        break;
                    }
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        }
    }
    async fn fixture(
        behavior: u8,
    ) -> (
        Client,
        Mock,
        oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mock = Mock {
            streams: Arc::new(AtomicUsize::new(0)),
            requests: Arc::new(AtomicUsize::new(0)),
            live: Arc::new(AtomicUsize::new(0)),
            behavior,
        };
        let (stop, stopped) = oneshot::channel();
        let service = mock.clone();
        let task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(wire::metadata_service_server::MetadataServiceServer::new(
                    service,
                ))
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    async {
                        let _ = stopped.await;
                    },
                )
                .await
                .unwrap();
        });

        (
            Client::new(address, RemoteIdentity { group: 1, node: 1 }),
            mock,
            stop,
            task,
        )
    }
    async fn stopped(mock: &Mock) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while mock.live.load(Ordering::SeqCst) != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn actor_rejects_seventeenth_pending_without_unbounded_waiters() {
        use std::time::Duration;
        let (client, mock, stop, server) = fixture(6).await;
        let (tx, rx) = mpsc::channel(MAX_IN_FLIGHT);
        let task = tokio::spawn(actor(
            client.owner.channel.clone(),
            client.owner.identity,
            rx,
        ));
        let mut replies = Vec::new();
        for shard in 0..MAX_IN_FLIGHT {
            let (reply, receive) = oneshot::channel();
            tx.send(Command {
                shard: shard as u64,
                end: Instant::now() + Duration::from_secs(5),
                reply,
            })
            .await
            .unwrap();
            replies.push(receive);
        }
        tokio::time::timeout(Duration::from_secs(3), async {
            while mock.requests.load(Ordering::SeqCst) != MAX_IN_FLIGHT {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (reply, receive) = oneshot::channel();
        tx.send(Command {
            shard: 99,
            end: Instant::now() + Duration::from_secs(1),
            reply,
        })
        .await
        .unwrap();
        assert!(matches!(
            receive.await.unwrap(),
            Err(RemoteError::ResourceExhausted)
        ));
        assert_eq!(mock.requests.load(Ordering::SeqCst), MAX_IN_FLIGHT);
        drop(tx);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        for reply in replies {
            assert!(matches!(
                reply.await.unwrap(),
                Err(RemoteError::Unavailable)
            ));
        }
        drop(client);
        stopped(&mock).await;
        let _ = stop.send(());
        server.await.unwrap();
    }
    #[tokio::test]
    async fn expired_before_send_does_not_leave_initialization_cached() {
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        let (tx, rx) = mpsc::channel(MAX_IN_FLIGHT);
        let (reply, receive) = oneshot::channel();
        tx.try_send(Command {
            shard: 7,
            end: Instant::now(),
            reply,
        })
        .unwrap();
        let task = tokio::spawn(actor(channel, RemoteIdentity { group: 1, node: 1 }, rx));
        assert!(matches!(
            receive.await.unwrap(),
            Err(RemoteError::Timeout { .. })
        ));
        tokio::time::timeout(std::time::Duration::from_millis(100), task)
            .await
            .unwrap()
            .unwrap();
        assert!(tx.is_closed()); // tx is deliberately still alive
    }
    #[tokio::test]
    async fn stream_status_resource_exhausted_is_preserved() {
        for behavior in [4, 5] {
            let (client, mock, stop, task) = fixture(behavior).await;
            assert!(matches!(
                client
                    .metadata(7, Instant::now() + std::time::Duration::from_secs(1))
                    .await,
                Err(RemoteError::ResourceExhausted)
            ));
            drop(client);
            stopped(&mock).await;
            let _ = stop.send(());
            task.await.unwrap();
        }
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn bounded_session_matches_ids_cancels_late_reply_reconnects_without_replay_and_drops() {
        use std::time::Duration;
        let (client, mock, stop, task) = fixture(1).await;
        assert_eq!(mock.streams.load(Ordering::SeqCst), 0); // constructor has no IO
        let first = client.clone();
        let pending = tokio::spawn(async move {
            first
                .metadata(7, Instant::now() + Duration::from_secs(1))
                .await
        });
        tokio::time::timeout(Duration::from_secs(1), async {
            while mock.requests.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        pending.abort();
        let _ = pending.await;
        let mut calls = Vec::new();
        for shard in 8..12 {
            let c = client.clone();
            calls.push(tokio::spawn(async move {
                (
                    shard,
                    c.metadata(shard, Instant::now() + Duration::from_secs(1))
                        .await,
                )
            }));
        }
        for call in calls {
            let (shard, result) = call.await.unwrap();
            assert_eq!(result.unwrap().0, shard);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            client
                .metadata(12, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .0,
            12
        );
        assert_eq!(mock.streams.load(Ordering::SeqCst), 1); // late ended ID neither poisons nor replays
        drop(client);
        stopped(&mock).await;
        let _ = stop.send(());
        task.await.unwrap();

        let (client, mock, stop, task) = fixture(2).await;
        assert!(matches!(
            client
                .metadata(7, Instant::now() + Duration::from_secs(1))
                .await,
            Err(RemoteError::Unavailable)
        ));
        // The next explicit call may open a fresh stream; the failed one is never replayed.
        assert_eq!(
            client
                .metadata(8, Instant::now() + Duration::from_secs(1))
                .await
                .unwrap()
                .0,
            8
        );
        assert_eq!(mock.requests.load(Ordering::SeqCst), 2);
        assert_eq!(mock.streams.load(Ordering::SeqCst), 2);
        drop(client);
        stopped(&mock).await;
        let _ = stop.send(());
        task.await.unwrap();

        let (client, mock, stop, task) = fixture(1).await;
        assert!(matches!(
            client
                .metadata(7, Instant::now() + Duration::from_millis(20))
                .await,
            Err(RemoteError::Timeout { request_id: None })
        ));
        assert!(matches!(
            client.metadata(7, Instant::now()).await,
            Err(RemoteError::Timeout { .. })
        ));
        drop(client);
        stopped(&mock).await;
        let _ = stop.send(());
        task.await.unwrap();

        let (client, mock, stop, task) = fixture(3).await;
        assert!(matches!(
            client
                .metadata(7, Instant::now() + Duration::from_secs(1))
                .await,
            Err(RemoteError::Protocol)
        ));
        drop(client);
        stopped(&mock).await;
        let _ = stop.send(());
        task.await.unwrap();
    }
}
