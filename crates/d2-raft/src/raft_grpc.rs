//! Versioned typed Raft RPC. Snapshot chunks remain opaque state-machine bytes.
//! No plaintext JSON transport fallback; upgrade all voters while stopped.
use crate::{NodeId, TypeConfig};
use openraft::{
    error::{InstallSnapshotError, PayloadTooLarge, RPCError, RaftError, RemoteError, Unreachable},
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::*,
    BasicNode,
};
use prost::Message;
use std::{io, time::Duration};
use tokio::{
    net::TcpListener,
    sync::{watch, Semaphore},
    time::{timeout_at, Instant},
};
use tonic::{
    transport::{Channel, Endpoint},
    Request, Response, Status,
};
pub(crate) mod admission;
mod bounded;
mod codec;
pub mod wire {
    include!("raft_grpc/raft_proto.rs");
}
use wire::raft_service_client::RaftServiceClient;
use wire::raft_service_server::{RaftService, RaftServiceServer};
const VERSION: u32 = 1;
pub const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(3);
static OUTBOUND: Semaphore = Semaphore::const_new(16);
type Raft = openraft::Raft<TypeConfig>;
fn missing() -> Status {
    Status::invalid_argument("missing raft field")
}
fn unreachable<E: std::fmt::Display>(e: E) -> Unreachable {
    Unreachable::new(&io::Error::new(io::ErrorKind::NotConnected, e.to_string()))
}
#[derive(Clone, Copy, Default)]
pub struct GrpcNetworkFactory;
pub struct GrpcNetwork {
    target: NodeId,
    channel: Result<Channel, String>,
}
impl RaftNetworkFactory<TypeConfig> for GrpcNetworkFactory {
    type Network = GrpcNetwork;
    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> GrpcNetwork {
        let channel = Endpoint::from_shared(format!("http://{}", node.addr))
            .map(|e| {
                e.connect_timeout(DEADLINE)
                    .timeout(DEADLINE)
                    .buffer_size(1)
                    .connect_lazy()
            })
            .map_err(|e| e.to_string());
        GrpcNetwork { target, channel }
    }
}
impl GrpcNetwork {
    fn client(&self) -> Result<RaftServiceClient<Channel>, Status> {
        Ok(RaftServiceClient::new(
            self.channel
                .as_ref()
                .map_err(|_| Status::unavailable("invalid peer endpoint"))?
                .clone(),
        )
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES))
    }
    fn request<T: Message>(&self, v: T, deadline: Instant) -> Result<Request<T>, Status> {
        if v.encoded_len() > MAX_MESSAGE_BYTES {
            return Err(Status::out_of_range("raft payload too large"));
        }
        let mut r = Request::new(v);
        r.set_timeout(deadline.saturating_duration_since(Instant::now()));
        Ok(r)
    }
}
impl RaftNetwork<TypeConfig> for GrpcNetwork {
    async fn vote(
        &mut self,
        r: VoteRequest<NodeId>,
        o: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let end = Instant::now() + o.hard_ttl().min(DEADLINE);
        let work = async {
            let _permit = OUTBOUND
                .try_acquire()
                .map_err(|_| Status::resource_exhausted("outbound raft capacity"))?;
            let r = self.request(
                wire::VoteRequest {
                    version: VERSION,
                    target: self.target,
                    vote: Some(r.vote.into()),
                    last_log: r.last_log_id.map(Into::into),
                },
                end,
            )?;
            let r = self.client()?.vote(r).await?.into_inner();
            Ok::<_, Status>(VoteResponse {
                vote: r.vote.ok_or_else(missing)?.into(),
                vote_granted: r.granted,
                last_log_id: r.last_log.map(Into::into),
            })
        };
        timeout_at(end, work)
            .await
            .map_err(|e| RPCError::Unreachable(unreachable(e)))?
            .map_err(|e| RPCError::Unreachable(unreachable(e)))
    }
    async fn append_entries(
        &mut self,
        r: AppendEntriesRequest<TypeConfig>,
        o: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let end = Instant::now() + o.hard_ttl().min(DEADLINE);
        let hint = (r.entries.len() / 2).max(1) as u64;
        let work = async {
            let _permit = OUTBOUND
                .try_acquire()
                .map_err(|_| Status::resource_exhausted("outbound raft capacity"))?;
            let r = self.request(
                wire::AppendRequest {
                    version: VERSION,
                    target: self.target,
                    vote: Some(r.vote.into()),
                    previous: r.prev_log_id.map(Into::into),
                    entries: r.entries.into_iter().map(Into::into).collect(),
                    committed: r.leader_commit.map(Into::into),
                },
                end,
            )?;
            let r = self.client()?.append(r).await?.into_inner();
            use wire::append_response::Result as R;
            Ok::<_, Status>(match r.result.ok_or_else(missing)? {
                R::Success(_) => AppendEntriesResponse::Success,
                R::Conflict(_) => AppendEntriesResponse::Conflict,
                R::Partial(p) => AppendEntriesResponse::PartialSuccess(p.matching.map(Into::into)),
                R::HigherVote(v) => AppendEntriesResponse::HigherVote(v.into()),
            })
        };
        timeout_at(end, work)
            .await
            .map_err(|e| RPCError::Unreachable(unreachable(e)))?
            .map_err(|e| {
                if e.code() == tonic::Code::OutOfRange {
                    RPCError::PayloadTooLarge(PayloadTooLarge::new_entries_hint(hint))
                } else {
                    RPCError::Unreachable(unreachable(e))
                }
            })
    }
    async fn install_snapshot(
        &mut self,
        r: InstallSnapshotRequest<TypeConfig>,
        o: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let end = Instant::now() + o.hard_ttl().min(DEADLINE);
        let work = async {
            let _permit = OUTBOUND
                .try_acquire()
                .map_err(|_| Status::resource_exhausted("outbound raft capacity"))?;
            let r = self.request(
                wire::SnapshotRequest {
                    version: VERSION,
                    target: self.target,
                    vote: Some(r.vote.into()),
                    meta: Some(r.meta.into()),
                    offset: r.offset,
                    data: r.data,
                    done: r.done,
                },
                end,
            )?;
            self.client()?
                .install_snapshot(r)
                .await
                .map(|r| r.into_inner())
        };
        let r = timeout_at(end, work)
            .await
            .map_err(|e| RPCError::Unreachable(unreachable(e)))?
            .map_err(|e| RPCError::Unreachable(unreachable(e)))?;
        match r
            .result
            .ok_or_else(|| RPCError::Unreachable(unreachable("missing snapshot result")))?
        {
            wire::snapshot_response::Result::Vote(v) => {
                Ok(InstallSnapshotResponse { vote: v.into() })
            }
            wire::snapshot_response::Result::Mismatch(m) => {
                let expected = m.expected.ok_or_else(|| {
                    RPCError::Unreachable(unreachable("missing snapshot expected"))
                })?;
                let received = m.received.ok_or_else(|| {
                    RPCError::Unreachable(unreachable("missing snapshot received"))
                })?;
                Err(RPCError::RemoteError(RemoteError::new(
                    self.target,
                    RaftError::APIError(InstallSnapshotError::SnapshotMismatch(
                        openraft::error::SnapshotMismatch {
                            expect: (expected.id, expected.offset).into(),
                            got: (received.id, received.offset).into(),
                        },
                    )),
                )))
            }
        }
    }
}
#[derive(Clone)]
struct Handler {
    id: NodeId,
    raft: Raft,
}
impl Handler {
    fn check(&self, version: u32, target: u64) -> Result<(), Status> {
        if version != VERSION {
            return Err(Status::failed_precondition("raft protocol version"));
        }
        if target != self.id {
            return Err(Status::failed_precondition("raft target mismatch"));
        }
        Ok(())
    }
}
#[tonic::async_trait]
impl RaftService for Handler {
    async fn vote(
        &self,
        r: Request<wire::VoteRequest>,
    ) -> Result<Response<wire::VoteResponse>, Status> {
        let r = r.into_inner();
        self.check(r.version, r.target)?;
        let r = self
            .raft
            .vote(VoteRequest {
                vote: r.vote.ok_or_else(missing)?.into(),
                last_log_id: r.last_log.map(Into::into),
            })
            .await
            .map_err(|_| Status::unavailable("raft stopped"))?;
        Ok(Response::new(wire::VoteResponse {
            vote: Some(r.vote.into()),
            granted: r.vote_granted,
            last_log: r.last_log_id.map(Into::into),
        }))
    }
    async fn append(
        &self,
        r: Request<wire::AppendRequest>,
    ) -> Result<Response<wire::AppendResponse>, Status> {
        let r = r.into_inner();
        self.check(r.version, r.target)?;
        let r = self
            .raft
            .append_entries(AppendEntriesRequest {
                vote: r.vote.ok_or_else(missing)?.into(),
                prev_log_id: r.previous.map(Into::into),
                entries: r
                    .entries
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
                leader_commit: r.committed.map(Into::into),
            })
            .await
            .map_err(|_| Status::unavailable("raft stopped"))?;
        use wire::append_response::Result as R;
        Ok(Response::new(wire::AppendResponse {
            result: Some(match r {
                AppendEntriesResponse::Success => R::Success(wire::Empty {}),
                AppendEntriesResponse::Conflict => R::Conflict(wire::Empty {}),
                AppendEntriesResponse::PartialSuccess(matching) => R::Partial(wire::Partial {
                    matching: matching.map(Into::into),
                }),
                AppendEntriesResponse::HigherVote(v) => R::HigherVote(v.into()),
            }),
        }))
    }
    async fn install_snapshot(
        &self,
        r: Request<wire::SnapshotRequest>,
    ) -> Result<Response<wire::SnapshotResponse>, Status> {
        let r = r.into_inner();
        self.check(r.version, r.target)?;
        let r = self
            .raft
            .install_snapshot(InstallSnapshotRequest {
                vote: r.vote.ok_or_else(missing)?.into(),
                meta: r.meta.ok_or_else(missing)?.try_into()?,
                offset: r.offset,
                data: r.data,
                done: r.done,
            })
            .await;
        let result = match r {
            Ok(r) => wire::snapshot_response::Result::Vote(r.vote.into()),
            Err(RaftError::APIError(InstallSnapshotError::SnapshotMismatch(m))) => {
                wire::snapshot_response::Result::Mismatch(wire::SnapshotMismatch {
                    expected: Some(wire::Segment {
                        id: m.expect.id,
                        offset: m.expect.offset,
                    }),
                    received: Some(wire::Segment {
                        id: m.got.id,
                        offset: m.got.offset,
                    }),
                })
            }
            Err(_) => return Err(Status::unavailable("raft stopped")),
        };
        Ok(Response::new(wire::SnapshotResponse {
            result: Some(result),
        }))
    }
}
pub(crate) async fn serve(
    listener: TcpListener,
    id: NodeId,
    raft: Raft,
    mut stop: watch::Receiver<bool>,
) {
    let service = RaftServiceServer::new(Handler { id, raft })
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
        .max_encoding_message_size(MAX_MESSAGE_BYTES);
    let service = admission::AdmittedService::new(service, 4, DEADLINE);
    let cancel = tokio_util::sync::CancellationToken::new();
    let guard = cancel.clone().drop_guard();
    let server = tonic::transport::Server::builder()
        .concurrency_limit_per_connection(4)
        .max_concurrent_streams(4)
        .timeout(DEADLINE)
        .add_service(service)
        .serve_with_incoming(admission::incoming(listener, cancel));
    tokio::pin!(server);
    tokio::select! {r=&mut server=>{if let Err(e)=r{tracing::warn!(%e,"raft grpc listener stopped")}},_=stop.changed()=>{}}
    drop(guard);
}
