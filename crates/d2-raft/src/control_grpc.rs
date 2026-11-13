//! Typed trusted-network driver control. One command, no replay or JSON fallback.
use crate::raft_grpc::admission;
use crate::tcp::{ControlRequest, ControlResponse, NodeStatus, Raft, MAX_LINE, RPC_TIMEOUT};
use crate::StateMachine;
use prost::Message;
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{watch, Semaphore},
    time::{timeout_at, Instant},
};
use tonic::{Request, Response, Status};
pub mod wire {
    include!("control_grpc/control_proto.rs");
}
pub(crate) mod bounded;
mod codec;
static OUTBOUND: Semaphore = Semaphore::const_new(16);
const CONNECTIONS: usize = 4;
pub const PROTOCOL_VERSION: u32 = 1;
pub const MAX_MESSAGE_BYTES: usize = MAX_LINE;

// Count the historical JSON representation without retaining/transmitting it.
// Keeping this admission rule avoids silently enlarging the old control API.
struct LogicalWriter {
    bytes: usize,
    end: Instant,
}
impl io::Write for LogicalWriter {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        if Instant::now() >= self.end {
            return Err(io::ErrorKind::TimedOut.into());
        }
        self.bytes = self
            .bytes
            .checked_add(b.len())
            .filter(|n| *n < MAX_LINE)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "control logical size exceeded")
            })?;
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn logical<T: serde::Serialize>(v: &T, end: Instant) -> Result<(), Status> {
    crate::blocking::blocking_section(|| {
        let mut writer = LogicalWriter { bytes: 0, end };
        serde_json::to_writer(&mut writer, v).map_err(|e| {
            if e.io_error_kind() == Some(io::ErrorKind::TimedOut) {
                Status::deadline_exceeded("control deadline")
            } else {
                Status::resource_exhausted("control logical size")
            }
        })?;
        if Instant::now() >= end {
            return Err(Status::deadline_exceeded("control deadline"));
        }
        Ok(())
    })
}
#[derive(Clone)]
struct Handler {
    raft: Raft,
    state: Arc<StateMachine>,
}
#[tonic::async_trait]
impl wire::control_service_server::ControlService for Handler {
    async fn call(
        &self,
        r: Request<wire::ControlRequest>,
    ) -> Result<Response<wire::ControlResponse>, Status> {
        let accepted = r
            .extensions()
            .get::<admission::RequestDeadline>()
            .ok_or_else(|| Status::internal("missing control admission"))?
            .0;
        let (request, budget) = codec::decode_request(r.into_inner())?;
        let end = accepted.min(Instant::now() + budget);
        logical(&request, end)?;
        if Instant::now() >= end {
            return Err(Status::deadline_exceeded("control deadline"));
        }
        let response = timeout_at(
            end,
            crate::tcp::execute_control(self.raft.clone(), self.state.clone(), request),
        )
        .await
        .map_err(|_| Status::deadline_exceeded("control outcome unknown"))?;
        logical(&response, end)?;
        let wire = codec::encode_response(response);
        if wire.encoded_len() > MAX_LINE {
            return Err(Status::resource_exhausted("control response size"));
        }
        if Instant::now() >= end {
            return Err(Status::deadline_exceeded("control outcome unknown"));
        }
        let mut response = Response::new(wire);
        response
            .extensions_mut()
            .insert(admission::RequestDeadline(end));
        Ok(response)
    }
}
pub(crate) async fn request(
    addr: &str,
    request: ControlRequest,
) -> anyhow::Result<ControlResponse> {
    let end = Instant::now() + RPC_TIMEOUT;
    let _permit = OUTBOUND
        .try_acquire()
        .map_err(|_| anyhow::anyhow!("outbound RPC capacity exhausted"))?;
    timeout_at(end, async {
        logical(&request, end)?;
        let remaining = end
            .checked_duration_since(Instant::now())
            .ok_or_else(|| Status::deadline_exceeded("control deadline"))?;
        let wire = codec::encode_request(request, remaining.as_millis() as u64);
        if wire.encoded_len() > MAX_LINE {
            return Err(anyhow::anyhow!("control payload too large"));
        }
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))?
            .connect_timeout(remaining)
            .buffer_size(1)
            .connect()
            .await?;
        let mut client = wire::control_service_client::ControlServiceClient::new(channel)
            .max_decoding_message_size(MAX_LINE)
            .max_encoding_message_size(MAX_LINE);
        let mut request = Request::new(wire);
        request.set_timeout(end.saturating_duration_since(Instant::now()));
        let response = client.call(request).await?.into_inner();
        let response = codec::decode_response(response)?;
        logical(&response, end)?;
        Ok(response)
    })
    .await
    .map_err(|_| anyhow::anyhow!("control RPC deadline expired; mutation outcome unknown"))?
}
pub(crate) async fn serve(
    listener: TcpListener,
    raft: Raft,
    state: Arc<StateMachine>,
    mut stop: watch::Receiver<bool>,
) {
    let service = wire::control_service_server::ControlServiceServer::new(Handler { raft, state })
        .max_decoding_message_size(MAX_LINE)
        .max_encoding_message_size(MAX_LINE);
    let service = admission::AdmittedService::new(service, CONNECTIONS, RPC_TIMEOUT);
    let cancel = tokio_util::sync::CancellationToken::new();
    let guard = cancel.clone().drop_guard();
    let shutdown_cancel = cancel.clone();
    let server = tonic::transport::Server::builder()
        .concurrency_limit_per_connection(CONNECTIONS)
        .max_concurrent_streams(CONNECTIONS as u32)
        .timeout(RPC_TIMEOUT)
        .add_service(service)
        .serve_with_incoming_shutdown(
            admission::incoming_with_limits(listener, cancel, CONNECTIONS, RPC_TIMEOUT),
            async move {
                let _ = stop.changed().await;
                shutdown_cancel.cancel();
            },
        );
    // Close admitted sockets, then wait for tonic to release all connections and
    // handlers before TcpNode proceeds to Raft shutdown and durable-store reopen.
    if let Err(e) = server.await {
        tracing::warn!(%e,"control grpc listener stopped");
    }
    drop(guard);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn logical_count_preserves_json_size_and_deadline_without_buffer() {
        for v in [
            ControlResponse::Ack,
            ControlResponse::Value(None),
            ControlResponse::Value(Some("中文\0\"\\".into())),
        ] {
            let mut w = LogicalWriter {
                bytes: 0,
                end: Instant::now() + RPC_TIMEOUT,
            };
            serde_json::to_writer(&mut w, &v).unwrap();
            assert_eq!(w.bytes, serde_json::to_vec(&v).unwrap().len());
        }
        let mut w = LogicalWriter {
            bytes: MAX_LINE - 2,
            end: Instant::now() + RPC_TIMEOUT,
        };
        assert!(io::Write::write(&mut w, b"x").is_ok());
        assert!(io::Write::write(&mut w, b"x").is_err());
        assert_eq!(
            logical(&ControlRequest::Status, Instant::now())
                .unwrap_err()
                .code(),
            tonic::Code::DeadlineExceeded
        );
    }
}
