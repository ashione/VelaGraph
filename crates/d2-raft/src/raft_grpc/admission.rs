use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio::{sync::Semaphore, time::Instant};
use tonic::{
    codegen::{http, BoxFuture, Bytes, Service},
    Status,
};
#[derive(Clone)]
pub(super) struct AdmittedService<S> {
    inner: S,
    admission: Arc<Semaphore>,
    deadline: Duration,
}
impl<S: tonic::server::NamedService> tonic::server::NamedService for AdmittedService<S> {
    const NAME: &'static str = S::NAME;
}
impl<B, S> Service<http::Request<B>> for AdmittedService<S>
where
    S: Service<
            http::Request<B>,
            Response = http::Response<tonic::body::Body>,
            Error = std::convert::Infallible,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;
    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let Ok(permit) = self.admission.clone().try_acquire_owned() else {
            return Box::pin(async {
                Ok(Status::resource_exhausted("raft inbound capacity").into_http())
            });
        };
        let budget = request
            .headers()
            .get("grpc-timeout")
            .and_then(|v| v.to_str().ok())
            .and_then(grpc_timeout)
            .map_or(self.deadline, |d| d.min(self.deadline));
        let end = Instant::now() + budget;
        let connection = request.extensions().get::<ConnectionStop>().cloned();
        let watcher = connection.map(|c| Watchdog::new(end, c));
        let future = self.inner.call(request);
        Box::pin(async move {
            let response = match tokio::time::timeout_at(end, future).await {
                Ok(r) => r?,
                Err(_) => return Ok(Status::deadline_exceeded("raft deadline").into_http()),
            };
            Ok(response.map(|body| {
                tonic::body::Body::new(AdmittedBody {
                    body,
                    _permit: permit,
                    _watcher: watcher,
                    deadline: Box::pin(tokio::time::sleep_until(end)),
                })
            }))
        })
    }
}
// Standard gRPC timeout grammar (at most eight digits plus one unit).
// Tonic enforces it while producing the response; retain it for the body too.
fn grpc_timeout(value: &str) -> Option<Duration> {
    if !value.is_ascii() || value.len() < 2 || value.len() > 9 {
        return None;
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n = digits.parse::<u64>().ok()?;
    Some(match unit {
        "H" => Duration::from_secs(n * 3600),
        "M" => Duration::from_secs(n * 60),
        "S" => Duration::from_secs(n),
        "m" => Duration::from_millis(n),
        "u" => Duration::from_micros(n),
        "n" => Duration::from_nanos(n),
        _ => return None,
    })
}
struct AdmittedBody {
    _watcher: Option<Watchdog>,
    body: tonic::body::Body,
    _permit: tokio::sync::OwnedSemaphorePermit,
    deadline: Pin<Box<tokio::time::Sleep>>,
}
impl http_body::Body for AdmittedBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Status>>> {
        if self.deadline.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Some(Err(Status::deadline_exceeded("response deadline"))));
        }
        Pin::new(&mut self.body).poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

impl<S> AdmittedService<S> {
    pub(super) fn new(inner: S, limit: usize, deadline: Duration) -> Self {
        Self {
            inner,
            admission: Arc::new(Semaphore::new(limit)),
            deadline,
        }
    }
}

use std::io;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
};
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
pub(super) struct LimitedIo {
    socket: TcpStream,
    _permit: tokio::sync::OwnedSemaphorePermit,
    stop: ConnectionStop,
    cancelled: Pin<Box<tokio_util::sync::WaitForCancellationFutureOwned>>,
}
impl tonic::transport::server::Connected for LimitedIo {
    type ConnectInfo = ConnectionStop;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.stop.clone()
    }
}
impl AsyncRead for LimitedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_read(cx, buf)
    }
}
impl AsyncWrite for LimitedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::ErrorKind::ConnectionAborted.into()));
        }
        Pin::new(&mut self.socket).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.socket).poll_shutdown(cx)
    }
}

pub(super) fn incoming(
    listener: TcpListener,
    stop: CancellationToken,
) -> impl tokio_stream::Stream<Item = io::Result<LimitedIo>> {
    let connections = Arc::new(Semaphore::new(256));
    tokio_stream::wrappers::TcpListenerStream::new(listener).filter_map(move |accepted| {
        match accepted {
            Err(e) => Some(Err(e)),
            Ok(socket) => connections.clone().try_acquire_owned().ok().map(|permit| {
                let token = stop.child_token();
                Ok(LimitedIo {
                    socket,
                    stop: ConnectionStop(token.clone()),
                    _permit: permit,
                    cancelled: Box::pin(token.cancelled_owned()),
                })
            }),
        }
    })
}

#[derive(Clone)]
pub(super) struct ConnectionStop(CancellationToken);
// Independent of Body::poll_frame: HTTP/2 zero-window peers may never poll it.
struct Watchdog(tokio::task::JoinHandle<()>);
impl Watchdog {
    fn new(end: Instant, stop: ConnectionStop) -> Self {
        Self(tokio::spawn(async move {
            tokio::time::sleep_until(end).await;
            stop.0.cancel();
        }))
    }
}
impl Drop for Watchdog {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn unpolled_response_budget_expires_and_completed_call_keeps_channel() {
        let token = CancellationToken::new();
        let _watcher = Watchdog::new(Instant::now(), ConnectionStop(token.clone()));
        tokio::time::timeout(Duration::from_secs(1), token.cancelled())
            .await
            .unwrap();
        let token = CancellationToken::new();
        let watcher = Watchdog::new(
            Instant::now() + Duration::from_secs(60),
            ConnectionStop(token.clone()),
        );
        drop(watcher);
        tokio::task::yield_now().await;
        assert!(!token.is_cancelled());
    }
}
