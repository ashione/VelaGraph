//! Bounded loopback HTTP/1 preview. One request per connection deliberately
//! keeps response-delivery ownership coupled to the actual connection outcome.
use super::*;
use axum::{
    body::{Body, Bytes},
    extract::Request as HttpRequest,
    http::{header, Method, Response as HttpResponse, StatusCode},
};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    pin::Pin,
    task::{Context, Poll},
};

/// Loopback-only HTTP adapter sharing the managed gateway's admission and state.
/// It is not an authentication boundary or a global-restore implementation.
pub struct HttpService {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl HttpService {
    pub fn local_addr(&self) -> SocketAddr {
        self.address
    }
    pub async fn shutdown(mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for HttpService {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

type Delivery = Arc<Mutex<Option<invocation::Invocation>>>;

pub(super) async fn bind(
    state: Arc<State>,
    mut gateway_stop: watch::Receiver<bool>,
    bind: SocketAddr,
    deadline: Duration,
) -> Result<HttpService> {
    if !bind.ip().is_loopback() || *gateway_stop.borrow() {
        return Err(Error::Rejected);
    }
    check_deadline(deadline)?;
    let listener = TcpListener::bind(bind).await.map_err(|_| Error::Unknown)?;
    let address = listener.local_addr().map_err(|_| Error::Unknown)?;
    let (stop, mut stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            let accepted = tokio::select! {
                _ = stopped.changed() => break,
                _ = gateway_stop.changed() => break,
                _ = connections.join_next(), if !connections.is_empty() => continue,
                accepted = listener.accept() => accepted,
            };
            let Ok((socket, _)) = accepted else {
                break;
            };
            let Ok(permit) = state.connections.clone().try_acquire_owned() else {
                continue;
            };
            let state = state.clone();
            let end = Instant::now() + deadline;
            connections.spawn(async move {
                let work = Arc::new(permit);
                let delivery: Delivery = Arc::new(Mutex::new(None));
                let response_delivery = delivery.clone();
                let (budget, remaining) = watch::channel(end);
                let service = service_fn(move |request| {
                    handle(
                        request,
                        state.clone(),
                        work.clone(),
                        end,
                        response_delivery.clone(),
                        budget.clone(),
                    )
                });
                let connection = hyper::server::conn::http1::Builder::new()
                    .keep_alive(false)
                    .max_buf_size(MAX_REQUEST_BYTES)
                    .serve_connection(TokioIo::new(socket), service);
                let sent = deliver_until(connection, remaining).await;
                if sent {
                    if let Some(invocation) = delivery.lock().unwrap().as_mut() {
                        // The response completed on this one-request connection.
                        // This still is not evidence of application receipt.
                        if invocation.live().is_ok() {
                            invocation.delivered();
                        }
                    }
                }
                // On error/timeout/drop the guard invalidates only new opens.
                // Existing sessions and uncertain durable writes are not undone.
            });
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    });
    Ok(HttpService {
        address,
        stop,
        task: Some(task),
    })
}

// The response-body future alone cannot detect a stalled socket flush. Keep
// the deadline around the complete Hyper connection, and accept only decreases.
async fn deliver_until<F>(connection: F, mut remaining: watch::Receiver<Instant>) -> bool
where
    F: std::future::Future<Output = std::result::Result<(), hyper::Error>>,
{
    tokio::pin!(connection);
    let mut end = *remaining.borrow_and_update();
    loop {
        end = end.min(*remaining.borrow_and_update());
        if Instant::now() >= end {
            return false;
        }
        tokio::select! {
            result = &mut connection => return result.is_ok(),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(end)) => return false,
            changed = remaining.changed() => { if changed.is_err() { return false; } },
        }
    }
}

async fn handle(
    request: HttpRequest<hyper::body::Incoming>,
    state: Arc<State>,
    work: Arc<OwnedSemaphorePermit>,
    end: Instant,
    delivery: Delivery,
    budget: watch::Sender<Instant>,
) -> std::result::Result<HttpResponse<Body>, Infallible> {
    // Admission was acquired before HTTP header/body parsing, and the entire
    // connection has a deadline. HTTP cannot allocate an independent queue.
    if state
        .jobs_stopping
        .load(std::sync::atomic::Ordering::Acquire)
    {
        return Ok(error_response(Error::Rejected));
    }
    if request.method() == Method::GET && request.uri().path() == "/preview1/service" {
        return Ok(HttpResponse::builder().status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{\"state\":\"live_managed_gateway\",\"restore_supported\":false,\"production_ready\":false,\"http_preview\":true}")).unwrap());
    }
    if request.method() != Method::POST
        || request.uri().query().is_some()
        || request
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            != Some("application/json")
    {
        return Ok(error_response(Error::Protocol));
    }
    if request
        .headers()
        .get_all(header::CONTENT_TYPE)
        .iter()
        .count()
        != 1
        || request
            .headers()
            .get_all("x-velagraph-timeout-ms")
            .iter()
            .count()
            > 1
    {
        return Ok(error_response(Error::Protocol));
    }
    // Public HTTP timeout is a bounded relative budget, not a session lease.
    let end = match request.headers().get("x-velagraph-timeout-ms") {
        None => end,
        Some(value) => match value.to_str().ok().and_then(|s| s.parse::<u64>().ok()) {
            Some(ms) if ms > 0 && ms <= MAX_DEADLINE.as_millis() as u64 => {
                end.min(Instant::now() + Duration::from_millis(ms))
            }
            _ => return Ok(error_response(Error::Protocol)),
        },
    };
    budget.send_replace(end);
    let path = request.uri().path().to_string();
    let result = tokio::time::timeout_at(tokio::time::Instant::from_std(end), async {
        let bytes = axum::body::to_bytes(Body::new(request.into_body()), MAX_REQUEST_BYTES)
            .await
            .map_err(|_| Error::Capacity)?;
        before_deadline(end)?;
        let operation = http_codec::decode(&path, &bytes)?;
        let end = invocation::operation_end(&operation, end);
        budget.send_replace(end);
        let request = Request {
            version: VERSION,
            request_id: random()?,
            binding: state.config.binding.clone(),
            operation,
        };
        let invocation = invocation::Invocation::begin(state, request, work, end).await?;
        let (status, bytes, invocation) = tokio::task::spawn_blocking(move || {
            invocation.preflight_http()?;
            let (status, bytes) = http_codec::encode(&invocation.response.result)?;
            invocation.live()?;
            Ok::<_, Error>((status, bytes, invocation))
        })
        .await
        .map_err(|_| Error::Unknown)??;
        invocation.live()?;
        budget.send_replace(invocation.delivery_end()?);
        *delivery.lock().unwrap() = Some(invocation);
        let body = GuardedBody {
            bytes: Some(Bytes::from(bytes)),
            delivery: delivery.clone(),
        };
        Ok::<_, Error>(
            HttpResponse::builder()
                .status(status)
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::new(body))
                .unwrap(),
        )
    })
    .await;
    Ok(match result {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => error_response(error),
        Err(_) => error_response(Error::Unknown),
    })
}

fn error_response(error: Error) -> HttpResponse<Body> {
    let (status, bytes) = http_codec::encode(&Err(error))
        .unwrap_or((500, b"{\"error\":{\"code\":\"unknown\"}}".to_vec()));
    HttpResponse::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(bytes))
        .unwrap()
}

struct GuardedBody {
    bytes: Option<Bytes>,
    delivery: Delivery,
}
impl http_body::Body for GuardedBody {
    type Data = Bytes;
    type Error = io::Error;
    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<http_body::Frame<Bytes>, Self::Error>>> {
        if let Some(bytes) = self.bytes.take() {
            if self
                .delivery
                .lock()
                .unwrap()
                .as_ref()
                .is_none_or(|invocation| invocation.live().is_err())
            {
                return Poll::Ready(Some(Err(io::Error::other("managed delivery expired"))));
            }
            return Poll::Ready(Some(Ok(http_body::Frame::data(bytes))));
        }
        Poll::Ready(None)
    }
    fn is_end_stream(&self) -> bool {
        self.bytes.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn http_delivery_budget_tracks_shortened_lease() {
        let (budget, remaining) = watch::channel(Instant::now() + Duration::from_secs(60));
        let blocked_flush = std::future::pending::<std::result::Result<(), hyper::Error>>();
        let task = tokio::spawn(deliver_until(blocked_flush, remaining));
        // This is the same budget update used for the invocation's expiry.
        budget.send_replace(Instant::now() + Duration::from_millis(20));
        assert!(!tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap());
    }
}
