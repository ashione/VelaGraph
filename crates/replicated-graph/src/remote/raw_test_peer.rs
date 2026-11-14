//! HTTP/2 fault peer for prefix limits and missing ACKs. Never a JSON proxy.
use super::*;

pub(super) async fn spawn(
    oversized_length: Option<usize>,
) -> (SocketAddr, tokio::sync::oneshot::Receiver<Vec<u8>>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (seen, observed) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::Builder::new()
            .handshake::<_, axum::body::Bytes>(socket).await.unwrap();
        let (request, mut respond) = connection.accept().await.unwrap().unwrap();
        assert_eq!(request.method(), axum::http::Method::POST);
        assert_eq!(request.headers()["content-type"], "application/grpc");
        let mut body = request.into_body();
        let process = async move {
            let mut frame = Vec::new();
            while let Some(chunk) = body.data().await {
                let chunk = chunk.unwrap();
                assert!(frame.len().saturating_add(chunk.len()) <= MAX_FRAME_BYTES + 5);
                frame.extend_from_slice(&chunk);
                body.flow_control().release_capacity(chunk.len()).unwrap();
            }
            assert!(frame.len() >= 5);
            let _ = seen.send(frame);
            if let Some(size) = oversized_length {
                let headers = axum::http::Response::builder().status(200)
                    .header("content-type", "application/grpc").body(()).unwrap();
                let mut stream = respond.send_response(headers, false).unwrap();
                let mut prefix = vec![0];
                prefix.extend_from_slice(&(size as u32).to_be_bytes());
                // Deliberately no body: rejection must follow the bounded prefix.
                stream.send_data(prefix.into(), false).unwrap();
                std::future::pending::<()>().await;
            } else {
                // Keep the response handle alive after observing the entire request.
                let _response = respond;
                std::future::pending::<()>().await;
            }
        };
        tokio::pin!(process);
        tokio::select! {
            _ = &mut process => {},
            next = connection.accept() => { assert!(next.is_none() || next.is_some_and(|v| v.is_err())); }
        }
    });
    (address, observed, task)
}
