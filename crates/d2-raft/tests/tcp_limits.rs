//! Real-socket resource regressions. These must run in CI, not be silently
//! skipped when a sandbox cannot bind sockets.
use d2_raft::{send_control, ControlRequest, ControlResponse, TcpNode, TcpNodeConfig};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{timeout, Instant},
};

async fn start(dir: &std::path::Path) -> anyhow::Result<TcpNode> {
    TcpNode::start(TcpNodeConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: dir.into(),
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flood_and_slow_clients_do_not_outlive_shutdown() -> anyhow::Result<()> {
    timeout(Duration::from_secs(12), async {
        let dir = tempfile::tempdir()?;
        let node = start(dir.path()).await?;
        let mut streams = Vec::new();
        for addr in [&node.control_addr, &node.raft_addr] {
            for _ in 0..32 {
                let mut stream = TcpStream::connect(addr).await?;
                stream.write_all(b"PRI * HTTP/2.0\r\n").await?;
                streams.push(stream);
            }
        }
        // Both listeners are flooded with incomplete frames, including queued
        // sockets. Shutdown must cancel and join all admitted handlers now.
        timeout(Duration::from_secs(1), node.shutdown()).await?;
        for mut stream in streams {
            // HTTP/2 may have already sent SETTINGS/GOAWAY before shutdown.
            // Drain bounded frames until EOF/reset rather than mistaking them
            // for a surviving socket.
            timeout(Duration::from_secs(1), async {
                let mut bytes = [0; 4096];
                for _ in 0..16 {
                    match stream.read(&mut bytes).await {
                        Ok(0) | Err(_) => return,
                        Ok(_) => {}
                    }
                }
                panic!("connection survived shutdown");
            })
            .await?;
        }
        // Joining handlers releases every server-owned state-machine Arc/DB.
        let reopened = start(dir.path()).await?;
        assert!(matches!(
            send_control(&reopened.control_addr, ControlRequest::Status).await?,
            ControlResponse::Status(_)
        ));
        reopened.shutdown().await;
        anyhow::Ok(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_reply_is_bounded_by_total_client_deadline() -> anyhow::Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(&mut stream).await.unwrap();
        let (_request, mut reply) = connection.accept().await.unwrap().unwrap();
        let headers = tonic::codegen::http::Response::builder()
            .header("content-type", "application/grpc")
            .body(())
            .unwrap();
        let mut body = reply.send_response(headers, false).unwrap();
        // A valid gRPC envelope, but a protobuf message arriving one byte at a
        // time: continued DATA progress must not refresh the total deadline.
        body.send_data(prost::bytes::Bytes::from_static(&[0, 0, 0, 4, 0]), false)
            .unwrap();
        let drip = async {
            loop {
                if body
                    .send_data(prost::bytes::Bytes::from_static(b" "), false)
                    .is_err()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        };
        let drive = async { while connection.accept().await.is_some() {} };
        tokio::select! { _ = drip => {}, _ = drive => {} }
    });
    let begin = Instant::now();
    let result = timeout(
        Duration::from_secs(5),
        send_control(&addr, ControlRequest::Status),
    )
    .await?;
    assert!(result.is_err());
    assert!(begin.elapsed() >= Duration::from_millis(2800));
    assert!(begin.elapsed() < Duration::from_secs(5));
    peer.abort();
    let _ = peer.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_control_protobuf_reply_is_rejected_before_body_allocation() -> anyhow::Result<()>
{
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?.to_string();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(&mut stream).await.unwrap();
        let (_request, mut reply) = connection.accept().await.unwrap().unwrap();
        let headers = tonic::codegen::http::Response::builder()
            .header("content-type", "application/grpc")
            .body(())
            .unwrap();
        let mut body = reply.send_response(headers, false).unwrap();
        let mut frame = vec![0];
        frame.extend_from_slice(&(64_u32 * 1024 * 1024 + 1).to_be_bytes());
        body.send_data(frame.into(), false).unwrap();
        // Keep the response open without allocating or sending its announced
        // body. The client must reject the size prefix, not rely on EOF.
        while connection.accept().await.is_some() {}
    });
    let result = timeout(
        Duration::from_secs(5),
        send_control(&addr, ControlRequest::Status),
    )
    .await?;
    let error = result.unwrap_err().to_string();
    assert!(
        error.contains("message length") || error.contains("too large"),
        "unexpected failure, not a size rejection: {error}"
    );
    peer.abort();
    let _ = peer.await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn slow_request_expires_and_normal_raft_write_still_commits() -> anyhow::Result<()> {
    timeout(Duration::from_secs(12), async {
        let dir = tempfile::tempdir()?;
        let node = start(dir.path()).await?;
        node.bootstrap(BTreeMap::from([(1, node.raft_addr.clone())])).await?;
        while node.raft.ensure_linearizable().await.is_err() { tokio::task::yield_now().await; }
        let socket = TcpStream::connect(&node.control_addr).await?;
        let (mut sender, connection) = h2::client::handshake(socket).await?;
        let driver = tokio::spawn(connection);
        let request = tonic::codegen::http::Request::builder()
            .method("POST")
            .uri(format!("http://{}/velagraph.internal.control.v1.ControlService/Call", node.control_addr))
            .header("content-type", "application/grpc").header("te", "trailers").body(())?;
        let (response, mut body) = sender.send_request(request, false)?;
        // Valid HTTP/2, incomplete protobuf body. Unlike old JSON garbage this
        // reaches request admission and exercises the request/body deadline.
        body.send_data(prost::bytes::Bytes::from_static(&[0, 0, 0, 4, 0, 8]), false)?;
        let began = Instant::now();
        let result = timeout(Duration::from_secs(5), response).await?;
        if let Ok(response) = result {
            let mut body = response.into_body();
            let mut status = None;
            let terminal = timeout(Duration::from_secs(1), async {
                while let Some(data) = body.data().await { if data.is_err() { return; } }
                if let Ok(Some(trailers)) = body.trailers().await {
                    status = trailers.get("grpc-status").cloned();
                }
            }).await;
            assert!(terminal.is_ok());
            // A reset at the absolute connection deadline is also terminal.
            if let Some(status) = status { assert_ne!(status, "0"); }
        }
        assert!(began.elapsed() >= Duration::from_millis(2500));
        assert!(began.elapsed() < Duration::from_secs(5));
        driver.abort();
        let _ = driver.await;
        assert!(matches!(send_control(&node.control_addr, ControlRequest::Write { key: "k".into(), value: "v".into() }).await?, ControlResponse::Index(_)));
        assert!(matches!(send_control(&node.control_addr, ControlRequest::Read { key: "k".into() }).await?, ControlResponse::Value(Some(v)) if v == "v"));
        node.shutdown().await;
        anyhow::Ok(())
    }).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn control_write_checks_replication_expansion_before_submission() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let node = start(dir.path()).await?;
    let before = node.state_machine.applied().await;
    let error = send_control(
        &node.control_addr,
        ControlRequest::Write {
            key: "oversized".into(),
            value: "x".repeat(16 * 1024 * 1024),
        },
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("replication payload budget"),
        "{error}"
    );
    assert_eq!(node.state_machine.applied().await, before);
    assert_eq!(node.state_machine.get("oversized").await, None);
    node.shutdown().await;
    Ok(())
}
