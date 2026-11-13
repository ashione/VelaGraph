//! Default node control ingress is typed gRPC, with no newline-JSON fallback.
use d2_raft::{
    control_grpc::wire as w, send_control, ControlRequest, ControlResponse, TcpNode, TcpNodeConfig,
};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::timeout,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_envelopes_and_legacy_json_cannot_mutate_node() -> anyhow::Result<()> {
    timeout(Duration::from_secs(15), async {
        let dir = tempfile::tempdir()?;
        let node = TcpNode::start(TcpNodeConfig {
            id: 1, raft_addr: "127.0.0.1:0".into(), control_addr: "127.0.0.1:0".into(), data_dir: dir.path().into(),
        }).await?;
        assert!(matches!(send_control(&node.control_addr, ControlRequest::Initialize {
            members: BTreeMap::from([(1, node.raft_addr.clone())]),
        }).await?, ControlResponse::Ack));
        while node.raft.ensure_linearizable().await.is_err() { tokio::task::yield_now().await; }
        let before = node.state_machine.applied().await;
        let mut client = w::control_service_client::ControlServiceClient::connect(format!("http://{}", node.control_addr)).await?;
        for (version, millis) in [(99, 3000), (1, 0), (1, 3001)] {
            assert!(client.call(w::ControlRequest {
                version, timeout_millis: millis,
                command: Some(w::control_request::Command::Write(w::Write { key: "rejected".into(), value: "v".into() })),
            }).await.is_err());
        }
        assert!(client.call(w::ControlRequest { version: 1, timeout_millis: 3000, command: None }).await.is_err());
        assert_eq!(node.state_machine.applied().await, before);
        assert_eq!(node.state_machine.get("rejected").await, None);
        // Former valid JSON must not execute even though it targets the same port.
        let mut old = TcpStream::connect(&node.control_addr).await?;
        old.write_all(b"{\"Write\":{\"key\":\"old-json\",\"value\":\"bad\"}}\n").await?;
        timeout(Duration::from_secs(4), async {
            let mut buf = [0; 4096];
            for _ in 0..16 {
                match old.read(&mut buf).await { Ok(0) | Err(_) => return, Ok(_) => {} }
            }
            panic!("legacy JSON connection did not terminate");
        }).await?;
        assert_eq!(node.state_machine.get("old-json").await, None);
        assert_eq!(node.state_machine.applied().await, before);
        assert!(matches!(send_control(&node.control_addr, ControlRequest::Write { key: "accepted".into(), value: "typed".into() }).await?, ControlResponse::Index(_)));
        for req in [ControlRequest::Read { key: "accepted".into() }, ControlRequest::Get { key: "accepted".into() }] {
            assert!(matches!(send_control(&node.control_addr, req).await?, ControlResponse::Value(Some(v)) if v == "typed"));
        }
        node.shutdown().await;
        anyhow::Ok(())
    }).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn blocked_response_bodies_release_control_admission() -> anyhow::Result<()> {
    use prost::Message;
    use tonic::codegen::http;
    let dir = tempfile::tempdir()?;
    let node = TcpNode::start(TcpNodeConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: dir.path().into(),
    })
    .await?;
    let socket = TcpStream::connect(&node.control_addr).await?;
    let (mut sender, connection) = h2::client::Builder::new()
        .initial_window_size(0)
        .handshake::<_, prost::bytes::Bytes>(socket)
        .await?;
    let driver = tokio::spawn(connection);
    let payload = w::ControlRequest {
        version: 1,
        timeout_millis: 3000,
        command: Some(w::control_request::Command::Status(w::Empty {})),
    }
    .encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    let mut pending = Vec::new();
    for _ in 0..4 {
        let req = http::Request::builder()
            .method("POST")
            .uri(format!(
                "http://{}/velagraph.internal.control.v1.ControlService/Call",
                node.control_addr
            ))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .body(())?;
        let (reply, mut body) = sender.send_request(req, false)?;
        body.send_data(frame.clone().into(), true)?;
        pending.push(reply);
    }
    let mut held = Vec::new();
    for reply in pending {
        let reply = timeout(Duration::from_secs(1), reply).await??;
        assert!(reply.headers().get("grpc-status").is_none());
        assert!(!reply.body().is_end_stream());
        held.push(reply);
    }
    // One physical connection occupies four operation permits through unread
    // response bodies; another connection must not bypass operation admission.
    assert!(send_control(&node.control_addr, ControlRequest::Status)
        .await
        .is_err());
    timeout(Duration::from_secs(4), async {
        let _ = driver.await;
    })
    .await?;
    assert!(matches!(
        send_control(&node.control_addr, ControlRequest::Status).await?,
        ControlResponse::Status(_)
    ));
    drop(held);
    node.shutdown().await;
    Ok(())
}
