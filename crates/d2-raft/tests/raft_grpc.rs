//! Real socket Raft RPC regressions. The process test also uses this transport.
use d2_raft::{
    raft_grpc::{wire, GrpcNetworkFactory},
    LogCommand, TcpNode, TcpNodeConfig,
};
use openraft::{
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::InstallSnapshotRequest,
    storage::RaftSnapshotBuilder,
    BasicNode, Vote,
};
use std::{collections::BTreeMap, time::Duration};
use tokio::time::{sleep, timeout};
async fn node(id: u64, path: &std::path::Path) -> anyhow::Result<TcpNode> {
    TcpNode::start(TcpNodeConfig {
        id,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: path.into(),
    })
    .await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_chunks_preserve_offset_mismatch_and_install() -> anyhow::Result<()> {
    timeout(Duration::from_secs(20), async {
        let a = tempfile::tempdir()?;
        let b = tempfile::tempdir()?;
        let source = node(1, a.path()).await?;
        let target = node(2, b.path()).await?;
        source
            .bootstrap(BTreeMap::from([(1, source.raft_addr.clone())]))
            .await?;
        while source.raft.ensure_linearizable().await.is_err() {
            sleep(Duration::from_millis(10)).await;
        }
        source
            .raft
            .client_write(LogCommand::put(b"key", vec![0, 255, 42]))
            .await?;
        let image = source.state_machine.clone().build_snapshot().await?;
        let data = image.snapshot.get_ref();
        let split = data.len() / 2;
        let mut client = GrpcNetworkFactory
            .new_client(2, &BasicNode::new(&target.raft_addr))
            .await;
        let request = InstallSnapshotRequest {
            vote: Vote::new_committed(9, 1),
            meta: image.meta.clone(),
            offset: 0,
            data: data[..split].to_vec(),
            done: false,
        };
        client
            .install_snapshot(request, RPCOption::new(Duration::from_secs(3)))
            .await?;
        let mut wrong_meta = image.meta.clone();
        wrong_meta.snapshot_id.push_str("-wrong-stream");
        let wrong = InstallSnapshotRequest {
            vote: Vote::new_committed(9, 1),
            meta: wrong_meta,
            offset: split as u64,
            data: data[split..].to_vec(),
            done: true,
        };
        let error = client
            .install_snapshot(wrong, RPCOption::new(Duration::from_secs(3)))
            .await
            .unwrap_err();
        assert!(
            matches!(error, openraft::error::RPCError::RemoteError(_)),
            "{error:?}"
        );
        let request = InstallSnapshotRequest {
            vote: Vote::new_committed(9, 1),
            meta: image.meta,
            offset: split as u64,
            data: data[split..].to_vec(),
            done: true,
        };
        client
            .install_snapshot(request, RPCOption::new(Duration::from_secs(3)))
            .await?;
        assert_eq!(
            target.state_machine.get_raw(b"key").await,
            Some(vec![0, 255, 42])
        );
        source.shutdown().await;
        target.shutdown().await;
        anyhow::Ok(())
    })
    .await?
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn version_and_target_are_checked_before_raft() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let node = node(2, dir.path()).await?;
    let mut client =
        wire::raft_service_client::RaftServiceClient::connect(format!("http://{}", node.raft_addr))
            .await?;
    let request = wire::VoteRequest {
        version: 1,
        target: 2,
        vote: Some(wire::Vote {
            term: 1,
            node: 1,
            committed: false,
        }),
        last_log: None,
    };
    let mut wrong = request;
    wrong.version = 2;
    assert_eq!(
        client.vote(wrong).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let mut wrong = request;
    wrong.target = 3;
    assert_eq!(
        client.vote(wrong).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert!(client.vote(request).await?.into_inner().granted);
    node.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_indivisible_legacy_batch_is_received_and_applied() -> anyhow::Result<()> {
    timeout(Duration::from_secs(10), async {
        use openraft::{
            raft::AppendEntriesRequest, CommittedLeaderId, Entry, EntryPayload, LogId, Membership,
        };
        let dir = tempfile::tempdir()?;
        let target = node(2, dir.path()).await?;
        let command = LogCommand::TxnBatch(vec![d2_raft::WriteOp::delete(b"k"); 131_072]);
        assert!(serde_json::to_vec(&command)?.len() < 64 * 1024 * 1024);
        let mut client = GrpcNetworkFactory
            .new_client(2, &BasicNode::new(&target.raft_addr))
            .await;
        let first = LogId::new(CommittedLeaderId::new(9, 1), 0);
        let last = LogId::new(CommittedLeaderId::new(9, 1), 1);
        let entries = vec![
            Entry {
                log_id: first,
                payload: EntryPayload::Membership(Membership::new(
                    vec![std::collections::BTreeSet::from([1, 2])],
                    BTreeMap::from([
                        (1, BasicNode::new("127.0.0.1:1")),
                        (2, BasicNode::new(&target.raft_addr)),
                    ]),
                )),
            },
            Entry {
                log_id: last,
                payload: EntryPayload::Normal(command),
            },
        ];
        // Verify transport capacity independently of the cluster's 100ms heartbeat RPC
        // deadline: do not relax that production setting to make this boundary pass.
        let result = client
            .append_entries(
                AppendEntriesRequest {
                    vote: Vote::new_committed(9, 1),
                    prev_log_id: None,
                    entries,
                    leader_commit: Some(last),
                },
                RPCOption::new(Duration::from_secs(3)),
            )
            .await?;
        assert!(result.is_success());
        while target
            .state_machine
            .applied()
            .await
            .is_none_or(|id| id.index < last.index)
        {
            sleep(Duration::from_millis(10)).await;
        }
        target.shutdown().await;
        anyhow::Ok(())
    })
    .await?
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn zero_http2_window_cannot_hold_raft_admission() -> anyhow::Result<()> {
    use prost::Message;
    use tonic::codegen::http;
    let dir = tempfile::tempdir()?;
    let node = node(2, dir.path()).await?;
    let socket = tokio::net::TcpStream::connect(&node.raft_addr).await?;
    let (mut sender, connection) = h2::client::Builder::new()
        .initial_window_size(0)
        .handshake::<_, prost::bytes::Bytes>(socket)
        .await?;
    let driver = tokio::spawn(connection);
    let message = wire::VoteRequest {
        version: 1,
        target: 2,
        vote: Some(wire::Vote {
            term: 1,
            node: 1,
            committed: false,
        }),
        last_log: None,
    }
    .encode_to_vec();
    let mut frame = vec![0];
    frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
    frame.extend_from_slice(&message);
    let mut responses = Vec::new();
    for _ in 0..4 {
        let request = http::Request::builder()
            .method("POST")
            .uri(format!(
                "http://{}/velagraph.internal.raft.v1.RaftService/Vote",
                node.raft_addr
            ))
            .header("content-type", "application/grpc")
            .header("te", "trailers")
            .header("grpc-timeout", "200m")
            .body(())?;
        let (response, mut body) = sender.send_request(request, false)?;
        body.send_data(frame.clone().into(), true)?;
        responses.push(response);
    }
    let mut held = Vec::new();
    for r in responses {
        let r = timeout(Duration::from_secs(1), r).await??;
        assert!(r.headers().get("grpc-status").is_none());
        assert!(!r.body().is_end_stream());
        held.push(r);
    }
    let _ = timeout(Duration::from_secs(2), driver).await??;
    let mut client =
        wire::raft_service_client::RaftServiceClient::connect(format!("http://{}", node.raft_addr))
            .await?;
    assert!(
        client
            .vote(wire::VoteRequest {
                version: 1,
                target: 2,
                vote: Some(wire::Vote {
                    term: 2,
                    node: 1,
                    committed: false
                }),
                last_log: None
            })
            .await?
            .into_inner()
            .granted
    );
    drop(held);
    node.shutdown().await;
    Ok(())
}
