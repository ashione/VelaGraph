//! Actual server admission/lifecycle tests, not mock metadata handlers.
use super::*;
use crate::remote::{GraphState, MAX_IN_FLIGHT};
use d2_raft::{D1Command, LogCommand};
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use wire::metadata_service_client::MetadataServiceClient;

async fn fixture(deadline: Duration) -> (tempfile::TempDir, d2_raft::TcpNode, GraphState) {
    let directory = tempfile::tempdir().unwrap();
    let node = d2_raft::TcpNode::start(d2_raft::TcpNodeConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: directory.path().into(),
    })
    .await
    .unwrap();
    node.bootstrap(std::collections::BTreeMap::from([(
        1,
        node.raft_addr.clone(),
    )]))
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while node.raft.ensure_linearizable().await.is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    node.raft
        .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 1 }))
        .await
        .unwrap();
    let state = GraphState::from_tcp(&node, 10, 7, deadline).unwrap();
    (directory, node, state)
}
async fn channel(address: std::net::SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{address}"))
        .unwrap()
        .connect()
        .await
        .unwrap()
}
async fn released(state: &GraphState) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while state.metadata_streams.available_permits() != MAX_IN_FLIGHT {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("metadata stream permits released");
}
fn metadata_request(id: u64) -> wire::MetadataRequest {
    wire::MetadataRequest {
        version: 1,
        id,
        identity: Some(wire::Identity { group: 10, node: 1 }),
        shard: 7,
        timeout_millis: 1000,
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn streams_share_graph_budget_without_consuming_unary_operations() {
    let (_directory, node, state) = fixture(Duration::from_secs(5)).await;
    let a = grpc::GrpcSnapshotService::start(&state, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let b = grpc::GrpcSnapshotService::start(&state, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let mut streams = Vec::new();
    for n in 0..MAX_IN_FLIGHT {
        let address = if n % 2 == 0 {
            a.local_addr()
        } else {
            b.local_addr()
        };
        let mut client = MetadataServiceClient::new(channel(address).await);
        let (tx, rx) = mpsc::channel::<wire::MetadataRequest>(1);
        let stream = client
            .exchange(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        streams.push((tx, stream));
    }
    assert_eq!(state.metadata_streams.available_permits(), 0);
    assert_eq!(state.admission.available_permits(), MAX_IN_FLIGHT);
    let mut overflow = MetadataServiceClient::new(channel(a.local_addr()).await);
    let (_hold, rx) = mpsc::channel::<wire::MetadataRequest>(1);
    let error = overflow
        .exchange(ReceiverStream::new(rx))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    let mut unary = crate::remote::operations_grpc::Client::connect(
        a.local_addr(),
        RemoteIdentity { group: 10, node: 1 },
        Duration::from_secs(1),
    )
    .await
    .unwrap();
    assert_eq!(unary.metadata(7).await.unwrap(), (7, 1, 0));
    drop(streams);
    released(&state).await;
    drop(unary);
    drop(overflow);
    a.shutdown().await;
    b.shutdown().await;
    node.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metadata_idle_and_bad_id_end_only_the_rpc() {
    let (_directory, node, state) = fixture(Duration::from_millis(100)).await;
    let service = grpc::GrpcSnapshotService::start(&state, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let shared = channel(service.local_addr()).await;
    let mut stream_client = MetadataServiceClient::new(shared.clone());
    let (_hold, rx) = mpsc::channel::<wire::MetadataRequest>(1);
    let mut idle = stream_client
        .exchange(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let err = tokio::time::timeout(Duration::from_secs(2), idle.message())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    drop(idle);
    released(&state).await;
    let mut unary = wire::shard_operations_client::ShardOperationsClient::new(shared.clone());
    let reply = unary
        .call(wire::Request {
            version: 1,
            identity: Some(wire::Identity { group: 10, node: 1 }),
            timeout_millis: 1000,
            operation: Some(wire::request::Operation::Metadata(7)),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        reply.result,
        Some(wire::response::Result::Metadata(_))
    ));
    let (tx, rx) = mpsc::channel::<wire::MetadataRequest>(2);
    let mut replies = stream_client
        .exchange(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    tx.send(metadata_request(1)).await.unwrap();
    assert_eq!(replies.message().await.unwrap().unwrap().id, 1);
    tx.send(metadata_request(1)).await.unwrap();
    assert_eq!(
        replies.message().await.unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    drop(replies);
    drop(tx);
    released(&state).await;
    drop(shared);
    drop(unary);
    drop(stream_client);
    service.shutdown().await;
    node.shutdown().await;
}
