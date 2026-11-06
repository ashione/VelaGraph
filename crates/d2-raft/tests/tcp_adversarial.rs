static OUTBOUND_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

use d2_raft::{send_control, ControlRequest, TcpNode, TcpNodeConfig};
use std::{
    future::{poll_fn, Future},
    task::Poll,
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
#[tokio::test]
async fn cancelled_shutdown_still_stops_core() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();
    let node = TcpNode::start(TcpNodeConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: path,
    })
    .await
    .unwrap();
    let raft = node.raft.clone();
    let _socket = TcpStream::connect(&node.control_addr).await.unwrap();
    tokio::task::yield_now().await;
    let mut shutdown = Box::pin(node.shutdown());
    poll_fn(|cx| {
        assert!(shutdown.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(shutdown);
    let mut metrics = raft.metrics();
    let stopped = timeout(Duration::from_secs(2), async {
        loop {
            if metrics.borrow().running_state.is_err() {
                break;
            }
            if metrics.changed().await.is_err() {
                break;
            }
        }
    })
    .await
    .is_ok();
    raft.shutdown().await.unwrap();
    assert!(
        stopped,
        "cancelled TcpNode::shutdown leaves Raft core running"
    );
}
#[tokio::test]
async fn outbound_limit_releases_on_cancellation() {
    let _guard = OUTBOUND_TEST_LOCK.lock().await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let mut tasks = Vec::new();
    let mut sockets = Vec::new();
    for _ in 0..16 {
        let a = addr.clone();
        tasks.push(tokio::spawn(async move {
            send_control(&a, ControlRequest::Status).await
        }));
        sockets.push(
            timeout(Duration::from_secs(1), listener.accept())
                .await
                .unwrap()
                .unwrap(),
        );
    }
    let err = send_control(&addr, ControlRequest::Status)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("capacity"), "{err}");
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    let task = tokio::spawn(async move { send_control(&addr, ControlRequest::Status).await });
    let accepted = timeout(Duration::from_secs(1), listener.accept()).await;
    task.abort();
    let _ = task.await;
    assert!(accepted.is_ok());
}

#[tokio::test]
async fn inbound_admission_recovers_after_client_disconnect() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = tempfile::tempdir().unwrap();
    let node = TcpNode::start(TcpNodeConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: dir.path().into(),
    })
    .await
    .unwrap();
    let mut held = Vec::new();
    for _ in 0..4 {
        let mut socket = TcpStream::connect(&node.control_addr).await.unwrap();
        socket.write_all(b"{").await.unwrap();
        held.push(socket);
    }
    let mut queued = TcpStream::connect(&node.control_addr).await.unwrap();
    queued.write_all(b"\"Status\"\n").await.unwrap();
    let mut byte = [0];
    assert!(timeout(Duration::from_millis(100), queued.read(&mut byte))
        .await
        .is_err());
    drop(held.pop());
    assert_eq!(
        timeout(Duration::from_secs(1), queued.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        1
    );
    drop(held);
    node.shutdown().await;
}

#[tokio::test]
async fn oversized_aggregate_append_returns_split_hint() {
    let _guard = OUTBOUND_TEST_LOCK.lock().await;
    use d2_raft::{tcp::TcpNetworkFactory, LogCommand, TypeConfig};
    use openraft::{
        error::RPCError,
        network::{RPCOption, RaftNetwork, RaftNetworkFactory},
        raft::AppendEntriesRequest,
        BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, Vote,
    };
    let mut factory = TcpNetworkFactory;
    let mut client = factory
        .new_client(
            2,
            &BasicNode {
                addr: "127.0.0.1:1".into(),
            },
        )
        .await;
    // Each entry individually fits. Together their JSON byte arrays do not.
    let entries: Vec<Entry<TypeConfig>> = (1..=2)
        .map(|index| Entry {
            log_id: LogId::new(CommittedLeaderId::new(1, 1), index),
            payload: EntryPayload::Normal(LogCommand::put(b"k", vec![255; 9 * 1024 * 1024])),
        })
        .collect();
    let error = client
        .append_entries(
            AppendEntriesRequest {
                vote: Vote::new(1, 1),
                prev_log_id: None,
                entries,
                leader_commit: None,
            },
            RPCOption::new(Duration::from_secs(30)),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, RPCError::PayloadTooLarge(ref hint) if hint.entries_hint() == 1),
        "{error}"
    );
}
