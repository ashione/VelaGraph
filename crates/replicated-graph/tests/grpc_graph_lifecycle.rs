//! Listener-free state owns shared budgets; explicitly bound RPC adapters own
//! their sockets and cleanup tasks. Legacy TCP is never created implicitly.
use d2_raft::{D1Command, D1Result, LogCommand, TcpNode, TcpNodeConfig};
use replicated_graph::remote::{
    distributed::grpc::wire as q, grpc::GrpcSnapshotService, GraphState,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    time::{sleep, timeout},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn listener_free_state_shares_rpc_sessions_and_releases_lifecycle() -> anyhow::Result<()> {
    timeout(Duration::from_secs(20), async {
        let removed_flag = std::process::Command::new(env!("CARGO_BIN_EXE_graph-node"))
            .env("TOKIO_WORKER_THREADS", "2")
            .args(["--grpc-snapshot", "127.0.0.1:1"])
            .output()?;
        assert!(!removed_flag.status.success());
        let error = String::from_utf8_lossy(&removed_flag.stderr);
        assert!(error.contains("--grpc-snapshot was removed") && error.contains("--graph"));
        let dir = tempfile::tempdir()?;
        let path = dir.path().to_path_buf();
        let node = TcpNode::start(TcpNodeConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".into(),
            control_addr: "127.0.0.1:0".into(),
            data_dir: path.clone(),
        })
        .await?;
        node.bootstrap(BTreeMap::from([(1, node.raft_addr.clone())]))
            .await?;
        while node.raft.ensure_linearizable().await.is_err() {
            sleep(Duration::from_millis(10)).await;
        }
        let activated = node
            .raft
            .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 3 }))
            .await?;
        assert_eq!(
            activated.data.d1,
            Some(Ok(D1Result::Activated { shard: 7, epoch: 3 }))
        );
        let context = || {
            Some(q::Context {
                version: 1,
                identity: Some(q::Identity { group: 9, node: 1 }),
                timeout_millis: 1000,
            })
        };
        // More than one cycle proves dropped hierarchy sessions release the
        // process-wide eight slots, rather than waiting for their 120s lease.
        for graceful in [true, false, true] {
            let before = owned_listeners()?;
            let state = GraphState::from_tcp(&node, 9, 7, Duration::from_millis(300))?;
            assert_eq!(
                owned_listeners()?,
                before,
                "state construction must not bind TCP"
            );
            let service = GrpcSnapshotService::start(&state, "127.0.0.1:0".parse()?).await?;
            let address = service.local_addr();
            assert_added_listener(&before, address)?;
            let mut client =
                q::query_service_client::QueryServiceClient::connect(format!("http://{address}"))
                    .await?;
            // A second explicitly requested adapter shares the exact same pool.
            let second = GrpcSnapshotService::start(&state, "127.0.0.1:0".parse()?).await?;
            let second_address = second.local_addr();
            let mut other = q::query_service_client::QueryServiceClient::connect(format!(
                "http://{second_address}"
            ))
            .await?;
            let mut descriptors = Vec::new();
            for _ in 0..8 {
                descriptors.push(
                    client
                        .open_hierarchy(q::OpenRequest {
                            context: context(),
                            shard: 7,
                        })
                        .await?
                        .into_inner()
                        .descriptor
                        .unwrap(),
                );
            }
            assert_eq!(
                other
                    .open_hierarchy(q::OpenRequest {
                        context: context(),
                        shard: 7
                    })
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::ResourceExhausted
            );
            other
                .close(q::CloseRequest {
                    context: context(),
                    descriptor: descriptors.pop(),
                })
                .await?;
            let mut short = context().unwrap();
            short.timeout_millis = 100;
            let expired = client
                .open(q::OpenRequest {
                    context: Some(short),
                    shard: 7,
                })
                .await?
                .into_inner();
            sleep(Duration::from_millis(250)).await;
            // There is room for a new session after the ordinary short lease.
            other
                .open_hierarchy(q::OpenRequest {
                    context: context(),
                    shard: 7,
                })
                .await?;
            assert_eq!(
                client
                    .fragment(q::FragmentRequest {
                        context: context(),
                        descriptor: Some(expired),
                        fragment: Some(q::Fragment {
                            kind: Some(q::fragment::Kind::Exists(q::VertexId {
                                partition: 0,
                                id: 1
                            }))
                        }),
                        remaining: Some(q::Cost {
                            bytes: 1024,
                            work: 100
                        }),
                    })
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::NotFound
            );
            drop(state);
            if graceful {
                timeout(Duration::from_secs(2), service.shutdown()).await?;
            } else {
                drop(service);
            }
            wait_closed(address).await?;
            // Stopping one adapter does not invalidate the other's shared pool.
            other
                .close(q::CloseRequest {
                    context: context(),
                    descriptor: descriptors.pop(),
                })
                .await?;
            drop(second);
            wait_closed(second_address).await?;
            drop((client, other));
            // Give completion callbacks a turn, not the 120s session lease.
            tokio::task::yield_now().await;
        }
        node.shutdown().await;
        // All adapters/task-owned Raft and state handles must be released.
        let reopened = TcpNode::start(TcpNodeConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".into(),
            control_addr: "127.0.0.1:0".into(),
            data_dir: path,
        })
        .await?;
        reopened.shutdown().await;
        anyhow::Ok(())
    })
    .await?
}
async fn wait_closed(address: SocketAddr) -> anyhow::Result<()> {
    timeout(Duration::from_secs(2), async {
        while TcpStream::connect(address).await.is_ok() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}
#[cfg(target_os = "linux")]
fn owned_listeners() -> anyhow::Result<BTreeSet<u16>> {
    let mut sockets = BTreeSet::new();
    for entry in std::fs::read_dir("/proc/self/fd")? {
        let Ok(target) = std::fs::read_link(entry?.path()) else {
            continue;
        };
        let target = target.to_string_lossy();
        if let Some(inode) = target
            .strip_prefix("socket:[")
            .and_then(|s| s.strip_suffix(']'))
        {
            sockets.insert(inode.to_owned());
        }
    }
    let mut ports = BTreeSet::new();
    for path in ["/proc/self/net/tcp", "/proc/self/net/tcp6"] {
        for line in std::fs::read_to_string(path)?.lines().skip(1) {
            let fields = line.split_whitespace().collect::<Vec<_>>();
            if fields.len() > 9 && fields[3] == "0A" && sockets.contains(fields[9]) {
                ports.insert(u16::from_str_radix(
                    fields[1].rsplit(':').next().unwrap(),
                    16,
                )?);
            }
        }
    }
    Ok(ports)
}
#[cfg(not(target_os = "linux"))]
fn owned_listeners() -> anyhow::Result<BTreeSet<u16>> {
    Ok(BTreeSet::new())
}
fn assert_added_listener(before: &BTreeSet<u16>, address: SocketAddr) -> anyhow::Result<()> {
    #[cfg(target_os = "linux")]
    {
        let mut expected = before.clone();
        expected.insert(address.port());
        assert_eq!(
            owned_listeners()?,
            expected,
            "RPC start must add exactly its advertised listener"
        );
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (before, address);
    Ok(())
}
