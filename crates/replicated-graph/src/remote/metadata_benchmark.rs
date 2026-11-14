//! Ignored, same-node transport diagnostic. No production instrumentation.
use super::*;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use velagraph::{ReadView, StorageNode};
const GROUP: u64 = 0x4d455441;
static METADATA: AtomicUsize = AtomicUsize::new(0);
pub(super) fn record(group: u64) {
    if group == GROUP {
        METADATA.fetch_add(1, Ordering::Relaxed);
    }
}
#[derive(Clone, Copy)]
pub(super) enum Phase {
    ServerDecode,
    ServerLogicalRequest,
    ServerExecute,
    ServerLogicalResponse,
    ServerEncode,
    ClientPrepare,
    ClientAwait,
    ClientFinish,
    ClientTotal,
}
const PHASES: [&str; 9] = [
    "server_decode",
    "server_logical_request",
    "server_execute",
    "server_logical_response",
    "server_encode",
    "client_prepare",
    "client_await",
    "client_finish",
    "client_total",
];
static NANOS: [AtomicU64; 9] = [const { AtomicU64::new(0) }; 9];
static COUNTS: [AtomicU64; 9] = [const { AtomicU64::new(0) }; 9];
pub(super) fn phase(group: u64, phase: Phase, start: Instant) {
    if group == GROUP {
        let index = phase as usize;
        NANOS[index].fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        COUNTS[index].fetch_add(1, Ordering::Relaxed);
    }
}
fn take_phases() -> serde_json::Value {
    let mut result = serde_json::Map::new();
    for (i, name) in PHASES.iter().enumerate() {
        let nanos = NANOS[i].swap(0, Ordering::SeqCst);
        let count = COUNTS[i].swap(0, Ordering::SeqCst);
        result.insert((*name).into(),serde_json::json!({"calls":count,"mean_us":if count>0{nanos as f64/count as f64/1000.}else{0.}}));
    }
    result.into()
}
const BUDGET: Duration = Duration::from_secs(60);
fn emit(lane: &str, round: usize, elapsed: Duration, mut samples: Vec<u64>, expected: usize) {
    let observed = METADATA.swap(0, Ordering::SeqCst);
    assert_eq!(observed, expected, "every get must check Metadata");
    samples.sort_unstable();
    let p = |n: usize| samples[(samples.len() * n).div_ceil(100).saturating_sub(1)];
    println!(
        "{}",
        serde_json::json!({"benchmark":"metadata_transport","lane":lane,"round":round,"n":samples.len(),"metadata_calls":observed,"total_ms":elapsed.as_secs_f64()*1000.,"p50_us":p(50) as f64/1000.,"p95_us":p(95) as f64/1000.,"p99_us":p(99) as f64/1000.,"runtime_workers":2,"debug_assertions":cfg!(debug_assertions),"phases":take_phases()})
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual same-node typed convenience/gRPC/adapter latency diagnostic"]
async fn metadata_transport_microbenchmark() {
    let dir = tempfile::tempdir().unwrap();
    let node = d2_raft::TcpNode::start(d2_raft::TcpNodeConfig {
        id: 1,
        raft_addr: "127.0.0.1:0".into(),
        control_addr: "127.0.0.1:0".into(),
        data_dir: dir.path().into(),
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
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    node.raft
        .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 1 }))
        .await
        .unwrap();
    node.raft
        .client_write(LogCommand::D1V1(D1Command::Write(D1WriteRequest {
            shard: 7,
            epoch: 1,
            request_id: [1; 16],
            expected_sequence: Some(0),
            operations: vec![d2_raft::WriteOp::Put {
                key: vec![1],
                value: vec![2],
            }],
        })))
        .await
        .unwrap();
    let convenience =
        GraphService::start_identified(&node, "127.0.0.1:0".parse().unwrap(), GROUP, 7, BUDGET)
            .await
            .unwrap();
    let grpc = grpc::GrpcSnapshotService::start(&convenience, "127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let id = RemoteIdentity {
        group: GROUP,
        node: 1,
    };
    let cold = Instant::now();
    let mut client = operations_grpc::Client::connect(grpc.local_addr(), id, BUDGET)
        .await
        .unwrap();
    println!(
        "{}",
        serde_json::json!({"benchmark":"metadata_transport","lane":"grpc_cold_connect","total_ms":cold.elapsed().as_secs_f64()*1000.})
    );
    let stream = super::metadata_stream_client::Client::new(grpc.local_addr(), id);
    let adapter =
        crate::RemoteStorageNode::new_identified(grpc.local_addr(), GROUP, 1, 7, BUDGET).unwrap();
    let view = ReadView::from_entries(
        7,
        1,
        1,
        std::collections::BTreeMap::from([(vec![1], vec![2])]),
    )
    .unwrap();
    // Warm every path before timed sampling. Do not include connection/setup in warm RPC latency.
    for _ in 0..100 {
        metadata_identified(convenience.local_addr(), id, 7, BUDGET)
            .await
            .unwrap();
        client.metadata(7).await.unwrap();
        stream
            .metadata(7, tokio::time::Instant::now() + BUDGET)
            .await
            .unwrap();
    }
    let a = adapter.clone();
    let v = view.clone();
    crate::run_blocking(move || {
        for _ in 0..100 {
            assert_eq!(a.get(&[1], &v).unwrap(), Some(vec![2]));
        }
    })
    .await
    .unwrap();
    METADATA.store(0, Ordering::SeqCst);
    take_phases();
    for round in 0..3 {
        for offset in 0..4 {
            let lane = (round + offset) % 4;
            let started = Instant::now();
            let mut samples = Vec::with_capacity(1000);
            match lane {
                0 => {
                    for _ in 0..1000 {
                        let t = Instant::now();
                        assert_eq!(
                            metadata_identified(convenience.local_addr(), id, 7, BUDGET)
                                .await
                                .unwrap(),
                            (7, 1, 1)
                        );
                        samples.push(t.elapsed().as_nanos() as u64);
                    }
                }
                1 => {
                    for _ in 0..1000 {
                        let t = Instant::now();
                        assert_eq!(client.metadata(7).await.unwrap(), (7, 1, 1));
                        samples.push(t.elapsed().as_nanos() as u64);
                    }
                }
                2 => {
                    for _ in 0..1000 {
                        let t = Instant::now();
                        assert_eq!(
                            stream
                                .metadata(7, tokio::time::Instant::now() + BUDGET)
                                .await
                                .unwrap(),
                            (7, 1, 1)
                        );
                        samples.push(t.elapsed().as_nanos() as u64);
                    }
                }
                _ => {
                    let a = adapter.clone();
                    let v = view.clone();
                    samples = crate::run_blocking(move || {
                        let mut times = Vec::with_capacity(1000);
                        for _ in 0..1000 {
                            let t = Instant::now();
                            assert_eq!(a.get(&[1], &v).unwrap(), Some(vec![2]));
                            times.push(t.elapsed().as_nanos() as u64);
                        }
                        times
                    })
                    .await
                    .unwrap();
                }
            }
            emit(
                [
                    "grpc_convenience",
                    "grpc_reused",
                    "grpc_stream",
                    "grpc_adapter_get",
                ][lane],
                round,
                started.elapsed(),
                samples,
                1000,
            );
        }
    }
    for lane in ["grpc_convenience_snapshot", "grpc_snapshot"] {
        let t = Instant::now();
        let mut times = vec![];
        for _ in 0..10 {
            let s = Instant::now();
            let snap = if lane == "grpc_convenience_snapshot" {
                paged_snapshot_identified(convenience.local_addr(), id, 7, BUDGET)
                    .await
                    .unwrap()
            } else {
                grpc::paged_snapshot(grpc.local_addr(), id, 7, BUDGET)
                    .await
                    .unwrap()
            };
            assert_eq!(snap.entries, vec![(vec![1], vec![2])]);
            times.push(s.elapsed().as_nanos() as u64);
        }
        emit(lane, 0, t.elapsed(), times, 0);
    }
    drop(adapter);
    drop(stream);
    drop(client);
    grpc.shutdown().await;
    convenience.shutdown().await;
    node.shutdown().await;
}
