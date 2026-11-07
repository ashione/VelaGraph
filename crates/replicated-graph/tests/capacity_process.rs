//! Explicit administrative capacity is durable and fenced across real processes.
use anyhow::ensure;
use d2_raft::{
    send_control, ControlRequest, ControlResponse, D1Error, D1Result, D1WriteRequest,
    D1WriteStatus, WriteOp, EXTENDED_DEDUP_BYTES, MAX_DEDUP_BYTES,
};
use replicated_graph::remote;
use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::Duration,
};
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn explicit_capacity_survives_three_process_restart() -> anyhow::Result<()> {
    tokio::time::timeout(Duration::from_secs(90), scenario()).await?
}
async fn scenario() -> anyhow::Result<()> {
    let reservations = (0..9)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<Vec<_>>>()?;
    let addresses = reservations
        .iter()
        .map(|s| s.local_addr())
        .collect::<std::io::Result<Vec<_>>>()?;
    drop(reservations);
    let members = (0..3)
        .map(|i| format!("{}={}", i + 1, addresses[3 * i]))
        .collect::<Vec<_>>()
        .join(",");
    let dirs = (0..3)
        .map(|_| tempfile::tempdir())
        .collect::<std::io::Result<Vec<_>>>()?;
    let spawn = |initialize: bool| -> anyhow::Result<Vec<Process>> {
        let mut children = Vec::new();
        for i in 0..3 {
            let mut c = Command::new(env!("CARGO_BIN_EXE_graph-node"));
            c.env("TOKIO_WORKER_THREADS", "2").args([
                "--allow-insecure-development",
                "--id",
                &(i + 1).to_string(),
                "--raft",
                &addresses[3 * i].to_string(),
                "--control",
                &addresses[3 * i + 1].to_string(),
                "--graph",
                &addresses[3 * i + 2].to_string(),
                "--shard",
                "7",
                "--epoch",
                "3",
                "--data-dir",
                dirs[i].path().to_str().unwrap(),
            ]);
            if initialize && i == 0 {
                c.args(["--bootstrap", &members]);
            }
            children.push(Process(
                c.stdout(Stdio::null()).stderr(Stdio::inherit()).spawn()?,
            ));
        }
        Ok(children)
    };
    let mut processes = spawn(true)?;
    let request = D1WriteRequest {
        shard: 7,
        epoch: 3,
        request_id: [42; 16],
        expected_sequence: Some(0),
        operations: vec![WriteOp::put(b"k", b"v")],
    };
    for round in 0..2 {
        let leader = tokio::time::timeout(Duration::from_secs(35), async {
            loop {
                for i in 0..3 {
                    if let Ok(ControlResponse::Status(s)) =
                        send_control(&addresses[3 * i + 1].to_string(), ControlRequest::Status)
                            .await
                    {
                        if s.leader == Some((i + 1) as u64)
                            && remote::probe_identified(
                                addresses[3 * i + 2],
                                remote::RemoteIdentity {
                                    group: 0,
                                    node: (i + 1) as u64,
                                },
                                7,
                                Duration::from_millis(300),
                            )
                            .await
                            .is_ok()
                        {
                            return i;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        })
        .await?;
        let control = addresses[3 * leader + 1].to_string();
        let capacity =
            send_control(&control, ControlRequest::D1Capacity { shard: 7, epoch: 3 }).await?;
        match capacity {
            ControlResponse::D1Capacity(Ok(c)) => ensure!(
                c.bytes_limit
                    == if round == 0 {
                        MAX_DEDUP_BYTES
                    } else {
                        EXTENDED_DEDUP_BYTES
                    }
            ),
            other => anyhow::bail!("unexpected capacity: {other:?}"),
        }
        let write = remote::write(
            addresses[3 * leader + 2],
            request.clone(),
            Duration::from_secs(10),
        )
        .await?;
        ensure!(
            matches!(write,D1Result::Write{sequence:1,status,..} if status==if round==0{D1WriteStatus::Committed}else{D1WriteStatus::Duplicate})
        );
        for _ in 0..2 {
            let result = send_control(
                &control,
                ControlRequest::D1SetCapacity {
                    shard: 7,
                    epoch: 3,
                    expected_bytes: MAX_DEDUP_BYTES,
                    new_bytes: EXTENDED_DEDUP_BYTES,
                },
            )
            .await?;
            ensure!(matches!(
                result,
                ControlResponse::D1Admin(Ok(D1Result::DedupCapacity {
                    bytes_limit: EXTENDED_DEDUP_BYTES
                }))
            ));
        }
        let result = send_control(
            &control,
            ControlRequest::D1SetCapacity {
                shard: 7,
                epoch: 2,
                expected_bytes: MAX_DEDUP_BYTES,
                new_bytes: EXTENDED_DEDUP_BYTES,
            },
        )
        .await?;
        ensure!(matches!(
            result,
            ControlResponse::D1Admin(Err(D1Error::Fenced { .. }))
        ));
        let invalid = send_control(
            &control,
            ControlRequest::D1SetCapacity {
                shard: 7,
                epoch: 3,
                expected_bytes: EXTENDED_DEDUP_BYTES,
                new_bytes: EXTENDED_DEDUP_BYTES + 1,
            },
        )
        .await?;
        ensure!(matches!(
            invalid,
            ControlResponse::D1Admin(Err(D1Error::InvalidDedupCapacity))
        ));
        if round == 0 {
            drop(processes);
            processes = spawn(false)?;
        }
    }
    drop(processes);
    Ok(())
}
