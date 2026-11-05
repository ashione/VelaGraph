//! Verify the prototype across three real OS processes and TCP sockets.
//!
//! The processes are started from the `d2-node` binary. No node shares memory
//! with another node or with the test driver.

use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{bail, Context};
use d2_raft::{send_control, ControlRequest, ControlResponse, NodeStatus};
use tokio::net::TcpListener;
use tokio::time::{sleep, timeout, Instant};

#[derive(Debug)]
struct NodeProcess {
    child: Child,
}

impl NodeProcess {
    fn spawn(id: u64, raft: &str, control: &str) -> anyhow::Result<Self> {
        let child = Command::new(env!("CARGO_BIN_EXE_d2-node"))
            .args([
                "--id",
                &id.to_string(),
                "--raft",
                raft,
                "--control",
                control,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("spawn d2-node {id}"))?;
        Ok(Self { child })
    }

    fn kill(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        self.kill();
    }
}

async fn free_ports() -> anyhow::Result<(u16, u16)> {
    let raft = TcpListener::bind("127.0.0.1:0").await?;
    let control = TcpListener::bind("127.0.0.1:0").await?;
    Ok((raft.local_addr()?.port(), control.local_addr()?.port()))
}

async fn wait_ready(control: &str, timeout_ms: u64) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        if send_control(control, ControlRequest::Status).await.is_ok() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("control port {control} did not become ready");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn status(control: &str) -> anyhow::Result<NodeStatus> {
    match send_control(control, ControlRequest::Status).await? {
        ControlResponse::Status(status) => Ok(status),
        other => bail!("unexpected status response: {other:?}"),
    }
}

async fn leader_of(controls: &BTreeMap<u64, String>, timeout_ms: u64) -> anyhow::Result<u64> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        for (id, control) in controls {
            if let Ok(status) = status(control).await {
                if status.leader == Some(*id) {
                    return Ok(*id);
                }
            }
        }
        if Instant::now() >= deadline {
            bail!("no process reported itself as leader");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn write(control: &str, key: &str, value: &str) -> anyhow::Result<u64> {
    match send_control(
        control,
        ControlRequest::Write {
            key: key.to_owned(),
            value: value.to_owned(),
        },
    )
    .await?
    {
        ControlResponse::Index(index) => Ok(index),
        other => bail!("unexpected write response: {other:?}"),
    }
}

async fn wait_value(
    control: &str,
    key: &str,
    expected: &str,
    timeout_ms: u64,
) -> anyhow::Result<()> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        match send_control(
            control,
            ControlRequest::Get {
                key: key.to_owned(),
            },
        )
        .await?
        {
            ControlResponse::Value(Some(value)) if value == expected => return Ok(()),
            ControlResponse::Value(_) | ControlResponse::Err(_) => {}
            other => bail!("unexpected get response: {other:?}"),
        }
        if Instant::now() >= deadline {
            bail!("{control} did not expose {key}={expected}");
        }
        sleep(Duration::from_millis(50)).await;
    }
}

async fn scenario() -> anyhow::Result<()> {
    let mut nodes = BTreeMap::new();
    let mut raft_addrs = BTreeMap::new();
    let mut control_addrs = BTreeMap::new();
    for id in 1..=3 {
        let (raft_port, control_port) = free_ports().await?;
        let raft = format!("127.0.0.1:{raft_port}");
        let control = format!("127.0.0.1:{control_port}");
        nodes.insert(id, NodeProcess::spawn(id, &raft, &control)?);
        raft_addrs.insert(id, raft);
        control_addrs.insert(id, control);
    }
    for control in control_addrs.values() {
        wait_ready(control, 10_000).await?;
    }

    let members: BTreeMap<u64, String> = raft_addrs.clone();
    send_control(&control_addrs[&1], ControlRequest::Initialize { members })
        .await
        .context("initialize all three nodes")?;

    let leader = leader_of(&control_addrs, 10_000).await?;
    for i in 0..50 {
        write(
            &control_addrs[&leader],
            &format!("k{i:02}"),
            &format!("v{i:02}"),
        )
        .await?;
    }
    for control in control_addrs.values() {
        for i in [0_usize, 49] {
            wait_value(control, &format!("k{i:02}"), &format!("v{i:02}"), 10_000).await?;
        }
    }

    // Kill the leader process; the two remaining processes must elect a leader.
    let mut old_leader = nodes.remove(&leader).context("leader process")?;
    old_leader.kill();
    let remaining: BTreeMap<u64, String> = control_addrs
        .iter()
        .filter(|(id, _)| **id != leader)
        .map(|(id, addr)| (*id, addr.clone()))
        .collect();
    let new_leader = leader_of(&remaining, 10_000).await?;
    write(&control_addrs[&new_leader], "after", "failover").await?;
    for control in remaining.values() {
        wait_value(control, "after", "failover", 10_000).await?;
    }

    // Restart the old leader with an empty in-memory log; the new leader must
    // bring it back through normal replication.
    let restarted = NodeProcess::spawn(leader, &raft_addrs[&leader], &control_addrs[&leader])?;
    nodes.insert(leader, restarted);
    wait_ready(&control_addrs[&leader], 10_000).await?;
    wait_value(&control_addrs[&leader], "after", "failover", 10_000).await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_processes_replicate_fail_over_and_restart() {
    let result = timeout(Duration::from_secs(60), scenario()).await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => panic!("multi-process test failed: {error:#}"),
        Err(_) => panic!("multi-process test timed out"),
    }
}
