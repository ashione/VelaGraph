//! Development-only plaintext remote graph node. Bind only on a trusted network.
//!
//! The process exposes a Raft RPC port and a small control port. The control
//! port is only a test/driver convenience; it is not a production client API.

use std::collections::BTreeMap;
use std::io::Write;

use anyhow::{bail, Context};
use d2_raft::{D1Command, D1Result, LogCommand, TcpNode, TcpNodeConfig};
use replicated_graph::remote::GraphService;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut id = None;
    let mut raft_addr = None;
    let mut control_addr = None;
    let mut data_dir = None;
    let mut bootstrap = None;
    let mut graph_addr = None;
    let mut shard = None;
    let mut epoch = None;
    let mut trusted = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--allow-insecure-development" => trusted = true,
            "--graph" => graph_addr = Some(args.next().context("--graph needs a value")?),
            "--shard" => shard = Some(args.next().context("--shard needs a value")?),
            "--epoch" => epoch = Some(args.next().context("--epoch needs a value")?),
            "--id" => id = Some(args.next().context("--id needs a value")?),
            "--raft" => raft_addr = Some(args.next().context("--raft needs a value")?),
            "--control" => control_addr = Some(args.next().context("--control needs a value")?),
            "--data-dir" => data_dir = Some(args.next().context("--data-dir needs a value")?),
            "--bootstrap" => bootstrap = Some(args.next().context("--bootstrap needs a value")?),
            other => bail!("unknown argument: {other}"),
        }
    }
    anyhow::ensure!(
        trusted,
        "plaintext ports require --allow-insecure-development; use only a trusted isolated network"
    );
    let graph_addr = graph_addr.context("--graph is required")?.parse()?;
    let shard: u64 = shard.context("--shard is required")?.parse()?;
    let epoch: u64 = epoch.context("--epoch is required")?.parse()?;
    anyhow::ensure!(epoch > 0, "epoch must be positive");
    let id: u64 = id.context("--id is required")?.parse()?;
    let raft_addr = raft_addr.context("--raft is required")?;
    let control_addr = control_addr.context("--control is required")?;
    let data_dir = data_dir.context("--data-dir is required")?;

    let node = TcpNode::start(TcpNodeConfig {
        id,
        raft_addr,
        control_addr,
        data_dir: data_dir.into(),
    })
    .await?;

    if let Some(spec) = bootstrap {
        let members = parse_members(&spec)?;
        node.bootstrap(members).await?;
    }

    // Initial activation is an explicit startup administrator action. Existing
    // ownership is checked, never overwritten. Every node may attempt the same
    // immutable activation; only a leader can commit it.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some((actual_shard, actual_epoch, _)) = node.state_machine.d1_metadata().await {
                anyhow::ensure!(
                    actual_shard == shard && actual_epoch == epoch,
                    "stored ownership differs from startup configuration"
                );
                return anyhow::Ok(());
            }
            if node.raft.ensure_linearizable().await.is_ok() {
                if let Ok(reply) = node
                    .raft
                    .client_write(LogCommand::D1V1(D1Command::Activate { shard, epoch }))
                    .await
                {
                    anyhow::ensure!(
                        reply.data.d1 == Some(Ok(D1Result::Activated { shard, epoch })),
                        "D1 activation rejected"
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await??;
    let service = GraphService::start(&node, graph_addr, shard, Duration::from_secs(3)).await?;
    eprintln!("WARNING: plaintext unauthenticated development service; trusted network only");
    println!("graph={}", service.local_addr());
    println!(
        "d2-node id={} raft={} control={}",
        node.id, node.raft_addr, node.control_addr
    );
    std::io::stdout().flush()?;

    // Run until the parent kills this process or SIGINT.
    tokio::signal::ctrl_c().await?;
    service.shutdown().await;
    node.shutdown().await;
    Ok(())
}

fn parse_members(spec: &str) -> anyhow::Result<BTreeMap<u64, String>> {
    let mut members = BTreeMap::new();
    for item in spec.split(',') {
        let (id, addr) = item
            .split_once('=')
            .with_context(|| format!("bootstrap member must be id=addr: {item}"))?;
        members.insert(id.parse()?, addr.to_owned());
    }
    Ok(members)
}
