//! One process of the D2 multi-process Raft prototype.
//!
//! The process exposes a Raft RPC port and a small control port. The control
//! port is only a test/driver convenience; it is not a production client API.

use std::collections::BTreeMap;
use std::io::Write;

use anyhow::{bail, Context};
use d2_raft::{TcpNode, TcpNodeConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut id = None;
    let mut raft_addr = None;
    let mut control_addr = None;
    let mut bootstrap = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--id" => id = Some(args.next().context("--id needs a value")?),
            "--raft" => raft_addr = Some(args.next().context("--raft needs a value")?),
            "--control" => control_addr = Some(args.next().context("--control needs a value")?),
            "--bootstrap" => bootstrap = Some(args.next().context("--bootstrap needs a value")?),
            other => bail!("unknown argument: {other}"),
        }
    }
    let id: u64 = id.context("--id is required")?.parse()?;
    let raft_addr = raft_addr.context("--raft is required")?;
    let control_addr = control_addr.context("--control is required")?;

    let node = TcpNode::start(TcpNodeConfig {
        id,
        raft_addr,
        control_addr,
    })
    .await?;

    if let Some(spec) = bootstrap {
        let members = parse_members(&spec)?;
        node.bootstrap(members).await?;
    }

    println!(
        "d2-node id={} raft={} control={}",
        node.id, node.raft_addr, node.control_addr
    );
    std::io::stdout().flush()?;

    // Run until the parent kills this process or SIGINT.
    tokio::signal::ctrl_c().await?;
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
