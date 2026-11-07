//! Small driver for a running `d2-node` control port.
//!
//! It is deliberately a test/operations helper, not a production SQL client.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{bail, Context};
use d2_raft::{send_control, ControlRequest, ControlResponse};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        usage();
        bail!("expected <control-addr> <command>");
    }
    let control = &args[0];
    let request = match args[1].as_str() {
        "status" => ControlRequest::Status,
        "d1-capacity" => {
            if args.len() != 4 {
                bail!("d1-capacity needs <shard> <epoch>");
            }
            ControlRequest::D1Capacity {
                shard: args[2].parse()?,
                epoch: args[3].parse()?,
            }
        }
        "d1-set-capacity" => {
            if args.len() != 6 {
                bail!("d1-set-capacity needs <shard> <epoch> <expected-bytes> <new-bytes>");
            }
            ControlRequest::D1SetCapacity {
                shard: args[2].parse()?,
                epoch: args[3].parse()?,
                expected_bytes: args[4].parse()?,
                new_bytes: args[5].parse()?,
            }
        }
        "init" => {
            let mut members = BTreeMap::new();
            for spec in &args[2..] {
                let (id, addr) = spec
                    .split_once('=')
                    .with_context(|| format!("member must be id=addr: {spec}"))?;
                members.insert(id.parse()?, addr.to_owned());
            }
            ControlRequest::Initialize { members }
        }
        "add-learner" => {
            if args.len() < 4 {
                bail!("add-learner needs <id> <raft-addr> [blocking|nonblocking]");
            }
            ControlRequest::AddLearner {
                id: args[2].parse()?,
                addr: args[3].clone(),
                blocking: args
                    .get(4)
                    .map(|mode| mode != "nonblocking")
                    .unwrap_or(true),
            }
        }
        "membership" => {
            let ids = args
                .get(2)
                .context("membership needs a comma-separated id list")?
                .split(',')
                .map(|id| id.parse())
                .collect::<Result<BTreeSet<_>, _>>()?;
            ControlRequest::ChangeMembership { ids }
        }
        "write" => {
            if args.len() < 4 {
                bail!("write needs <key> <value>");
            }
            ControlRequest::Write {
                key: args[2].clone(),
                value: args[3].clone(),
            }
        }
        "get" | "read" => {
            let key = args.get(2).context("get/read needs a key")?.clone();
            if args[1] == "get" {
                ControlRequest::Get { key }
            } else {
                ControlRequest::Read { key }
            }
        }
        other => bail!("unknown command {other}; run without arguments for usage"),
    };

    let response = send_control(control, request).await?;
    match response {
        ControlResponse::Err(error) => bail!("{error}"),
        ControlResponse::D1Admin(Err(error)) | ControlResponse::D1Capacity(Err(error)) => {
            bail!("D1 rejected: {error:?}")
        }
        other => println!("{}", serde_json::to_string_pretty(&other)?),
    }
    Ok(())
}

fn usage() {
    eprintln!(
        "usage:\n  d2-ctl <control-addr> status\n  d2-ctl <control-addr> d1-capacity <shard> <epoch>\n  d2-ctl <control-addr> d1-set-capacity <shard> <epoch> <expected-bytes> <new-bytes>\n  d2-ctl <control-addr> init <id=raft-addr>...\n  d2-ctl <control-addr> add-learner <id> <raft-addr> [blocking|nonblocking]\n  d2-ctl <control-addr> membership <id,id,...>\n  d2-ctl <control-addr> write <key> <value>\n  d2-ctl <control-addr> get <key>\n  d2-ctl <control-addr> read <key>"
    );
}
