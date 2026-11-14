//! Fresh-only loopback managed service demonstration. Not a restore/bootstrap
//! command for existing deployments; each shard has only one voter.
use anyhow::{anyhow, bail, Context};
use d2_raft::{
    managed_rpc::{Identity, Role, Server},
    participant_writes::Authority,
    transaction_decisions::{Cohort, Command as DecisionCommand, Journal, Member},
    versioned_reads::Target,
    Cluster, D1Command, LogCommand, NodeHandle,
};
use replicated_graph::{managed_gateway::{Binding, Mapping}, remote::GraphService};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap, fs, io::{Read, Write}, net::SocketAddr,
    path::{Path, PathBuf}, process::{Child, Command, Stdio}, sync::Arc,
    time::Duration,
};
const BUDGET: Duration = Duration::from_secs(30);
const STOP_BUDGET: Duration = Duration::from_secs(5);
const MAX_ANNOUNCE: u64 = 64 * 1024;
fn authority() -> Authority {
    Authority {
        shard: 9,
        mode: [9; 16],
    }
}
fn cohort() -> Cohort {
    Cohort {
        profile: Default::default(),
        coordinator_shard: 9,
        coordinator_epoch: 1,
        members: [7, 8]
            .map(|shard| Member {
                shard,
                epoch: 1,
                mode: [shard as u8; 16],
            })
            .to_vec(),
    }
}
fn target(shard: u64) -> Target {
    Target {
        shard,
        epoch: 1,
        mode: [shard as u8; 16],
    }
}
fn identity(shard: u64) -> Identity {
    Identity {
        group: shard,
        node: 1,
        target: target(shard),
        role: if shard == 9 {
            Role::Coordinator
        } else {
            Role::Participant
        },
        authority: authority(),
        cohort: cohort(),
    }
}
fn binding() -> Binding {
    Binding {
        authority: authority(),
        cohort: cohort(),
        mapping: Mapping::PartitionEqualsShardV1,
    }
}
async fn apply(node: &NodeHandle, command: D1Command) -> anyhow::Result<()> {
    node.raft
        .client_write(LogCommand::D1V1(command))
        .await?
        .data
        .d1
        .context("missing D1 reply")?
        .map_err(|e| anyhow!("activation: {e:?}"))?;
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct NodeReady { pid: u32, control: SocketAddr, graph: Option<SocketAddr> }

async fn signal() -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { r=tokio::signal::ctrl_c()=>r?, _=term.recv()=>{} }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
async fn node(directory: PathBuf, shard: u64) -> anyhow::Result<()> {
    anyhow::ensure!([7,8,9].contains(&shard), "demo shard must be 7, 8 or 9");
    fs::create_dir(&directory).context("node directory must not already exist; no restore")?;
    let cluster = Cluster::new().in_group(shard);
    let node = Arc::new(cluster.start_durable(1, directory.join("data")).await?);
    node.raft
        .initialize(BTreeMap::from([(1, openraft::BasicNode::default())]))
        .await?;
    tokio::time::timeout(BUDGET, async {
        while node.raft.ensure_linearizable().await.is_err() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await?;
    apply(&node, D1Command::Activate { shard, epoch: 1 }).await?;
    apply(
        &node,
        D1Command::EnableManagedWrites {
            shard,
            epoch: 1,
            expected_sequence: 0,
            mode_id: target(shard).mode,
        },
    )
    .await?;
    if shard == 9 {
        apply(
            &node,
            D1Command::EnableCoordinator {
                shard,
                epoch: 1,
                mode_id: target(shard).mode,
            },
        )
        .await?;
        apply(
            &node,
            D1Command::Coordinator {
                shard,
                epoch: 1,
                mode_id: target(shard).mode,
                command: DecisionCommand::BindCohort { cohort: cohort() },
            },
        )
        .await?;
    } else {
        apply(
            &node,
            D1Command::EnableParticipant {
                shard,
                epoch: 1,
                mode_id: target(shard).mode,
                authority: authority(),
            },
        )
        .await?;
        let mut journal = Journal::new(authority().mode).map_err(|e| anyhow!("{e:?}"))?;
        journal
            .apply(DecisionCommand::BindCohort { cohort: cohort() })
            .map_err(|e| anyhow!("{e:?}"))?;
        apply(
            &node,
            D1Command::EnableVersionedReads {
                shard,
                epoch: 1,
                mode_id: target(shard).mode,
                journal,
            },
        )
        .await?;
    }

    let managed = Server::bind(node.clone(), identity(shard), "127.0.0.1:0".parse()?, BUDGET).await?;
    let graph = if shard != 9 {
        Some(GraphService::start_managed(&node, "127.0.0.1:0".parse()?, shard, shard, BUDGET).await?)
    } else { None };
    let ready = NodeReady { pid:std::process::id(), control:managed.local_addr(), graph:graph.as_ref().map(GraphService::local_addr) };
    fs::write(directory.join("ready.tmp"), serde_json::to_vec(&ready)?)?;
    fs::rename(directory.join("ready.tmp"),directory.join("ready.json"))?;
    let result = signal().await;
    if let Some(graph)=graph { graph.shutdown().await; }
    managed.shutdown().await;
    cluster.shutdown(&node).await;
    result
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() { let _=self.0.kill(); }
        let _=self.0.wait();
    }
}
impl Process {
    async fn stop(&mut self) -> anyhow::Result<()> {
        if self.0.try_wait()?.is_some() { return Ok(()); }
        #[cfg(target_os="linux")]
        rustix::process::kill_process(
            rustix::process::Pid::from_raw(self.0.id() as i32).context("child pid")?,
            rustix::process::Signal::TERM,
        )?;
        #[cfg(not(target_os="linux"))]
        self.0.kill()?;
        tokio::time::timeout(STOP_BUDGET, async {
            loop {
                if let Some(status)=self.0.try_wait()? {
                    #[cfg(target_os="linux")]
                    anyhow::ensure!(status.success(),"child {} exit {status}",self.0.id());
                    #[cfg(not(target_os="linux"))]
                    let _=status;
                    return Ok::<_,anyhow::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }).await.context("child shutdown deadline; remaining child will be killed/reaped")?
    }
}
fn read_json(path: &Path) -> anyhow::Result<Option<Value>> {
    let file=match fs::File::open(path) {
        Ok(file)=>file,
        Err(e) if e.kind()==std::io::ErrorKind::NotFound=>return Ok(None),
        Err(e)=>return Err(e.into()),
    };
    let mut bytes=Vec::new(); file.take(MAX_ANNOUNCE+1).read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() as u64<=MAX_ANNOUNCE,"announcement too large");
    if bytes.is_empty() { return Ok(None); }
    // Service writes a single newline-terminated JSON announcement. A partial
    // file is not a readiness signal.
    if !bytes.ends_with(b"\n") && path.file_name().is_some_and(|p|p=="service.jsonl") { return Ok(None); }
    Ok(Some(serde_json::from_slice(&bytes)?))
}
async fn wait_ready(process: &mut Process,path: &Path) -> anyhow::Result<Value> {
    tokio::time::timeout(BUDGET,async {
        loop {
            anyhow::ensure!(process.0.try_wait()?.is_none(),"child exited before readiness");
            if let Some(value)=read_json(path)? {
                anyhow::ensure!(value["pid"].as_u64()==Some(process.0.id() as u64),"stale readiness");
                return Ok::<_,anyhow::Error>(value);
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.context("startup deadline")?
}
struct Options { data:PathBuf, service:PathBuf, http:SocketAddr, grpc:SocketAddr }
fn options(mut args:impl Iterator<Item=String>) -> anyhow::Result<Options> {
    let (mut data,mut service,mut http,mut grpc)=(None,None,None,None);
    let mut insecure=false;
    while let Some(arg)=args.next() {
        match arg.as_str() {
            "--data-dir" if data.is_none()=>data=Some(PathBuf::from(args.next().context("--data-dir needs path")?)),
            "--service-bin" if service.is_none()=>service=Some(PathBuf::from(args.next().context("--service-bin needs path")?)),
            "--http" if http.is_none()=>http=Some(args.next().context("--http needs address")?.parse::<SocketAddr>()?),
            "--grpc" if grpc.is_none()=>grpc=Some(args.next().context("--grpc needs address")?.parse::<SocketAddr>()?),
            "--allow-insecure-development" if !insecure=>insecure=true,
            _=>bail!("unknown or repeated argument: {arg}"),
        }
    }
    anyhow::ensure!(insecure,"requires --allow-insecure-development; loopback plaintext demo only");
    let http=http.unwrap_or("127.0.0.1:0".parse()?);
    let grpc=grpc.unwrap_or("127.0.0.1:0".parse()?);
    anyhow::ensure!(http.ip().is_loopback() && grpc.ip().is_loopback(),"listeners must be loopback");
    let service=fs::canonicalize(service.context("--service-bin required")?).context("graph-service binary")?;
    let data=data.context("--data-dir required")?;
    Ok(Options{data,service,http,grpc})
}
async fn parent(options:Options, cancel:tokio_util::sync::CancellationToken) -> anyhow::Result<()> {
    fs::create_dir(&options.data).context("data directory must not exist; this demo cannot restore or overwrite")?;
    let directory=fs::canonicalize(&options.data)?;
    let executable=std::env::current_exe()?;
    let mut processes=Vec::new();
    let mut ready=Vec::new();
    for shard in [7,8,9] {
        let child_dir=directory.join(format!("node-{shard}"));
        let child=Command::new(&executable).arg("--node").arg(&child_dir).arg(shard.to_string())
            .stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::inherit()).spawn()?;
        processes.push(Process(child));
        let ready_path=child_dir.join("ready.json");
        let value=tokio::select! { r=wait_ready(processes.last_mut().context("spawned node")?,&ready_path)=>r?, _=cancel.cancelled()=>bail!("startup cancelled") };
        ready.push(serde_json::from_value::<NodeReady>(value)?);
    }
    let config=json!({
        "binding":binding(),
        "coordinator":{"endpoint":ready[2].control,"identity":identity(9)},
        "members":[
            {"endpoint":ready[0].control,"identity":identity(7),"graph_endpoint":ready[0].graph.context("shard7 graph")?},
            {"endpoint":ready[1].control,"identity":identity(8),"graph_endpoint":ready[1].graph.context("shard8 graph")?}
        ]
    });
    let config_path=directory.join("service-config.json");
    fs::write(&config_path,serde_json::to_vec_pretty(&config)?)?;
    let output=directory.join("service.jsonl");
    let child=Command::new(&options.service).arg("--config").arg(&config_path)
        .args(["--http",&options.http.to_string(),"--grpc",&options.grpc.to_string(),
            "--allow-insecure-development","--timeout-ms","15000"])
        .stdin(Stdio::null()).stdout(Stdio::from(fs::File::create(&output)?)).stderr(Stdio::inherit()).spawn()?;
    let mut service=Process(child);
    let announcement=tokio::select! { r=wait_ready(&mut service,&output)=>r?, _=cancel.cancelled()=>bail!("startup cancelled") };
    anyhow::ensure!(announcement["restore_supported"]==false && announcement["production_ready"]==false,"unexpected service scope");
    println!("{}",json!({
        "pid":std::process::id(),"http":announcement["http"],"grpc":announcement["grpc"],
        "node_pids":ready.iter().map(|n|n.pid).collect::<Vec<_>>(),"service_pid":service.0.id(),
        "binding":binding(),"fresh_only":true,"restore_supported":false,"production_ready":false
    }));
    std::io::stdout().flush()?;
    eprintln!("Fresh single-voter demo serving; Ctrl-C to stop. Data retained at {} (not reopenable by this demo).",directory.display());
    let serving=async {
        loop {
            anyhow::ensure!(service.0.try_wait()?.is_none(),"graph-service exited unexpectedly");
            for process in &mut processes {
                anyhow::ensure!(process.0.try_wait()?.is_none(),"node exited unexpectedly");
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        #[allow(unreachable_code)]
        Ok::<(),anyhow::Error>(())
    };
    let result=tokio::select! { _=cancel.cancelled()=>Ok(()), r=serving=>r };
    // Stop accepting public requests before stopping the managed participants.
    let mut cleanup=service.stop().await;
    for process in processes.iter_mut().rev() {
        let stopped=process.stop().await;
        if cleanup.is_ok() { cleanup=stopped; }
    }
    result.and(cleanup)
}
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args=std::env::args().skip(1);
    let first=args.next();
    if first.as_deref()==Some("--node") {
        let directory=PathBuf::from(args.next().context("--node directory")?);
        let shard=args.next().context("--node shard")?.parse::<u64>()?;
        anyhow::ensure!(args.next().is_none(),"extra node arguments");
        node(directory,shard).await
    } else {
        let options=options(first.into_iter().chain(args))?;
        let cancel=tokio_util::sync::CancellationToken::new();
        let notify=cancel.clone();
        let signals=tokio::spawn(async move { let result=signal().await; notify.cancel(); result });
        let result=parent(options,cancel).await;
        signals.abort();
        result
    }
}
