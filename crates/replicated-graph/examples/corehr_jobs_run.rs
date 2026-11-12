//! Real loopback Raft processes, bounded fixture import, oracle-checked queries.
use anyhow::{ensure, Context};
use replicated_graph::{
    catalog::{Catalog, Placement},
    remote::{
        self,
        distributed::{
            self, CoordinatorService, DistributedRequest, PartitionRoute, StaticCatalog,
        },
        query::{QualifiedElement, QueryCompleteness, QueryFailure, QueryRow},
        RemoteIdentity,
    },
    routing::{
        experimental::{
            AdjacencyCompleteness, ExperimentalRouteError, ObservedPathElement, ObservedRowV3,
        },
        RemoteReplica, RouteError, RouteResolver,
    },
    run_blocking,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command},
    time::{Duration, Instant},
};
#[derive(Clone)]
struct JobCase {
    name: &'static str,
    partition: u32,
    query: String,
    count: Option<u64>,
    error: Option<QueryFailure>,
}
fn level_count(total: u64, fanout: u64, depth: usize) -> u64 {
    let (mut first, mut width) = (1u64, 1u64);
    for _ in 0..depth {
        first = first.saturating_add(width);
        if first > total {
            return 0;
        }
        width = width.saturating_mul(fanout);
    }
    width.min(total - first + 1)
}
fn dedup_key_lower_bound(total: u64) -> u64 {
    let mut seen = 0;
    let mut bound = 0;
    for depth in 1..=32 {
        let width = level_count(total, 8, depth);
        seen += width;
        bound = bound.max(seen + width);
    }
    bound
}
fn job_cases(people: u64, departments: u64) -> Vec<JobCase> {
    let department = people + 1;
    let mut cases = Vec::new();
    for (name, start, query, count) in [
        (
            "ceo_emit32",
            1,
            "g.V(1).repeat(__.in('reports_to')).emit().times(32).count()".into(),
            people - 1,
        ),
        (
            "ceo_exact6",
            1,
            "g.V(1).repeat(__.in('reports_to')).times(6).count()".into(),
            level_count(people, 8, 6),
        ),
        (
            "ceo_exact32",
            1,
            "g.V(1).repeat(__.in('reports_to')).times(32).count()".into(),
            0,
        ),
        (
            "duplicate_ceo_emit32",
            1,
            "g.V(1,1).repeat(__.in('reports_to')).emit().times(32).count()".into(),
            2 * (people - 1),
        ),
        (
            "department_emit32",
            department,
            format!(
                "g.V({department}).repeat(__.in('parent_department')).emit().times(32).count()"
            ),
            departments - 1,
        ),
        (
            "department_exact6",
            department,
            format!("g.V({department}).repeat(__.in('parent_department')).times(6).count()"),
            level_count(departments, 5, 6),
        ),
    ] {
        cases.push(JobCase {
            name,
            partition: 1 + (start % 3) as u32,
            query,
            count: Some(count),
            error: None,
        });
    }
    for (name, start, query, count) in [
        (
            "ceo_emit32_dedup",
            1,
            "g.V(1).repeat(__.in('reports_to')).emit().times(32).dedup().count()".into(),
            people - 1,
        ),
        (
            "duplicate_ceo_emit32_dedup",
            1,
            "g.V(1,1).repeat(__.in('reports_to')).emit().times(32).dedup().count()".into(),
            people - 1,
        ),
        (
            "department_emit32_dedup",
            department,
            format!("g.V({department}).repeat(__.in('parent_department')).emit().times(32).dedup().count()"),
            departments - 1,
        ),
    ] {
        cases.push(JobCase {
            name,
            partition: 1 + (start % 3) as u32,
            query,
            count: Some(count),
            error: None,
        });
    }
    cases
}
async fn execute_job(
    endpoint: std::net::SocketAddr,
    case: &JobCase,
    phase: &str,
    sample: usize,
) -> anyhow::Result<Value> {
    use replicated_graph::remote::distributed::jobs::{self, Output, Request};
    let started = Instant::now();
    let mut token_saved = None;
    let mut times = Vec::new();
    let mut completed = false;
    println!(
        "{}",
        json!({"event":"job_begin","name":case.name,"phase":phase,"sample":sample,"query":case.query,"expected_count":case.count,"expected_error":case.error})
    );
    let result=tokio::time::timeout(Duration::from_secs(135),async {
        let mut request=Request::Start{start_partition:case.partition,text:case.query.clone()};
        let mut fixed=None;
        for round in 0..20000u64 {
            let tick=Instant::now();
            let response=jobs::request(endpoint,request,Duration::from_secs(5)).await?;
            times.push(tick.elapsed().as_secs_f64()*1000.);
            let output=match response {
                Ok(v)=>v,
                Err(e)=>{
                    ensure!(case.error==Some(e),"unexpected job failure {e:?}");
                    ensure!(token_saved.is_some() && round>0,"boundary rejection occurred before a job advanced");
                    return Ok::<Value,anyhow::Error>(json!({"event":"job_outcome","name":case.name,"phase":phase,"sample":sample,"expected_rejection":e,"requests":times.len(),"seconds":started.elapsed().as_secs_f64(),"rpc_latency":summary(times.clone()),"partial_count_returned":false}));
                }
            };
            let Output::Progress{token,next_sequence,complete,count,completeness,snapshots}=output else {anyhow::bail!("unexpected Cancelled response")};
            if let Some(t)=token_saved {ensure!(t==token,"token changed");} else {token_saved=Some(token);}
            ensure!(next_sequence==round,"sequence changed");
            ensure!(completeness==QueryCompleteness::Unverified);
            if let Some(stamps)=&fixed {ensure!(*stamps==snapshots,"snapshots changed");} else {ensure!(snapshots.len()==3);fixed=Some(snapshots.clone());}
            ensure!(complete==count.is_some(),"partial count returned");
            println!("{}",json!({"event":"job_progress","name":case.name,"phase":phase,"sample":sample,"sequence":next_sequence,"complete":complete,"count":count,"rpc_ms":times.last(),"elapsed_seconds":started.elapsed().as_secs_f64(),"snapshots":snapshots}));
            if complete {
                ensure!(case.error.is_none() && count==case.count,"unexpected completed count {count:?}");
                let elapsed=started.elapsed().as_secs_f64();
                let replay_started=Instant::now();
                let replay=jobs::request(endpoint,Request::Advance{token,sequence:next_sequence.checked_sub(1).context("completed without advance")?},Duration::from_secs(5)).await??;
                let Output::Progress{token:t,next_sequence:s,complete:c,count:n,completeness:k,snapshots:v}=replay else {anyhow::bail!("replay cancelled")};
                ensure!(t==token && s==next_sequence && c && n==count && k==completeness && v==snapshots,"completed replay changed output");
                completed=true;
                return Ok(json!({"event":"job_outcome","name":case.name,"phase":phase,"sample":sample,"correct":true,"count":count,"requests":times.len(),"seconds":elapsed,"rpc_latency":summary(times.clone()),"fixed_snapshots":snapshots,"partial_count_returned":false,"replay_verified":true,"replay_ms":replay_started.elapsed().as_secs_f64()*1000.}));
            }
            request=Request::Advance{token,sequence:next_sequence};
        }
        anyhow::bail!("driver request ceiling exceeded")
    }).await.map_err(|_|anyhow::anyhow!("135-second driver safety timeout")).and_then(|r|r);
    if let Some(token) = token_saved {
        let cancel =
            jobs::request(endpoint, Request::Cancel { token }, Duration::from_secs(5)).await;
        let unavailable = jobs::request(
            endpoint,
            Request::Advance { token, sequence: 0 },
            Duration::from_secs(5),
        )
        .await;
        if result.is_ok() {
            if completed {
                ensure!(
                    matches!(cancel, Ok(Ok(Output::Cancelled))),
                    "completed job cleanup failed"
                );
            } else {
                ensure!(
                    matches!(cancel, Ok(Err(QueryFailure::Unavailable))),
                    "failed job unexpectedly retained"
                );
            }
            ensure!(
                matches!(unavailable, Ok(Err(QueryFailure::Unavailable))),
                "cancelled/failed job resumed"
            );
        }
        // Neither request nor token is serialized to evidence.
        println!(
            "{}",
            json!({"event":"job_cleanup","name":case.name,"phase":phase,"sample":sample,"cancelled":matches!(cancel,Ok(Ok(Output::Cancelled))),"already_removed":matches!(cancel,Ok(Err(QueryFailure::Unavailable))),"advance_unavailable":matches!(unavailable,Ok(Err(QueryFailure::Unavailable)))})
        );
    }
    match &result {
        Ok(record) => println!("{record}"),
        Err(error) => println!(
            "{}",
            json!({"event":"job_failure","name":case.name,"phase":phase,"sample":sample,"error":format!("{error:#}"),"seconds":started.elapsed().as_secs_f64(),"requests":times.len()})
        ),
    }
    result
}
async fn cancel_active_job(endpoint: std::net::SocketAddr) -> anyhow::Result<()> {
    use replicated_graph::remote::distributed::jobs::{self, Output, Request};
    let start = jobs::request(
        endpoint,
        Request::Start {
            start_partition: 2,
            text: "g.V(1).repeat(__.in('reports_to')).emit().times(32).count()".into(),
        },
        Duration::from_secs(5),
    )
    .await??;
    let Output::Progress {
        token,
        next_sequence,
        complete: false,
        count: None,
        snapshots,
        ..
    } = start
    else {
        anyhow::bail!("unexpected start result")
    };
    let progress = jobs::request(
        endpoint,
        Request::Advance {
            token,
            sequence: next_sequence,
        },
        Duration::from_secs(5),
    )
    .await??;
    let Output::Progress {
        token: t,
        next_sequence: s,
        complete: false,
        count: None,
        snapshots: v,
        ..
    } = progress
    else {
        anyhow::bail!("partial job unexpectedly completed")
    };
    ensure!(t == token && s == next_sequence + 1 && snapshots == v);
    let cancel =
        jobs::request(endpoint, Request::Cancel { token }, Duration::from_secs(5)).await??;
    ensure!(matches!(cancel, Output::Cancelled));
    let after = jobs::request(
        endpoint,
        Request::Advance { token, sequence: s },
        Duration::from_secs(5),
    )
    .await?;
    ensure!(matches!(after, Err(QueryFailure::Unavailable)));
    println!(
        "{}",
        json!({"event":"job_cancel_probe","correct":true,"partial_count_returned":false,"fixed_snapshots":snapshots,"advance_unavailable":true})
    );
    Ok(())
}
#[test]
fn count_job_oracles_follow_breadth_first_tree_formulas() {
    assert_eq!(level_count(200000, 8, 6), 162551);
    assert_eq!(level_count(5000, 5, 6), 1094);
    assert_eq!(level_count(2000, 8, 6), 0);
    assert_eq!(level_count(200000, 8, 32), 0);
    assert_eq!(dedup_key_lower_bound(200000), 362550);
    assert!(dedup_key_lower_bound(2000) < 250000);
    let c = job_cases(200000, 5000);
    assert_eq!(c.len(), 9);
    assert_eq!(c[3].count, Some(399998));
    assert_eq!(c[6].count, Some(199999));
    assert_eq!(c[7].count, Some(199999));
    assert_eq!(c[8].count, Some(4999));
    assert!(c.iter().all(|c| c.error.is_none()));
}

const BUDGET: Duration = Duration::from_secs(60);
struct Group {
    child: Child,
    control: std::net::SocketAddr,
    placement: Placement,
    replica: RemoteReplica,
}
impl Drop for Group {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
impl Group {
    async fn start(bin: &str, root: &Path, g: u64) -> anyhow::Result<Self> {
        let sockets = (0..3)
            .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
            .collect::<std::io::Result<Vec<_>>>()?;
        let a = sockets
            .iter()
            .map(|s| s.local_addr().unwrap())
            .collect::<Vec<_>>();
        drop(sockets);
        let log = File::create(root.join(format!("node-{g}.log")))?;
        let shard = if g == 0 { 100 } else { g };
        let child = Command::new(bin)
            .args([
                "--allow-insecure-development",
                "--group",
                &g.to_string(),
                "--id",
                "1",
                "--raft",
                &a[0].to_string(),
                "--control",
                &a[1].to_string(),
                "--graph",
                &a[2].to_string(),
                "--shard",
                &shard.to_string(),
                "--epoch",
                "1",
                "--data-dir",
                root.join(format!("node-{g}")).to_str().unwrap(),
                "--bootstrap",
                &format!("1={}", a[0]),
            ])
            .stdout(log.try_clone()?)
            .stderr(log)
            .spawn()?;
        let out = Self {
            child,
            control: a[1],
            placement: Placement {
                group: g,
                shard,
                epoch: 1,
                replicas: vec![1],
            },
            replica: RemoteReplica {
                group: g,
                node: 1,
                endpoint: a[2],
            },
        };
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if remote::probe_identified(
                    a[2],
                    RemoteIdentity { group: g, node: 1 },
                    shard,
                    Duration::from_millis(500),
                )
                .await
                .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await?;
        Ok(out)
    }
}
fn read_record(r: &mut BufReader<File>) -> anyhow::Result<Option<(Vec<u8>, Vec<u8>)>> {
    let mut lens = [0u8; 8];
    let n = r.read(&mut lens[..1])?;
    if n == 0 {
        return Ok(None);
    }
    r.read_exact(&mut lens[1..])?;
    let k = u32::from_le_bytes(lens[..4].try_into()?) as usize;
    let v = u32::from_le_bytes(lens[4..].try_into()?) as usize;
    ensure!(k < 1024 && v < 1024 * 1024, "invalid fixture length");
    let mut key = vec![0; k];
    let mut val = vec![0; v];
    r.read_exact(&mut key)?;
    r.read_exact(&mut val)?;
    Ok(Some((key, val)))
}
fn summary(mut xs: Vec<f64>) -> Value {
    xs.sort_by(f64::total_cmp);
    json!({"n":xs.len(),"p50_ms":xs[xs.len()/2],"p95_ms":xs[(xs.len()*95).div_ceil(100)-1],"min_ms":xs[0],"max_ms":xs[xs.len()-1]})
}
fn observed(rows: &[ObservedRowV3], map_projection: bool, scalar_projection: bool) -> Value {
    // A zero-row response carries no row variant. The fixture supplies only
    // that empty shape hint; all nonempty results are decoded from actual rows.
    if rows.is_empty() && map_projection {
        return json!({"maps":[]});
    }
    if rows.is_empty() && scalar_projection {
        return json!({"values":[]});
    }
    if matches!(rows.first(), Some(ObservedRowV3::PropertyMap { .. })) {
        return json!({"maps": rows.iter().map(|r| match r {
            ObservedRowV3::PropertyMap { source, properties } => json!({"source":[source.partition,source.id],"properties":properties}),
            _ => panic!("mixed projected rows"),
        }).collect::<Vec<_>>()});
    }
    if matches!(rows.first(), Some(ObservedRowV3::Path(_))) {
        return json!({"paths": rows.iter().map(|r| match r { ObservedRowV3::Path(p) => p.iter().map(|e| match e { ObservedPathElement::Vertex(v) => json!(["v",v.partition,v.id]), ObservedPathElement::Edge(e) => json!(["e",e.partition,e.id]) }).collect::<Vec<_>>(), _ => panic!("mixed path rows") }).collect::<Vec<_>>()});
    }
    if matches!(rows.first(), Some(ObservedRowV3::Value { .. })) {
        return json!({"values": rows.iter().map(|r| if let ObservedRowV3::Value{value,..}=r {json!(value)} else {Value::Null}).collect::<Vec<_>>()});
    }

    if let [ObservedRowV3::Count(n)] = rows {
        return json!({"count":n});
    }
    json!({"vertices":rows.iter().map(|r|match r{ObservedRowV3::Vertex(v)=>json!([v.partition,v.id]),_=>json!(format!("{r:?}"))}).collect::<Vec<_>>()})
}
fn server_rows(rows: &[QueryRow], map_projection: bool) -> Value {
    if rows.is_empty() && map_projection {
        return json!({"maps":[]});
    }
    if matches!(rows.first(), Some(QueryRow::PropertyMap { .. })) {
        return json!({"maps": rows.iter().map(|r| match r {
            QueryRow::PropertyMap { source: QualifiedElement::Vertex { partition, id }, properties } => json!({"source":[partition,id],"properties":properties}),
            _ => panic!("mixed projected rows"),
        }).collect::<Vec<_>>()});
    }
    if matches!(rows.first(), Some(QueryRow::Path(_))) {
        return json!({"paths": rows.iter().map(|r| match r { QueryRow::Path(p) => p.iter().map(|e| match e { QualifiedElement::Vertex{partition,id} => json!(["v",partition,id]), QualifiedElement::Edge{partition,id} => json!(["e",partition,id]) }).collect::<Vec<_>>(), _ => panic!("mixed path rows") }).collect::<Vec<_>>()});
    }
    if matches!(rows.first(), Some(QueryRow::Value { .. })) {
        return json!({"values": rows.iter().map(|r| if let QueryRow::Value{value,..}=r {json!(value)} else {Value::Null}).collect::<Vec<_>>()});
    }

    if let [QueryRow::Count(n)] = rows {
        return json!({"count":n});
    }
    json!({"vertices":rows.iter().map(|r|match r{QueryRow::Element(QualifiedElement::Vertex{partition,id})=>json!([partition,id]),_=>json!(format!("{r:?}"))}).collect::<Vec<_>>()})
}
fn disk(p: &Path) -> u64 {
    std::fs::read_dir(p)
        .unwrap()
        .map(|e| {
            let e = e.unwrap();
            if e.file_type().unwrap().is_dir() {
                disk(&e.path())
            } else {
                e.metadata().unwrap().len()
            }
        })
        .sum()
}
fn rss(pid: u32) -> Value {
    let s = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap_or_default();
    json!(s
        .lines()
        .filter(|l| l.starts_with("VmRSS:") || l.starts_with("VmHWM:"))
        .collect::<Vec<_>>())
}
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    let fixture = PathBuf::from(&args[1]);
    let root = PathBuf::from(&args[2]);
    ensure!(
        !root.exists() || std::fs::read_dir(&root)?.next().is_none(),
        "refusing to reuse nonempty run directory"
    );
    std::fs::create_dir_all(&root)?;
    let bin = &args[3];
    let manifest: Value = serde_json::from_slice(&std::fs::read(fixture.join("manifest.json"))?)?;
    let mut groups = Vec::new();
    for g in 0..=3 {
        groups.push(Group::start(bin, &root, g).await?);
    }
    for group in &groups[1..] {
        let request = d2_raft::ControlRequest::D1Capacity {
            shard: group.placement.shard,
            epoch: 1,
        };
        let before = d2_raft::send_control(&group.control.to_string(), request.clone()).await?;
        ensure!(
            matches!(&before,d2_raft::ControlResponse::D1Capacity(Ok(c)) if c.bytes_limit==64*1024*1024 && c.retained_requests==0 && c.retained_bytes==0),
            "not a fresh default-capacity shard: {before:?}"
        );
        let estimate = manifest["shards"][group.placement.shard as usize - 1]
            ["d1_accounted_estimate"]
            .as_u64()
            .unwrap();
        println!(
            "{}",
            json!({"event":"capacity_before","shard":group.placement.shard,"estimate":estimate,"response":before})
        );
        if estimate > 64 * 1024 * 1024 {
            ensure!(
                estimate <= 128 * 1024 * 1024,
                "fixture exceeds supported capacity"
            );
            let response = d2_raft::send_control(
                &group.control.to_string(),
                d2_raft::ControlRequest::D1SetCapacity {
                    shard: group.placement.shard,
                    epoch: 1,
                    expected_bytes: 64 * 1024 * 1024,
                    new_bytes: 128 * 1024 * 1024,
                },
            )
            .await?;
            ensure!(
                matches!(&response,d2_raft::ControlResponse::D1Admin(Ok(d2_raft::D1Result::DedupCapacity{bytes_limit})) if *bytes_limit==128*1024*1024),
                "capacity change failed {response:?}"
            );
            println!(
                "{}",
                json!({"event":"explicit_capacity_upgrade","shard":group.placement.shard,"response":response,"new_limit_bytes":128*1024*1024})
            );
        }
    }
    let load_start = Instant::now();
    let mut total_rows = 0;
    let mut latencies = Vec::new();
    for group in &groups[1..] {
        let mut r = BufReader::new(File::open(
            fixture.join(format!("shard-{}.bin", group.placement.shard)),
        )?);
        let mut batch = 0u64;
        let mut pending = None;
        loop {
            let mut operations = Vec::new();
            let mut raw_bytes = 0usize;
            for _ in 0..5000 {
                match if pending.is_some() {
                    pending.take()
                } else {
                    read_record(&mut r)?
                } {
                    Some((key, value)) => {
                        if !operations.is_empty()
                            && raw_bytes + key.len() + value.len() > 2 * 1024 * 1024
                        {
                            pending = Some((key, value));
                            break;
                        }
                        raw_bytes += key.len() + value.len();
                        operations.push(d2_raft::WriteOp::Put { key, value });
                    }
                    None => break,
                }
            }
            if operations.is_empty() {
                break;
            }
            let n = operations.len();
            let mut id = [0; 16];
            id[..8].copy_from_slice(&batch.to_le_bytes());
            id[8..].copy_from_slice(b"corehr01");
            let t = Instant::now();
            let response = remote::write_identified(
                group.replica.endpoint,
                RemoteIdentity {
                    group: group.placement.group,
                    node: 1,
                },
                d2_raft::D1WriteRequest {
                    shard: group.placement.shard,
                    epoch: 1,
                    request_id: id,
                    expected_sequence: None,
                    operations,
                },
                Duration::from_secs(30),
            )
            .await
            .context("bounded fixture import; unknown failures are not retried")?;
            ensure!(
                response
                    == d2_raft::D1Result::Write {
                        sequence: batch + 1,
                        status: d2_raft::D1WriteStatus::Committed,
                        applied_operations: n
                    },
                "import was not a fresh committed batch: {response:?}"
            );
            latencies.push(t.elapsed().as_secs_f64() * 1000.);
            batch += 1;
            total_rows += n;
        }
        println!(
            "{}",
            json!({"event":"loaded_shard","shard":group.placement.shard,"batches":batch})
        );
    }
    println!(
        "{}",
        json!({"event":"load","seconds":load_start.elapsed().as_secs_f64(),"kv_rows":total_rows,"latency":summary(latencies),"kind":"offline-encoded fixture imported through Raft; not ordinary graph mutation throughput"})
    );
    for group in &groups[1..] {
        let response = d2_raft::send_control(
            &group.control.to_string(),
            d2_raft::ControlRequest::D1Capacity {
                shard: group.placement.shard,
                epoch: 1,
            },
        )
        .await?;
        let estimate = manifest["shards"][group.placement.shard as usize - 1]
            ["d1_accounted_estimate"]
            .as_u64()
            .unwrap();
        ensure!(
            matches!(&response,d2_raft::ControlResponse::D1Capacity(Ok(c)) if c.retained_bytes as u64==estimate),
            "dedup accounting differs from fixture estimate: {response:?}"
        );
        println!(
            "{}",
            json!({"event":"capacity_after","shard":group.placement.shard,"response":response})
        );
    }
    // Exact byte verification of every canonical vertex/edge and both indexes.
    for group in &groups[1..] {
        let t = Instant::now();
        let snap = remote::paged_snapshot_identified(
            group.replica.endpoint,
            RemoteIdentity {
                group: group.placement.group,
                node: 1,
            },
            group.placement.shard,
            BUDGET,
        )
        .await?;
        let entries: BTreeMap<_, _> = snap.entries.into_iter().collect();
        let mut r = BufReader::new(File::open(
            fixture.join(format!("shard-{}.bin", group.placement.shard)),
        )?);
        let mut n = 0;
        while let Some((k, v)) = read_record(&mut r)? {
            ensure!(entries.get(&k) == Some(&v), "stored fixture differs");
            n += 1;
        }
        ensure!(entries.len() == n, "extra/missing stored records");
        println!(
            "{}",
            json!({"event":"all_kv_verified","shard":group.placement.shard,"rows":n,"seconds":t.elapsed().as_secs_f64()})
        );
    }
    let metadata = &groups[0];
    let catalog = Catalog::new_remote(
        metadata.replica.endpoint,
        1,
        metadata.placement.clone(),
        BUDGET,
    )?;
    let placements = groups[1..]
        .iter()
        .map(|g| g.placement.clone())
        .collect::<Vec<_>>();
    let resolver = RouteResolver::new_remote(
        metadata.placement.clone(),
        std::slice::from_ref(&metadata.replica),
        &groups[1..].iter().map(|g| g.replica).collect::<Vec<_>>(),
    )?;
    let people = manifest["people"].as_u64().unwrap();
    let departments = manifest["departments"].as_u64().unwrap();
    let cases = manifest["queries"].as_array().unwrap().clone();
    let jobs_matrix = std::env::var_os("COREHR_JOBS_MATRIX").is_some();
    let client_cases = if jobs_matrix {
        Vec::new()
    } else {
        cases.clone()
    };
    run_blocking(move || -> anyhow::Result<()> {
        for p in placements {
            catalog.commit_registration(&catalog.prepare_registration([p.group as u8; 16], p.group as u32, p)?)?;
        }
        // Predetermined fixture phases, never adaptive renewal or retries.
        // Filtering is stable, retaining each phase's original manifest order.
        for case in &client_cases {
            ensure!(["legacy", "repeat_explicit", "repeat_global"].contains(
                &case["client_phase"].as_str().unwrap_or("legacy")), "unknown client phase");
        }
        for phase in ["legacy", "repeat_explicit", "repeat_global"] {
            let phase_cases: Vec<_> = client_cases.iter().filter(|case|
                case["client_phase"].as_str().unwrap_or("legacy") == phase
            ).collect();
            if phase != "legacy" && phase_cases.is_empty() {
                continue;
            }
            // Matrix-only mode preserves its original one admission check,
            // but deliberately executes no client query phase.
            let skip_reason = jobs_matrix.then_some("jobs_matrix");
            let t = Instant::now();
            let session = resolver.experimental_read_session(BUDGET);
            let session_error_category = session.as_ref().err().map(|error| match error {
                ExperimentalRouteError::Route(RouteError::Storage(
                    velagraph::StorageNodeError::ResourceExhausted(_),
                )) => "ResourceExhausted",
                _ => "Unexpected",
            });
            println!("{}", json!({"event":"client_session","phase":phase,"planned_cases":phase_cases.len(),"skip_reason":skip_reason,"error_category":session_error_category,"seconds":t.elapsed().as_secs_f64(),"error":session.as_ref().err().map(|e|format!("{e:?}"))}));
            match session {
                Ok(s) => for c in phase_cases {
                    let query = c["query"].as_str().unwrap();
                    let t = Instant::now();
                    let result = s.observed_gremlin_v3(c["partition"].as_u64().unwrap() as u32, query);
                    let client_unsupported = c["client_expect_unsupported"].as_bool() == Some(true);
                    // Explicit client results can differ from a server budget
                    // rejection; never infer the client's budget from the server.
                    let expect_error = c.get("client_expected").is_none()
                        && (c["expect_error"].as_bool() == Some(true) || client_unsupported
                            || c.get("client_expected_error").is_some());
                    let report = match result {
                        Ok(output) => {
                            ensure!(!expect_error, "unexpected success {query}");
                            ensure!(output.completeness == AdjacencyCompleteness::Unverified);
                            let expected = c.get("client_expected").unwrap_or(&c["expected"]);
                            let actual = observed(&output.rows, expected.get("maps").is_some(), expected.get("values").is_some());
                            ensure!(actual == *expected, "client oracle mismatch {query}: {actual} != {expected}");
                            json!({"correct":true,"rows":output.rows.len()})
                        },
                        Err(e) => {
                            ensure!(expect_error, "unexpected client error {query}: {e:?}");
                            let category = match &e {
                                    ExperimentalRouteError::Unsupported(_)
                                    | ExperimentalRouteError::Query(velagraph::Error::UnsupportedGremlinStep(_)
                                        | velagraph::Error::UnsupportedGremlinArgument(_)
                                        | velagraph::Error::UnsupportedGremlinValue(_)) => "Unsupported",
                                    ExperimentalRouteError::Query(velagraph::Error::TraversalLimitExceeded(_)) => "ResourceExhausted",
                                    ExperimentalRouteError::Query(velagraph::Error::InvalidGremlinLimit(0)) => "InvalidArgument",
                                    _ => "Unexpected",
                                };
                            if let Some(expected) = c["client_expected_error"].as_str() {
                                ensure!(category == expected, "client error category {category} != {expected}: {query}: {e:?}");
                            }
                            if client_unsupported { ensure!(matches!(&e, ExperimentalRouteError::Unsupported(_)), "server-only client case must explicitly reject Unsupported: {e:?}"); }
                            ensure!(client_unsupported || (c["name"] == "global_zero_limit_count" && matches!(&e, ExperimentalRouteError::Query(velagraph::Error::InvalidGremlinLimit(0)))) || matches!(&e,
                                ExperimentalRouteError::Query(velagraph::Error::UnsupportedGremlinStep(_)
                                | velagraph::Error::UnsupportedGremlinArgument(_)
                                | velagraph::Error::UnsupportedGremlinValue(_)
                                | velagraph::Error::TraversalLimitExceeded(_))), "unexpected error category: {e:?}");
                            json!({"expected_rejection":format!("{e:?}"),"error_category":category})
                        }
                    };
                    println!("{}", json!({"event":"client_query","phase":phase,"name":c["name"],"query":query,"ms":t.elapsed().as_secs_f64()*1000.,"result":report}));
                },
                Err(e) => {
                    ensure!(phase == "legacy", "later client phase acquisition failed: {phase}: {e:?}");
                    ensure!(matches!(&e, ExperimentalRouteError::Route(RouteError::Storage(velagraph::StorageNodeError::ResourceExhausted(_)))), "unexpected session failure: {e:?}");
                    ensure!(manifest["shards"].as_array().unwrap().iter().map(|s|s["kv_rows"].as_u64().unwrap()).sum::<u64>() > replicated_graph::routing::MAX_SCAN_VIEW_ROWS as u64 || manifest["shards"].as_array().unwrap().iter().map(|s|s["raw_bytes"].as_u64().unwrap()).sum::<u64>() > replicated_graph::routing::MAX_SCAN_VIEW_BYTES as u64, "small session unexpectedly rejected");
                    // One full-fixture admission failure, not three retries.
                    break;
                }
            }
        }
        Ok(())
    }).await??;
    let service = CoordinatorService::start(
        "127.0.0.1:0".parse()?,
        StaticCatalog {
            partitions: groups[1..]
                .iter()
                .map(|g| PartitionRoute {
                    partition: g.placement.group as u32,
                    shard: g.placement.shard,
                    epoch: 1,
                    identity: RemoteIdentity {
                        group: g.placement.group,
                        node: 1,
                    },
                    endpoint: g.replica.endpoint,
                })
                .collect(),
        },
        Duration::from_secs(5),
    )
    .await?;
    if std::env::var_os("COREHR_JOBS_MATRIX").is_some() {
        println!(
            "{}",
            json!({"event":"job_matrix_begin","samples":3,"fixed_lease_seconds":120,"request_max_seconds":5,"work_cap":8000000,"logical_bytes_cap":1073741824u64,"retained_key_cap":250000,"legacy_combined_key_lower_bound":dedup_key_lower_bound(people),"dedup_representation":"shared_qualified_identity"})
        );
        let result=async {
            for query in ["g.V(1).repeat(__.in('reports_to')).emit().times(32).count()","g.V(1).repeat(__.in('reports_to')).emit().times(32).limit(1)"] {
                let response=distributed::query(service.local_addr(),DistributedRequest{start_partition:2,text:query.into(),timeout_millis:5000},Duration::from_secs(6)).await?;
                ensure!(matches!(response,Err(QueryFailure::ResourceExhausted)),"old synchronous broad traversal no longer rejected");
                println!("{}",json!({"event":"synchronous_guard","query":query,"expected_rejection":"ResourceExhausted"}));
            }
            cancel_active_job(service.local_addr()).await?;
            let jobs=job_cases(people,departments);
            let mut errors=Vec::new();
            for index in [4,6] {
                if let Err(e)=execute_job(service.local_addr(),&jobs[index],"probe",0).await {errors.push(format!("{}: {e:#}",jobs[index].name));}
            }
            ensure!(errors.is_empty(),"probe failures: {errors:?}");
            for case in &jobs {
                for sample in 0..3 {execute_job(service.local_addr(),case,"matrix",sample).await?;}
            }
            println!("{}",json!({"event":"job_matrix_complete","cases":jobs.len(),"samples":3,"probe_cases":2,"complete":true}));
            Ok::<(),anyhow::Error>(())
        }.await;
        if let Err(e) = &result {
            println!(
                "{}",
                json!({"event":"matrix_failure","error":format!("{e:#}")})
            );
        }
        service.shutdown().await;
        println!(
            "{}",
            json!({"event":"resources","database_bytes":disk(&root),"runner":rss(std::process::id()),"nodes":groups.iter().map(|g|json!({"group":g.placement.group,"memory":rss(g.child.id())})).collect::<Vec<_>>(),"complete":result.is_ok(),"query_failures":u64::from(result.is_err())})
        );
        return result;
    }
    let mut query_failures = 0usize;
    for c in cases {
        let supported = c["server_supported"].as_bool() == Some(true);
        let mut times = Vec::new();
        let mut result_json = Value::Null;
        let mut sample_results = Vec::new();
        for _ in 0..if supported { 7 } else { 1 } {
            let t = Instant::now();
            let result = distributed::query(
                service.local_addr(),
                DistributedRequest {
                    start_partition: c["partition"].as_u64().unwrap() as u32,
                    text: c["query"].as_str().unwrap().into(),
                    timeout_millis: 5000,
                },
                Duration::from_secs(6),
            )
            .await?;
            times.push(t.elapsed().as_secs_f64() * 1000.);
            match result {
                Ok(o) => {
                    ensure!(supported, "unexpected server success {}", c["name"]);
                    ensure!(o.completeness == QueryCompleteness::Unverified);
                    let actual = server_rows(&o.rows, c["expected"].get("maps").is_some());
                    ensure!(
                        actual == c["expected"],
                        "server oracle mismatch {}: {actual} != {}",
                        c["name"],
                        c["expected"]
                    );
                    result_json =
                        json!({"correct":true,"rows":o.rows.len(),"snapshots":o.snapshots});
                }
                Err(e) => {
                    if supported {
                        query_failures += 1;
                        result_json = json!({"unexpected_failure":format!("{e:?}")});
                    } else {
                        let expected_error = if c["server_expected_error"] == "InvalidArgument" {
                            QueryFailure::InvalidArgument
                        } else if c["server_expected_error"] == "ResourceExhausted" {
                            QueryFailure::ResourceExhausted
                        } else {
                            QueryFailure::Unsupported
                        };
                        ensure!(e == expected_error, "unexpected rejection {e:?}");
                        result_json = json!({"expected_rejection":format!("{e:?}")});
                    }
                }
            }
            sample_results.push(result_json.clone());
        }
        println!(
            "{}",
            json!({"event":"server_query","name":c["name"],"query":c["query"],"latency":summary(times),"result":result_json,"samples":sample_results})
        );
    }
    // Explicit bounded application composition, NOT Gremlin repeat/recursion.
    for (label, start, base, fanout) in [
        ("reports_to", people, 0, 8),
        ("parent_department", people + departments, people, 5),
    ] {
        let t = Instant::now();
        let mut current = start;
        let mut chain = Vec::new();
        for _ in 0..64 {
            let output = distributed::query(
                service.local_addr(),
                DistributedRequest {
                    start_partition: 1 + (current % 3) as u32,
                    text: format!("g.V({current}).out('{label}')"),
                    timeout_millis: 5000,
                },
                Duration::from_secs(6),
            )
            .await?
            .map_err(|e| anyhow::anyhow!("chain failed {e:?}"))?;
            ensure!(output.completeness == QueryCompleteness::Unverified);
            if current == base + 1 {
                ensure!(output.rows.is_empty());
                break;
            }
            let expected = base + 1 + (current - base - 2) / fanout;
            ensure!(
                output.rows
                    == vec![QueryRow::Element(QualifiedElement::Vertex {
                        partition: 1 + (expected % 3) as u32,
                        id: expected
                    })]
            );
            chain.push(expected);
            current = expected;
        }
        ensure!(current == base + 1, "chain exceeded application bound");
        println!(
            "{}",
            json!({"event":"application_chain","label":label,"start":start,"chain":chain,"ms":t.elapsed().as_secs_f64()*1000.,"correct":true,"atomic_snapshot":false})
        );
    }
    service.shutdown().await;
    println!(
        "{}",
        json!({"event":"resources","database_bytes":disk(&root),"runner":rss(std::process::id()),"nodes":groups.iter().map(|g|json!({"group":g.placement.group,"memory":rss(g.child.id())})).collect::<Vec<_>>(),"complete":query_failures==0,"query_failures":query_failures})
    );
    ensure!(
        query_failures == 0,
        "supported queries had {query_failures} failures"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use velagraph::Value as GraphValue;

    #[test]
    fn server_property_maps_preserve_qualified_sources_properties_and_bags() {
        let properties = BTreeMap::from([
            ("name".into(), vec![GraphValue::Text("Ada".into())]),
            ("active".into(), vec![GraphValue::Boolean(true)]),
            ("nullable".into(), vec![GraphValue::Null]),
        ]);
        let first = QueryRow::PropertyMap {
            source: QualifiedElement::Vertex {
                partition: 7,
                id: 20,
            },
            properties,
        };
        let other_partition = QueryRow::PropertyMap {
            source: QualifiedElement::Vertex {
                partition: 8,
                id: 20,
            },
            properties: BTreeMap::new(),
        };
        let expected_first = json!({
            "source":[7,20],
            "properties":{
                "active":[{"Boolean":true}],
                "name":[{"Text":"Ada"}],
                "nullable":["Null"]
            }
        });
        let output = server_rows(&[first.clone(), other_partition, first], true);
        assert_eq!(
            output,
            json!({"maps":[
                expected_first.clone(), {"source":[8,20],"properties":{}}, expected_first
            ]})
        );
        assert!(output["maps"][0]["properties"].get("missing").is_none());
        assert_eq!(output["maps"][0], output["maps"][2]);
    }

    #[test]
    #[should_panic(expected = "mixed projected rows")]
    fn server_property_maps_do_not_hide_mixed_rows() {
        server_rows(
            &[
                QueryRow::PropertyMap {
                    source: QualifiedElement::Vertex {
                        partition: 7,
                        id: 20,
                    },
                    properties: BTreeMap::new(),
                },
                QueryRow::Count(1),
            ],
            true,
        );
    }
}

#[cfg(test)]
mod projection_tests {
    use super::*;
    use velagraph::{experimental_graph::VertexRef, Value as PropertyValue};

    #[test]
    fn scalar_empty_hint_does_not_retype_nonempty_rows() {
        assert_eq!(observed(&[], false, true), json!({"values":[]}));
        assert_eq!(observed(&[], false, false), json!({"vertices":[]}));
        assert_eq!(observed(&[], true, false), json!({"maps":[]}));
        assert_eq!(
            observed(
                &[ObservedRowV3::Vertex(VertexRef {
                    partition: 3,
                    id: 20
                })],
                false,
                true,
            ),
            json!({"vertices":[[3,20]]})
        );
        assert_eq!(
            observed(
                &[ObservedRowV3::PropertyMap {
                    source: VertexRef {
                        partition: 3,
                        id: 20
                    },
                    properties: BTreeMap::new(),
                }],
                false,
                true,
            ),
            json!({"maps":[{"source":[3,20],"properties":{}}]})
        );
        assert_eq!(
            observed(&[ObservedRowV3::Count(0)], false, true),
            json!({"count":0})
        );
    }

    #[test]
    fn map_normalization_preserves_qualified_bags_null_and_empty_maps() {
        let properties = BTreeMap::from([
            ("name".into(), vec![PropertyValue::Text("same".into())]),
            ("nullable".into(), vec![PropertyValue::Null]),
        ]);
        let sources = [(3, 20), (1, 20), (3, 20), (2, 23)];
        let client: Vec<_> = sources
            .iter()
            .enumerate()
            .map(|(i, &(partition, id))| ObservedRowV3::PropertyMap {
                source: VertexRef { partition, id },
                properties: if i == 3 {
                    BTreeMap::new()
                } else {
                    properties.clone()
                },
            })
            .collect();
        let server: Vec<_> = sources
            .iter()
            .enumerate()
            .map(|(i, &(partition, id))| QueryRow::PropertyMap {
                source: QualifiedElement::Vertex { partition, id },
                properties: if i == 3 {
                    BTreeMap::new()
                } else {
                    properties.clone()
                },
            })
            .collect();
        let expected = json!({"maps":[
            {"source":[3,20],"properties":{"name":[{"Text":"same"}],"nullable":["Null"]}},
            {"source":[1,20],"properties":{"name":[{"Text":"same"}],"nullable":["Null"]}},
            {"source":[3,20],"properties":{"name":[{"Text":"same"}],"nullable":["Null"]}},
            {"source":[2,23],"properties":{}}
        ]});
        assert_eq!(observed(&client, true, false), expected);
        assert_eq!(server_rows(&server, true), expected);
        assert_eq!(observed(&[], true, false), json!({"maps":[]}));
        assert_eq!(server_rows(&[], true), json!({"maps":[]}));
        assert_eq!(observed(&[], false, false), json!({"vertices":[]}));
        assert_eq!(server_rows(&[], false), json!({"vertices":[]}));
        // The empty-result shape hint must never conceal a wrong nonempty type.
        assert_eq!(
            observed(
                &[ObservedRowV3::Vertex(VertexRef {
                    partition: 3,
                    id: 20
                })],
                true,
                false
            ),
            json!({"vertices":[[3,20]]})
        );
        assert_eq!(
            server_rows(
                &[QueryRow::Element(QualifiedElement::Vertex {
                    partition: 3,
                    id: 20
                })],
                true
            ),
            json!({"vertices":[[3,20]]})
        );
    }

    #[test]
    #[should_panic(expected = "mixed projected rows")]
    fn client_map_normalization_rejects_mixed_rows() {
        observed(
            &[
                ObservedRowV3::PropertyMap {
                    source: VertexRef {
                        partition: 3,
                        id: 20,
                    },
                    properties: BTreeMap::new(),
                },
                ObservedRowV3::Vertex(VertexRef {
                    partition: 3,
                    id: 20,
                }),
            ],
            true,
            false,
        );
    }

    #[test]
    #[should_panic(expected = "mixed projected rows")]
    fn server_map_normalization_rejects_mixed_rows() {
        server_rows(
            &[
                QueryRow::PropertyMap {
                    source: QualifiedElement::Vertex {
                        partition: 3,
                        id: 20,
                    },
                    properties: BTreeMap::new(),
                },
                QueryRow::Element(QualifiedElement::Vertex {
                    partition: 3,
                    id: 20,
                }),
            ],
            true,
        );
    }
}
