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
fn observed(rows: &[ObservedRowV3], map_projection: bool) -> Value {
    // A zero-row response carries no row variant. The fixture supplies only
    // that empty shape hint; all nonempty results are decoded from actual rows.
    if rows.is_empty() && map_projection {
        return json!({"maps":[]});
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
    let client_cases = cases.clone();
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
            let skip_reason: Option<&str> = None;
            let t = Instant::now();
            let session = resolver.experimental_read_session(BUDGET);
            let session_error_category = session.as_ref().err().map(|error| match error {
                ExperimentalRouteError::Route(RouteError::Storage(
                    rocksdb_sql_table::StorageNodeError::ResourceExhausted(_),
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
                            let actual = observed(&output.rows, expected.get("maps").is_some());
                            ensure!(actual == *expected, "client oracle mismatch {query}: {actual} != {expected}");
                            json!({"correct":true,"rows":output.rows.len()})
                        },
                        Err(e) => {
                            ensure!(expect_error, "unexpected client error {query}: {e:?}");
                            let category = match &e {
                                    ExperimentalRouteError::Unsupported(_)
                                    | ExperimentalRouteError::Query(rocksdb_sql_table::Error::UnsupportedGremlinStep(_)
                                        | rocksdb_sql_table::Error::UnsupportedGremlinArgument(_)
                                        | rocksdb_sql_table::Error::UnsupportedGremlinValue(_)) => "Unsupported",
                                    ExperimentalRouteError::Query(rocksdb_sql_table::Error::TraversalLimitExceeded(_)) => "ResourceExhausted",
                                    ExperimentalRouteError::Query(rocksdb_sql_table::Error::InvalidGremlinLimit(0)) => "InvalidArgument",
                                    _ => "Unexpected",
                                };
                            if let Some(expected) = c["client_expected_error"].as_str() {
                                ensure!(category == expected, "client error category {category} != {expected}: {query}: {e:?}");
                            }
                            if client_unsupported { ensure!(matches!(&e, ExperimentalRouteError::Unsupported(_)), "server-only client case must explicitly reject Unsupported: {e:?}"); }
                            ensure!(client_unsupported || (c["name"] == "global_zero_limit_count" && matches!(&e, ExperimentalRouteError::Query(rocksdb_sql_table::Error::InvalidGremlinLimit(0)))) || matches!(&e,
                                ExperimentalRouteError::Query(rocksdb_sql_table::Error::UnsupportedGremlinStep(_)
                                | rocksdb_sql_table::Error::UnsupportedGremlinArgument(_)
                                | rocksdb_sql_table::Error::UnsupportedGremlinValue(_)
                                | rocksdb_sql_table::Error::TraversalLimitExceeded(_))), "unexpected error category: {e:?}");
                            json!({"expected_rejection":format!("{e:?}"),"error_category":category})
                        }
                    };
                    println!("{}", json!({"event":"client_query","phase":phase,"name":c["name"],"query":query,"ms":t.elapsed().as_secs_f64()*1000.,"result":report}));
                },
                Err(e) => {
                    ensure!(phase == "legacy", "later client phase acquisition failed: {phase}: {e:?}");
                    ensure!(matches!(&e, ExperimentalRouteError::Route(RouteError::Storage(rocksdb_sql_table::StorageNodeError::ResourceExhausted(_)))), "unexpected session failure: {e:?}");
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
mod projection_tests {
    use super::*;
    use rocksdb_sql_table::{experimental_graph::VertexRef, Value as PropertyValue};

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
        assert_eq!(observed(&client, true), expected);
        assert_eq!(server_rows(&server, true), expected);
        assert_eq!(observed(&[], true), json!({"maps":[]}));
        assert_eq!(server_rows(&[], true), json!({"maps":[]}));
        assert_eq!(observed(&[], false), json!({"vertices":[]}));
        assert_eq!(server_rows(&[], false), json!({"vertices":[]}));
        // The empty-result shape hint must never conceal a wrong nonempty type.
        assert_eq!(
            observed(
                &[ObservedRowV3::Vertex(VertexRef {
                    partition: 3,
                    id: 20
                })],
                true
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
