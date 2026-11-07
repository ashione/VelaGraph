//! Full ordered result-page checks against the synthetic input relationship list.
//! Usage: corehr_result_pages FIXTURE EMPTY_RUN_DIR GRAPH_NODE_BINARY
//! Default fixture: 200000 people / 5000 departments; smaller smoke fixtures work too.
use anyhow::{ensure, Context};
use replicated_graph::{
    catalog::Placement,
    remote::{
        self,
        distributed::{
            jobs::{self, Output, Request},
            CoordinatorService, PartitionRoute, StaticCatalog,
        },
        query::{QualifiedElement, QueryCompleteness, QueryFailure},
        RemoteIdentity,
    },
    routing::RemoteReplica,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    io::{BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command},
    time::{Duration, Instant},
};
#[allow(dead_code)]
#[path = "../../../examples/corehr/model.rs"]
mod model;
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
#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    ensure!(
        args.len() == 4,
        "usage: corehr_result_pages FIXTURE EMPTY_RUN_DIR GRAPH_NODE_BINARY"
    );
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
    for g in 1..=3 {
        groups.push(Group::start(bin, &root, g).await?);
    }
    for group in &groups[..] {
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
    for group in &groups[..] {
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
    for group in &groups[..] {
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
    for group in &groups[..] {
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
    let service = CoordinatorService::start(
        "127.0.0.1:0".parse()?,
        StaticCatalog {
            partitions: groups[..]
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

    let people = manifest["people"].as_u64().context("people missing")?;
    let departments = manifest["departments"]
        .as_u64()
        .context("departments missing")?;
    let input = model::Model::new(people, departments);
    let cases = result_cases(people);
    let result = async {
        for case in &cases {
            execute(service.local_addr(), &input, case).await?;
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;
    service.shutdown().await;
    println!(
        "{}",
        json!({"event":"result_matrix_complete","correct":result.is_ok(),
        "cases":cases.len(), "people":people,"departments":departments,
        "full_fixture":people==200000 && departments==5000,
        "oracle":"input relationship list; every qualified identity in ordered BFS"})
    );
    result
}

struct Case {
    name: &'static str,
    roots: Vec<u64>,
    label: &'static str,
    depth: usize,
    emit: bool,
    dedup: bool,
    page_size: usize,
}
fn result_cases(people: u64) -> Vec<Case> {
    let department = people + 1;
    [
        ("ceo_all", vec![1], "reports_to", 32, true, false, 1000),
        (
            "duplicate_manager",
            vec![7, 7],
            "reports_to",
            32,
            true,
            false,
            1000,
        ),
        (
            "duplicate_manager_dedup",
            vec![7, 7],
            "reports_to",
            32,
            true,
            true,
            997,
        ),
        ("ceo_exact6", vec![1], "reports_to", 6, false, false, 1000),
        (
            "ceo_exact32_empty",
            vec![1],
            "reports_to",
            32,
            false,
            false,
            1,
        ),
        (
            "direct_reports_page1",
            vec![1],
            "reports_to",
            1,
            true,
            false,
            1,
        ),
        (
            "ordered_roots",
            vec![7, 1, 7],
            "reports_to",
            2,
            true,
            false,
            7,
        ),
        (
            "departments",
            vec![department],
            "parent_department",
            32,
            true,
            false,
            137,
        ),
        (
            "department_exact6",
            vec![department],
            "parent_department",
            6,
            false,
            false,
            137,
        ),
        (
            "duplicate_ceo_rejected",
            vec![1, 1],
            "reports_to",
            32,
            true,
            false,
            1000,
        ),
        ("ceo_all_dedup", vec![1], "reports_to", 32, true, true, 1000),
    ]
    .into_iter()
    .map(|(name, roots, label, depth, emit, dedup, page_size)| Case {
        name,
        roots,
        label,
        depth,
        emit,
        dedup,
        page_size,
    })
    .collect()
}

// Index the input relationships once, in their fixture edge-ID order. This
// algorithm never consults engine output, encoded adjacency, or count results.
fn oracle(input: &model::Model, case: &Case) -> Vec<QualifiedElement> {
    let mut incoming = BTreeMap::<u64, Vec<u64>>::new();
    for edge in &input.edges {
        if edge.label == case.label {
            incoming.entry(edge.to).or_default().push(edge.from);
        }
    }
    let mut frontier = case.roots.clone();
    let mut emitted = Vec::new();
    for _ in 0..case.depth {
        frontier = frontier
            .iter()
            .flat_map(|id| incoming.get(id).into_iter().flatten().copied())
            .collect();
        if case.emit {
            emitted.extend_from_slice(&frontier);
        }
        if frontier.is_empty() {
            break;
        }
    }
    let mut result = if case.emit { emitted } else { frontier };
    if case.dedup {
        let mut seen = BTreeSet::new();
        result.retain(|id| seen.insert((model::partition(*id), *id)));
    }
    result
        .into_iter()
        .map(|id| QualifiedElement::Vertex {
            partition: model::partition(id),
            id,
        })
        .collect()
}

async fn execute(
    endpoint: std::net::SocketAddr,
    input: &model::Model,
    case: &Case,
) -> anyhow::Result<()> {
    let expected = oracle(input, case);
    if matches!(case.name, "ceo_all" | "ceo_all_dedup") {
        ensure!(expected.len() == input.people as usize - 1);
        // Independent fixture sanity: this tree's BFS happens to be numeric order.
        for (row, id) in expected.iter().zip(2..=input.people) {
            ensure!(
                *row == QualifiedElement::Vertex {
                    partition: model::partition(id),
                    id
                }
            );
        }
    }
    let roots = case
        .roots
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let query = format!(
        "g.V({roots}).repeat(__.in('{}')){}.times({}){}",
        case.label,
        if case.emit { ".emit()" } else { "" },
        case.depth,
        if case.dedup { ".dedup()" } else { "" }
    );
    println!(
        "{}",
        json!({"event":"result_begin","name":case.name,"query":query,
        "expected_rows":expected.len(),"page_size":case.page_size})
    );
    let expect_rejection = case.name == "duplicate_ceo_rejected" && expected.len() > 250000;
    let start = Instant::now();
    let mut saved_token = None;
    let result = async {
        let mut request = Request::StartRows {
            start_partition: model::partition(case.roots[0]),
            text: query,
            page_size: case.page_size,
        };
        let mut fixed = None;
        let mut sequence = 0;
        let (token, stamps) = loop {
            let output = match jobs::request(endpoint,request,Duration::from_secs(5)).await? {
                Ok(output) => output,
                Err(error) => {
                    ensure!(expect_rejection && error==QueryFailure::ResourceExhausted,
                        "{} unexpected preparation failure: {error:?}",case.name);
                    println!("{}",json!({"event":"result_rejection","name":case.name,
                        "expected_rejection":"ResourceExhausted","before_ready":true,
                        "rows_exposed":0,"oracle_rows":expected.len(),"seconds":start.elapsed().as_secs_f64()}));
                    return Ok(());
                }
            };
            let Output::Preparing {
                token,
                next_sequence,
                ready,
                completeness,
                snapshots,
            } = output
            else {
                anyhow::bail!("{}: rows exposed before preparation finished", case.name)
            };
            if let Some(t) = saved_token {
                ensure!(t == token, "token changed");
            } else {
                saved_token = Some(token);
            }
            ensure!(next_sequence == sequence, "unexpected sequence");
            ensure!(completeness == QueryCompleteness::Unverified);
            if let Some(stamps) = &fixed {
                ensure!(*stamps == snapshots, "snapshot drift");
            } else {
                ensure!(snapshots.len() == 3);
                fixed = Some(snapshots.clone());
            }
            if ready {
                ensure!(!expect_rejection,"over-cap job became ready");
                break (token, snapshots);
            }
            ensure!(sequence < 20000, "preparation request ceiling");
            request = Request::Advance {
                token,
                sequence: next_sequence,
            };
            sequence += 1;
        };
        let preparation_seconds = start.elapsed().as_secs_f64();
        let mut actual = Vec::with_capacity(expected.len());
        let mut cursor = 0;
        let mut pages = 0;
        let mut first_response = None;
        let mut last_response = None;
        let mut read_ms = Vec::new();
        loop {
            let tick = Instant::now();
            let output = jobs::request(
                endpoint,
                Request::Read { token, cursor },
                Duration::from_secs(5),
            )
            .await??;
            read_ms.push(tick.elapsed().as_secs_f64() * 1000.);
            let serialized = serde_json::to_value(&output)?;
            let Output::Page {
                token: t,
                cursor: c,
                next_cursor,
                done,
                rows,
                completeness,
                snapshots,
            } = output
            else {
                anyhow::bail!("Read did not return Page")
            };
            ensure!(
                t == token
                    && c == cursor
                    && completeness == QueryCompleteness::Unverified
                    && snapshots == stamps
            );
            let end = (cursor + case.page_size).min(expected.len());
            ensure!(
                rows == expected[cursor..end],
                "{}: ordered identity mismatch at cursor {}",
                case.name,
                cursor
            );
            ensure!(rows.len() <= case.page_size);
            ensure!(done == (end == expected.len()));
            ensure!(
                next_cursor == if done { None } else { Some(end) },
                "cursor must be immutable aligned offset"
            );
            if first_response.is_none() {
                first_response = Some(serialized.clone());
            }
            if let Some(previous) = last_response.replace((cursor, serialized.clone())) {
                // Replay an earlier page after reading a later one: no moving cursor.
                if pages == 1 {
                    let replay = jobs::request(
                        endpoint,
                        Request::Read {
                            token,
                            cursor: previous.0,
                        },
                        Duration::from_secs(5),
                    )
                    .await??;
                    ensure!(
                        serde_json::to_value(replay)? == previous.1,
                        "old page replay changed"
                    );
                }
            }
            actual.extend(rows);
            pages += 1;
            if done {
                break;
            }
            ensure!(end > cursor, "non-progressing page");
            cursor = end;
        }
        ensure!(
            actual == expected,
            "concatenated full ordered result mismatch"
        );
        // Arbitrary valid offsets remain readable even after the final page.
        for offset in [
            0,
            (expected.len() / 2 / case.page_size) * case.page_size,
            cursor,
        ] {
            let output = jobs::request(
                endpoint,
                Request::Read {
                    token,
                    cursor: offset,
                },
                Duration::from_secs(5),
            )
            .await??;
            if offset == 0 {
                ensure!(serde_json::to_value(&output)? == *first_response.as_ref().unwrap());
            }
            let Output::Page {
                token: t,
                cursor: c,
                rows,
                completeness,
                snapshots,
                next_cursor,
                done,
            } = output
            else {
                anyhow::bail!("replay not Page")
            };
            let end = (offset + case.page_size).min(expected.len());
            ensure!(
                t == token
                    && c == offset
                    && rows == expected[offset..end]
                    && snapshots == stamps
                    && completeness == QueryCompleteness::Unverified
            );
            ensure!(
                done == (end == expected.len())
                    && next_cursor == if done { None } else { Some(end) }
            );
        }
        println!(
            "{}",
            json!({"event":"result_outcome","name":case.name,"correct":true,
            "verified_ordered_identities":actual.len(),"page_size":case.page_size,"pages":pages,
            "preparation_advances":sequence,"preparation_seconds":preparation_seconds,
            "seconds":start.elapsed().as_secs_f64(),"read_latency":summary(read_ms),
            "fixed_snapshots":stamps,"old_and_arbitrary_page_replay":true,
            "oracle":"input relationship BFS; not count/progress"})
        );
        Ok::<(), anyhow::Error>(())
    }
    .await;
    if let Some(token) = saved_token {
        let cancel =
            jobs::request(endpoint, Request::Cancel { token }, Duration::from_secs(5)).await;
        let after = jobs::request(
            endpoint,
            Request::Read { token, cursor: 0 },
            Duration::from_secs(5),
        )
        .await;
        if result.is_ok() {
            if expect_rejection {
                ensure!(
                    matches!(cancel, Ok(Err(QueryFailure::Unavailable))),
                    "failed job retained"
                );
            } else {
                ensure!(
                    matches!(cancel, Ok(Ok(Output::Cancelled))),
                    "cleanup failed"
                );
            }
            ensure!(
                matches!(after, Ok(Err(QueryFailure::Unavailable))),
                "cancelled page still readable"
            );
        }
    }
    result
}

#[test]
fn independent_relationship_oracle_preserves_bfs_and_root_bags() {
    let input = model::Model::new(200000, 5000);
    let cases = result_cases(input.people);
    let all = oracle(&input, &cases[0]);
    assert_eq!(all.len(), 199999);
    assert_eq!(cases.len(), 11);
    assert_eq!(cases[10].name, "ceo_all_dedup");
    assert_eq!(oracle(&input, &cases[10]), all);
    assert_eq!(
        all.first(),
        Some(&QualifiedElement::Vertex {
            partition: 3,
            id: 2
        })
    );
    assert_eq!(
        all.last(),
        Some(&QualifiedElement::Vertex {
            partition: 3,
            id: 200000
        })
    );
    let duplicates = oracle(&input, &cases[1]);
    assert_eq!(&duplicates[..8], &duplicates[8..16]);
    assert_eq!(
        duplicates[0],
        QualifiedElement::Vertex {
            partition: model::partition(50),
            id: 50
        }
    );
    let dedup = oracle(&input, &cases[2]);
    assert_eq!(duplicates.len(), 2 * dedup.len());
    let unique: BTreeSet<_> = duplicates
        .iter()
        .map(|v| serde_json::to_string(v).unwrap())
        .collect();
    assert_eq!(unique.len(), dedup.len());
    assert_eq!(oracle(&input, &cases[9]).len(), 399998);
    assert_eq!(oracle(&input, &cases[3]).len(), 162551);
    assert!(oracle(&input, &cases[4]).is_empty());
    let roots = oracle(&input, &cases[6]);
    let prefix = [
        50, 51, 52, 53, 54, 55, 56, 57, 2, 3, 4, 5, 6, 7, 8, 9, 50, 51, 52, 53, 54, 55, 56, 57,
    ];
    assert_eq!(
        &roots[..prefix.len()],
        prefix
            .map(|id| QualifiedElement::Vertex {
                partition: model::partition(id),
                id
            })
            .as_slice()
    );
    assert_eq!(oracle(&input, &cases[7]).len(), 4999);
    assert_eq!(oracle(&input, &cases[8]).len(), 1094);
}
