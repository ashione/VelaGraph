use super::*;
use d2_raft::{cohort_reads, managed_rpc::NodeRef, versioned_reads::ReadLimit, D1Error};
use replicated_graph::{
    managed_gateway::{
        self as gateway, Gateway, LocalReadProfile, LocalSession, Mutation, Operation, PageCursor,
        Query, QueryPage, RemoteConfig, Reply, VertexData, Write, WriteStatus,
    },
    managed_graph::{self, GraphCohort, Outcome},
    remote::query::{QualifiedElement, QueryCompleteness, QueryRow},
};
use rocksdb_sql_table::{Value, Vertex};
use std::{collections::BTreeSet, time::Instant};

const TTL: Duration = Duration::from_secs(60);
const QUERY: &str = "g.V(1,5,1).path()";
const EVIDENCE_ENV: &str = "VELAGRAPH_LOCAL_CAPACITY_EVIDENCE";

fn original_limit() -> ReadLimit {
    ReadLimit {
        rows: 10_000,
        bytes: 2 * 1024 * 1024,
        operations: 100_000,
    }
}

fn query() -> Query {
    Query {
        partition: 7,
        text: QUERY.into(),
    }
}

fn path(id: u64) -> QueryRow {
    QueryRow::Path(vec![QualifiedElement::Vertex { partition: 7, id }])
}

fn payload_vertex(id: u64, bytes: usize) -> Vertex {
    Vertex {
        id,
        labels: vec!["record".into()],
        properties: BTreeMap::from([("payload".into(), Value::Text("A".repeat(bytes)))]),
    }
}

fn gateway_write(id: u8, payload_bytes: usize) -> Write {
    Write {
        external_id: [id; 16],
        mutation: Mutation::CreateVertex {
            partition: 7,
            vertex: VertexData::from(payload_vertex(u64::from(id), payload_bytes)),
        },
    }
}

async fn call(g: &Gateway, operation: Operation) -> gateway::Result<Reply> {
    gateway::request(g.local_addr(), binding(), operation, BUDGET).await
}

async fn route(process: &Process) -> anyhow::Result<gateway::LocalGraphRoute> {
    fs::write(process.directory.path().join("graph.request"), b"start")?;
    let endpoint = tokio::time::timeout(BUDGET, async {
        loop {
            if let Ok(bytes) = fs::read(process.directory.path().join("graph-ready.json")) {
                let (pid, endpoint): (u32, SocketAddr) = serde_json::from_slice(&bytes)?;
                if pid == process.child.id() {
                    return Ok::<_, anyhow::Error>(endpoint);
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await??;
    Ok(gateway::LocalGraphRoute {
        endpoint,
        identity: replicated_graph::remote::RemoteIdentity {
            group: process.shard,
            node: 1,
        },
        managed_identity: identity_in(process.shard, cohort()),
    })
}

#[derive(Debug, Serialize)]
struct CaptureEvidence {
    version: u64,
    rows: usize,
    payload_bytes: usize,
    replay_operations: usize,
    coordinator_journal_encoded_bytes: usize,
    terminal_prefix_json_bytes: usize,
}

// This diagnostic materialization is separate from the measured Open, and can
// warm retained-history/cache state. It never imports or changes graph KV.
async fn capture_evidence(
    coordinator: &Client,
    a: &Client,
    b: &Client,
    version: u64,
    limit: ReadLimit,
) -> anyhow::Result<CaptureEvidence> {
    let captured = cohort_reads::acquire_endpoints(
        NodeRef::Remote(coordinator),
        authority(),
        1,
        vec![
            (NodeRef::Remote(a), target(7)),
            (NodeRef::Remote(b), target(8)),
        ],
        Some(version),
        limit,
        TTL,
    )
    .await
    .map_err(|e| anyhow!("diagnostic certified capture: {e:?}"))?;
    assert_eq!(captured.version().map_err(|e| anyhow!("{e:?}"))?, version);
    let mut rows = 0;
    let mut payload_bytes = 0;
    let mut replay_operations = 0;
    for shard in [7, 8] {
        let snapshot = captured.shard(shard).map_err(|e| anyhow!("{e:?}"))?;
        assert_eq!(snapshot.version, version);
        rows += snapshot.entries.len();
        payload_bytes += snapshot
            .entries
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum::<usize>();
        replay_operations += snapshot.replay_operations;
    }
    drop(captured);
    let journal = NodeRef::Remote(coordinator)
        .journal(&authority(), 1)
        .await?;
    assert_eq!(journal.resolved_prefix(), version);
    let coordinator_journal_encoded_bytes = journal.encode().map_err(|e| anyhow!("{e:?}"))?.len();
    drop(journal);
    let proof = NodeRef::Remote(coordinator)
        .terminal(&authority(), 1, version)
        .await?;
    assert_eq!(proof.version(), version);
    assert_eq!(proof.cohort(), &cohort());
    let terminal_prefix_json_bytes = serde_json::to_vec(&proof)?.len();
    Ok(CaptureEvidence {
        version,
        rows,
        payload_bytes,
        replay_operations,
        coordinator_journal_encoded_bytes,
        terminal_prefix_json_bytes,
    })
}

#[derive(Debug, Serialize)]
struct ProcessResources {
    pid: u32,
    user_cpu_ticks: Option<u64>,
    system_cpu_ticks: Option<u64>,
    vm_hwm_kib: Option<u64>,
}

fn resources(pid: u32) -> ProcessResources {
    let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok();
    // comm may contain whitespace and parentheses; fields after its final ')'
    // start at field 3, so utime/stime (14/15) have offsets 11/12.
    let fields = stat
        .as_deref()
        .and_then(|text| text.rsplit_once(')'))
        .map(|(_, rest)| rest.split_whitespace().collect::<Vec<_>>());
    let ticks = |index| {
        fields
            .as_ref()
            .and_then(|fields| fields.get(index))
            .and_then(|value: &&str| value.parse::<u64>().ok())
    };
    let vm_hwm_kib = fs::read_to_string(format!("/proc/{pid}/status"))
        .ok()
        .and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("VmHWM:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
            })
        });
    ProcessResources {
        pid,
        user_cpu_ticks: ticks(11),
        system_cpu_ticks: ticks(12),
        vm_hwm_kib,
    }
}

fn resource_sample(processes: &[&Process]) -> BTreeMap<String, ProcessResources> {
    let mut result = BTreeMap::from([("gateway_parent".into(), resources(std::process::id()))]);
    for process in processes {
        result.insert(
            format!("shard_{}", process.shard),
            resources(process.child.id()),
        );
    }
    result
}

fn visible_commit(reply: &Reply, write: &Write, ordinal: u64) -> bool {
    matches!(
        reply,
        Reply::Write {
            external_id,
            transaction_id: Some(_),
            status: WriteStatus::Committed { ordinal: found, visible: true },
        } if *external_id == write.external_id && *found == ordinal
    )
}

// At most one exact retry. Status is observational and cannot make an unfinished
// Commit visible. Unknown never permits a new ID or a re-prepared mutation.
async fn submit_fifth(g: &Gateway, write: &Write) -> anyhow::Result<serde_json::Value> {
    let initial = call(g, Operation::Submit(write.clone())).await;
    match &initial {
        Ok(Reply::Write {
            external_id,
            status,
            ..
        }) => {
            assert_eq!(*external_id, write.external_id);
            assert!(matches!(
                status,
                WriteStatus::Pending | WriteStatus::Committed { .. }
            ));
        }
        Err(gateway::Error::Unknown) => {}
        other => anyhow::bail!("unexpected fifth Submit: {other:?}"),
    }
    let status = call(g, Operation::Status(write.clone())).await?;
    let mut retry = None;
    let final_status = if visible_commit(&status, write, 5) {
        status.clone()
    } else {
        let result = call(g, Operation::Submit(write.clone())).await;
        match &result {
            Ok(reply) => assert!(matches!(reply, Reply::Write { .. })),
            Err(gateway::Error::Unknown) => {}
            other => anyhow::bail!("unexpected exact fifth Submit retry: {other:?}"),
        }
        retry = Some(result);
        call(g, Operation::Status(write.clone())).await?
    };
    assert!(visible_commit(&final_status, write, 5), "{final_status:?}");
    Ok(serde_json::json!({
        "initial_submit": initial,
        "status_after_initial": status,
        "exact_same_id_command_retry": retry,
        "final_status": final_status,
    }))
}

async fn open_local(
    g: &Gateway,
    version: u64,
    profile: Option<LocalReadProfile>,
) -> anyhow::Result<(LocalSession, QueryPage)> {
    let operation = if let Some(profile) = profile {
        Operation::OpenLocalQueryWithProfile {
            profile,
            version: Some(version),
            ttl_millis: 60_000,
            query: query(),
            page_size: 1,
        }
    } else {
        Operation::OpenLocalQuery {
            version: Some(version),
            ttl_millis: 60_000,
            query: query(),
            page_size: 1,
        }
    };
    let (session, page) = match (profile, call(g, operation).await?) {
        (
            None,
            Reply::OpenLocalQuery {
                session,
                query: echoed,
                page_size,
                page,
            },
        ) => {
            assert_eq!(echoed, query());
            assert_eq!(page_size, 1);
            (session, page)
        }
        (
            Some(expected),
            Reply::OpenLocalQueryWithProfile {
                profile: returned,
                session,
                query: echoed,
                page_size,
                page,
            },
        ) => {
            assert_eq!(returned, expected);
            assert_eq!(echoed, query());
            assert_eq!(page_size, 1);
            (session, page)
        }
        other => anyhow::bail!("unexpected local Open: {other:?}"),
    };
    assert_eq!(session.version, version);
    Ok((session, page))
}

#[derive(Clone)]
struct Ready {
    session: LocalSession,
    pages: Vec<(Option<PageCursor>, QueryPage)>,
}

impl Ready {
    async fn collect(
        g: &Gateway,
        session: LocalSession,
        first: QueryPage,
        expected: &[QueryRow],
    ) -> anyhow::Result<Self> {
        let mut cursor = None;
        let mut page = first;
        let mut pages = Vec::new();
        let mut rows = Vec::new();
        // A page must advance exactly one offset; expected length bounds this
        // fixture loop even if the implementation produces a cyclic cursor.
        loop {
            assert!(pages.len() < expected.len());
            assert_eq!(page.offset as usize, rows.len());
            assert_eq!(page.total as usize, expected.len());
            assert_eq!(page.data.completeness, QueryCompleteness::Unverified);
            assert_eq!(page.data.rows.len(), 1);
            rows.extend(page.data.rows.iter().cloned());
            let next = page.next.clone();
            pages.push((cursor, page));
            let Some(next) = next else { break };
            cursor = Some(next);
            page = fetch(g, &session, cursor.clone()).await?;
        }
        assert_eq!(
            rows, expected,
            "complete ordered bag must preserve duplicates"
        );
        let result = Self { session, pages };
        result.replay(g).await?;
        Ok(result)
    }

    async fn replay(&self, g: &Gateway) -> anyhow::Result<()> {
        for (cursor, expected) in &self.pages {
            assert_eq!(fetch(g, &self.session, cursor.clone()).await?, *expected);
        }
        Ok(())
    }
}

async fn fetch(
    g: &Gateway,
    session: &LocalSession,
    cursor: Option<PageCursor>,
) -> anyhow::Result<QueryPage> {
    match call(
        g,
        Operation::FetchLocalQuery {
            session: session.clone(),
            query: query(),
            cursor: cursor.clone(),
            page_size: 1,
        },
    )
    .await?
    {
        Reply::FetchLocalQuery {
            session: returned,
            query: echoed,
            cursor: returned_cursor,
            page_size,
            page,
        } => {
            assert_eq!(returned, *session);
            assert_eq!(echoed, query());
            assert_eq!(returned_cursor, cursor);
            assert_eq!(page_size, 1);
            Ok(page)
        }
        other => anyhow::bail!("unexpected Fetch: {other:?}"),
    }
}

pub(super) async fn scenario() -> anyhow::Result<()> {
    let scenario_started = Instant::now();
    let phase = |name: &str| {
        eprintln!(
            "capacity phase {name}: {} ms",
            scenario_started.elapsed().as_millis()
        )
    };
    phase("start");
    let coordinator = Process::start(9).await?;
    let mut a = Process::start(7).await?;
    let mut b = Process::start(8).await?;
    assert_eq!(
        BTreeSet::from([
            coordinator.child.id(),
            a.child.id(),
            b.child.id(),
            std::process::id(),
        ])
        .len(),
        4
    );
    assert_eq!(
        BTreeSet::from([coordinator.address, a.address, b.address]).len(),
        3
    );
    let c_client = coordinator.client()?;
    let a_client = a.client()?;
    let b_client = b.client()?;
    let graph = GraphCohort::from_endpoints(
        NodeRef::Remote(&c_client),
        authority(),
        1,
        vec![
            (NodeRef::Remote(&a_client), target(7)),
            (NodeRef::Remote(&b_client), target(8)),
        ],
    );
    assert_eq!(
        LocalReadProfile::Parity2MiBV1.capture_limit(),
        original_limit()
    );
    let expanded = LocalReadProfile::LocalRead8MiBV1.capture_limit();
    assert_eq!(
        expanded,
        ReadLimit {
            bytes: 8 * 1024 * 1024,
            ..original_limit()
        }
    );

    // Four documented typed managed GraphSnapshot -> Plan -> execute writes,
    // not raw KV/import and not Gateway Submit. These 510,000-byte properties
    // exceed the unchanged 64 KiB Gateway command / 256 KiB archived Plan caps.
    let mut direct_plans = Vec::new();
    for id in 1..=4 {
        let partition = if id % 2 == 1 { 7 } else { 8 };
        let snapshot = graph.snapshot(Some(id - 1), original_limit(), TTL).await?;
        assert_eq!(snapshot.version()?, id - 1);
        let plan = snapshot.prepare_vertex(partition, &payload_vertex(id, 510_000))?;
        assert_eq!(plan.validation_version(), id - 1);
        drop(snapshot);
        let initial = graph.execute(&plan).await;
        let (outcome, retried) = match initial {
            Ok(Outcome::Committed { visible: false, .. })
            | Err(managed_graph::Error::Uncertain) => (graph.execute(&plan).await?, true),
            other => (other?, false),
        };
        assert_eq!(
            outcome,
            Outcome::Committed {
                ordinal: id,
                visible: true
            }
        );
        phase(&format!("direct write {id} visible"));
        direct_plans.push(serde_json::json!({
            "vertex_id": id, "partition": partition, "plan_id": plan.id(),
            "validation_version": plan.validation_version(), "exact_plan_retry": retried,
        }));
    }
    phase("capture before");
    let before = capture_evidence(&c_client, &a_client, &b_client, 4, original_limit()).await?;
    assert_eq!(
        (before.rows, before.payload_bytes, before.replay_operations),
        (4, 2_040_332, 4)
    );
    let g = Gateway::bind_remote_local_queries(
        RemoteConfig {
            coordinator: c_client.clone(),
            binding: binding(),
            members: vec![a_client.clone(), b_client.clone()],
        },
        vec![route(&a).await?, route(&b).await?],
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    phase("legacy V4 open");
    let (old_session, old_page) = open_local(&g, 4, None).await?;
    let old = Ready::collect(&g, old_session, old_page, &[path(1), path(1)]).await?;
    phase("legacy V4 ready; submit fifth");
    let fifth = gateway_write(5, 60_000);
    let fifth_status = submit_fifth(&g, &fifth).await?;
    phase("fifth visible; replay original V4");
    old.replay(&g).await?;

    phase("capture after");
    let after = capture_evidence(&c_client, &a_client, &b_client, 5, expanded).await?;
    assert_eq!(
        (after.rows, after.payload_bytes, after.replay_operations),
        (5, 2_100_415, 5)
    );
    assert_eq!(after.payload_bytes - original_limit().bytes, 3_263);
    // All diagnostic snapshots/journals above have dropped before this sample.
    // This is a warmed debug/test run, not cold/release throughput evidence.
    phase("measure expanded V5 open");
    let resources_before = resource_sample(&[&coordinator, &a, &b]);
    let started = Instant::now();
    let (session, first) = open_local(&g, 5, Some(LocalReadProfile::LocalRead8MiBV1)).await?;
    let open_whole_result_seal_wall_micros = started.elapsed().as_micros();
    let resources_after = resource_sample(&[&coordinator, &a, &b]);
    let expected = vec![path(1), path(5), path(1)];
    phase("expanded V5 open done; collect");
    let current = Ready::collect(&g, session, first, &expected).await?;
    phase("fresh expanded V4 open");
    let (fresh_old, fresh_page) =
        open_local(&g, 4, Some(LocalReadProfile::LocalRead8MiBV1)).await?;
    let fresh_old = Ready::collect(&g, fresh_old, fresh_page, &[path(1), path(1)]).await?;
    phase("fresh expanded V4 ready; replay original V4");
    old.replay(&g).await?;

    phase("stop nodes");
    a.stop()?;
    b.stop()?;
    for ready in [&old, &current, &fresh_old] {
        ready.replay(&g).await?;
        ready.replay(&g).await?;
    }
    assert_eq!(
        call(
            &g,
            Operation::OpenLocalQueryWithProfile {
                profile: LocalReadProfile::LocalRead8MiBV1,
                version: Some(5),
                ttl_millis: 60_000,
                query: query(),
                page_size: 1,
            }
        )
        .await,
        Err(gateway::Error::Unknown),
        "new capture must fail on unavailable members, not hide behind Capacity"
    );
    phase("reopen nodes");
    a.reopen().await?;
    b.reopen().await?;
    for ready in [&old, &current, &fresh_old] {
        ready.replay(&g).await?;
        assert_eq!(
            call(
                &g,
                Operation::CloseLocalQuery {
                    session: ready.session.clone()
                }
            )
            .await?,
            Reply::CloseLocalQuery(ready.session.clone())
        );
        assert_eq!(
            call(
                &g,
                Operation::FetchLocalQuery {
                    session: ready.session.clone(),
                    query: query(),
                    cursor: None,
                    page_size: 1,
                }
            )
            .await,
            Err(gateway::Error::SessionUnavailable)
        );
    }
    // These independent negative probes must not consume the immutable Ready
    // lease. Keep every probe at the same V5 after all original cross-write,
    // stop/reopen and exact replay checks have completed and closed their views.
    assert_eq!(g.retained_payload_charge(), 0);
    g.shutdown().await;
    // Reopen gives each graph service a new ephemeral endpoint. Explicitly
    // verify those routes and construct a new test gateway for negative probes.
    // Never redirect, recapture or refresh any original Ready session/token.
    let g = Gateway::bind_remote_local_queries(
        RemoteConfig {
            coordinator: c_client.clone(),
            binding: binding(),
            members: vec![a_client.clone(), b_client.clone()],
        },
        vec![route(&a).await?, route(&b).await?],
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    assert_eq!(g.retained_payload_charge(), 0);
    phase("all ready results closed; new test gateway negative probes");
    // Preserve and separately expose the underlying old-cap rejection rather
    // than changing existing Gateway error mapping to simplify this test.
    assert!(matches!(
        graph.snapshot(Some(5), original_limit(), TTL).await,
        Err(managed_graph::Error::Snapshot(
            cohort_reads::Error::Application(D1Error::VisibilityReadLimit)
        ))
    ));
    assert_eq!(
        call(
            &g,
            Operation::OpenSession {
                version: Some(5),
                ttl_millis: 60_000,
            }
        )
        .await,
        Err(gateway::Error::Rejected)
    );
    assert_eq!(
        call(
            &g,
            Operation::OpenLocalQuery {
                version: Some(5),
                ttl_millis: 60_000,
                query: query(),
                page_size: 1,
            }
        )
        .await,
        Err(gateway::Error::Capacity)
    );
    assert_eq!(
        call(
            &g,
            Operation::OpenLocalQueryWithProfile {
                profile: LocalReadProfile::Parity2MiBV1,
                version: Some(5),
                ttl_millis: 60_000,
                query: query(),
                page_size: 1,
            }
        )
        .await,
        Err(gateway::Error::Capacity)
    );
    phase("negative captures done; next write");
    let sixth = gateway_write(6, 1);
    assert_eq!(
        call(&g, Operation::Submit(sixth.clone())).await,
        Err(gateway::Error::Rejected)
    );
    assert_eq!(
        call(&g, Operation::Status(sixth.clone())).await?,
        Reply::Write {
            external_id: sixth.external_id,
            transaction_id: None,
            status: WriteStatus::Absent,
        }
    );

    phase("negative probes done; terminal verification");
    let terminal = NodeRef::Remote(&c_client)
        .terminal(&authority(), 1, 5)
        .await?;
    assert_eq!(terminal.version(), 5);
    let final_journal = NodeRef::Remote(&c_client).journal(&authority(), 1).await?;
    assert_eq!(final_journal.resolved_prefix(), 5);
    assert!(final_journal.transaction_at(6).is_none());
    let report = serde_json::json!({
        "fixture": "fresh_managed_local_capacity_m2",
        "build_debug_assertions": cfg!(debug_assertions),
        "actual_version": 5,
        "apis": {
            "classification": "mixed typed managed APIs, not all-Gateway loading",
            "setup": "GraphCohort.snapshot -> GraphSnapshot.prepare_vertex -> GraphCohort.execute",
            "setup_plans": direct_plans,
            "final_write": "Gateway Submit external_id [5;16], unchanged 60000-byte Text payload",
            "final_write_outcome": fifth_status,
            "next_write": "Gateway Submit [6;16] Rejected, Status Absent, terminal prefix remains 5",
        },
        "before": before,
        "after": after,
        "query": query(),
        "page_size": 1,
        "profile": LocalReadProfile::LocalRead8MiBV1,
        "result_rows": expected,
        "result_rows_json_bytes": serde_json::to_vec(&expected)?.len(),
        "open_whole_result_seal_wall_micros": open_whole_result_seal_wall_micros,
        "measurement_scope": "warm diagnostic reads precede Open; wall time includes client TCP/Open/whole-result sealing, not later page replay; no cold or release throughput claim",
        "resources_before_measured_open": resources_before,
        "resources_after_measured_open": resources_after,
        "resource_caveats": "VmHWM includes setup; CPU ticks include background activity; optional /proc values are null when unavailable",
        "lock_duration": null,
        "certification_scratch_peak_bytes": null,
        "node_pool": "128 MiB logical payload reservation, not RSS",
        "negative_probe_scope": "same V5 after original Ready stop/reopen/replay/Close; explicitly new test gateway with verified restarted graph endpoints; no original session refresh; does not cover negative probes interleaved with live Ready",
        "unproven": ["full 8 MiB loading", "sustained writes", "#111", "full managed CoreHR", "multi-host HA"],
        "assertions": "old V4 and new V5 ordered bags, exact page replay with stopped/reopened members, fail-closed new capture, close invalidation, unchanged old rejection mapping",
    });
    let json = serde_json::to_vec_pretty(&report)?;
    eprintln!(
        "managed local capacity evidence:\n{}",
        std::str::from_utf8(&json)?
    );
    let evidence_path = std::env::var_os(EVIDENCE_ENV)
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("VELAGRAPH_REMOTE_EVIDENCE_DIR")
                .map(|directory| PathBuf::from(directory).join("managed-local-capacity.json"))
        });
    if let Some(path) = evidence_path {
        fs::write(path, json)?;
    }
    g.shutdown().await;
    Ok(())
}
