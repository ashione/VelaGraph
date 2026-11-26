//! Deliver two committed graph writes to a restarted learner in a controlled order.
use super::*;
use d2_raft::{raft_grpc::GrpcNetworkFactory, DurableLogStore, StateMachine, TypeConfig};
use openraft::{
    network::{RPCOption, RaftNetwork, RaftNetworkFactory},
    raft::{AppendEntriesRequest, AppendEntriesResponse},
    storage::{RaftLogReader, RaftLogStorage},
    BasicNode,
};
use replicated_graph::{run_blocking, RemoteStorageNode};
use velagraph::{Edge, GraphEngine, KeyRange, ScanLimit, StorageNode, Vertex};

const LEARNER: usize = 4;
const SNAPSHOT_ROWS: usize = 8;
const SNAPSHOT_BYTES: usize = 64 * 1024;

async fn wait_applied(control: SocketAddr, expected: u64) -> anyhow::Result<()> {
    timeout(BUDGET, async {
        loop {
            if let Ok(ControlResponse::Status(status)) =
                send_control(&control.to_string(), ControlRequest::Status).await
            {
                ensure!(status.id == LEARNER as u64, "unexpected member {status:?}");
                if status.applied >= expected {
                    ensure!(
                        status.applied == expected,
                        "unexpected applied index {status:?}"
                    );
                    return anyhow::Ok(());
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .context("learner did not reach expected applied index")?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_learner_handles_reordered_and_duplicate_graph_appends() -> anyhow::Result<()> {
    timeout(Duration::from_secs(90), delivery()).await?
}

async fn delivery() -> anyhow::Result<()> {
    let mut cluster = Cluster::start().await?;
    let leader = cluster.leader;
    let node = RemoteStorageNode::new_identified(
        cluster.grpc(leader),
        GROUP,
        cluster.identity(leader).node,
        SHARD,
        BUDGET,
    )?;
    let graph = run_blocking(move || -> velagraph::Result<_> {
        let graph = GraphEngine::from_storage_node(node, SHARD, 4);
        graph.create_vertex(&Vertex::new(1))?;
        graph.create_vertex(&Vertex::new(2))?;
        Ok(graph)
    })
    .await??;

    let dir = tempfile::tempdir()?;
    let addresses = reserve_addresses(4).await?;
    let mut learner = ObservedProcess::spawn(LEARNER, &addresses, dir.path(), None)?;
    let response = send_control(
        &cluster.addresses[4 * leader + 1].to_string(),
        ControlRequest::AddLearner {
            id: LEARNER as u64,
            addr: addresses[0].to_string(),
            blocking: true,
        },
    )
    .await?;
    let ControlResponse::Index(base) = response else {
        anyhow::bail!("unexpected AddLearner response {response:?}")
    };
    wait_applied(addresses[1], base).await?;
    learner.process.0.kill()?;
    learner.process.0.wait()?;
    learner.success = true;
    drop(learner);

    let expected = run_blocking(move || -> velagraph::Result<_> {
        graph.create_edge(&Edge::new(9, 1, 2, "member_of"))?;
        let moved = Edge::new(9, 2, 1, "contains");
        graph.update_edge(&moved)?;
        assert_eq!(graph.get_edge(9)?, Some(moved.clone()));
        assert_eq!(graph.out_edges(2, Some("contains"))?, vec![moved.clone()]);
        assert_eq!(graph.in_edges(1, Some("contains"))?, vec![moved]);
        assert!(graph.out_edges(1, Some("member_of"))?.is_empty());
        assert!(graph.in_edges(2, Some("member_of"))?.is_empty());
        assert_eq!(graph.get_vertex(1)?, Some(Vertex::new(1)));
        assert_eq!(graph.get_vertex(2)?, Some(Vertex::new(2)));
        let view = graph.records().node().read_view()?;
        assert_eq!(view.sequence, 4);
        let entries = view.scan(
            KeyRange::all(),
            ScanLimit::new(SNAPSHOT_ROWS, SNAPSHOT_BYTES)?,
        )?;
        assert_eq!(
            entries.len(),
            5,
            "two vertices, one edge, two adjacency entries"
        );
        Ok(entries
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect::<std::collections::BTreeMap<_, _>>())
    })
    .await??;
    // Stop every sender before waking the learner. Only the scheduled appends
    // below can fill its missing suffix.
    cluster.stop()?;
    let [first, second] = committed_appends(cluster._dirs[leader].path(), base).await?;
    let mut learner = ObservedProcess::spawn(LEARNER, &addresses, dir.path(), None)?;
    wait_applied(addresses[1], base).await?;
    let identity = RemoteIdentity {
        group: GROUP,
        node: LEARNER as u64,
    };
    let mut observer = timeout(BUDGET, async {
        loop {
            match Client::connect(addresses[3], identity, BUDGET).await {
                Ok(client) => return Ok(client),
                Err(RemoteError::Unavailable) => sleep(Duration::from_millis(20)).await,
                Err(error) => return Err(error),
            }
        }
    })
    .await??;
    let mut peer = GrpcNetworkFactory
        .new_client(LEARNER as u64, &BasicNode::new(addresses[0]))
        .await;
    let send = || RPCOption::new(BUDGET);
    assert!(matches!(
        peer.append_entries(second.clone(), send()).await?,
        AppendEntriesResponse::Conflict
    ));
    wait_applied(addresses[1], base).await?;
    assert_eq!(observer.metadata(SHARD).await?, (SHARD, 3, 2));
    for (request, index, sequence) in [
        (first.clone(), base + 1, 3),
        (first.clone(), base + 1, 3),
        (second.clone(), base + 2, 4),
        (first, base + 2, 4),
        (second, base + 2, 4),
    ] {
        assert!(matches!(
            peer.append_entries(request, send()).await?,
            AppendEntriesResponse::Success
        ));
        wait_applied(addresses[1], index).await?;
        assert_eq!(observer.metadata(SHARD).await?, (SHARD, 3, sequence));
    }
    learner.process.0.kill()?;
    learner.process.0.wait()?;
    // Use the production state loader on the stopped process's own database.
    let state = StateMachine::open(dir.path().join("state.db"))?;
    let recovered = state
        .d1_read_snapshot(SNAPSHOT_ROWS, SNAPSHOT_BYTES)
        .await
        .map_err(|_| anyhow::anyhow!("unexpected recovery snapshot size"))?
        .context("missing D1 state")?;
    assert_eq!(
        (recovered.shard, recovered.epoch, recovered.sequence),
        (SHARD, 3, 4)
    );
    assert_eq!(recovered.entries, expected);
    learner.success = true;
    Ok(())
}

async fn committed_appends(
    dir: &std::path::Path,
    base: u64,
) -> anyhow::Result<[AppendEntriesRequest<TypeConfig>; 2]> {
    let mut logs = DurableLogStore::open(dir.join("raft.db"))?;
    let vote = logs.read_vote().await?.context("missing source vote")?;
    let entries = logs.try_get_log_entries(base..=base + 2).await?;
    let [prefix, create, update] = entries.as_slice() else {
        anyhow::bail!("missing graph log suffix")
    };
    assert_eq!(
        [
            prefix.log_id.index,
            create.log_id.index,
            update.log_id.index
        ],
        [base, base + 1, base + 2]
    );
    for (entry, sequence) in [(create, 2), (update, 3)] {
        let openraft::EntryPayload::Normal(d2_raft::LogCommand::D1V1(d2_raft::D1Command::Write(
            write,
        ))) = &entry.payload
        else {
            anyhow::bail!("expected a graph write at {:?}", entry.log_id);
        };
        assert_eq!(
            (write.shard, write.epoch, write.expected_sequence),
            (SHARD, 3, Some(sequence))
        );
    }
    let append = |previous: &openraft::Entry<TypeConfig>, entry: &openraft::Entry<TypeConfig>| {
        AppendEntriesRequest {
            vote,
            prev_log_id: Some(previous.log_id),
            entries: vec![entry.clone()],
            leader_commit: Some(entry.log_id),
        }
    };
    Ok([append(prefix, create), append(create, update)])
}
