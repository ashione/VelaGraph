//! Real three-voter metadata and data groups. Numeric replica and graph IDs
//! deliberately overlap: placement must select both the group and partition.
use std::{collections::BTreeMap, time::Duration};

use d2_raft::{Cluster, D1Command, D1Result, LogCommand, NodeHandle};
use openraft::{storage::RaftStateMachine, BasicNode};
use replicated_graph::{
    catalog::{Catalog, Placement},
    routing::{RouteError, RouteResolver, RoutedPreparedMutation, RoutedReadView},
    run_blocking,
};
use tokio::time::{sleep, timeout};
use velagraph::{GremlinRow, KeyRange, ScanLimit, StorageNodeError, Value, Vertex};

const BUDGET: Duration = Duration::from_secs(20);
fn placement(group: u64) -> Placement {
    Placement {
        shard: group + 100,
        group,
        epoch: 1,
        replicas: vec![1, 2, 3],
    }
}
async fn leader(nodes: &[NodeHandle], excluded: Option<u64>) -> anyhow::Result<usize> {
    Ok(timeout(Duration::from_secs(10), async {
        loop {
            for (i, node) in nodes.iter().enumerate() {
                if Some(node.id) != excluded
                    && node.raft.metrics().borrow().current_leader == Some(node.id)
                    && matches!(
                        timeout(Duration::from_millis(150), node.raft.ensure_linearizable()).await,
                        Ok(Ok(_))
                    )
                {
                    return i;
                }
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await?)
}
struct Group {
    cluster: Cluster,
    nodes: Vec<NodeHandle>,
    placement: Placement,
}
impl Group {
    async fn start(root: &Cluster, group: u64) -> anyhow::Result<Self> {
        let cluster = root.in_group(group);
        let mut nodes = Vec::new();
        for id in 1..=3 {
            nodes.push(cluster.start(id).await?);
        }
        nodes[0]
            .raft
            .initialize(
                (1..=3)
                    .map(|id| (id, BasicNode::default()))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await?;
        let current = leader(&nodes, None).await?;
        let placement = placement(group);
        let result = nodes[current]
            .raft
            .client_write(LogCommand::D1V1(D1Command::Activate {
                shard: placement.shard,
                epoch: 1,
            }))
            .await?;
        assert_eq!(
            result.data.d1.unwrap(),
            Ok(D1Result::Activated {
                shard: placement.shard,
                epoch: 1
            })
        );
        Ok(Self {
            cluster,
            nodes,
            placement,
        })
    }
    async fn converge(&self) -> anyhow::Result<()> {
        let current = leader(&self.nodes, None).await?;
        let index = self.nodes[current]
            .state_machine
            .clone()
            .applied_state()
            .await?
            .0
            .unwrap()
            .index;
        for n in &self.nodes {
            self.cluster.wait_for_applied(n, index, 10_000).await?;
        }
        Ok(())
    }
    async fn isolate(&self, id: u64) {
        for n in &self.nodes {
            if n.id != id {
                self.cluster.policy.deny(id, n.id).await;
                self.cluster.policy.deny(n.id, id).await;
            }
        }
    }
    async fn stop(&self) {
        for n in &self.nodes {
            self.cluster.shutdown(n).await;
        }
    }
}
struct Fixture {
    metadata: Group,
    a: Group,
    b: Group,
    resolver: RouteResolver,
}
impl Fixture {
    async fn new() -> anyhow::Result<Self> {
        let root = Cluster::new();
        let metadata = Group::start(&root, 10).await?;
        let mut a = Group::start(&root, 20).await?;
        let mut b = Group::start(&root, 30).await?;
        let i = leader(&metadata.nodes, None).await?;
        let catalog = Catalog::new(&metadata.nodes[i], metadata.placement.clone(), BUDGET)?;
        for (partition, p) in [(1, a.placement.clone()), (2, b.placement.clone())] {
            let c = catalog.clone();
            run_blocking(move || {
                let prepared = c.prepare_registration([partition as u8; 16], partition, p)?;
                c.commit_registration(&prepared)
            })
            .await??;
        }
        let mut data = std::mem::take(&mut a.nodes);
        data.append(&mut b.nodes);
        let resolver = RouteResolver::new(metadata.placement.clone(), &metadata.nodes, &data)?;
        b.nodes = data.split_off(3);
        a.nodes = data;
        Ok(Self {
            metadata,
            a,
            b,
            resolver,
        })
    }
    async fn stop(&self) {
        self.metadata.stop().await;
        self.a.stop().await;
        self.b.stop().await;
    }
}
async fn read(r: &RouteResolver, partition: u32) -> Result<RoutedReadView, RouteError> {
    let r = r.clone();
    run_blocking(move || r.read_view(partition, BUDGET)).await?
}
async fn commit(
    r: &RouteResolver,
    p: &RoutedPreparedMutation,
    budget: Duration,
) -> Result<(), RouteError> {
    let (r, p) = (r.clone(), p.clone());
    run_blocking(move || r.commit_prepared(&p, budget)).await?
}
async fn prepare(
    r: &RouteResolver,
    partition: u32,
    id: u64,
    label: &'static str,
) -> anyhow::Result<RoutedPreparedMutation> {
    let view = read(r, partition).await?;
    Ok(run_blocking(move || {
        let mut v = Vertex::new(id);
        v.labels.push(label.into());
        view.prepare_create_vertex(&v)
    })
    .await??)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_ids_unknown_partition_and_metadata_quorum_fail_closed() -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), async {
        let f = Fixture::new().await?;
        for (partition, label) in [(1, "alpha"), (2, "beta")] {
            let p = prepare(&f.resolver, partition, 1, label).await?;
            commit(&f.resolver, &p, BUDGET).await?;
        }
        for (partition, label, group) in [(1, "alpha", 20), (2, "beta", 30)] {
            let view = read(&f.resolver, partition).await?;
            run_blocking(move || {
                assert_eq!(view.placement().group, group);
                assert_eq!(view.sequence(), 1);
                assert_eq!(view.get_vertex(1)?.unwrap().labels, vec![label]);
                assert_eq!(
                    view.execute_gremlin("g.V().label()")?.rows,
                    vec![GremlinRow::Value(Value::Text(label.into()))]
                );
                Ok::<_, RouteError>(())
            })
            .await??;
        }
        assert!(matches!(
            read(&f.resolver, 999).await,
            Err(RouteError::MissingPartition(999))
        ));
        let pending = prepare(&f.resolver, 1, 2, "pending").await?;
        // Remove every metadata quorum, leaving both data groups healthy.
        for n in &f.metadata.nodes {
            f.metadata.isolate(n.id).await;
        }
        for partition in [1, 999] {
            let r = f.resolver.clone();
            let result =
                run_blocking(move || r.read_view(partition, Duration::from_millis(300))).await?;
            assert!(matches!(
                result,
                Err(RouteError::Storage(
                    StorageNodeError::Timeout { .. }
                        | StorageNodeError::Unavailable(_)
                        | StorageNodeError::NotLeader { .. }
                ))
            ));
        }
        assert!(matches!(
            commit(&f.resolver, &pending, Duration::from_millis(300)).await,
            Err(RouteError::Storage(
                StorageNodeError::Timeout { .. }
                    | StorageNodeError::Unavailable(_)
                    | StorageNodeError::NotLeader { .. }
            ))
        ));
        for group in [&f.a, &f.b] {
            group.converge().await?;
            for n in &group.nodes {
                assert_eq!(
                    n.state_machine.d1_metadata().await,
                    Some((group.placement.shard, 1, 1))
                );
            }
        }
        f.metadata.cluster.policy.clear().await;
        f.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_keeps_retained_pages_and_queries_historical_then_fences_epoch(
) -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), async {
        let f = Fixture::new().await?;
        for id in [1, 2] {
            let p = prepare(&f.resolver, 1, id, "original").await?;
            commit(&f.resolver, &p, BUDGET).await?;
        }
        let retained = read(&f.resolver, 1).await?;
        let v = retained.clone();
        let first = run_blocking(move || {
            v.scan_page(KeyRange::all(), ScanLimit::new(1, 4096).unwrap(), None)
        })
        .await??;
        assert_eq!(first.rows.len(), 1);
        let cursor = first.next_cursor.expect("two vertices need a second page");
        f.a.converge().await?;
        let old = leader(&f.a.nodes, None).await?;
        let old_id = f.a.nodes[old].id;
        f.a.isolate(old_id).await;
        let new = leader(&f.a.nodes, Some(old_id)).await?;
        assert_ne!(new, old);
        f.a.cluster.policy.clear().await;
        timeout(Duration::from_secs(10), async {
            while f.a.nodes[old].raft.metrics().borrow().current_leader != Some(f.a.nodes[new].id) {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await?;
        // The same resolver must discover the new leader, without replacing handles.
        let p = prepare(&f.resolver, 1, 3, "later").await?;
        commit(&f.resolver, &p, BUDGET).await?;
        let fresh = read(&f.resolver, 1).await?;
        assert_eq!(fresh.sequence(), 3);
        run_blocking(move || {
            assert_eq!(
                fresh.execute_gremlin("g.V().count()")?.rows,
                vec![GremlinRow::Count(3)]
            );
            assert!(fresh.execute_gremlin("g.addV('forbidden')").is_err());
            Ok::<_, RouteError>(())
        })
        .await??;
        let v = retained.clone();
        run_blocking(move || {
            assert_eq!(v.sequence(), 2);
            assert!(v.get_vertex(3)?.is_none());
            assert_eq!(
                v.execute_gremlin("g.V().count()")?.rows,
                vec![GremlinRow::Count(2)]
            );
            let page = v.scan_page(
                KeyRange::all(),
                ScanLimit::new(1, 4096).unwrap(),
                Some(&cursor),
            )?;
            assert_eq!(page.rows.len(), 1);
            assert!(
                page.next_cursor.is_none(),
                "must not reroute into sequence 3"
            );
            Ok::<_, RouteError>(())
        })
        .await??;
        let result = f.a.nodes[new]
            .raft
            .client_write(LogCommand::D1V1(D1Command::SetEpoch {
                shard: f.a.placement.shard,
                expected_epoch: 1,
                new_epoch: 2,
            }))
            .await?;
        assert_eq!(result.data.d1.unwrap(), Ok(D1Result::Epoch { epoch: 2 }));
        f.a.converge().await?;
        let result = run_blocking(move || retained.get_vertex(1)).await?;
        assert!(matches!(
            result,
            Err(RouteError::Storage(StorageNodeError::Fenced {
                current: 2,
                ..
            }))
        ));
        assert!(matches!(
            read(&f.resolver, 1).await,
            Err(RouteError::Storage(StorageNodeError::Fenced {
                current: 2,
                ..
            }))
        ));
        assert!(matches!(
            commit(&f.resolver, &p, BUDGET).await,
            Err(RouteError::Storage(StorageNodeError::Fenced {
                current: 2,
                ..
            }))
        ));
        assert_eq!(read(&f.resolver, 2).await?.sequence(), 0);
        f.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn apply_locked_timeout_exact_retry_and_conflict_do_not_add_sequences() -> anyhow::Result<()>
{
    timeout(Duration::from_secs(60), async {
        let f = Fixture::new().await?;
        let p = prepare(&f.resolver, 1, 1, "committed").await?;
        let loser = prepare(&f.resolver, 1, 2, "conflicting").await?;
        assert_eq!(p.expected_sequence(), loser.expected_sequence());
        let i = leader(&f.a.nodes, None).await?;
        let guard = f.a.nodes[i].state_machine.pause_state().await;
        let result = commit(&f.resolver, &p, Duration::from_millis(500)).await;
        assert!(matches!(result, Err(RouteError::Storage(StorageNodeError::Timeout { request_id: Some(id) })) if id == p.request_id()));
        drop(guard);
        timeout(Duration::from_secs(5), async {
            while f.a.nodes[i].state_machine.d1_metadata().await != Some((f.a.placement.shard, 1, 1)) { sleep(Duration::from_millis(10)).await; }
        }).await?;
        // No re-preparation: preserve bytes, request identity and original CAS.
        commit(&f.resolver, &p, BUDGET).await?;
        commit(&f.resolver, &p, BUDGET).await?;
        for _ in 0..2 {
            assert!(matches!(commit(&f.resolver, &loser, BUDGET).await, Err(RouteError::Storage(StorageNodeError::Conflict { expected: 0, current: 1 }))));
        }
        f.a.converge().await?;
        for n in &f.a.nodes { assert_eq!(n.state_machine.d1_metadata().await, Some((f.a.placement.shard, 1, 1))); }
        let view = read(&f.resolver, 1).await?;
        run_blocking(move || { assert!(view.get_vertex(1)?.is_some()); assert!(view.get_vertex(2)?.is_none()); Ok::<_, RouteError>(()) }).await??;
        f.stop().await;
        Ok::<_, anyhow::Error>(())
    }).await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stopped_first_candidates_do_not_hide_healthy_metadata_and_data_quorums(
) -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), async {
        let f = Fixture::new().await?;
        let p = prepare(&f.resolver, 1, 1, "before").await?;
        commit(&f.resolver, &p, BUDGET).await?;
        f.metadata.converge().await?;
        f.a.converge().await?;
        // The registered NodeHandle remains, but candidate 1 cannot answer.
        // Both groups still have the two voters needed for a fresh quorum.
        f.metadata.cluster.shutdown(&f.metadata.nodes[0]).await;
        f.a.cluster.shutdown(&f.a.nodes[0]).await;
        leader(&f.metadata.nodes, Some(1)).await?;
        leader(&f.a.nodes, Some(1)).await?;
        let view = read(&f.resolver, 1).await?;
        assert_eq!(view.sequence(), 1);
        let next = run_blocking(move || {
            assert!(view.get_vertex(1)?.is_some());
            view.prepare_create_vertex(&Vertex::new(2))
        })
        .await??;
        commit(&f.resolver, &next, BUDGET).await?;
        commit(&f.resolver, &p, BUDGET).await?;
        assert_eq!(read(&f.resolver, 1).await?.sequence(), 2);
        f.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn routed_crud_and_retained_session_absolute_deadline() -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), async {
        let f = Fixture::new().await?;
        for id in [1, 2] {
            let p = prepare(&f.resolver, 1, id, "original").await?;
            commit(&f.resolver, &p, BUDGET).await?;
        }
        let v = read(&f.resolver, 1).await?;
        let edge =
            run_blocking(move || v.prepare_create_edge(&velagraph::Edge::new(10, 1, 2, "knows")))
                .await??;
        commit(&f.resolver, &edge, BUDGET).await?;
        let v = read(&f.resolver, 1).await?;
        let update = run_blocking(move || {
            let mut edge = v.get_edge(10)?.unwrap();
            edge.label = "likes".into();
            v.prepare_update_edge(&edge)
        })
        .await??;
        commit(&f.resolver, &update, BUDGET).await?;
        let v = read(&f.resolver, 1).await?;
        let update = run_blocking(move || {
            assert_eq!(v.get_edge(10)?.unwrap().label, "likes");
            assert_eq!(
                v.execute_gremlin("g.V(1).out('likes').count()")?.rows,
                vec![GremlinRow::Count(1)]
            );
            let mut vertex = v.get_vertex(1)?.unwrap();
            vertex.labels = vec!["updated".into()];
            v.prepare_update_vertex(&vertex)
        })
        .await??;
        commit(&f.resolver, &update, BUDGET).await?;
        let v = read(&f.resolver, 1).await?;
        let delete = run_blocking(move || v.prepare_delete_edge(10)).await??;
        commit(&f.resolver, &delete, BUDGET).await?;
        let v = read(&f.resolver, 1).await?;
        let delete = run_blocking(move || {
            assert!(v.get_edge(10)?.is_none());
            assert_eq!(v.get_vertex(1)?.unwrap().labels, vec!["updated"]);
            v.prepare_delete_vertex(1)
        })
        .await??;
        commit(&f.resolver, &delete, BUDGET).await?;
        let r = f.resolver.clone();
        let expired = run_blocking(move || r.read_view(1, Duration::from_millis(200))).await??;
        let clone = expired.clone();
        sleep(Duration::from_millis(250)).await;
        run_blocking(move || {
            assert!(matches!(
                expired.get_vertex(2),
                Err(RouteError::Storage(StorageNodeError::Timeout { .. }))
            ));
            assert!(matches!(
                clone.scan_page(KeyRange::all(), ScanLimit::default(), None),
                Err(RouteError::Storage(StorageNodeError::Timeout { .. }))
            ));
            assert!(matches!(
                clone.execute_gremlin("g.V().count()"),
                Err(RouteError::Storage(StorageNodeError::Timeout { .. }))
            ));
            assert!(matches!(
                clone.prepare_delete_vertex(2),
                Err(RouteError::Storage(StorageNodeError::Timeout { .. }))
            ));
        })
        .await?;
        let fresh = read(&f.resolver, 1).await?;
        run_blocking(move || {
            assert!(fresh.get_vertex(1)?.is_none());
            assert!(fresh.get_vertex(2)?.is_some());
            Ok::<_, RouteError>(())
        })
        .await??;
        f.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn route_identity_validation_and_fresh_metadata_sequence() -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), async {
        let f = Fixture::new().await?;
        for replicas in [vec![], vec![1, 1, 3], vec![3, 2, 1], (1..=32).collect()] {
            let metadata = Placement {
                replicas,
                ..f.metadata.placement.clone()
            };
            assert!(matches!(
                RouteResolver::new(metadata, &f.metadata.nodes, &f.a.nodes),
                Err(RouteError::Catalog(
                    replicated_graph::catalog::CatalogError::InvalidArgument(_)
                ))
            ));
        }
        assert!(matches!(
            RouteResolver::new(
                Placement {
                    group: 999,
                    ..f.metadata.placement.clone()
                },
                &f.metadata.nodes,
                &f.a.nodes
            ),
            Err(RouteError::Catalog(
                replicated_graph::catalog::CatalogError::MetadataMismatch
            ))
        ));
        assert!(matches!(
            RouteResolver::new(
                f.metadata.placement.clone(),
                &f.metadata.nodes[..2],
                &f.a.nodes
            ),
            Err(RouteError::InvalidArgument(_))
        ));
        assert!(matches!(
            RouteResolver::new(
                f.metadata.placement.clone(),
                &f.metadata.nodes,
                &f.metadata.nodes
            ),
            Err(RouteError::InvalidArgument(_))
        ));
        let incomplete = RouteResolver::new(
            f.metadata.placement.clone(),
            &f.metadata.nodes,
            &f.a.nodes[..2],
        )?;
        assert!(matches!(
            read(&incomplete, 1).await,
            Err(RouteError::MissingReplica { group: 20, node: 3 })
        ));

        let historical = read(&f.resolver, 1).await?;
        assert_eq!(historical.metadata_sequence(), 2);
        let v = historical.clone();
        let prepared = run_blocking(move || v.prepare_create_vertex(&Vertex::new(1))).await??;
        commit(&f.resolver, &prepared, BUDGET).await?;
        let i = leader(&f.metadata.nodes, None).await?;
        let catalog = Catalog::new(&f.metadata.nodes[i], f.metadata.placement.clone(), BUDGET)?;
        let p = f.b.placement.clone();
        run_blocking(move || {
            let registration = catalog.prepare_registration([3; 16], 3, p)?;
            catalog.commit_registration(&registration)
        })
        .await??;
        let fresh = read(&f.resolver, 1).await?;
        assert_eq!(fresh.metadata_sequence(), 3);
        assert_eq!(
            historical.metadata_sequence(),
            2,
            "retained metadata revision is historical"
        );
        assert_eq!(fresh.placement(), prepared.placement());
        commit(&f.resolver, &prepared, BUDGET).await?;
        assert_eq!(
            read(&f.resolver, 1).await?.sequence(),
            1,
            "unrelated metadata revision must not change exact retry identity"
        );

        // Reuse the empty second data group as an independent metadata authority.
        // Its partition 1 placement is byte-for-byte equal, but its authority is not.
        let i = leader(&f.b.nodes, None).await?;
        let other_catalog = Catalog::new(&f.b.nodes[i], f.b.placement.clone(), BUDGET)?;
        let p = f.a.placement.clone();
        run_blocking(move || {
            let registration = other_catalog.prepare_registration([4; 16], 1, p)?;
            other_catalog.commit_registration(&registration)
        })
        .await??;
        let other = RouteResolver::new(f.b.placement.clone(), &f.b.nodes, &f.a.nodes)?;
        assert_eq!(read(&other, 1).await?.placement(), prepared.placement());
        assert!(matches!(
            commit(&other, &prepared, BUDGET).await,
            Err(RouteError::PlacementMismatch)
        ));
        assert_eq!(read(&f.resolver, 1).await?.sequence(), 1);
        f.stop().await;
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_retained_admission_requires_last_clone_drop() -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), async {
        let f = Fixture::new().await?;
        let mut views = Vec::new();
        for _ in 0..32 {
            views.push(read(&f.resolver, 1).await?);
        }
        assert!(matches!(
            read(&f.resolver, 2).await,
            Err(RouteError::Storage(StorageNodeError::ResourceExhausted(_)))
        ));
        let held = views.pop().unwrap();
        let clone = held.clone();
        drop(held);
        assert!(matches!(
            read(&f.resolver, 2).await,
            Err(RouteError::Storage(StorageNodeError::ResourceExhausted(_)))
        ));
        drop(clone);
        views.push(read(&f.resolver, 2).await?);
        assert!(matches!(
            read(&f.resolver, 1).await,
            Err(RouteError::Storage(StorageNodeError::ResourceExhausted(_)))
        ));
        drop(views);
        assert!(read(&f.resolver, 1).await.is_ok());
        f.stop().await;
        Ok(())
    })
    .await?
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_metadata_and_data_share_deadline() -> anyhow::Result<()> {
    timeout(Duration::from_secs(60), async {
        let f = Fixture::new().await?;
        let mi = leader(&f.metadata.nodes, None).await?;
        let di = leader(&f.a.nodes, None).await?;
        let mg = f.metadata.nodes[mi].state_machine.pause_state().await;
        let dg = f.a.nodes[di].state_machine.pause_state().await;
        let r = f.resolver.clone();
        let (ready, begun) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            run_blocking(move || {
                ready.send(std::time::Instant::now()).unwrap();
                r.read_view(1, Duration::from_secs(2))
            })
            .await
        });
        let started = begun.await?;
        sleep(Duration::from_millis(1500)).await;
        drop(mg);
        let result = task.await??;
        assert!(matches!(
            result,
            Err(RouteError::Storage(StorageNodeError::Timeout { .. }))
        ));
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "deadline renewed: {:?}",
            started.elapsed()
        );
        drop(dg);
        f.stop().await;
        Ok(())
    })
    .await?
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_expired_sessions_still_consume_slots_and_runtime_rejects() -> anyhow::Result<()>
{
    timeout(Duration::from_secs(60), async {
        let f = Fixture::new().await?;
        assert!(matches!(
            f.resolver.read_view(1, BUDGET),
            Err(RouteError::Storage(StorageNodeError::Unsupported(_)))
        ));
        let mut views = Vec::new();
        for _ in 0..32 {
            let r = f.resolver.clone();
            views.push(run_blocking(move || r.read_view(1, Duration::from_millis(200))).await??);
        }
        sleep(Duration::from_millis(250)).await;
        assert!(matches!(
            read(&f.resolver, 1).await,
            Err(RouteError::Storage(StorageNodeError::ResourceExhausted(_)))
        ));
        drop(views);
        assert!(read(&f.resolver, 1).await.is_ok());
        f.stop().await;
        Ok(())
    })
    .await?
}
