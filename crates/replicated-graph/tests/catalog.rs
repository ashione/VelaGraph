//! Durable three-voter catalog tests. Retained preparations are the retry identity;
//! a fresh lookup always needs the metadata quorum, never a local placement cache.
use std::{collections::BTreeMap, time::Duration};

use d2_raft::{Cluster, D1Command, D1Result, LogCommand, NodeHandle};
use openraft::{
    storage::{RaftSnapshotBuilder, RaftStateMachine},
    BasicNode,
};
use replicated_graph::{
    catalog::{Catalog, CatalogError, Placement, PreparedRegistration},
    run_blocking,
};
use tokio::time::{sleep, timeout};
use velagraph::{StorageNodeError, WriteResult, WriteStatus};

fn metadata() -> Placement {
    Placement {
        shard: 900,
        group: 100,
        epoch: 1,
        replicas: vec![1, 2, 3],
    }
}
fn placement(shard: u64) -> Placement {
    Placement {
        shard,
        group: shard + 200,
        epoch: 1,
        replicas: vec![1, 2, 3],
    }
}
fn catalog(node: &NodeHandle) -> Catalog {
    Catalog::new(node, metadata(), Duration::from_millis(800)).unwrap()
}
async fn on<T: Send + 'static>(
    c: &Catalog,
    f: impl FnOnce(&Catalog) -> Result<T, CatalogError> + Send + 'static,
) -> Result<T, CatalogError> {
    let c = c.clone();
    run_blocking(move || f(&c)).await?
}
async fn prepare(
    c: &Catalog,
    id: u8,
    partition: u32,
    p: Placement,
) -> Result<PreparedRegistration, CatalogError> {
    on(c, move |c| c.prepare_registration([id; 16], partition, p)).await
}
async fn commit(c: &Catalog, p: &PreparedRegistration) -> Result<WriteResult, CatalogError> {
    let p = p.clone();
    on(c, move |c| c.commit_registration(&p)).await
}
fn written(result: WriteResult, sequence: u64, status: WriteStatus) {
    assert_eq!(
        result,
        WriteResult {
            sequence,
            status,
            applied_operations: 1
        }
    );
}
async fn leader(nodes: &[NodeHandle], excluded: Option<u64>) -> anyhow::Result<usize> {
    Ok(timeout(Duration::from_secs(10), async {
        loop {
            for (i, n) in nodes.iter().enumerate() {
                if Some(n.id) != excluded
                    && n.raft.metrics().borrow().current_leader == Some(n.id)
                    && matches!(
                        timeout(Duration::from_millis(150), n.raft.ensure_linearizable()).await,
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
async fn start(cluster: &Cluster, dirs: &[tempfile::TempDir]) -> anyhow::Result<Vec<NodeHandle>> {
    let mut nodes = Vec::new();
    for (i, dir) in dirs.iter().enumerate() {
        nodes.push(cluster.start_durable(i as u64 + 1, dir.path()).await?);
    }
    Ok(nodes)
}
async fn bootstrap(nodes: &[NodeHandle]) -> anyhow::Result<usize> {
    nodes[0]
        .raft
        .initialize(
            nodes
                .iter()
                .map(|n| {
                    (
                        n.id,
                        BasicNode {
                            addr: format!("inproc://{}", n.id),
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        )
        .await?;
    let i = leader(nodes, None).await?;
    let response = nodes[i]
        .raft
        .client_write(LogCommand::D1V1(D1Command::Activate {
            shard: 900,
            epoch: 1,
        }))
        .await?;
    assert_eq!(
        response.data.d1.unwrap(),
        Ok(D1Result::Activated {
            shard: 900,
            epoch: 1
        })
    );
    Ok(i)
}
async fn converge(cluster: &Cluster, nodes: &[NodeHandle], current: usize) -> anyhow::Result<()> {
    let index = nodes[current]
        .state_machine
        .clone()
        .applied_state()
        .await?
        .0
        .unwrap()
        .index;
    let expected = nodes[current]
        .state_machine
        .d1_read_snapshot(10, 1024 * 1024)
        .await
        .expect("bounded metadata snapshot")
        .unwrap();
    for n in nodes {
        cluster.wait_for_applied(n, index, 10_000).await?;
        let actual = n
            .state_machine
            .d1_read_snapshot(10, 1024 * 1024)
            .await
            .expect("bounded metadata snapshot")
            .unwrap();
        assert_eq!(
            (actual.shard, actual.epoch, actual.sequence),
            (expected.shard, expected.epoch, expected.sequence)
        );
        assert_eq!(actual.entries, expected.entries);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registration_cas_exact_retry_and_immutable_bindings() -> anyhow::Result<()> {
    timeout(Duration::from_secs(45), async {
        let cluster = Cluster::new().in_group(100);
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let nodes = start(&cluster, &dirs).await?;
        let current = bootstrap(&nodes).await?;
        let c = catalog(&nodes[current]);
        let other = catalog(&nodes[current]);
        let empty = on(&c, Catalog::snapshot).await?;
        assert_eq!(
            (
                empty.sequence(),
                empty.partition_count(),
                empty.shard_count()
            ),
            (0, 0, 0)
        );
        assert_eq!(on(&c, |c| c.lookup(9)).await?, None);
        let a = prepare(&c, 1, 10, placement(10)).await?;
        let b = prepare(&other, 2, 20, placement(20)).await?;
        assert_eq!(a.expected_sequence(), b.expected_sequence());
        assert_ne!(a.request_id(), b.request_id());
        // Both contenders were prepared from the same quorum snapshot. Whichever
        // wins, the other must conflict instead of overwriting its registration.
        let (ar, br) = tokio::join!(commit(&c, &a), commit(&other, &b));
        let (winner, loser, missing_partition, missing_placement) = match (ar, br) {
            (
                Ok(r),
                Err(CatalogError::Storage(StorageNodeError::Conflict {
                    expected: 0,
                    current: 1,
                })),
            ) => {
                written(r, 1, WriteStatus::Committed);
                (a.clone(), b.clone(), 20, placement(20))
            }
            (
                Err(CatalogError::Storage(StorageNodeError::Conflict {
                    expected: 0,
                    current: 1,
                })),
                Ok(r),
            ) => {
                written(r, 1, WriteStatus::Committed);
                (b.clone(), a.clone(), 10, placement(10))
            }
            result => panic!("one CAS must win: {result:?}"),
        };
        let fresh = on(&c, Catalog::snapshot).await?;
        assert_eq!(
            (
                fresh.sequence(),
                fresh.partition_count(),
                fresh.shard_count()
            ),
            (1, 1, 1)
        );
        assert!(fresh.placement(missing_partition).is_none());
        assert_eq!(
            empty.partition_count(),
            0,
            "retained snapshot stays historical"
        );
        let repaired = on(&c, move |c| {
            c.prepare_registration(loser.request_id(), missing_partition, missing_placement)
        })
        .await?;
        written(commit(&c, &repaired).await?, 2, WriteStatus::Committed);
        let third = prepare(&c, 3, 11, placement(10)).await?;
        written(commit(&c, &third).await?, 3, WriteStatus::Committed);
        written(commit(&c, &winner).await?, 1, WriteStatus::Duplicate);
        written(commit(&c, &repaired).await?, 2, WriteStatus::Duplicate);
        // Reusing a committed ID for newly prepared contents is not exact retry.
        let changed = on(&c, move |c| {
            c.prepare_registration(winner.request_id(), 30, placement(30))
        })
        .await?;
        assert!(matches!(
            commit(&c, &changed).await,
            Err(CatalogError::Storage(
                StorageNodeError::IdempotencyConflict(_)
            ))
        ));
        for (partition, p) in [
            (10, placement(20)), // partition cannot move
            (
                12,
                Placement {
                    epoch: 2,
                    ..placement(10)
                },
            ),
            (
                12,
                Placement {
                    replicas: vec![1, 2, 4],
                    ..placement(10)
                },
            ),
            (
                12,
                Placement {
                    group: 999,
                    ..placement(10)
                },
            ),
            (
                12,
                Placement {
                    group: 210,
                    ..placement(30)
                },
            ), // group cannot serve a new shard
        ] {
            assert!(matches!(
                prepare(&c, 4, partition, p).await,
                Err(CatalogError::BindingConflict)
            ));
        }
        assert!(matches!(
            c.move_partition(10, placement(20)),
            Err(CatalogError::Unsupported)
        ));
        let follower = catalog(&nodes[(current + 1) % 3]);
        assert!(matches!(
            on(&follower, |c| c.lookup(10)).await,
            Err(CatalogError::Storage(StorageNodeError::NotLeader { .. }))
        ));
        assert!(matches!(
            commit(&follower, &third).await,
            Err(CatalogError::Storage(StorageNodeError::NotLeader { .. }))
        ));
        on(&c, |c| {
            let view = c.snapshot()?;
            assert_eq!(
                (view.sequence(), view.partition_count(), view.shard_count()),
                (3, 3, 2)
            );
            assert_eq!(view.placement(10), Some(&placement(10)));
            assert_eq!(view.placement(11), Some(&placement(10)));
            assert_eq!(view.placement(20), Some(&placement(20)));
            assert!(view.placement(30).is_none());
            Ok(())
        })
        .await?;
        converge(&cluster, &nodes, current).await?;
        for n in &nodes {
            cluster.shutdown(n).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn metadata_failover_minority_snapshot_purge_and_durable_retry() -> anyhow::Result<()> {
    timeout(Duration::from_secs(90), async {
        let cluster = Cluster::new().in_group(100);
        let dirs = (0..4)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let mut nodes = start(&cluster, &dirs[..3]).await?;
        let current = bootstrap(&nodes).await?;
        let c = catalog(&nodes[current]);
        let retained = prepare(&c, 10, 1, placement(10)).await?;
        commit(&c, &retained).await?; // discard reply, model an unknown outcome
        converge(&cluster, &nodes, current).await?;
        let old_id = nodes[current].id;
        cluster.shutdown(&nodes[current]).await;
        drop(c);
        drop(nodes.remove(current));
        let current = leader(&nodes, None).await?;
        let c = catalog(&nodes[current]);
        written(commit(&c, &retained).await?, 1, WriteStatus::Duplicate);
        drop(c);
        nodes.push(
            cluster
                .start_durable(old_id, dirs[old_id as usize - 1].path())
                .await?,
        );
        converge(&cluster, &nodes, current).await?;
        let isolated = leader(&nodes, None).await?;
        let isolated_id = nodes[isolated].id;
        let c = catalog(&nodes[isolated]);
        let pending = prepare(&c, 11, 2, placement(20)).await?;
        let historical = on(&c, Catalog::snapshot).await?;
        for n in &nodes {
            if n.id != isolated_id {
                cluster.policy.deny(isolated_id, n.id).await;
                cluster.policy.deny(n.id, isolated_id).await;
            }
        }
        let result = commit(&c, &pending).await;
        assert!(
            matches!(
                result,
                Err(CatalogError::Storage(
                    StorageNodeError::Timeout { .. }
                        | StorageNodeError::NotLeader { .. }
                        | StorageNodeError::Unavailable(_)
                ))
            ),
            "minority write: {result:?}"
        );
        for partition in [1, 999] {
            let result = on(&c, move |c| c.lookup(partition)).await;
            assert!(
                matches!(
                    result,
                    Err(CatalogError::Storage(
                        StorageNodeError::Timeout { .. }
                            | StorageNodeError::NotLeader { .. }
                            | StorageNodeError::Unavailable(_)
                    ))
                ),
                "minority lookup: {result:?}"
            );
        }
        assert!(prepare(&c, 12, 3, placement(30)).await.is_err());
        assert_eq!(historical.placement(1), Some(&placement(10)));
        for n in &nodes {
            assert_eq!(n.state_machine.d1_metadata().await, Some((900, 1, 1)));
        }
        drop(c);
        let current = leader(&nodes, Some(isolated_id)).await?;
        let c = catalog(&nodes[current]);
        written(commit(&c, &pending).await?, 2, WriteStatus::Committed);
        cluster.policy.clear().await;
        converge(&cluster, &nodes, current).await?;
        written(commit(&c, &retained).await?, 1, WriteStatus::Duplicate);
        written(commit(&c, &pending).await?, 2, WriteStatus::Duplicate);
        drop(c);
        // Compact every voter's history so restart cannot depend on an old log
        // retaining the original registration or its idempotency record.
        converge(&cluster, &nodes, current).await?;
        for n in &nodes {
            let applied = n
                .state_machine
                .clone()
                .applied_state()
                .await?
                .0
                .unwrap()
                .index;
            n.raft.trigger().snapshot().await?;
            let snapshot_index = timeout(Duration::from_secs(10), async {
                loop {
                    if let Some(log) = n.raft.metrics().borrow().snapshot {
                        if log.index >= applied {
                            break log.index;
                        }
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await?;
            n.raft.trigger().purge_log(snapshot_index).await?;
            timeout(Duration::from_secs(10), async {
                loop {
                    if n.raft
                        .metrics()
                        .borrow()
                        .purged
                        .is_some_and(|log| log.index >= snapshot_index)
                    {
                        break;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await?;
            assert!(n
                .state_machine
                .clone()
                .get_current_snapshot()
                .await?
                .is_some());
        }
        // A new non-voting learner has no original logs. It must install the
        // compacted catalog snapshot; this does not change configured voters.
        let source_snapshot = nodes[current]
            .state_machine
            .clone()
            .get_current_snapshot()
            .await?
            .expect("leader snapshot was built before purge");
        let snapshot_index = source_snapshot.meta.last_log_id.unwrap().index;
        let learner = cluster.start_durable(4, dirs[3].path()).await?;
        assert_eq!(learner.state_machine.d1_metadata().await, None);
        assert!(learner
            .state_machine
            .clone()
            .get_current_snapshot()
            .await?
            .is_none());
        timeout(
            Duration::from_secs(10),
            nodes[current].raft.add_learner(
                4,
                BasicNode {
                    addr: "inproc://4".into(),
                },
                true,
            ),
        )
        .await??;
        cluster
            .wait_for_applied(&learner, snapshot_index, 10_000)
            .await?;
        let installed = learner
            .state_machine
            .clone()
            .get_current_snapshot()
            .await?
            .expect("empty learner must install a snapshot after log purge");
        assert!(installed.meta.last_log_id.unwrap().index >= snapshot_index);
        assert_eq!(
            installed.snapshot.get_ref(),
            source_snapshot.snapshot.get_ref(),
            "snapshot transfer must preserve catalog bytes and D1 dedup records"
        );
        assert_eq!(learner.state_machine.d1_metadata().await, Some((900, 1, 2)));
        // Re-encode the learner's *decoded state*, rather than merely checking
        // its stored received buffer. Equality includes every D1 retry record,
        // original request bytes, original condition, and result sequence.
        let rebuilt = learner.state_machine.clone().build_snapshot().await?;
        assert_eq!(
            rebuilt.snapshot.get_ref(),
            source_snapshot.snapshot.get_ref(),
            "installed catalog and dedup state must survive decode/re-encode"
        );
        nodes[current]
            .raft
            .wait(Some(Duration::from_secs(5)))
            .voter_ids([1, 2, 3], "learner installation does not alter voters")
            .await?;
        nodes.push(learner);
        converge(&cluster, &nodes, current).await?;
        for n in &nodes {
            cluster.shutdown(n).await;
        }
        drop(nodes);
        let nodes = start(&cluster, &dirs).await?;
        let current = leader(&nodes, None).await?;
        let c = catalog(&nodes[current]);
        written(commit(&c, &retained).await?, 1, WriteStatus::Duplicate);
        written(commit(&c, &pending).await?, 2, WriteStatus::Duplicate);
        on(&c, |c| {
            let view = c.snapshot()?;
            assert_eq!(
                (view.sequence(), view.partition_count(), view.shard_count()),
                (2, 2, 2)
            );
            assert_eq!(view.placement(1), Some(&placement(10)));
            assert_eq!(view.placement(2), Some(&placement(20)));
            Ok(())
        })
        .await?;
        let third = prepare(&c, 12, 3, placement(30)).await?;
        written(commit(&c, &third).await?, 3, WriteStatus::Committed);
        converge(&cluster, &nodes, current).await?;
        for n in &nodes {
            cluster.shutdown(n).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configuration_runtime_and_deadline_fail_closed() -> anyhow::Result<()> {
    timeout(Duration::from_secs(45), async {
        let cluster = Cluster::new().in_group(100);
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let nodes = start(&cluster, &dirs).await?;
        let current = bootstrap(&nodes).await?;
        let node = &nodes[current];
        // Construction must fail outside the runtime hosting the node.
        std::thread::scope(|scope| {
            scope
                .spawn(|| {
                    assert!(matches!(
                        Catalog::new(node, metadata(), Duration::from_secs(1)),
                        Err(CatalogError::Storage(StorageNodeError::InvalidArgument(_)))
                    ));
                })
                .join()
                .unwrap();
        });
        assert!(matches!(
            Catalog::new(node, metadata(), Duration::ZERO),
            Err(CatalogError::Storage(StorageNodeError::InvalidArgument(_)))
        ));
        for p in [
            Placement {
                group: 101,
                ..metadata()
            },
            Placement {
                replicas: vec![4, 5, 6],
                ..metadata()
            },
        ] {
            assert!(matches!(
                Catalog::new(node, p, Duration::from_secs(1)),
                Err(CatalogError::MetadataMismatch)
            ));
        }
        for replicas in [vec![], vec![2, 1], vec![1, 1], (1..=32).collect()] {
            assert!(matches!(
                Catalog::new(
                    node,
                    Placement {
                        replicas,
                        ..metadata()
                    },
                    Duration::from_secs(1)
                ),
                Err(CatalogError::InvalidArgument(_))
            ));
        }
        let c = catalog(node);
        assert!(matches!(
            c.snapshot(),
            Err(CatalogError::Storage(StorageNodeError::Unsupported(_)))
        ));
        assert!(matches!(
            c.prepare_registration([1; 16], 1, placement(10)),
            Err(CatalogError::Storage(StorageNodeError::Unsupported(_)))
        ));
        let prepared = prepare(&c, 1, 1, placement(10)).await?;
        assert!(matches!(
            c.commit_registration(&prepared),
            Err(CatalogError::Storage(StorageNodeError::Unsupported(_)))
        ));
        written(commit(&c, &prepared).await?, 1, WriteStatus::Committed);
        for p in [
            Placement {
                epoch: 2,
                ..metadata()
            },
            Placement {
                replicas: vec![1, 2, 3, 4],
                ..metadata()
            },
        ] {
            let bad = Catalog::new(node, p, Duration::from_secs(1))?;
            assert!(matches!(
                on(&bad, Catalog::snapshot).await,
                Err(CatalogError::MetadataMismatch)
            ));
            assert!(matches!(
                commit(&bad, &prepared).await,
                Err(CatalogError::MetadataMismatch)
            ));
        }
        let wrong_shard = Catalog::new(
            node,
            Placement {
                shard: 901,
                ..metadata()
            },
            Duration::from_secs(1),
        )?;
        assert!(on(&wrong_shard, Catalog::snapshot).await.is_err());
        for p in [
            Placement {
                shard: 900,
                ..placement(10)
            },
            Placement {
                group: 100,
                ..placement(10)
            },
            Placement {
                replicas: vec![2, 1],
                ..placement(10)
            },
        ] {
            assert!(matches!(
                prepare(&c, 2, 2, p).await,
                Err(CatalogError::InvalidArgument(_))
            ));
        }
        let tiny = Catalog::new(node, metadata(), Duration::from_nanos(1))?;
        // With an operation deadline shorter than dispatch, the call must still
        // finish within this generous bound. A race with a completed quorum is
        // permitted, but a successful read must contain the committed binding.
        let result = timeout(Duration::from_secs(2), on(&tiny, |c| c.lookup(1))).await?;
        match result {
            Ok(p) => assert_eq!(p, Some(placement(10))),
            Err(CatalogError::Storage(StorageNodeError::Timeout { request_id: None })) => {}
            other => panic!("unexpected deadline result: {other:?}"),
        }
        converge(&cluster, &nodes, current).await?;
        for n in &nodes {
            cluster.shutdown(n).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adversarial_bootstrap_identity_cas_and_epoch_fencing() -> anyhow::Result<()> {
    timeout(Duration::from_secs(45), async {
        let cluster = Cluster::new().in_group(100);
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let nodes = start(&cluster, &dirs).await?;
        let current = bootstrap(&nodes).await?;
        let c = catalog(&nodes[current]);
        let different = Catalog::new(
            &nodes[current],
            Placement {
                replicas: vec![1, 2, 3, 4],
                ..metadata()
            },
            Duration::from_secs(1),
        )?;
        let a = prepare(&c, 71, 1, placement(10)).await?;
        let b = prepare(&different, 72, 2, placement(20)).await?;
        commit(&c, &a).await?;
        assert!(matches!(
            commit(&different, &b).await,
            Err(CatalogError::Storage(StorageNodeError::Conflict { .. }))
        ));
        assert!(matches!(
            on(&different, Catalog::snapshot).await,
            Err(CatalogError::MetadataMismatch)
        ));
        let next = prepare(&c, 73, 3, placement(30)).await?;
        let response = nodes[current]
            .raft
            .client_write(LogCommand::D1V1(D1Command::SetEpoch {
                shard: 900,
                expected_epoch: 1,
                new_epoch: 2,
            }))
            .await?;
        assert_eq!(
            response.data.d1.expect("D1 epoch reply"),
            Ok(D1Result::Epoch { epoch: 2 })
        );
        for p in [&a, &next] {
            assert!(matches!(
                commit(&c, p).await,
                Err(CatalogError::Storage(StorageNodeError::Fenced { .. }))
            ));
        }
        assert!(matches!(
            on(&c, Catalog::snapshot).await,
            Err(CatalogError::MetadataMismatch)
        ));
        for n in &nodes {
            cluster.shutdown(n).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adversarial_deleted_catalog_never_resets_to_empty() -> anyhow::Result<()> {
    timeout(Duration::from_secs(45), async {
        let cluster = Cluster::new().in_group(100);
        let dirs = (0..3)
            .map(|_| tempfile::tempdir())
            .collect::<Result<Vec<_>, _>>()?;
        let nodes = start(&cluster, &dirs).await?;
        let current = bootstrap(&nodes).await?;
        let c = catalog(&nodes[current]);
        let a = prepare(&c, 81, 1, placement(10)).await?;
        commit(&c, &a).await?;
        let response = nodes[current]
            .raft
            .client_write(LogCommand::D1V1(D1Command::Write(
                d2_raft::D1WriteRequest {
                    shard: 900,
                    epoch: 1,
                    request_id: [82; 16],
                    expected_sequence: Some(1),
                    operations: vec![d2_raft::WriteOp::Delete {
                        key: b"catalog/v1/state".to_vec(),
                    }],
                },
            )))
            .await?;
        assert_eq!(
            response.data.d1.expect("D1 deletion reply"),
            Ok(D1Result::Write {
                sequence: 2,
                status: d2_raft::D1WriteStatus::Committed,
                applied_operations: 1,
            })
        );
        assert!(matches!(
            on(&c, Catalog::snapshot).await,
            Err(CatalogError::Corrupt(_))
        ));
        assert!(prepare(&c, 83, 2, placement(20)).await.is_err());
        // Exact retry succeeds as a duplicate but must not reconstruct deleted bytes.
        assert_eq!(commit(&c, &a).await?.status, WriteStatus::Duplicate);
        assert!(matches!(
            on(&c, Catalog::snapshot).await,
            Err(CatalogError::Corrupt(_))
        ));
        for n in &nodes {
            cluster.shutdown(n).await;
        }
        Ok::<_, anyhow::Error>(())
    })
    .await?
}
