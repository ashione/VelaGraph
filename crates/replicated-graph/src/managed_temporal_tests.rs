use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporal_managed_partial_apply_abort_retry_and_local_head_mapping() {
    use velagraph::temporal_reference::Interval;
    use velagraph::temporal_store::TemporalMutation;
    let change = |id, from, to, label: &str| TemporalMutation::Vertex {
        valid: Interval { from, to },
        vertex: Vertex {
            id,
            labels: vec![label.into()],
            properties: BTreeMap::new(),
        },
    };
    let key = |partition| VertexRef { partition, id: 1 };
    let cluster = Cluster::new();
    let c = start(&cluster, 9).await;
    let a = start(&cluster, 7).await;
    let b = start(&cluster, 8).await;
    let graph = self::graph(&c, &a, &b);
    // Existing graph/v2 records share the same state machine, not temporal IDs.
    vertex(&graph, 7, 1).await;
    let initial = snapshot(&graph).await;
    let large = |id| TemporalMutation::Vertex {
        valid: Interval { from: 0, to: None },
        vertex: Vertex {
            id,
            labels: vec!["large".into()],
            properties: BTreeMap::from([(
                "payload".into(),
                velagraph::Value::Text("x".repeat(9 * 1024 * 1024)),
            )]),
        },
    };
    assert!(matches!(
        initial
            .prepare_temporal_vertices(&BTreeMap::from([(7, vec![large(9)]), (8, vec![large(9)])])),
        Err(Error::Capacity)
    ));
    let base = initial
        .prepare_temporal_vertices(&BTreeMap::from([
            (7, vec![change(1, 0, None, "old")]),
            (8, vec![change(1, 0, None, "old")]),
        ]))
        .unwrap();
    graph.execute(&base).await.unwrap();
    let old = snapshot(&graph).await;
    let update = old
        .prepare_temporal_vertices(&BTreeMap::from([
            (7, vec![change(1, 10, Some(20), "corrected")]),
            (8, vec![change(1, 10, Some(20), "corrected")]),
        ]))
        .unwrap();
    let tx = graph.decide(&update, false).await.unwrap();
    let ordinal = tx.ordinal;
    assert_eq!(tx.decision, Some(Decision::Commit));
    versioned_reads::advance(&c, &a, authority(), 1, target(7), ordinal)
        .await
        .unwrap();
    let partial = snapshot(&graph).await;
    assert_eq!(partial.version().unwrap(), old.version().unwrap());
    for shard in [7, 8] {
        assert_eq!(
            partial
                .temporal(15)
                .vertex(key(shard))
                .unwrap()
                .unwrap()
                .labels[0],
            "old"
        );
    }
    assert!(graph
        .snapshot(Some(ordinal), limit(), Duration::from_secs(20))
        .await
        .is_err());
    assert_eq!(
        graph.execute(&update).await.unwrap(),
        Outcome::Committed {
            ordinal,
            visible: true
        }
    );
    assert_eq!(
        graph.execute(&update).await.unwrap(),
        Outcome::Committed {
            ordinal,
            visible: true
        }
    );
    let now = snapshot(&graph).await;
    for shard in [7, 8] {
        assert_eq!(
            now.temporal(15).vertex(key(shard)).unwrap().unwrap().labels[0],
            "corrected"
        );
        for t in [9, 20] {
            assert_eq!(
                now.temporal(t).vertex(key(shard)).unwrap().unwrap().labels[0],
                "old"
            );
        }
        assert_eq!(
            old.temporal(15).vertex(key(shard)).unwrap().unwrap().labels[0],
            "old"
        );
        assert_eq!(now.temporal(15).local_version(shard).unwrap(), 2);
    }
    assert!(now.vertex(key(7)).unwrap().is_some());
    assert!(now.vertex(key(8)).unwrap().is_none());
    let aborted = now
        .prepare_temporal_vertices(&BTreeMap::from([
            (7, vec![change(1, 0, None, "never")]),
            (8, vec![change(1, 0, None, "never")]),
        ]))
        .unwrap();
    assert!(matches!(
        graph.abort(&aborted).await.unwrap(),
        Outcome::Aborted { visible: true, .. }
    ));
    let after_abort = snapshot(&graph).await;
    assert_eq!(after_abort.temporal(15).local_version(7).unwrap(), 2);
    let one = after_abort
        .prepare_temporal_vertices(&BTreeMap::from([(7, vec![change(1, 30, None, "future")])]))
        .unwrap();
    let (result, ()) = tokio::join!(graph.execute(&one), async {
        for _ in 0..4 {
            assert_eq!(
                now.temporal(15).vertex(key(7)).unwrap().unwrap().labels[0],
                "corrected"
            );
            assert_eq!(
                now.temporal(15).vertex(key(8)).unwrap().unwrap().labels[0],
                "corrected"
            );
            tokio::task::yield_now().await;
        }
    });
    result.unwrap();
    let latest = snapshot(&graph).await;
    assert_eq!(latest.temporal(30).local_version(7).unwrap(), 3);
    assert_eq!(latest.temporal(30).local_version(8).unwrap(), 2);
    assert_eq!(
        latest.temporal(30).vertex(key(7)).unwrap().unwrap().labels[0],
        "future"
    );
    assert_eq!(
        latest.temporal(30).vertex(key(8)).unwrap().unwrap().labels[0],
        "old"
    );
    assert!(latest.temporal(30).vertex(key(99)).is_err());
    let short = graph
        .snapshot(None, limit(), Duration::from_secs(1))
        .await
        .unwrap();
    tokio::time::sleep_until(tokio::time::Instant::from_std(
        short.session.expires_at().unwrap(),
    ))
    .await;
    assert!(matches!(
        short.temporal(15).vertex(key(7)),
        Err(Error::Snapshot(cohort_reads::Error::Expired))
    ));
    assert!(matches!(
        short.temporal(15).local_version(7),
        Err(Error::Snapshot(cohort_reads::Error::Expired))
    ));
    for node in [&a, &b, &c] {
        node.raft.shutdown().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn temporal_managed_durable_request_reopen_and_history() {
    use velagraph::temporal_reference::Interval;
    use velagraph::temporal_store::TemporalMutation;
    let change = |label: &str| TemporalMutation::Vertex {
        valid: Interval { from: 0, to: None },
        vertex: Vertex {
            id: 1,
            labels: vec![label.into()],
            properties: BTreeMap::new(),
        },
    };
    let cluster = Cluster::new();
    let dirs = [
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
        tempfile::tempdir().unwrap(),
    ];
    let mut nodes = Vec::new();
    for (shard, dir) in [9, 7, 8].into_iter().zip(&dirs) {
        nodes.push(
            initialize(
                cluster
                    .in_group(shard)
                    .start_durable(1, dir.path())
                    .await
                    .unwrap(),
                shard,
            )
            .await,
        );
    }
    let old_version;
    let durable_id;
    {
        let graph = self::graph(&nodes[0], &nodes[1], &nodes[2]);
        let empty = snapshot(&graph).await;
        let base = empty
            .prepare_temporal_vertices(&BTreeMap::from([
                (7, vec![change("base")]),
                (8, vec![change("base")]),
            ]))
            .unwrap();
        graph.execute(&base).await.unwrap();
        let old = snapshot(&graph).await;
        old_version = old.version().unwrap();
        let stale = old
            .prepare_temporal_vertices(&BTreeMap::from([(7, vec![change("stale")])]))
            .unwrap();
        let correction = old
            .prepare_temporal_vertices(&BTreeMap::from([
                (7, vec![change("corrected")]),
                (8, vec![change("corrected")]),
            ]))
            .unwrap();
        let admitted = graph
            .admit_request([91; 16], b"temporal-correction".to_vec(), &correction)
            .await
            .unwrap();
        durable_id = admitted.id();
        graph.execute(&admitted).await.unwrap();
        assert!(!matches!(
            graph.execute(&stale).await,
            Ok(Outcome::Committed { .. })
        ));
    }
    for (node, shard) in nodes.iter().zip([9, 7, 8]) {
        cluster.in_group(shard).shutdown(node).await;
    }
    nodes.clear();
    for (shard, dir) in [9, 7, 8].into_iter().zip(&dirs) {
        nodes.push(
            cluster
                .in_group(shard)
                .start_durable(1, dir.path())
                .await
                .unwrap(),
        );
    }
    for node in &nodes {
        node.raft
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader == Some(1), "reopened")
            .await
            .unwrap();
    }
    {
        let graph = self::graph(&nodes[0], &nodes[1], &nodes[2]);
        let recovered = graph
            .recover_request([91; 16], b"temporal-correction")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.id(), durable_id);
        assert!(matches!(
            graph.execute(&recovered).await.unwrap(),
            Outcome::Committed { visible: true, .. }
        ));
        let now = snapshot(&graph).await;
        let old = graph
            .snapshot(Some(old_version), limit(), Duration::from_secs(20))
            .await
            .unwrap();
        for partition in [7, 8] {
            let key = VertexRef { partition, id: 1 };
            assert_eq!(now.temporal(0).local_version(partition).unwrap(), 2);
            assert_eq!(
                now.temporal(0).vertex(key).unwrap().unwrap().labels,
                vec!["corrected"]
            );
            assert_eq!(old.temporal(0).local_version(partition).unwrap(), 1);
            assert_eq!(
                old.temporal(0).vertex(key).unwrap().unwrap().labels,
                vec!["base"]
            );
        }
    }
    for (node, shard) in nodes.iter().zip([9, 7, 8]) {
        cluster.in_group(shard).shutdown(node).await;
    }
}
