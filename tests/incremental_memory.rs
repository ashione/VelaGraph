use velagraph::{
    Edge, IncrementalGraph, IncrementalLimits, IncrementalMode, IncrementalMutation as M, Value,
    Vertex,
};
fn graph(mode: IncrementalMode) -> IncrementalGraph {
    IncrementalGraph::new("test", mode, IncrementalLimits::default()).unwrap()
}
fn seed(g: &IncrementalGraph) {
    g.apply(
        &g.token().unwrap(),
        &[
            M::PutVertex(Vertex::new(1)),
            M::PutVertex(Vertex::new(2)),
            M::PutVertex(Vertex::new(3)),
            M::PutEdge(Edge::new(10, 1, 2, "x")),
            M::PutEdge(Edge::new(11, 2, 2, "x")),
            M::PutEdge(Edge::new(12, 3, 2, "y")),
        ],
        b"initial",
    )
    .unwrap();
}
#[test]
fn invalid_batch_does_not_change_records_metadata_or_version() {
    for mode in [IncrementalMode::Locked, IncrementalMode::Cow] {
        let g = graph(mode);
        seed(&g);
        let before = g.token().unwrap();
        assert!(g
            .apply(
                &before,
                &[M::DeleteVertex(2), M::PutEdge(Edge::new(20, 1, 99, "x"))],
                b"bad"
            )
            .is_err());
        let v = g.read().unwrap();
        assert_eq!(v.token(), before);
        assert!(v.get_vertex(2).is_some());
        assert_eq!(v.edges().count(), 3);
        assert_eq!(v.metadata(), b"initial");
    }
}
#[test]
fn deletion_cascades_both_directions_and_self_loop() {
    for mode in [IncrementalMode::Locked, IncrementalMode::Cow] {
        let g = graph(mode);
        seed(&g);
        g.apply(&g.token().unwrap(), &[M::DeleteVertex(2)], b"deleted")
            .unwrap();
        let v = g.read().unwrap();
        assert!(v.get_vertex(1).is_some());
        assert!(v.get_vertex(3).is_some());
        assert!(v.get_vertex(2).is_none());
        assert_eq!(v.edges().count(), 0);
        assert_eq!(v.adjacent(1, false).unwrap().count(), 0);
    }
}
#[test]
fn retained_cow_view_and_cross_identity_conflict() {
    let g = graph(IncrementalMode::Cow);
    seed(&g);
    let old = g.read().unwrap();
    let mut v = Vertex::new(1);
    v.properties.insert("value".into(), Value::Integer(2));
    g.apply(&old.token(), &[M::PutVertex(v)], b"new").unwrap();
    assert!(!old.get_vertex(1).unwrap().properties.contains_key("value"));
    assert_eq!(old.metadata(), b"initial");
    assert!(g.apply(&old.token(), &[], b"stale").is_err());
    let other = IncrementalGraph::new(
        "different",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
    )
    .unwrap();
    assert!(g.apply(&other.token().unwrap(), &[], b"foreign").is_err());
}
#[test]
fn checkpoint_restore_keeps_version_metadata_and_query() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("state.bin");
    let g = graph(IncrementalMode::Cow);
    seed(&g);
    g.checkpoint(&p).unwrap();
    let restored = IncrementalGraph::restore(
        &p,
        "test",
        IncrementalMode::Locked,
        IncrementalLimits::default(),
    )
    .unwrap();
    assert_eq!(g.token().unwrap(), restored.token().unwrap());
    assert_eq!(restored.read().unwrap().metadata(), b"initial");
    assert_eq!(
        g.read()
            .unwrap()
            .execute_gremlin("g.V(1).out('x').id()")
            .unwrap()
            .rows,
        restored
            .read()
            .unwrap()
            .execute_gremlin("g.V(1).out('x').id()")
            .unwrap()
            .rows
    );
    assert!(IncrementalGraph::restore(
        &p,
        "foreign",
        IncrementalMode::Cow,
        IncrementalLimits::default()
    )
    .is_err());
    let mut bytes = std::fs::read(&p).unwrap();
    let n = bytes.len();
    bytes[n - 1] ^= 1;
    std::fs::write(&p, bytes).unwrap();
    assert!(IncrementalGraph::restore(
        &p,
        "test",
        IncrementalMode::Cow,
        IncrementalLimits::default()
    )
    .is_err());
}
#[test]
fn pinned_versions_backpressure_recovers_after_release() {
    let l = IncrementalLimits {
        max_pinned_versions: 1,
        ..IncrementalLimits::default()
    };
    let g = IncrementalGraph::new("pin", IncrementalMode::Cow, l).unwrap();
    seed(&g);
    let a = g.read().unwrap();
    g.apply(&a.token(), &[], b"two").unwrap();
    let b = g.read().unwrap();
    assert!(g.apply(&b.token(), &[], b"three").is_err());
    assert_eq!(g.token().unwrap(), b.token());
    drop(a);
    g.apply(&b.token(), &[], b"three").unwrap();
    assert_eq!(b.metadata(), b"two");
}
#[test]
fn poisoned_transaction_and_record_limit_leave_graph_unchanged() {
    let l = IncrementalLimits {
        max_records: 2,
        ..IncrementalLimits::default()
    };
    let g = IncrementalGraph::new("limit", IncrementalMode::Cow, l).unwrap();
    let t = g.token().unwrap();
    assert!(g
        .apply(
            &t,
            &[
                M::PutVertex(Vertex::new(1)),
                M::PutVertex(Vertex::new(2)),
                M::PutVertex(Vertex::new(3))
            ],
            b"bad"
        )
        .is_err());
    assert_eq!(g.token().unwrap(), t);
    let mut tx = g.begin().unwrap();
    tx.put_vertex(Vertex::new(1)).unwrap();
    assert!(tx.put_edge(Edge::new(1, 1, 9, "x")).is_err());
    assert!(tx.commit().is_err());
    assert_eq!(g.read().unwrap().vertices().count(), 0);
}
#[test]
fn high_degree_change_copies_one_chunk_not_entire_adjacency() {
    let g = graph(IncrementalMode::Cow);
    for chunk in (0..3001).collect::<Vec<_>>().chunks(128) {
        let ops: Vec<_> = chunk
            .iter()
            .map(|&id| M::PutVertex(Vertex::new(id)))
            .collect();
        g.apply(&g.token().unwrap(), &ops, &[]).unwrap();
    }
    for chunk in (1..3001).collect::<Vec<_>>().chunks(128) {
        let ops: Vec<_> = chunk
            .iter()
            .map(|&id| M::PutEdge(Edge::new(id, id, 0, "parent")))
            .collect();
        g.apply(&g.token().unwrap(), &ops, &[]).unwrap();
    }
    let old = g.read().unwrap();
    let stats = g
        .apply(
            &old.token(),
            &[
                M::PutVertex(Vertex::new(3001)),
                M::PutEdge(Edge::new(3001, 3001, 0, "parent")),
            ],
            b"next",
        )
        .unwrap();
    assert!(stats.copied_adjacency_ids <= 256);
    assert!(stats.copied_index_entries < 1000);
    assert_eq!(old.adjacent(0, true).unwrap().count(), 3000);
    assert_eq!(g.read().unwrap().adjacent(0, true).unwrap().count(), 3001);
}
#[test]
fn locked_reader_delays_publication_and_cow_reader_does_not() {
    use std::{
        sync::{mpsc, Arc},
        time::Duration,
    };
    for mode in [IncrementalMode::Locked, IncrementalMode::Cow] {
        let g = Arc::new(graph(mode));
        seed(&g);
        let old = g.read().unwrap();
        let token = old.token();
        let gg = g.clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            done_tx
                .send(gg.apply(&token, &[], b"next").is_ok())
                .unwrap()
        });
        started_rx.recv().unwrap();
        if mode == IncrementalMode::Locked {
            assert!(done_rx.recv_timeout(Duration::from_millis(30)).is_err());
            drop(old);
            assert!(done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
        } else {
            assert!(done_rx.recv_timeout(Duration::from_secs(2)).unwrap());
            assert_eq!(old.metadata(), b"initial");
        }
        thread.join().unwrap();
    }
}
#[test]
fn sorted_global_scan_and_edge_replacement_remain_generic() {
    let g = graph(IncrementalMode::Cow);
    seed(&g);
    g.apply(
        &g.token().unwrap(),
        &[M::PutEdge(Edge::new(10, 3, 1, "other"))],
        b"replace",
    )
    .unwrap();
    let v = g.read().unwrap();
    assert_eq!(v.adjacent(1, false).unwrap().count(), 0);
    assert_eq!(v.adjacent(1, true).unwrap().count(), 1);
    assert_eq!(
        v.execute_gremlin("g.V().limit(2).id()").unwrap().rows,
        vec![
            velagraph::GremlinRow::Value(Value::Integer(1)),
            velagraph::GremlinRow::Value(Value::Integer(2))
        ]
    );
}
#[test]
fn fill_delete_reuse_releases_index_capacity_with_old_cow_view() {
    for mode in [IncrementalMode::Locked, IncrementalMode::Cow] {
        let g = graph(mode);
        for chunk in (1..2001).collect::<Vec<_>>().chunks(128) {
            g.apply(
                &g.token().unwrap(),
                &chunk
                    .iter()
                    .map(|&id| M::PutVertex(Vertex::new(id)))
                    .collect::<Vec<_>>(),
                &[],
            )
            .unwrap();
        }
        let old = if mode == IncrementalMode::Cow {
            Some(g.read().unwrap())
        } else {
            None
        };
        for chunk in (1..2001).collect::<Vec<_>>().chunks(128) {
            g.apply(
                &g.token().unwrap(),
                &chunk
                    .iter()
                    .map(|&id| M::DeleteVertex(id))
                    .collect::<Vec<_>>(),
                &[],
            )
            .unwrap();
        }
        assert_eq!(g.read().unwrap().index_reserved_bytes(), 0);
        if let Some(old) = old {
            assert_eq!(old.vertices().count(), 2000);
        }
        g.apply(&g.token().unwrap(), &[M::PutVertex(Vertex::new(99))], &[])
            .unwrap();
        assert_eq!(g.read().unwrap().vertices().count(), 1);
    }
}
#[test]
fn valid_checkpoint_restores_under_small_transaction_limits() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("small.bin");
    for mode in [IncrementalMode::Locked, IncrementalMode::Cow] {
        let l = IncrementalLimits {
            max_batch_operations: 4,
            ..IncrementalLimits::default()
        };
        let g = IncrementalGraph::new("small", mode, l.clone()).unwrap();
        for id in 1..10 {
            g.apply(&g.token().unwrap(), &[M::PutVertex(Vertex::new(id))], &[])
                .unwrap();
        }
        g.checkpoint(&p).unwrap();
        let r = IncrementalGraph::restore(&p, "small", mode, l).unwrap();
        assert_eq!(r.token().unwrap(), g.token().unwrap());
        assert_eq!(r.read().unwrap().vertices().count(), 9);
    }
}
#[test]
fn checkpoint_accepts_bare_relative_path() {
    let g = graph(IncrementalMode::Cow);
    seed(&g);
    let p = format!("incremental-bare-{}.bin", std::process::id());
    let result = g.checkpoint(&p);
    let _ = std::fs::remove_file(&p);
    assert!(result.is_ok(), "{result:?}");
}
#[test]
fn empty_checkpoint_metadata_cannot_exceed_restore_budget() {
    let g = graph(IncrementalMode::Cow);
    let base = g.read().unwrap().accounted_bytes();
    g.apply(&g.token().unwrap(), &[], b"metadata").unwrap();
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("meta.bin");
    g.checkpoint(&p).unwrap();
    let l = IncrementalLimits {
        max_accounted_bytes: base,
        ..IncrementalLimits::default()
    };
    assert!(IncrementalGraph::restore(&p, "test", IncrementalMode::Cow, l).is_err());
}
#[test]
fn initial_metadata_copy_is_charged_to_batch_budget() {
    let l = IncrementalLimits {
        max_batch_bytes: 1024,
        ..IncrementalLimits::default()
    };
    let g = IncrementalGraph::new("meta", IncrementalMode::Locked, l).unwrap();
    g.apply(&g.token().unwrap(), &[], &vec![1; 900]).unwrap();
    let before = g.token().unwrap();
    let mut tx = g.begin().unwrap();
    assert!(tx.put_vertex(Vertex::new(1)).is_err());
    assert!(tx.commit().is_err());
    assert_eq!(g.token().unwrap(), before);
}
#[test]
fn locked_index_growth_is_charged_before_publication() {
    let g = graph(IncrementalMode::Locked);
    for chunk in (0..3000).collect::<Vec<_>>().chunks(128) {
        g.apply(
            &g.token().unwrap(),
            &chunk
                .iter()
                .map(|&id| M::PutVertex(Vertex::new(id)))
                .collect::<Vec<_>>(),
            &[],
        )
        .unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("index.bin");
    g.checkpoint(&p).unwrap();
    let l = IncrementalLimits {
        max_batch_bytes: 1024,
        ..IncrementalLimits::default()
    };
    let r = IncrementalGraph::restore(&p, "test", IncrementalMode::Locked, l).unwrap();
    let before = r.token().unwrap();
    assert!(r
        .apply(&before, &[M::PutVertex(Vertex::new(3001))], &[])
        .is_err());
    assert_eq!(r.token().unwrap(), before);
    assert!(r.read().unwrap().get_vertex(3001).is_none());
}
#[test]
fn concurrent_readers_never_observe_mixed_record_and_metadata_versions() {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    for mode in [IncrementalMode::Locked, IncrementalMode::Cow] {
        let g = Arc::new(graph(mode));
        g.apply(
            &g.token().unwrap(),
            &[M::PutVertex(Vertex::new(1)), M::PutVertex(Vertex::new(2))],
            &0i64.to_le_bytes(),
        )
        .unwrap();
        let done = Arc::new(AtomicBool::new(false));
        let gg = g.clone();
        let dd = done.clone();
        let writer = std::thread::spawn(move || {
            for i in 1i64..500 {
                let mut a = Vertex::new(1);
                a.properties.insert("generation".into(), Value::Integer(i));
                let mut b = a.clone();
                b.id = 2;
                gg.apply(
                    &gg.token().unwrap(),
                    &[M::PutVertex(a), M::PutVertex(b)],
                    &i.to_le_bytes(),
                )
                .unwrap();
            }
            dd.store(true, Ordering::Release);
        });
        loop {
            let v = g.read().unwrap();
            let generation = i64::from_le_bytes(v.metadata().try_into().unwrap());
            for id in [1, 2] {
                assert_eq!(
                    v.get_vertex(id)
                        .unwrap()
                        .properties
                        .get("generation")
                        .cloned(),
                    if generation == 0 {
                        None
                    } else {
                        Some(Value::Integer(generation))
                    }
                );
            }
            if done.load(Ordering::Acquire) {
                break;
            }
        }
        writer.join().unwrap();
    }
}
#[test]
fn checkpoint_rebuild_fits_exact_retained_budget() {
    let g = IncrementalGraph::from_records(
        "exact",
        IncrementalMode::Cow,
        IncrementalLimits::default(),
        (0..1200).map(Vertex::new).collect(),
        (1..1200)
            .rev()
            .map(|id| Edge::new(id, id, 0, "x"))
            .collect(),
        b"progress".to_vec(),
    )
    .unwrap();
    let bytes = g.read().unwrap().accounted_bytes();
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("exact.bin");
    g.checkpoint(&p).unwrap();
    let l = IncrementalLimits {
        max_accounted_bytes: bytes,
        ..IncrementalLimits::default()
    };
    let restored = IncrementalGraph::restore(&p, "exact", IncrementalMode::Cow, l).unwrap();
    assert!(restored.read().unwrap().accounted_bytes() <= bytes);
    assert_eq!(
        restored.read().unwrap().adjacent(0, true).unwrap().count(),
        1199
    );
}
#[test]
fn labeled_checkpoint_fits_exact_retained_budget() {
    let g = graph(IncrementalMode::Locked);
    let mut v = Vertex::new(1);
    v.labels = vec!["x".into()];
    g.apply(&g.token().unwrap(), &[M::PutVertex(v)], &[])
        .unwrap();
    let bytes = g.read().unwrap().accounted_bytes();
    let d = tempfile::tempdir().unwrap();
    let p = d.path().join("label.bin");
    g.checkpoint(&p).unwrap();
    let l = IncrementalLimits {
        max_accounted_bytes: bytes,
        ..Default::default()
    };
    let r = IncrementalGraph::restore(&p, "test", IncrementalMode::Cow, l).unwrap();
    assert_eq!(r.read().unwrap().get_vertex(1).unwrap().labels, vec!["x"]);
}
#[test]
fn cached_record_fee_changes_with_record_but_old_view_keeps_old_fee() {
    let g = graph(IncrementalMode::Cow);
    let mut v = Vertex::new(1);
    v.properties
        .insert("p".into(), Value::Text("x".repeat(100)));
    g.apply(&g.token().unwrap(), &[M::PutVertex(v)], &[])
        .unwrap();
    let old = g.read().unwrap();
    let q = velagraph::GremlinQuery::parse("g.V(1).id()").unwrap();
    let exact = velagraph::IncrementalQueryLimits {
        max_operations: 1,
        max_work_bytes: 128 + 128 + 1 + 100 + 8,
    };
    assert!(old.execute_query_with_limits(&q, exact).is_ok());
    assert!(old
        .execute_query_with_limits(
            &q,
            velagraph::IncrementalQueryLimits {
                max_work_bytes: exact.max_work_bytes - 1,
                ..exact
            }
        )
        .is_err());
    let mut v = Vertex::new(1);
    v.properties
        .insert("p".into(), Value::Text("y".repeat(1000)));
    g.apply(&old.token(), &[M::PutVertex(v)], &[]).unwrap();
    assert!(g
        .read()
        .unwrap()
        .execute_query_with_limits(&q, exact)
        .is_err());
    assert!(old.execute_query_with_limits(&q, exact).is_ok());
}

#[cfg(feature = "query-profiling")]
#[test]
fn incremental_profile_counts_queries_points_and_candidates() {
    let g = graph(IncrementalMode::Cow);
    seed(&g);
    velagraph::query_profile::begin();
    g.read()
        .unwrap()
        .execute_gremlin("g.V(1).out('x').id()")
        .unwrap();
    let totals = velagraph::query_profile::finish();
    assert_eq!(totals.incremental_queries, 1);
    assert_eq!(totals.incremental_vertex_reads, 2);
    assert_eq!(totals.incremental_candidates, 1);
}
