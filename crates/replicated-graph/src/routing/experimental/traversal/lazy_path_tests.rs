use super::*;
use std::cell::Cell;

struct SyntheticRead {
    checks: Cell<usize>,
    fail_after: usize,
    fanout: usize,
}
impl SyntheticRead {
    fn new(fanout: usize, fail_after: usize) -> Self {
        Self {
            checks: Cell::new(0),
            fail_after,
            fanout,
        }
    }
}
impl TraversalRead for SyntheticRead {
    type Node = crate::remote::query::FrozenNode;
    fn check(&self) -> ExperimentalResult<()> {
        let count = self.checks.get();
        self.checks.set(count + 1);
        if count >= self.fail_after {
            Err(limit("synthetic cancellation"))
        } else {
            Ok(())
        }
    }
    fn partitions(&self) -> Vec<u32> {
        unreachable!()
    }
    fn metadata_bytes(&self) -> usize {
        0
    }
    fn store(&self, partition: u32) -> ExperimentalResult<AppendOnlyStore<Self::Node>> {
        let shard = u64::from(partition);
        Ok(AppendOnlyStore::new(
            crate::remote::query::FrozenNode {
                view: velagraph::ReadView::from_entries(shard, 1, 0, BTreeMap::new())?,
                end: std::time::Instant::now() + std::time::Duration::from_secs(5),
            },
            shard,
            partition,
        ))
    }
    fn get_vertex(&self, vertex: VertexRef) -> ExperimentalResult<Option<Vertex>> {
        Ok(Some(Vertex {
            id: vertex.id,
            labels: vec!["K".into()],
            properties: BTreeMap::from([("x".into(), Value::Integer(vertex.id as i64))]),
        }))
    }
    fn get_edge(&self, edge: EdgeRef) -> ExperimentalResult<Option<CrossEdge>> {
        Ok(Some(CrossEdge {
            id: edge.id,
            from: VertexRef {
                partition: edge.partition,
                id: 3,
            },
            to: VertexRef {
                partition: edge.partition + 1,
                id: 3,
            },
            label: "K".into(),
            properties: BTreeMap::new(),
        }))
    }
    fn verified_edge_record(&self, _: EdgeRef) -> ExperimentalResult<Option<EdgeRecord>> {
        unreachable!()
    }
    fn observed_adjacency(
        &self,
        vertex: VertexRef,
        _: Direction,
    ) -> ExperimentalResult<Vec<CrossEdge>> {
        Ok((0..self.fanout)
            .map(|index| CrossEdge {
                id: index as u64,
                from: vertex,
                // A cyclic multigraph deliberately keeps bag duplicates alive.
                to: vertex,
                label: "K".into(),
                properties: BTreeMap::new(),
            })
            .collect())
    }
}
fn rows(retain: bool) -> Vec<ElementRow> {
    [7, 8]
        .into_iter()
        .map(|partition| {
            start_row(
                ObservedElementRef::Vertex(VertexRef { partition, id: 3 }),
                retain,
            )
        })
        .collect()
}
fn signature(rows: &[ElementRow]) -> Vec<(ObservedElementRef, usize, u128)> {
    rows.iter()
        .map(|row| (row.element, row.path.len(), row.objects))
        .collect()
}

#[test]
fn path_policy_uses_the_complete_validated_ast() {
    for (text, expected) in [
        ("g.V(1).out().count()", false),
        (
            "g.V().repeat(out()).times(32).emit().id().dedup().is(1)",
            false,
        ),
        ("g.E().bothV().range(0,2).values('x')", false),
        ("g.V().has('x').outE().inV().valueMap('x')", false),
        ("g.V(1).out().path()", true),
        ("g.E(1).bothV().repeat(out()).times(32).path()", true),
        ("g.V(1).simplePath().out().count()", true),
        ("g.V().repeat(out()).times(32).cyclicPath().id()", true),
        ("g.V(999).simplePath().range(0,0).valueMap('x')", true),
    ] {
        let query = velagraph::ObservedGremlinQuery::parse(text).unwrap();
        validate_steps(&query).unwrap();
        assert_eq!(retains_paths(&query), expected, "{text}");
    }
}

#[test]
fn real_executor_keeps_endpoint_and_scalar_projection_composition() {
    let read = SyntheticRead::new(2, usize::MAX);
    let vertex = |partition| ObservedElementRef::Vertex(VertexRef { partition, id: 3 });
    assert_eq!(
        read.execute_traversal(7, "g.E(4).bothV().id().dedup().range(0,1)")
            .unwrap(),
        vec![ObservedRowV3::Value {
            source: vertex(7),
            value: Value::Integer(3)
        }]
    );
    assert_eq!(
        read.execute_traversal(7, "g.E(4).bothV().range(1,2).values('x').is(3)")
            .unwrap(),
        vec![ObservedRowV3::Value {
            source: vertex(8),
            value: Value::Integer(3)
        }]
    );
    assert_eq!(
        read.execute_traversal(7, "g.V(3,3).out().values('x').is(3).dedup().count()")
            .unwrap(),
        vec![ObservedRowV3::Count(1)]
    );
    assert_eq!(
        read.execute_traversal(7, "g.V(3).repeat(out()).times(2).emit().id().is(3).count()")
            .unwrap(),
        vec![ObservedRowV3::Count(6)]
    );
    assert_eq!(
        read.execute_traversal(7, "g.V(3).out().simplePath().count()")
            .unwrap(),
        vec![ObservedRowV3::Count(0)]
    );
    assert_eq!(
        read.execute_traversal(7, "g.V(3).out().cyclicPath().id().dedup()")
            .unwrap(),
        vec![ObservedRowV3::Value {
            source: vertex(7),
            value: Value::Integer(3)
        }]
    );
}

#[test]
fn logical_paths_keep_exact_lengths_masks_work_and_checks_without_payloads() {
    assert!(std::mem::size_of::<ElementRow>() <= 128);
    for (fanout, depth) in [(1, 1), (1, 8), (1, 32), (2, 8)] {
        for emit in [false, true] {
            let mut outputs = Vec::new();
            for retain in [false, true] {
                let read = SyntheticRead::new(fanout, usize::MAX);
                let mut work = Work {
                    visits: 0,
                    bytes: 0,
                };
                let result =
                    repeat_rows(&read, rows(retain), false, None, depth, emit, &mut work).unwrap();
                for row in &result {
                    if retain {
                        assert_eq!(row.path.retained().unwrap().len(), row.path.len());
                    } else {
                        assert!(matches!(row.path, ElementPath::Length(_)));
                        assert!(row.path.retained().is_err());
                    }
                }
                outputs.push((
                    signature(&result),
                    work.visits,
                    work.bytes,
                    read.checks.get(),
                ));
            }
            assert_eq!(
                outputs[0], outputs[1],
                "fanout={fanout} depth={depth} emit={emit}"
            );
        }
    }
    // Explicit edge starts and endpoint transitions use the same representation.
    for retain in [false, true] {
        let mut row = start_row(
            ObservedElementRef::Edge(EdgeRef {
                partition: 7,
                id: 3,
            }),
            retain,
        );
        row.objects = mark_object(row.objects, row.path.len()).unwrap();
        row.path.push(ObservedPathElement::Vertex(VertexRef {
            partition: 8,
            id: 3,
        }));
        assert_eq!((row.path.len(), row.objects), (2, 3));
        // Exactly as scalar conversion does before is/dedup/range retention.
        row.path.clear();
        row.objects = 0;
        row.value = Some(Value::Integer(3));
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        let output = dedup_rows(vec![row], &mut work, || Ok(())).unwrap();
        assert_eq!((output[0].path.len(), work.bytes, work.visits), (0, 256, 1));
    }
}

#[test]
fn logical_paths_preserve_budget_failures_and_cancellation_order() {
    // Every cancellation point in one complete run, plus exact byte/work cuts.
    let read = SyntheticRead::new(1, usize::MAX);
    let mut work = Work {
        visits: 0,
        bytes: 0,
    };
    repeat_rows(&read, rows(true), false, None, 8, true, &mut work).unwrap();
    let total = (work.visits, work.bytes, read.checks.get());
    let mut cases = (0..=total.2).map(|fail| (0, 0, fail)).collect::<Vec<_>>();
    cases.extend([
        (10_000 - total.0, 0, usize::MAX),
        (10_001 - total.0, 0, usize::MAX),
        (0, 64 * 1024 * 1024 - total.1, usize::MAX),
        (0, 64 * 1024 * 1024 - total.1 + 1, usize::MAX),
        (0, 64 * 1024 * 1024 - 257, usize::MAX),
    ]);
    for (visits, bytes, fail_after) in cases {
        let mut outcomes = Vec::new();
        for retain in [false, true] {
            let read = SyntheticRead::new(1, fail_after);
            let mut work = Work { visits, bytes };
            let result = repeat_rows(&read, rows(retain), false, None, 8, true, &mut work);
            let result = result
                .map(|rows| signature(&rows))
                .map_err(|e| format!("{e:?}"));
            outcomes.push((result, work.visits, work.bytes, read.checks.get()));
        }
        assert_eq!(
            outcomes[0], outcomes[1],
            "visits={visits} bytes={bytes} fail_after={fail_after}"
        );
    }
    for retain in [false, true] {
        let read = SyntheticRead::new(2, usize::MAX);
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        assert!(repeat_rows(&read, rows(retain), false, None, 32, true, &mut work).is_err());
        let mut work = Work {
            visits: 0,
            bytes: 0,
        };
        assert!(admit_element(10_000, 66, &mut work).is_err());
        assert_eq!((work.visits, work.bytes), (0, 0));
    }
    let row = start_row(
        ObservedElementRef::Vertex(VertexRef {
            partition: 7,
            id: 3,
        }),
        false,
    );
    assert!(path_is_simple(
        &row,
        &mut Work {
            visits: 0,
            bytes: 0
        },
        &mut || Ok(())
    )
    .is_err());
    assert!(row.path.into_retained().is_err());
    for path in [
        ElementPath::Length(128),
        ElementPath::Retained(vec![
            ObservedPathElement::Vertex(VertexRef {
                partition: 7,
                id: 3
            });
            128
        ]),
    ] {
        assert!(mark_object(0, path.len()).is_err());
    }
}

#[test]
#[ignore = "opt-in release microbenchmark; synthetic provider, not distributed capacity evidence"]
fn lazy_path_repeat_benchmark() {
    use std::hint::black_box;
    use std::time::Instant;
    for (fanout, depth, iterations) in [(1, 1, 2000), (1, 8, 2000), (1, 32, 2000), (2, 8, 100)] {
        for emit in [false, true] {
            // Unmeasured warmup of both representations before paired samples.
            for retain in [true, false] {
                let read = SyntheticRead::new(fanout, usize::MAX);
                for _ in 0..20 {
                    black_box(
                        repeat_rows(
                            &read,
                            rows(retain),
                            false,
                            None,
                            depth,
                            emit,
                            &mut Work {
                                visits: 0,
                                bytes: 0,
                            },
                        )
                        .unwrap(),
                    );
                }
            }
            for sample in 0..10 {
                for retain in if sample % 2 == 0 {
                    [true, false]
                } else {
                    [false, true]
                } {
                    let read = SyntheticRead::new(fanout, usize::MAX);
                    let started = Instant::now();
                    let mut checksum = 0;
                    let mut charged_bytes = 0;
                    let mut path_entries = 0;
                    for _ in 0..iterations {
                        let mut work = Work {
                            visits: 0,
                            bytes: 0,
                        };
                        let result =
                            repeat_rows(&read, rows(retain), false, None, depth, emit, &mut work)
                                .unwrap();
                        checksum += result.iter().map(|r| r.path.len()).sum::<usize>();
                        path_entries += result
                            .iter()
                            .map(|r| match &r.path {
                                ElementPath::Length(_) => 0,
                                ElementPath::Retained(path) => path.len(),
                            })
                            .sum::<usize>();
                        charged_bytes += work.bytes;
                        black_box(result);
                    }
                    println!("lazy_path fanout={fanout} depth={depth} emit={emit} retain={retain} sample={sample} iterations={iterations} elapsed_ns={} checksum={checksum} charged_bytes={charged_bytes} final_path_entries={path_entries}", started.elapsed().as_nanos());
                }
            }
        }
    }
}
