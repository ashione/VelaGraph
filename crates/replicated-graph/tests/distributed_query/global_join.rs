//! Independent ordered-vector oracle. Expected answers use only fixture input,
//! never database results, production plans, or fragment implementations.
use super::*;
use std::collections::BTreeSet;

type Id = (u32, u64);
const PARTS: [u32; 4] = [2, 7, 11, 23];
fn owner(p: u32) -> usize {
    match p {
        2 | 11 => 0,
        7 => 1,
        23 => 2,
        _ => unreachable!(),
    }
}
#[derive(Clone)]
struct Model {
    vertices: BTreeMap<Id, Vertex>,
    edges: Vec<CrossEdge>,
}
#[derive(Clone)]
enum Op {
    Label(&'static str),
    Has(&'static str, Option<Value>),
    Limit(usize),
    Hop(bool, Option<&'static str>),
    Both(Option<&'static str>),
    Repeat(bool, Option<&'static str>, usize, bool),
    Dedup,
    Count,
    Map,
    Values(&'static str),
}
fn text(ops: &[Op]) -> String {
    let mut text = String::from("g.V()");
    for op in ops {
        text.push_str(&match op {
            Op::Label(label) => format!(".hasLabel('{label}')"),
            Op::Has(key, None) => format!(".has('{key}')"),
            Op::Has(key, Some(Value::Boolean(value))) => format!(".has('{key}',{value})"),
            Op::Has(key, Some(Value::Integer(value))) => format!(".has('{key}',{value})"),
            Op::Has(key, Some(Value::Text(value))) => format!(".has('{key}','{value}')"),
            Op::Has(_, Some(_)) => unreachable!("test generator uses supported literals"),
            Op::Limit(n) => format!(".limit({n})"),
            Op::Hop(incoming, label) => format!(
                ".{}({})",
                if *incoming { "in" } else { "out" },
                label.map(|s| format!("'{s}'")).unwrap_or_default()
            ),
            Op::Both(label) => format!(
                ".both({})",
                label.map(|s| format!("'{s}'")).unwrap_or_default()
            ),
            Op::Repeat(incoming, label, times, emit) => format!(
                ".repeat(__.{}({})).times({times}){}",
                if *incoming { "in" } else { "out" },
                label.map(|s| format!("'{s}'")).unwrap_or_default(),
                if *emit { ".emit()" } else { "" }
            ),
            Op::Dedup => ".dedup()".into(),
            Op::Count => ".count()".into(),
            Op::Values(key) => format!(".values('{key}')"),
            Op::Map => ".valueMap('name','nullable','absent')".into(),
        });
    }
    text
}
fn oracle(model: &Model, ops: &[Op]) -> Vec<QueryRow> {
    let ids: Vec<Id> = model.vertices.keys().copied().collect();
    let mut result = evaluate(model, ids, ops);
    if !ops.iter().any(|op| {
        matches!(
            op,
            Op::Hop(..) | Op::Both(..) | Op::Repeat(..) | Op::Limit(_) | Op::Count
        )
    }) {
        result.truncate(100);
    }
    result
}
fn evaluate(model: &Model, mut ids: Vec<Id>, ops: &[Op]) -> Vec<QueryRow> {
    for op in ops {
        match op {
            Op::Label(label) => {
                ids.retain(|id| model.vertices[id].labels.iter().any(|v| v == label))
            }
            Op::Has(key, expected) => ids.retain(|id| {
                model.vertices[id]
                    .properties
                    .get(*key)
                    .is_some_and(|actual| expected.as_ref().is_none_or(|v| v == actual))
            }),
            Op::Limit(n) => ids.truncate(*n),
            Op::Hop(incoming, label) => {
                let mut expanded = Vec::new();
                for current in ids {
                    let mut edges: Vec<_> = model
                        .edges
                        .iter()
                        .filter(|e| {
                            let endpoint = if *incoming { e.to } else { e.from };
                            (endpoint.partition, endpoint.id) == current
                                && label.is_none_or(|l| e.label == l)
                        })
                        .collect();
                    edges.sort_by_key(|e| (e.id, e.from.partition));
                    for edge in edges {
                        let endpoint = if *incoming { edge.from } else { edge.to };
                        expanded.push((endpoint.partition, endpoint.id));
                    }
                }
                ids = expanded;
            }
            Op::Both(label) => {
                let mut expanded = Vec::new();
                for current in ids {
                    // Iterate fixture edges once: an exact self-loop has one
                    // identity, whereas parallel edges retain separate rows.
                    let mut edges: Vec<_> = model
                        .edges
                        .iter()
                        .filter(|e| {
                            ((e.from.partition, e.from.id) == current
                                || (e.to.partition, e.to.id) == current)
                                && label.is_none_or(|l| e.label == l)
                        })
                        .collect();
                    edges.sort_by_key(|e| (e.id, e.from.partition));
                    expanded.extend(edges.into_iter().map(|e| {
                        if (e.from.partition, e.from.id) == current {
                            (e.to.partition, e.to.id)
                        } else {
                            (e.from.partition, e.from.id)
                        }
                    }));
                }
                ids = expanded;
            }
            Op::Repeat(incoming, label, times, emit) => {
                let mut emitted = Vec::new();
                for _ in 0..*times {
                    ids = evaluate(model, ids, &[Op::Hop(*incoming, *label)])
                        .into_iter()
                        .map(|row| match row {
                            QueryRow::Element(QualifiedElement::Vertex { partition, id }) => {
                                (partition, id)
                            }
                            _ => unreachable!(),
                        })
                        .collect();
                    if *emit {
                        emitted.extend(ids.iter().copied());
                    }
                }
                if *emit {
                    ids = emitted;
                }
            }
            Op::Dedup => {
                let mut seen = BTreeSet::new();
                ids.retain(|id| seen.insert(*id));
            }
            Op::Count => return vec![QueryRow::Count(ids.len() as u64)],
            Op::Map | Op::Values(_) => {}
        }
    }
    ids.into_iter()
        .filter(|id| match ops.last() {
            Some(Op::Values(key)) => model.vertices[id].properties.contains_key(*key),
            _ => true,
        })
        .map(|id| {
            let source = vertex(id.0, id.1);
            if matches!(ops.last(), Some(Op::Map)) {
                QueryRow::PropertyMap {
                    source,
                    properties: ["name", "nullable", "absent"]
                        .into_iter()
                        .filter_map(|k| {
                            model.vertices[&id]
                                .properties
                                .get(k)
                                .map(|v| (k.to_string(), vec![v.clone()]))
                        })
                        .collect(),
                }
            } else if let Some(Op::Values(key)) = ops.last() {
                QueryRow::Value {
                    source: Some(source),
                    value: model.vertices[&id].properties[*key].clone(),
                }
            } else {
                QueryRow::Element(source)
            }
        })
        .collect()
}
fn model(seed: u64) -> Model {
    let mut vertices = BTreeMap::new();
    for p in PARTS {
        for id in (1..=8).chain(std::iter::once(u64::MAX)) {
            let mut v = Vertex::new(id);
            v.labels.push("node".into());
            v.properties
                .insert("active".into(), Value::Boolean(id % 3 != 0));
            v.properties.insert(
                "typed".into(),
                if id.is_multiple_of(2) {
                    Value::Integer(0)
                } else {
                    Value::Text("0".into())
                },
            );
            if id.is_multiple_of(2) {
                v.properties
                    .insert("name".into(), Value::Text(format!("{p}:{id}")));
                v.properties.insert("nullable".into(), Value::Null);
            }
            vertices.insert((p, id), v);
        }
    }
    // More than two pages; the only 'late' edge source is beyond initial row 100.
    for id in 1000..1270 {
        let mut v = Vertex::new(id);
        v.labels.push("page".into());
        v.properties
            .insert("active".into(), Value::Boolean(id.is_multiple_of(2)));
        vertices.insert((2, id), v);
    }
    let mut edges = Vec::new();
    let mut next = BTreeMap::<u32, u64>::new();
    let mut add = |from: Id, to: Id, label: &str| {
        let id = next.entry(from.0).or_insert(1);
        edges.push(CrossEdge {
            id: *id,
            from: VertexRef {
                partition: from.0,
                id: from.1,
            },
            to: VertexRef {
                partition: to.0,
                id: to.1,
            },
            label: label.into(),
            properties: BTreeMap::new(),
        });
        *id += 1;
    };
    // Parallel edges, equal local IDs, incoming tie across owners, and a cycle.
    add((2, 1), (7, 1), "a");
    add((2, 1), (7, 1), "a");
    add((11, 1), (7, 1), "a");
    add((7, 1), (11, 1), "a");
    add((11, 1), (2, 1), "a");
    add((23, 1), (23, 1), "a");
    add((2, 1200), (23, u64::MAX), "late");
    add((2, 1000), (7, 1), "page");
    add((2, 1127), (11, 1), "page");
    add((2, 1128), (23, 1), "page");
    for _ in 0..101 {
        add((2, 1), (7, 2), "many");
    }
    let mut state = seed;
    let mut random = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..36 {
        let from = (PARTS[(random() % 4) as usize], 1 + random() % 8);
        let to = (PARTS[(random() % 4) as usize], 1 + random() % 8);
        add(from, to, if random().is_multiple_of(2) { "a" } else { "b" });
    }
    Model { vertices, edges }
}
fn export(model: &Model, missing: Option<bool>) -> anyhow::Result<Vec<Export>> {
    let dirs = (0..3)
        .map(|_| tempfile::tempdir())
        .collect::<Result<Vec<_>, _>>()?;
    let nodes = dirs
        .iter()
        .enumerate()
        .map(|(i, d)| {
            Ok(Shared(Arc::new(LocalStorageNode::open(
                d.path(),
                120 + i as u64 * 10,
            )?)))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    for (&(p, _), v) in &model.vertices {
        let i = owner(p);
        AppendOnlyStore::new(nodes[i].clone(), 120 + i as u64 * 10, p)
            .prepare_vertex(v)?
            .submit(&nodes[i])?;
    }
    for e in &model.edges {
        let i = owner(e.from.partition);
        let j = owner(e.to.partition);
        let corrupt = e.id == u64::MAX && e.from.partition == 23;
        if !(corrupt && missing == Some(true)) {
            AppendOnlyStore::new(nodes[i].clone(), 120 + i as u64 * 10, e.from.partition)
                .prepare_source(e)?
                .submit(&nodes[i])?;
        }
        if !(corrupt && missing == Some(false)) {
            AppendOnlyStore::new(nodes[j].clone(), 120 + j as u64 * 10, e.to.partition)
                .prepare_reverse(e)?
                .submit(&nodes[j])?;
        }
    }
    nodes.iter().map(|n| Ok(n.admin().export()?)).collect()
}
fn cases() -> Vec<Vec<Op>> {
    use Op::*;
    let mut cases = vec![
        vec![Label("node"), Both(Some("a"))],
        vec![Label("node"), Both(None), Map],
        vec![Label("node"), Both(None), Dedup, Map],
        vec![Label("node"), Limit(1), Both(Some("many")), Count],
        vec![Label("node"), Limit(1), Both(Some("many")), Dedup, Count],
        vec![
            Label("node"),
            Both(Some("a")),
            Limit(3),
            Has("active", Some(Value::Boolean(true))),
            Map,
        ],
        vec![
            Label("node"),
            Both(Some("a")),
            Has("active", Some(Value::Boolean(true))),
            Limit(3),
            Map,
        ],
        vec![
            Label("node"),
            Limit(2),
            Both(Some("a")),
            Hop(true, Some("b")),
            Both(Some("a")),
            Count,
        ],
        vec![
            Label("node"),
            Limit(1),
            Both(Some("a")),
            Repeat(false, Some("b"), 2, true),
            Count,
        ],
        vec![
            Label("node"),
            Limit(1),
            Repeat(true, Some("b"), 2, false),
            Both(Some("a")),
            Map,
        ],
        vec![Label("none"), Both(None), Map],
        vec![Label("none"), Both(None), Count],
        vec![Dedup, Hop(false, Some("many")), Count],
        vec![Dedup, Hop(false, Some("many")), Dedup, Count],
        vec![Label("node"), Dedup, Limit(7), Hop(true, Some("a")), Map],
        vec![
            Limit(7),
            Dedup,
            Has("active", Some(Value::Boolean(true))),
            Hop(false, None),
            Count,
        ],
        vec![
            Has("active", Some(Value::Boolean(true))),
            Dedup,
            Limit(7),
            Hop(false, None),
            Count,
        ],
        vec![Dedup, Limit(129), Dedup, Hop(false, None), Count],
        vec![
            Label("node"),
            Dedup,
            Repeat(false, Some("a"), 3, true),
            Count,
        ],
        vec![
            Label("node"),
            Dedup,
            Repeat(false, Some("a"), 3, true),
            Dedup,
            Count,
        ],
        vec![Map],
        vec![Dedup, Map],
        vec![Dedup, Limit(129), Map],
        vec![Dedup, Count],
        vec![Limit(100), Map],
        vec![Label("node"), Dedup, Limit(100), Map],
        vec![
            Limit(7),
            Dedup,
            Has("active", Some(Value::Boolean(true))),
            Map,
        ],
        vec![
            Has("active", Some(Value::Boolean(true))),
            Dedup,
            Limit(7),
            Map,
        ],
        vec![Label("none"), Dedup, Map],
        vec![Label("none"), Dedup, Count],
        vec![Hop(false, Some("late"))],
        vec![Hop(false, Some("many"))],
        vec![Hop(false, Some("many")), Map],
        vec![Hop(false, Some("late")), Count],
        vec![Label("node"), Hop(false, None), Limit(1000)],
        vec![Label("node"), Hop(true, None), Limit(1000)],
        vec![
            Label("node"),
            Hop(false, Some("a")),
            Dedup,
            Hop(true, Some("a")),
            Count,
        ],
        vec![
            Label("node"),
            Hop(false, Some("a")),
            Hop(true, Some("a")),
            Dedup,
            Count,
        ],
        vec![
            Label("node"),
            Hop(false, Some("a")),
            Dedup,
            Hop(true, Some("a")),
            Dedup,
            Limit(1000),
        ],
        vec![
            Label("node"),
            Hop(false, Some("a")),
            Hop(true, Some("b")),
            Hop(false, Some("a")),
            Count,
        ],
        vec![
            Label("node"),
            Hop(false, None),
            Limit(2),
            Has("active", Some(Value::Boolean(true))),
            Count,
        ],
        vec![
            Label("node"),
            Hop(false, None),
            Has("active", Some(Value::Boolean(true))),
            Limit(2),
            Count,
        ],
        vec![
            Label("node"),
            Hop(false, None),
            Has("typed", Some(Value::Integer(0))),
            Count,
        ],
        vec![
            Label("node"),
            Hop(false, None),
            Has("typed", Some(Value::Text("0".into()))),
            Count,
        ],
        vec![
            Label("node"),
            Hop(false, None),
            Has("nullable", None),
            Limit(1000),
            Map,
        ],
        vec![Label("node"), Hop(false, None), Limit(1000), Map],
        vec![Label("none"), Hop(false, None), Count],
    ];
    for n in [1, 127, 128, 129, 255, 256] {
        cases.push(vec![Label("page"), Limit(n), Both(Some("page")), Count]);
        cases.push(vec![
            Label("page"),
            Has("active", Some(Value::Boolean(true))),
            Limit(n),
            Both(Some("page")),
            Count,
        ]);
        cases.push(vec![
            Label("page"),
            Limit(n),
            Has("active", Some(Value::Boolean(true))),
            Hop(false, Some("page")),
            Count,
        ]);
        cases.push(vec![
            Label("page"),
            Has("active", Some(Value::Boolean(true))),
            Limit(n),
            Hop(false, Some("page")),
            Count,
        ]);
    }
    for incoming in [false, true] {
        for emit in [false, true] {
            for suffix in [
                vec![],
                vec![Dedup, Count],
                vec![Has("active", Some(Value::Boolean(true))), Limit(13), Map],
            ] {
                let mut case = vec![
                    Label("node"),
                    Limit(2),
                    Repeat(incoming, Some("a"), 4, emit),
                ];
                case.extend(suffix);
                cases.push(case);
            }
        }
    }
    cases.extend([
        vec![Label("none"), Repeat(false, None, 32, true), Count],
        vec![
            Label("node"),
            Limit(1),
            Repeat(false, Some("many"), 1, true),
        ],
        vec![
            Label("node"),
            Limit(1),
            Repeat(false, Some("a"), 2, true),
            Hop(true, Some("b")),
            Dedup,
            Count,
        ],
        vec![
            Label("node"),
            Limit(2),
            Hop(false, Some("a")),
            Repeat(true, Some("b"), 2, false),
            Count,
        ],
        vec![
            Label("page"),
            Limit(129),
            Repeat(false, Some("page"), 1, true),
            Count,
        ],
    ]);
    // Scalar expectations derive only from input properties and ordered edges.
    // Include >100 equal scalars, missing versus Null, and both limit positions.
    for key in ["name", "nullable", "absent", "typed", "active"] {
        for prefix in [
            vec![Limit(1)],
            vec![Limit(129)],
            vec![Limit(1000)],
            vec![Dedup, Limit(1000), Dedup],
            vec![Label("none"), Limit(1000)],
            vec![Limit(7), Has("active", Some(Value::Boolean(true)))],
            vec![Has("active", Some(Value::Boolean(true))), Limit(7)],
            vec![
                Limit(129),
                Has("active", Some(Value::Boolean(true))),
                Limit(3),
            ],
            vec![Both(Some("many"))],
            vec![Label("node"), Both(None), Dedup],
            vec![Label("none"), Both(None)],
            vec![Hop(false, Some("many"))],
            vec![Label("node"), Hop(false, None)],
            vec![Label("node"), Hop(true, None), Dedup],
            vec![Label("none"), Hop(false, None)],
            vec![
                Limit(7),
                Has("active", Some(Value::Boolean(true))),
                Hop(false, None),
            ],
            vec![
                Has("active", Some(Value::Boolean(true))),
                Limit(7),
                Hop(false, None),
            ],
            vec![Label("node"), Hop(false, None), Limit(2)],
            vec![Label("node"), Limit(2), Repeat(false, Some("a"), 3, true)],
        ] {
            let mut case = prefix;
            case.push(Values(key));
            cases.push(case);
        }
    }
    cases
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_join_matches_independent_seeded_ordered_oracle() -> anyhow::Result<()> {
    let _guard = PROCESS_TESTS.lock().await;
    for seed in [1, 42] {
        let model = model(seed);
        ensure!(oracle(&model, &[Op::Hop(false, Some("many"))]).len() == 101);
        ensure!(
            oracle(
                &model,
                &[Op::Dedup, Op::Hop(false, Some("many")), Op::Count]
            ) == vec![QueryRow::Count(101)]
        );
        ensure!(
            oracle(
                &model,
                &[
                    Op::Dedup,
                    Op::Hop(false, Some("many")),
                    Op::Dedup,
                    Op::Count
                ]
            ) == vec![QueryRow::Count(1)]
        );
        let copy = model.clone();
        let exports = run_blocking(move || export(&copy, None)).await??;
        let mut groups = Vec::new();
        for (i, p) in [2, 7, 23].into_iter().enumerate() {
            groups.push(Group::start(p, 120 + i as u64 * 10).await?);
        }
        for (g, e) in groups.iter().zip(exports) {
            g.seed(e).await?;
        }
        let mut shared = groups[0].route.clone();
        shared.partition = 11;
        let service = CoordinatorService::start(
            "127.0.0.1:0".parse()?,
            StaticCatalog {
                partitions: vec![
                    groups[2].route.clone(),
                    shared,
                    groups[1].route.clone(),
                    groups[0].route.clone(),
                ],
            },
            BUDGET,
        )
        .await?;
        for ops in cases() {
            let text = text(&ops);
            let actual = distributed::query(
                service.local_addr(),
                DistributedRequest {
                    start_partition: 11,
                    text: text.clone(),
                    timeout_millis: 5000,
                },
                BUDGET,
            )
            .await?
            .with_context(|| format!("seed={seed} query={text}"))?;
            ensure!(actual.completeness == QueryCompleteness::Unverified);
            ensure!(
                actual.snapshots.iter().map(|s| s.shard).collect::<Vec<_>>() == [120, 130, 140]
            );
            ensure!(
                actual.rows == oracle(&model, &ops),
                "seed={seed} query={text}: ordered oracle mismatch: {:?}",
                actual.rows
            );
        }
        for suffix in ["limit(1).count()", "dedup().limit(1).count()"] {
            let result = distributed::query(
                service.local_addr(),
                DistributedRequest {
                    start_partition: 11,
                    text: format!("g.V().hasLabel('node').limit(1).both('a').repeat(__.out('a')).times(31).{suffix}"),
                    timeout_millis: 5000,
                },
                BUDGET,
            ).await?;
            ensure!(
                matches!(result, Err(QueryFailure::ResourceExhausted)),
                "both repeat cumulative work: {result:?}"
            );
        }
        // A tiny downstream answer cannot forgive cumulative repeated work.
        for depth in [1, 32] {
            let result = distributed::query(
                service.local_addr(),
                DistributedRequest {
                    start_partition: 11,
                    text: format!("g.V().hasLabel('node').limit(1).repeat(__.out('a')).times({depth}).emit().dedup().limit(1).count()"),
                    timeout_millis: 5000,
                },
                BUDGET,
            ).await?;
            if depth == 1 {
                ensure!(result?.rows == vec![QueryRow::Count(1)]);
            } else {
                ensure!(
                    matches!(result, Err(QueryFailure::ResourceExhausted)),
                    "cumulative repeat work: {result:?}"
                );
            }
        }
        service.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_join_late_corruption_is_not_hidden_by_filter_or_limit() -> anyhow::Result<()> {
    let _guard = PROCESS_TESTS.lock().await;
    for missing_source in [false, true] {
        let mut model = model(7);
        model.edges.push(CrossEdge {
            id: u64::MAX,
            from: VertexRef {
                partition: 23,
                id: u64::MAX,
            },
            to: VertexRef {
                partition: 7,
                id: u64::MAX,
            },
            label: "corrupt".into(),
            properties: BTreeMap::new(),
        });
        // Incoming traversal reaches the corrupt canonical record only at
        // depth two; outgoing reaches the absent reverse via the existing late edge.
        model.edges.push(CrossEdge {
            id: u64::MAX - 1,
            from: VertexRef {
                partition: 7,
                id: u64::MAX,
            },
            to: VertexRef {
                partition: 2,
                id: 1201,
            },
            label: "bridge".into(),
            properties: BTreeMap::new(),
        });
        let copy = model.clone();
        let exports = run_blocking(move || export(&copy, Some(missing_source))).await??;
        let mut groups = Vec::new();
        for (i, p) in [2, 7, 23].into_iter().enumerate() {
            groups.push(Group::start(p, 120 + i as u64 * 10).await?);
        }
        for (g, e) in groups.iter().zip(exports) {
            g.seed(e).await?;
        }
        let mut shared = groups[0].route.clone();
        shared.partition = 11;
        let service = CoordinatorService::start(
            "127.0.0.1:0".parse()?,
            StaticCatalog {
                partitions: vec![
                    groups[2].route.clone(),
                    shared,
                    groups[1].route.clone(),
                    groups[0].route.clone(),
                ],
            },
            BUDGET,
        )
        .await?;
        let direction = if missing_source { "in" } else { "out" };
        for query in [
            "g.V().hasLabel('node').both('absent').dedup().limit(1).count()".into(),
            "g.V().hasLabel('node').both().values('absent')".into(),
            format!("g.V().hasLabel('node').{direction}().count()"),
            format!("g.V().hasLabel('node').{direction}().values('absent')"),
            format!(
                "g.V().hasLabel('page').repeat(__.{direction}()).times(2).emit().limit(1).count()"
            ),
            format!(
                "g.V().hasLabel('node').repeat(__.{direction}('absent')).times(2).limit(1).count()"
            ),
            format!("g.V().hasLabel('node').{direction}('absent').limit(1).count()"),
        ] {
            let result = distributed::query(
                service.local_addr(),
                DistributedRequest {
                    start_partition: 11,
                    text: query.clone(),
                    timeout_millis: 5000,
                },
                BUDGET,
            )
            .await?;
            ensure!(
                matches!(result, Err(QueryFailure::IncompleteOrCorrupt)),
                "late corruption became a partial success or wrong error: {query}: {result:?}"
            );
        }
        // A genuine upstream limit does not traverse the unseen suffix. This
        // distinguishes safe bounded acquisition from hiding acquired corruption.
        let ops = [Op::Limit(1), Op::Hop(false, Some("a")), Op::Count];
        let actual = distributed::query(
            service.local_addr(),
            DistributedRequest {
                start_partition: 11,
                text: text(&ops),
                timeout_millis: 5000,
            },
            BUDGET,
        )
        .await??;
        ensure!(actual.rows == oracle(&model, &ops));
        service.shutdown().await;
    }
    Ok(())
}
