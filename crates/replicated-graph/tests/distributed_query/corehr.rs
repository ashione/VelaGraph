//! Small CI-scale CoreHR regression. The oracle walks the deterministic input
//! relationships only, never database query results or the Gremlin interpreter.
use super::*;

fn part(id: u64) -> u32 {
    1 + (id % 3) as u32
}
fn reference(id: u64) -> VertexRef {
    VertexRef {
        partition: part(id),
        id,
    }
}
fn element(id: u64) -> QualifiedElement {
    vertex(part(id), id)
}
fn active(id: u64) -> bool {
    !id.is_multiple_of(5)
}

fn model() -> Vec<CrossEdge> {
    let mut edges = Vec::new();
    let mut add = |id, from, to, label: &str| {
        edges.push(CrossEdge {
            id,
            from: reference(from),
            to: reference(to),
            label: label.into(),
            properties: BTreeMap::new(),
        })
    };
    for person in 2..=200 {
        // Equal local edge IDs across different source partitions exercise
        // the secondary ordering key on incoming adjacency.
        add(100 + person / 3, person, 1 + (person - 2) / 8, "reports_to");
    }
    for department in 2..=25 {
        add(
            5000 + department,
            200 + department,
            201 + (department - 2) / 5,
            "parent_department",
        );
    }
    for person in 1..=200 {
        let department = 201 + (person * 7) % 25;
        add(10000 + person * 3, person, department, "member_of");
        if person % 5 == 0 {
            add(
                10001 + person * 3,
                person,
                201 + (person * 11) % 25,
                "member_of",
            );
        }
        if person % 20 == 0 {
            add(10002 + person * 3, person, department, "member_of");
        }
    }
    edges
}

fn fixture() -> anyhow::Result<Vec<Export>> {
    let directories = (0..3)
        .map(|_| tempfile::tempdir())
        .collect::<Result<Vec<_>, _>>()?;
    let nodes = directories
        .iter()
        .enumerate()
        .map(|(i, d)| {
            Ok(Shared(Arc::new(LocalStorageNode::open(
                d.path(),
                120 + i as u64 * 10,
            )?)))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let stores = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| AppendOnlyStore::new(n.clone(), 120 + i as u64 * 10, i as u32 + 1))
        .collect::<Vec<_>>();
    for id in 1..=225 {
        let index = part(id) as usize - 1;
        let mut v = Vertex::new(id);
        v.labels
            .push(if id <= 200 { "person" } else { "department" }.into());
        v.properties
            .insert("active".into(), Value::Boolean(active(id)));
        v.properties
            .insert("name".into(), Value::Text(format!("entity-{id}")));
        stores[index].prepare_vertex(&v)?.submit(&nodes[index])?;
    }
    // Extra isolated records exercise multiple scan pages and equal local IDs
    // in different partitions without altering the CoreHR relationship model.
    for id in std::iter::once(1).chain(1000..1400) {
        let mut v = Vertex::new(id);
        v.labels.push("scan_extra".into());
        stores[0].prepare_vertex(&v)?.submit(&nodes[0])?;
    }
    for edge in model() {
        let from = edge.from.partition as usize - 1;
        let to = edge.to.partition as usize - 1;
        stores[from].prepare_source(&edge)?.submit(&nodes[from])?;
        stores[to].prepare_reverse(&edge)?.submit(&nodes[to])?;
    }
    nodes.iter().map(|n| Ok(n.admin().export()?)).collect()
}

type Paths = Vec<Vec<QualifiedElement>>;
fn starts(ids: &[u64]) -> Paths {
    ids.iter().map(|id| vec![element(*id)]).collect()
}
fn end_id(path: &[QualifiedElement]) -> u64 {
    match path.last().unwrap() {
        QualifiedElement::Vertex { id, .. } => *id,
        _ => panic!("vertex endpoint"),
    }
}
fn hop(paths: Paths, label: &str, incoming: bool) -> Paths {
    let edges = model();
    let mut result = Vec::new();
    for path in paths {
        let current = end_id(&path);
        let mut adjacent = edges
            .iter()
            .filter(|e| {
                e.label == label
                    && if incoming {
                        e.to.id == current
                    } else {
                        e.from.id == current
                    }
            })
            .collect::<Vec<_>>();
        adjacent.sort_by_key(|e| (e.id, e.from.partition));
        for edge in adjacent {
            let mut expanded = path.clone();
            expanded.push(QualifiedElement::Edge {
                partition: edge.from.partition,
                id: edge.id,
            });
            expanded.push(element(if incoming { edge.from.id } else { edge.to.id }));
            result.push(expanded);
        }
    }
    result
}
fn vertices(paths: Paths) -> Vec<QueryRow> {
    paths
        .into_iter()
        .map(|p| QueryRow::Element(*p.last().unwrap()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn corehr_incoming_matches_ordered_bag_oracle_across_three_processes() -> anyhow::Result<()> {
    let _serial = PROCESS_TESTS.lock().await;
    timeout(Duration::from_secs(120), async {
        let mut groups = Vec::new();
        for i in 0..3 {
            groups.push(Group::start(i + 1, 120 + i as u64 * 10).await?);
        }
        for (group, export) in groups.iter().zip(run_blocking(fixture).await??) {
            group.seed(export).await?;
        }
        let service = CoordinatorService::start(
            "127.0.0.1:0".parse()?,
            StaticCatalog {
                partitions: groups.iter().map(|g| g.route.clone()).collect(),
            },
            BUDGET,
        )
        .await?;
        let reports = hop(starts(&[1]), "reports_to", true);
        assert_eq!(reports.len(), 8);
        let members = hop(starts(&[201]), "member_of", true);
        let mut cases = vec![
            (
                1,
                "g.V(1).in('reports_to').path()",
                reports.clone().into_iter().map(QueryRow::Path).collect(),
            ),
            (
                1,
                "g.V(1,1).in('reports_to')",
                vertices(hop(starts(&[1, 1]), "reports_to", true)),
            ),
            (
                1,
                "g.V(1).in('reports_to').in('reports_to').count()",
                vec![QueryRow::Count(
                    hop(reports.clone(), "reports_to", true).len() as u64,
                )],
            ),
            (
                201,
                "g.V(201).in('member_of').limit(1000)",
                vertices(members.clone()),
            ),
            (
                201,
                "g.V(201).in('member_of').has('active',true)",
                vertices(
                    members
                        .iter()
                        .filter(|p| active(end_id(p)))
                        .cloned()
                        .collect(),
                ),
            ),
            (
                201,
                "g.V(201).in('parent_department').in('parent_department').path()",
                hop(
                    hop(starts(&[201]), "parent_department", true),
                    "parent_department",
                    true,
                )
                .into_iter()
                .map(QueryRow::Path)
                .collect(),
            ),
            (
                20,
                "g.V(20).out('member_of').in('member_of').count()",
                vec![QueryRow::Count(
                    hop(hop(starts(&[20]), "member_of", false), "member_of", true).len() as u64,
                )],
            ),
            (
                200,
                "g.V(200).in('reports_to').count()",
                vec![QueryRow::Count(0)],
            ),
        ];
        // Independently reduce model paths by qualified endpoint, keeping the
        // first path. Parallel/multiple memberships make bag and set counts differ.
        let unique = |paths: Paths| {
            let mut seen = std::collections::BTreeSet::new();
            paths
                .into_iter()
                .filter(|path| {
                    let QualifiedElement::Vertex { partition, id } = path.last().unwrap() else {
                        panic!()
                    };
                    seen.insert((*partition, *id))
                })
                .collect::<Paths>()
        };
        let colleagues = hop(hop(starts(&[20]), "member_of", false), "member_of", true);
        let distinct = unique(colleagues.clone());
        assert!(distinct.len() < colleagues.len());
        cases.push((
            20,
            "g.V(20).out('member_of').in('member_of').dedup().count()",
            vec![QueryRow::Count(distinct.len() as u64)],
        ));
        cases.push((
            20,
            "g.V(20).out('member_of').in('member_of').dedup().path()",
            distinct.clone().into_iter().map(QueryRow::Path).collect(),
        ));
        cases.push((
            20,
            "g.V(20).out('member_of').in('member_of').dedup().in('reports_to').count()",
            vec![QueryRow::Count(
                hop(distinct.clone(), "reports_to", true).len() as u64,
            )],
        ));
        cases.push((
            201,
            "g.V(201).in('member_of').dedup().has('active',true).count()",
            vec![QueryRow::Count(
                unique(members.clone())
                    .iter()
                    .filter(|p| active(end_id(p)))
                    .count() as u64,
            )],
        ));
        let mut limited = colleagues.clone();
        limited.truncate(4);
        cases.push((
            20,
            "g.V(20).out('member_of').in('member_of').limit(4).dedup()",
            vertices(unique(limited)),
        ));
        let mut limited = distinct;
        limited.truncate(4);
        cases.push((
            20,
            "g.V(20).out('member_of').in('member_of').dedup().limit(4)",
            vertices(limited),
        ));
        cases.push((
            20,
            "g.V(20,20).dedup().dedup().count()",
            vec![QueryRow::Count(1)],
        ));
        cases.push((
            200,
            "g.V(200).in('reports_to').dedup().count()",
            vec![QueryRow::Count(0)],
        ));
        let mut first = reports.clone();
        first.truncate(4);
        let selected = first
            .into_iter()
            .filter(|p| active(end_id(p)))
            .collect::<Paths>();
        cases.push((
            1,
            "g.V(1).in('reports_to').limit(4).has('active',true)",
            vertices(selected),
        ));
        let mut selected = reports
            .into_iter()
            .filter(|p| active(end_id(p)))
            .collect::<Paths>();
        selected.truncate(4);
        cases.push((
            1,
            "g.V(1).in('reports_to').has('active',true).limit(4).values('name')",
            selected
                .iter()
                .map(|p| QueryRow::Value {
                    source: p.last().copied(),
                    value: Value::Text(format!("entity-{}", end_id(p))),
                })
                .collect(),
        ));
        let mut all = (1..=225)
            .map(|id| (part(id), id))
            .chain(std::iter::once((1, 1)))
            .chain((1000..1400).map(|id| (1, id)))
            .collect::<Vec<_>>();
        all.sort_unstable();
        let elements = |xs: Vec<(u32, u64)>| {
            xs.into_iter()
                .map(|(p, id)| QueryRow::Element(vertex(p, id)))
                .collect::<Vec<_>>()
        };
        let person = |(p, id): &(u32, u64)| *id <= 200 && *p == part(*id);
        cases.push((1, "g.V().count()", vec![QueryRow::Count(all.len() as u64)]));
        cases.push((
            1,
            "g.V()",
            elements(all.iter().copied().take(100).collect()),
        ));
        cases.push((1, "g.V().limit(1000)", elements(all.clone())));
        cases.push((
            1,
            "g.V().hasLabel('person').count()",
            vec![QueryRow::Count(200)],
        ));
        cases.push((
            1,
            "g.V().hasLabel('person').has('active',true).count()",
            vec![QueryRow::Count(160)],
        ));
        cases.push((1, "g.V().has('missing').count()", vec![QueryRow::Count(0)]));
        cases.push((
            1,
            "g.V().limit(80).hasLabel('person').count()",
            vec![QueryRow::Count(
                all.iter().take(80).filter(|v| person(v)).count() as u64,
            )],
        ));
        cases.push((
            1,
            "g.V().hasLabel('person').limit(80).count()",
            vec![QueryRow::Count(80)],
        ));
        cases.push((
            1,
            "g.V().limit(500).hasLabel('person').limit(90)",
            elements(
                all.iter()
                    .copied()
                    .take(500)
                    .filter(person)
                    .take(90)
                    .collect(),
            ),
        ));
        cases.push((
            1,
            "g.V().hasLabel('department').limit(23)",
            elements(
                all.iter()
                    .copied()
                    .filter(|(p, id)| *id > 200 && *id <= 225 && *p == part(*id))
                    .take(23)
                    .collect(),
            ),
        ));
        for (start, text, expected) in cases {
            let output = distributed::query(
                service.local_addr(),
                DistributedRequest {
                    start_partition: part(start),
                    text: text.into(),
                    timeout_millis: 5000,
                },
                BUDGET,
            )
            .await??;
            assert_eq!(output.completeness, QueryCompleteness::Unverified);
            assert_eq!(output.snapshots.len(), 3);
            assert_eq!(output.rows, expected, "{text}");
        }
        service.shutdown().await;
        Ok::<(), anyhow::Error>(())
    })
    .await?
}
