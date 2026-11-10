//! Input-only small-graph oracle. No engine parser, adjacency, snapshot or reducer
//! computes expectations: exhaustive fixture-vector scans deliberately trade
//! efficiency for an implementation independent of the production indexes.
use super::*;

type Key = (u32, u64);
const LABEL: &str = "model";
const PATH_QUERY: &str = "g.V(10).out('model').in('model').path()";
const SCALAR_QUERY: &str = "g.V().hasLabel('model').values('marker').dedup()";

#[derive(Clone, Debug)]
struct ModelEdge {
    id: Key,
    from: Key,
    to: Key,
    weight: Option<Value>,
}
#[derive(Clone, Debug, Default)]
struct Model {
    vertices: Vec<(Key, Option<Value>)>,
    edges: Vec<ModelEdge>,
}
#[derive(Clone, Debug)]
enum Change {
    Vertex(Key, Value),
    Edge(Key, Key, u64),
    VertexProperty(Key, Option<Value>),
    EdgeProperty(Key, Option<Value>),
    DeleteEdge(Key),
}
fn entity(key: Key) -> Entity {
    Entity {
        partition: key.0,
        id: key.1,
    }
}
fn vertex_element(key: Key) -> QualifiedElement {
    element(key.0, key.1)
}
fn edge_element(key: Key) -> QualifiedElement {
    QualifiedElement::Edge {
        partition: key.0,
        id: key.1,
    }
}
fn projected(source: QualifiedElement, value: Value) -> QueryRow {
    QueryRow::Value {
        source: Some(source),
        value,
    }
}
fn patch(key: &str, value: &Option<Value>) -> Patch {
    match value {
        Some(value) => Patch {
            set: BTreeMap::from([(key.into(), value.clone())]),
            remove: BTreeSet::new(),
        },
        None => Patch {
            set: BTreeMap::new(),
            remove: BTreeSet::from([key.into()]),
        },
    }
}
impl Change {
    /// Encode input, independently of the reference state's transition below.
    fn write(&self, index: usize) -> Write {
        let mutation = match self {
            Self::Vertex(key, value) => Mutation::CreateVertex {
                partition: key.0,
                vertex: VertexData {
                    id: key.1,
                    labels: vec![LABEL.into()],
                    properties: BTreeMap::from([("marker".into(), value.clone())]),
                },
            },
            Self::Edge(from, to, id) => Mutation::CreateEdge {
                edge: EdgeData {
                    id: *id,
                    from: entity(*from),
                    to: entity(*to),
                    label: LABEL.into(),
                    properties: BTreeMap::from([("weight".into(), Value::Integer(1))]),
                },
            },
            Self::VertexProperty(key, value) => Mutation::PatchVertex {
                vertex: entity(*key),
                patch: patch("marker", value),
            },
            Self::EdgeProperty(key, value) => Mutation::PatchEdge {
                edge: entity(*key),
                patch: patch("weight", value),
            },
            Self::DeleteEdge(key) => Mutation::DeleteEdge { edge: entity(*key) },
        };
        // Disjoint from the original exact-prefix fixture's external IDs 1..=6.
        Write {
            external_id: [u8::try_from(100 + index).unwrap(); 16],
            mutation,
        }
    }
}
impl Model {
    fn apply(&mut self, change: &Change) {
        match change {
            Change::Vertex(key, value) => {
                assert!(!self.vertices.iter().any(|(existing, _)| existing == key));
                self.vertices.push((*key, Some(value.clone())));
            }
            Change::Edge(from, to, id) => {
                for endpoint in [from, to] {
                    assert!(self.vertices.iter().any(|(key, _)| key == endpoint));
                }
                let id = (from.0, *id);
                assert!(!self.edges.iter().any(|edge| edge.id == id));
                self.edges.push(ModelEdge {
                    id,
                    from: *from,
                    to: *to,
                    weight: Some(Value::Integer(1)),
                });
            }
            Change::VertexProperty(key, value) => {
                self.vertices
                    .iter_mut()
                    .find(|(candidate, _)| candidate == key)
                    .unwrap()
                    .1 = value.clone();
            }
            Change::EdgeProperty(key, value) => {
                self.edges
                    .iter_mut()
                    .find(|edge| edge.id == *key)
                    .unwrap()
                    .weight = value.clone();
            }
            Change::DeleteEdge(key) => {
                let index = self.edges.iter().position(|edge| edge.id == *key).unwrap();
                self.edges.remove(index);
            }
        }
    }
    fn vertices(&self) -> Vec<Key> {
        let mut keys: Vec<_> = self.vertices.iter().map(|(key, _)| *key).collect();
        keys.sort_by_key(|key| (key.1, key.0));
        keys
    }
    fn edges(&self) -> Vec<&ModelEdge> {
        let mut edges: Vec<_> = self.edges.iter().collect();
        edges.sort_by_key(|edge| (edge.id.1, edge.id.0));
        edges
    }
    fn markers(&self, keys: &[Key]) -> Vec<QueryRow> {
        keys.iter()
            .filter_map(|key| {
                self.vertices
                    .iter()
                    .find(|(candidate, _)| candidate == key)
                    .and_then(|(_, value)| value.clone())
                    .map(|value| projected(vertex_element(*key), value))
            })
            .collect()
    }
    fn both(&self, roots: &[Key]) -> Vec<Key> {
        roots
            .iter()
            .flat_map(|root| {
                self.edges().into_iter().filter_map(move |edge| {
                    // One edge visit makes a self-loop appear once, but parallel
                    // edges and independently repeated roots remain distinct.
                    if edge.from == *root {
                        Some(edge.to)
                    } else if edge.to == *root {
                        Some(edge.from)
                    } else {
                        None
                    }
                })
            })
            .collect()
    }
    fn paths(&self) -> Vec<QueryRow> {
        let mut rows = Vec::new();
        for first in self.edges().into_iter().filter(|edge| edge.from == (7, 10)) {
            for second in self.edges().into_iter().filter(|edge| edge.to == first.to) {
                rows.push(QueryRow::Path(vec![
                    vertex_element((7, 10)),
                    edge_element(first.id),
                    vertex_element(first.to),
                    edge_element(second.id),
                    vertex_element(second.from),
                ]));
            }
        }
        rows
    }
    fn cases(&self) -> Vec<(&'static str, Vec<QueryRow>)> {
        let rows = |keys: Vec<Key>| {
            keys.into_iter()
                .map(|key| QueryRow::Element(vertex_element(key)))
                .collect::<Vec<_>>()
        };
        let both = rows(self.both(&[(7, 10), (7, 10), (7, 11)]));
        let first_two = both.iter().take(2).cloned().collect();
        let scalar = self.markers(&self.vertices());
        let endpoints: Vec<_> = [10, 11, 12]
            .into_iter()
            .flat_map(|id| {
                self.edges
                    .iter()
                    .find(|edge| edge.id == (7, id))
                    .map(|edge| vec![edge.from, edge.to])
                    .unwrap_or_default()
            })
            .collect();
        let incoming: Vec<_> = self
            .edges()
            .into_iter()
            .filter(|edge| edge.to == (7, 10))
            .map(|edge| edge.from)
            .collect();
        let filtered: Vec<_> = self
            .vertices
            .iter()
            .filter(|(_, value)| value == &Some(Value::Integer(42)))
            .map(|(key, _)| *key)
            .collect();
        let mut filtered = filtered;
        filtered.sort_by_key(|key| (key.1, key.0));
        vec![
            ("g.V().hasLabel('model')", rows(self.vertices())),
            (
                "g.E().hasLabel('model')",
                self.edges()
                    .into_iter()
                    .map(|edge| QueryRow::Element(edge_element(edge.id)))
                    .collect(),
            ),
            ("g.V().hasLabel('model').values('marker')", scalar.clone()),
            (SCALAR_QUERY, distinct(scalar, true)),
            (
                "g.E().hasLabel('model').values('weight')",
                self.edges()
                    .into_iter()
                    .filter_map(|edge| {
                        edge.weight
                            .clone()
                            .map(|value| projected(edge_element(edge.id), value))
                    })
                    .collect(),
            ),
            ("g.V(10,10,11).both('model')", both.clone()),
            (
                "g.V(10,10,11).both('model').limit(2).dedup()",
                distinct(first_two, false),
            ),
            (
                "g.V(10,10,11).both('model').dedup().limit(2)",
                distinct(both, false).into_iter().take(2).collect(),
            ),
            (PATH_QUERY, self.paths()),
            (
                "g.E(10,11,12).bothV().values('marker').dedup()",
                distinct(self.markers(&endpoints), true),
            ),
            (
                "g.V(10).in('model').values('marker')",
                self.markers(&incoming),
            ),
            ("g.V().hasLabel('model').has('marker',42)", rows(filtered)),
            (
                "g.E().hasLabel('model').count()",
                vec![QueryRow::Count(self.edges.len() as u64)],
            ),
        ]
    }
}
/// Linear equality oracle, not the executor's canonical keys or identity sets.
fn distinct(rows: Vec<QueryRow>, scalar: bool) -> Vec<QueryRow> {
    let mut result = Vec::new();
    for row in rows {
        let duplicate = result.iter().any(|prior| match (scalar, prior, &row) {
            (true, QueryRow::Value { value: left, .. }, QueryRow::Value { value: right, .. }) => {
                left == right
            }
            (false, left, right) => left == right,
            _ => panic!("wrong oracle row kind"),
        });
        if !duplicate {
            result.push(row);
        }
    }
    result
}
fn script() -> Vec<Change> {
    use Change::*;
    vec![
        Vertex((8, 11), Value::Real(1.0)),
        Vertex((7, 10), Value::Integer(1)),
        Vertex((8, 10), Value::Text("1".into())),
        Vertex((7, 11), Value::Null),
        Edge((7, 10), (8, 10), 10),
        Edge((7, 10), (8, 10), 11),
        Edge((8, 10), (7, 10), 10),
        Edge((7, 10), (7, 10), 12),
        Edge((7, 11), (7, 10), 13),
        Edge((8, 11), (7, 10), 11),
        VertexProperty((8, 10), Some(Value::Integer(42))),
        VertexProperty((7, 10), None),
        VertexProperty((7, 10), Some(Value::Null)),
        EdgeProperty((7, 10), Some(Value::Integer(9))),
        EdgeProperty((8, 10), None),
        DeleteEdge((7, 11)),
        Edge((8, 11), (7, 11), 12),
        Vertex((7, 12), Value::Boolean(true)),
        Edge((7, 12), (8, 10), 14),
        DeleteEdge((7, 12)),
        VertexProperty((8, 10), Some(Value::Text("42".into()))),
    ]
}

async fn verify(g: &Gateway, session: &Session, model: &Model) -> anyhow::Result<()> {
    for (query, expected) in model.cases() {
        assert_eq!(
            traverse(g, session, 7, query).await?,
            expected,
            "V{}: {query}",
            session.version
        );
    }
    Ok(())
}
struct Pinned {
    session: Session,
    started: std::time::Instant,
    model: Model,
    query: &'static str,
    expected: Vec<QueryRow>,
    first: QueryPage,
}
impl Pinned {
    async fn new(
        g: &Gateway,
        version: u64,
        model: Model,
        query: &'static str,
    ) -> anyhow::Result<Self> {
        let started = std::time::Instant::now();
        let session = open(g, Some(version)).await?;
        assert_eq!(session.version, version);
        let expected = model
            .cases()
            .into_iter()
            .find(|(text, _)| *text == query)
            .unwrap()
            .1;
        assert!(expected.len() > 1, "fixture must exercise a continuation");
        let first = page(g, &session, 7, query, None, 1).await?;
        Ok(Self {
            session,
            started,
            model,
            query,
            expected,
            first,
        })
    }
    async fn verify(&self, g: &Gateway) -> anyhow::Result<()> {
        verify(g, &self.session, &self.model).await?;
        assert_eq!(
            page(g, &self.session, 7, self.query, None, 1).await?,
            self.first
        );
        // Fixed expected result length bounds continuation consumption even if
        // an erroneous server returns a cycle of otherwise replayable tokens.
        let mut cursor = None;
        for (offset, expected) in self.expected.iter().enumerate() {
            let got = page(g, &self.session, 7, self.query, cursor.clone(), 1).await?;
            assert_eq!(got, page(g, &self.session, 7, self.query, cursor, 1).await?);
            assert_eq!(got.offset as usize, offset);
            assert_eq!(got.total as usize, self.expected.len());
            assert_eq!(got.data.rows, vec![expected.clone()]);
            assert_eq!(got.next.is_some(), offset + 1 < self.expected.len());
            cursor = got.next;
        }
        assert!(cursor.is_none());
        Ok(())
    }
}

pub(super) async fn scenario(
    g: Gateway,
    coordinator: &mut Process,
    a: &mut Process,
    b: &mut Process,
) -> anyhow::Result<()> {
    let mut model = Model::default();
    let mut history = vec![(6, model.clone())];
    let mut pinned = Vec::<Pinned>::new();
    let changes = script();
    assert_eq!(changes.len(), 21);
    for (index, change) in changes.iter().enumerate() {
        // Sequential read phases on each session avoid the documented try-lock
        // contention error. Submit and reads are polled concurrently; neither
        // assertion assumes which internal commit stage wins the race.
        let reads = async {
            for retained in &pinned {
                retained.verify(&g).await?;
            }
            Ok::<_, anyhow::Error>(())
        };
        let (submitted, checked) =
            tokio::join!(call(&g, Operation::Submit(change.write(index))), reads);
        checked?;
        let (_, version) = committed(submitted?);
        assert_eq!(version, 7 + index as u64);
        model.apply(change);
        history.push((version, model.clone()));
        let fresh = open(&g, Some(version)).await?;
        assert_eq!(fresh.version, version);
        verify(&g, &fresh, &model).await?;
        call(&g, Operation::Close { session: fresh }).await?;
        for retained in &pinned {
            retained.verify(&g).await?;
        }
        if index == 9 || index == 13 {
            let query = if index == 9 { PATH_QUERY } else { SCALAR_QUERY };
            pinned.push(Pinned::new(&g, version, model.clone(), query).await?);
        }
    }
    for retained in &pinned {
        eprintln!(
            "dynamic model: retained V{} completed interleaved checks after {:?}; unchanged 60-second TTL",
            retained.session.version,
            retained.started.elapsed()
        );
    }
    // Explicitly replay every intermediate model before restart too, including
    // empty model V6, partial construction and property absence versus Null.
    for (version, expected) in &history {
        let session = open(&g, Some(*version)).await?;
        assert_eq!(session.version, *version);
        verify(&g, &session, expected).await?;
        call(&g, Operation::Close { session }).await?;
    }
    g.shutdown().await;
    for process in [&mut *coordinator, &mut *a, &mut *b] {
        process.stop()?;
    }
    for process in [&mut *coordinator, &mut *a, &mut *b] {
        process.reopen().await?;
    }
    let restarted = Gateway::bind_remote(
        RemoteConfig {
            coordinator: coordinator.client()?,
            binding: binding(),
            members: vec![a.client()?, b.client()?],
        },
        "127.0.0.1:0".parse()?,
        BUDGET,
    )
    .await?;
    for retained in &pinned {
        assert_eq!(
            call(
                &restarted,
                Operation::TraversePage {
                    session: retained.session.clone(),
                    query: Query {
                        partition: 7,
                        text: retained.query.into()
                    },
                    cursor: retained.first.next.clone(),
                    page_size: 1,
                }
            )
            .await,
            Err(gateway::Error::SessionUnavailable)
        );
    }
    for (version, expected) in &history {
        let session = open(&restarted, Some(*version)).await?;
        assert_eq!(session.version, *version);
        verify(&restarted, &session, expected).await?;
        call(&restarted, Operation::Close { session }).await?;
    }
    // A new historical session cannot adopt a token from the old incarnation.
    for retained in &pinned {
        let new = Pinned::new(
            &restarted,
            retained.session.version,
            retained.model.clone(),
            retained.query,
        )
        .await?;
        assert_ne!(new.session.incarnation, retained.session.incarnation);
        // The old descriptor may also have expired during history replay.
        // Isolate incarnation fencing with a live new token/V and new cursor,
        // changing only the incarnation. This cannot pass merely due to expiry
        // of the original retained session; the live session is checked below.
        let mut wrong_incarnation = new.session.clone();
        wrong_incarnation.incarnation = retained.session.incarnation;
        assert_eq!(
            call(
                &restarted,
                Operation::TraversePage {
                    session: wrong_incarnation,
                    query: Query {
                        partition: 7,
                        text: new.query.into()
                    },
                    cursor: new.first.next.clone(),
                    page_size: 1,
                }
            )
            .await,
            Err(gateway::Error::SessionUnavailable)
        );
        assert_eq!(
            call(
                &restarted,
                Operation::TraversePage {
                    session: new.session.clone(),
                    query: Query {
                        partition: 7,
                        text: new.query.into()
                    },
                    cursor: retained.first.next.clone(),
                    page_size: 1,
                }
            )
            .await,
            Err(gateway::Error::Identity)
        );
        new.verify(&restarted).await?;
        call(
            &restarted,
            Operation::Close {
                session: new.session,
            },
        )
        .await?;
    }
    restarted.shutdown().await;
    eprintln!("dynamic model: 21 typed writes; 22 historical versions x 13 complete-result queries before/after three-child and gateway restart; two retained paged versions interleaved with writes");
    Ok(())
}

#[test]
fn independent_model_has_qualified_bags_and_strict_values() {
    let mut model = Model::default();
    for change in script().iter().take(10) {
        model.apply(change);
    }
    assert_eq!(model.vertices(), [(7, 10), (8, 10), (7, 11), (8, 11)]);
    assert_eq!(
        model.both(&[(7, 10)]),
        [(8, 10), (8, 10), (8, 10), (8, 11), (7, 10), (7, 11)]
    );
    assert_eq!(model.paths().len(), 8);
    assert_eq!(distinct(model.markers(&model.vertices()), true).len(), 4);
    model.apply(&Change::VertexProperty((7, 10), None));
    assert_eq!(
        model.markers(&[(7, 10), (7, 11)]),
        [projected(vertex_element((7, 11)), Value::Null)]
    );
    model.apply(&Change::VertexProperty((7, 10), Some(Value::Null)));
    assert_eq!(
        distinct(model.markers(&[(7, 10), (7, 11)]), true),
        [projected(vertex_element((7, 10)), Value::Null)]
    );
    model.apply(&Change::DeleteEdge((7, 11)));
    assert_eq!(model.paths().len(), 5);
    assert!(model.edges.iter().any(|edge| edge.id == (8, 11)));
}
