//! Offline fixture encoder, NOT a production StorageNode or mutation benchmark.
//! Public v2 preparation validates/encodes records against frozen vertex views.
//! Captured prepared operations are imported later through bounded Raft writes.
use std::{
    collections::BTreeMap,
    fs::File,
    io::{BufWriter, Write},
    path::Path,
    sync::{Arc, Mutex},
};
use velagraph::{
    experimental_graph::{AppendOnlyStore, CrossEdge, VertexRef},
    *,
};
#[path = "corehr/model.rs"]
mod model;
type R<T> = std::result::Result<T, StorageNodeError>;
#[derive(Clone)]
struct Capture {
    view: ReadView,
    out: Arc<Mutex<BufWriter<File>>>,
    vertices: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    stats: Arc<Mutex<(u64, u64)>>,
}
impl StorageAdmin for Capture {
    fn export(&self) -> R<Export> {
        Err(StorageNodeError::Unsupported("offline encoder".into()))
    }
    fn import(&self, _: Export) -> R<()> {
        Err(StorageNodeError::Unsupported("offline encoder".into()))
    }
    fn set_epoch(&self, _: u64) -> R<()> {
        Err(StorageNodeError::Unsupported("offline encoder".into()))
    }
}
impl StorageNode for Capture {
    fn read_view(&self) -> R<ReadView> {
        Ok(self.view.clone())
    }
    fn get(&self, k: &[u8], v: &ReadView) -> R<Option<Vec<u8>>> {
        Ok(v.get(k))
    }
    fn scan(&self, r: KeyRange, l: ScanLimit, v: &ReadView) -> R<Vec<KeyValue>> {
        v.scan(r, l)
    }
    fn batch_write(&self, _: WriteRequest) -> R<WriteResult> {
        Err(StorageNodeError::Unsupported("offline encoder".into()))
    }
    fn batch_write_conditional(&self, r: WriteRequest, s: Option<u64>) -> R<WriteResult> {
        assert_eq!(s, Some(0));
        assert_eq!(r.shard, self.view.shard);
        assert_eq!(r.epoch, 1);
        let mut w = self.out.lock().unwrap();
        let mut stats = self.stats.lock().unwrap();
        for op in &r.operations {
            if let WriteOp::Put { key, value } = op {
                w.write_all(&(key.len() as u32).to_le_bytes()).unwrap();
                w.write_all(&(value.len() as u32).to_le_bytes()).unwrap();
                w.write_all(key).unwrap();
                w.write_all(value).unwrap();
                stats.0 += 1;
                stats.1 += (key.len() + value.len()) as u64;
                if self.view.row_count() == 0 {
                    assert!(self
                        .vertices
                        .lock()
                        .unwrap()
                        .insert(key.clone(), value.clone())
                        .is_none());
                }
            } else {
                panic!("unexpected fixture delete");
            }
        }
        Ok(WriteResult {
            sequence: 0,
            status: WriteStatus::Committed,
            applied_operations: r.operations.len(),
        })
    }
    fn admin(&self) -> &dyn StorageAdmin {
        self
    }
}
fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let args: Vec<_> = std::env::args().collect();
    let root = Path::new(&args[1]);
    std::fs::create_dir_all(root)?;
    let people = args
        .get(2)
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(200_000);
    let departments = args.get(3).map(|s| s.parse()).transpose()?.unwrap_or(5_000);
    let m = model::Model::new(people, departments);
    let t = std::time::Instant::now();
    let mut nodes = (1..=3)
        .map(|p| Capture {
            view: ReadView::from_entries(p, 1, 0, BTreeMap::new()).unwrap(),
            out: Arc::new(Mutex::new(BufWriter::new(
                File::create(root.join(format!("shard-{p}.bin"))).unwrap(),
            ))),
            vertices: Default::default(),
            stats: Default::default(),
        })
        .collect::<Vec<_>>();
    for id in 1..=people + departments {
        let p = model::partition(id);
        let n = &nodes[p as usize - 1];
        let mut v = Vertex::new(id);
        v.labels
            .push(if id <= people { "person" } else { "department" }.into());
        v.properties
            .insert("active".into(), Value::Boolean(model::active(id)));
        v.properties.insert(
            "country".into(),
            Value::Text(
                ["CN", "US", "DE", "SG", "GB"][(model::mix(id ^ 0x434f554e545259) % 5) as usize]
                    .into(),
            ),
        );
        v.properties.insert(
            "name".into(),
            Value::Text(format!(
                "{}-{id:06}",
                if id <= people {
                    "Employee"
                } else {
                    "Department"
                }
            )),
        );
        if id <= people {
            v.properties
                .insert("multi_department".into(), Value::Boolean(id % 5 == 0));
        }
        AppendOnlyStore::new(n.clone(), p as u64, p)
            .prepare_vertex(&v)?
            .submit(n)?;
    }
    for n in &mut nodes {
        let entries = std::mem::take(&mut *n.vertices.lock().unwrap());
        n.view = ReadView::from_entries(n.view.shard, 1, 0, entries)?;
    }
    let mut cross = 0;
    for (i, e) in m.edges.iter().enumerate() {
        let from = model::partition(e.from);
        let to = model::partition(e.to);
        cross += u64::from(from != to);
        let edge = CrossEdge {
            id: i as u64 + 1,
            from: VertexRef {
                partition: from,
                id: e.from,
            },
            to: VertexRef {
                partition: to,
                id: e.to,
            },
            label: e.label.into(),
            properties: BTreeMap::new(),
        };
        let a = &nodes[from as usize - 1];
        let b = &nodes[to as usize - 1];
        AppendOnlyStore::new(a.clone(), from as u64, from)
            .prepare_source(&edge)?
            .submit(a)?;
        AppendOnlyStore::new(b.clone(), to as u64, to)
            .prepare_reverse(&edge)?
            .submit(b)?;
    }
    let stats=nodes.iter().enumerate().map(|(i,n)|{n.out.lock().unwrap().flush().unwrap();let (rows,bytes)=*n.stats.lock().unwrap();serde_json::json!({"partition":i+1,"kv_rows":rows,"raw_bytes":bytes,"d1_accounted_estimate":bytes+64*rows+128*rows.div_ceil(5000)})}).collect::<Vec<_>>();
    let manifest = serde_json::json!({"people":people,"departments":departments,"vertices":people+departments,"edges":m.edges.len(),"cross_partition_edges":cross,"multi_department_people":people/5,"three_department_people":people/20,"fixture_seconds":t.elapsed().as_secs_f64(),"shards":stats,"queries":m.queries()});
    std::fs::write(
        root.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    println!("{}", manifest);
    Ok(())
}
