use super::*;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
mod destination;
const MAGIC: &[u8; 8] = b"VGIM0001";
const MAX_FILE_BYTES: usize = 64 * 1024 * 1024;
const HEADER_BYTES: usize = 24;
const RESTORE_DEADLINE: Duration = Duration::from_secs(10);
const TEMP_PREFIX: &str = ".incremental-";
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    identity: String,
    sequence: u64,
    metadata: Vec<u8>,
    vertices: Vec<Vertex>,
    edges: Vec<Edge>,
}
fn io(e: std::io::Error) -> Error {
    Error::IncrementalMemory(Box::new(IncrementalMemoryError::Io(e)))
}
fn hash(bytes: &[u8]) -> u64 {
    let mut h = 14695981039346656037u64;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(1099511628211)
    }
    h
}
struct Limited(Vec<u8>, bool);
impl Write for Limited {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(b.len()) > MAX_FILE_BYTES - HEADER_BYTES {
            self.1 = true;
            return Err(std::io::Error::other("checkpoint limit"));
        }
        self.0.extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn compact_properties(props: BTreeMap<String, crate::Value>) -> BTreeMap<String, crate::Value> {
    props
        .into_iter()
        .map(|(key, value)| {
            (
                key.into_boxed_str().into_string(),
                match value {
                    crate::Value::Text(s) => crate::Value::Text(s.into_boxed_str().into_string()),
                    v => v,
                },
            )
        })
        .collect()
}
fn compact_vertex(mut vertex: Vertex) -> Vertex {
    vertex.labels = vertex
        .labels
        .into_iter()
        .map(|s| s.into_boxed_str().into_string())
        .collect::<Vec<_>>()
        .into_boxed_slice()
        .into_vec();
    vertex.properties = compact_properties(vertex.properties);
    vertex
}
fn compact_edge(mut edge: Edge) -> Edge {
    edge.label = edge.label.into_boxed_str().into_string();
    edge.properties = compact_properties(edge.properties);
    edge
}
fn compacted_properties_bytes(properties: &BTreeMap<String, crate::Value>) -> usize {
    properties.iter().fold(0usize, |sum, (key, value)| {
        sum.saturating_add(crate::query_memory::MAP_ENTRY)
            .saturating_add(key.len())
            .saturating_add(match value {
                crate::Value::Text(s) => s.len(),
                _ => 0,
            })
    })
}
fn compacted_vertex_bytes(vertex: &Vertex) -> usize {
    crate::query_memory::RECORD_BASE
        .saturating_add(
            vertex
                .labels
                .len()
                .saturating_mul(std::mem::size_of::<String>()),
        )
        .saturating_add(
            vertex
                .labels
                .iter()
                .fold(0usize, |sum, s| sum.saturating_add(s.len())),
        )
        .saturating_add(compacted_properties_bytes(&vertex.properties))
        .saturating_add(INDEX_FEE)
}
fn compacted_edge_bytes(edge: &Edge) -> usize {
    crate::query_memory::RECORD_BASE
        .saturating_add(edge.label.len())
        .saturating_add(compacted_properties_bytes(&edge.properties))
        .saturating_add(INDEX_FEE)
}
fn admit_bulk_bytes(current: usize, additional: usize, maximum: usize) -> Result<usize> {
    let next = current
        .checked_add(additional)
        .ok_or_else(|| limit("restore accounting"))?;
    if next > maximum {
        return Err(limit("restore retained bytes"));
    }
    Ok(next)
}
fn read_checkpoint(path: &std::path::Path) -> Result<Vec<u8>> {
    if !fs::symlink_metadata(path)
        .map_err(io)?
        .file_type()
        .is_file()
    {
        return Err(bad("checkpoint must be a regular file"));
    }
    let mut f = fs::File::open(path).map_err(io)?;
    let metadata = f.metadata().map_err(io)?;
    if !metadata.file_type().is_file() {
        return Err(bad("checkpoint must be a regular file"));
    }
    if metadata.len() > MAX_FILE_BYTES as u64 {
        return Err(limit("checkpoint file size"));
    }
    let mut bytes = Vec::new();
    Read::by_ref(&mut f)
        .take((MAX_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(io)?;
    if bytes.len() < HEADER_BYTES || bytes.len() > MAX_FILE_BYTES || &bytes[..8] != MAGIC {
        return Err(bad("checkpoint header"));
    }
    let n = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let digest = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    if n != (bytes.len() - HEADER_BYTES) as u64 || hash(&bytes[HEADER_BYTES..]) != digest {
        return Err(bad("checkpoint length/checksum"));
    }
    Ok(bytes)
}
impl IncrementalGraph {
    /// Checkpoint with cooperative caller control before the rename point.
    pub fn checkpoint_with_control(
        &self,
        path: impl AsRef<std::path::Path>,
        control: &crate::ExecutionControl,
    ) -> Result<()> {
        self.checkpoint_checked(path.as_ref(), &|| control.check())
    }
    /// Write a versioned local checkpoint of one fixed view (maximum 64 MiB).
    /// Sync a same-directory temporary file, rename it, then sync the directory.
    /// A persistent directory lock coordinates cooperating writers. An older
    /// sequence, different identity or conflicting equal sequence is rejected.
    /// The captured graph view is released before disk I/O. Protocol lock/temp
    /// filename prefixes are reserved; invalid existing framing/checksums reject.
    /// An I/O failure after rename may leave a complete new file visible; this
    /// does not provide per-update durability or remote recovery certification.
    pub fn checkpoint(&self, path: impl AsRef<std::path::Path>) -> Result<()> {
        self.checkpoint_checked(path.as_ref(), &|| Ok(()))
    }
    fn checkpoint_checked(
        &self,
        path: &std::path::Path,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<()> {
        check()?;
        let path = destination::resolve(path)?;
        let view = self.read()?;
        check()?;
        let token = view.token();
        let s = view.state();
        #[derive(Serialize)]
        struct Borrowed<'a> {
            identity: &'a str,
            sequence: u64,
            metadata: &'a [u8],
            vertices: Vec<&'a Vertex>,
            edges: Vec<&'a Edge>,
        }
        let mut vertices: Vec<_> = view.vertices().collect();
        let mut edges: Vec<_> = view.edges().collect();
        vertices.sort_unstable_by_key(|v| v.id);
        edges.sort_unstable_by_key(|e| e.id);
        let payload = Borrowed {
            identity: &s.identity,
            sequence: s.sequence,
            metadata: &s.metadata,
            vertices,
            edges,
        };
        let mut bytes = Limited(Vec::new(), false);
        let encoded = serde_json::to_writer(&mut bytes, &payload);
        if bytes.1 {
            return Err(limit("checkpoint encoded byte limit"));
        }
        encoded?;
        drop(payload);
        drop(view);
        check()?;
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let mut nonce = [0u8; 8];
        getrandom::fill(&mut nonce).map_err(|_| bad("checkpoint nonce"))?;
        let tmp = parent.join(format!(
            "{TEMP_PREFIX}{:016x}.tmp",
            u64::from_le_bytes(nonce)
        ));
        let mut created = false;
        let result = (|| {
            let mut opts = OpenOptions::new();
            opts.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            let mut f = opts.open(&tmp).map_err(io)?;
            created = true;
            f.write_all(MAGIC).map_err(io)?;
            f.write_all(&(bytes.0.len() as u64).to_le_bytes())
                .map_err(io)?;
            f.write_all(&hash(&bytes.0).to_le_bytes()).map_err(io)?;
            f.write_all(&bytes.0).map_err(io)?;
            f.sync_all().map_err(io)?;
            drop(f);
            check()?;
            let _publication = destination::lock(parent)?;
            check()?;
            destination::check_previous(&path, &token, &bytes.0)?;
            check()?;
            fs::rename(&tmp, &path).map_err(io)?;
            fs::File::open(parent)
                .and_then(|f| f.sync_all())
                .map_err(io)?;
            Ok(())
        })();
        if result.is_err() && created {
            let _ = fs::remove_file(&tmp);
        }
        result
    }
    /// Validate a regular local checkpoint and rebuild its private indexes.
    /// Identity must match. Mutation-batch limits do not constrain restoration;
    /// file size, graph retention and a 10-second index-build deadline do.
    pub fn restore(
        path: impl AsRef<std::path::Path>,
        identity: &str,
        mode: IncrementalMode,
        limits: IncrementalLimits,
    ) -> Result<Self> {
        Self::restore_checked(path.as_ref(), identity, mode, limits, &|| Ok(()))
    }
    /// Restore with cooperative controls before and after file decoding and
    /// through private graph construction. File I/O itself is not preemptible.
    pub fn restore_with_control(
        path: impl AsRef<std::path::Path>,
        identity: &str,
        mode: IncrementalMode,
        limits: IncrementalLimits,
        control: &crate::ExecutionControl,
    ) -> Result<Self> {
        Self::restore_checked(path.as_ref(), identity, mode, limits, &|| control.check())
    }
    fn restore_checked(
        path: &std::path::Path,
        identity: &str,
        mode: IncrementalMode,
        limits: IncrementalLimits,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<Self> {
        check()?;
        let bytes = read_checkpoint(path)?;
        check()?;
        let p: Payload = serde_json::from_slice(&bytes[HEADER_BYTES..])?;
        if p.identity != identity
            || p.metadata.len() > MAX_METADATA
            || p.vertices.len().saturating_add(p.edges.len()) > limits.max_records
        {
            return Err(bad("checkpoint identity/size"));
        }
        drop(bytes);
        Self::load_checked(p, mode, limits, check)
    }
    /// Build a private graph from complete records, starting at sequence zero.
    /// Duplicate IDs, missing endpoints or exceeded construction bounds reject
    /// the whole graph. Metadata is caller-defined and limited to 64 KiB.
    pub fn from_records(
        identity: impl Into<String>,
        mode: IncrementalMode,
        limits: IncrementalLimits,
        vertices: Vec<Vertex>,
        edges: Vec<Edge>,
        metadata: Vec<u8>,
    ) -> Result<Self> {
        Self::load(identity.into(), 0, metadata, vertices, edges, mode, limits)
    }
    /// Construct an unpublished graph with cooperative caller control.
    pub fn from_records_with_control(
        identity: impl Into<String>,
        mode: IncrementalMode,
        limits: IncrementalLimits,
        vertices: Vec<Vertex>,
        edges: Vec<Edge>,
        metadata: Vec<u8>,
        control: &crate::ExecutionControl,
    ) -> Result<Self> {
        Self::load_checked(
            Payload {
                identity: identity.into(),
                sequence: 0,
                metadata,
                vertices,
                edges,
            },
            mode,
            limits,
            &|| control.check(),
        )
    }
    fn load(
        identity: String,
        sequence: u64,
        metadata: Vec<u8>,
        vertices: Vec<Vertex>,
        edges: Vec<Edge>,
        mode: IncrementalMode,
        limits: IncrementalLimits,
    ) -> Result<Self> {
        Self::load_checked(
            Payload {
                identity,
                sequence,
                metadata,
                vertices,
                edges,
            },
            mode,
            limits,
            &|| Ok(()),
        )
    }
    fn load_checked(
        payload: Payload,
        mode: IncrementalMode,
        limits: IncrementalLimits,
        check: &dyn Fn() -> Result<()>,
    ) -> Result<Self> {
        check()?;
        let Payload {
            identity,
            sequence,
            metadata,
            vertices,
            mut edges,
        } = payload;
        if metadata.len() > MAX_METADATA
            || vertices.len().saturating_add(edges.len()) > limits.max_records
        {
            return Err(limit("bulk record/metadata bounds"));
        }
        let graph = Self::new(identity, mode, limits)?;
        {
            let mut guard = graph.inner.write().map_err(|_| bad("poisoned lock"))?;
            let s = Arc::get_mut(&mut guard.current).ok_or_else(|| bad("restore state shared"))?;
            let deadline = Instant::now() + RESTORE_DEADLINE;
            s.bytes = admit_bulk_bytes(s.bytes, metadata.len(), graph.limits.max_accounted_bytes)?;
            s.metadata = metadata.into_boxed_slice().into_vec();
            edges.sort_unstable_by_key(|edge| edge.id);
            for v in vertices {
                check()?;
                if Instant::now() >= deadline {
                    return Err(limit("restore deadline"));
                }
                let admitted = admit_bulk_bytes(
                    s.bytes,
                    compacted_vertex_bytes(&v),
                    graph.limits.max_accounted_bytes,
                )?;
                let v = compact_vertex(v);
                if Instant::now() >= deadline {
                    return Err(limit("restore deadline"));
                }
                validate_vertex(&v)?;
                if s.vertices.get(v.id).is_some() {
                    return Err(bad("duplicate vertex"));
                }
                let id = v.id;
                let entry = VEntry {
                    record: Arc::new(Cached::new(v)),
                    incoming: Adj::default(),
                    outgoing: Adj::default(),
                };
                s.bytes = admitted;
                s.vertices.set(id, Some(entry));
                s.vertex_count += 1;
            }
            for e in edges {
                check()?;
                if Instant::now() >= deadline {
                    return Err(limit("restore deadline"));
                }
                let admitted = admit_bulk_bytes(
                    s.bytes,
                    compacted_edge_bytes(&e),
                    graph.limits.max_accounted_bytes,
                )?;
                let e = compact_edge(e);
                if Instant::now() >= deadline {
                    return Err(limit("restore deadline"));
                }
                validate_edge(&e)?;
                if s.edges.get(e.id).is_some() {
                    return Err(bad("duplicate edge"));
                }
                if s.vertices.get(e.from).is_none() || s.vertices.get(e.to).is_none() {
                    return Err(bad("restore missing endpoint"));
                }
                s.bytes = admitted;
                for (id, incoming) in [(e.from, false), (e.to, true)] {
                    let v = s
                        .vertices
                        .get_mut(id)
                        .ok_or_else(|| bad("restore endpoint"))?;
                    let before = v.bytes();
                    let adj = if incoming {
                        &mut v.incoming
                    } else {
                        &mut v.outgoing
                    };
                    adj.append_sorted(e.id);
                    s.bytes = s
                        .bytes
                        .checked_sub(before)
                        .and_then(|n| n.checked_add(v.bytes()))
                        .ok_or_else(|| limit("restore accounting"))?;
                }
                if s.bytes > graph.limits.max_accounted_bytes {
                    return Err(limit("restore retained bytes"));
                }
                s.edges.set(e.id, Some(Arc::new(Cached::new(e))));
                s.edge_count += 1;
            }
            s.sequence = sequence;
        }
        check()?;
        Ok(graph)
    }
}

#[cfg(test)]
mod control_tests {
    use super::*;
    use std::cell::Cell;
    #[test]
    fn checkpoint_cancel_preserves_destination_and_removes_temporary_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.bin");
        let graph =
            IncrementalGraph::new("save", IncrementalMode::Cow, IncrementalLimits::default())
                .unwrap();
        graph.checkpoint(&path).unwrap();
        let old = fs::read(&path).unwrap();
        graph
            .apply(
                &graph.token().unwrap(),
                &[IncrementalMutation::PutVertex(Vertex::new(1))],
                b"next",
            )
            .unwrap();
        let count = Cell::new(0);
        graph
            .checkpoint_checked(&path, &|| {
                count.set(count.get() + 1);
                Ok(())
            })
            .unwrap();
        let new = fs::read(&path).unwrap();
        assert_ne!(old, new);
        for stop in 1..=count.get() + 1 {
            fs::write(&path, &old).unwrap();
            let calls = Cell::new(0);
            let result = graph.checkpoint_checked(&path, &|| {
                calls.set(calls.get() + 1);
                if calls.get() == stop {
                    Err(Error::ExecutionCancelled)
                } else {
                    Ok(())
                }
            });
            if stop <= count.get() {
                assert!(matches!(result, Err(Error::ExecutionCancelled)));
                assert_eq!(fs::read(&path).unwrap(), old);
            } else {
                result.unwrap();
                assert_eq!(fs::read(&path).unwrap(), new);
            }
            assert!(!fs::read_dir(dir.path()).unwrap().any(|e| e
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".tmp")));
            graph.checkpoint(&path).unwrap();
        }
    }
}
