use super::*;
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
const MAGIC: &[u8; 8] = b"VGIM0001";
const MAX_FILE_BYTES: usize = 64 * 1024 * 1024;
const HEADER_BYTES: usize = 24;
const RESTORE_DEADLINE: Duration = Duration::from_secs(10);
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
struct Limited(Vec<u8>);
impl Write for Limited {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        if self.0.len().saturating_add(b.len()) > MAX_FILE_BYTES - HEADER_BYTES {
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
impl IncrementalGraph {
    /// Write a versioned local checkpoint of one fixed view (maximum 64 MiB).
    /// Sync a same-directory temporary file, rename it, then sync the directory.
    /// An I/O failure after rename may leave a complete new file visible; this
    /// does not provide per-update durability or remote recovery certification.
    pub fn checkpoint(&self, path: impl AsRef<std::path::Path>) -> Result<()> {
        let view = self.read()?;
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
        let mut bytes = Limited(Vec::new());
        serde_json::to_writer(&mut bytes, &payload)?;
        let path = path.as_ref();
        let parent = path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| std::path::Path::new("."));
        let mut nonce = [0u8; 8];
        getrandom::fill(&mut nonce).map_err(|_| bad("checkpoint nonce"))?;
        let tmp = parent.join(format!(
            ".incremental-{:016x}.tmp",
            u64::from_le_bytes(nonce)
        ));
        let result = (|| {
            let mut opts = OpenOptions::new();
            opts.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                opts.mode(0o600);
            }
            let mut f = opts.open(&tmp).map_err(io)?;
            f.write_all(MAGIC).map_err(io)?;
            f.write_all(&(bytes.0.len() as u64).to_le_bytes())
                .map_err(io)?;
            f.write_all(&hash(&bytes.0).to_le_bytes()).map_err(io)?;
            f.write_all(&bytes.0).map_err(io)?;
            f.sync_all().map_err(io)?;
            drop(f);
            fs::rename(&tmp, path).map_err(io)?;
            fs::File::open(parent)
                .and_then(|f| f.sync_all())
                .map_err(io)?;
            Ok(())
        })();
        if result.is_err() {
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
        let path = path.as_ref();
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
        if n as usize != bytes.len() - HEADER_BYTES || hash(&bytes[HEADER_BYTES..]) != digest {
            return Err(bad("checkpoint length/checksum"));
        }
        let p: Payload = serde_json::from_slice(&bytes[HEADER_BYTES..])?;
        if p.identity != identity
            || p.metadata.len() > MAX_METADATA
            || p.vertices.len().saturating_add(p.edges.len()) > limits.max_records
        {
            return Err(bad("checkpoint identity/size"));
        }
        drop(bytes);
        Self::load(
            p.identity, p.sequence, p.metadata, p.vertices, p.edges, mode, limits,
        )
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
    fn load(
        identity: String,
        sequence: u64,
        metadata: Vec<u8>,
        vertices: Vec<Vertex>,
        mut edges: Vec<Edge>,
        mode: IncrementalMode,
        limits: IncrementalLimits,
    ) -> Result<Self> {
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
            s.metadata = metadata.into_boxed_slice().into_vec();
            edges.sort_unstable_by_key(|edge| edge.id);
            s.bytes = s
                .bytes
                .checked_add(s.metadata.len())
                .ok_or_else(|| limit("restore metadata accounting"))?;
            if s.bytes > graph.limits.max_accounted_bytes {
                return Err(limit("restore metadata budget"));
            }
            for v in vertices {
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
                    incoming: Arc::new(Adj::default()),
                    outgoing: Arc::new(Adj::default()),
                };
                s.bytes = s
                    .bytes
                    .checked_add(entry.bytes())
                    .ok_or_else(|| limit("restore accounting"))?;
                if s.bytes > graph.limits.max_accounted_bytes {
                    return Err(limit("restore retained bytes"));
                }
                s.vertices.set(id, Some(entry));
                s.vertex_count += 1;
            }
            for e in edges {
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
                s.bytes = s
                    .bytes
                    .checked_add(e.accounted_bytes() + INDEX_FEE)
                    .ok_or_else(|| limit("restore accounting"))?;
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
                    Arc::make_mut(adj).append_sorted(e.id);
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
        Ok(graph)
    }
}
