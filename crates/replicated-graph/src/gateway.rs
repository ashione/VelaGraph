//! Same-host, same-trust-domain read-only graph sessions over Linux UDS.
//! This is not a cross-host service or multi-tenant authorization boundary.
//! Directory owners and root are trusted; no write/admin operation is exposed.
use crate::routing::{RouteError, RouteResolver};
use rocksdb_sql_table::{GremlinResult, GremlinRow, PathElement, StorageNodeError, Value};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{self, Read, Write},
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Maximum live connection workers per server, including expiring work.
pub const MAX_CONNECTIONS: usize = 8;
/// Maximum JSON request length, excluding the four-byte header.
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
/// Maximum JSON response length, excluding the four-byte header.
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_BUDGET: Duration = Duration::from_secs(60);
const CLIENT_IO_TIMEOUT: Duration = Duration::from_secs(5);

/// Server policy. Directory must already exist, owned by the effective UID,
/// mode 0700; the socket path must not exist. No stale path is deleted at bind.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// Nonexistent socket pathname inside the trusted directory.
    pub socket_path: PathBuf,
    /// Only this kernel-reported peer UID is admitted.
    pub peer_uid: u32,
    /// Absolute per-frame I/O bound, also the idle timeout (1 ms to 5 s).
    pub io_timeout: Duration,
}
impl GatewayConfig {
    /// Configure a one-second frame/idle timeout.
    pub fn new(path: impl Into<PathBuf>, peer_uid: u32) -> Self {
        Self {
            socket_path: path.into(),
            peer_uid,
            io_timeout: Duration::from_secs(1),
        }
    }
}
/// Stable snapshot identity returned by the leader-barrier open operation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    /// Pinned data sequence.
    pub sequence: u64,
    /// Pinned data epoch.
    pub epoch: u64,
    /// Metadata sequence that selected this placement.
    pub metadata_sequence: u64,
}
/// Bounded wire errors intentionally omit storage keys and query contents.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
#[serde(tag = "code", rename_all = "snake_case", deny_unknown_fields)]
pub enum RemoteError {
    #[error("session timed out")]
    Timeout {},
    #[error("resource limit exceeded")]
    ResourceExhausted {},
    #[error("invalid request")]
    InvalidRequest {},
    #[error("unsupported operation")]
    Unsupported {},
    #[error("invalid Gremlin syntax")]
    Syntax {},
    #[error("graph operation failed")]
    Graph {},
    #[error("storage unavailable")]
    Unavailable {},
    #[error("storage failure")]
    Storage {},
    #[error("not leader")]
    NotLeader { leader: Option<u64> },
    #[error("shard fenced")]
    Fenced {
        shard: u64,
        epoch: u64,
        current: u64,
    },
}
/// Transport/protocol errors lose the session; no operation is replayed.
#[derive(Debug, thiserror::Error)]
pub enum GatewayError {
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Remote(#[from] RemoteError),
    #[error("invalid gateway protocol")]
    Protocol,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Request {
    OpenRead {
        version: u32,
        partition: u32,
        budget_ms: u64,
    },
    Gremlin {
        query: String,
    },
    Close {},
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
enum Response {
    Opened { snapshot: Snapshot },
    Rows { rows: Vec<Row> },
    Closed {},
    Error { error: RemoteError },
}
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Row {
    Vertex(u64),
    Edge(u64),
    Value(Value),
    Path(Vec<Element>),
    Count(u64),
}
#[derive(Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "id",
    rename_all = "snake_case",
    deny_unknown_fields
)]
enum Element {
    Vertex(u64),
    Edge(u64),
}
impl From<GremlinRow> for Row {
    fn from(row: GremlinRow) -> Self {
        match row {
            GremlinRow::Vertex(v) => Self::Vertex(v),
            GremlinRow::Edge(v) => Self::Edge(v),
            GremlinRow::Value(v) => Self::Value(v),
            GremlinRow::Count(v) => Self::Count(v),
            GremlinRow::Path(v) => Self::Path(
                v.into_iter()
                    .map(|e| match e {
                        PathElement::Vertex(v) => Element::Vertex(v),
                        PathElement::Edge(v) => Element::Edge(v),
                    })
                    .collect(),
            ),
        }
    }
}
impl From<Row> for GremlinRow {
    fn from(row: Row) -> Self {
        match row {
            Row::Vertex(v) => Self::Vertex(v),
            Row::Edge(v) => Self::Edge(v),
            Row::Value(v) => Self::Value(v),
            Row::Count(v) => Self::Count(v),
            Row::Path(v) => Self::Path(
                v.into_iter()
                    .map(|e| match e {
                        Element::Vertex(v) => PathElement::Vertex(v),
                        Element::Edge(v) => PathElement::Edge(v),
                    })
                    .collect(),
            ),
        }
    }
}
fn route_error(e: RouteError) -> RemoteError {
    match e {
        RouteError::Storage(e) => match e {
            StorageNodeError::Timeout { .. } => RemoteError::Timeout {},
            StorageNodeError::Fenced {
                shard,
                epoch,
                current,
            } => RemoteError::Fenced {
                shard,
                epoch,
                current,
            },
            StorageNodeError::NotLeader { leader } => RemoteError::NotLeader { leader },
            StorageNodeError::ResourceExhausted(_) => RemoteError::ResourceExhausted {},
            StorageNodeError::Unsupported(_) => RemoteError::Unsupported {},
            StorageNodeError::Unavailable(_) => RemoteError::Unavailable {},
            StorageNodeError::InvalidArgument(_) => RemoteError::InvalidRequest {},
            _ => RemoteError::Storage {},
        },
        RouteError::Graph(e) => match e {
            rocksdb_sql_table::Error::GremlinParse(_) => RemoteError::Syntax {},
            rocksdb_sql_table::Error::UnsupportedGremlinStep(_)
            | rocksdb_sql_table::Error::UnsupportedGremlinArgument(_)
            | rocksdb_sql_table::Error::UnsupportedGremlinValue(_) => RemoteError::Unsupported {},
            rocksdb_sql_table::Error::TraversalLimitExceeded(_)
            | rocksdb_sql_table::Error::GremlinComplexityLimit { .. } => {
                RemoteError::ResourceExhausted {}
            }
            _ => RemoteError::Graph {},
        },
        RouteError::InvalidArgument(_) | RouteError::MissingPartition(_) => {
            RemoteError::InvalidRequest {}
        }
        RouteError::MissingReplica { .. } => RemoteError::Unavailable {},
        _ => RemoteError::Storage {},
    }
}
fn invalid(s: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, s)
}
fn remaining(end: Instant) -> io::Result<Duration> {
    end.checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "gateway deadline"))
}
fn io_end(session: Instant, timeout: Duration) -> Instant {
    session.min(Instant::now() + timeout)
}
fn peer(stream: &UnixStream, uid: u32) -> io::Result<()> {
    if rustix::net::sockopt::socket_peercred(stream)?.uid.as_raw() != uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "peer UID mismatch",
        ));
    }
    Ok(())
}
// Canonicalize once; all ancestors must be owned by root/expected UID, and
// unwritable by others except root-owned sticky directories (e.g. /tmp).
fn trusted_path(path: &Path, uid: u32) -> io::Result<PathBuf> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .ok_or_else(|| invalid("socket needs directory"))?;
    let name = path
        .file_name()
        .ok_or_else(|| invalid("socket needs filename"))?;
    if fs::symlink_metadata(parent)?.file_type().is_symlink() {
        return Err(invalid("symlink directory"));
    }
    let parent = parent.canonicalize()?;
    let m = fs::symlink_metadata(&parent)?;
    if !m.is_dir() || m.uid() != uid || m.mode() & 0o7777 != 0o700 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "directory must be owned and mode 0700",
        ));
    }
    for p in parent.ancestors().skip(1) {
        let m = fs::symlink_metadata(p)?;
        if !m.is_dir()
            || (m.uid() != 0 && m.uid() != uid)
            || (m.mode() & 0o022 != 0 && !(m.uid() == 0 && m.mode() & 0o1000 != 0))
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "untrusted ancestor",
            ));
        }
    }
    Ok(parent.join(name))
}
fn read_exact_until(stream: &mut UnixStream, mut buf: &mut [u8], end: Instant) -> io::Result<()> {
    while !buf.is_empty() {
        stream.set_read_timeout(Some(remaining(end)?))?;
        match stream.read(buf) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "closed frame")),
            Ok(n) => buf = &mut buf[n..],
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    remaining(end)?;
    Ok(())
}
fn read_frame(stream: &mut UnixStream, cap: usize, end: Instant) -> io::Result<Vec<u8>> {
    let mut header = [0; 4];
    read_exact_until(stream, &mut header, end)?;
    let len = u32::from_be_bytes(header) as usize;
    if len == 0 || len > cap {
        return Err(invalid("frame length limit"));
    }
    let mut body = vec![0; len];
    read_exact_until(stream, &mut body, end)?;
    Ok(body)
}
struct Capped {
    bytes: Vec<u8>,
    cap: usize,
}
impl Write for Capped {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.len() > self.cap - self.bytes.len() {
            return Err(invalid("frame length limit"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn encode(value: &impl Serialize, cap: usize) -> io::Result<Vec<u8>> {
    let mut writer = Capped {
        bytes: Vec::new(),
        cap,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| invalid("encoding or frame limit"))?;
    Ok(writer.bytes)
}
fn write_frame(stream: &mut UnixStream, bytes: &[u8], end: Instant) -> io::Result<()> {
    let header = (bytes.len() as u32).to_be_bytes();
    for mut buf in [&header[..], bytes] {
        while !buf.is_empty() {
            stream.set_write_timeout(Some(remaining(end)?))?;
            match stream.write(buf) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(n) => buf = &buf[n..],
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
    }
    remaining(end)?;
    Ok(())
}
fn response(stream: &mut UnixStream, value: Response, end: Instant) -> io::Result<()> {
    let bytes = encode(&value, MAX_RESPONSE_BYTES).or_else(|_| {
        encode(
            &Response::Error {
                error: RemoteError::ResourceExhausted {},
            },
            MAX_RESPONSE_BYTES,
        )
    })?;
    write_frame(stream, &bytes, end)
}
fn serve(mut stream: UnixStream, resolver: RouteResolver, config: GatewayConfig) -> io::Result<()> {
    peer(&stream, config.peer_uid)?;
    let frame = read_frame(
        &mut stream,
        MAX_REQUEST_BYTES,
        Instant::now() + config.io_timeout,
    )?;
    let request =
        serde_json::from_slice::<Request>(&frame).map_err(|_| invalid("invalid request"))?;
    let (partition, budget) = match request {
        Request::OpenRead {
            version: 1,
            partition,
            budget_ms,
        } if (1..=60000).contains(&budget_ms) => (partition, Duration::from_millis(budget_ms)),
        _ => return Err(invalid("expected version 1 open")),
    };
    let end = Instant::now() + budget;
    let view = match resolver.read_view(partition, remaining(end)?) {
        Ok(v) => v,
        Err(e) => {
            response(
                &mut stream,
                Response::Error {
                    error: route_error(e),
                },
                io_end(end, config.io_timeout),
            )?;
            return Ok(());
        }
    };
    response(
        &mut stream,
        Response::Opened {
            snapshot: Snapshot {
                sequence: view.sequence(),
                epoch: view.epoch(),
                metadata_sequence: view.metadata_sequence(),
            },
        },
        io_end(end, config.io_timeout),
    )?;
    loop {
        let frame = read_frame(
            &mut stream,
            MAX_REQUEST_BYTES,
            io_end(end, config.io_timeout),
        )?;
        let request =
            serde_json::from_slice::<Request>(&frame).map_err(|_| invalid("invalid request"))?;
        let reply = match request {
            Request::Gremlin { query } => match view.execute_gremlin(&query) {
                Ok(result) => Response::Rows {
                    rows: result.rows.into_iter().map(Row::from).collect(),
                },
                Err(e) => Response::Error {
                    error: route_error(e),
                },
            },
            Request::Close {} => {
                return response(
                    &mut stream,
                    Response::Closed {},
                    io_end(end, config.io_timeout),
                );
            }
            Request::OpenRead { .. } => return Err(invalid("session already open")),
        };
        response(&mut stream, reply, io_end(end, config.io_timeout))?;
    }
}
struct Slot(Arc<AtomicUsize>);
impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
struct SocketFile {
    path: PathBuf,
    dev: u64,
    ino: u64,
}
impl Drop for SocketFile {
    fn drop(&mut self) {
        if let Ok(m) = fs::symlink_metadata(&self.path) {
            if m.dev() == self.dev && m.ino() == self.ino && m.file_type().is_socket() {
                let _ = fs::remove_file(&self.path);
            }
        }
    }
}
/// Owns the accept thread and bounded workers. Drop/shutdown joins real workers,
/// so call shutdown off Tokio workers while the hosting runtime remains alive.
pub struct GatewayServer {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    active: Arc<AtomicUsize>,
    thread: Option<JoinHandle<io::Result<()>>>,
}
impl GatewayServer {
    /// Bind without replacing existing files and start bounded worker threads.
    /// The embedding application must keep the resolver's runtime running.
    pub fn bind(resolver: RouteResolver, config: GatewayConfig) -> io::Result<Self> {
        if config.io_timeout < Duration::from_millis(1) || config.io_timeout > CLIENT_IO_TIMEOUT {
            return Err(invalid("I/O timeout must be 1ms..5s"));
        }
        let path = trusted_path(&config.socket_path, rustix::process::geteuid().as_raw())?;
        match fs::symlink_metadata(&path) {
            Ok(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "socket path exists",
                ))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let listener = UnixListener::bind(&path)?;
        let m = fs::symlink_metadata(&path)?;
        let socket = SocketFile {
            path: path.clone(),
            dev: m.dev(),
            ino: m.ino(),
        };
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        let stop = Arc::new(AtomicBool::new(false));
        let active = Arc::new(AtomicUsize::new(0));
        let stopping = stop.clone();
        let count = active.clone();
        let thread = thread::Builder::new()
            .name("graph-uds-accept".into())
            .spawn(move || {
                let _socket = socket;
                let mut workers: Vec<JoinHandle<()>> = Vec::new();
                let result = (|| {
                    while !stopping.load(Ordering::SeqCst) {
                        let mut i = 0;
                        while i < workers.len() {
                            if workers[i].is_finished() {
                                let _ = workers.swap_remove(i).join();
                            } else {
                                i += 1;
                            }
                        }
                        match listener.accept() {
                            Ok((stream, _)) => {
                                if workers.len() >= MAX_CONNECTIONS {
                                    drop(stream);
                                    continue;
                                }
                                if peer(&stream, config.peer_uid).is_err() {
                                    continue;
                                }
                                count.fetch_add(1, Ordering::SeqCst);
                                let slot = Slot(count.clone());
                                let resolver = resolver.clone();
                                let config = config.clone();
                                workers.push(
                                    thread::Builder::new()
                                        .name("graph-uds-session".into())
                                        .spawn(move || {
                                            let _slot = slot;
                                            let _ = serve(stream, resolver, config);
                                        })?,
                                );
                            }
                            Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                                thread::sleep(Duration::from_millis(5))
                            }
                            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                            Err(e) => return Err(e),
                        }
                    }
                    Ok(())
                })();
                drop(listener);
                for worker in workers {
                    let _ = worker.join();
                }
                result
            })?;
        Ok(Self {
            path,
            stop,
            active,
            thread: Some(thread),
        })
    }
    /// Canonical pathname of this server's socket.
    pub fn local_addr(&self) -> &Path {
        &self.path
    }
    /// Live admitted workers, including ones finishing after client departure.
    pub fn active_connections(&self) -> usize {
        self.active.load(Ordering::SeqCst)
    }
    /// Stop accepting, join bounded workers, and remove only our socket inode.
    /// Call outside the hosting Tokio worker threads.
    pub fn shutdown(mut self) -> io::Result<()> {
        self.join()
    }
    fn join(&mut self) -> io::Result<()> {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            t.join()
                .map_err(|_| io::Error::other("gateway accept thread panicked"))?
        } else {
            Ok(())
        }
    }
}
impl Drop for GatewayServer {
    fn drop(&mut self) {
        let _ = self.join();
    }
}
/// One explicit snapshot session. No reconnect, replay, or snapshot substitution.
pub struct Client {
    stream: UnixStream,
    end: Instant,
    snapshot: Snapshot,
    failed: bool,
}
impl Client {
    /// Open one session with a total local budget of 1 ms to 60 s.
    /// Socket ownership/mode and kernel peer credentials are verified. A full
    /// connection backlog fails immediately rather than blocking or retrying.
    pub fn open(
        path: impl AsRef<Path>,
        expected_server_uid: u32,
        partition: u32,
        budget: Duration,
    ) -> Result<Self, GatewayError> {
        if budget < Duration::from_millis(1) || budget > MAX_BUDGET {
            return Err(invalid("budget must be 1ms..60s").into());
        }
        let end = Instant::now() + budget;
        let path = trusted_path(path.as_ref(), expected_server_uid)?;
        let m = fs::symlink_metadata(&path)?;
        if !m.file_type().is_socket()
            || m.uid() != expected_server_uid
            || m.mode() & 0o7777 != 0o600
        {
            return Err(io::Error::new(io::ErrorKind::PermissionDenied, "untrusted socket").into());
        }
        let fd = rustix::net::socket_with(
            rustix::net::AddressFamily::UNIX,
            rustix::net::SocketType::STREAM,
            rustix::net::SocketFlags::CLOEXEC | rustix::net::SocketFlags::NONBLOCK,
            None,
        )
        .map_err(io::Error::from)?;
        rustix::net::connect(
            &fd,
            &rustix::net::SocketAddrUnix::new(&path).map_err(io::Error::from)?,
        )
        .map_err(io::Error::from)?;
        let stream = UnixStream::from(fd);
        stream.set_nonblocking(false)?;
        peer(&stream, expected_server_uid)?;
        let mut client = Self {
            stream,
            end,
            snapshot: Snapshot {
                sequence: 0,
                epoch: 0,
                metadata_sequence: 0,
            },
            failed: false,
        };
        let budget_ms = remaining(end)?.as_millis() as u64;
        if budget_ms == 0 {
            return Err(io::Error::new(io::ErrorKind::TimedOut, "open budget exhausted").into());
        }
        match client.request(Request::OpenRead {
            version: 1,
            partition,
            budget_ms,
        })? {
            Response::Opened { snapshot } => client.snapshot = snapshot,
            _ => return Err(GatewayError::Protocol),
        }
        remaining(client.end)?;
        Ok(client)
    }
    /// Identity of the fixed view; it never changes during this connection.
    pub fn snapshot(&self) -> &Snapshot {
        &self.snapshot
    }
    fn request(&mut self, request: Request) -> Result<Response, GatewayError> {
        if self.failed {
            return Err(GatewayError::Protocol);
        }
        let result = (|| {
            let bytes = encode(&request, MAX_REQUEST_BYTES)?;
            write_frame(
                &mut self.stream,
                &bytes,
                io_end(self.end, CLIENT_IO_TIMEOUT),
            )?;
            let frame = read_frame(
                &mut self.stream,
                MAX_RESPONSE_BYTES,
                io_end(self.end, CLIENT_IO_TIMEOUT),
            )?;
            let reply: Response =
                serde_json::from_slice(&frame).map_err(|_| GatewayError::Protocol)?;
            remaining(self.end)?;
            match reply {
                Response::Error { error } => Err(GatewayError::Remote(error)),
                v => Ok(v),
            }
        })();
        if result.is_err() {
            self.failed = true;
            let _ = self.stream.shutdown(std::net::Shutdown::Both);
        }
        result
    }
    /// Execute standard supported read-only Gremlin against the pinned view.
    /// Any error makes this client unusable. Open a new client explicitly.
    pub fn execute_gremlin(&mut self, query: &str) -> Result<GremlinResult, GatewayError> {
        if query.len() > MAX_REQUEST_BYTES {
            self.failed = true;
            let _ = self.stream.shutdown(std::net::Shutdown::Both);
            return Err(RemoteError::ResourceExhausted {}.into());
        }
        match self.request(Request::Gremlin {
            query: query.to_owned(),
        })? {
            Response::Rows { rows } => {
                let result = GremlinResult {
                    rows: rows.into_iter().map(GremlinRow::from).collect(),
                };
                if let Err(e) = remaining(self.end) {
                    self.failed = true;
                    let _ = self.stream.shutdown(std::net::Shutdown::Both);
                    return Err(e.into());
                }
                Ok(result)
            }
            _ => {
                self.failed = true;
                let _ = self.stream.shutdown(std::net::Shutdown::Both);
                Err(GatewayError::Protocol)
            }
        }
    }
    /// Explicitly release the session. Dropping the client also disconnects.
    pub fn close(mut self) -> Result<(), GatewayError> {
        match self.request(Request::Close {})? {
            Response::Closed {} => Ok(()),
            _ => Err(GatewayError::Protocol),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_rejects_unsupported_fields_versions_and_trailing_json() {
        for bytes in [
            br#"{"op":"open_read","version":1,"partition":0,"budget_ms":10,"admin":true}"#
                .as_slice(),
            br#"{"op":"gremlin","query":"g.V()","write":true}"#,
            br#"{"op":"write","query":"g.V()"}"#,
            br#"{"op":"close","write":true}"#,
            br#"{"op":"close"} {}"#,
            br#"{"op":"close","op":"close"}"#,
        ] {
            assert!(serde_json::from_slice::<Request>(bytes).is_err());
        }
        assert!(matches!(
            serde_json::from_slice::<Request>(
                br#"{"op":"open_read","version":2,"partition":0,"budget_ms":10}"#
            )
            .unwrap(),
            Request::OpenRead { version: 2, .. }
        )); // Version support is checked before resolver dispatch in serve().
    }

    #[test]
    fn unit_shaped_responses_and_errors_reject_unknown_fields() {
        assert!(serde_json::from_str::<Request>(r#"{"op":"close"}"#).is_ok());
        assert!(serde_json::from_str::<Response>(r#"{"status":"closed"}"#).is_ok());
        assert!(serde_json::from_str::<Response>(r#"{"status":"closed","extra":0}"#).is_err());
        for code in [
            "timeout",
            "resource_exhausted",
            "invalid_request",
            "unsupported",
            "syntax",
            "graph",
            "unavailable",
            "storage",
        ] {
            let plain = format!(r#"{{"code":"{code}"}}"#);
            assert!(
                serde_json::from_str::<RemoteError>(&plain).is_ok(),
                "{code}"
            );
            let extra = format!(r#"{{"code":"{code}","extra":0}}"#);
            assert!(
                serde_json::from_str::<RemoteError>(&extra).is_err(),
                "{code}"
            );
            let wrapped = format!(r#"{{"status":"error","error":{extra}}}"#);
            assert!(
                serde_json::from_str::<Response>(&wrapped).is_err(),
                "{code}"
            );
        }
    }

    #[test]
    fn route_errors_preserve_fencing_timeout_and_leader_identity() {
        assert_eq!(
            route_error(RouteError::Storage(StorageNodeError::Fenced {
                shard: 17,
                epoch: 21,
                current: 34,
            })),
            RemoteError::Fenced {
                shard: 17,
                epoch: 21,
                current: 34
            }
        );
        assert_eq!(
            route_error(RouteError::Storage(StorageNodeError::Timeout {
                request_id: None,
            })),
            RemoteError::Timeout {}
        );
        for leader in [None, Some(99)] {
            assert_eq!(
                route_error(RouteError::Storage(StorageNodeError::NotLeader { leader })),
                RemoteError::NotLeader { leader }
            );
        }
    }

    #[test]
    fn parser_complexity_is_a_remote_resource_error() {
        assert_eq!(
            route_error(RouteError::Graph(
                rocksdb_sql_table::Error::GremlinComplexityLimit {
                    marks: 65,
                    limit: 64,
                }
            )),
            RemoteError::ResourceExhausted {}
        );
    }

    #[test]
    fn encoding_never_appends_past_cap() {
        let mut writer = Capped {
            bytes: Vec::new(),
            cap: 8,
        };
        writer.write_all(b"12345678").unwrap();
        assert!(writer.write_all(b"9").is_err());
        assert_eq!(writer.bytes, b"12345678");
        assert!(encode(&"123456789", 8).is_err());
        assert_eq!(encode(&"123456", 8).unwrap(), b"\"123456\"");
    }

    #[test]
    fn oversized_header_fails_without_body() {
        let (mut reader, mut writer) = UnixStream::pair().unwrap();
        writer.write_all(&u32::MAX.to_be_bytes()).unwrap();
        let e = read_frame(
            &mut reader,
            MAX_REQUEST_BYTES,
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap_err();
        assert_eq!(e.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn row_wire_round_trips_every_supported_shape() {
        let rows = vec![
            GremlinRow::Vertex(u64::MAX),
            GremlinRow::Edge(42),
            GremlinRow::Value(Value::Null),
            GremlinRow::Value(Value::Integer(i64::MIN)),
            GremlinRow::Value(Value::Real(1.25)),
            GremlinRow::Value(Value::Boolean(true)),
            GremlinRow::Value(Value::Text("hello".into())),
            GremlinRow::Path(vec![PathElement::Vertex(1), PathElement::Edge(2)]),
            GremlinRow::Count(7),
        ];
        let response = Response::Rows {
            rows: rows.clone().into_iter().map(Row::from).collect(),
        };
        let bytes = encode(&response, MAX_RESPONSE_BYTES).unwrap();
        let Response::Rows { rows: decoded } = serde_json::from_slice(&bytes).unwrap() else {
            panic!("wrong response")
        };
        assert_eq!(
            decoded
                .into_iter()
                .map(GremlinRow::from)
                .collect::<Vec<_>>(),
            rows
        );
    }
}
