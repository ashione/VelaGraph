//! Opt-in failure metadata, never request/response values. No wire/API changes.
use super::RemoteError;
use std::{
    fmt, io,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        OnceLock,
    },
};
use tokio::{net::TcpStream, time::Instant};

static ENABLED: OnceLock<bool> = OnceLock::new();
static COUNT: AtomicUsize = AtomicUsize::new(0);
const MAX_LINES: usize = 64;
const MAX_BYTES: usize = 768;
fn enabled_value(value: Option<std::ffi::OsString>) -> bool {
    value.is_some_and(|value| value == "1")
}
fn enabled() -> bool {
    *ENABLED.get_or_init(|| enabled_value(std::env::var_os("VELAGRAPH_REMOTE_DIAGNOSTICS")))
}

pub(super) fn kind(error: &RemoteError) -> &'static str {
    match error {
        RemoteError::NotLeader { .. } => "not_leader",
        RemoteError::Application(_) => "application",
        RemoteError::InvalidArgument => "invalid_argument",
        RemoteError::ResourceExhausted => "resource_exhausted",
        RemoteError::Unavailable => "unavailable",
        RemoteError::Timeout { .. } => "timeout",
        RemoteError::SessionUnavailable => "session_unavailable",
        RemoteError::Protocol => "protocol",
    }
}
/// Classify without formatting Raft errors, which can contain peer/storage data.
pub(super) fn read_barrier_kind(
    error: &openraft::error::RaftError<
        u64,
        openraft::error::CheckIsLeaderError<u64, openraft::BasicNode>,
    >,
) -> &'static str {
    use openraft::error::{CheckIsLeaderError, Fatal, RaftError};
    match error {
        RaftError::APIError(CheckIsLeaderError::ForwardToLeader(_)) => "forward_to_leader",
        RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_)) => "quorum_not_enough",
        RaftError::Fatal(Fatal::StorageError(_)) => "raft_storage",
        RaftError::Fatal(Fatal::Panicked) => "raft_panicked",
        RaftError::Fatal(Fatal::Stopped) => "raft_stopped",
    }
}

struct Line {
    bytes: [u8; MAX_BYTES],
    len: usize,
}
impl fmt::Write for Line {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        for c in text.chars() {
            let c = if c.is_control() { ' ' } else { c };
            let mut bytes = [0; 4];
            let bytes = c.encode_utf8(&mut bytes).as_bytes();
            if self.len + bytes.len() >= MAX_BYTES {
                break;
            }
            self.bytes[self.len..self.len + bytes.len()].copy_from_slice(bytes);
            self.len += bytes.len();
        }
        Ok(())
    }
}
fn emit_to(on: bool, count: &AtomicUsize, writer: &mut impl io::Write, args: fmt::Arguments<'_>) {
    if !on {
        return;
    }
    let Ok(index) = count.try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
        (n < MAX_LINES).then_some(n + 1)
    }) else {
        return;
    };
    let mut line = Line {
        bytes: [0; MAX_BYTES],
        len: 0,
    };
    let _ = fmt::write(
        &mut line,
        format_args!("remote_diag pid={} seq={} ", std::process::id(), index),
    );
    let _ = if index == MAX_LINES - 1 {
        fmt::write(&mut line, format_args!("event=diagnostics_truncated"))
    } else {
        fmt::write(&mut line, args)
    };
    line.bytes[line.len] = b'\n';
    // One bounded best-effort write: no panic, flush, retry, or changed result.
    let _ = writer.write(&line.bytes[..line.len + 1]);
}
pub(super) fn failure(args: fmt::Arguments<'_>) {
    if enabled() {
        emit_to(true, &COUNT, &mut io::stderr(), args);
    }
}

/// Tiny local phase marker. No shared span registry, task or retained storage lock.
pub(super) struct Phase {
    state: Option<(Instant, Instant, Option<SocketAddr>, Option<SocketAddr>)>,
    stage: &'static str,
    done: bool,
    #[cfg(test)]
    sink: Option<TestSink>,
}
#[cfg(test)]
type TestSink = (
    std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    std::sync::Arc<AtomicUsize>,
);
impl Phase {
    pub(super) fn new(stage: &'static str, socket: Option<&TcpStream>, end: Instant) -> Self {
        Self {
            state: enabled().then(|| {
                (
                    Instant::now(),
                    end,
                    socket.and_then(|s| s.local_addr().ok()),
                    socket.and_then(|s| s.peer_addr().ok()),
                )
            }),
            stage,
            done: false,
            #[cfg(test)]
            sink: None,
        }
    }
    pub(super) fn stage(&mut self, stage: &'static str) {
        self.stage = stage;
    }
    fn report(&mut self, reason: &'static str, error: Option<&io::Error>) {
        if self.done {
            return;
        }
        self.done = true;
        if let Some((start, end, local, peer)) = self.state {
            self.emit(format_args!("phase={} reason={} local={local:?} peer={peer:?} elapsed_us={} expired={} io_kind={:?} os={:?}", self.stage, reason, start.elapsed().as_micros(), Instant::now() >= end, error.map(io::Error::kind), error.and_then(io::Error::raw_os_error)));
        }
    }
    fn emit(&self, args: fmt::Arguments<'_>) {
        #[cfg(test)]
        if let Some((sink, count)) = &self.sink {
            if let Ok(mut writer) = sink.lock() {
                emit_to(true, count, &mut *writer, args);
            }
            return;
        }
        failure(args);
    }
    #[cfg(test)]
    pub(super) fn io(&mut self, error: &io::Error) {
        self.report("io", Some(error));
    }
    pub(super) fn finish<T>(&mut self, result: &Result<T, RemoteError>) {
        if let Err(error) = result {
            self.report(kind(error), None);
        }
        self.done = true;
    }
}
impl Drop for Phase {
    fn drop(&mut self) {
        self.report(
            if std::thread::panicking() {
                "unwind"
            } else {
                "abandoned"
            },
            None,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn read_barrier_categories_and_query_label_do_not_expose_payloads() {
        use openraft::error::{
            CheckIsLeaderError, Fatal, ForwardToLeader, QuorumNotEnough, RaftError,
        };
        let cases = [
            (
                RaftError::APIError(CheckIsLeaderError::ForwardToLeader(ForwardToLeader::empty())),
                "forward_to_leader",
            ),
            (
                RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(QuorumNotEnough {
                    cluster: "private peer details".into(),
                    got: [42].into_iter().collect(),
                })),
                "quorum_not_enough",
            ),
            (RaftError::Fatal(Fatal::Panicked), "raft_panicked"),
            (RaftError::Fatal(Fatal::Stopped), "raft_stopped"),
            (
                RaftError::Fatal(Fatal::StorageError(openraft::StorageError::from_io_error(
                    openraft::ErrorSubject::Store,
                    openraft::ErrorVerb::Read,
                    std::io::Error::other("private storage path"),
                ))),
                "raft_storage",
            ),
        ];
        for (error, expected) in cases {
            assert_eq!(read_barrier_kind(&error), expected);
        }
    }

    #[test]
    fn disabled_utf8_total_cap_and_write_failure_are_bounded() {
        assert!(!enabled_value(None));
        assert!(!enabled_value(Some("true".into())));
        assert!(enabled_value(Some("1".into())));
        let count = AtomicUsize::new(0);
        let mut out = Vec::new();
        emit_to(false, &count, &mut out, format_args!("hidden"));
        assert!(out.is_empty());
        assert_eq!(count.load(Ordering::Relaxed), 0);
        for _ in 0..100 {
            emit_to(
                true,
                &count,
                &mut out,
                format_args!("{}\n", "界".repeat(1000)),
            );
        }
        let text = std::str::from_utf8(&out).unwrap();
        assert_eq!(text.lines().count(), MAX_LINES);
        assert!(text.lines().all(|line| line.len() < MAX_BYTES));
        assert_eq!(text.matches("diagnostics_truncated").count(), 1);
        struct Broken;
        impl io::Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> io::Result<()> {
                panic!("must not flush")
            }
        }
        emit_to(
            true,
            &AtomicUsize::new(0),
            &mut Broken,
            format_args!("failure"),
        );
    }
    #[test]
    fn categories_never_expose_write_identity_and_results_are_not_replaced() {
        let original = Err::<(), _>(RemoteError::Timeout {
            request_id: Some([42; 16]),
        });
        let mut phase = Phase {
            state: None,
            stage: "test",
            done: false,
            #[cfg(test)]
            sink: None,
        };
        phase.finish(&original);
        assert_eq!(kind(original.as_ref().unwrap_err()), "timeout");
        assert!(matches!(
            original,
            Err(RemoteError::Timeout {
                request_id: Some([42, ..])
            })
        ));
        assert!(phase.done);
        let mut success = Phase {
            state: None,
            stage: "test",
            done: false,
            #[cfg(test)]
            sink: None,
        };
        success.finish(&Ok(()));
        assert!(success.done);
    }
    fn captured(expired: bool) -> (Phase, std::sync::Arc<std::sync::Mutex<Vec<u8>>>) {
        let sink = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let now = Instant::now();
        let end = if expired {
            now - std::time::Duration::from_secs(1)
        } else {
            now + std::time::Duration::from_secs(3600)
        };
        (
            Phase {
                state: Some((now, end, None, None)),
                stage: "read_body",
                done: false,
                sink: Some((sink.clone(), std::sync::Arc::new(AtomicUsize::new(0)))),
            },
            sink,
        )
    }
    fn output(sink: &std::sync::Arc<std::sync::Mutex<Vec<u8>>>) -> String {
        String::from_utf8(sink.lock().unwrap().clone()).unwrap()
    }
    #[test]
    fn enabled_success_and_error_preserve_results_and_emit_once() {
        let (mut phase, sink) = captured(false);
        phase.finish(&Ok(()));
        drop(phase);
        assert!(output(&sink).is_empty());
        let (mut phase, sink) = captured(false);
        let original = Err::<(), _>(RemoteError::Unavailable);
        phase.io(&io::ErrorKind::BrokenPipe.into());
        phase.finish(&original);
        drop(phase);
        let text = output(&sink);
        assert_eq!(text.lines().count(), 1);
        assert!(
            text.contains("reason=io")
                && text.contains("expired=false")
                && text.contains("BrokenPipe")
        );
        assert!(matches!(original, Err(RemoteError::Unavailable)));
    }
    #[test]
    fn cancellation_unwind_and_expired_drop_are_distinct_without_sleep() {
        let (phase, sink) = captured(false);
        let mut future = Box::pin(async move {
            let _phase = phase;
            std::future::pending::<()>().await
        });
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(future.as_mut(), &mut context).is_pending());
        drop(future);
        assert!(
            output(&sink).contains("reason=abandoned") && output(&sink).contains("expired=false")
        );
        let (phase, sink) = captured(false);
        assert!(std::panic::catch_unwind(|| {
            let _phase = phase;
            panic!("test unwind");
        })
        .is_err());
        assert!(output(&sink).contains("reason=unwind") && output(&sink).contains("expired=false"));
        let (phase, sink) = captured(true);
        drop(phase);
        assert!(
            output(&sink).contains("reason=abandoned") && output(&sink).contains("expired=true")
        );
    }
}
