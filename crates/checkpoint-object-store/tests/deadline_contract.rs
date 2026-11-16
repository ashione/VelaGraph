//! Cumulative body time consumes one operation budget, not a renewed GET budget.
mod common;

use async_trait::async_trait;
use bytes::Bytes;
use checkpoint_object_store::{CheckpointStore, CreateCapability, Error, Limits};
use futures::{stream::BoxStream, Future, Stream, StreamExt};
use object_store::{path::Path, *};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::{Instant, Sleep},
};

const BUDGET: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Debug)]
enum Scenario {
    ManifestItems,
    ChunkItems,
    AcrossGets,
}

#[derive(Debug, Default)]
struct Observations {
    gets: Vec<Path>,
    yielded: Vec<(Path, usize)>,
    dropped: Vec<Path>,
}
#[derive(Debug)]
struct Armed {
    key: Path,
    item: usize,
    deadline: Instant,
}

struct DelayedBody {
    key: Path,
    parts: VecDeque<Bytes>,
    item: usize,
    delay: Duration,
    timer: Option<Pin<Box<Sleep>>>,
    notified: bool,
    armed: mpsc::UnboundedSender<Armed>,
    observations: Arc<Mutex<Observations>>,
}
impl Stream for DelayedBody {
    type Item = Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.parts.is_empty() {
            return Poll::Ready(None);
        }
        if self.timer.is_none() {
            self.timer = Some(Box::pin(tokio::time::sleep(self.delay)));
        }
        // Poll first: the timer must actually be registered before the driver
        // receives permission to move time. Every body item has finite delay.
        if self.timer.as_mut().unwrap().as_mut().poll(cx).is_pending() {
            if !self.notified {
                self.armed
                    .send(Armed {
                        key: self.key.clone(),
                        item: self.item,
                        deadline: self.timer.as_ref().unwrap().deadline(),
                    })
                    .unwrap();
                self.notified = true;
            }
            return Poll::Pending;
        }
        self.timer = None;
        self.notified = false;
        let item = self.item;
        self.item += 1;
        self.observations
            .lock()
            .unwrap()
            .yielded
            .push((self.key.clone(), item));
        Poll::Ready(Some(Ok(self.parts.pop_front().unwrap())))
    }
}
impl Drop for DelayedBody {
    fn drop(&mut self) {
        self.observations
            .lock()
            .unwrap()
            .dropped
            .push(self.key.clone());
    }
}

#[derive(Debug)]
struct TimedStore {
    inner: Arc<memory::InMemory>,
    manifest: Path,
    chunk: Path,
    scenario: Scenario,
    delay: Duration,
    armed: mpsc::UnboundedSender<Armed>,
    observations: Arc<Mutex<Observations>>,
}
impl std::fmt::Display for TimedStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "timed-store")
    }
}
#[async_trait]
impl ObjectStore for TimedStore {
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        assert!(!options.head);
        self.observations.lock().unwrap().gets.push(path.clone());
        let mut result = self.inner.get_opts(path, options).await?;
        let selected = match self.scenario {
            Scenario::ManifestItems => path == &self.manifest,
            Scenario::ChunkItems => path == &self.chunk,
            Scenario::AcrossGets => path == &self.manifest || path == &self.chunk,
        };
        if selected {
            let bytes = self.inner.get(path).await?.bytes().await?;
            let parts = if matches!(self.scenario, Scenario::AcrossGets) {
                vec![bytes]
            } else {
                let middle = bytes.len() / 2;
                assert!(middle > 0);
                vec![bytes.slice(..middle), bytes.slice(middle..)]
            };
            result.payload = GetResultPayload::Stream(
                DelayedBody {
                    key: path.clone(),
                    parts: parts.into(),
                    item: 0,
                    delay: self.delay,
                    timer: None,
                    notified: false,
                    armed: self.armed.clone(),
                    observations: self.observations.clone(),
                }
                .boxed(),
            );
        }
        Ok(result)
    }
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        panic!("unexpected PUT")
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        panic!("unexpected multipart")
    }
    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        panic!("unexpected DELETE")
    }
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        panic!("unexpected LIST")
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        panic!("unexpected LIST")
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        panic!("unexpected COPY")
    }
}

// A runnable yield task prevents paused-clock auto-advance while tokio::fs is
// waiting for its real blocking pool. Only the driver below advances time.
struct ManualClock(tokio::task::JoinHandle<()>);
impl ManualClock {
    fn start() -> Self {
        tokio::time::pause();
        Self(tokio::spawn(async {
            loop {
                tokio::task::yield_now().await;
            }
        }))
    }
}
impl Drop for ManualClock {
    fn drop(&mut self) {
        self.0.abort();
        tokio::time::resume();
    }
}

// Independent wall-clock watchdog: unlike a paused Tokio timeout, it can wake
// a stalled handshake. The thread is stopped/joined on every return or panic.
struct Watchdog {
    stop: std::sync::mpsc::Sender<()>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Watchdog {
    fn start() -> (Self, oneshot::Receiver<()>) {
        let (stop, receive) = std::sync::mpsc::channel();
        let (expired, timeout) = oneshot::channel();
        let thread = std::thread::spawn(move || {
            if matches!(
                receive.recv_timeout(Duration::from_secs(20)),
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ) {
                let _ = expired.send(());
            }
        });
        (
            Self {
                stop,
                thread: Some(thread),
            },
            timeout,
        )
    }
}
impl Drop for Watchdog {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(thread) = self.thread.take() {
            thread.join().unwrap();
        }
    }
}

async fn drive(
    operation: impl Future<Output = checkpoint_object_store::Result<()>>,
    mut armed: mpsc::UnboundedReceiver<Armed>,
) -> (
    checkpoint_object_store::Result<()>,
    Vec<Armed>,
    Duration,
    usize,
) {
    let _clock = ManualClock::start();
    let (_watchdog, mut expired) = Watchdog::start();
    let start = Instant::now();
    // One millisecond avoids timer-wheel boundary ambiguity, still strictly
    // before the second body item at 12s. Never jump straight from 6s to 12s:
    // Timeout polls its inner future first, which might then already succeed.
    let boundary = start + BUDGET + Duration::from_millis(1);
    let mut boundary_polls = 0;
    let mut notifications = Vec::new();
    tokio::pin!(operation);
    let result = loop {
        tokio::select! {
            biased;
            _=&mut expired => panic!("real-time watchdog: stalled deadline test"),
            result=&mut operation => break result,
            next=armed.recv() => {
                let next=next.expect("body notification channel closed");
                // Wake just beyond the timer-wheel tick, without auto-advance.
                let deadline=next.deadline+Duration::from_millis(1);
                notifications.push(next);
                if Instant::now()<boundary && boundary<deadline {
                    tokio::time::advance(boundary-Instant::now()).await;
                    boundary_polls+=1;
                    let result=std::future::poll_fn(|cx| Poll::Ready(operation.as_mut().poll(cx))).await;
                    if let Poll::Ready(result)=result { break result; }
                    // A reset-budget mutant stays pending at the global boundary.
                    // Let its finite, valid body finish so it demonstrably succeeds
                    // instead of merely being stopped by the test watchdog.
                }
                tokio::time::advance(deadline.saturating_duration_since(Instant::now())).await;
            }
        }
    };
    (
        result,
        notifications,
        Instant::now() - start,
        boundary_polls,
    )
}

async fn contract(scenario: Scenario, download: bool) {
    for slow in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        common::fixture(&source, b"data");
        let inner = Arc::new(memory::InMemory::new());
        let writer = CheckpointStore::new(
            inner.clone(),
            "clock".into(),
            Limits {
                chunk_bytes: 4,
                ..Limits::default()
            },
            CreateCapability::AtomicCreateVerified,
        )
        .unwrap();
        let receipt = writer.upload(&source, "one").await.unwrap();
        let manifest = Path::from("clock/checkpoints/one/checkpoint.json");
        let chunk = Path::from(format!("clock/blobs/{:x}", Sha256::digest(b"data")));
        let (armed, receive) = mpsc::unbounded_channel();
        let observations = Arc::new(Mutex::new(Observations::default()));
        let reader = CheckpointStore::new(
            Arc::new(TimedStore {
                inner,
                manifest: manifest.clone(),
                chunk: chunk.clone(),
                scenario,
                delay: Duration::from_secs(if slow { 6 } else { 3 }),
                armed,
                observations: observations.clone(),
            }),
            "clock".into(),
            Limits {
                chunk_bytes: 4,
                timeout: BUDGET,
                ..Limits::default()
            },
            CreateCapability::AtomicCreateVerified,
        )
        .unwrap();
        let destination = dir.path().join("restored");
        let (result, notices, elapsed, boundary_polls) = drive(
            async {
                if download {
                    reader.download(&receipt, &destination).await
                } else {
                    reader.verify(&receipt).await.map(|report| {
                        assert_eq!((report.files, report.bytes, report.chunks), (2, 7, 2))
                    })
                }
            },
            receive,
        )
        .await;
        if slow {
            assert!(
                matches!(result, Err(Error::Deadline)),
                "global budget was reset: {scenario:?}, download={download}: {result:?}"
            );
        }
        assert_eq!(notices.len(), 2, "{scenario:?}, download={download}");
        let expected_notices = match scenario {
            Scenario::ManifestItems => vec![(manifest.clone(), 0), (manifest.clone(), 1)],
            Scenario::ChunkItems => vec![(chunk.clone(), 0), (chunk.clone(), 1)],
            Scenario::AcrossGets => vec![(manifest.clone(), 0), (chunk.clone(), 0)],
        };
        assert_eq!(
            notices
                .iter()
                .map(|n| (n.key.clone(), n.item))
                .collect::<Vec<_>>(),
            expected_notices
        );
        let observation = observations.lock().unwrap();
        assert_eq!(
            observation.yielded,
            expected_notices[..if slow { 1 } else { 2 }]
        );
        assert_eq!(
            observation.dropped.len(),
            if matches!(scenario, Scenario::AcrossGets) {
                2
            } else {
                1
            }
        );
        assert_eq!(observation.gets.first(), Some(&manifest));
        assert_eq!(
            observation.gets.len(),
            if slow && matches!(scenario, Scenario::ManifestItems) {
                1
            } else {
                3
            }
        );
        if slow {
            assert!(
                matches!(result, Err(Error::Deadline)),
                "{scenario:?}, download={download}: {result:?}"
            );
            assert_eq!(boundary_polls, 1);
            assert!(elapsed >= BUDGET && elapsed < Duration::from_secs(12));
            assert!(!destination.join("checkpoint.json").exists());
            assert!(!destination.join(".checkpoint-pending").exists());
            if download && !matches!(scenario, Scenario::ManifestItems) {
                assert_eq!(
                    std::fs::read(destination.join("raft.db/000001.sst")).unwrap(),
                    b"log"
                );
                // Unfinished state-file bytes have no durability promise.
                assert!(destination.join("state.db/000002.sst").is_file());
            } else {
                assert!(!destination.exists());
            }
        } else {
            result.unwrap();
            assert_eq!(boundary_polls, 0);
            assert_eq!(elapsed, Duration::from_secs(6) + Duration::from_millis(2));
            if download {
                for name in [
                    "checkpoint.json",
                    "raft.db/000001.sst",
                    "state.db/000002.sst",
                ] {
                    assert_eq!(
                        std::fs::read(source.join(name)).unwrap(),
                        std::fs::read(destination.join(name)).unwrap()
                    );
                }
                assert!(!destination.join(".checkpoint-pending").exists());
            } else {
                assert!(!destination.exists());
            }
        }
    }
}

#[tokio::test]
async fn verify_accumulates_manifest_body_time() {
    contract(Scenario::ManifestItems, false).await;
}
#[tokio::test]
async fn download_accumulates_manifest_body_time() {
    contract(Scenario::ManifestItems, true).await;
}
#[tokio::test]
async fn verify_accumulates_chunk_body_time() {
    contract(Scenario::ChunkItems, false).await;
}
#[tokio::test]
async fn download_accumulates_chunk_body_time() {
    contract(Scenario::ChunkItems, true).await;
}
#[tokio::test]
async fn verify_accumulates_time_across_gets() {
    contract(Scenario::AcrossGets, false).await;
}
#[tokio::test]
async fn download_accumulates_time_across_gets() {
    contract(Scenario::AcrossGets, true).await;
}
