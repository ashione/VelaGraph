//! Synthetic body faults exercise the reader, not an S3 provider guarantee.
mod common;

use async_trait::async_trait;
use bytes::Bytes;
use checkpoint_object_store::{CheckpointStore, CreateCapability, Error, Limits};
use futures::{stream, stream::BoxStream, Stream, StreamExt};
use object_store::{path::Path, *};
use sha2::{Digest, Sha256};
use std::{
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

#[derive(Clone, Copy, Debug)]
enum Fault {
    Healthy,
    OversizedMetadata,
    OversizedBody,
    TruncatedBody,
    MidstreamError,
    PendingBody,
}

// Observe that the faulty body was actually consumed (or rejected before its
// first poll), and that cancellation/error drops it rather than leaving work.
struct ObservedBody {
    inner: BoxStream<'static, Result<Bytes>>,
    yielded: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl Stream for ObservedBody {
    type Item = Result<Bytes>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.inner.as_mut().poll_next(cx);
        if matches!(&result, Poll::Ready(Some(Ok(_)))) {
            self.yielded.fetch_add(1, Ordering::SeqCst);
        }
        result
    }
}
impl Drop for ObservedBody {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct ReadFaultStore {
    inner: Arc<memory::InMemory>,
    target: Path,
    fault: Fault,
    maximum: usize,
    gets: Mutex<Vec<Path>>,
    yielded: Arc<AtomicUsize>,
    dropped: Arc<AtomicUsize>,
}
impl std::fmt::Display for ReadFaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "read-fault-store")
    }
}
fn interrupted() -> object_store::Error {
    object_store::Error::Generic {
        store: "read-fault-test",
        source: std::io::Error::other("injected body disconnect").into(),
    }
}

#[async_trait]
impl ObjectStore for ReadFaultStore {
    async fn get_opts(&self, path: &Path, options: GetOptions) -> Result<GetResult> {
        assert!(!options.head, "reader must consume GET bodies, not HEAD");
        self.gets.lock().unwrap().push(path.clone());
        let mut result = self.inner.get_opts(path, options).await?;
        if path != &self.target {
            return Ok(result);
        }
        let bytes = self.inner.get(path).await?.bytes().await?;
        assert!(bytes.len() > 1);
        let body: BoxStream<'static, Result<Bytes>> = match self.fault {
            Fault::Healthy => stream::iter([Ok(bytes.slice(..1)), Ok(bytes.slice(1..))]).boxed(),
            Fault::OversizedMetadata => {
                result.meta.size = self.maximum as u64 + 1;
                stream::iter([Ok(bytes)]).boxed()
            }
            Fault::OversizedBody => {
                // Metadata still advertises the authentic size. Cross the cap
                // only on the second body item, not in the initial GET result.
                assert_eq!(bytes.len(), self.maximum);
                stream::iter([Ok(bytes), Ok(Bytes::from_static(b"!"))]).boxed()
            }
            Fault::TruncatedBody => stream::iter([Ok(bytes.slice(..bytes.len() - 1))]).boxed(),
            Fault::MidstreamError => {
                stream::iter([Ok(bytes.slice(..1)), Err(interrupted())]).boxed()
            }
            Fault::PendingBody => stream::iter([Ok(bytes.slice(..1))])
                .chain(stream::pending())
                .boxed(),
        };
        result.payload = GetResultPayload::Stream(
            ObservedBody {
                inner: body,
                yielded: self.yielded.clone(),
                dropped: self.dropped.clone(),
            }
            .boxed(),
        );
        Ok(result)
    }
    async fn put_opts(&self, _: &Path, _: PutPayload, _: PutOptions) -> Result<PutResult> {
        panic!("read operation attempted PUT")
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        panic!("read operation attempted multipart upload")
    }
    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        panic!("read operation attempted DELETE")
    }
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        panic!("read operation attempted LIST")
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        panic!("read operation attempted LIST")
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        panic!("read operation attempted COPY")
    }
}

async fn read_faults(fault: Fault) {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"abcdefgh");
    let inner = Arc::new(memory::InMemory::new());
    let writer = CheckpointStore::new(
        inner.clone(),
        "read-faults".into(),
        Limits {
            chunk_bytes: 4,
            ..Limits::default()
        },
        CreateCapability::AtomicCreateVerified,
    )
    .unwrap();
    let receipt = writer.upload(&source, "one").await.unwrap();
    let manifest_key = Path::from("read-faults/checkpoints/one/checkpoint.json");
    let manifest = inner
        .get(&manifest_key)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    // Fail the last chunk, after one entire file and an earlier state chunk.
    let last_chunk = Path::from(format!("read-faults/blobs/{:x}", Sha256::digest(b"efgh")));
    for target in [manifest_key.clone(), last_chunk] {
        for download in [false, true] {
            let injected = Arc::new(ReadFaultStore {
                inner: inner.clone(),
                maximum: if target == manifest_key {
                    manifest.len()
                } else {
                    4
                },
                target: target.clone(),
                fault,
                gets: Mutex::new(Vec::new()),
                yielded: Arc::new(AtomicUsize::new(0)),
                dropped: Arc::new(AtomicUsize::new(0)),
            });
            let reader = CheckpointStore::new(
                injected.clone(),
                "read-faults".into(),
                Limits {
                    chunk_bytes: 4,
                    manifest_bytes: manifest.len(),
                    timeout: if matches!(fault, Fault::PendingBody) {
                        Duration::from_secs(2)
                    } else {
                        Limits::default().timeout
                    },
                    ..Limits::default()
                },
                CreateCapability::AtomicCreateVerified,
            )
            .unwrap();
            let destination = dir
                .path()
                .join(format!("download-{}", target == manifest_key));
            // A separate watchdog prevents a missing production deadline from
            // hanging the suite. No elapsed-time/scheduling-sensitive assertion.
            let result = tokio::time::timeout(Duration::from_secs(10), async {
                if download {
                    reader.download(&receipt, &destination).await
                } else {
                    let entries_before = std::fs::read_dir(dir.path()).unwrap().count();
                    let result = reader.verify(&receipt).await;
                    assert_eq!(
                        std::fs::read_dir(dir.path()).unwrap().count(),
                        entries_before
                    );
                    result.map(|report| {
                        assert_eq!((report.files, report.bytes, report.chunks), (2, 11, 3));
                    })
                }
            })
            .await
            .expect("reader ignored the operation deadline");
            let expected = match fault {
                Fault::Healthy => result.is_ok(),
                Fault::OversizedMetadata | Fault::OversizedBody => {
                    matches!(result, Err(Error::Limit))
                }
                Fault::TruncatedBody => matches!(result, Err(Error::Integrity(_))),
                Fault::MidstreamError => matches!(result, Err(Error::Store(_))),
                Fault::PendingBody => matches!(result, Err(Error::Deadline)),
            };
            assert!(
                expected,
                "{fault:?}, target={target}, download={download}: {result:?}"
            );
            if matches!(fault, Fault::Healthy) && download {
                for name in [
                    "checkpoint.json",
                    "raft.db/000001.sst",
                    "state.db/000002.sst",
                ] {
                    assert_eq!(
                        std::fs::read(destination.join(name)).unwrap(),
                        std::fs::read(source.join(name)).unwrap()
                    );
                }
            } else {
                assert!(!destination.join("checkpoint.json").exists());
            }
            assert!(!destination.join(".checkpoint-pending").exists());
            if matches!(fault, Fault::Healthy) && download {
                // All output bytes were checked above.
            } else if download && target != manifest_key {
                assert_eq!(
                    std::fs::read(destination.join("raft.db/000001.sst")).unwrap(),
                    b"log"
                );
                assert!(destination.join("RESTORED_OFFLINE_ONLY").is_file());
                // The unfinished file has no durability guarantee: Tokio may
                // still have an earlier write queued when the read fails and
                // the file is dropped. Only completed/synced files above and
                // absence of the completion marker are contractual here.
                assert!(destination.join("state.db/000002.sst").is_file());
            } else {
                assert!(!destination.exists());
            }
            let gets = injected.gets.lock().unwrap();
            assert_eq!(gets.first(), Some(&manifest_key));
            if !matches!(fault, Fault::Healthy) {
                assert_eq!(gets.last(), Some(&target));
            }
            assert_eq!(
                gets.len(),
                if target == manifest_key && !matches!(fault, Fault::Healthy) {
                    1
                } else {
                    4
                }
            );
            assert_eq!(injected.dropped.load(Ordering::SeqCst), 1);
            assert_eq!(
                injected.yielded.load(Ordering::SeqCst),
                match fault {
                    Fault::OversizedMetadata => 0,
                    Fault::Healthy | Fault::OversizedBody => 2,
                    _ => 1,
                }
            );
        }
    }
    // The fixture remains usable through an unfaulted reader after all failures.
    writer.verify(&receipt).await.unwrap();
    writer
        .download(&receipt, &dir.path().join("healthy"))
        .await
        .unwrap();
}

#[tokio::test]
async fn oversized_get_metadata_is_rejected_before_body_poll() {
    read_faults(Fault::OversizedMetadata).await;
}
#[tokio::test]
async fn understated_get_metadata_cannot_bypass_stream_budget() {
    read_faults(Fault::OversizedBody).await;
}
#[tokio::test]
async fn truncated_get_stream_never_verifies_or_publishes() {
    read_faults(Fault::TruncatedBody).await;
}
#[tokio::test]
async fn midstream_get_error_never_verifies_or_publishes() {
    read_faults(Fault::MidstreamError).await;
}
#[tokio::test]
async fn pending_get_body_is_cancelled_at_operation_deadline() {
    read_faults(Fault::PendingBody).await;
}

#[tokio::test]
async fn healthy_split_get_streams_verify_and_publish() {
    read_faults(Fault::Healthy).await;
}
