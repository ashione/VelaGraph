//! A committed manifest can outlive a lost ACK and an untrustworthy readback.
//! These controlled InMemory faults do not establish provider guarantees. A
//! corrupted response payload leaves backing bytes intact: Integrity means this
//! verification failed, not proof of permanent server-side corruption. Neither
//! error rolls back publication or authorizes overwriting existing objects.
mod common;
use async_trait::async_trait;
use bytes::Bytes;
use checkpoint_object_store::{CheckpointStore, CreateCapability, Error, Limits};
use futures::{stream, stream::BoxStream, StreamExt, TryStreamExt};
use object_store::{path::Path, *};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

#[derive(Clone, Copy, Debug)]
enum Fault {
    Unavailable,
    CorruptResponse,
}
#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Put(Path),
    Get(Path),
}
#[derive(Debug)]
struct ReconciliationStore {
    inner: Arc<memory::InMemory>,
    fault: Fault,
    lose_ack: AtomicBool,
    bad_read: AtomicBool,
    calls: Mutex<Vec<Call>>,
    committed: Mutex<Option<BTreeMap<Path, Vec<u8>>>>,
}
impl std::fmt::Display for ReconciliationStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "manifest-reconciliation")
    }
}
fn unavailable() -> object_store::Error {
    object_store::Error::Generic {
        store: "injected",
        source: std::io::Error::other("synthetic response loss").into(),
    }
}
fn keys() -> [Path; 3] {
    [
        Path::from(format!("reconcile/blobs/{:x}", Sha256::digest(b"log"))),
        Path::from(format!("reconcile/blobs/{:x}", Sha256::digest(b"data"))),
        Path::from("reconcile/checkpoints/one/checkpoint.json"),
    ]
}
async fn inventory(inner: &memory::InMemory) -> BTreeMap<Path, Vec<u8>> {
    let objects: Vec<_> = inner.list(None).try_collect().await.unwrap();
    let mut result = BTreeMap::new();
    for object in objects {
        result.insert(
            object.location.clone(),
            inner
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec(),
        );
    }
    result
}
#[async_trait]
impl ObjectStore for ReconciliationStore {
    async fn put_opts(&self, p: &Path, data: PutPayload, options: PutOptions) -> Result<PutResult> {
        assert!(
            matches!(options.mode, PutMode::Create),
            "all attempts must be atomic creates"
        );
        self.calls.lock().unwrap().push(Call::Put(p.clone()));
        let result = self.inner.put_opts(p, data, options).await;
        if p == &keys()[2] && self.lose_ack.swap(false, Ordering::SeqCst) {
            assert!(result.is_ok(), "inject loss only after a real commit");
            // Direct test oracle, not library LIST or a substitute for GET.
            *self.committed.lock().unwrap() = Some(inventory(&self.inner).await);
            return Err(unavailable());
        }
        result
    }
    async fn get_opts(&self, p: &Path, options: GetOptions) -> Result<GetResult> {
        assert!(!options.head, "verification must consume GET bodies");
        self.calls.lock().unwrap().push(Call::Get(p.clone()));
        if p == &keys()[2] && self.bad_read.swap(false, Ordering::SeqCst) {
            assert!(
                !self.lose_ack.load(Ordering::SeqCst),
                "GET must follow committed lost ACK"
            );
            match self.fault {
                Fault::Unavailable => return Err(unavailable()),
                Fault::CorruptResponse => {
                    let mut result = self.inner.get_opts(p, options).await?;
                    let mut bytes = self.inner.get(p).await?.bytes().await?.to_vec();
                    assert!(!bytes.is_empty());
                    bytes[0] ^= 1;
                    assert_eq!(bytes.len() as u64, result.meta.size);
                    result.payload =
                        GetResultPayload::Stream(stream::iter([Ok(Bytes::from(bytes))]).boxed());
                    return Ok(result);
                }
            }
        }
        self.inner.get_opts(p, options).await
    }
    async fn put_multipart_opts(
        &self,
        _: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        panic!("no multipart");
    }
    fn delete_stream(
        &self,
        _: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        panic!("no delete/rollback");
    }
    fn list(&self, _: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        panic!("no library LIST");
    }
    async fn list_with_delimiter(&self, _: Option<&Path>) -> Result<ListResult> {
        panic!("no library LIST");
    }
    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        panic!("no copy");
    }
}
async fn scenario(fault: Fault) {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let inner = Arc::new(memory::InMemory::new());
    let wrapper = Arc::new(ReconciliationStore {
        inner: inner.clone(),
        fault,
        lose_ack: AtomicBool::new(true),
        bad_read: AtomicBool::new(true),
        calls: Mutex::new(Vec::new()),
        committed: Mutex::new(None),
    });
    let remote = CheckpointStore::new(
        wrapper.clone(),
        "reconcile".into(),
        Limits {
            chunk_bytes: 4,
            ..Limits::default()
        },
        CreateCapability::AtomicCreateVerified,
    )
    .unwrap();
    let result = remote.upload(&source, "one").await;
    match fault {
        Fault::Unavailable => assert!(
            matches!(result, Err(Error::OutcomeUnknown(_))),
            "{result:?}"
        ),
        Fault::CorruptResponse => assert!(matches!(result, Err(Error::Integrity(_))), "{result:?}"),
    }
    let round: Vec<_> = keys()
        .into_iter()
        .flat_map(|key| [Call::Put(key.clone()), Call::Get(key)])
        .collect();
    assert_eq!(*wrapper.calls.lock().unwrap(), round);
    assert!(!wrapper.lose_ack.load(Ordering::SeqCst));
    assert!(!wrapper.bad_read.load(Ordering::SeqCst));
    let committed = wrapper.committed.lock().unwrap().clone().unwrap();
    assert_eq!(committed.len(), 3);
    assert_eq!(committed.get(&keys()[0]).unwrap(), b"log");
    assert_eq!(committed.get(&keys()[1]).unwrap(), b"data");
    assert_eq!(
        inventory(&inner).await,
        committed,
        "fault changed only the response, not storage"
    );
    let receipt = remote.upload(&source, "one").await.unwrap();
    let mut calls = round.clone();
    calls.extend(round);
    assert_eq!(
        *wrapper.calls.lock().unwrap(),
        calls,
        "one exact retry verifies all existing objects"
    );
    assert_eq!(
        inventory(&inner).await,
        committed,
        "retry must not overwrite committed bytes"
    );
    assert_eq!(receipt.checkpoint_id, "one");
    assert_eq!(
        receipt.manifest_sha256,
        format!("{:x}", Sha256::digest(committed.get(&keys()[2]).unwrap()))
    );
    let report = remote.verify(&receipt).await.unwrap();
    assert_eq!((report.files, report.bytes, report.chunks), (2, 7, 2));
    let target = dir.path().join("download");
    remote.download(&receipt, &target).await.unwrap();
    for name in [
        "checkpoint.json",
        "raft.db/000001.sst",
        "state.db/000002.sst",
    ] {
        assert_eq!(
            std::fs::read(source.join(name)).unwrap(),
            std::fs::read(target.join(name)).unwrap()
        );
    }
    assert!(!target.join(".checkpoint-pending").exists());
    for _ in 0..2 {
        calls.extend(
            [keys()[2].clone(), keys()[0].clone(), keys()[1].clone()]
                .into_iter()
                .map(Call::Get),
        );
    }
    assert_eq!(*wrapper.calls.lock().unwrap(), calls);
    assert_eq!(inventory(&inner).await, committed);
}
#[tokio::test]
async fn committed_manifest_with_lost_ack_and_failed_readback_remains_unknown() {
    scenario(Fault::Unavailable).await;
}
#[tokio::test]
async fn committed_manifest_with_lost_ack_and_corrupt_readback_fails_integrity() {
    scenario(Fault::CorruptResponse).await;
}
