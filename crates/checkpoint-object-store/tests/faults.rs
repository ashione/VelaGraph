mod common;
use async_trait::async_trait;
use checkpoint_object_store::{CheckpointStore, CreateCapability, Error, Limits};
use futures::stream::BoxStream;
use object_store::{path::Path, *};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
#[derive(Debug)]
struct FaultStore {
    inner: Arc<memory::InMemory>,
    calls: AtomicUsize,
    fail_at: usize,
    after_commit: bool,
}
impl std::fmt::Display for FaultStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "fault-store")
    }
}
fn lost() -> object_store::Error {
    object_store::Error::Generic {
        store: "test",
        source: std::io::Error::other("injected disconnect").into(),
    }
}
#[async_trait]
impl ObjectStore for FaultStore {
    async fn put_opts(&self, p: &Path, data: PutPayload, opts: PutOptions) -> Result<PutResult> {
        let fail = self.calls.fetch_add(1, Ordering::SeqCst) == self.fail_at;
        if fail && !self.after_commit {
            return Err(lost());
        }
        let result = self.inner.put_opts(p, data, opts).await;
        if fail {
            Err(lost())
        } else {
            result
        }
    }
    async fn put_multipart_opts(
        &self,
        p: &Path,
        o: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(p, o).await
    }
    async fn get_opts(&self, p: &Path, o: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(p, o).await
    }
    fn delete_stream(
        &self,
        p: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(p)
    }
    fn list(&self, p: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(p)
    }
    async fn list_with_delimiter(&self, p: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(p).await
    }
    async fn copy_opts(&self, a: &Path, b: &Path, o: CopyOptions) -> Result<()> {
        self.inner.copy_opts(a, b, o).await
    }
}
#[tokio::test]
async fn lost_ack_at_each_object_and_manifest_reconciles() {
    for fail_at in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        common::fixture(&src, b"data");
        let inner = Arc::new(memory::InMemory::new());
        let fault = Arc::new(FaultStore {
            inner,
            calls: AtomicUsize::new(0),
            fail_at,
            after_commit: true,
        });
        let remote = CheckpointStore::new(
            fault,
            "faults".into(),
            Limits::default(),
            CreateCapability::AtomicCreateVerified,
        )
        .unwrap();
        let receipt = remote.upload(&src, "one").await.unwrap();
        remote
            .download(&receipt, &dir.path().join("restored"))
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn failed_before_commit_stays_unknown_and_retry_finishes() {
    for fail_at in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("src");
        common::fixture(&src, b"data");
        let inner = Arc::new(memory::InMemory::new());
        let fault = Arc::new(FaultStore {
            inner: inner.clone(),
            calls: AtomicUsize::new(0),
            fail_at,
            after_commit: false,
        });
        let remote = CheckpointStore::new(
            fault,
            "faults".into(),
            Limits::default(),
            CreateCapability::AtomicCreateVerified,
        )
        .unwrap();
        assert!(matches!(
            remote.upload(&src, "one").await,
            Err(Error::OutcomeUnknown(_))
        ));
        assert!(inner
            .get(&Path::from("faults/checkpoints/one/checkpoint.json"))
            .await
            .is_err());
        let receipt = remote.upload(&src, "one").await.unwrap();
        remote
            .download(&receipt, &dir.path().join("restored"))
            .await
            .unwrap();
    }
}
