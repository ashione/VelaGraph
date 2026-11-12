//! Real SDK HTTP tests; no cloud credentials or remote endpoints accepted.
mod common;
use checkpoint_object_store::{CheckpointStore, CreateCapability, Limits};
use object_store::{
    aws::{AmazonS3Builder, S3ConditionalPut},
    path::Path,
    ObjectStore, ObjectStoreExt, PutMode,
};
use std::{sync::Arc, time::Duration};
#[tokio::test]
#[ignore = "requires disposable SeaweedFS endpoint and bucket"]
async fn actual_s3_checkpoint_transport() {
    let endpoint = std::env::var("VELAGRAPH_TEST_S3_ENDPOINT").expect("explicit test endpoint");
    let port: u16 = endpoint
        .strip_prefix("http://127.0.0.1:")
        .expect("loopback HTTP only")
        .parse()
        .expect("numeric port only");
    assert_ne!(port, 0);
    let bucket = std::env::var("VELAGRAPH_TEST_S3_BUCKET").expect("explicit test bucket");
    assert_eq!(bucket, "velagraph-ci");
    let inner = Arc::new(
        AmazonS3Builder::new()
            .with_access_key_id("velagraph-ci-only")
            .with_secret_access_key("velagraph-ci-only-secret")
            .with_region("us-east-1")
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_allow_http(true)
            .with_virtual_hosted_style_request(false)
            .with_conditional_put(S3ConditionalPut::ETagMatch)
            .build()
            .unwrap(),
    );
    let key = Path::from("contract/create-race");
    let (a, b) = tokio::join!(
        inner.put_opts(&key, "a".into(), PutMode::Create.into()),
        inner.put_opts(&key, "b".into(), PutMode::Create.into())
    );
    assert_ne!(a.is_ok(), b.is_ok());
    assert!(matches!(
        a.err().or(b.err()).unwrap(),
        object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }
    ));
    let remote = CheckpointStore::new(
        inner.clone(),
        "contract/checkpoint".into(),
        Limits {
            chunk_bytes: 1024,
            timeout: Duration::from_secs(60),
            ..Limits::default()
        },
        CreateCapability::AtomicCreateVerified,
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, &vec![b'x'; 16 * 1024 + 7]);
    let receipt = remote.upload(&source, "first").await.unwrap();
    assert_eq!(
        receipt.manifest_sha256,
        remote
            .upload(&source, "first")
            .await
            .unwrap()
            .manifest_sha256
    );
    let destination = dir.path().join("download");
    remote.download(&receipt, &destination).await.unwrap();
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
    use sha2::{Digest, Sha256};
    let blob = Path::from(format!(
        "contract/checkpoint/blobs/{:x}",
        Sha256::digest(vec![b'x'; 1024])
    ));
    inner.delete(&blob).await.unwrap();
    let missing = dir.path().join("missing");
    assert!(remote.download(&receipt, &missing).await.is_err());
    assert!(!missing.join("checkpoint.json").exists());
    inner.put(&blob, "corrupt".into()).await.unwrap();
    let corrupt = dir.path().join("corrupt");
    assert!(remote.download(&receipt, &corrupt).await.is_err());
    assert!(!corrupt.join("checkpoint.json").exists());
    println!("real S3 race, chunk transfer, retry and corruption rejection passed; synthetic bytes, not RocksDB restore");
}
