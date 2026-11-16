mod common;
use checkpoint_object_store::{CheckpointStore, CreateCapability, Limits};
use object_store::{memory::InMemory, path::Path, ObjectStore, ObjectStoreExt};
use std::sync::Arc;
fn store(inner: Arc<dyn ObjectStore>) -> CheckpointStore {
    CheckpointStore::new(
        inner,
        "tests".into(),
        Limits {
            chunk_bytes: 4,
            ..Limits::default()
        },
        CreateCapability::AtomicCreateVerified,
    )
    .unwrap()
}
#[tokio::test]
async fn chunked_roundtrip_retry_and_existing_destination() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"many-chunks-of-state");
    let remote = store(Arc::new(InMemory::new()));
    let receipt = remote.upload(&source, "cut-1").await.unwrap();
    assert_eq!(
        receipt.manifest_sha256,
        remote
            .upload(&source, "cut-1")
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
    assert!(!destination.join(".checkpoint-pending").exists());
    assert!(remote.download(&receipt, &destination).await.is_err());
}
#[tokio::test]
async fn competing_publications_only_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    common::fixture(&a, b"one");
    common::fixture(&b, b"two");
    let remote = store(Arc::new(InMemory::new()));
    let (a, b) = tokio::join!(remote.upload(&a, "same"), remote.upload(&b, "same"));
    assert_ne!(a.is_ok(), b.is_ok());
    remote
        .download(&a.or(b).unwrap(), &dir.path().join("download"))
        .await
        .unwrap();
}
#[tokio::test]
async fn missing_and_corrupt_blob_fail_without_completion_manifest() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let inner = Arc::new(InMemory::new());
    let remote = store(inner.clone());
    let receipt = remote.upload(&source, "one").await.unwrap();
    let key = Path::from(format!("tests/blobs/{:x}", Sha256::digest(b"data")));
    inner.delete(&key).await.unwrap();
    let missing = dir.path().join("missing");
    assert!(remote.download(&receipt, &missing).await.is_err());
    assert!(!missing.join("checkpoint.json").exists());
    inner.put(&key, "bad".into()).await.unwrap();
    let corrupt = dir.path().join("corrupt");
    assert!(remote.download(&receipt, &corrupt).await.is_err());
    assert!(!corrupt.join("checkpoint.json").exists());
}
#[tokio::test]
async fn local_checksum_and_path_errors_never_publish() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let inner = Arc::new(InMemory::new());
    let remote = store(inner.clone());
    std::fs::write(source.join("state.db/000002.sst"), b"oops").unwrap();
    assert!(remote.upload(&source, "one").await.is_err());
    assert!(inner
        .get(&Path::from("tests/checkpoints/one/checkpoint.json"))
        .await
        .is_err());
    assert!(remote.upload(&source, "../escape").await.is_err());
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(source.join("checkpoint.json")).unwrap()).unwrap();
    manifest["files"][0]["name"] = "../escape".into();
    std::fs::write(
        source.join("checkpoint.json"),
        serde_json::to_vec(&manifest).unwrap(),
    )
    .unwrap();
    assert!(remote.upload(&source, "two").await.is_err());
}
#[tokio::test]
async fn invalid_receipt_and_small_budget_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let inner = Arc::new(InMemory::new());
    let remote = store(inner.clone());
    let mut receipt = remote.upload(&source, "one").await.unwrap();
    receipt.manifest_sha256 = "0".repeat(64);
    assert!(remote
        .download(&receipt, &dir.path().join("bad"))
        .await
        .is_err());
    assert!(!dir.path().join("bad").exists());
    let remote = CheckpointStore::new(
        inner,
        "small".into(),
        Limits {
            total_bytes: 1,
            ..Limits::default()
        },
        CreateCapability::AtomicCreateVerified,
    )
    .unwrap();
    assert!(remote.upload(&source, "one").await.is_err());
}
#[tokio::test]
async fn deadline_is_global_and_does_not_publish() {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let inner = Arc::new(InMemory::new());
    let throttled = ThrottledStore::new(
        inner.clone(),
        ThrottleConfig {
            wait_put_per_call: Duration::from_secs(1),
            ..ThrottleConfig::default()
        },
    );
    let remote = CheckpointStore::new(
        Arc::new(throttled),
        "deadline".into(),
        Limits {
            timeout: Duration::from_millis(10),
            ..Limits::default()
        },
        CreateCapability::AtomicCreateVerified,
    )
    .unwrap();
    assert!(matches!(
        remote.upload(&source, "one").await,
        Err(checkpoint_object_store::Error::Deadline)
    ));
    assert!(inner
        .get(&Path::from("deadline/checkpoints/one/checkpoint.json"))
        .await
        .is_err());
}
#[cfg(unix)]
#[tokio::test]
async fn symlink_file_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let file = source.join("state.db/000002.sst");
    std::fs::remove_file(&file).unwrap();
    std::os::unix::fs::symlink("../raft.db/000001.sst", &file).unwrap();
    assert!(store(Arc::new(InMemory::new()))
        .upload(&source, "one")
        .await
        .is_err());
}
#[tokio::test]
async fn matching_digest_malformed_envelope_is_rejected_before_directory_creation() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let inner = Arc::new(InMemory::new());
    let remote = store(inner.clone());
    let original_receipt = remote.upload(&source, "one").await.unwrap();
    let key = Path::from("tests/checkpoints/one/checkpoint.json");
    let original = inner.get(&key).await.unwrap().bytes().await.unwrap();
    for case in 0..5 {
        let mut envelope: serde_json::Value = serde_json::from_slice(&original).unwrap();
        match case {
            0 => envelope["format"] = 2.into(),
            1 => envelope["files"][0]["chunks"][0]["size"] = 99.into(),
            2 => envelope["files"][0]["chunks"][0]["size"] = 1.into(),
            3 | 4 => {
                let mut physical: serde_json::Value =
                    serde_json::from_str(envelope["physical_manifest"].as_str().unwrap()).unwrap();
                let name = if case == 3 {
                    "../escape"
                } else {
                    "state.db/000002.sst"
                };
                physical["files"][0]["name"] = name.into();
                envelope["physical_manifest"] = serde_json::to_string(&physical).unwrap().into();
                envelope["files"][0]["name"] = name.into();
            }
            _ => unreachable!(),
        }
        let bytes = serde_json::to_vec(&envelope).unwrap();
        let mut receipt = original_receipt.clone();
        receipt.manifest_sha256 = format!("{:x}", Sha256::digest(&bytes));
        inner.put(&key, bytes.into()).await.unwrap();
        let target = dir.path().join(format!("invalid-{case}"));
        assert!(
            remote.download(&receipt, &target).await.is_err(),
            "case {case}"
        );
        assert!(!target.exists(), "case {case}");
        assert!(remote.verify(&receipt).await.is_err(), "case {case}");
    }
}

#[tokio::test]
async fn verification_reads_complete_bytes_without_local_destination() {
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"abcdabcd");
    let remote = store(Arc::new(InMemory::new()));
    let receipt = remote.upload(&source, "verify").await.unwrap();
    std::fs::remove_dir_all(&source).unwrap();
    let report = remote.verify(&receipt).await.unwrap();
    assert_eq!(report.files, 2);
    assert_eq!(report.bytes, 11);
    assert_eq!(report.chunks, 3);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    assert_eq!(report, remote.verify(&receipt).await.unwrap());
}

#[tokio::test]
async fn verification_rechecks_objects_and_rejects_bad_receipt() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let inner = Arc::new(InMemory::new());
    let remote = store(inner.clone());
    let receipt = remote.upload(&source, "verify").await.unwrap();
    remote.verify(&receipt).await.unwrap();
    let key = Path::from(format!("tests/blobs/{:x}", Sha256::digest(b"data")));
    inner.put(&key, "oops".into()).await.unwrap();
    assert!(matches!(
        remote.verify(&receipt).await,
        Err(checkpoint_object_store::Error::Integrity(_))
    ));
    inner.delete(&key).await.unwrap();
    assert!(remote.verify(&receipt).await.is_err());
    let mut bad = receipt.clone();
    bad.checkpoint_id = "../escape".into();
    assert!(matches!(
        remote.verify(&bad).await,
        Err(checkpoint_object_store::Error::Invalid(_))
    ));
    bad = receipt;
    bad.manifest_sha256 = "0".repeat(64);
    assert!(matches!(
        remote.verify(&bad).await,
        Err(checkpoint_object_store::Error::Integrity(_))
    ));
}

#[tokio::test]
async fn verification_validates_file_digest_not_just_chunk_hashes_and_limits() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"");
    let inner = Arc::new(InMemory::new());
    let remote = store(inner.clone());
    let receipt = remote.upload(&source, "empty").await.unwrap();
    let report = remote.verify(&receipt).await.unwrap();
    assert_eq!((report.files, report.bytes, report.chunks), (2, 3, 1));
    let limited = CheckpointStore::new(
        inner.clone(),
        "tests".into(),
        Limits {
            total_bytes: 2,
            ..Limits::default()
        },
        CreateCapability::AtomicCreateVerified,
    )
    .unwrap();
    assert!(matches!(
        limited.verify(&receipt).await,
        Err(checkpoint_object_store::Error::Limit)
    ));
    let key = Path::from("tests/checkpoints/empty/checkpoint.json");
    let bytes = inner.get(&key).await.unwrap().bytes().await.unwrap();
    let mut manifest: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let mut physical: serde_json::Value =
        serde_json::from_str(manifest["physical_manifest"].as_str().unwrap()).unwrap();
    // Coherent metadata with authentic chunks still must fail file reassembly hash.
    physical["files"][0]["sha256"] = "0".repeat(64).into();
    manifest["files"][0]["sha256"] = "0".repeat(64).into();
    manifest["physical_manifest"] = serde_json::to_string(&physical).unwrap().into();
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let bad = checkpoint_object_store::Receipt {
        checkpoint_id: "empty".into(),
        manifest_sha256: format!("{:x}", Sha256::digest(&bytes)),
    };
    inner.put(&key, bytes.into()).await.unwrap();
    assert!(matches!(
        remote.verify(&bad).await,
        Err(checkpoint_object_store::Error::Integrity(_))
    ));
}

#[tokio::test]
async fn verification_enforces_reader_limits_and_get_deadline() {
    use object_store::throttle::{ThrottleConfig, ThrottledStore};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let inner = Arc::new(InMemory::new());
    let receipt = store(inner.clone())
        .upload(&source, "bounded")
        .await
        .unwrap();
    for limits in [
        Limits {
            manifest_bytes: 1,
            ..Limits::default()
        },
        Limits {
            files: 1,
            ..Limits::default()
        },
        Limits {
            total_bytes: 1,
            ..Limits::default()
        },
        Limits {
            chunk_bytes: 1,
            ..Limits::default()
        },
    ] {
        let reader = CheckpointStore::new(
            inner.clone(),
            "tests".into(),
            limits,
            CreateCapability::AtomicCreateVerified,
        )
        .unwrap();
        assert!(reader.verify(&receipt).await.is_err());
    }
    let slow = ThrottledStore::new(
        inner,
        ThrottleConfig {
            wait_get_per_call: Duration::from_secs(1),
            ..ThrottleConfig::default()
        },
    );
    let reader = CheckpointStore::new(
        Arc::new(slow),
        "tests".into(),
        Limits {
            timeout: Duration::from_millis(10),
            ..Limits::default()
        },
        CreateCapability::AtomicCreateVerified,
    )
    .unwrap();
    assert!(matches!(
        reader.verify(&receipt).await,
        Err(checkpoint_object_store::Error::Deadline)
    ));
}
