//! Local physical backup, not a remote durability or cluster-cloning protocol.
use super::*;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct FileRecord {
    name: String,
    size: u64,
    sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: u32,
    group_id: u64,
    node_id: NodeId,
    applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, BasicNode>,
    snapshot: Option<SnapshotMeta<NodeId, BasicNode>>,
    log: Option<crate::durable_log_store::CheckpointLogBoundary>,
    files: Vec<FileRecord>,
}

fn inventory(root: &Path) -> anyhow::Result<Vec<FileRecord>> {
    let mut files = Vec::new();
    for directory in ["raft.db", "state.db"] {
        for entry in fs::read_dir(root.join(directory))? {
            let entry = entry?;
            anyhow::ensure!(entry.file_type()?.is_file(), "non-file in checkpoint");
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("non-UTF8 checkpoint filename"))?;
            let mut file = File::open(entry.path())?;
            let mut hash = Sha256::new();
            let mut size = 0;
            let mut buffer = [0_u8; 65536];
            loop {
                let n = file.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                size += n as u64;
                hash.update(&buffer[..n]);
            }
            files.push(FileRecord {
                name: format!("{directory}/{name}"),
                size,
                sha256: format!("{:x}", hash.finalize()),
            });
        }
    }
    files.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(files)
}

fn sync_parent(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(parent)?.sync_all()?;
    Ok(())
}

fn write_new(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

impl StateMachine {
    pub(crate) async fn physical_checkpoint(
        &self,
        logs: &crate::DurableLogStore,
        root: &Path,
        group_id: u64,
        node_id: NodeId,
    ) -> anyhow::Result<()> {
        let storage = self
            .storage
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("physical checkpoint requires durable state"))?;
        // Existing destinations, including interrupted attempts, are never overwritten.
        fs::create_dir(root)?;
        // Matches snapshot installation/builder ordering. All state, snapshot and
        // log writes are excluded until BOTH DB checkpoint handles have finished.
        let state = self.state.write().await;
        let snapshot = self.current_snapshot.read().await;
        let mut manifest = Manifest {
            format: 1,
            group_id,
            node_id,
            applied: state.last_applied_log,
            membership: state.last_membership.clone(),
            snapshot: snapshot.as_ref().map(|s| s.meta.clone()),
            log: None,
            files: Vec::new(),
        };
        logs.checkpoint_pair(
            manifest.applied,
            manifest.snapshot.as_ref().and_then(|s| s.last_log_id),
            &root.join("raft.db"),
            || {
                rocksdb::checkpoint::Checkpoint::new(&storage.db)?
                    .create_checkpoint(root.join("state.db"))?;
                Ok(())
            },
        )
        .await?;
        drop(snapshot);
        drop(state);
        // Slow hashing and fsync run after serving writes are resumed.
        blocking_section(|| {
            manifest.log = Some(crate::durable_log_store::validate_checkpoint_log(
                &root.join("raft.db"),
                manifest.applied,
                manifest.snapshot.as_ref().and_then(|s| s.last_log_id),
            )?);
            manifest.files = inventory(root)?;
            for record in &manifest.files {
                File::open(root.join(&record.name))?.sync_all()?;
            }
            for dir in ["raft.db", "state.db"] {
                File::open(root.join(dir))?.sync_all()?;
            }
            // Completion record is published LAST. A partial manifest is invalid.
            write_new(
                &root.join("checkpoint.json"),
                &serde_json::to_vec(&manifest)?,
            )?;
            File::open(root)?.sync_all()?;
            sync_parent(root)?;
            Ok(())
        })
    }
}

/// Restore a local backup into a newly created directory. Never overwrites an
/// existing path, joins a cluster, or changes identity. The original replica
/// cannot be safely replaced in a live cluster from an old vote state. Normal
/// startup rejects the restored offline-only marker. This is NOT an
/// acknowledged-write zero-loss guarantee: writes after the cut are absent.
/// Interrupted destinations are retained for inspection and rejected on retry.
pub fn restore_physical_checkpoint(
    backup: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    group_id: u64,
    node_id: NodeId,
) -> anyhow::Result<()> {
    let backup = backup.as_ref();
    let destination = destination.as_ref();
    let manifest: Manifest = serde_json::from_slice(&fs::read(backup.join("checkpoint.json"))?)?;
    anyhow::ensure!(manifest.format == 1, "unsupported checkpoint format");
    anyhow::ensure!(
        manifest.group_id == group_id && manifest.node_id == node_id,
        "checkpoint node/group identity mismatch"
    );
    // Derive names from actual files, not untrusted manifest paths.
    let files = inventory(backup)?;
    anyhow::ensure!(
        manifest.files == files,
        "checkpoint file inventory/checksum mismatch"
    );
    let log = crate::durable_log_store::validate_checkpoint_log(
        &backup.join("raft.db"),
        manifest.applied,
        manifest.snapshot.as_ref().and_then(|s| s.last_log_id),
    )?;
    anyhow::ensure!(
        manifest.log.as_ref() == Some(&log),
        "checkpoint log metadata mismatch"
    );
    fs::create_dir(destination)?;
    write_new(&destination.join("RESTORE_INCOMPLETE"), b"1")?;
    File::open(destination)?.sync_all()?;
    for directory in ["raft.db", "state.db"] {
        fs::create_dir(destination.join(directory))?;
    }
    for file in files {
        // Copy rather than hard link: the restored DB must not mutate the backup.
        let mut source = File::open(backup.join(&file.name))?;
        let mut target = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination.join(&file.name))?;
        let mut hash = Sha256::new();
        let mut size = 0_u64;
        let mut buffer = [0_u8; 65536];
        loop {
            let n = source.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            target.write_all(&buffer[..n])?;
            hash.update(&buffer[..n]);
            size += n as u64;
        }
        anyhow::ensure!(
            size == file.size && format!("{:x}", hash.finalize()) == file.sha256,
            "checkpoint changed while restoring"
        );
        target.sync_all()?;
    }
    // Opening performs the existing full state/ledger validation, not a weakened
    // replacement loader. Verify the applied cut and membership before startup.
    let state = StateMachine::open(destination.join("state.db"))?;
    let actual = state.state.try_read().expect("new state has no readers");
    anyhow::ensure!(
        actual.last_applied_log == manifest.applied
            && actual.last_membership == manifest.membership,
        "checkpoint state metadata mismatch"
    );
    let snapshot = state
        .current_snapshot
        .try_read()
        .expect("new snapshot has no readers");
    anyhow::ensure!(
        snapshot.as_ref().map(|s| &s.meta) == manifest.snapshot.as_ref(),
        "checkpoint snapshot metadata mismatch"
    );
    drop(snapshot);
    drop(actual);
    drop(state);
    let identity = serde_json::json!({"version":1,"group_id":group_id,"node_id":node_id});
    write_new(
        &destination.join("raft-group.json"),
        &serde_json::to_vec(&identity)?,
    )?;
    for dir in ["raft.db", "state.db"] {
        File::open(destination.join(dir))?.sync_all()?;
    }
    File::open(destination)?.sync_all()?;
    write_new(
        &destination.join("RESTORED_OFFLINE_ONLY"),
        b"Restored votes must not rejoin a live consensus group",
    )?;
    File::open(destination)?.sync_all()?;
    fs::remove_file(destination.join("RESTORE_INCOMPLETE"))?;
    File::open(destination)?.sync_all()?;
    sync_parent(destination)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::storage::RaftLogStorage;

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refuses_uncovered_purge_without_changing_purge_policy() -> anyhow::Result<()> {
        let dirs = tempfile::tempdir()?;
        let state = StateMachine::open(dirs.path().join("state"))?;
        let mut logs = crate::DurableLogStore::open(dirs.path().join("logs"))?;
        logs.purge(LogId::new(openraft::CommittedLeaderId::new(1, 1), 2))
            .await?;
        let backup = dirs.path().join("backup");
        assert!(state
            .physical_checkpoint(&logs, &backup, 0, 1)
            .await
            .is_err());
        assert!(!backup.join("checkpoint.json").exists());
        assert!(restore_physical_checkpoint(&backup, dirs.path().join("restored"), 0, 1).is_err());
        Ok(())
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_committed_watermark_is_valid() -> anyhow::Result<()> {
        let dirs = tempfile::tempdir()?;
        let state = StateMachine::open(dirs.path().join("state"))?;
        let mut logs = crate::DurableLogStore::open(dirs.path().join("logs"))?;
        logs.save_committed(None).await?;
        let backup = dirs.path().join("backup");
        state.physical_checkpoint(&logs, &backup, 0, 1).await?;
        restore_physical_checkpoint(&backup, dirs.path().join("restore"), 0, 1)?;
        Ok(())
    }
}
