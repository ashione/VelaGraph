//! Immutable checkpoint transport. No live-DB access, remote write ACK or rejoin.
//! Large files are streamed as bounded content-addressed objects. Publication is
//! one conditional manifest PUT; neither LIST nor HEAD+PUT is used as a lock.
use bytes::Bytes;
use futures::TryStreamExt;
use object_store::{path::Path as ObjectPath, ObjectStore, ObjectStoreExt, PutMode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid checkpoint: {0}")]
    Invalid(String),
    #[error("checkpoint resource limit exceeded")]
    Limit,
    #[error("object content differs: {0}")]
    Integrity(String),
    #[error("write outcome unknown: {0}")]
    OutcomeUnknown(String),
    #[error("operation deadline exceeded; writes may have completed")]
    Deadline,
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Store(#[from] object_store::Error),
}
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct Limits {
    pub chunk_bytes: usize,
    pub manifest_bytes: usize,
    pub files: usize,
    pub total_bytes: u64,
    pub timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            chunk_bytes: 8 * 1024 * 1024,
            manifest_bytes: 16 * 1024 * 1024,
            files: 100_000,
            total_bytes: 1024 * 1024 * 1024 * 1024,
            timeout: Duration::from_secs(300),
        }
    }
}
/// Caller must bind this capability to its actual provider/bucket configuration.
/// OSS version-enabled/suspended forbid-overwrite is NOT this capability.
#[derive(Clone, Copy, Debug)]
pub enum CreateCapability {
    AtomicCreateVerified,
}
#[derive(Clone)]
pub struct CheckpointStore {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    limits: Limits,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Chunk {
    sha256: String,
    size: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteFile {
    name: String,
    size: u64,
    sha256: String,
    chunks: Vec<Chunk>,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteManifest {
    format: u32,
    physical_manifest: String,
    files: Vec<RemoteFile>,
}
#[derive(Debug, Deserialize)]
struct PhysicalManifest {
    format: u32,
    files: Vec<PhysicalFile>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PhysicalFile {
    name: String,
    size: u64,
    sha256: String,
}
/// Retain this outside the disposable database. No mutable latest lookup.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub checkpoint_id: String,
    pub manifest_sha256: String,
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn safe_path(value: &str) -> Result<()> {
    if value.is_empty()
        || value.contains('\\')
        || value
            .split('/')
            .any(|s| s.is_empty() || s == "." || s == "..")
    {
        return Err(Error::Invalid("unsafe object path".into()));
    }
    if !value
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"-_./".contains(&b))
    {
        return Err(Error::Invalid("unsupported path character".into()));
    }
    Ok(())
}
fn safe_id(id: &str) -> Result<()> {
    safe_path(id)?;
    if id.contains('/') || id.len() > 128 {
        return Err(Error::Invalid("invalid checkpoint id".into()));
    }
    Ok(())
}
fn valid_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
fn validate_physical(bytes: &[u8], limits: &Limits) -> Result<PhysicalManifest> {
    if bytes.len() > limits.manifest_bytes {
        return Err(Error::Limit);
    }
    let manifest: PhysicalManifest = serde_json::from_slice(bytes)?;
    if manifest.format != 1 || manifest.files.is_empty() {
        return Err(Error::Invalid(
            "unsupported or empty physical manifest".into(),
        ));
    }
    if manifest.files.len() > limits.files {
        return Err(Error::Limit);
    }
    let mut names = BTreeSet::new();
    let mut total = 0u64;
    for f in &manifest.files {
        safe_path(&f.name)?;
        let parts: Vec<_> = f.name.split('/').collect();
        if parts.len() != 2
            || !["raft.db", "state.db"].contains(&parts[0])
            || !valid_hash(&f.sha256)
            || !names.insert(&f.name)
        {
            return Err(Error::Invalid("invalid or duplicate physical file".into()));
        }
        total = total.checked_add(f.size).ok_or(Error::Limit)?;
        if total > limits.total_bytes {
            return Err(Error::Limit);
        }
    }
    Ok(manifest)
}
impl CheckpointStore {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: String,
        limits: Limits,
        _: CreateCapability,
    ) -> Result<Self> {
        safe_path(&prefix)?;
        if limits.chunk_bytes == 0
            || limits.chunk_bytes > 16 * 1024 * 1024
            || limits.manifest_bytes == 0
            || limits.files == 0
            || limits.timeout.is_zero()
        {
            return Err(Error::Limit);
        }
        Ok(Self {
            store,
            prefix,
            limits,
        })
    }
    fn manifest_key(&self, id: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/checkpoints/{id}/checkpoint.json", self.prefix))
    }
    fn chunk_key(&self, digest: &str) -> ObjectPath {
        ObjectPath::from(format!("{}/blobs/{digest}", self.prefix))
    }
    async fn read_bounded(&self, key: &ObjectPath, max: usize) -> Result<Vec<u8>> {
        let result = self.store.get(key).await?;
        if result.meta.size > max as u64 {
            return Err(Error::Limit);
        }
        let mut stream = result.into_stream();
        let mut data = Vec::new();
        while let Some(chunk) = stream.try_next().await? {
            if data.len().checked_add(chunk.len()).ok_or(Error::Limit)? > max {
                return Err(Error::Limit);
            }
            data.extend_from_slice(&chunk);
        }
        Ok(data)
    }
    async fn create_verified(&self, key: &ObjectPath, data: Vec<u8>) -> Result<()> {
        let size = data.len();
        let expected = hash(&data);
        // No application retry loop. The whole operation has one deadline.
        let write = self
            .store
            .put_opts(key, Bytes::from(data).into(), PutMode::Create.into())
            .await;
        match write {
            Ok(_)
            | Err(object_store::Error::AlreadyExists { .. })
            | Err(object_store::Error::Precondition { .. }) => {}
            Err(error) => {
                return match self.read_bounded(key, size).await {
                    Ok(actual) if actual.len() == size && hash(&actual) == expected => Ok(()),
                    Ok(_) => Err(Error::Integrity(key.to_string())),
                    Err(_) => Err(Error::OutcomeUnknown(error.to_string())),
                };
            }
        }
        let actual = self.read_bounded(key, size).await?;
        if actual.len() != size || hash(&actual) != expected {
            return Err(Error::Integrity(key.to_string()));
        }
        Ok(())
    }
    pub async fn upload(&self, root: &Path, checkpoint_id: &str) -> Result<Receipt> {
        safe_id(checkpoint_id)?;
        tokio::time::timeout(self.limits.timeout, self.upload_inner(root, checkpoint_id))
            .await
            .map_err(|_| Error::Deadline)?
    }
    async fn upload_inner(&self, root: &Path, id: &str) -> Result<Receipt> {
        // Caller owns an immutable completed directory throughout the transfer.
        require_directory(root).await?;
        let manifest_path = root.join("checkpoint.json");
        require_file(&manifest_path).await?;
        if tokio::fs::metadata(&manifest_path).await?.len() > self.limits.manifest_bytes as u64 {
            return Err(Error::Limit);
        }
        let original = tokio::fs::read(&manifest_path).await?;
        let physical = validate_physical(&original, &self.limits)?;
        for directory in ["raft.db", "state.db"] {
            require_directory(&root.join(directory)).await?;
        }
        let mut files = Vec::new();
        let mut metadata_budget = original.len();
        for expected in physical.files {
            let path = root.join(&expected.name);
            require_file(&path).await?;
            if tokio::fs::metadata(&path).await?.len() != expected.size {
                return Err(Error::Integrity(expected.name));
            }
            let mut file = tokio::fs::File::open(&path).await?;
            let mut hasher = Sha256::new();
            let mut chunks = Vec::new();
            let mut total = 0u64;
            loop {
                let mut data = vec![0; self.limits.chunk_bytes];
                let mut filled = 0;
                while filled < data.len() {
                    let n = file.read(&mut data[filled..]).await?;
                    if n == 0 {
                        break;
                    }
                    filled += n;
                }
                if filled == 0 {
                    break;
                }
                data.truncate(filled);
                total = total.checked_add(filled as u64).ok_or(Error::Limit)?;
                if total > expected.size {
                    return Err(Error::Integrity(expected.name));
                }
                hasher.update(&data);
                let digest = hash(&data);
                self.create_verified(&self.chunk_key(&digest), data).await?;
                metadata_budget = metadata_budget.checked_add(128).ok_or(Error::Limit)?;
                if metadata_budget > self.limits.manifest_bytes {
                    return Err(Error::Limit);
                }
                chunks.push(Chunk {
                    sha256: digest,
                    size: filled as u64,
                });
                if chunks.len() > self.limits.manifest_bytes / 64 {
                    return Err(Error::Limit);
                }
            }
            if total != expected.size || format!("{:x}", hasher.finalize()) != expected.sha256 {
                return Err(Error::Integrity(expected.name));
            }
            metadata_budget = metadata_budget
                .checked_add(expected.name.len() + 160)
                .ok_or(Error::Limit)?;
            if metadata_budget > self.limits.manifest_bytes {
                return Err(Error::Limit);
            }
            files.push(RemoteFile {
                name: expected.name,
                size: total,
                sha256: expected.sha256,
                chunks,
            });
        }
        let manifest = RemoteManifest {
            format: 1,
            physical_manifest: String::from_utf8(original)
                .map_err(|_| Error::Invalid("manifest UTF8".into()))?,
            files,
        };
        let bytes = serde_json::to_vec(&manifest)?;
        if bytes.len() > self.limits.manifest_bytes {
            return Err(Error::Limit);
        }
        let receipt = Receipt {
            checkpoint_id: id.into(),
            manifest_sha256: hash(&bytes),
        };
        self.create_verified(&self.manifest_key(id), bytes).await?;
        Ok(receipt)
    }
    /// Download backup bytes, not a runnable node. Existing directories rejected.
    /// Before publication incomplete attempts have no checkpoint.json. Errors
    /// after publication may leave a complete backup, requiring validation.
    pub async fn download(&self, receipt: &Receipt, destination: &Path) -> Result<()> {
        safe_id(&receipt.checkpoint_id)?;
        if !valid_hash(&receipt.manifest_sha256) {
            return Err(Error::Invalid("receipt digest".into()));
        }
        tokio::time::timeout(
            self.limits.timeout,
            self.download_inner(receipt, destination),
        )
        .await
        .map_err(|_| Error::Deadline)?
    }
    async fn download_inner(&self, receipt: &Receipt, destination: &Path) -> Result<()> {
        let bytes = self
            .read_bounded(
                &self.manifest_key(&receipt.checkpoint_id),
                self.limits.manifest_bytes,
            )
            .await?;
        if hash(&bytes) != receipt.manifest_sha256 {
            return Err(Error::Integrity("remote manifest".into()));
        }
        let remote: RemoteManifest = serde_json::from_slice(&bytes)?;
        if remote.format != 1 {
            return Err(Error::Invalid("remote manifest format".into()));
        }
        let physical = validate_physical(remote.physical_manifest.as_bytes(), &self.limits)?;
        if remote.files.len() != physical.files.len() {
            return Err(Error::Invalid("remote inventory".into()));
        }
        for (actual, expected) in remote.files.iter().zip(&physical.files) {
            if actual.name != expected.name
                || actual.size != expected.size
                || actual.sha256 != expected.sha256
            {
                return Err(Error::Invalid("remote file metadata".into()));
            }
            let mut size = 0u64;
            for chunk in &actual.chunks {
                if !valid_hash(&chunk.sha256)
                    || chunk.size == 0
                    || chunk.size > self.limits.chunk_bytes as u64
                {
                    return Err(Error::Invalid("chunk metadata".into()));
                }
                size = size.checked_add(chunk.size).ok_or(Error::Limit)?;
            }
            if size != expected.size {
                return Err(Error::Invalid("chunk length".into()));
            }
        }
        tokio::fs::create_dir(destination).await?;
        for dir in ["raft.db", "state.db"] {
            tokio::fs::create_dir(destination.join(dir)).await?;
        }
        for f in remote.files {
            let mut output = tokio::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(destination.join(&f.name))
                .await?;
            let mut hasher = Sha256::new();
            for chunk in f.chunks {
                let data = self
                    .read_bounded(&self.chunk_key(&chunk.sha256), chunk.size as usize)
                    .await?;
                if data.len() as u64 != chunk.size || hash(&data) != chunk.sha256 {
                    return Err(Error::Integrity(f.name));
                }
                hasher.update(&data);
                output.write_all(&data).await?;
            }
            if format!("{:x}", hasher.finalize()) != f.sha256 {
                return Err(Error::Integrity(f.name));
            }
            output.sync_all().await?;
        }
        for dir in ["raft.db", "state.db"] {
            tokio::fs::File::open(destination.join(dir))
                .await?
                .sync_all()
                .await?;
        }
        let mut output = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(destination.join(".checkpoint-pending"))
            .await?;
        output
            .write_all(remote.physical_manifest.as_bytes())
            .await?;
        output.sync_all().await?;
        drop(output);
        tokio::fs::rename(
            destination.join(".checkpoint-pending"),
            destination.join("checkpoint.json"),
        )
        .await?;
        tokio::fs::File::open(destination).await?.sync_all().await?;
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        tokio::fs::File::open(parent).await?.sync_all().await?;
        Ok(())
    }
}
async fn require_file(path: &Path) -> Result<()> {
    if !tokio::fs::symlink_metadata(path)
        .await?
        .file_type()
        .is_file()
    {
        return Err(Error::Invalid("not a regular file".into()));
    }
    Ok(())
}
async fn require_directory(path: &Path) -> Result<()> {
    if !tokio::fs::symlink_metadata(path)
        .await?
        .file_type()
        .is_dir()
    {
        return Err(Error::Invalid("not a directory".into()));
    }
    Ok(())
}
