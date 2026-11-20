//! Coordinate conditional checkpoint publication without holding a graph lock.
use super::*;
use std::path::{Path, PathBuf};

const LOCK_NAME: &str = ".velagraph-checkpoint.lock";

pub(super) fn resolve(path: &Path) -> Result<PathBuf> {
    let name = path.file_name().ok_or_else(|| bad("checkpoint filename"))?;
    if [LOCK_NAME, TEMP_PREFIX].iter().any(|reserved| {
        name.as_encoded_bytes()
            .get(..reserved.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(reserved.as_bytes()))
    }) {
        return Err(bad("reserved checkpoint lock filename"));
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Ok(fs::canonicalize(parent).map_err(io)?.join(name))
}

pub(super) fn lock(parent: &Path) -> Result<fs::File> {
    let path = parent.join(LOCK_NAME);
    match fs::symlink_metadata(&path) {
        Ok(m) if !m.file_type().is_file() => {
            return Err(bad("checkpoint lock must be a regular file"))
        }
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(io(e)),
        _ => {}
    }
    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    // Independently open on every call: cloned handles can share lock ownership.
    // Keep this empty sidecar on disk so waiting writers cannot split lock inodes.
    let file = options.open(&path).map_err(io)?;
    if !file.metadata().map_err(io)?.file_type().is_file() {
        return Err(bad("checkpoint lock must be a regular file"));
    }
    file.lock().map_err(io)?;
    if file.metadata().map_err(io)?.len() != 0 {
        return Err(bad("checkpoint lock namespace occupied"));
    }
    Ok(file)
}

pub(super) fn check_previous(path: &Path, token: &IncrementalToken, payload: &[u8]) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(io(e)),
        Ok(_) => {}
    }
    let bytes = read_checkpoint(path)?;
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Envelope {
        identity: String,
        sequence: u64,
        #[serde(rename = "metadata")]
        _metadata: serde::de::IgnoredAny,
        #[serde(rename = "vertices")]
        _vertices: serde::de::IgnoredAny,
        #[serde(rename = "edges")]
        _edges: serde::de::IgnoredAny,
    }
    let previous: Envelope = serde_json::from_slice(&bytes[HEADER_BYTES..])?;
    if previous.identity != token.identity
        || previous.sequence > token.sequence
        || (previous.sequence == token.sequence && &bytes[HEADER_BYTES..] != payload)
    {
        return Err(Error::TransactionConflict);
    }
    // An equal version is rewritten and synced too: retry must finish durability
    // after a prior rename succeeded but its directory sync returned an error.
    Ok(())
}
