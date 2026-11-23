//! Explicitly selected local generations of fixed-cohort D2 decision-prefix inputs.
use super::super::{blocking_section, create_offline_input, restore_replica, sync_parent};
use super::{capture_replicas, inspect, Authority, Cohort, NodeHandle, NodeId};
use anyhow::{ensure, Context};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};

const FILE: &str = "prefix-generation.json";
const FORMAT: &str = "d2-local-decision-prefix-v1";
const MAX_BYTES: usize = 64 * 1024;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    format: String,
    generation: [u8; 16],
    authority: Authority,
    cohort: Cohort,
    version: u64,
    coordinator_node: NodeId,
    member_nodes: Vec<NodeId>,
    // One checkpoint.json SHA-256 per group, coordinator first.
    checkpoint_sha256: Vec<[u8; 32]>,
}

/// A completed local capture, before its final directory entry is published.
/// Keep its directory and ancestors immutable and retained across both steps.
pub struct Prepared {
    root: PathBuf,
    bytes: Vec<u8>,
}

impl Prepared {
    /// Persist this digest independently with the intended directory and cut
    /// before publishing. Recovery must get it from that trusted selection.
    pub fn digest(&self) -> [u8; 32] {
        Sha256::digest(&self.bytes).into()
    }

    /// Publish the complete record create-only, then sync its directory.
    /// Repeating after an uncertain result accepts only the exact same bytes.
    pub async fn publish(&self) -> anyhow::Result<()> {
        let manifest: Manifest = serde_json::from_slice(&self.bytes)?;
        verify_components(&self.root, &manifest).await?;
        tokio::task::yield_now().await;
        blocking_section(|| publish_record(&self.root, &self.bytes))
    }
}

fn publish_record(root: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let mut staged = tempfile::NamedTempFile::new_in(root)?;
    staged.write_all(bytes)?;
    staged.as_file().sync_all()?;
    match staged.persist_noclobber(root.join(FILE)) {
        Ok(_) => {}
        Err(error) if error.error.kind() == ErrorKind::AlreadyExists => {
            ensure!(read_manifest(root)? == bytes, "conflicting generation");
        }
        Err(error) => return Err(error.error.into()),
    }
    File::open(root)?.sync_all()?;
    Ok(())
}

/// Capture the durable D2 Raft/state pair for each group from one in-process
/// registry. Publication reopens the files to verify the requested decision prefix.
/// This profile covers only these pairs. It cannot certify an application
/// catalog, external temporal DB or query readiness. The caller owns the
/// source selection and exclusive, immutable retention of the fresh root.
pub async fn prepare(
    root: &Path,
    authority: &Authority,
    cohort: &Cohort,
    version: u64,
    coordinator: &NodeHandle,
    members: &[&NodeHandle],
) -> anyhow::Result<Prepared> {
    super::check_request(authority, cohort, version, members.len())?;
    ensure!(
        members
            .iter()
            .all(|node| coordinator.shares_transport(node)),
        "replicas belong to different local registries"
    );
    let mut generation = [0; 16];
    getrandom::fill(&mut generation).map_err(|error| anyhow::anyhow!("generation ID: {error}"))?;
    let root = std::path::absolute(root)?;
    let checkpoint_sha256 =
        capture_replicas(&root, authority, cohort, version, coordinator, members).await?;
    let bytes = serde_json::to_vec(&Manifest {
        format: FORMAT.into(),
        generation,
        authority: authority.clone(),
        cohort: cohort.clone(),
        version,
        coordinator_node: coordinator.id,
        member_nodes: members.iter().map(|node| node.id).collect(),
        checkpoint_sha256,
    })?;
    ensure!(bytes.len() <= MAX_BYTES, "generation manifest too large");
    Ok(Prepared { root, bytes })
}

fn read_manifest(root: &Path) -> anyhow::Result<Vec<u8>> {
    let root: PathBuf = root.components().collect();
    ensure!(
        fs::symlink_metadata(&root)?.is_dir(),
        "invalid generation directory"
    );
    let path = root.join(FILE);
    ensure!(
        fs::symlink_metadata(&path)?.is_file(),
        "invalid generation manifest"
    );
    let mut bytes = Vec::new();
    File::open(path)?
        .take(MAX_BYTES as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= MAX_BYTES, "generation manifest too large");
    Ok(bytes)
}

/// Reopen the explicitly selected generation read-only. Authenticate the record
/// against the producer's independently retained digest, then revalidate every
/// required physical file, replica identity and terminal decision through V.
pub async fn verify(
    root: &Path,
    digest: [u8; 32],
    authority: &Authority,
    cohort: &Cohort,
    version: u64,
) -> anyhow::Result<()> {
    SelectedManifest::read(root, digest, authority, cohort, version)?
        .verify_components(root)
        .await
}

/// Restore one published D2 generation into a fresh, exclusively owned directory
/// outside the immutable source tree. Copies preserve the original replica IDs
/// and remain offline. On error or cancellation keep the attempt offline and
/// retry with a new destination. Success certifies the decision prefix through
/// `version`; the copied databases may also contain later decisions.
pub async fn restore_offline(
    source: &Path,
    destination: &Path,
    digest: [u8; 32],
    authority: &Authority,
    cohort: &Cohort,
    version: u64,
) -> anyhow::Result<()> {
    let selected = blocking_section(|| -> anyhow::Result<_> {
        let selected = SelectedManifest::read(source, digest, authority, cohort, version)?;
        let parent = destination
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        ensure!(
            !parent.canonicalize()?.starts_with(source.canonicalize()?),
            "restore destination overlaps the selected source"
        );
        create_offline_input(destination).context("create fresh offline cohort destination")?;
        sync_parent(destination)?;
        Ok(selected)
    })?;
    let nodes = std::iter::once(selected.manifest.coordinator_node)
        .chain(selected.manifest.member_nodes.iter().copied());
    for ((group, expected), node) in selected.replicas().zip(nodes) {
        tokio::task::yield_now().await;
        blocking_section(|| {
            let directory = group.to_string();
            restore_replica(
                &source.join(&directory),
                &destination.join(directory),
                group,
                node,
                Some(expected),
            )
        })
        .with_context(|| format!("restore group {group}"))?;
    }
    selected.verify_components(destination).await?;
    tokio::task::yield_now().await;
    blocking_section(|| publish_record(destination, selected.as_bytes()))
}

/// A local producer's record selected by an independently retained digest and
/// cut. Selecting metadata alone establishes no claim about available data.
/// Use `verify_components` on the actual captured or transferred directories.
pub struct SelectedManifest {
    bytes: Vec<u8>,
    manifest: Manifest,
}

impl SelectedManifest {
    /// Read the bounded final record from a trusted immutable root.
    pub fn read(
        root: &Path,
        digest: [u8; 32],
        authority: &Authority,
        cohort: &Cohort,
        version: u64,
    ) -> anyhow::Result<Self> {
        let bytes = read_manifest(root).context("read published prefix generation")?;
        Self::decode(bytes, digest, authority, cohort, version)
    }

    /// Select transported record bytes using the same independent selection.
    pub fn decode(
        bytes: Vec<u8>,
        digest: [u8; 32],
        authority: &Authority,
        cohort: &Cohort,
        version: u64,
    ) -> anyhow::Result<Self> {
        ensure!(bytes.len() <= MAX_BYTES, "generation manifest too large");
        ensure!(
            <[u8; 32]>::from(Sha256::digest(&bytes)) == digest,
            "generation digest mismatch"
        );
        let manifest: Manifest = serde_json::from_slice(&bytes)?;
        ensure!(
            manifest.format == FORMAT,
            "unsupported prefix generation format"
        );
        ensure!(
            manifest.authority == *authority
                && manifest.cohort == *cohort
                && manifest.version == version,
            "generation cut mismatch"
        );
        super::check_request(authority, cohort, version, manifest.member_nodes.len())?;
        ensure!(
            manifest.checkpoint_sha256.len() == cohort.members.len() + 1,
            "incomplete generation"
        );
        Ok(Self { bytes, manifest })
    }

    /// Exact producer bytes, including its generation identity.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Coordinator followed by each cohort member, with the producer's physical
    /// checkpoint.json SHA-256 for that replica.
    pub fn replicas(&self) -> impl Iterator<Item = (u64, [u8; 32])> + '_ {
        std::iter::once(self.manifest.authority.shard)
            .chain(
                self.manifest
                    .cohort
                    .members
                    .iter()
                    .map(|member| member.shard),
            )
            .zip(self.manifest.checkpoint_sha256.iter().copied())
    }

    /// Verify all local physical bytes, identities and the actual decision cut.
    pub async fn verify_components(&self, root: &Path) -> anyhow::Result<()> {
        verify_components(root, &self.manifest).await
    }
}

async fn verify_components(root: &Path, manifest: &Manifest) -> anyhow::Result<()> {
    ensure!(
        manifest.checkpoint_sha256.len() == manifest.cohort.members.len() + 1,
        "incomplete generation"
    );
    let actual = inspect(
        root,
        &manifest.authority,
        &manifest.cohort,
        manifest.version,
        manifest.coordinator_node,
        &manifest.member_nodes,
    )
    .await?;
    ensure!(
        actual == manifest.checkpoint_sha256,
        "physical checkpoint generation mismatch"
    );
    Ok(())
}
