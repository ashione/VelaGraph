//! Transport for the explicitly selected local D2 decision-prefix profile.
//! The caller owns immutable source and backend retention throughout each use.
use super::{
    hash, CheckpointReader, CheckpointStore, Error, InventoryBudget, ObjectPath, Receipt, Result,
};
use d2_raft::{
    cohort_inputs::generation::SelectedManifest, participant_writes::Authority,
    transaction_decisions::Cohort,
};
use serde::{Deserialize, Serialize};
use sha2::{digest::Output, Sha256};
use std::path::Path;

const FORMAT: &str = "d2-prefix-transport-v1";
const MAX_ENVELOPE: usize = 256 * 1024;

/// Store outside the backup with the backend, prefix and intended cut before
/// the final PUT. Both digests are required to verify after an uncertain ACK.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrefixReceipt {
    pub local_sha256: [u8; 32],
    pub envelope_sha256: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    format: String,
    local_record: String,
    replicas: Vec<Receipt>,
}

/// Uploaded replicas awaiting final readback, semantic validation and publication.
pub struct StagedPrefix<'a> {
    store: &'a CheckpointStore,
    selection: SelectedManifest,
    replicas: Vec<Receipt>,
    envelope: Vec<u8>,
    receipt: PrefixReceipt,
}

fn invalid_prefix(error: impl std::fmt::Display) -> Error {
    Error::Invalid(format!("D2 prefix: {error:#}"))
}
fn digest_hex(digest: [u8; 32]) -> String {
    format!("{:x}", Output::<Sha256>::from(digest))
}
fn replica_id(local: &str, shard: u64) -> String {
    format!("d2-{local}-{shard}")
}

impl CheckpointStore {
    fn prefix_key(&self, digest: [u8; 32]) -> ObjectPath {
        ObjectPath::from(format!(
            "{}/d2-prefixes/{}/generation.json",
            self.prefix,
            digest_hex(digest)
        ))
    }

    /// Reopen a published local generation selected by its trusted producer
    /// digest and cut. Upload its physical replicas using create-only receipts.
    /// Retrying requires the same backend, prefix, input and chunk size.
    pub async fn stage_prefix(
        &self,
        root: &Path,
        digest: [u8; 32],
        authority: &Authority,
        cohort: &Cohort,
        version: u64,
    ) -> Result<StagedPrefix<'_>> {
        tokio::time::timeout(self.limits.timeout, async {
            let selection = SelectedManifest::read(root, digest, authority, cohort, version)
                .map_err(invalid_prefix)?;
            let local = digest_hex(digest);
            let mut budget = InventoryBudget::new(&self.limits);
            let mut replicas = Vec::new();
            for (shard, physical_digest) in selection.replicas() {
                let receipt = self
                    .upload_inner(
                        &root.join(shard.to_string()),
                        &replica_id(&local, shard),
                        Some(physical_digest),
                        &mut budget,
                    )
                    .await?;
                replicas.push(receipt);
            }
            let record = std::str::from_utf8(selection.as_bytes()).map_err(invalid_prefix)?;
            let envelope = serde_json::to_vec(&Envelope {
                format: FORMAT.into(),
                local_record: record.to_owned(),
                replicas: replicas.clone(),
            })?;
            if envelope.len() > MAX_ENVELOPE.min(self.limits.manifest_bytes) {
                return Err(Error::Limit);
            }
            let receipt = PrefixReceipt {
                local_sha256: digest,
                envelope_sha256: hash(&envelope),
            };
            Ok(StagedPrefix {
                store: self,
                selection,
                replicas,
                envelope,
                receipt,
            })
        })
        .await
        .map_err(|_| Error::Deadline)?
    }

    async fn verify_replicas(
        &self,
        selection: &SelectedManifest,
        replicas: &[Receipt],
        digest: [u8; 32],
        scratch_parent: &Path,
    ) -> Result<()> {
        let local = digest_hex(digest);
        if selection.replicas().count() != replicas.len()
            || selection
                .replicas()
                .zip(replicas)
                .any(|((shard, _), receipt)| receipt.checkpoint_id != replica_id(&local, shard))
        {
            return Err(Error::Invalid("generation replica set".into()));
        }
        let mut directory = tempfile::Builder::new();
        directory.prefix("d2-prefix-");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            directory.permissions(std::fs::Permissions::from_mode(0o700));
        }
        let scratch = directory.tempdir_in(scratch_parent)?;
        let mut budget = InventoryBudget::new(&self.limits);
        for ((shard, physical_digest), receipt) in selection.replicas().zip(replicas) {
            let remote = self.read_manifest(receipt, &mut budget).await?;
            if hash(remote.physical_manifest.as_bytes()) != digest_hex(physical_digest) {
                return Err(Error::Integrity(format!("group {shard} physical manifest")));
            }
            self.write_download(remote, &scratch.path().join(shard.to_string()))
                .await?;
        }
        selection
            .verify_components(scratch.path())
            .await
            .map_err(invalid_prefix)?;
        scratch.close()?;
        Ok(())
    }
}

impl StagedPrefix<'_> {
    pub fn receipt(&self) -> &PrefixReceipt {
        &self.receipt
    }

    /// Re-read the object closure into private scratch, verify the exact local
    /// capture and fixed V, then conditionally create the generation record.
    pub async fn publish(&self, scratch_parent: &Path) -> Result<()> {
        let start = tokio::time::Instant::now();
        tokio::time::timeout(self.store.limits.timeout, async {
            self.store
                .verify_replicas(
                    &self.selection,
                    &self.replicas,
                    self.receipt.local_sha256,
                    scratch_parent,
                )
                .await?;
            if start.elapsed() >= self.store.limits.timeout {
                return Err(Error::Deadline);
            }
            self.store
                .create_verified(
                    &self.store.prefix_key(self.receipt.local_sha256),
                    self.envelope.clone(),
                )
                .await
        })
        .await
        .map_err(|_| Error::Deadline)?
    }
}

impl CheckpointReader {
    /// Verify an explicit receipt on a read-only backend. Every required
    /// manifest and chunk is fetched and the staged D2 state is reopened.
    /// Scratch is private to this call and successful verification grants no pin.
    pub async fn verify_prefix(
        &self,
        receipt: &PrefixReceipt,
        authority: &Authority,
        cohort: &Cohort,
        version: u64,
        scratch_parent: &Path,
    ) -> Result<()> {
        let start = tokio::time::Instant::now();
        tokio::time::timeout(self.inner.limits.timeout, async {
            if !super::valid_hash(&receipt.envelope_sha256) {
                return Err(Error::Invalid("generation receipt digest".into()));
            }
            let bytes = self
                .inner
                .read_bounded(
                    &self.inner.prefix_key(receipt.local_sha256),
                    MAX_ENVELOPE.min(self.inner.limits.manifest_bytes),
                )
                .await?;
            if hash(&bytes) != receipt.envelope_sha256 {
                return Err(Error::Integrity("generation envelope".into()));
            }
            let envelope: Envelope = serde_json::from_slice(&bytes)?;
            if envelope.format != FORMAT {
                return Err(Error::Invalid("generation envelope format".into()));
            }
            let selection = SelectedManifest::decode(
                envelope.local_record.into_bytes(),
                receipt.local_sha256,
                authority,
                cohort,
                version,
            )
            .map_err(invalid_prefix)?;
            self.inner
                .verify_replicas(
                    &selection,
                    &envelope.replicas,
                    receipt.local_sha256,
                    scratch_parent,
                )
                .await?;
            if start.elapsed() >= self.inner.limits.timeout {
                return Err(Error::Deadline);
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::Deadline)?
    }
}
