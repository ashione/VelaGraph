//! Test-only immutable evidence. Production storage and wire formats are unchanged.
use super::{
    authority, byte_binding, canonical_write, extended_write, Binding, MemberCapacityGrant,
    Metrics, RequestRecord, EXTENDED_WRITES,
};
use anyhow::{anyhow, bail, ensure, Context};
use d2_raft::{
    capacity_policy::MAX_BYTE_PROFILE_TERMINAL_BYTES,
    terminal_prefix::TerminalPrefix,
    transaction_decisions::{MAX_REQUEST_BYTES, MAX_REQUEST_PAYLOAD_BYTES},
};
use rustix::fs::{self as rfs, Dir, FlockOperation, Mode, OFlags, RenameFlags, CWD};
use serde::{de, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::{self, File},
    io::{Read, Write},
    ops::{Deref, DerefMut},
    os::unix::fs::MetadataExt,
    path::{Component, Path, PathBuf},
};

const ORIGINALS: [u64; 4] = [1, 256, 1024, 1025];
const PROOFS: [u64; 5] = [0, 1, 256, 1024, 1025];
// Each JSON byte occupies at most three digits plus a separator. The empty
// schema contributes all field names/punctuation; this is conservative by 4.
const ORIGINAL_LIMIT: usize = b"{\"id\":[],\"request\":[],\"payload\":[],\"transaction_id\":[]}"
    .len()
    + 4 * (MAX_REQUEST_BYTES + MAX_REQUEST_PAYLOAD_BYTES + 32);

fn ordinals<'de, D, T>(deserializer: D) -> Result<BTreeMap<u64, T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Entries<T>(std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> de::Visitor<'de> for Entries<T> {
        type Value = BTreeMap<u64, T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("unique canonical lifecycle ordinal keys")
        }
        fn visit_map<A: de::MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut values = BTreeMap::new();
            while let Some(key) = map.next_key::<String>()? {
                let ordinal: u64 = key.parse().map_err(de::Error::custom)?;
                if key != ordinal.to_string()
                    || !PROOFS.contains(&ordinal)
                    || values.contains_key(&ordinal)
                {
                    return Err(de::Error::custom("invalid or duplicate lifecycle key"));
                }
                values.insert(ordinal, map.next_value()?);
            }
            Ok(values)
        }
    }
    deserializer.deserialize_map(Entries(std::marker::PhantomData))
}

// `Option` normally accepts a missing field as None. A persisted transition must
// distinguish explicit null from a missing required identity/state field.
fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

// Only actual v1 input uses this embedded representation. Its serializer is
// retained for migration validation and exact work-accounting tests, not saves.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct LegacyCheckpoint {
    pub(super) format: u32,
    pub(super) binding: Binding,
    pub(super) source_sha256: String,
    pub(super) elf_sha256: String,
    pub(super) executable: PathBuf,
    pub(super) harness_timeout_secs: u64,
    pub(super) verified_ordinal: u64,
    #[serde(deserialize_with = "required_option")]
    pub(super) pending_external_id: Option<[u8; 16]>,
    #[serde(deserialize_with = "ordinals")]
    pub(super) originals: BTreeMap<u64, RequestRecord>,
    #[serde(deserialize_with = "ordinals")]
    pub(super) proofs: BTreeMap<u64, Vec<u8>>,
    pub(super) grants: Vec<MemberCapacityGrant>,
    pub(super) ack_loss_recovered: bool,
    pub(super) snapshots_restored: bool,
    pub(super) completed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Kind {
    Original,
    Proof,
    LegacyCheckpoint,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactRef {
    format: u32,
    kind: Kind,
    key: u64,
    relative_path: String,
    byte_length: u64,
    sha256: String,
}
impl ArtifactRef {
    fn filename(kind: Kind, key: u64) -> anyhow::Result<String> {
        Ok(match kind {
            Kind::Original if ORIGINALS.contains(&key) => format!("original-{key}.json"),
            Kind::Proof if PROOFS.contains(&key) => format!("proof-v{key}.json"),
            Kind::LegacyCheckpoint if key == 1 => "legacy-checkpoint-v1.json".into(),
            _ => bail!("invalid artifact kind/key"),
        })
    }
    fn validate(&self, kind: Kind, key: u64, limit: usize) -> anyhow::Result<String> {
        let name = Self::filename(kind, key)?;
        ensure!(
            self.format == 1
                && self.kind == kind
                && self.key == key
                && self.relative_path == format!("artifacts/{name}")
                && self.byte_length > 0
                && self.byte_length <= limit as u64
                && self.sha256.len() == 64
                && self
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
            "invalid artifact reference"
        );
        Ok(name)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    format: u32,
    binding: Binding,
    source_sha256: String,
    elf_sha256: String,
    executable: PathBuf,
    harness_timeout_secs: u64,
    verified_ordinal: u64,
    #[serde(deserialize_with = "required_option")]
    pending_external_id: Option<[u8; 16]>,
    #[serde(deserialize_with = "ordinals")]
    originals: BTreeMap<u64, ArtifactRef>,
    #[serde(deserialize_with = "ordinals")]
    proofs: BTreeMap<u64, ArtifactRef>,
    grants: Vec<MemberCapacityGrant>,
    ack_loss_recovered: bool,
    snapshots_restored: bool,
    completed: bool,
    #[serde(deserialize_with = "required_option")]
    migration: Option<ArtifactRef>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Boundary {
    Create,
    Write,
    FileSync,
    Rename,
    DirectorySync,
}

#[derive(Default)]
struct Fault {
    // Store-local deterministic hook; ordinary parallel tests never share state.
    cut: Option<(String, Boundary, bool)>,
}
impl Fault {
    fn check(&self, name: &str, boundary: Boundary, after: bool) -> anyhow::Result<()> {
        ensure!(
            self.cut.as_ref() != Some(&(name.to_owned(), boundary, after)),
            "injected {name} {boundary:?} after={after}"
        );
        Ok(())
    }
}

struct OpenArtifact {
    file: File,
    reference: ArtifactRef,
}
// A writer's lifetime ends when its lease drops, even if a concurrent process
// launch temporarily retains a duplicate of the same open-file description.
struct WriterLease {
    file: File,
}
impl WriterLease {
    fn open(path: &Path) -> anyhow::Result<Self> {
        let file = open_directory(path)?;
        rfs::flock(&file, FlockOperation::NonBlockingLockExclusive)
            .context("lifecycle already has a writer")?;
        Ok(Self { file })
    }
}
impl Drop for WriterLease {
    fn drop(&mut self) {
        // Never panic during cleanup. File close remains the fallback if unlock
        // fails, but alone cannot release a lock while a duplicate still lives.
        let _ = rfs::flock(&self.file, FlockOperation::Unlock);
    }
}

struct Store {
    path: PathBuf,
    root: WriterLease,
    owns_lock: bool,
    artifacts: Option<File>,
    directory_synced: bool,
    published: BTreeMap<String, OpenArtifact>,
    metrics: Metrics,
    fault: Fault,
}

fn open_directory(path: &Path) -> anyhow::Result<File> {
    let mut directory: File = rfs::openat(
        CWD,
        if path.is_absolute() { "/" } else { "." },
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )?
    .into();
    for part in path.components() {
        match part {
            Component::RootDir | Component::CurDir => {}
            Component::Normal(name) => {
                directory = rfs::openat(
                    &directory,
                    name,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                )?
                .into()
            }
            _ => bail!("noncanonical lifecycle directory"),
        }
    }
    Ok(directory)
}
fn regular(file: &File) -> anyhow::Result<fs::Metadata> {
    let metadata = file.metadata()?;
    ensure!(
        metadata.is_file() && metadata.nlink() == 1,
        "evidence must be a regular single-link file"
    );
    Ok(metadata)
}
fn open_file(directory: &File, name: &str) -> anyhow::Result<Option<File>> {
    match rfs::openat(
        directory,
        name,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(fd) => {
            let file = File::from(fd);
            regular(&file)?;
            Ok(Some(file))
        }
        Err(rustix::io::Errno::NOENT) => Ok(None),
        Err(error) => Err(error.into()),
    }
}
fn same_file(left: &File, right: &File) -> anyhow::Result<bool> {
    let left = left.metadata()?;
    let right = right.metadata()?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}
fn read_bounded(file: &mut File, limit: usize) -> anyhow::Result<Vec<u8>> {
    use std::io::{Seek, SeekFrom};
    let before = regular(file)?;
    ensure!(
        before.len() > 0 && before.len() <= limit as u64,
        "invalid evidence byte length"
    );
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::with_capacity(before.len() as usize);
    (&mut *file)
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    ensure!(
        bytes.len() as u64 == before.len() && regular(file)?.len() == before.len(),
        "evidence changed during read"
    );
    Ok(bytes)
}

impl Store {
    fn open(path: &Path, metrics: Metrics) -> anyhow::Result<Self> {
        Self::from_lease(path, WriterLease::open(path)?, metrics)
    }
    fn from_lease(path: &Path, root: WriterLease, metrics: Metrics) -> anyhow::Result<Self> {
        let artifacts = match rfs::openat(
            &root.file,
            "artifacts",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(fd) => Some(File::from(fd)),
            Err(rustix::io::Errno::NOENT) => None,
            Err(error) => return Err(error.into()),
        };
        let value = Self {
            path: path.into(),
            root,
            owns_lock: true,
            artifacts,
            directory_synced: false,
            published: BTreeMap::new(),
            metrics,
            fault: Fault::default(),
        };
        value.check_owner()?;
        Ok(value)
    }
    fn check_owner(&self) -> anyhow::Result<()> {
        ensure!(self.owns_lock, "lifecycle writer ownership was lost");
        self.metrics.work("store.owner_metadata_checks", 0);
        let current = open_directory(&self.path)?;
        ensure!(
            same_file(&self.root.file, &current)?,
            "lifecycle directory replaced"
        );
        // A second open-file description must be unable to take our lock. If it
        // succeeds, ownership was lost; do not silently reacquire and continue.
        match rfs::flock(&current, FlockOperation::NonBlockingLockExclusive) {
            Err(rustix::io::Errno::WOULDBLOCK) => {}
            Ok(()) => {
                // This probe must never become a replacement writer lease.
                // Explicit release also covers a temporarily duplicated probe.
                let _ = rfs::flock(&current, FlockOperation::Unlock);
                bail!("lifecycle writer lock lost");
            }
            Err(error) => return Err(error.into()),
        }
        if let Some(directory) = &self.artifacts {
            let current: File = rfs::openat(
                &self.root.file,
                "artifacts",
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?
            .into();
            ensure!(
                same_file(directory, &current)?,
                "artifact directory replaced"
            );
        }
        Ok(())
    }
    #[cfg(test)]
    fn lose_ownership(&mut self) -> anyhow::Result<()> {
        self.owns_lock = false; // irreversible; never silently reacquire
        rfs::flock(&self.root.file, FlockOperation::Unlock)?;
        Ok(())
    }
    fn ensure_artifacts(&mut self) -> anyhow::Result<()> {
        self.check_owner()?;
        if self.artifacts.is_none() {
            self.fault.check("artifacts", Boundary::Create, false)?;
            rfs::mkdirat(&self.root.file, "artifacts", Mode::RWXU)?;
            let directory = rfs::openat(
                &self.root.file,
                "artifacts",
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )?;
            self.artifacts = Some(directory.into());
            self.fault.check("artifacts", Boundary::Create, true)?;
        }
        if !self.directory_synced {
            self.fault
                .check("artifacts", Boundary::DirectorySync, false)?;
            self.root.file.sync_all()?;
            self.metrics.work("artifact.directory_creation_sync", 0);
            self.fault
                .check("artifacts", Boundary::DirectorySync, true)?;
            self.directory_synced = true;
        }
        Ok(())
    }
    fn names(&self) -> anyhow::Result<BTreeSet<String>> {
        let mut names = BTreeSet::new();
        if let Some(directory) = &self.artifacts {
            for entry in Dir::read_from(directory)? {
                let entry = entry?;
                let name = entry.file_name().to_str()?.to_owned();
                if name != "." && name != ".." {
                    names.insert(name);
                }
            }
        }
        Ok(names)
    }
    fn hash(&self, bytes: &[u8]) -> String {
        let mut span = self.metrics.span("artifact.hash");
        span.bytes(bytes.len());
        format!("{:x}", Sha256::digest(bytes))
    }
    fn read_ref(
        &mut self,
        reference: &ArtifactRef,
        kind: Kind,
        key: u64,
        limit: usize,
    ) -> anyhow::Result<Vec<u8>> {
        self.check_owner()?;
        let name = reference.validate(kind, key, limit)?;
        let directory = self
            .artifacts
            .as_ref()
            .context("missing artifact directory")?;
        let mut file =
            open_file(directory, &name)?.context("missing referenced artifact; no repair")?;
        let bytes = read_bounded(&mut file, limit)?;
        self.metrics.work("artifact.read", bytes.len());
        ensure!(
            bytes.len() as u64 == reference.byte_length && self.hash(&bytes) == reference.sha256,
            "referenced artifact content changed; no repair"
        );
        self.published.insert(
            name,
            OpenArtifact {
                file,
                reference: reference.clone(),
            },
        );
        Ok(bytes)
    }
    fn check_published(&self) -> anyhow::Result<()> {
        self.check_owner()?;
        for (name, saved) in &self.published {
            self.metrics.work("artifact.entry_metadata_checks", 0);
            let current = open_file(self.artifacts.as_ref().context("artifact directory")?, name)?
                .context("missing referenced artifact; no repair")?;
            ensure!(
                same_file(&saved.file, &current)?
                    && regular(&current)?.len() == saved.reference.byte_length,
                "referenced artifact replaced or resized; no repair"
            );
        }
        Ok(())
    }
    fn write(
        &self,
        directory: &File,
        name: &str,
        bytes: &[u8],
        immutable: bool,
        category: &str,
    ) -> anyhow::Result<()> {
        self.check_owner()?;
        let stage = format!("{name}.tmp");
        self.fault.check(name, Boundary::Create, false)?;
        let mut file: File = {
            let _span = self.metrics.span(format!("{category}.create"));
            match rfs::openat(
                directory,
                &stage,
                OFlags::RDWR | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::RUSR | Mode::WUSR,
            ) {
                Ok(fd) => File::from(fd),
                Err(rustix::io::Errno::EXIST) => {
                    let file = File::from(rfs::openat(
                        directory,
                        &stage,
                        OFlags::RDWR | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                        Mode::empty(),
                    )?);
                    regular(&file)?;
                    file.set_len(0)?;
                    file
                }
                Err(error) => return Err(error.into()),
            }
        };
        self.fault.check(name, Boundary::Create, true)?;
        for boundary in [
            Boundary::Write,
            Boundary::FileSync,
            Boundary::Rename,
            Boundary::DirectorySync,
        ] {
            self.fault.check(name, boundary, false)?;
            let label = match boundary {
                Boundary::Write => "write",
                Boundary::FileSync => "file_sync",
                Boundary::Rename => "rename",
                Boundary::DirectorySync => "directory_sync",
                _ => unreachable!(),
            };
            let mut span = self.metrics.span(format!("{category}.{label}"));
            match boundary {
                Boundary::Write => {
                    span.bytes(bytes.len());
                    file.write_all(bytes)?;
                }
                Boundary::FileSync => file.sync_all()?,
                Boundary::Rename => {
                    self.check_owner()?;
                    // The stage itself must still be the regular file we wrote.
                    let current = open_file(directory, &stage)?.context("missing staging file")?;
                    ensure!(same_file(&file, &current)?, "staging file replaced");
                    if immutable {
                        rfs::renameat_with(
                            directory,
                            &stage,
                            directory,
                            name,
                            RenameFlags::NOREPLACE,
                        )?;
                    } else {
                        rfs::renameat(directory, &stage, directory, name)?;
                    }
                }
                Boundary::DirectorySync => directory.sync_all()?,
                _ => unreachable!(),
            }
            drop(span);
            self.fault.check(name, boundary, true)?;
        }
        self.check_owner()?;
        Ok(())
    }
    fn publish(
        &mut self,
        kind: Kind,
        key: u64,
        bytes: &[u8],
        limit: usize,
    ) -> anyhow::Result<ArtifactRef> {
        ensure!(
            !bytes.is_empty() && bytes.len() <= limit,
            "artifact exceeds existing codec bound"
        );
        let name = ArtifactRef::filename(kind, key)?;
        let reference = ArtifactRef {
            format: 1,
            kind,
            key,
            relative_path: format!("artifacts/{name}"),
            byte_length: bytes.len() as u64,
            sha256: self.hash(bytes),
        };
        self.ensure_artifacts()?;
        let directory = self.artifacts.as_ref().context("artifact directory")?;
        if let Some(mut existing) = open_file(directory, &name)? {
            let original = read_bounded(&mut existing, limit)?;
            self.metrics.work("artifact.read", original.len());
            ensure!(
                original == bytes,
                "unreferenced original evidence differs; never overwrite"
            );
            // A previous rename may have survived a failed directory fsync.
            // Revalidate exact bytes and repeat both durability barriers.
            existing.sync_all()?;
            directory.sync_all()?;
            self.metrics.work("artifact.recovered_file_sync", 0);
            self.metrics.work("artifact.recovered_directory_sync", 0);
        } else {
            self.write(directory, &name, bytes, true, "artifact")?;
        }
        self.read_ref(&reference, kind, key, limit)?;
        Ok(reference)
    }
}

pub(super) struct LifecycleCheckpoint {
    state: LegacyCheckpoint,
    store: Store,
    original_refs: BTreeMap<u64, ArtifactRef>,
    proof_refs: BTreeMap<u64, ArtifactRef>,
    migration: Option<ArtifactRef>,
    embedded_lengths: BTreeMap<(u8, u64), usize>,
    pub(super) metrics: Metrics,
}
impl Deref for LifecycleCheckpoint {
    type Target = LegacyCheckpoint;
    fn deref(&self) -> &Self::Target {
        &self.state
    }
}
impl DerefMut for LifecycleCheckpoint {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}
impl Drop for LifecycleCheckpoint {
    fn drop(&mut self) {
        self.metrics.emit(
            &self.source_sha256,
            &self.elf_sha256,
            self.verified_ordinal,
            self.completed,
        );
    }
}

fn exact_fields(value: &serde_json::Value, fields: &[&str]) -> anyhow::Result<()> {
    ensure!(
        value
            .as_object()
            .is_some_and(|object| object.len() == fields.len()
                && fields.iter().all(|field| object.contains_key(*field))),
        "unknown or missing nested historical evidence fields"
    );
    Ok(())
}

fn valid_original(ordinal: u64, original: &RequestRecord) -> anyhow::Result<()> {
    ensure!(
        ORIGINALS.contains(&ordinal)
            && original.id == extended_write(ordinal).external_id
            && original.request.len() <= MAX_REQUEST_BYTES
            && original.payload.len() <= MAX_REQUEST_PAYLOAD_BYTES
            && original.request == canonical_write(&extended_write(ordinal))?,
        "invalid original request identity"
    );
    let plan = d2_raft::request_plans::Plan::decode(&original.payload)
        .map_err(|e| anyhow!("invalid original Plan: {e:?}"))?;
    // The production WriteOp reader permits unknown struct-variant fields.
    // Reject them in this exact archive without changing that production type
    // or replacing the original Plan bytes. Participant is already strict.
    let raw: serde_json::Value = serde_json::from_slice(&original.payload)?;
    for part in raw["parts"].as_array().context("Plan parts")? {
        for operation in part["request"]["operations"]
            .as_array()
            .context("Plan operations")?
        {
            if let Some(put) = operation.get("Put") {
                exact_fields(put, &["key", "value"])?;
            } else if let Some(delete) = operation.get("Delete") {
                exact_fields(delete, &["key"])?;
            } else {
                bail!("unknown historical Plan operation");
            }
        }
    }
    ensure!(
        plan.id == original.transaction_id
            && plan.authority == authority()
            && plan.cohort == byte_binding().cohort
            && plan.validation_version == ordinal - 1
            && plan.parts.len() == 1
            && plan.parts[0].descriptor.shard == 7
            && plan.parts[0].request.expected_sequence == Some(ordinal - 1),
        "original Plan identity changed"
    );
    Ok(())
}
fn valid_proof(version: u64, bytes: &[u8]) -> anyhow::Result<()> {
    ensure!(
        PROOFS.contains(&version) && bytes.len() <= MAX_BYTE_PROFILE_TERMINAL_BYTES,
        "invalid proof bound/key"
    );
    let proof: TerminalPrefix = serde_json::from_slice(bytes)?;
    ensure!(
        proof.authority() == authority().mode
            && proof.version() == version
            && proof.cohort() == &byte_binding().cohort,
        "proof identity mismatch"
    );
    let encoded: serde_json::Value = serde_json::from_slice(bytes)?;
    for terminal in encoded["segments"]
        .as_array()
        .context("proof segments")?
        .iter()
        .flat_map(|s| s.as_array().into_iter().flatten())
    {
        ensure!(
            terminal["decision"] == "Commit"
                && terminal["prepared"] == serde_json::json!([true])
                && terminal["participants"]
                    .as_array()
                    .is_some_and(|p| p.len() == 1)
                && terminal["participants"][0]["shard"] == 7,
            "proof is not complete fixture Commit evidence"
        );
    }
    Ok(())
}
fn validate_state(state: &LegacyCheckpoint, expected: &LegacyCheckpoint) -> anyhow::Result<()> {
    ensure!(
        (state.format == 1 || state.format == 2)
            && state.binding == expected.binding
            && state.source_sha256 == expected.source_sha256
            && state.elf_sha256 == expected.elf_sha256
            && state.executable == expected.executable
            && state.verified_ordinal <= EXTENDED_WRITES,
        "refusing changed source, ELF, binding, executable or format"
    );
    ensure!(
        (1..=86400).contains(&state.harness_timeout_secs),
        "invalid historical timeout"
    );
    ensure!(
        state
            .pending_external_id
            .is_none_or(|id| state.verified_ordinal < EXTENDED_WRITES
                && id == extended_write(state.verified_ordinal + 1).external_id),
        "pending ID is not original next request"
    );
    let mut originals: BTreeSet<_> = ORIGINALS
        .into_iter()
        .filter(|v| *v <= state.verified_ordinal)
        .collect();
    if state.verified_ordinal == 1024
        && state.pending_external_id.is_some()
        && state.originals.contains_key(&1025)
    {
        originals.insert(1025);
    }
    ensure!(
        state.originals.keys().copied().collect::<BTreeSet<_>>() == originals,
        "missing or future historical original"
    );
    let proofs: BTreeSet<_> = PROOFS
        .into_iter()
        .filter(|v| *v <= state.verified_ordinal)
        .collect();
    let initial = state.verified_ordinal == 0
        && state.pending_external_id.is_none()
        && state.originals.is_empty()
        && state.proofs.is_empty()
        && state.grants.is_empty();
    ensure!(
        initial || state.proofs.keys().copied().collect::<BTreeSet<_>>() == proofs,
        "missing or future historical proof"
    );
    ensure!(
        initial || state.grants.len() == 2,
        "missing historical member grants"
    );
    for (member, grant) in state.binding.cohort.members.iter().zip(&state.grants) {
        ensure!(
            member == &grant.member && grant.activation.validate().is_ok(),
            "invalid member grant"
        );
    }
    ensure!(
        state.ack_loss_recovered == (state.verified_ordinal == EXTENDED_WRITES)
            && (!state.snapshots_restored || state.ack_loss_recovered)
            && (!state.completed
                || (state.snapshots_restored && state.pending_external_id.is_none())),
        "unreachable lifecycle flags"
    );
    for (&ordinal, record) in &state.originals {
        valid_original(ordinal, record)?;
    }
    for (&version, bytes) in &state.proofs {
        valid_proof(version, bytes)?;
        let proof: serde_json::Value = serde_json::from_slice(bytes)?;
        for (&key, original) in state.originals.iter().filter(|(key, _)| **key <= version) {
            let index = (key - 1) as usize;
            ensure!(
                proof["segments"][index / 32][index % 32]["id"]
                    == serde_json::to_value(original.transaction_id)?,
                "original/proof transaction ID mismatch"
            );
        }
    }
    Ok(())
}

impl LifecycleCheckpoint {
    fn progress(&self) -> Progress {
        Progress {
            format: 2,
            binding: self.binding.clone(),
            source_sha256: self.source_sha256.clone(),
            elf_sha256: self.elf_sha256.clone(),
            executable: self.executable.clone(),
            harness_timeout_secs: self.harness_timeout_secs,
            verified_ordinal: self.verified_ordinal,
            pending_external_id: self.pending_external_id,
            originals: self.original_refs.clone(),
            proofs: self.proof_refs.clone(),
            grants: self.grants.clone(),
            ack_loss_recovered: self.ack_loss_recovered,
            snapshots_restored: self.snapshots_restored,
            completed: self.completed,
            migration: self.migration.clone(),
        }
    }
    fn expected(retained: bool, timeout: u64) -> anyhow::Result<LegacyCheckpoint> {
        let source_sha256 = std::env::var("BYTE_BOUND_PROCESS_SOURCE_SHA256").unwrap_or_default();
        let elf_sha256 = std::env::var("BYTE_BOUND_PROCESS_ELF_SHA256").unwrap_or_default();
        if retained {
            for (name, hash) in [("source", &source_sha256), ("ELF", &elf_sha256)] {
                ensure!(
                    hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit()),
                    "retained lifecycle requires externally verified {name} SHA-256 metadata"
                );
            }
        }
        Ok(LegacyCheckpoint {
            format: 2,
            binding: byte_binding(),
            source_sha256,
            elf_sha256,
            executable: std::env::current_exe()?,
            harness_timeout_secs: timeout,
            verified_ordinal: 0,
            pending_external_id: None,
            originals: BTreeMap::new(),
            proofs: BTreeMap::new(),
            grants: vec![],
            ack_loss_recovered: false,
            snapshots_restored: false,
            completed: false,
        })
    }
    // Identity-dependent envelope accounts for the actual path/escaped strings.
    // Five Vec<u8> proofs expand at most fourfold; originals are JSON objects.
    fn legacy_limit(expected: &LegacyCheckpoint) -> anyhow::Result<usize> {
        Ok(
            serde_json::to_vec(expected)?.len() + 4 * ORIGINAL_LIMIT + 5 * (4 * MAX_BYTE_PROFILE_TERMINAL_BYTES + 32)
            // Two grants, bounded u64/32-byte arrays, flags, pending ID and all
            // remaining schema punctuation fit within one existing managed frame.
            + d2_raft::managed_rpc::MAX_FRAME_BYTES,
        )
    }
    pub(super) fn load(directory: &Path, retained: bool, timeout: u64) -> anyhow::Result<Self> {
        Self::load_expected(directory, Self::expected(retained, timeout)?)
    }
    fn load_expected(directory: &Path, expected: LegacyCheckpoint) -> anyhow::Result<Self> {
        Self::load_with_fault(directory, expected, Fault::default())
    }
    fn load_with_fault(
        directory: &Path,
        expected: LegacyCheckpoint,
        fault: Fault,
    ) -> anyhow::Result<Self> {
        let metrics = Metrics::default();
        let mut store = Store::open(directory, metrics.clone())?;
        store.fault = fault;
        let mut result = Self {
            state: expected.clone(),
            store,
            original_refs: BTreeMap::new(),
            proof_refs: BTreeMap::new(),
            migration: None,
            embedded_lengths: BTreeMap::new(),
            metrics,
        };
        let limit = Self::legacy_limit(&expected)?;
        if let Some(mut file) = open_file(&result.store.root.file, "progress.json")? {
            let bytes = read_bounded(&mut file, limit)?;
            #[derive(Deserialize)]
            struct Version {
                format: u32,
            }
            let version: Version = serde_json::from_slice(&bytes)?;
            match version.format {
                1 => {
                    let legacy: LegacyCheckpoint = serde_json::from_slice(&bytes)?;
                    validate_state(&legacy, &expected)?; // ZERO writes before all validation.
                    result.state = legacy;
                    result.check_orphans(true)?;
                    result.preflight_migration(&bytes, limit)?;
                    result.migration = Some(result.store.publish(
                        Kind::LegacyCheckpoint,
                        1,
                        &bytes,
                        limit,
                    )?);
                    result.capture()?;
                    result.state.format = 2;
                    result.save(directory, "migration")?;
                }
                2 => {
                    let progress: Progress = serde_json::from_slice(&bytes)?;
                    result.state = LegacyCheckpoint {
                        format: 2,
                        binding: progress.binding,
                        source_sha256: progress.source_sha256,
                        elf_sha256: progress.elf_sha256,
                        executable: progress.executable,
                        harness_timeout_secs: progress.harness_timeout_secs,
                        verified_ordinal: progress.verified_ordinal,
                        pending_external_id: progress.pending_external_id,
                        originals: BTreeMap::new(),
                        proofs: BTreeMap::new(),
                        grants: progress.grants,
                        ack_loss_recovered: progress.ack_loss_recovered,
                        snapshots_restored: progress.snapshots_restored,
                        completed: progress.completed,
                    };
                    // Check complete identity before touching any artifact content.
                    ensure!(
                        result.binding == expected.binding
                            && result.source_sha256 == expected.source_sha256
                            && result.elf_sha256 == expected.elf_sha256
                            && result.executable == expected.executable,
                        "changed lifecycle identity"
                    );
                    result.original_refs = progress.originals;
                    result.proof_refs = progress.proofs;
                    result.migration = progress.migration;
                    result.load_artifacts(limit)?;
                    validate_state(&result.state, &expected)?;
                    result.check_orphans(false)?;
                }
                _ => bail!("unknown lifecycle checkpoint format"),
            }
        } else {
            ensure!(
                ![7, 8, 9].iter().any(|shard| directory
                    .join(format!("shard-{shard}"))
                    .join("data")
                    .exists()),
                "durable databases without original checkpoint must not be reinitialized"
            );
            ensure!(
                result.store.names()?.is_empty(),
                "evidence without original checkpoint must not be reset"
            );
        }
        result.state.harness_timeout_secs = expected.harness_timeout_secs;
        result.state.completed = false;
        Ok(result)
    }
    fn load_artifacts(&mut self, legacy_limit: usize) -> anyhow::Result<()> {
        for (key, reference) in self.original_refs.clone() {
            let bytes = self
                .store
                .read_ref(&reference, Kind::Original, key, ORIGINAL_LIMIT)?;
            let original = serde_json::from_slice(&bytes)?;
            valid_original(key, &original)?;
            self.state.originals.insert(key, original);
            self.embedded_lengths.insert((0, key), bytes.len());
        }
        for (key, reference) in self.proof_refs.clone() {
            let bytes = self.store.read_ref(
                &reference,
                Kind::Proof,
                key,
                MAX_BYTE_PROFILE_TERMINAL_BYTES,
            )?;
            valid_proof(key, &bytes)?;
            self.embedded_lengths
                .insert((1, key), serde_json::to_vec(&bytes)?.len());
            self.state.proofs.insert(key, bytes);
        }
        if let Some(reference) = self.migration.clone() {
            let bytes = self
                .store
                .read_ref(&reference, Kind::LegacyCheckpoint, 1, legacy_limit)?;
            let legacy: LegacyCheckpoint = serde_json::from_slice(&bytes)?;
            ensure!(legacy.format == 1, "migration backup is not v1");
            validate_state(&legacy, &self.state)?;
            ensure!(
                legacy.verified_ordinal <= self.verified_ordinal,
                "migration backup is from a future state"
            );
            ensure!(
                legacy.grants.is_empty() || legacy.grants == self.grants,
                "migration member generation changed"
            );
            for (key, original) in legacy.originals {
                ensure!(
                    self.originals.get(&key) == Some(&original),
                    "migration original changed"
                );
            }
            for (key, proof) in legacy.proofs {
                ensure!(
                    self.proofs.get(&key) == Some(&proof),
                    "migration proof changed"
                );
            }
        }
        Ok(())
    }
    fn preflight_migration(&self, raw: &[u8], limit: usize) -> anyhow::Result<()> {
        let Some(directory) = &self.store.artifacts else {
            return Ok(());
        };
        let mut payloads = vec![("legacy-checkpoint-v1.json".to_owned(), raw.to_vec(), limit)];
        for (&key, original) in &self.originals {
            payloads.push((
                ArtifactRef::filename(Kind::Original, key)?,
                serde_json::to_vec(original)?,
                ORIGINAL_LIMIT,
            ));
        }
        for (&key, proof) in &self.proofs {
            payloads.push((
                ArtifactRef::filename(Kind::Proof, key)?,
                proof.clone(),
                MAX_BYTE_PROFILE_TERMINAL_BYTES,
            ));
        }
        for (name, expected, bound) in payloads {
            if let Some(mut file) = open_file(directory, &name)? {
                let bytes = read_bounded(&mut file, bound)?;
                self.metrics.work("artifact.read", bytes.len());
                ensure!(
                    bytes == expected,
                    "migration orphan differs from immutable v1 preimage"
                );
            }
        }
        Ok(())
    }
    fn check_orphans(&self, migration: bool) -> anyhow::Result<()> {
        let mut allowed: BTreeSet<String> = self
            .original_refs
            .values()
            .chain(self.proof_refs.values())
            .chain(self.migration.iter())
            .map(|r| r.relative_path.trim_start_matches("artifacts/").to_owned())
            .collect();
        if migration {
            allowed.insert("legacy-checkpoint-v1.json".into());
            for &key in self.originals.keys() {
                allowed.insert(ArtifactRef::filename(Kind::Original, key)?);
            }
            for &key in self.proofs.keys() {
                allowed.insert(ArtifactRef::filename(Kind::Proof, key)?);
            }
        } else if !self.completed {
            if self.verified_ordinal == 0 {
                allowed.insert(ArtifactRef::filename(Kind::Proof, 0)?);
            }
            if self.pending_external_id.is_some()
                && ORIGINALS.contains(&(self.verified_ordinal + 1))
            {
                for kind in [Kind::Original, Kind::Proof] {
                    allowed.insert(ArtifactRef::filename(kind, self.verified_ordinal + 1)?);
                }
            }
        }
        let mut permitted = allowed.clone();
        if !self.completed || migration {
            permitted.extend(allowed.iter().map(|n| format!("{n}.tmp")));
        }
        let names = self.store.names()?;
        ensure!(
            names.is_subset(&permitted),
            "unexpected artifact/staging/orphan evidence"
        );
        if self.completed && !migration {
            ensure!(names == allowed, "incomplete artifact closure");
        }
        if let Some(directory) = &self.store.artifacts {
            for name in names.difference(&self.store.published.keys().cloned().collect()) {
                let mut file =
                    open_file(directory, name)?.context("missing orphan/staging file")?;
                if name.ends_with(".tmp") {
                    continue;
                } // partial, never authoritative
                let bound = if name.starts_with("original-") {
                    ORIGINAL_LIMIT
                } else if name.starts_with("proof-v") {
                    MAX_BYTE_PROFILE_TERMINAL_BYTES
                } else {
                    Self::legacy_limit(&self.state)?
                };
                let bytes = read_bounded(&mut file, bound)?;
                self.metrics.work("artifact.read", bytes.len());
                if let Some(key) = name
                    .strip_prefix("original-")
                    .and_then(|n| n.strip_suffix(".json"))
                {
                    valid_original(key.parse()?, &serde_json::from_slice(&bytes)?)?;
                } else if let Some(key) = name
                    .strip_prefix("proof-v")
                    .and_then(|n| n.strip_suffix(".json"))
                {
                    valid_proof(key.parse()?, &bytes)?;
                }
            }
        }
        // No directory entry changes state or creates a ref. Capture is the only
        // normal adoption path and compares the already-present bytes exactly.
        Ok(())
    }
    fn capture(&mut self) -> anyhow::Result<()> {
        for (&key, original) in &self.state.originals {
            if !self.original_refs.contains_key(&key) {
                valid_original(key, original)?;
                let bytes = {
                    let mut span = self.metrics.span("artifact.original_serialize");
                    let bytes = serde_json::to_vec(original)?;
                    span.bytes(bytes.len());
                    bytes
                };
                let reference = self
                    .store
                    .publish(Kind::Original, key, &bytes, ORIGINAL_LIMIT)?;
                self.embedded_lengths.insert((0, key), bytes.len());
                self.original_refs.insert(key, reference);
            }
        }
        for (&key, bytes) in &self.state.proofs {
            if !self.proof_refs.contains_key(&key) {
                valid_proof(key, bytes)?;
                let reference =
                    self.store
                        .publish(Kind::Proof, key, bytes, MAX_BYTE_PROFILE_TERMINAL_BYTES)?;
                self.embedded_lengths
                    .insert((1, key), serde_json::to_vec(bytes)?.len());
                self.proof_refs.insert(key, reference);
            }
        }
        Ok(())
    }
    fn legacy_size(&self) -> anyhow::Result<usize> {
        // Serialize only bounded metadata. Cache immutable embedded payload sizes
        // once per capture/load; map punctuation/digit widths are exact.
        let empty = LegacyCheckpoint {
            originals: BTreeMap::new(),
            proofs: BTreeMap::new(),
            format: 1,
            ..self.state.clone_without_evidence()
        };
        let mut bytes = serde_json::to_vec(&empty)?.len();
        for (kind, keys) in [
            (0, self.original_refs.keys().copied().collect::<Vec<_>>()),
            (1, self.proof_refs.keys().copied().collect()),
        ] {
            if !keys.is_empty() {
                bytes += keys
                    .iter()
                    .map(|key| key.to_string().len() + 3 + self.embedded_lengths[&(kind, *key)])
                    .sum::<usize>()
                    + keys.len()
                    - 1;
            }
        }
        Ok(bytes)
    }
    pub(super) fn save(&mut self, directory: &Path, phase: &str) -> anyhow::Result<()> {
        ensure!(
            directory == self.store.path,
            "changed checkpoint destination"
        );
        self.store.check_published()?;
        self.capture()?;
        self.check_orphans(false)?;
        if self.completed {
            self.load_artifacts(Self::legacy_limit(&self.state)?)?;
            validate_state(&self.state, &self.state)?;
        }
        self.metrics
            .work("checkpoint.equivalent_v1_bytes", self.legacy_size()?);
        let bytes = {
            let mut span = self.metrics.span(format!("checkpoint.{phase}.serialize"));
            let bytes = serde_json::to_vec(&self.progress())?;
            span.bytes(bytes.len());
            bytes
        };
        self.metrics.work("checkpoint.encoded", bytes.len());
        self.store.write(
            &self.store.root.file,
            "progress.json",
            &bytes,
            false,
            &format!("checkpoint.{phase}"),
        )
    }
}
impl LegacyCheckpoint {
    fn clone_without_evidence(&self) -> Self {
        Self {
            format: self.format,
            binding: self.binding.clone(),
            source_sha256: self.source_sha256.clone(),
            elf_sha256: self.elf_sha256.clone(),
            executable: self.executable.clone(),
            harness_timeout_secs: self.harness_timeout_secs,
            verified_ordinal: self.verified_ordinal,
            pending_external_id: self.pending_external_id,
            originals: BTreeMap::new(),
            proofs: BTreeMap::new(),
            grants: self.grants.clone(),
            ack_loss_recovered: self.ack_loss_recovered,
            snapshots_restored: self.snapshots_restored,
            completed: self.completed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use d2_raft::{
        capacity_policy::CapacityActivation,
        request_plans::{Part, Plan},
        transaction_decisions::Participant,
        D1WriteRequest, WriteOp,
    };

    fn expected() -> LegacyCheckpoint {
        let mut state = LifecycleCheckpoint::expected(false, 21600).unwrap();
        state.source_sha256 = "a".repeat(64);
        state.elf_sha256 = "b".repeat(64);
        state
    }
    fn participant() -> Participant {
        Participant {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
            intent: [1; 32],
        }
    }
    fn original(ordinal: u64) -> RequestRecord {
        let id = extended_write(ordinal).external_id;
        let plan = Plan {
            version: 1,
            id,
            authority: authority(),
            cohort: byte_binding().cohort,
            validation_version: ordinal - 1,
            parts: vec![Part {
                descriptor: participant(),
                request: D1WriteRequest {
                    shard: 7,
                    epoch: 1,
                    request_id: id,
                    expected_sequence: Some(ordinal - 1),
                    operations: vec![WriteOp::Put {
                        key: vec![1],
                        value: vec![2],
                    }],
                },
            }],
        };
        RequestRecord {
            id,
            request: canonical_write(&extended_write(ordinal)).unwrap(),
            payload: plan.encode().unwrap(),
            transaction_id: id,
        }
    }
    fn proof(version: u64) -> Vec<u8> {
        let rows: Vec<_> = (1..=version).map(|ordinal| serde_json::json!({"id": extended_write(ordinal).external_id, "ordinal": ordinal, "participants": [participant()], "prepared": [true], "decision": "Commit"})).collect();
        let segments: Vec<_> = rows.chunks(32).collect();
        let typed: TerminalPrefix = serde_json::from_value(serde_json::json!({"version":2, "authority":authority().mode, "cohort":byte_binding().cohort, "through":version, "segments":segments})).unwrap();
        serde_json::to_vec(&typed).unwrap()
    }
    fn state(through: u64) -> LegacyCheckpoint {
        let mut state = expected();
        state.format = 1;
        state.verified_ordinal = through;
        state.ack_loss_recovered = through == 1025;
        state.grants = state
            .binding
            .cohort
            .members
            .iter()
            .map(|member| MemberCapacityGrant {
                member: member.clone(),
                activation: CapacityActivation {
                    nonce: [member.shard as u8; 32],
                    log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
                },
            })
            .collect();
        for key in ORIGINALS.into_iter().filter(|v| *v <= through) {
            state.originals.insert(key, original(key));
        }
        for key in PROOFS.into_iter().filter(|v| *v <= through) {
            state.proofs.insert(key, proof(key));
        }
        state
    }
    fn put_v1(directory: &Path, state: &LegacyCheckpoint) -> Vec<u8> {
        let bytes = serde_json::to_vec(state).unwrap();
        fs::write(directory.join("progress.json"), &bytes).unwrap();
        bytes
    }
    fn tree(directory: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
        fn visit(root: &Path, directory: &Path, output: &mut BTreeMap<PathBuf, Vec<u8>>) {
            for entry in fs::read_dir(directory).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    visit(root, &path, output);
                } else {
                    output.insert(
                        path.strip_prefix(root).unwrap().to_owned(),
                        fs::read(path).unwrap(),
                    );
                }
            }
        }
        let mut output = BTreeMap::new();
        visit(directory, directory, &mut output);
        output
    }
    #[test]
    fn strict_migration_preserves_every_original_and_reachable_state() {
        let mut states = vec![
            {
                let mut s = expected();
                s.format = 1;
                s
            },
            state(0),
            state(1),
            state(256),
            state(1024),
            state(1025),
        ];
        let mut pending = state(1);
        pending.pending_external_id = Some(extended_write(2).external_id);
        states.push(pending);
        let mut lost_ack = state(1024);
        lost_ack.pending_external_id = Some(extended_write(1025).external_id);
        lost_ack.originals.insert(1025, original(1025));
        states.push(lost_ack);
        let mut complete = state(1025);
        complete.snapshots_restored = true;
        complete.completed = true;
        states.push(complete);
        for old in states {
            let directory = tempfile::tempdir().unwrap();
            let raw = put_v1(directory.path(), &old);
            let current = LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
            assert_eq!(current.verified_ordinal, old.verified_ordinal);
            assert_eq!(current.pending_external_id, old.pending_external_id);
            assert_eq!(current.originals, old.originals);
            assert_eq!(current.proofs, old.proofs);
            assert_eq!(current.grants, old.grants);
            assert_eq!(current.ack_loss_recovered, old.ack_loss_recovered);
            assert_eq!(current.snapshots_restored, old.snapshots_restored);
            assert!(!current.completed);
            assert_eq!(
                fs::read(directory.path().join("artifacts/legacy-checkpoint-v1.json")).unwrap(),
                raw
            );
            drop(current);
            LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
        }
    }
    #[test]
    fn identity_malformed_and_missing_history_reject_with_zero_writes() {
        for case in 0..9 {
            let directory = tempfile::tempdir().unwrap();
            let mut old = state(1);
            match case {
                0 => old.source_sha256 = "c".repeat(64),
                1 => old.elf_sha256 = "c".repeat(64),
                2 => old.executable = "/wrong/executable".into(),
                3 => old.binding.authority.shard = 99,
                4 => old.pending_external_id = Some([0; 16]),
                5 => old.format = 99,
                6 => {
                    old.originals.remove(&1);
                }
                7 => {
                    old.proofs.remove(&0);
                }
                _ => old.originals.get_mut(&1).unwrap().payload[0] ^= 1,
            }
            put_v1(directory.path(), &old);
            let before = tree(directory.path());
            assert!(LifecycleCheckpoint::load_expected(directory.path(), expected()).is_err());
            assert_eq!(tree(directory.path()), before);
        }
    }
    #[test]
    fn nullable_fields_are_required_and_only_explicit_null_means_none() {
        let initial = expected();
        let mut legacy = initial.clone();
        legacy.format = 1;
        let directory = tempfile::tempdir().unwrap();
        put_v1(directory.path(), &legacy);
        drop(LifecycleCheckpoint::load_expected(directory.path(), initial.clone()).unwrap());
        for (version, field) in [
            (1, "pending_external_id"),
            (2, "pending_external_id"),
            (2, "migration"),
        ] {
            let directory = tempfile::tempdir().unwrap();
            if version == 1 {
                put_v1(directory.path(), &legacy);
            } else {
                let mut current =
                    LifecycleCheckpoint::load_expected(directory.path(), initial.clone()).unwrap();
                current.save(directory.path(), "setup").unwrap();
                drop(current);
            }
            let path = directory.path().join("progress.json");
            let mut value: serde_json::Value =
                serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
            assert_eq!(value[field], serde_json::Value::Null);
            value.as_object_mut().unwrap().remove(field);
            fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
            let before = tree(directory.path());
            assert!(LifecycleCheckpoint::load_expected(directory.path(), initial.clone()).is_err());
            assert_eq!(tree(directory.path()), before);
        }
    }

    #[test]
    fn duplicate_keys_unknown_fields_and_noncanonical_map_keys_reject() {
        for (old, new) in [
            ("\"format\":1", "\"format\":1,\"format\":1"),
            ("\"format\":1", "\"format\":1,\"unknown\":false"),
            ("\"originals\":{\"1\":", "\"originals\":{\"01\":"),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let raw = String::from_utf8(put_v1(directory.path(), &state(1)))
                .unwrap()
                .replacen(old, new, 1);
            fs::write(directory.path().join("progress.json"), raw).unwrap();
            let before = tree(directory.path());
            assert!(LifecycleCheckpoint::load_expected(directory.path(), expected()).is_err());
            assert_eq!(tree(directory.path()), before);
        }
    }
    #[test]
    fn nested_evidence_fields_remain_strict_without_changing_production_codec() {
        for case in ["Plan participant", "Plan operation", "proof participant"] {
            let directory = tempfile::tempdir().unwrap();
            let mut old = state(1);
            if case.starts_with("Plan") {
                let record = old.originals.get_mut(&1).unwrap();
                let mut raw: serde_json::Value = serde_json::from_slice(&record.payload).unwrap();
                if case == "Plan participant" {
                    raw["parts"][0]["descriptor"]["extra"] = true.into();
                } else {
                    raw["parts"][0]["request"]["operations"][0]["Put"]["extra"] = true.into();
                }
                record.payload = serde_json::to_vec(&raw).unwrap();
                if case == "Plan participant" {
                    assert!(d2_raft::request_plans::Plan::decode(&record.payload).is_err());
                } else {
                    assert!(d2_raft::request_plans::Plan::decode(&record.payload).is_ok());
                }
            } else {
                let bytes = old.proofs.get_mut(&1).unwrap();
                let mut raw: serde_json::Value = serde_json::from_slice(bytes).unwrap();
                raw["segments"][0][0]["participants"][0]["extra"] = true.into();
                *bytes = serde_json::to_vec(&raw).unwrap();
                assert!(serde_json::from_slice::<TerminalPrefix>(bytes).is_err());
            }
            put_v1(directory.path(), &old);
            let before = tree(directory.path());
            assert!(LifecycleCheckpoint::load_expected(directory.path(), expected()).is_err());
            assert_eq!(tree(directory.path()), before);
        }
    }

    #[test]
    fn artifact_directory_creation_and_parent_sync_cuts_never_publish_refs() {
        for boundary in [Boundary::Create, Boundary::DirectorySync] {
            for after in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let old = state(1);
                let original = put_v1(directory.path(), &old);
                let fault = Fault {
                    cut: Some(("artifacts".into(), boundary, after)),
                };
                assert!(
                    LifecycleCheckpoint::load_with_fault(directory.path(), expected(), fault)
                        .is_err()
                );
                assert_eq!(
                    fs::read(directory.path().join("progress.json")).unwrap(),
                    original
                );
                assert!(!directory
                    .path()
                    .join("artifacts/legacy-checkpoint-v1.json")
                    .exists());
                let resumed =
                    LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
                assert_eq!(resumed.verified_ordinal, 1);
                assert_eq!(resumed.originals, old.originals);
                assert_eq!(resumed.proofs, old.proofs);
            }
        }
    }

    #[test]
    fn every_migration_publication_cut_preserves_v1_or_complete_v2() {
        for name in [
            "legacy-checkpoint-v1.json",
            "original-1.json",
            "proof-v0.json",
            "proof-v1.json",
            "progress.json",
        ] {
            for boundary in [
                Boundary::Create,
                Boundary::Write,
                Boundary::FileSync,
                Boundary::Rename,
                Boundary::DirectorySync,
            ] {
                for after in [false, true] {
                    let directory = tempfile::tempdir().unwrap();
                    let old = state(1);
                    let raw = put_v1(directory.path(), &old);
                    let fault = Fault {
                        cut: Some((name.into(), boundary, after)),
                    };
                    assert!(LifecycleCheckpoint::load_with_fault(
                        directory.path(),
                        expected(),
                        fault
                    )
                    .is_err());
                    let bytes = fs::read(directory.path().join("progress.json")).unwrap();
                    let version: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    if version["format"] == 1 {
                        assert_eq!(bytes, raw);
                    } else {
                        assert_eq!(version["format"], 2);
                    }
                    let resumed =
                        LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
                    assert_eq!(resumed.originals, old.originals);
                    assert_eq!(resumed.proofs, old.proofs);
                    assert_eq!(resumed.verified_ordinal, 1);
                }
            }
        }
    }
    #[test]
    fn missing_corrupt_replaced_and_symlink_refs_never_repair_from_backup() {
        use std::os::unix::fs::symlink;
        for case in 0..5 {
            let directory = tempfile::tempdir().unwrap();
            put_v1(directory.path(), &state(1));
            drop(LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap());
            let path = directory.path().join("artifacts/proof-v1.json");
            let bytes = fs::read(&path).unwrap();
            match case {
                0 => fs::remove_file(&path).unwrap(),
                1 => fs::write(&path, &bytes[..bytes.len() / 2]).unwrap(),
                2 => {
                    let mut changed = bytes;
                    changed[0] ^= 1;
                    fs::write(&path, changed).unwrap();
                }
                3 => {
                    fs::remove_file(&path).unwrap();
                    symlink("proof-v0.json", &path).unwrap();
                }
                _ => {
                    fs::remove_file(directory.path().join("artifacts/legacy-checkpoint-v1.json"))
                        .unwrap();
                }
            }
            let before = tree(directory.path());
            assert!(LifecycleCheckpoint::load_expected(directory.path(), expected()).is_err());
            assert_eq!(tree(directory.path()), before);
        }
    }
    #[test]
    fn stale_orphan_cannot_advance_and_existing_bytes_are_not_overwritten() {
        let directory = tempfile::tempdir().unwrap();
        let mut current = LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
        current.state = state(0);
        current.state.format = 2;
        current.save(directory.path(), "setup").unwrap();
        current.pending_external_id = Some(extended_write(1).external_id);
        current.save(directory.path(), "pending").unwrap();
        let bytes = serde_json::to_vec(&original(1)).unwrap();
        current
            .store
            .publish(Kind::Original, 1, &bytes, ORIGINAL_LIMIT)
            .unwrap();
        drop(current);
        let mut resumed = LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
        assert_eq!(resumed.verified_ordinal, 0);
        assert!(resumed.originals.is_empty());
        let mut wrong = original(1);
        wrong.payload.push(b' ');
        let before = fs::read(directory.path().join("artifacts/original-1.json")).unwrap();
        assert!(resumed
            .store
            .publish(
                Kind::Original,
                1,
                &serde_json::to_vec(&wrong).unwrap(),
                ORIGINAL_LIMIT
            )
            .is_err());
        assert_eq!(
            fs::read(directory.path().join("artifacts/original-1.json")).unwrap(),
            before
        );
    }
    #[test]
    fn writer_lease_drop_releases_duplicated_description_without_unlocking_successor() {
        let directory = tempfile::tempdir().unwrap();
        let current = Store::open(directory.path(), Metrics::default()).unwrap();
        let duplicate = current.root.file.try_clone().unwrap();
        assert!(Store::open(directory.path(), Metrics::default()).is_err());
        drop(current);
        let replacement = Store::open(directory.path(), Metrics::default()).unwrap();
        assert!(Store::open(directory.path(), Metrics::default()).is_err());
        drop(duplicate);
        assert!(Store::open(directory.path(), Metrics::default()).is_err());
        drop(replacement);

        // An already-lost old lease must not unlock a distinct successor's OFD.
        let mut old = Store::open(directory.path(), Metrics::default()).unwrap();
        let duplicate = old.root.file.try_clone().unwrap();
        old.lose_ownership().unwrap();
        let replacement = Store::open(directory.path(), Metrics::default()).unwrap();
        drop(old);
        assert!(Store::open(directory.path(), Metrics::default()).is_err());
        drop(duplicate);
        assert!(Store::open(directory.path(), Metrics::default()).is_err());
        replacement.check_owner().unwrap();
    }

    #[test]
    fn writer_lease_construction_error_releases_duplicated_description() {
        let directory = tempfile::tempdir().unwrap();
        // Force the actual fallible artifacts open after successful acquisition.
        fs::write(directory.path().join("artifacts"), b"not a directory").unwrap();
        let lease = WriterLease::open(directory.path()).unwrap();
        let duplicate = lease.file.try_clone().unwrap();
        assert!(Store::from_lease(directory.path(), lease, Metrics::default()).is_err());
        fs::remove_file(directory.path().join("artifacts")).unwrap();
        let replacement = Store::open(directory.path(), Metrics::default()).unwrap();
        assert!(Store::open(directory.path(), Metrics::default()).is_err());
        drop(duplicate);
        assert!(Store::open(directory.path(), Metrics::default()).is_err());
        replacement.check_owner().unwrap();
    }

    #[test]
    fn owner_loss_and_directory_replacement_prevent_publication() {
        let directory = tempfile::tempdir().unwrap();
        let mut first = LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
        assert!(LifecycleCheckpoint::load_expected(directory.path(), expected()).is_err());
        first.store.lose_ownership().unwrap();
        let second = LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
        assert!(first.save(directory.path(), "setup").is_err());
        assert!(!directory.path().join("progress.json").exists());
        drop(second);
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("run");
        fs::create_dir(&path).unwrap();
        let mut saved = LifecycleCheckpoint::load_expected(&path, expected()).unwrap();
        fs::rename(&path, parent.path().join("old")).unwrap();
        fs::create_dir(&path).unwrap();
        assert!(saved.save(&path, "setup").is_err());
    }
    #[test]
    fn same_inode_same_length_damage_may_pass_small_save_but_not_full_validation() {
        let directory = tempfile::tempdir().unwrap();
        put_v1(directory.path(), &state(1025));
        let mut current = LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
        let path = directory.path().join("artifacts/proof-v1.json");
        let mut bytes = fs::read(&path).unwrap();
        bytes[0] ^= 1;
        fs::write(&path, bytes).unwrap();
        current.save(directory.path(), "setup").unwrap();
        current.snapshots_restored = true;
        current.completed = true;
        assert!(current.save(directory.path(), "complete").is_err());
        let saved: serde_json::Value =
            serde_json::from_slice(&fs::read(directory.path().join("progress.json")).unwrap())
                .unwrap();
        assert_eq!(saved["completed"], false);
        drop(current);
        assert!(LifecycleCheckpoint::load_expected(directory.path(), expected()).is_err());
    }
    #[test]
    fn normal_pending_and_verified_publication_cuts_preserve_original_ids() {
        for name in ["original-1.json", "proof-v1.json", "progress.json"] {
            for boundary in [
                Boundary::Create,
                Boundary::Write,
                Boundary::FileSync,
                Boundary::Rename,
                Boundary::DirectorySync,
            ] {
                for after in [false, true] {
                    let directory = tempfile::tempdir().unwrap();
                    let mut current =
                        LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
                    current.state = state(0);
                    current.state.format = 2;
                    current.save(directory.path(), "setup").unwrap();
                    current.pending_external_id = Some(extended_write(1).external_id);
                    current.save(directory.path(), "pending").unwrap();
                    current.state = state(1);
                    current.state.format = 2;
                    current.store.fault = Fault {
                        cut: Some((name.into(), boundary, after)),
                    };
                    assert!(current.save(directory.path(), "verified").is_err());
                    drop(current);
                    let resumed =
                        LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
                    assert!(resumed.verified_ordinal <= 1);
                    if resumed.verified_ordinal == 0 {
                        assert_eq!(
                            resumed.pending_external_id,
                            Some(extended_write(1).external_id)
                        );
                        assert!(resumed.originals.is_empty());
                    } else {
                        assert_eq!(resumed.pending_external_id, None);
                        assert_eq!(resumed.originals[&1], original(1));
                        assert_eq!(resumed.proofs[&1], proof(1));
                    }
                }
            }
        }
        for boundary in [
            Boundary::Create,
            Boundary::Write,
            Boundary::FileSync,
            Boundary::Rename,
            Boundary::DirectorySync,
        ] {
            for after in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let mut current =
                    LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
                current.state = state(0);
                current.state.format = 2;
                current.save(directory.path(), "setup").unwrap();
                current.pending_external_id = Some(extended_write(1).external_id);
                current.store.fault = Fault {
                    cut: Some(("progress.json".into(), boundary, after)),
                };
                // The harness's Submit is after this fallible durable save.
                let mut sends = 0;
                if current.save(directory.path(), "pending").is_ok() {
                    sends += 1;
                }
                assert_eq!(sends, 0);
                drop(current);
                let resumed =
                    LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
                assert_eq!(resumed.verified_ordinal, 0);
                assert!(
                    resumed.pending_external_id.is_none()
                        || resumed.pending_external_id == Some(extended_write(1).external_id)
                );
            }
        }
    }

    #[test]
    fn fixture_work_keeps_2055_saves_4110_syncs_and_nine_payload_publications() {
        // This is only persistence work accounting with synthetic records. It
        // does not run Submit, processes or protocol acceptance and cannot pass
        // the mandatory release lifecycle gate.
        let directory = tempfile::tempdir().unwrap();
        let mut current = LifecycleCheckpoint::load_expected(directory.path(), expected()).unwrap();
        current.save(directory.path(), "setup").unwrap();
        current.state = state(0);
        current.state.format = 2;
        current.save(directory.path(), "setup").unwrap();
        for ordinal in 1..=1025 {
            current.pending_external_id = Some(extended_write(ordinal).external_id);
            current.save(directory.path(), "pending").unwrap();
            if ordinal == 1025 {
                current.originals.insert(ordinal, original(ordinal));
                current.save(directory.path(), "ack_loss").unwrap();
            }
            if ORIGINALS.contains(&ordinal) {
                current.originals.insert(ordinal, original(ordinal));
                current.proofs.insert(ordinal, proof(ordinal));
            }
            current.verified_ordinal = ordinal;
            current.pending_external_id = None;
            current.ack_loss_recovered = ordinal == 1025;
            current.save(directory.path(), "verified").unwrap();
        }
        current.snapshots_restored = true;
        current.save(directory.path(), "snapshot_flag").unwrap();
        current.completed = true;
        current.save(directory.path(), "complete").unwrap();
        assert_eq!(current.metrics.count("checkpoint.encoded"), 2055);
        let syncs: u64 = [
            "setup",
            "pending",
            "verified",
            "ack_loss",
            "snapshot_flag",
            "complete",
        ]
        .into_iter()
        .map(|phase| {
            current
                .metrics
                .count(&format!("checkpoint.{phase}.file_sync"))
                + current
                    .metrics
                    .count(&format!("checkpoint.{phase}.directory_sync"))
        })
        .sum();
        assert_eq!(syncs, 4110);
        assert_eq!(current.metrics.count("checkpoint.pending.file_sync"), 1025);
        assert_eq!(current.metrics.count("checkpoint.verified.file_sync"), 1025);
        assert_eq!(current.metrics.count("artifact.write"), 9);
        assert_eq!(current.metrics.count("artifact.original_serialize"), 4);
        assert_eq!(current.metrics.count("artifact.read"), 18); // publication + COMPLETE
        assert_eq!(current.metrics.count("artifact.hash"), 27); // hash, read-back, COMPLETE
    }

    #[test]
    fn exact_v1_work_accounting_matches_real_serialization() {
        for ordinal in [0, 1, 9, 99, 256, 999, 1024, 1025] {
            let directory = tempfile::tempdir().unwrap();
            let mut old = state(ordinal);
            old.executable = PathBuf::from("/escaped/quote\"/backslash\\/unicode-图");
            let expected = old.clone_without_evidence();
            put_v1(directory.path(), &old);
            let mut current =
                LifecycleCheckpoint::load_expected(directory.path(), expected).unwrap();
            for timeout in [1, 9, 99, 999, 86400] {
                current.harness_timeout_secs = timeout;
                let mut embedded = current.state.clone();
                embedded.format = 1;
                assert_eq!(
                    current.legacy_size().unwrap(),
                    serde_json::to_vec(&embedded).unwrap().len()
                );
            }
        }
    }
}
