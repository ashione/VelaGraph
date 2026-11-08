//! Derived durable terminal segments. Original decision events remain authoritative.
//! There is no independent admission quota, history truncation or ordinal reset.
use crate::d1::D1State;
use crate::participant_writes::Authority;
use crate::terminal_prefix::{Terminal, TerminalPrefix, SEGMENT_TRANSACTIONS};
use crate::transaction_decisions::{Cohort, Journal, Transaction};
use crate::{D1Error, NodeId};
use openraft::LogId;
use rocksdb::{Direction, IteratorMode, WriteBatch, DB};
use serde::{Deserialize, Deserializer, Serialize};

pub(crate) const PREFIX: &[u8] = b"sm/decision/";
pub(crate) const PREFIX_END: &[u8] = b"sm/decision0";
pub(crate) const MANIFEST_KEY: &[u8] = b"sm/decision-manifest";
const MAX_SEGMENT_BYTES: usize = 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Domain {
    role: Role,
    shard: u64,
    epoch: u64,
    mode: [u8; 16],
    authority: Authority,
    cohort: Cohort,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Role {
    Coordinator,
    Member,
}
impl Domain {
    fn key(&self, first: u64) -> Vec<u8> {
        let mut key = PREFIX.to_vec();
        key.extend_from_slice(&self.cohort.profile.storage_format().to_be_bytes());
        key.push(match self.role {
            Role::Coordinator => 0,
            Role::Member => 1,
        });
        key.extend_from_slice(&self.shard.to_be_bytes());
        key.extend_from_slice(&self.epoch.to_be_bytes());
        key.extend_from_slice(&self.mode);
        key.extend_from_slice(&self.authority.shard.to_be_bytes());
        key.extend_from_slice(&self.authority.mode);
        key.extend_from_slice(&self.cohort.coordinator_epoch.to_be_bytes());
        key.extend_from_slice(&first.to_be_bytes());
        key
    }
}
struct View<'a> {
    domain: Domain,
    journal: &'a Journal,
    through: u64,
}
impl<'a> View<'a> {
    fn of(state: &'a D1State) -> Option<Self> {
        let (role, journal, through) = if let Some(journal) = &state.coordinator {
            (
                Role::Coordinator,
                journal.full()?,
                journal.resolved_prefix(),
            )
        } else {
            let visibility = state.visibility.as_ref()?;
            (Role::Member, visibility.journal.full()?, visibility.applied)
        };
        let cohort = journal.cohort()?.clone();
        Some(Self {
            domain: Domain {
                role,
                shard: state.shard,
                epoch: state.epoch,
                mode: state.managed.as_ref()?.mode_id,
                authority: Authority {
                    shard: cohort.coordinator_shard,
                    mode: journal.authority(),
                },
                cohort,
            },
            journal,
            through,
        })
    }
    fn key(&self, first: u64) -> Vec<u8> {
        self.domain.key(first)
    }
    fn row(&self, first: u64) -> anyhow::Result<Row> {
        anyhow::ensure!(
            first != 0
                && (first - 1).is_multiple_of(SEGMENT_TRANSACTIONS as u64)
                && first <= self.through,
            "invalid absolute decision segment start"
        );
        let last = first
            .checked_add(SEGMENT_TRANSACTIONS as u64 - 1)
            .ok_or_else(|| anyhow::anyhow!("decision ordinal overflow"))?
            .min(self.through);
        let records = (first..=last)
            .map(|ordinal| {
                let tx = self
                    .journal
                    .transaction_at(ordinal)
                    .ok_or_else(|| anyhow::anyhow!("missing decision ordinal"))?;
                anyhow::ensure!(tx.ordinal == ordinal, "decision ordinal mismatch");
                Ok(Terminal {
                    id: tx.id,
                    ordinal,
                    participants: tx.participants.clone(),
                    prepared: tx.prepared.clone(),
                    decision: tx
                        .decision
                        .ok_or_else(|| anyhow::anyhow!("unresolved decision"))?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(Row {
            version: self.domain.cohort.profile.storage_format(),
            domain: self.domain.clone(),
            first,
            records,
        })
    }
}
pub(crate) fn enabled(state: &D1State) -> bool {
    state
        .coordinator
        .as_ref()
        .is_some_and(|coordinator| coordinator.cohort().is_some())
        || state
            .visibility
            .as_ref()
            .is_some_and(|v| v.journal.cohort().is_some())
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    version: u32,
    domain: Domain,
    first: u64,
    #[serde(deserialize_with = "deserialize_records")]
    records: Vec<Terminal>,
}
fn deserialize_records<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Terminal>, D::Error> {
    crate::capacity_policy::deserialize_bounded_vec(d, SEGMENT_TRANSACTIONS)
}
impl Row {
    fn bytes(&self) -> anyhow::Result<Vec<u8>> {
        let bytes = serde_json::to_vec(self)?;
        anyhow::ensure!(
            bytes.len() <= MAX_SEGMENT_BYTES,
            "decision segment exceeds bound"
        );
        Ok(bytes)
    }
    fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(
            bytes.len() <= MAX_SEGMENT_BYTES,
            "decision segment exceeds bound"
        );
        let row: Self = serde_json::from_slice(bytes)?;
        anyhow::ensure!(
            row.version == row.domain.cohort.profile.storage_format() && row.bytes()? == bytes,
            "noncanonical decision segment"
        );
        Ok(row)
    }
}
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    version: u32,
    domain: Domain,
    through: u64,
    segments: u64,
    encoded_bytes: u64,
    applied: Option<LogId<NodeId>>,
}
impl Manifest {
    fn of(view: &View<'_>, applied: Option<LogId<NodeId>>) -> anyhow::Result<Self> {
        let mut encoded_bytes = 0u64;
        for first in (1..=view.through).step_by(SEGMENT_TRANSACTIONS) {
            encoded_bytes = encoded_bytes
                .checked_add((view.key(first).len() + view.row(first)?.bytes()?.len()) as u64)
                .ok_or_else(|| anyhow::anyhow!("decision size overflow"))?;
        }
        Ok(Self {
            version: view.domain.cohort.profile.storage_format(),
            domain: view.domain.clone(),
            through: view.through,
            segments: view.through.div_ceil(SEGMENT_TRANSACTIONS as u64),
            encoded_bytes,
            applied,
        })
    }
}

/// Canonical expected manifest for a strictly validated publication candidate.
/// Shared with the small archive witness; it is not an additional disk format.
pub(crate) fn manifest_bytes(
    state: &D1State,
    applied: Option<LogId<NodeId>>,
) -> anyhow::Result<Vec<u8>> {
    let view = View::of(state).ok_or_else(|| anyhow::anyhow!("missing decision state"))?;
    Ok(serde_json::to_vec(&Manifest::of(&view, applied)?)?)
}

/// Full strict validation on reopen; a legacy format cannot carry orphan rows.
pub(crate) fn validate(
    db: &DB,
    state: Option<&D1State>,
    applied: Option<LogId<NodeId>>,
    indexed: bool,
) -> anyhow::Result<()> {
    let view = state.and_then(View::of);
    let manifest = db.get(MANIFEST_KEY)?;
    let mut count = 0u64;
    let mut encoded_bytes = 0u64;
    for item in db.iterator(IteratorMode::From(PREFIX, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(PREFIX) {
            break;
        }
        anyhow::ensure!(indexed, "orphan or mixed-format decision segment");
        let view = view
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("decision domain missing"))?;
        let first = count
            .checked_mul(SEGMENT_TRANSACTIONS as u64)
            .and_then(|n| n.checked_add(1))
            .ok_or_else(|| anyhow::anyhow!("decision ordinal overflow"))?;
        let expected = view.row(first)?;
        anyhow::ensure!(
            key.as_ref() == view.key(first) && Row::decode(&value)? == expected,
            "decision segment disagrees with authority"
        );
        count += 1;
        encoded_bytes += (key.len() + value.len()) as u64;
    }
    if indexed {
        let view = view.ok_or_else(|| anyhow::anyhow!("indexed decision state missing"))?;
        let bytes = manifest.ok_or_else(|| anyhow::anyhow!("decision manifest missing"))?;
        anyhow::ensure!(
            bytes.len() <= MAX_MANIFEST_BYTES,
            "decision manifest exceeds bound"
        );
        let expected = Manifest::of(&view, applied)?;
        let actual: Manifest = serde_json::from_slice(&bytes)?;
        anyhow::ensure!(
            actual == expected && serde_json::to_vec(&actual)? == bytes,
            "decision manifest disagrees with authority"
        );
        anyhow::ensure!(
            count == actual.segments && encoded_bytes == actual.encoded_bytes,
            "missing or extra decision segment"
        );
    } else {
        anyhow::ensure!(
            manifest.is_none(),
            "orphan or mixed-format decision manifest"
        );
    }
    Ok(())
}

/// Append new segments and replace only a growing partial tail in the same batch.
/// Snapshot install and explicit legacy migration rebuild the complete derived set.
pub(crate) fn stage(
    batch: &mut WriteBatch,
    before: Option<&D1State>,
    next: Option<&D1State>,
    applied: Option<LogId<NodeId>>,
    indexed: bool,
    replace: bool,
) -> anyhow::Result<Option<Vec<u8>>> {
    if replace {
        batch.delete_range(PREFIX, PREFIX_END);
        batch.delete(MANIFEST_KEY);
    }
    let Some(view) = next.and_then(View::of) else {
        return Ok(None);
    };
    let start = if indexed && !replace {
        let before = before
            .and_then(View::of)
            .ok_or_else(|| anyhow::anyhow!("decision history missing"))?;
        anyhow::ensure!(
            before.domain == view.domain && before.through <= view.through,
            "decision history changed"
        );
        // A complete previous tail is immutable; only a growing partial tail
        // is replaced. Commands which do not advance the cut write no rows.
        if before.through == view.through {
            view.through
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("decision ordinal overflow"))?
        } else {
            before.through / SEGMENT_TRANSACTIONS as u64 * SEGMENT_TRANSACTIONS as u64 + 1
        }
    } else {
        1
    };
    for first in (start..=view.through).step_by(SEGMENT_TRANSACTIONS) {
        batch.put(view.key(first), view.row(first)?.bytes()?);
    }
    let manifest = serde_json::to_vec(&Manifest::of(&view, applied)?)?;
    batch.put(MANIFEST_KEY, &manifest);
    Ok(Some(manifest))
}

/// Full immutable role, local identity and original coordinator authority.
/// Member terminal frontiers are A; coordinator terminal frontiers are R.
pub(crate) struct CheckpointIdentity {
    pub(crate) role: Role,
    pub(crate) authority: Authority,
    pub(crate) shard: u64,
    pub(crate) epoch: u64,
    pub(crate) mode: [u8; 16],
    pub(crate) cohort: Cohort,
}
impl CheckpointIdentity {
    fn domain(&self) -> anyhow::Result<Domain> {
        self.cohort
            .validate()
            .map_err(|e| anyhow::anyhow!("invalid decision cohort: {e:?}"))?;
        let role = match self.role {
            Role::Coordinator => {
                anyhow::ensure!(
                    self.shard == self.cohort.coordinator_shard
                        && self.epoch == self.cohort.coordinator_epoch
                        && self.authority.shard == self.shard
                        && self.authority.mode == self.mode,
                    "decision coordinator identity mismatch"
                );
                Role::Coordinator
            }
            Role::Member => {
                anyhow::ensure!(
                    self.cohort.members.iter().any(|m| m.shard == self.shard
                        && m.epoch == self.epoch
                        && m.mode == self.mode),
                    "decision member identity mismatch"
                );
                Role::Member
            }
        };
        anyhow::ensure!(
            self.mode != [0; 16]
                && self.authority.mode != [0; 16]
                && self.authority.shard == self.cohort.coordinator_shard,
            "decision authority mismatch"
        );
        Ok(Domain {
            role,
            shard: self.shard,
            epoch: self.epoch,
            mode: self.mode,
            authority: self.authority.clone(),
            cohort: self.cohort.clone(),
        })
    }
}

/// Key of the sole row an ordinary append may replace. Sealed rows are immutable.
pub(crate) fn checkpoint_tail_key(
    identity: &CheckpointIdentity,
    through: u64,
) -> anyhow::Result<Option<Vec<u8>>> {
    let domain = identity.domain()?;
    anyhow::ensure!(
        through <= domain.cohort.profile.max_transactions() as u64,
        "decision checkpoint exceeds transaction bound"
    );
    Ok(
        (!through.is_multiple_of(SEGMENT_TRANSACTIONS as u64)).then(|| {
            domain.key(through / SEGMENT_TRANSACTIONS as u64 * SEGMENT_TRANSACTIONS as u64 + 1)
        }),
    )
}

/// Validated private publication bytes and at most one raw-verified partial row.
/// The caller must obtain all supplied stored bytes from its one pinned snapshot
/// and the published manifest from its private cut, never from a disk assertion.
pub(crate) struct CheckedCheckpointTail {
    manifest: Manifest,
    tail: Option<Row>,
    tail_encoded_bytes: u64,
}

fn terminal(tx: &Transaction, ordinal: u64) -> anyhow::Result<Terminal> {
    anyhow::ensure!(tx.ordinal == ordinal, "decision ordinal mismatch");
    Ok(Terminal {
        id: tx.id,
        ordinal,
        participants: tx.participants.clone(),
        prepared: tx.prepared.clone(),
        decision: tx
            .decision
            .ok_or_else(|| anyhow::anyhow!("unresolved decision"))?,
    })
}

/// Validate the exact canonical manifest envelope for an independently verified
/// checkpoint. The encoded total comes from the private prior manifest and exact
/// changed-row accounting, not from a caller's untrusted disk claim.
pub(crate) fn validate_checkpoint_manifest(
    identity: &CheckpointIdentity,
    through: u64,
    applied: Option<LogId<NodeId>>,
    bytes: &[u8],
) -> anyhow::Result<()> {
    anyhow::ensure!(
        bytes.len() <= MAX_MANIFEST_BYTES,
        "decision manifest exceeds bound"
    );
    let domain = identity.domain()?;
    anyhow::ensure!(
        through <= domain.cohort.profile.max_transactions() as u64,
        "decision checkpoint exceeds transaction bound"
    );
    let manifest: Manifest = serde_json::from_slice(bytes)?;
    anyhow::ensure!(
        serde_json::to_vec(&manifest)? == bytes
            && manifest.version == domain.cohort.profile.storage_format()
            && manifest.domain == domain
            && manifest.through == through
            && manifest.segments == through.div_ceil(SEGMENT_TRANSACTIONS as u64)
            && manifest.applied == applied
            && (through != 0 || manifest.encoded_bytes == 0),
        "decision manifest disagrees with published checkpoint"
    );
    Ok(())
}

/// Incremental comparison of a drained raw prefix with the complete derived
/// namespace. At most one bounded terminal row is resident.
pub(crate) struct CheckpointRows<I> {
    rows: I,
    manifest: Manifest,
    current: Option<Row>,
    next_ordinal: u64,
    count: u64,
    encoded: u64,
}
impl<I> CheckpointRows<I>
where
    I: Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), rocksdb::Error>>,
{
    pub(crate) fn new(
        identity: &CheckpointIdentity,
        through: u64,
        applied: Option<LogId<NodeId>>,
        published: &[u8],
        rows: I,
    ) -> anyhow::Result<Self> {
        validate_checkpoint_manifest(identity, through, applied, published)?;
        Ok(Self {
            rows,
            manifest: serde_json::from_slice(published)?,
            current: None,
            next_ordinal: 1,
            count: 0,
            encoded: 0,
        })
    }

    pub(crate) fn observe(&mut self, tx: &Transaction) -> anyhow::Result<()> {
        if tx.ordinal > self.manifest.through {
            return Ok(());
        }
        anyhow::ensure!(
            tx.ordinal == self.next_ordinal,
            "decision projection ordinal gap"
        );
        if self.current.is_none() {
            anyhow::ensure!(
                self.count < self.manifest.segments,
                "extra decision projection"
            );
            let (key, value) = self
                .rows
                .next()
                .ok_or_else(|| anyhow::anyhow!("missing decision row"))??;
            let first = self.next_ordinal;
            anyhow::ensure!(
                key.as_ref() == self.manifest.domain.key(first),
                "foreign or missing decision row"
            );
            let row = Row::decode(&value)?;
            let length = (self.manifest.through - first + 1).min(SEGMENT_TRANSACTIONS as u64);
            anyhow::ensure!(
                row.version == self.manifest.version
                    && row.domain == self.manifest.domain
                    && row.first == first
                    && row.records.len() as u64 == length,
                "decision row header or length mismatch"
            );
            self.encoded = self
                .encoded
                .checked_add((key.len() + value.len()) as u64)
                .ok_or_else(|| anyhow::anyhow!("decision encoded overflow"))?;
            anyhow::ensure!(
                self.encoded <= self.manifest.encoded_bytes,
                "extra decision encoded bytes"
            );
            self.count += 1;
            self.current = Some(row);
        }
        let row = self.current.as_ref().unwrap();
        let offset = (tx.ordinal - row.first) as usize;
        anyhow::ensure!(
            row.records.get(offset) == Some(&terminal(tx, tx.ordinal)?),
            "decision row disagrees with original archive"
        );
        self.next_ordinal += 1;
        if offset + 1 == row.records.len() {
            self.current = None;
        }
        Ok(())
    }

    pub(crate) fn finish(mut self) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.next_ordinal - 1 == self.manifest.through
                && self.current.is_none()
                && self.count == self.manifest.segments
                && self.encoded == self.manifest.encoded_bytes,
            "incomplete decision namespace"
        );
        if let Some(item) = self.rows.next() {
            let (key, _) = item?;
            anyhow::ensure!(!key.starts_with(PREFIX), "extra or foreign decision row");
        }
        Ok(())
    }
}

/// Check the old partial tail against exact transactions projected from the raw
/// authority. No Journal or historical terminal collection is constructed here.
pub(crate) fn check_checkpoint_tail(
    identity: &CheckpointIdentity,
    through: u64,
    applied: Option<LogId<NodeId>>,
    published_manifest: &[u8],
    stored_manifest: &[u8],
    stored_tail: Option<&[u8]>,
    projected_tail: &[Transaction],
) -> anyhow::Result<CheckedCheckpointTail> {
    anyhow::ensure!(
        published_manifest.len() <= MAX_MANIFEST_BYTES && published_manifest == stored_manifest,
        "decision manifest changed"
    );
    validate_checkpoint_manifest(identity, through, applied, published_manifest)?;
    let domain = identity.domain()?;
    let manifest: Manifest = serde_json::from_slice(published_manifest)?;
    let key = checkpoint_tail_key(identity, through)?;
    let (tail, tail_encoded_bytes) = if let Some(key) = key {
        let count = (through % SEGMENT_TRANSACTIONS as u64) as usize;
        anyhow::ensure!(
            projected_tail.len() == count,
            "decision tail projection count"
        );
        let first = through - count as u64 + 1;
        let expected = Row {
            version: domain.cohort.profile.storage_format(),
            domain,
            first,
            records: projected_tail
                .iter()
                .enumerate()
                .map(|(offset, tx)| terminal(tx, first + offset as u64))
                .collect::<anyhow::Result<Vec<_>>>()?,
        };
        let bytes = stored_tail.ok_or_else(|| anyhow::anyhow!("decision tail missing"))?;
        anyhow::ensure!(Row::decode(bytes)? == expected, "decision tail changed");
        (Some(expected), (key.len() + bytes.len()) as u64)
    } else {
        anyhow::ensure!(
            stored_tail.is_none() && projected_tail.is_empty(),
            "unexpected decision tail"
        );
        (None, 0)
    };
    anyhow::ensure!(
        manifest.encoded_bytes >= tail_encoded_bytes
            && (through != 0 || manifest.encoded_bytes == 0),
        "invalid decision encoded total"
    );
    Ok(CheckedCheckpointTail {
        manifest,
        tail,
        tail_encoded_bytes,
    })
}

/// Exact manifest produced by checked incremental row staging. Its private
/// constructor prevents publication-cut minting from accepting caller-supplied
/// encoded totals. Identity, resolved prefix and full applied marker are carried
/// in the canonical bytes and checked against the candidate by the cut minter.
pub(crate) struct StagedCheckpointManifest {
    bytes: Vec<u8>,
}
impl StagedCheckpointManifest {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Stage only newly contiguous terminals and the growing partial tail. The
/// trusted old total is adjusted by exact changed-row sizes, without a historical
/// recount. Even an empty append refreshes the complete applied LogId.
/// All errors occur before the caller's atomic database publication.
pub(crate) fn stage_checkpoint(
    batch: &mut WriteBatch,
    before: &CheckedCheckpointTail,
    drained: &[Transaction],
    next_through: u64,
    applied: Option<LogId<NodeId>>,
) -> anyhow::Result<StagedCheckpointManifest> {
    let old = &before.manifest;
    anyhow::ensure!(
        next_through >= old.through
            && next_through <= old.domain.cohort.profile.max_transactions() as u64
            && next_through - old.through == drained.len() as u64,
        "decision checkpoint history changed"
    );
    let mut encoded_bytes = old.encoded_bytes;
    if !drained.is_empty() {
        encoded_bytes = encoded_bytes
            .checked_sub(before.tail_encoded_bytes)
            .ok_or_else(|| anyhow::anyhow!("decision size underflow"))?;
        let mut first = old.through / SEGMENT_TRANSACTIONS as u64 * SEGMENT_TRANSACTIONS as u64 + 1;
        let mut records = before
            .tail
            .as_ref()
            .map_or_else(Vec::new, |tail| tail.records.clone());
        for (offset, tx) in drained.iter().enumerate() {
            let ordinal = old.through + offset as u64 + 1;
            records.push(terminal(tx, ordinal)?);
            if records.len() == SEGMENT_TRANSACTIONS || ordinal == next_through {
                let row = Row {
                    version: old.version,
                    domain: old.domain.clone(),
                    first,
                    records,
                };
                let key = old.domain.key(first);
                let bytes = row.bytes()?;
                encoded_bytes = encoded_bytes
                    .checked_add((key.len() + bytes.len()) as u64)
                    .ok_or_else(|| anyhow::anyhow!("decision size overflow"))?;
                batch.put(key, bytes);
                first = ordinal + 1;
                records = Vec::new();
            }
        }
    }
    let manifest = Manifest {
        version: old.version,
        domain: old.domain.clone(),
        through: next_through,
        segments: next_through.div_ceil(SEGMENT_TRANSACTIONS as u64),
        encoded_bytes,
        applied,
    };
    let bytes = serde_json::to_vec(&manifest)?;
    anyhow::ensure!(
        bytes.len() <= MAX_MANIFEST_BYTES,
        "decision manifest exceeds bound"
    );
    batch.put(MANIFEST_KEY, &bytes);
    Ok(StagedCheckpointManifest { bytes })
}

/// A member manifest is bound to the exact state and raw staging witness, not
/// merely its A/checkpoint values. Only the member witness writer can mint it.
pub(crate) struct StagedMemberCheckpointManifest<'a> {
    manifest: StagedCheckpointManifest,
    state: &'a D1State,
    descriptor: &'a crate::state_machine::event_ledger::Descriptor,
}
impl<'a> StagedMemberCheckpointManifest<'a> {
    pub(crate) fn bytes(&self) -> &[u8] {
        self.manifest.bytes()
    }
    pub(crate) fn into_bytes(self) -> Vec<u8> {
        self.manifest.into_bytes()
    }
    pub(crate) fn state(&self) -> &'a D1State {
        self.state
    }
    pub(crate) fn descriptor(&self) -> &'a crate::state_machine::event_ledger::Descriptor {
        self.descriptor
    }
}

/// Member terminals advance through applied A, independently of observed R.
/// Accept only exact projections privately bound to the staged raw candidate.
pub(crate) fn stage_member_checkpoint<'a>(
    batch: &mut WriteBatch,
    before: &CheckedCheckpointTail,
    consumed: &crate::state_machine::event_ledger::StagedMemberTerminals<'a>,
    applied: Option<LogId<NodeId>>,
) -> anyhow::Result<StagedMemberCheckpointManifest<'a>> {
    let domain = &before.manifest.domain;
    let descriptor = consumed.descriptor();
    anyhow::ensure!(
        domain.role == Role::Member
            && consumed.previous() == before.manifest.through
            && serde_json::to_vec(&before.manifest)? == consumed.previous_manifest()
            && descriptor.domain.role == crate::state_machine::event_ledger::Role::Member
            && descriptor.domain.shard == domain.shard
            && descriptor.domain.epoch == domain.epoch
            && descriptor.domain.mode == domain.mode
            && descriptor.domain.authority == domain.authority
            && descriptor.domain.cohort == domain.cohort
            && descriptor.applied == applied,
        "member terminal checkpoint does not match raw witness"
    );
    let manifest = stage_checkpoint(
        batch,
        before,
        consumed.records(),
        consumed.through(),
        applied,
    )?;
    Ok(StagedMemberCheckpointManifest {
        manifest,
        state: consumed.state(),
        descriptor,
    })
}

fn checked_manifest(db: &DB, cached: &[u8], view: &View<'_>) -> Result<Manifest, D1Error> {
    if db
        .get(MANIFEST_KEY)
        .map_err(|_| D1Error::DecisionIndexUnavailable)?
        .as_deref()
        != Some(cached)
    {
        return Err(D1Error::DecisionIndexHistory);
    }
    let manifest: Manifest =
        serde_json::from_slice(cached).map_err(|_| D1Error::DecisionIndexHistory)?;
    if manifest.version != view.domain.cohort.profile.storage_format()
        || manifest.domain != view.domain
        || manifest.through != view.through
    {
        return Err(D1Error::DecisionIndexHistory);
    }
    Ok(manifest)
}
/// Summary reads the atomically published manifest, without fetching any segment.
pub(crate) fn frontier(state: &D1State, index: Option<(&DB, &[u8])>) -> Result<u64, D1Error> {
    let (history, role, through) = if let Some(history) = &state.coordinator {
        (history, Role::Coordinator, history.resolved_prefix())
    } else {
        let visibility = state
            .visibility
            .as_ref()
            .ok_or(D1Error::DecisionIndexHistory)?;
        (&visibility.journal, Role::Member, visibility.applied)
    };
    let identity = CheckpointIdentity {
        role,
        shard: state.shard,
        epoch: state.epoch,
        mode: state
            .managed
            .as_ref()
            .ok_or(D1Error::DecisionIndexHistory)?
            .mode_id,
        authority: Authority {
            shard: history
                .cohort()
                .ok_or(D1Error::DecisionIndexHistory)?
                .coordinator_shard,
            mode: history.authority(),
        },
        cohort: history
            .cohort()
            .ok_or(D1Error::DecisionIndexHistory)?
            .clone(),
    };
    if let Some((db, cached)) = index {
        if db
            .get(MANIFEST_KEY)
            .map_err(|_| D1Error::DecisionIndexUnavailable)?
            .as_deref()
            != Some(cached)
        {
            return Err(D1Error::DecisionIndexHistory);
        }
        let manifest: Manifest =
            serde_json::from_slice(cached).map_err(|_| D1Error::DecisionIndexHistory)?;
        validate_checkpoint_manifest(&identity, through, manifest.applied, cached)
            .map_err(|_| D1Error::DecisionIndexHistory)?;
    }
    Ok(through)
}
/// Fetch only segments intersecting 1..=version, checking every loaded row against
/// that exact authority. An old partial tail is trimmed without changing old V.
pub(crate) fn prefix(
    state: &D1State,
    version: u64,
    index: Option<(&DB, &[u8])>,
) -> Result<TerminalPrefix, D1Error> {
    let view = View::of(state).ok_or(D1Error::DecisionIndexHistory)?;
    if version > view.through {
        return Err(D1Error::VisibilityNotApplied);
    }
    let Some((db, cached)) = index else {
        return view
            .journal
            .terminal_prefix(version)
            .map_err(|_| D1Error::DecisionIndexHistory);
    };
    checked_manifest(db, cached, &view)?;
    let mut segments = Vec::new();
    for first in (1..=version).step_by(SEGMENT_TRANSACTIONS) {
        let bytes = db
            .get(view.key(first))
            .map_err(|_| D1Error::DecisionIndexUnavailable)?
            .ok_or(D1Error::DecisionIndexHistory)?;
        let mut row = Row::decode(&bytes).map_err(|_| D1Error::DecisionIndexHistory)?;
        if row != view.row(first).map_err(|_| D1Error::DecisionIndexHistory)? {
            return Err(D1Error::DecisionIndexHistory);
        }
        row.records
            .truncate((version - first + 1).min(SEGMENT_TRANSACTIONS as u64) as usize);
        segments.push(row.records);
    }
    TerminalPrefix::from_segments(
        view.journal.authority(),
        view.domain.cohort,
        version,
        segments,
    )
    .map_err(|_| D1Error::DecisionIndexHistory)
}

/// Ordinary apply must not conceal corruption by rewriting an existing partial
/// tail. Sealed segments are never rewritten except by explicit snapshot install.
pub(crate) fn check_tail(db: &DB, state: &D1State) -> anyhow::Result<()> {
    let view = View::of(state).ok_or_else(|| anyhow::anyhow!("decision domain missing"))?;
    if view.through != 0 && !view.through.is_multiple_of(SEGMENT_TRANSACTIONS as u64) {
        let first = view.through / SEGMENT_TRANSACTIONS as u64 * SEGMENT_TRANSACTIONS as u64 + 1;
        let bytes = db
            .get(view.key(first))?
            .ok_or_else(|| anyhow::anyhow!("decision tail missing"))?;
        anyhow::ensure!(
            Row::decode(&bytes)? == view.row(first)?,
            "decision tail changed"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::{Decision, Member, Participant, MAX_PARTICIPANTS};

    fn byte_state(count: u64) -> D1State {
        use crate::transaction_decisions::{
            CapacityActivation, CapacityProfile, Command, MemberCapacityGrant,
        };
        let cohort = Cohort {
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: vec![Member {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
            }],
            profile: CapacityProfile::ByteBound16MiBV1,
        };
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .replay_event(Command::BindCohort {
                cohort: cohort.clone(),
            })
            .unwrap();
        journal
            .replay_event(Command::ConfirmCohortCapacity {
                grants: vec![MemberCapacityGrant {
                    member: cohort.members[0].clone(),
                    activation: CapacityActivation {
                        nonce: [3; 32],
                        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 0), 1),
                    },
                }],
                cohort,
            })
            .unwrap();
        for n in 1..=count {
            let mut id = [1; 16];
            id[..8].copy_from_slice(&n.to_be_bytes());
            journal
                .replay_event(Command::Begin {
                    id,
                    participants: vec![Participant {
                        shard: 7,
                        epoch: 1,
                        mode: [7; 16],
                        intent: [8; 32],
                    }],
                })
                .unwrap();
            journal
                .replay_event(Command::Decide {
                    id,
                    decision: Decision::Abort,
                })
                .unwrap();
        }
        let mut state = D1State::new(9, 1);
        state.version = 4;
        state.managed = Some(crate::D1ManagedMode {
            mode_id: [9; 16],
            activated_sequence: 0,
        });
        state.coordinator = Some(journal.into());
        state
    }

    fn checkpoint_identity(view: &View<'_>) -> CheckpointIdentity {
        CheckpointIdentity {
            role: Role::Coordinator,
            authority: view.domain.authority.clone(),
            shard: view.domain.shard,
            epoch: view.domain.epoch,
            mode: view.domain.mode,
            cohort: view.domain.cohort.clone(),
        }
    }

    #[test]
    fn checkpoint_terminal_append_matches_full_codec_and_preserves_sealed_rows() {
        for (old_count, next_count) in [(0, 0), (0, 1), (1, 31), (31, 32), (32, 33), (33, 65)] {
            let old = byte_state(old_count);
            let next = byte_state(next_count);
            let old_view = View::of(&old).unwrap();
            let next_view = View::of(&next).unwrap();
            let identity = checkpoint_identity(&old_view);
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let mut initial = WriteBatch::default();
            let published = stage(&mut initial, None, Some(&old), None, false, true)
                .unwrap()
                .unwrap();
            db.write(initial).unwrap();
            let stored = db.get(MANIFEST_KEY).unwrap().unwrap();
            let tail_key = checkpoint_tail_key(&identity, old_count).unwrap();
            let tail = tail_key.as_ref().map(|key| db.get(key).unwrap().unwrap());
            let tail_count = old_count % SEGMENT_TRANSACTIONS as u64;
            let projected: Vec<_> = (old_count - tail_count + 1..=old_count)
                .map(|n| old_view.journal.transaction_at(n).unwrap().clone())
                .collect();
            let checked = check_checkpoint_tail(
                &identity,
                old_count,
                None,
                &published,
                &stored,
                tail.as_deref(),
                &projected,
            )
            .unwrap();
            let drained: Vec<_> = (old_count + 1..=next_count)
                .map(|n| next_view.journal.transaction_at(n).unwrap().clone())
                .collect();
            let sealed: Vec<_> = (1..=old_count / 32)
                .map(|n| {
                    let key = old_view.key((n - 1) * 32 + 1);
                    let value = db.get(&key).unwrap().unwrap();
                    (key, value)
                })
                .collect();
            let applied = Some(LogId::new(openraft::CommittedLeaderId::new(3, 2), 99));
            let mut batch = WriteBatch::default();
            let manifest =
                stage_checkpoint(&mut batch, &checked, &drained, next_count, applied).unwrap();
            assert_eq!(manifest.bytes(), manifest_bytes(&next, applied).unwrap());
            validate_checkpoint_manifest(&identity, next_count, applied, manifest.bytes()).unwrap();
            assert_eq!(
                manifest.into_bytes(),
                manifest_bytes(&next, applied).unwrap()
            );
            db.write(batch).unwrap();
            for (key, value) in sealed {
                assert_eq!(db.get(key).unwrap().unwrap(), value);
            }
            validate(&db, Some(&next), applied, true).unwrap();
        }
    }

    #[test]
    fn checkpoint_tail_and_manifest_substitution_fail_closed() {
        let old = byte_state(1);
        let view = View::of(&old).unwrap();
        let identity = checkpoint_identity(&view);
        let published = manifest_bytes(&old, None).unwrap();
        let tail = view.row(1).unwrap().bytes().unwrap();
        let projected = vec![view.journal.transaction_at(1).unwrap().clone()];
        assert!(check_checkpoint_tail(
            &identity, 1, None, &published, &published, None, &projected,
        )
        .is_err());
        let mut wrong_tx = projected.clone();
        wrong_tx[0].id = [44; 16];
        assert!(check_checkpoint_tail(
            &identity,
            1,
            None,
            &published,
            &published,
            Some(&tail),
            &wrong_tx,
        )
        .is_err());
        let wrong_applied = Some(LogId::new(openraft::CommittedLeaderId::new(2, 0), 4));
        assert!(validate_checkpoint_manifest(&identity, 1, wrong_applied, &published).is_err());
        let mut noncanonical = published.clone();
        noncanonical.push(b' ');
        assert!(validate_checkpoint_manifest(&identity, 1, None, &noncanonical).is_err());
        let mut different_identity = checkpoint_identity(&view);
        different_identity.mode = [77; 16];
        assert!(validate_checkpoint_manifest(&different_identity, 1, None, &published).is_err());
        let checked = check_checkpoint_tail(
            &identity,
            1,
            None,
            &published,
            &published,
            Some(&tail),
            &projected,
        )
        .unwrap();
        let mut batch = WriteBatch::default();
        assert!(stage_checkpoint(&mut batch, &checked, &[], 2, None).is_err());
        let mut unresolved = projected;
        unresolved[0].ordinal = 2;
        unresolved[0].decision = None;
        assert!(stage_checkpoint(&mut batch, &checked, &unresolved, 2, None).is_err());
    }

    #[test]
    fn byte_profile_terminal_segments_reopen_preserve_old_cuts_and_reject_substitution() {
        let state = byte_state(1025);
        let view = View::of(&state).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let manifest = {
            let db = DB::open_default(dir.path()).unwrap();
            let mut batch = WriteBatch::default();
            let manifest = stage(&mut batch, None, Some(&state), None, false, true)
                .unwrap()
                .unwrap();
            db.write(batch).unwrap();
            assert_eq!(
                serde_json::from_slice::<Manifest>(&manifest)
                    .unwrap()
                    .version,
                2
            );
            assert_eq!(&view.key(1)[PREFIX.len()..PREFIX.len() + 4], &[0, 0, 0, 2]);
            manifest
        };
        let db = DB::open_default(dir.path()).unwrap();
        validate(&db, Some(&state), None, true).unwrap();
        assert_eq!(frontier(&state, Some((&db, &manifest))).unwrap(), 1025);
        for cut in [0, 1, 32, 256, 1024, 1025] {
            assert_eq!(
                prefix(&state, cut, Some((&db, &manifest))).unwrap(),
                state
                    .coordinator
                    .as_ref()
                    .unwrap()
                    .full()
                    .unwrap()
                    .terminal_prefix(cut)
                    .unwrap()
            );
        }
        for fault in 0..6 {
            let mut batch = WriteBatch::default();
            stage(&mut batch, None, Some(&state), None, false, true).unwrap();
            db.write(batch).unwrap();
            match fault {
                0 => {
                    db.delete(view.key(1025)).unwrap();
                }
                1 => {
                    let mut row = view.row(1).unwrap();
                    row.version = 1;
                    db.put(view.key(1), row.bytes().unwrap()).unwrap();
                }
                2 => {
                    let mut row = view.row(1).unwrap();
                    row.domain.cohort.profile =
                        crate::capacity_policy::CapacityProfile::LegacyCountV1;
                    db.put(view.key(1), row.bytes().unwrap()).unwrap();
                }
                3 => {
                    let mut key = view.key(1);
                    key[PREFIX.len() + 3] = 1;
                    db.put(key, view.row(1).unwrap().bytes().unwrap()).unwrap();
                }
                4 => {
                    let mut bad: Manifest = serde_json::from_slice(&manifest).unwrap();
                    bad.version = 1;
                    db.put(MANIFEST_KEY, serde_json::to_vec(&bad).unwrap())
                        .unwrap();
                }
                5 => {
                    db.put(view.key(1057), b"{}").unwrap();
                }
                _ => unreachable!(),
            }
            assert!(
                validate(&db, Some(&state), None, true).is_err(),
                "fault {fault}"
            );
            if fault != 3 && fault != 5 {
                assert!(
                    prefix(&state, 1025, Some((&db, &manifest))).is_err(),
                    "fault {fault}"
                );
            }
        }
    }

    #[test]
    fn maximum_width_schema_fits_segment_bound_without_a_new_quota() {
        for profile in [
            crate::capacity_policy::CapacityProfile::LegacyCountV1,
            crate::capacity_policy::CapacityProfile::ByteBound16MiBV1,
        ] {
            // Worst-width integer and byte encodings, maximum cohort/participants,
            // all false bits (longer than true), and a full 32-record segment. This
            // deliberately overestimates the profile-derived real ordinal ceiling.
            let cohort = Cohort {
                profile,
                coordinator_shard: 0,
                coordinator_epoch: u64::MAX,
                members: (0..MAX_PARTICIPANTS as u64)
                    .map(|n| Member {
                        shard: u64::MAX - MAX_PARTICIPANTS as u64 + n,
                        epoch: u64::MAX,
                        mode: [255; 16],
                    })
                    .collect(),
            };
            let domain = Domain {
                role: Role::Coordinator,
                shard: u64::MAX,
                epoch: u64::MAX,
                mode: [255; 16],
                authority: Authority {
                    shard: u64::MAX,
                    mode: [255; 16],
                },
                cohort,
            };
            let participants: Vec<_> = domain
                .cohort
                .members
                .iter()
                .map(|m| Participant {
                    shard: m.shard,
                    epoch: m.epoch,
                    mode: m.mode,
                    intent: [255; 32],
                })
                .collect();
            let row = Row {
                version: profile.storage_format(),
                domain: domain.clone(),
                first: u64::MAX,
                records: (0..SEGMENT_TRANSACTIONS)
                    .map(|_| Terminal {
                        id: [255; 16],
                        ordinal: u64::MAX,
                        participants: participants.clone(),
                        prepared: vec![false; MAX_PARTICIPANTS],
                        decision: Decision::Abort,
                    })
                    .collect(),
            };
            assert!(row.bytes().unwrap().len() < MAX_SEGMENT_BYTES);
            let manifest = Manifest {
                version: profile.storage_format(),
                domain,
                through: u64::MAX,
                segments: u64::MAX,
                encoded_bytes: u64::MAX,
                applied: Some(LogId::new(
                    openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
                    u64::MAX,
                )),
            };
            assert!(serde_json::to_vec(&manifest).unwrap().len() < MAX_MANIFEST_BYTES);
        }
    }
}
