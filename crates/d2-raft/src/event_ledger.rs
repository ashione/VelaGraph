//! Sole persistent live Journal authority for fixed-cohort disk formats.
//! Raw commands are replayed with the original admission and reservation rules.
//! The full namespace is closed: no derived index or archived snapshot is a fallback.
use crate::d1::D1State;
use crate::participant_writes::Authority;
use crate::request_index::{RecoveredRequest, RequestLookup};
use crate::transaction_decisions::checkpoint::{ActiveCheckpoint, LookupKey, LookupResult};
use crate::transaction_decisions::{
    CapacityProfile, Cohort, Command, HistoryCursor, HistoryPage, Journal, RequestRecord,
    Transaction, MAX_HISTORY_PAGE_BYTES, MAX_HISTORY_PAGE_EVENTS, MAX_JOURNAL_BYTES,
    MAX_REQUEST_EVENT_BYTES,
};
use crate::{D1Error, NodeId};
use openraft::LogId;
use rocksdb::{Direction, IteratorMode, Snapshot, WriteBatch, DB};
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::Arc;

pub(super) const PREFIX: &[u8] = b"sm/ledger/";
const PREFIX_END: &[u8] = b"sm/ledger0";
// Every byte-full nonfinal row contains > (4 MiB - 2 MiB) event bytes;
// every other nonfinal row contains 32 events. Both charge existing Journal
// limits, not a new admission quota. The final +1 covers the unsealed tail.
fn max_segments(profile: CapacityProfile) -> u64 {
    profile
        .max_history_events()
        .div_ceil(MAX_HISTORY_PAGE_EVENTS as u64)
        + (MAX_JOURNAL_BYTES / (MAX_HISTORY_PAGE_BYTES - MAX_REQUEST_EVENT_BYTES)) as u64
        + 1
}
// Worst-width JSON: a Member is <= 160 bytes; 64 members plus commas
// consume <= 10,304. The remaining domain fields are < 512, both root arrays
// <= 258, row fields/32 event separators < 256, and a binary key < 128.
// Their sum < 11,458, so 16 KiB covers key + row overhead for every legal row.
// Tests pin the actual maximum-width representation and all Command variants.
const MAX_ROW_OVERHEAD: usize = 16 * 1024;
const MAX_ROW_BYTES: usize = MAX_HISTORY_PAGE_BYTES + MAX_ROW_OVERHEAD;
fn max_encoded_bytes(profile: CapacityProfile) -> u64 {
    MAX_JOURNAL_BYTES as u64 + max_segments(profile) * MAX_ROW_OVERHEAD as u64
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Role {
    Coordinator,
    Member,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Domain {
    pub(super) role: Role,
    pub(super) shard: u64,
    pub(super) epoch: u64,
    pub(super) mode: [u8; 16],
    pub(super) authority: Authority,
    pub(super) cohort: Cohort,
}
impl Domain {
    fn validate(&self) -> anyhow::Result<()> {
        self.cohort
            .validate()
            .map_err(|e| anyhow::anyhow!("invalid ledger cohort: {e:?}"))?;
        anyhow::ensure!(
            self.mode != [0; 16]
                && self.authority.mode != [0; 16]
                && self.authority.shard == self.cohort.coordinator_shard,
            "invalid ledger authority"
        );
        let matches = match self.role {
            Role::Coordinator => {
                self.shard == self.authority.shard
                    && self.mode == self.authority.mode
                    && self.epoch == self.cohort.coordinator_epoch
            }
            Role::Member => self.cohort.members.iter().any(|member| {
                member.shard == self.shard && member.epoch == self.epoch && member.mode == self.mode
            }),
        };
        anyhow::ensure!(matches, "ledger local identity outside cohort");
        Ok(())
    }
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
    fn zero_cursor(&self) -> anyhow::Result<HistoryCursor> {
        HistoryCursor::empty(self.authority.clone(), self.cohort.clone()).map_err(history_error)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Descriptor {
    pub(super) version: u32,
    pub(super) domain: Domain,
    pub(super) event_count: u64,
    pub(super) segment_count: u64,
    pub(super) encoded_bytes: u64,
    pub(super) head_root: [u8; 32],
    #[serde(deserialize_with = "required_applied")]
    pub(super) applied: Option<LogId<NodeId>>,
}
fn required_applied<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<LogId<NodeId>>, D::Error> {
    Option::deserialize(deserializer)
}
impl Descriptor {
    pub(super) fn validate(&self) -> anyhow::Result<()> {
        self.domain.validate()?;
        anyhow::ensure!(
            self.version == self.domain.cohort.profile.storage_format()
                && (1..=self.domain.cohort.profile.max_history_events())
                    .contains(&self.event_count)
                && (1..=max_segments(self.domain.cohort.profile)).contains(&self.segment_count)
                && self.segment_count <= self.event_count
                && self.event_count <= self.segment_count * MAX_HISTORY_PAGE_EVENTS as u64
                && self.encoded_bytes > 0
                && self.encoded_bytes <= max_encoded_bytes(self.domain.cohort.profile),
            "invalid ledger descriptor bounds"
        );
        Ok(())
    }
    pub(super) fn validate_identity(
        &self,
        shard: u64,
        epoch: u64,
        mode: [u8; 16],
        role: Role,
    ) -> anyhow::Result<()> {
        self.validate()?;
        anyhow::ensure!(
            self.domain.shard == shard
                && self.domain.epoch == epoch
                && self.domain.mode == mode
                && self.domain.role == role,
            "ledger descriptor disagrees with state identity"
        );
        Ok(())
    }
}
struct View<'a> {
    domain: Domain,
    journal: &'a Journal,
}
impl<'a> View<'a> {
    fn of(state: &'a D1State) -> Option<Self> {
        let (role, journal) = match &state.coordinator {
            Some(coordinator) => (Role::Coordinator, coordinator.full()?),
            None => (Role::Member, &state.visibility.as_ref()?.journal),
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
        })
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    version: u32,
    domain: Domain,
    first: u64,
    before_root: [u8; 32],
    after_root: [u8; 32],
    #[serde(deserialize_with = "deserialize_events")]
    events: Vec<Command>,
}
impl Row {
    fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(bytes.len() <= MAX_ROW_BYTES, "ledger row exceeds bound");
        let row: Self = serde_json::from_slice(bytes)?;
        anyhow::ensure!(
            row.version == row.domain.cohort.profile.storage_format()
                && !row.events.is_empty()
                && serde_json::to_vec(&row)? == bytes,
            "noncanonical ledger row"
        );
        Ok(row)
    }
}
// Share the bounded decoder with Cohort and Journal rather than maintaining a
// second collection decoder with subtly different excess-item behavior.
fn deserialize_events<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Command>, D::Error> {
    crate::capacity_policy::deserialize_bounded_vec(d, MAX_HISTORY_PAGE_EVENTS)
}
fn history_error(error: crate::transaction_decisions::Error) -> anyhow::Error {
    anyhow::anyhow!("invalid ledger history: {error:?}")
}
fn journal(authority: [u8; 16]) -> anyhow::Result<Journal> {
    Journal::new(authority).map_err(history_error)
}
fn replay(journal: &mut Journal, event: Command) -> anyhow::Result<()> {
    journal.replay_event(event).map_err(history_error)
}
fn event_size(event: &Command) -> anyhow::Result<usize> {
    let bytes = serde_json::to_vec(event)?.len();
    anyhow::ensure!(
        bytes <= MAX_REQUEST_EVENT_BYTES,
        "ledger event exceeds existing bound"
    );
    Ok(bytes)
}
fn fits(count: usize, bytes: usize, next_bytes: usize) -> bool {
    count < MAX_HISTORY_PAGE_EVENTS && next_bytes <= MAX_HISTORY_PAGE_BYTES - bytes
}
struct Tail {
    events: Vec<Command>,
    first: u64,
    before: HistoryCursor,
    count: usize,
    event_bytes: usize,
    encoded_bytes: u64,
}
/// Raw authority validated by one load under the caller's state lock. Neither
/// the reconstructed Journal nor its descriptor can be mutated through this proof.
pub(super) struct Loaded {
    journal: Journal,
    descriptor: Descriptor,
    tail: Tail,
}
impl Loaded {
    pub(super) fn journal(&self) -> &Journal {
        &self.journal
    }

    pub(super) fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    /// Hydration must use the exact reconstructed Journal and complete domain.
    /// The raw load already checked every event root and the full applied marker;
    /// comparing the complete Journal also checks all replay-derived state.
    pub(super) fn validate_hydrated_state(&self, state: &D1State) -> anyhow::Result<()> {
        let descriptor = self.descriptor();
        state.validate_capacity_activation(descriptor.applied)?;
        let view = View::of(state).ok_or_else(|| anyhow::anyhow!("ledger state is not bound"))?;
        anyhow::ensure!(
            descriptor.domain == view.domain && self.journal() == view.journal,
            "ledger authority disagrees with hydrated state"
        );
        Ok(())
    }
}

/// A descriptor proven by staging this exact immutable candidate. This borrow
/// is operation-local, never a deserialized token or a cache across state locks.
pub(super) struct Staged<'a> {
    candidate: &'a D1State,
    descriptor: Descriptor,
}
impl<'a> Staged<'a> {
    fn new(candidate: &'a D1State, descriptor: Descriptor) -> anyhow::Result<Self> {
        descriptor.validate()?;
        Ok(Self {
            candidate,
            descriptor,
        })
    }

    /// Even an equal clone is not the candidate whose history was staged.
    pub(super) fn descriptor_for(&self, state: &D1State) -> anyhow::Result<&Descriptor> {
        anyhow::ensure!(
            std::ptr::eq(self.candidate, state),
            "ledger proof belongs to a different candidate"
        );
        state.validate_capacity_activation(self.descriptor.applied)?;
        Ok(&self.descriptor)
    }
}

/// One structural walker owns all raw namespace, canonical partition, and hash
/// closure checks. Observers may collect semantic replay or one bounded result;
/// neither can return a successful result before the entire scan closes.
fn walk(
    snapshot: &Snapshot<'_>,
    descriptor: &Descriptor,
    mut observe: impl FnMut(Command, &HistoryCursor, usize) -> anyhow::Result<()>,
) -> anyhow::Result<Tail> {
    descriptor.validate()?;
    let applied: Option<LogId<NodeId>> = match snapshot.get(super::APPLIED_KEY)? {
        Some(bytes) => serde_json::from_slice(&bytes)?,
        None => None,
    };
    anyhow::ensure!(
        applied == descriptor.applied,
        "ledger applied marker mismatch"
    );
    let mut cursor = descriptor.domain.zero_cursor()?;
    let mut count = 0u64;
    let mut encoded_bytes = 0u64;
    let mut canonical_bytes = 0usize;
    let mut tail: Option<Tail> = None;
    for item in snapshot.iterator(IteratorMode::From(PREFIX, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(PREFIX) {
            break;
        }
        anyhow::ensure!(count < descriptor.segment_count, "extra ledger row");
        let first = cursor
            .event_count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("ledger event position overflow"))?;
        anyhow::ensure!(
            key.as_ref() == descriptor.domain.key(first),
            "ledger key gap or foreign domain"
        );
        let row_bytes = key
            .len()
            .checked_add(value.len())
            .ok_or_else(|| anyhow::anyhow!("ledger row byte overflow"))?;
        encoded_bytes = encoded_bytes
            .checked_add(row_bytes as u64)
            .ok_or_else(|| anyhow::anyhow!("ledger byte total overflow"))?;
        anyhow::ensure!(
            encoded_bytes <= descriptor.encoded_bytes,
            "extra ledger bytes"
        );
        let row = Row::decode(&value)?;
        let remaining = descriptor
            .event_count
            .checked_sub(cursor.event_count)
            .ok_or_else(|| anyhow::anyhow!("extra ledger event"))?;
        anyhow::ensure!(
            row.domain == descriptor.domain
                && row.first == first
                && row.before_root == cursor.root
                && row.events.len() as u64 <= remaining,
            "ledger row header mismatch"
        );
        let mut bytes = 0usize;
        for event in &row.events {
            bytes = bytes
                .checked_add(event_size(event)?)
                .filter(|bytes| *bytes <= MAX_HISTORY_PAGE_BYTES)
                .ok_or_else(|| anyhow::anyhow!("ledger event bytes exceed row bound"))?;
        }
        canonical_bytes = canonical_bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= MAX_JOURNAL_BYTES)
            .ok_or_else(|| anyhow::anyhow!("ledger canonical bytes exceed lifetime bound"))?;
        if let Some(previous) = &tail {
            anyhow::ensure!(
                !fits(
                    previous.count,
                    previous.event_bytes,
                    event_size(&row.events[0])?
                ),
                "noncanonical ledger partition"
            );
        }
        let row_count = row.events.len();
        let before = cursor.clone();
        let original_events = row.events.clone();
        for event in row.events {
            let size = event_size(&event)?;
            cursor = cursor
                .extend(std::slice::from_ref(&event))
                .map_err(history_error)?;
            observe(event, &cursor, size)?;
        }
        anyhow::ensure!(cursor.root == row.after_root, "ledger row root mismatch");
        tail = Some(Tail {
            events: original_events,
            first: row.first,
            before,
            count: row_count,
            event_bytes: bytes,
            encoded_bytes: row_bytes as u64,
        });
        count = count
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("ledger row count overflow"))?;
    }
    anyhow::ensure!(
        count == descriptor.segment_count
            && encoded_bytes == descriptor.encoded_bytes
            && cursor.event_count == descriptor.event_count
            && cursor.root == descriptor.head_root,
        "incomplete or substituted ledger history"
    );
    tail.ok_or_else(|| anyhow::anyhow!("missing cohort binding event"))
}

/// Strict cold/writer replay shares the raw walker, but still reconstructs and
/// validates every semantic transition. A self-consistent forged descriptor is
/// never a substitute for this validation and cannot mint a published cut.
pub(super) fn load(db: &DB, descriptor: &Descriptor) -> anyhow::Result<Loaded> {
    #[cfg(test)]
    crate::transaction_decisions::assert_full_journal_allowed();
    let snapshot = db.snapshot();
    let mut journal = journal(descriptor.domain.authority.mode)?;
    let tail = walk(&snapshot, descriptor, |event, _, _| {
        replay(&mut journal, event)
    })?;
    anyhow::ensure!(
        journal.cohort() == Some(&descriptor.domain.cohort),
        "ledger replay cohort mismatch"
    );
    Ok(Loaded {
        journal,
        descriptor: descriptor.clone(),
        tail,
    })
}

/// An integrity expectation minted from one strictly validated, published
/// coordinator state. It contains no Journal, events or drained transaction collection,
/// is not serializable, and has no descriptor-only or public constructor.
/// SHA-256 equality proves integrity relative to this cut, not authentication.
#[derive(Debug)]
pub(super) struct PublishedArchiveCut {
    descriptor: Descriptor,
    metadata: Vec<u8>,
    decision_manifest: Vec<u8>,
    resolved_prefix: u64,
    checkpoint: Arc<ActiveCheckpoint>,
}
impl Loaded {
    /// Caller must first complete typed metadata and derived namespace checks,
    /// and publish this witness under the same state lock as that exact state.
    pub(super) fn published_cut(
        &self,
        state: &D1State,
        metadata: &[u8],
        decision_manifest: &[u8],
    ) -> anyhow::Result<PublishedArchiveCut> {
        self.validate_hydrated_state(state)?;
        let staged = Staged::new(state, self.descriptor.clone())?;
        staged.published_cut(state, metadata, decision_manifest)
    }
}
impl Staged<'_> {
    /// Prepare alongside the exact raw/metadata/index/applied batch. The caller
    /// may publish this value only after syncing that batch, under its state lock;
    /// a failed write must discard it. All fallible work precedes disk commit.
    pub(super) fn published_cut(
        &self,
        state: &D1State,
        metadata: &[u8],
        decision_manifest: &[u8],
    ) -> anyhow::Result<PublishedArchiveCut> {
        let descriptor = self.descriptor_for(state)?;
        anyhow::ensure!(
            descriptor.domain.role == Role::Coordinator,
            "archive recovery cut requires a bound coordinator"
        );
        let journal = state
            .coordinator
            .as_ref()
            .and_then(|coordinator| coordinator.full())
            .ok_or_else(|| anyhow::anyhow!("missing coordinator archive"))?;
        let checkpoint = ActiveCheckpoint::from_journal(journal).map_err(history_error)?;
        if super::state_codec::d1_metadata_bytes(state, Some(self))? != metadata {
            super::state_codec::validate_active_metadata(metadata, descriptor, &checkpoint)?;
        }
        anyhow::ensure!(
            crate::decision_segments::manifest_bytes(state, descriptor.applied)?
                == decision_manifest,
            "published archive decision manifest differs from staged state"
        );
        Ok(PublishedArchiveCut {
            descriptor: descriptor.clone(),
            metadata: metadata.to_vec(),
            decision_manifest: decision_manifest.to_vec(),
            resolved_prefix: journal.resolved_prefix(),
            checkpoint: Arc::new(checkpoint),
        })
    }
}
impl PublishedArchiveCut {
    pub(super) fn checkpoint(&self) -> &ActiveCheckpoint {
        self.checkpoint.as_ref()
    }

    pub(super) fn checkpoint_arc(&self) -> &Arc<ActiveCheckpoint> {
        &self.checkpoint
    }

    pub(super) fn descriptor(&self) -> &Descriptor {
        &self.descriptor
    }

    fn snapshot<'a>(&self, db: &'a DB) -> Result<Snapshot<'a>, D1Error> {
        let snapshot = db.snapshot();
        if snapshot
            .get(super::D1_KEY)
            .map_err(|_| D1Error::EventLedgerUnavailable)?
            .as_deref()
            != Some(self.metadata.as_slice())
        {
            return Err(D1Error::EventLedgerHistory);
        }
        if snapshot
            .get(crate::decision_segments::MANIFEST_KEY)
            .map_err(|_| D1Error::DecisionIndexUnavailable)?
            .as_deref()
            != Some(self.decision_manifest.as_slice())
        {
            return Err(D1Error::DecisionIndexHistory);
        }
        crate::request_index::reject_retired_snapshot(&snapshot).map_err(|error| {
            if error.is::<rocksdb::Error>() {
                D1Error::RequestIndexUnavailable
            } else {
                D1Error::RequestIndexHistory
            }
        })?;
        Ok(snapshot)
    }

    fn head(&self) -> HistoryCursor {
        HistoryCursor {
            version: self.descriptor.domain.cohort.profile.history_version(),
            authority: self.descriptor.domain.authority.clone(),
            cohort: self.descriptor.domain.cohort.clone(),
            event_count: self.descriptor.event_count,
            root: self.descriptor.head_root,
        }
    }
}

/// An operation-local snapshot verified against the private publication witness.
/// It retains only the last bounded raw row, never a replayed Journal.
pub(super) struct VerifiedArchive<'db, 'cut> {
    snapshot: Snapshot<'db>,
    cut: &'cut PublishedArchiveCut,
    tail: Tail,
}

pub(super) fn verify_active<'db, 'cut>(
    db: &'db DB,
    cut: &'cut PublishedArchiveCut,
) -> Result<VerifiedArchive<'db, 'cut>, D1Error> {
    let snapshot = cut.snapshot(db)?;
    cut.checkpoint
        .validate()
        .map_err(|_| D1Error::EventLedgerHistory)?;
    let tail =
        walk(&snapshot, &cut.descriptor, |_, _, _| Ok(())).map_err(super::ledger_read_error)?;
    Ok(VerifiedArchive {
        snapshot,
        cut,
        tail,
    })
}

impl VerifiedArchive<'_, '_> {
    pub(super) fn checkpoint(&self) -> &ActiveCheckpoint {
        &self.cut.checkpoint
    }

    pub(super) fn descriptor(&self) -> &Descriptor {
        &self.cut.descriptor
    }

    pub(super) fn get(&self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        Ok(self.snapshot.get(key)?)
    }

    pub(super) fn decision_manifest(&self) -> &[u8] {
        &self.cut.decision_manifest
    }

    /// Bounded ordinal projection for the prior terminal tail. Every raw row
    /// still closes against the same cut, including rows after this interval.
    pub(super) fn transactions(
        &self,
        first: u64,
        last: u64,
        overlay: &[Command],
    ) -> anyhow::Result<Vec<Transaction>> {
        anyhow::ensure!(
            first > 0 && last >= first && last - first < 32,
            "archive transaction projection exceeds terminal tail bound"
        );
        self.check_overlay(overlay)?;
        let mut ordinal = 0u64;
        let mut result: Vec<Transaction> = Vec::new();
        let mut observe = |event: &Command| -> anyhow::Result<()> {
            match event {
                Command::Begin { id, participants }
                | Command::AdmitRequest {
                    transaction_id: id,
                    participants,
                    ..
                } => {
                    ordinal = ordinal
                        .checked_add(1)
                        .ok_or_else(|| anyhow::anyhow!("archive ordinal overflow"))?;
                    if (first..=last).contains(&ordinal) {
                        result.push(Transaction {
                            id: *id,
                            ordinal,
                            participants: participants.clone(),
                            prepared: vec![false; participants.len()],
                            decision: None,
                        });
                    }
                }
                Command::Prepared { id, participant } => {
                    if let Some(tx) = result.iter_mut().find(|tx| tx.id == *id) {
                        anyhow::ensure!(
                            tx.record_prepared(participant).map_err(history_error)?,
                            "duplicate retained prepare"
                        );
                    }
                }
                Command::Decide { id, decision } => {
                    if let Some(tx) = result.iter_mut().find(|tx| tx.id == *id) {
                        anyhow::ensure!(
                            tx.record_decision(*decision).map_err(history_error)?,
                            "duplicate retained decision"
                        );
                    }
                }
                _ => {}
            }
            Ok(())
        };
        walk(&self.snapshot, &self.cut.descriptor, |event, _, _| {
            observe(&event)
        })?;
        for event in overlay {
            observe(event)?;
        }
        anyhow::ensure!(
            result.len() as u64 == last - first + 1,
            "archive transaction interval is incomplete"
        );
        Ok(result)
    }

    /// The overlay is the complete accepted original Command suffix of this
    /// operation, including Commands for transactions already drained from the
    /// candidate checkpoint. It is deliberately not an active-record overlay.
    pub(super) fn lookup(
        &self,
        key: LookupKey,
        overlay: &[Command],
    ) -> anyhow::Result<LookupResult> {
        let mut projection = Projection::new(key);
        walk(&self.snapshot, &self.cut.descriptor, |event, _, _| {
            projection.observe(&event)
        })?;
        self.check_overlay(overlay)?;
        for event in overlay {
            projection.observe(event)?;
        }
        Ok(projection.finish())
    }

    fn check_overlay(&self, overlay: &[Command]) -> anyhow::Result<()> {
        let count = self
            .cut
            .descriptor
            .event_count
            .checked_add(overlay.len() as u64)
            .ok_or_else(|| anyhow::anyhow!("archive overlay event overflow"))?;
        anyhow::ensure!(
            count
                <= self
                    .cut
                    .descriptor
                    .domain
                    .cohort
                    .profile
                    .max_history_events(),
            "archive overlay exceeds event bound"
        );
        let mut bytes = 0usize;
        for event in overlay {
            bytes = bytes
                .checked_add(event_size(event)?)
                .filter(|bytes| *bytes <= MAX_JOURNAL_BYTES)
                .ok_or_else(|| anyhow::anyhow!("archive overlay exceeds byte bound"))?;
        }
        Ok(())
    }
}

/// One requested binding or transaction plus its absolute admission ordinal.
/// This projection performs no admissions and is not an alternate authority.
struct Projection {
    key: LookupKey,
    ordinal: u64,
    request: Option<RequestRecord>,
    transaction: Option<Transaction>,
}
impl Projection {
    fn new(key: LookupKey) -> Self {
        Self {
            key,
            ordinal: 0,
            request: None,
            transaction: None,
        }
    }

    fn observe(&mut self, event: &Command) -> anyhow::Result<()> {
        if matches!(event, Command::Begin { .. } | Command::AdmitRequest { .. }) {
            self.ordinal = self
                .ordinal
                .checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("archive ordinal overflow"))?;
        }
        match event {
            Command::AdmitRequest {
                id,
                request,
                payload,
                transaction_id,
                participants,
            } => {
                if matches!(&self.key, LookupKey::Request(wanted) if wanted == id) {
                    anyhow::ensure!(self.request.is_none(), "duplicate retained request");
                    self.request = Some(RequestRecord {
                        id: *id,
                        request: request.clone(),
                        payload: payload.clone(),
                        transaction_id: *transaction_id,
                    });
                }
                if matches!(&self.key, LookupKey::Transaction(wanted) if wanted == transaction_id) {
                    self.admit(*transaction_id, participants)?;
                }
            }
            Command::Begin { id, participants } => {
                if matches!(&self.key, LookupKey::Transaction(wanted) if wanted == id) {
                    self.admit(*id, participants)?;
                }
            }
            Command::Prepared { id, participant } => {
                if let Some(tx) = self.transaction.as_mut().filter(|tx| tx.id == *id) {
                    anyhow::ensure!(
                        tx.record_prepared(participant).map_err(history_error)?,
                        "duplicate retained prepare"
                    );
                }
            }
            Command::Decide { id, decision } => {
                if let Some(tx) = self.transaction.as_mut().filter(|tx| tx.id == *id) {
                    anyhow::ensure!(
                        tx.record_decision(*decision).map_err(history_error)?,
                        "duplicate retained decision"
                    );
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn admit(
        &mut self,
        id: [u8; 16],
        participants: &[crate::transaction_decisions::Participant],
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.transaction.is_none(), "duplicate retained transaction");
        self.transaction = Some(Transaction {
            id,
            ordinal: self.ordinal,
            participants: participants.to_vec(),
            prepared: vec![false; participants.len()],
            decision: None,
        });
        Ok(())
    }

    fn finish(self) -> LookupResult {
        match self.key {
            LookupKey::Request(_) => LookupResult::Request(self.request),
            LookupKey::Transaction(_) => LookupResult::Transaction(self.transaction),
        }
    }
}

/// Exact checkpoint candidate proven by reapplying only the new raw suffix with
/// the shared checkpoint transition implementation. No Journal is constructed.
pub(super) struct StagedActive<'a> {
    candidate: &'a ActiveCheckpoint,
    descriptor: Descriptor,
    // Operation-local output of the exact shared transitions below. These
    // records are handed to terminal staging and never retained by the cut.
    drained: Vec<Transaction>,
}
impl StagedActive<'_> {
    pub(super) fn drained(&self) -> &[Transaction] {
        &self.drained
    }

    pub(super) fn descriptor_for(
        &self,
        checkpoint: &ActiveCheckpoint,
    ) -> anyhow::Result<&Descriptor> {
        anyhow::ensure!(
            std::ptr::eq(self.candidate, checkpoint),
            "active ledger proof belongs to a different candidate"
        );
        Ok(&self.descriptor)
    }

    pub(super) fn published_cut(
        &self,
        checkpoint: &Arc<ActiveCheckpoint>,
        metadata: &[u8],
        decision_manifest: &crate::decision_segments::StagedCheckpointManifest,
    ) -> anyhow::Result<PublishedArchiveCut> {
        let descriptor = self.descriptor_for(checkpoint.as_ref())?;
        let decision_manifest = decision_manifest.bytes();
        super::state_codec::validate_active_metadata(metadata, descriptor, checkpoint)?;
        crate::decision_segments::validate_checkpoint_manifest(
            &crate::decision_segments::CheckpointIdentity {
                shard: descriptor.domain.shard,
                epoch: descriptor.domain.epoch,
                mode: descriptor.domain.mode,
                cohort: descriptor.domain.cohort.clone(),
            },
            checkpoint.resolved_prefix(),
            descriptor.applied,
            decision_manifest,
        )?;
        Ok(PublishedArchiveCut {
            descriptor: descriptor.clone(),
            metadata: metadata.to_vec(),
            decision_manifest: decision_manifest.to_vec(),
            resolved_prefix: checkpoint.resolved_prefix(),
            checkpoint: Arc::clone(checkpoint),
        })
    }
}

/// Preserve every sealed row and rewrite only a growing old tail plus new rows.
/// Caller owns the atomic metadata/terminal/raw/applied batch and must discard
/// the entire batch on any error, including a failed proof construction.
pub(super) fn stage_active<'a>(
    batch: &mut WriteBatch,
    before: &VerifiedArchive<'_, '_>,
    candidate: &'a ActiveCheckpoint,
    suffix: &[Command],
    applied: Option<LogId<NodeId>>,
) -> anyhow::Result<StagedActive<'a>> {
    before.check_overlay(suffix)?;
    candidate.validate().map_err(history_error)?;
    let mut expected = before.checkpoint().clone();
    let drained_count = candidate
        .resolved_prefix()
        .checked_sub(expected.resolved_prefix())
        .ok_or_else(|| anyhow::anyhow!("checkpoint resolved prefix regressed"))?;
    let mut drained = Vec::new();
    for (index, event) in suffix.iter().enumerate() {
        let mut lookup_failure = None;
        let transition = expected.apply(event.clone(), &mut |key| {
            before.lookup(key, &suffix[..index]).map_err(|error| {
                lookup_failure = Some(error);
                crate::transaction_decisions::Error::InvalidJournal
            })
        });
        if let Some(error) = lookup_failure {
            return Err(error);
        }
        let transition = transition.map_err(history_error)?;
        anyhow::ensure!(
            transition.accepted.as_ref() == Some(event),
            "raw suffix contains a rejected or duplicate event"
        );
        let count = drained
            .len()
            .checked_add(transition.drained.len())
            .ok_or_else(|| anyhow::anyhow!("checkpoint drained count overflow"))?;
        anyhow::ensure!(
            count as u64 <= drained_count,
            "raw suffix drains beyond candidate prefix"
        );
        drained.extend(transition.drained);
    }
    anyhow::ensure!(
        &expected == candidate && drained.len() as u64 == drained_count,
        "raw suffix checkpoint mismatch"
    );
    let old = &before.cut.descriptor;
    if suffix.is_empty() {
        return Ok(StagedActive {
            candidate,
            descriptor: Descriptor {
                applied,
                ..old.clone()
            },
            drained,
        });
    }
    let mut cursor = before.tail.before.clone();
    let mut segments = old
        .segment_count
        .checked_sub(1)
        .ok_or_else(|| anyhow::anyhow!("missing raw tail"))?;
    let mut encoded = old
        .encoded_bytes
        .checked_sub(before.tail.encoded_bytes)
        .ok_or_else(|| anyhow::anyhow!("raw tail byte mismatch"))?;
    // Only one bounded row is assembled at a time; the operation's suffix is
    // already owned by the caller and is not copied into an archive-sized Vec.
    let mut events = before.tail.events.clone();
    let mut bytes = before.tail.event_bytes;
    for event in suffix {
        let size = event_size(event)?;
        if !fits(events.len(), bytes, size) {
            write_active_row(
                batch,
                before,
                &mut cursor,
                &events,
                bytes,
                &mut segments,
                &mut encoded,
            )?;
            events.clear();
            bytes = 0;
        }
        events.push(event.clone());
        bytes = bytes
            .checked_add(size)
            .ok_or_else(|| anyhow::anyhow!("raw tail bytes overflow"))?;
    }
    write_active_row(
        batch,
        before,
        &mut cursor,
        &events,
        bytes,
        &mut segments,
        &mut encoded,
    )?;
    let descriptor = Descriptor {
        version: old.version,
        domain: old.domain.clone(),
        event_count: cursor.event_count,
        segment_count: segments,
        encoded_bytes: encoded,
        head_root: cursor.root,
        applied,
    };
    descriptor.validate()?;
    anyhow::ensure!(
        descriptor.event_count == candidate.event_count(),
        "checkpoint event mismatch"
    );
    Ok(StagedActive {
        candidate,
        descriptor,
        drained,
    })
}

fn write_active_row(
    batch: &mut WriteBatch,
    before: &VerifiedArchive<'_, '_>,
    cursor: &mut HistoryCursor,
    events: &[Command],
    event_bytes: usize,
    segments: &mut u64,
    encoded: &mut u64,
) -> anyhow::Result<()> {
    anyhow::ensure!(!events.is_empty(), "empty staged ledger row");
    let domain = &before.cut.descriptor.domain;
    let after = cursor.extend(events).map_err(history_error)?;
    let first = cursor
        .event_count
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("ledger row position overflow"))?;
    let row = Row {
        version: domain.cohort.profile.storage_format(),
        domain: domain.clone(),
        first,
        before_root: cursor.root,
        after_root: after.root,
        events: events.to_vec(),
    };
    let key = domain.key(first);
    let value = serde_json::to_vec(&row)?;
    let size = key
        .len()
        .checked_add(value.len())
        .ok_or_else(|| anyhow::anyhow!("ledger encoded row overflow"))?;
    anyhow::ensure!(
        size <= MAX_ROW_OVERHEAD + event_bytes,
        "ledger row schema exceeds proven overhead"
    );
    *encoded = encoded
        .checked_add(size as u64)
        .ok_or_else(|| anyhow::anyhow!("ledger encoded total overflow"))?;
    *segments = segments
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("ledger segment count overflow"))?;
    if first != before.tail.first || events != before.tail.events.as_slice() {
        batch.put(key, value);
    }
    *cursor = after;
    Ok(())
}

/// Recover only one original external-ID binding and its exact transaction.
/// The ordinal counts admissions (Begin/AdmitRequest), never binding/grant
/// events. Full structural closure still checks every later raw row on absence,
/// pending and terminal results, including decisions beyond an earlier hole.
pub(super) fn lookup_request(
    db: &DB,
    cut: &PublishedArchiveCut,
    id: [u8; 16],
) -> Result<RequestLookup, D1Error> {
    let snapshot = cut.snapshot(db)?;
    let mut ordinal = 0u64;
    let mut record: Option<RecoveredRequest> = None;
    walk(&snapshot, &cut.descriptor, |event, _, _| {
        if matches!(&event, Command::Begin { .. } | Command::AdmitRequest { .. }) {
            ordinal = ordinal
                .checked_add(1)
                .filter(|n| *n <= cut.descriptor.domain.cohort.profile.max_transactions() as u64)
                .ok_or_else(|| anyhow::anyhow!("archive transaction ordinal exceeds bound"))?;
        }
        match event {
            Command::AdmitRequest {
                id: event_id,
                request,
                payload,
                transaction_id,
                participants,
            } if event_id == id => {
                anyhow::ensure!(record.is_none(), "duplicate retained external request");
                anyhow::ensure!(
                    !request.is_empty()
                        && request.len() <= crate::transaction_decisions::MAX_REQUEST_BYTES
                        && !payload.is_empty()
                        && payload.len() <= crate::transaction_decisions::MAX_REQUEST_PAYLOAD_BYTES,
                    "retained request exceeds original bounds"
                );
                let prepared = vec![false; participants.len()];
                record = Some(RecoveredRequest {
                    record: RequestRecord {
                        id,
                        request,
                        payload,
                        transaction_id,
                    },
                    transaction: Transaction {
                        id: transaction_id,
                        ordinal,
                        participants,
                        prepared,
                        decision: None,
                    },
                });
            }
            Command::Prepared { id, participant } => {
                if let Some(found) = record.as_mut().filter(|found| found.transaction.id == id) {
                    anyhow::ensure!(
                        found
                            .transaction
                            .record_prepared(&participant)
                            .map_err(history_error)?,
                        "duplicate retained prepare"
                    );
                }
            }
            Command::Decide { id, decision } => {
                if let Some(found) = record.as_mut().filter(|found| found.transaction.id == id) {
                    anyhow::ensure!(
                        found
                            .transaction
                            .record_decision(decision)
                            .map_err(history_error)?,
                        "duplicate retained decision"
                    );
                }
            }
            _ => {}
        }
        Ok(())
    })
    .map_err(super::ledger_read_error)?;
    let result = RequestLookup {
        authority: cut.descriptor.domain.authority.clone(),
        cohort: cut.descriptor.domain.cohort.clone(),
        resolved_prefix: cut.resolved_prefix,
        id,
        record,
    };
    result
        .validate()
        .map_err(|_| D1Error::RequestIndexHistory)?;
    Ok(result)
}

/// Recover one unchanged bounded page while validating the entire published
/// raw archive. Base and frozen-cut roots are checked at their absolute event
/// positions, even when the page ends early or the archive has a later tail.
pub(super) fn history_page(
    db: &DB,
    published: &PublishedArchiveCut,
    base: &HistoryCursor,
    requested_cut: Option<&HistoryCursor>,
) -> Result<HistoryPage, D1Error> {
    let domain = &published.descriptor.domain;
    base.validate_identity(&domain.authority, &domain.cohort)
        .map_err(|_| D1Error::VisibilityHistory)?;
    let cut = requested_cut.cloned().unwrap_or_else(|| published.head());
    cut.validate_identity(&domain.authority, &domain.cohort)
        .map_err(|_| D1Error::VisibilityHistory)?;
    if base.event_count > cut.event_count || cut.event_count > published.descriptor.event_count {
        return Err(D1Error::VisibilityHistory);
    }
    let snapshot = published.snapshot(db)?;
    if snapshot
        .get(crate::decision_segments::MANIFEST_KEY)
        .map_err(|_| D1Error::DecisionIndexUnavailable)?
        .as_deref()
        != Some(published.decision_manifest.as_slice())
    {
        return Err(D1Error::DecisionIndexHistory);
    }
    let mut base_seen = base.event_count == 0;
    let mut cut_seen = cut.event_count == 0;
    let mut end = base.clone();
    let mut events = Vec::new();
    let mut bytes = 0usize;
    let mut page_full = false;
    walk(&snapshot, &published.descriptor, |event, cursor, size| {
        if cursor.event_count == base.event_count {
            base_seen = cursor == base;
        }
        if cursor.event_count == cut.event_count {
            cut_seen = cursor == &cut;
        }
        if cursor.event_count > base.event_count
            && cursor.event_count <= cut.event_count
            && !page_full
        {
            if fits(events.len(), bytes, size) {
                bytes = bytes
                    .checked_add(size)
                    .ok_or_else(|| anyhow::anyhow!("archive page byte overflow"))?;
                events.push(event);
                end = cursor.clone();
            } else {
                page_full = true;
            }
        }
        Ok(())
    })
    .map_err(super::ledger_read_error)?;
    if !base_seen || !cut_seen {
        return Err(D1Error::VisibilityHistory);
    }
    let page = HistoryPage {
        version: domain.cohort.profile.history_version(),
        base: base.clone(),
        end,
        cut,
        events,
    };
    page.validate().map_err(|_| D1Error::VisibilityHistory)?;
    Ok(page)
}

pub(super) fn reject_orphans(db: &DB) -> anyhow::Result<()> {
    if let Some(item) = db
        .iterator(IteratorMode::From(PREFIX, Direction::Forward))
        .next()
    {
        let (key, _) = item?;
        anyhow::ensure!(
            !key.starts_with(PREFIX),
            "orphan or mixed-format ledger row"
        );
    }
    Ok(())
}

/// Stage only a growing prior tail and new rows. The caller owns the atomic
/// metadata/data/marker batch and must never commit it after any staging error.
/// Explicit validated snapshot replacement alone can clear damaged raw rows.
pub(super) fn stage<'a>(
    batch: &mut WriteBatch,
    before: Option<&Loaded>,
    next: Option<&'a D1State>,
    applied: Option<LogId<NodeId>>,
    replace: bool,
) -> anyhow::Result<Option<Staged<'a>>> {
    if let Some(state) = next {
        state.validate_capacity_activation(applied)?;
    }
    let view = next.and_then(|state| View::of(state).map(|view| (state, view)));
    let Some((candidate, view)) = view else {
        anyhow::ensure!(before.is_none(), "ledger authority cannot disappear");
        if replace {
            batch.delete_range(PREFIX, PREFIX_END);
        }
        return Ok(None);
    };
    view.domain.validate()?;
    let mut replayed = if let Some(before) = before {
        anyhow::ensure!(
            before.descriptor.domain == view.domain,
            "ledger domain changed"
        );
        before
            .journal
            .validate_successor(view.journal)
            .map_err(history_error)?;
        before.journal.clone()
    } else {
        journal(view.domain.authority.mode)?
    };
    let start = replayed.events().len();
    for event in &view.journal.events()[start..] {
        replay(&mut replayed, event.clone())?;
    }
    anyhow::ensure!(
        replayed == *view.journal,
        "ledger successor derived state differs"
    );
    if replace {
        batch.delete_range(PREFIX, PREFIX_END);
    }
    let prior = before.filter(|_| !replace);
    if let Some(before) = prior {
        if before.journal.events().len() == view.journal.events().len() {
            return Staged::new(
                candidate,
                Descriptor {
                    applied,
                    ..before.descriptor.clone()
                },
            )
            .map(Some);
        }
    }
    let (mut cursor, mut segment_count, mut encoded_bytes) = match prior {
        Some(before) => (
            before.tail.before.clone(),
            before.descriptor.segment_count - 1,
            before.descriptor.encoded_bytes - before.tail.encoded_bytes,
        ),
        None => (view.domain.zero_cursor()?, 0, 0),
    };
    let events = view.journal.events();
    while cursor.event_count < events.len() as u64 {
        let first = cursor.event_count as usize;
        let (end, event_bytes) = row_end(events, first)?;
        let after = cursor.extend(&events[first..end]).map_err(history_error)?;
        let row = Row {
            version: view.domain.cohort.profile.storage_format(),
            domain: view.domain.clone(),
            first: first as u64 + 1,
            before_root: cursor.root,
            after_root: after.root,
            events: events[first..end].to_vec(),
        };
        let key = view.domain.key(row.first);
        let bytes = serde_json::to_vec(&row)?;
        anyhow::ensure!(
            key.len() + bytes.len() <= MAX_ROW_OVERHEAD + event_bytes,
            "ledger row schema exceeds proven overhead"
        );
        encoded_bytes += (key.len() + bytes.len()) as u64;
        segment_count += 1;
        // If the old tail was already maximal, it is sealed and unchanged.
        // Compare actual staging operations in tests, not just final DB bytes.
        let unchanged = prior.is_some_and(|before| {
            row.first == before.tail.first && row.events.len() == before.tail.count
        });
        if !unchanged {
            batch.put(key, bytes);
        }
        cursor = after;
    }
    let descriptor = Descriptor {
        version: view.domain.cohort.profile.storage_format(),
        domain: view.domain,
        event_count: cursor.event_count,
        segment_count,
        encoded_bytes,
        head_root: cursor.root,
        applied,
    };
    Staged::new(candidate, descriptor).map(Some)
}
fn row_end(events: &[Command], first: usize) -> anyhow::Result<(usize, usize)> {
    let mut end = first;
    let mut bytes = 0;
    while end < events.len() {
        let size = event_size(&events[end])?;
        if !fits(end - first, bytes, size) {
            break;
        }
        bytes += size;
        end += 1;
    }
    anyhow::ensure!(end > first, "ledger row cannot make progress");
    Ok((end, bytes))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::{
        CapacityActivation, Decision, Member, MemberCapacityGrant, Participant, MAX_PARTICIPANTS,
        MAX_REQUEST_BYTES, MAX_REQUEST_PAYLOAD_BYTES, MAX_TRANSACTIONS,
    };
    use rocksdb::WriteBatchIterator;

    fn cohort() -> Cohort {
        Cohort {
            profile: CapacityProfile::LegacyCountV1,
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: vec![Member {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
            }],
        }
    }
    fn id(n: u64) -> [u8; 16] {
        let mut id = [1; 16];
        id[..8].copy_from_slice(&n.to_be_bytes());
        id
    }
    fn participant() -> Participant {
        Participant {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
            intent: [8; 32],
        }
    }
    fn begin(n: u64) -> Command {
        Command::Begin {
            id: id(n),
            participants: vec![participant()],
        }
    }
    fn decide(n: u64, decision: Decision) -> Command {
        Command::Decide {
            id: id(n),
            decision,
        }
    }
    fn bound() -> Journal {
        let mut j = Journal::new([9; 16]).unwrap();
        j.replay_event(Command::BindCohort { cohort: cohort() })
            .unwrap();
        j
    }
    fn state(journal: Journal) -> D1State {
        let mut state = D1State::new(9, 1);
        state.version = 4;
        state.managed = Some(crate::D1ManagedMode {
            mode_id: [9; 16],
            activated_sequence: 0,
        });
        state.coordinator = Some(journal.into());
        state
    }
    fn history(count: u64) -> D1State {
        let mut j = bound();
        for n in 1..count {
            j.replay_event(begin(n)).unwrap();
        }
        state(j)
    }
    fn byte_history(count: u64) -> D1State {
        let mut c = cohort();
        c.profile = CapacityProfile::ByteBound16MiBV1;
        let grants = c
            .members
            .iter()
            .map(|member| MemberCapacityGrant {
                member: member.clone(),
                activation: CapacityActivation {
                    nonce: [3; 32],
                    log_id: marker(1).unwrap(),
                },
            })
            .collect();
        let mut j = Journal::new([9; 16]).unwrap();
        j.replay_event(Command::BindCohort { cohort: c.clone() })
            .unwrap();
        // This is the existing privileged raw-log fixture boundary. Production
        // confirmation must collect these generations from the actual members.
        j.replay_event(Command::ConfirmCohortCapacity { cohort: c, grants })
            .unwrap();
        for n in 1..=count {
            j.replay_event(begin(n)).unwrap();
        }
        state(j)
    }
    fn marker(index: u64) -> Option<LogId<NodeId>> {
        Some(LogId::new(openraft::CommittedLeaderId::new(3, 4), index))
    }
    fn persist(db: &DB, state: &D1State, applied: Option<LogId<NodeId>>) -> Descriptor {
        let mut batch = WriteBatch::default();
        let staged = stage(&mut batch, None, Some(state), applied, true)
            .unwrap()
            .unwrap();
        let descriptor = staged.descriptor_for(state).unwrap().clone();
        batch.put(
            super::super::APPLIED_KEY,
            serde_json::to_vec(&applied).unwrap(),
        );
        db.write(batch).unwrap();
        descriptor
    }
    fn rows(db: &DB) -> Vec<(Vec<u8>, Vec<u8>)> {
        db.iterator(IteratorMode::From(PREFIX, Direction::Forward))
            .map(Result::unwrap)
            .take_while(|(key, _)| key.starts_with(PREFIX))
            .map(|(key, value)| (key.to_vec(), value.to_vec()))
            .collect()
    }
    // Write intentionally forged authority rows with internally matching roots
    // and totals. Semantic rejection must come from original Journal replay.
    fn forge(db: &DB, events: &[Command], ends: &[usize]) -> Descriptor {
        forge_domain(db, View::of(&history(1)).unwrap().domain, events, ends)
    }
    fn forge_domain(db: &DB, domain: Domain, events: &[Command], ends: &[usize]) -> Descriptor {
        let mut cursor = domain.zero_cursor().unwrap();
        let mut batch = WriteBatch::default();
        batch.delete_range(PREFIX, PREFIX_END);
        let mut total = 0u64;
        let mut first = 0;
        for &end in ends {
            let after = cursor.extend(&events[first..end]).unwrap();
            let row = Row {
                version: domain.cohort.profile.storage_format(),
                domain: domain.clone(),
                first: first as u64 + 1,
                before_root: cursor.root,
                after_root: after.root,
                events: events[first..end].to_vec(),
            };
            let key = domain.key(row.first);
            let bytes = serde_json::to_vec(&row).unwrap();
            total += (key.len() + bytes.len()) as u64;
            batch.put(key, bytes);
            cursor = after;
            first = end;
        }
        batch.put(super::super::APPLIED_KEY, b"null");
        db.write(batch).unwrap();
        Descriptor {
            version: domain.cohort.profile.storage_format(),
            domain,
            event_count: events.len() as u64,
            segment_count: ends.len() as u64,
            encoded_bytes: total,
            head_root: cursor.root,
            applied: None,
        }
    }

    fn archive_state(profile: CapacityProfile, requests: u8) -> D1State {
        let original =
            Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
        let template = crate::request_plans::Plan::decode(&original.requests()[0].payload).unwrap();
        let mut cohort = cohort();
        cohort.profile = profile;
        cohort.members.push(Member {
            shard: 8,
            epoch: 1,
            mode: [8; 16],
        });
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .replay_event(Command::BindCohort {
                cohort: cohort.clone(),
            })
            .unwrap();
        if !profile.is_legacy() {
            let grants = cohort
                .members
                .iter()
                .map(|member| MemberCapacityGrant {
                    member: member.clone(),
                    activation: CapacityActivation {
                        nonce: [3; 32],
                        log_id: marker(1).unwrap(),
                    },
                })
                .collect();
            journal
                .replay_event(Command::ConfirmCohortCapacity {
                    cohort: cohort.clone(),
                    grants,
                })
                .unwrap();
        }
        // An unresolved non-request transaction makes every later terminal
        // request lie beyond a hole, while both kinds still consume ordinals.
        journal.replay_event(begin(999)).unwrap();
        for n in 1..=requests {
            let mut plan = template.clone();
            plan.id = [n; 16];
            plan.cohort = cohort.clone();
            plan.parts[0].request.request_id = plan.id;
            let mut second = plan.parts[0].clone();
            second.descriptor.shard = 8;
            second.descriptor.mode = [8; 16];
            second.request.shard = 8;
            plan.parts.push(second);
            let participants: Vec<_> = plan
                .parts
                .iter()
                .map(|part| part.descriptor.clone())
                .collect();
            let event = Command::AdmitRequest {
                id: [n + 64; 16],
                request: vec![n],
                payload: plan.encode().unwrap(),
                transaction_id: plan.id,
                participants: participants.clone(),
            };
            journal.replay_event(event.clone()).unwrap();
            if n == 1 {
                // A later losing Plan is ignored for an identical external
                // request. Raw recovery must retain only the first Plan bytes.
                let mut retry = event;
                if let Command::AdmitRequest { payload, .. } = &mut retry {
                    *payload = b"losing plan must not replace the original".to_vec();
                }
                journal.apply(retry).unwrap();
            }
            if n == 1 || n == 3 {
                journal
                    .replay_event(Command::Prepared {
                        id: plan.id,
                        participant: participants[0].clone(),
                    })
                    .unwrap();
            }
            if n == 1 {
                journal
                    .replay_event(Command::Prepared {
                        id: plan.id,
                        participant: participants[1].clone(),
                    })
                    .unwrap();
                journal
                    .replay_event(Command::Decide {
                        id: plan.id,
                        decision: Decision::Commit,
                    })
                    .unwrap();
            } else if n != 3 && n != 4 {
                journal
                    .replay_event(Command::Decide {
                        id: plan.id,
                        decision: Decision::Abort,
                    })
                    .unwrap();
            }
            if n % 5 == 0 {
                journal.replay_event(begin(1000 + u64::from(n))).unwrap();
            }
        }
        state(journal)
    }

    fn persist_archive(
        db: &DB,
        state: &D1State,
        applied: Option<LogId<NodeId>>,
    ) -> PublishedArchiveCut {
        let mut batch = WriteBatch::default();
        let staged = stage(&mut batch, None, Some(state), applied, true)
            .unwrap()
            .unwrap();
        let metadata = super::super::state_codec::d1_metadata_bytes(state, Some(&staged)).unwrap();
        batch.put(super::super::D1_KEY, &metadata);
        let manifest =
            crate::decision_segments::stage(&mut batch, None, Some(state), applied, false, true)
                .unwrap()
                .unwrap();
        batch.delete_range(
            crate::request_index::PREFIX,
            crate::request_index::PREFIX_END,
        );
        batch.delete(crate::request_index::MANIFEST_KEY);
        batch.put(
            super::super::APPLIED_KEY,
            serde_json::to_vec(&applied).unwrap(),
        );
        let cut = staged.published_cut(state, &metadata, &manifest).unwrap();
        let mut options = rocksdb::WriteOptions::default();
        options.set_sync(true);
        db.write_opt(batch, &options).unwrap();
        cut
    }

    fn stage_active_terminals(
        batch: &mut WriteBatch,
        verified: &VerifiedArchive<'_, '_>,
        drained: &[Transaction],
        through: u64,
        applied: Option<LogId<NodeId>>,
    ) -> crate::decision_segments::StagedCheckpointManifest {
        let descriptor = verified.descriptor();
        let identity = crate::decision_segments::CheckpointIdentity {
            shard: descriptor.domain.shard,
            epoch: descriptor.domain.epoch,
            mode: descriptor.domain.mode,
            cohort: descriptor.domain.cohort.clone(),
        };
        let old_r = verified.checkpoint().resolved_prefix();
        let tail_key = crate::decision_segments::checkpoint_tail_key(&identity, old_r).unwrap();
        let tail = tail_key
            .as_ref()
            .map(|key| verified.get(key).unwrap().unwrap());
        let projected = if tail.is_some() {
            verified
                .transactions(old_r / 32 * 32 + 1, old_r, &[])
                .unwrap()
        } else {
            Vec::new()
        };
        let stored_manifest = verified
            .get(crate::decision_segments::MANIFEST_KEY)
            .unwrap()
            .unwrap();
        let checked = crate::decision_segments::check_checkpoint_tail(
            &identity,
            old_r,
            descriptor.applied,
            verified.decision_manifest(),
            &stored_manifest,
            tail.as_deref(),
            &projected,
        )
        .unwrap();
        crate::decision_segments::stage_checkpoint(batch, &checked, drained, through, applied)
            .unwrap()
    }

    #[test]
    fn active_raw_stage_matches_legacy_rows_without_a_full_journal() {
        for count in [30, 31, 32, 33, 63, 64] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let old = history(count);
            let cut = persist_archive(&db, &old, marker(10));
            let original = rows(&db);
            let suffix = vec![begin(count), decide(count, Decision::Abort)];
            let mut full = old.coordinator.as_ref().unwrap().full().unwrap().clone();
            for event in &suffix {
                full.replay_event(event.clone()).unwrap();
            }
            let candidate = Arc::new(ActiveCheckpoint::from_journal(&full).unwrap());
            let expected_dir = tempfile::tempdir().unwrap();
            let expected_db = DB::open_default(expected_dir.path()).unwrap();
            let expected_state = state(full);
            let expected = persist_archive(&expected_db, &expected_state, marker(11));
            let mut live = expected_state.clone();
            live.coordinator = Some(crate::coordinator_state::Coordinator::Active(
                candidate.clone(),
            ));
            crate::transaction_decisions::without_full_journal(|| {
                let verified = verify_active(&db, &cut).unwrap();
                let mut empty_batch = WriteBatch::default();
                let unchanged = stage_active(
                    &mut empty_batch,
                    &verified,
                    verified.checkpoint(),
                    &[],
                    marker(11),
                )
                .unwrap();
                assert_eq!(
                    unchanged
                        .descriptor_for(verified.checkpoint())
                        .unwrap()
                        .applied,
                    marker(11)
                );
                assert!(unchanged.drained().is_empty());
                let mut empty_operations = Operations::default();
                empty_batch.iterate(&mut empty_operations);
                assert!(empty_operations.puts.is_empty() && empty_operations.deletes.is_empty());
                let mut batch = WriteBatch::default();
                let staged =
                    stage_active(&mut batch, &verified, &candidate, &suffix, marker(11)).unwrap();
                assert_eq!(
                    staged.descriptor_for(&candidate).unwrap(),
                    &expected.descriptor
                );
                assert!(staged.descriptor_for(&candidate.as_ref().clone()).is_err());
                let mut operations = Operations::default();
                batch.iterate(&mut operations);
                let changed: Vec<_> = rows(&expected_db)
                    .into_iter()
                    .filter(|row| !original.contains(row))
                    .map(|row| row.0)
                    .collect();
                assert_eq!(operations.puts, changed);
                assert!(operations.deletes.is_empty());
                let manifest = stage_active_terminals(
                    &mut batch,
                    &verified,
                    staged.drained(),
                    candidate.resolved_prefix(),
                    marker(11),
                );
                let metadata = super::super::state_codec::active_metadata_bytes(
                    &live,
                    staged.descriptor_for(&candidate).unwrap(),
                    &candidate,
                )
                .unwrap();
                let next_cut = staged
                    .published_cut(&candidate, &metadata, &manifest)
                    .unwrap();
                assert!(Arc::ptr_eq(next_cut.checkpoint_arc(), &candidate));
                assert!(std::ptr::eq(
                    next_cut.checkpoint(),
                    live.coordinator.as_ref().unwrap().active().unwrap()
                ));
                let equal_copy = Arc::new(candidate.as_ref().clone());
                assert!(staged
                    .published_cut(&equal_copy, &metadata, &manifest)
                    .is_err());
                let wrong_marker = stage_active_terminals(
                    &mut WriteBatch::default(),
                    &verified,
                    &[],
                    candidate.resolved_prefix(),
                    marker(12),
                );
                assert!(staged
                    .published_cut(&candidate, &metadata, &wrong_marker)
                    .is_err());
                let mut noncanonical = metadata.clone();
                noncanonical.push(b' ');
                assert!(staged
                    .published_cut(&candidate, &noncanonical, &manifest)
                    .is_err());
                batch.put(super::super::D1_KEY, metadata);
                batch.put(
                    super::super::APPLIED_KEY,
                    serde_json::to_vec(&marker(11)).unwrap(),
                );
                db.write(batch).unwrap();
                assert_eq!(
                    verify_active(&db, &next_cut).unwrap().checkpoint(),
                    candidate.as_ref()
                );
            });
            assert_eq!(rows(&db), rows(&expected_db));
            for row in original.iter().take(original.len().saturating_sub(1)) {
                assert_eq!(db.get(&row.0).unwrap().as_deref(), Some(row.1.as_slice()));
            }
        }
    }

    #[test]
    fn active_projection_preserves_drained_same_batch_identity() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let old = state(bound());
        let cut = persist_archive(&db, &old, marker(1));
        let overlay = vec![begin(1), decide(1, Decision::Abort), begin(2)];
        let mut live = old.clone();
        crate::transaction_decisions::without_full_journal(|| {
            let verified = verify_active(&db, &cut).unwrap();
            let mut checkpoint = verified.checkpoint().clone();
            for (index, event) in overlay.iter().enumerate() {
                checkpoint
                    .apply(event.clone(), &mut |key| {
                        verified
                            .lookup(key, &overlay[..index])
                            .map_err(|_| crate::transaction_decisions::Error::InvalidJournal)
                    })
                    .unwrap();
            }
            assert_eq!(checkpoint.resolved_prefix(), 1);
            let recovered = verified
                .lookup(LookupKey::Transaction(id(1)), &overlay)
                .unwrap();
            match recovered {
                LookupResult::Transaction(Some(tx)) => {
                    assert_eq!(tx.ordinal, 1);
                    assert_eq!(tx.decision, Some(Decision::Abort));
                }
                _ => panic!("drained original transaction was lost"),
            }
            assert_eq!(verified.transactions(1, 2, &overlay).unwrap().len(), 2);
            assert!(verified.transactions(1, 33, &overlay).is_err());
            let retried = checkpoint
                .apply(begin(1), &mut |key| {
                    verified
                        .lookup(key, &overlay)
                        .map_err(|_| crate::transaction_decisions::Error::InvalidJournal)
                })
                .unwrap();
            assert!(retried.accepted.is_none());
            let checkpoint = Arc::new(checkpoint);
            let mut batch = WriteBatch::default();
            let staged =
                stage_active(&mut batch, &verified, &checkpoint, &overlay, marker(2)).unwrap();
            assert_eq!(
                staged.drained(),
                verified.transactions(1, 1, &overlay).unwrap()
            );
            let manifest =
                stage_active_terminals(&mut batch, &verified, staged.drained(), 1, marker(2));
            live.coordinator = Some(crate::coordinator_state::Coordinator::Active(
                checkpoint.clone(),
            ));
            let metadata = super::super::state_codec::active_metadata_bytes(
                &live,
                staged.descriptor_for(&checkpoint).unwrap(),
                &checkpoint,
            )
            .unwrap();
            staged
                .published_cut(&checkpoint, &metadata, &manifest)
                .unwrap();
            let wrong_prefix =
                stage_active_terminals(&mut WriteBatch::default(), &verified, &[], 0, marker(2));
            assert!(staged
                .published_cut(&checkpoint, &metadata, &wrong_prefix)
                .is_err());
            let mut bad = overlay.clone();
            bad.push(begin(1));
            assert!(stage_active(
                &mut WriteBatch::default(),
                &verified,
                &checkpoint,
                &bad,
                marker(2)
            )
            .is_err());
        });
    }

    #[test]
    fn active_verification_checks_manifest_marker_and_later_raw_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let old = history(65);
        for fault in 0..3 {
            let cut = persist_archive(&db, &old, marker(1));
            match fault {
                0 => db
                    .put(crate::decision_segments::MANIFEST_KEY, b"{}")
                    .unwrap(),
                1 => db
                    .put(
                        super::super::APPLIED_KEY,
                        serde_json::to_vec(&marker(2)).unwrap(),
                    )
                    .unwrap(),
                _ => db.put(&rows(&db).last().unwrap().0, b"{}").unwrap(),
            }
            crate::transaction_decisions::without_full_journal(|| {
                let expected = if fault == 0 {
                    D1Error::DecisionIndexHistory
                } else {
                    D1Error::EventLedgerHistory
                };
                assert_eq!(verify_active(&db, &cut).err(), Some(expected));
            });
        }
    }

    #[test]
    fn archive_readers_match_replay_after_every_journal_is_dropped() {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let state = archive_state(profile, 36);
            let journal = state.coordinator.as_ref().unwrap().full().unwrap();
            let expected: Vec<_> = [65, 66, 67, 68, 82, 100, 200]
                .into_iter()
                .map(|id| crate::request_index::lookup(&state, [id; 16], None).unwrap())
                .collect();
            assert_eq!(expected[0].record.as_ref().unwrap().transaction.ordinal, 2);
            assert_eq!(
                expected[0].record.as_ref().unwrap().transaction.decision,
                Some(Decision::Commit)
            );
            assert_eq!(
                expected[1].record.as_ref().unwrap().transaction.decision,
                Some(Decision::Abort)
            );
            assert_eq!(
                expected[2].record.as_ref().unwrap().transaction.prepared,
                vec![true, false]
            );
            assert_eq!(
                expected[3].record.as_ref().unwrap().transaction.prepared,
                vec![false, false]
            );
            assert!(expected.last().unwrap().record.is_none());
            assert!(expected.iter().all(|found| found.resolved_prefix == 0));
            let mut pages = Vec::new();
            for base in [0, 1, 31, 32, 33] {
                for end in [base, 33, journal.events().len() as u64] {
                    if end < base {
                        continue;
                    }
                    let base = journal.history_cursor_at(base).unwrap();
                    let old_cut = journal.history_cursor_at(end).unwrap();
                    pages.push(journal.history_page(&base, Some(&old_cut)).unwrap());
                }
            }
            let cut = persist_archive(&db, &state, marker(8));
            let loaded = load(&db, &cut.descriptor).unwrap();
            loaded.validate_hydrated_state(&state).unwrap();
            crate::request_index::validate(&db, Some(&state), marker(8), false).unwrap();
            crate::decision_segments::validate(&db, Some(&state), marker(8), true).unwrap();
            let reopened = loaded
                .published_cut(&state, &cut.metadata, &cut.decision_manifest)
                .unwrap();
            drop(loaded);
            drop(state);
            crate::transaction_decisions::without_full_journal(|| {
                for expected in expected {
                    assert_eq!(
                        lookup_request(&db, &reopened, expected.id).unwrap(),
                        expected
                    );
                }
                for page in pages {
                    assert_eq!(
                        history_page(&db, &reopened, &page.base, Some(&page.cut)).unwrap(),
                        page
                    );
                }
                let zero = reopened.descriptor.domain.zero_cursor().unwrap();
                let latest = history_page(&db, &reopened, &zero, None).unwrap();
                assert_eq!(latest.events.len(), MAX_HISTORY_PAGE_EVENTS);
                assert_eq!(latest.cut, reopened.head());
                let mut foreign = latest.cut.clone();
                foreign.root[0] ^= 1;
                assert_eq!(
                    history_page(&db, &reopened, &zero, Some(&foreign)),
                    Err(D1Error::VisibilityHistory)
                );
                assert_eq!(
                    history_page(&db, &reopened, &foreign, None),
                    Err(D1Error::VisibilityHistory)
                );
                foreign = zero.clone();
                foreign.cohort.coordinator_epoch += 1;
                assert_eq!(
                    history_page(&db, &reopened, &foreign, None),
                    Err(D1Error::VisibilityHistory)
                );
            });
        }
    }

    #[test]
    fn archive_page_obeys_byte_bound_without_full_history() {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let mut journal = if profile.is_legacy() {
                bound()
            } else {
                byte_history(0).coordinator.unwrap().into_full().unwrap()
            };
            for n in 1..=6 {
                let mut event = large_admit(n);
                if let Command::AdmitRequest { payload, .. } = &mut event {
                    let mut plan = crate::request_plans::Plan::decode(payload).unwrap();
                    plan.cohort = journal.cohort().unwrap().clone();
                    *payload = plan.encode().unwrap();
                }
                journal.replay_event(event).unwrap();
            }
            let zero = journal.history_cursor_at(0).unwrap();
            let expected = journal.history_page(&zero, None).unwrap();
            assert!(expected.events.len() < MAX_HISTORY_PAGE_EVENTS);
            assert!(expected.end.event_count < expected.cut.event_count);
            let state = state(journal);
            let cut = persist_archive(&db, &state, marker(8));
            drop(state);
            crate::transaction_decisions::without_full_journal(|| {
                assert_eq!(history_page(&db, &cut, &zero, None).unwrap(), expected);
                let last = lookup_request(&db, &cut, id(106)).unwrap().record.unwrap();
                assert_eq!(last.record.request.len(), MAX_REQUEST_BYTES);
                assert!(last.record.payload.len() <= MAX_REQUEST_PAYLOAD_BYTES);
            });
        }
    }

    #[test]
    fn archive_reads_check_corrupt_future_tails_and_exact_publication() {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let state = archive_state(profile, 36);
            let zero = state
                .coordinator
                .as_ref()
                .unwrap()
                .full()
                .unwrap()
                .history_cursor_at(0)
                .unwrap();
            let old_cut = state
                .coordinator
                .as_ref()
                .unwrap()
                .full()
                .unwrap()
                .history_cursor_at(3)
                .unwrap();
            for fault in 0..12 {
                let cut = persist_archive(&db, &state, marker(8));
                let original = rows(&db);
                match fault {
                    0 => db.delete(&original[0].0).unwrap(),
                    1 => db.delete(&original[1].0).unwrap(),
                    2 => db.delete(&original.last().unwrap().0).unwrap(),
                    3 => db.put(&original.last().unwrap().0, b"{}").unwrap(),
                    4 => db.put(b"sm/ledger/foreign", b"{}").unwrap(),
                    5 => db
                        .put(
                            cut.descriptor.domain.key(cut.descriptor.event_count + 1),
                            b"{}",
                        )
                        .unwrap(),
                    6 => {
                        let mut bytes = original.last().unwrap().1.clone();
                        bytes.push(b' ');
                        db.put(&original.last().unwrap().0, bytes).unwrap();
                    }
                    7 => db.put(super::super::D1_KEY, b"{}").unwrap(),
                    8 => db
                        .put(
                            super::super::APPLIED_KEY,
                            serde_json::to_vec(&Some(LogId::new(
                                openraft::CommittedLeaderId::new(4, 4),
                                8,
                            )))
                            .unwrap(),
                        )
                        .unwrap(),
                    9 => db.put(crate::request_index::MANIFEST_KEY, b"{}").unwrap(),
                    10 => db.put(b"sm/request/foreign", b"{}").unwrap(),
                    11 => {
                        // Internal row roots can be self-consistent without
                        // matching the privately published final commitment.
                        let mut row: Row =
                            serde_json::from_slice(&original.last().unwrap().1).unwrap();
                        if let Some(Command::Begin { id, .. }) = row.events.last_mut() {
                            id[15] ^= 1;
                        } else if let Some(Command::Decide { id, .. }) = row.events.last_mut() {
                            id[15] ^= 1;
                        } else {
                            panic!("fixture tail changed");
                        }
                        let before = HistoryCursor {
                            event_count: row.first - 1,
                            root: row.before_root,
                            ..zero.clone()
                        };
                        row.after_root = before.extend(&row.events).unwrap().root;
                        db.put(
                            &original.last().unwrap().0,
                            serde_json::to_vec(&row).unwrap(),
                        )
                        .unwrap();
                    }
                    _ => unreachable!(),
                }
                crate::transaction_decisions::without_full_journal(|| {
                    assert!(
                        lookup_request(&db, &cut, [65; 16]).is_err(),
                        "found fault {fault}"
                    );
                    assert!(
                        lookup_request(&db, &cut, [200; 16]).is_err(),
                        "absent fault {fault}"
                    );
                    assert!(
                        history_page(&db, &cut, &zero, Some(&old_cut)).is_err(),
                        "page fault {fault}"
                    );
                });
            }
            let cut = persist_archive(&db, &state, marker(8));
            db.delete(crate::decision_segments::MANIFEST_KEY).unwrap();
            assert_eq!(
                history_page(&db, &cut, &zero, None),
                Err(D1Error::DecisionIndexHistory)
            );
            let next = persist_archive(&db, &state, marker(9));
            assert!(lookup_request(&db, &cut, [65; 16]).is_err());
            assert!(history_page(&db, &cut, &zero, None).is_err());
            assert!(lookup_request(&db, &next, [65; 16]).is_ok());
        }
    }

    #[test]
    fn publication_cut_rejects_wrong_candidate_metadata_and_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let state = archive_state(CapacityProfile::LegacyCountV1, 4);
        let cut = persist_archive(&db, &state, marker(8));
        let loaded = load(&db, &cut.descriptor).unwrap();
        let mut batch = WriteBatch::default();
        let staged = stage(&mut batch, Some(&loaded), Some(&state), marker(8), false)
            .unwrap()
            .unwrap();
        assert!(staged
            .published_cut(&state.clone(), &cut.metadata, &cut.decision_manifest)
            .is_err());
        assert!(staged
            .published_cut(&state, b"{}", &cut.decision_manifest)
            .is_err());
        assert!(staged.published_cut(&state, &cut.metadata, b"{}").is_err());
        assert!(loaded
            .published_cut(&state, b"{}", &cut.decision_manifest)
            .is_err());
    }

    #[test]
    fn canonical_goldens_preserve_binding_and_original_admitted_request() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let bound_state = history(1);
        let descriptor = persist(&db, &bound_state, None);
        let stored = rows(&db);
        assert_eq!(
            stored[0].1,
            include_bytes!("../tests/fixtures/event-ledger-bound-row-v1.json")
        );
        let hex: String = stored[0].0.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            include_str!("../tests/fixtures/event-ledger-coordinator-key-v1.hex").trim()
        );
        let mut member = descriptor.domain.clone();
        member.role = Role::Member;
        member.shard = 7;
        member.mode = [7; 16];
        member.validate().unwrap();
        let hex: String = member.key(1).iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(
            hex,
            include_str!("../tests/fixtures/event-ledger-member-key-v1.hex").trim()
        );
        let loaded = load(&db, &descriptor).unwrap();
        assert_eq!(
            loaded.journal,
            *bound_state.coordinator.as_ref().unwrap().full().unwrap()
        );
        loaded.validate_hydrated_state(&bound_state).unwrap();
        let j =
            Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
        let admitted = state(j.clone());
        let descriptor = persist(&db, &admitted, None);
        assert_eq!(
            rows(&db)[0].1,
            include_bytes!("../tests/fixtures/event-ledger-admitted-row-v1.json")
        );
        let recovered = load(&db, &descriptor).unwrap().journal;
        assert_eq!(recovered, j);
        assert_eq!(
            recovered.encode().unwrap(),
            include_bytes!("../tests/fixtures/request-journal-v3.json")
        );
        assert_eq!(recovered.requests(), j.requests());
    }
    #[test]
    fn byte_profile_ledger_reopens_and_rejects_mixed_formats_and_missing_grant() {
        let dir = tempfile::tempdir().unwrap();
        let state = byte_history(1025);
        let descriptor = {
            let db = DB::open_default(dir.path()).unwrap();
            let descriptor = persist(&db, &state, marker(2048));
            assert_eq!(descriptor.version, 2);
            let hex: String = rows(&db)[0].0.iter().map(|b| format!("{b:02x}")).collect();
            assert_eq!(
                hex,
                include_str!("../tests/fixtures/event-ledger-coordinator-key-v1.hex")
                    .trim()
                    .replacen("00000001", "00000002", 1)
            );
            descriptor
        };
        let db = DB::open_default(dir.path()).unwrap();
        assert_eq!(
            load(&db, &descriptor).unwrap().journal,
            *state.coordinator.as_ref().unwrap().full().unwrap()
        );
        let small = byte_history(33);
        for fault in 0..7 {
            let mut descriptor = persist(&db, &small, marker(64));
            let original = rows(&db);
            match fault {
                0 => descriptor.version = 1,
                1 => descriptor.domain.cohort.profile = CapacityProfile::LegacyCountV1,
                2 => {
                    let mut row: Row = serde_json::from_slice(&original[0].1).unwrap();
                    row.version = 1;
                    db.put(&original[0].0, serde_json::to_vec(&row).unwrap())
                        .unwrap();
                }
                3 => {
                    let mut key = original[0].0.clone();
                    key[PREFIX.len() + 3] = 1;
                    db.put(key, &original[0].1).unwrap();
                }
                4 => {
                    db.delete(&original[1].0).unwrap();
                }
                5 => {
                    db.put(descriptor.domain.key(100), b"{}").unwrap();
                }
                6 => {
                    let mut row: Row = serde_json::from_slice(&original[0].1).unwrap();
                    row.domain.cohort.profile = CapacityProfile::LegacyCountV1;
                    db.put(&original[0].0, serde_json::to_vec(&row).unwrap())
                        .unwrap();
                }
                _ => unreachable!(),
            }
            assert!(load(&db, &descriptor).is_err(), "fault {fault}");
        }
        let domain = View::of(&small).unwrap().domain;
        let events = small.coordinator.as_ref().unwrap().full().unwrap().events();
        let ungranted = vec![events[0].clone(), events[2].clone(), events[1].clone()];
        let descriptor = forge_domain(&db, domain, &ungranted, &[3]);
        assert!(load(&db, &descriptor).is_err());
        assert_eq!(max_segments(CapacityProfile::ByteBound16MiBV1), 7499);
        assert_eq!(
            max_encoded_bytes(CapacityProfile::ByteBound16MiBV1),
            139_640_832
        );
    }

    #[test]
    fn namespace_closure_and_descriptor_source_headers_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        for state in [history(65), byte_history(63)] {
            for fault in 0..15 {
                let mut descriptor = persist(&db, &state, marker(8));
                let original = rows(&db);
                match fault {
                    0 => db.delete(&original[1].0).unwrap(),
                    1 => db.put(descriptor.domain.key(66), b"{}").unwrap(),
                    2 => db.put(b"sm/ledger/foreign", b"{}").unwrap(),
                    3 => db.put(&original[0].0, b"{}").unwrap(),
                    4 => {
                        let mut row: Row = serde_json::from_slice(&original[0].1).unwrap();
                        row.version += 1;
                        db.put(&original[0].0, serde_json::to_vec(&row).unwrap())
                            .unwrap();
                    }
                    5 => {
                        let mut bytes = original[0].1.clone();
                        bytes.push(b' ');
                        db.put(&original[0].0, bytes).unwrap();
                    }
                    6 => descriptor.event_count += 1,
                    7 => descriptor.segment_count += 1,
                    8 => descriptor.encoded_bytes += 1,
                    9 => descriptor.head_root[0] ^= 1,
                    10 => descriptor.domain.cohort.members[0].mode[0] ^= 1,
                    11 => descriptor.domain.authority.mode[0] ^= 1,
                    12 => descriptor.domain.epoch += 1,
                    13 => descriptor.domain.role = Role::Member,
                    14 => {
                        // Same index, different leader identity must not compare equal.
                        descriptor.applied =
                            Some(LogId::new(openraft::CommittedLeaderId::new(4, 4), 8));
                    }
                    _ => unreachable!(),
                }
                assert!(load(&db, &descriptor).is_err(), "fault {fault}");
            }
            assert!(reject_orphans(&db).is_err());
            let mut batch = WriteBatch::default();
            stage(&mut batch, None, None, None, true).unwrap();
            db.write(batch).unwrap();
            reject_orphans(&db).unwrap();
        }
    }

    #[test]
    fn rehashed_noop_unknown_and_false_commit_events_are_not_authority() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let binding = bound().events()[0].clone();
        let prepared = Command::Prepared {
            id: id(1),
            participant: participant(),
        };
        for events in [
            vec![binding.clone(), binding.clone()],
            vec![binding.clone(), begin(1), begin(1)],
            vec![binding.clone(), decide(1, Decision::Abort)],
            vec![binding.clone(), begin(1), decide(1, Decision::Commit)],
            vec![binding.clone(), begin(1), prepared.clone(), prepared],
            vec![
                binding.clone(),
                begin(1),
                decide(1, Decision::Abort),
                decide(1, Decision::Abort),
            ],
            vec![begin(1), binding],
        ] {
            let descriptor = forge(&db, &events, &[events.len()]);
            assert!(load(&db, &descriptor).is_err());
        }
        let state = history(33);
        let events = state.coordinator.as_ref().unwrap().full().unwrap().events();
        let descriptor = forge(&db, events, &[31, 33]);
        assert!(load(&db, &descriptor).is_err());
        let descriptor = forge(&db, events, &[32, 33]);
        assert_eq!(load(&db, &descriptor).unwrap().journal.events(), events);
    }

    #[derive(Default)]
    struct Operations {
        puts: Vec<Vec<u8>>,
        deletes: Vec<Vec<u8>>,
    }
    impl WriteBatchIterator for Operations {
        fn put(&mut self, key: &[u8], _: &[u8]) {
            self.puts.push(key.to_vec());
        }
        fn delete(&mut self, key: &[u8]) {
            self.deletes.push(key.to_vec());
        }
    }
    #[test]
    fn actual_staged_keys_never_rewrite_sealed_rows_or_unchanged_history() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let make_state = |count| {
                if profile == CapacityProfile::LegacyCountV1 {
                    history(count)
                } else {
                    byte_history(count - 2)
                }
            };
            for (old, next, first) in [
                (31, 32, Some(1)),
                (32, 33, Some(33)),
                (33, 34, Some(33)),
                (65, 65, None),
            ] {
                let old_state = make_state(old);
                let descriptor = persist(&db, &old_state, marker(1));
                let loaded = load(&db, &descriptor).unwrap();
                let mut batch = WriteBatch::default();
                let next_state = make_state(next);
                let staged = stage(
                    &mut batch,
                    Some(&loaded),
                    Some(&next_state),
                    marker(2),
                    false,
                )
                .unwrap()
                .unwrap();
                let next_descriptor = staged.descriptor_for(&next_state).unwrap();
                let mut operations = Operations::default();
                batch.iterate(&mut operations);
                assert_eq!(
                    operations.puts,
                    first
                        .into_iter()
                        .map(|n| descriptor.domain.key(n))
                        .collect::<Vec<_>>()
                );
                assert!(operations.deletes.is_empty());
                batch.put(
                    super::super::APPLIED_KEY,
                    serde_json::to_vec(&marker(2)).unwrap(),
                );
                db.write(batch).unwrap();
                assert_eq!(
                    load(&db, next_descriptor).unwrap().journal,
                    *next_state.coordinator.as_ref().unwrap().full().unwrap()
                );
                loaded
                    .journal
                    .validate_successor(next_state.coordinator.as_ref().unwrap().full().unwrap())
                    .unwrap();
            }
        }
    }

    #[test]
    fn unchanged_length_and_replacement_staging_reject_foreign_or_regressed_history() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        for old in [history(2), byte_history(1)] {
            let descriptor = persist(&db, &old, marker(1));
            let loaded = load(&db, &descriptor).unwrap();
            let mut prefix = journal(loaded.journal().authority()).unwrap();
            let events = loaded.journal().events();
            for event in &events[..events.len() - 1] {
                replay(&mut prefix, event.clone()).unwrap();
            }
            let regressed = state(prefix.clone());
            replay(&mut prefix, begin(2)).unwrap();
            let substituted = state(prefix);
            assert_eq!(
                substituted
                    .coordinator
                    .as_ref()
                    .unwrap()
                    .full()
                    .unwrap()
                    .events()
                    .len(),
                events.len()
            );
            for candidate in [&regressed, &substituted] {
                for replace in [false, true] {
                    let mut batch = WriteBatch::default();
                    assert!(stage(
                        &mut batch,
                        Some(&loaded),
                        Some(candidate),
                        marker(2),
                        replace,
                    )
                    .is_err());
                    assert!(batch.is_empty());
                }
            }
        }
    }

    #[test]
    fn strict_descriptor_fields_and_full_identity() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let descriptor = persist(&db, &history(1), None);
        let bytes = serde_json::to_vec(&descriptor).unwrap();
        assert_eq!(
            serde_json::from_slice::<Descriptor>(&bytes).unwrap(),
            descriptor
        );
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value.as_object_mut().unwrap().remove("applied");
        assert!(serde_json::from_value::<Descriptor>(value).is_err());
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        value["events"] = serde_json::json!([]);
        assert!(serde_json::from_value::<Descriptor>(value).is_err());
        descriptor
            .validate_identity(9, 1, [9; 16], Role::Coordinator)
            .unwrap();
        for (shard, epoch, mode, role) in [
            (8, 1, [9; 16], Role::Coordinator),
            (9, 2, [9; 16], Role::Coordinator),
            (9, 1, [8; 16], Role::Coordinator),
            (9, 1, [9; 16], Role::Member),
        ] {
            assert!(descriptor
                .validate_identity(shard, epoch, mode, role)
                .is_err());
        }
    }

    #[test]
    fn maximum_width_schema_and_events_fit_only_derived_bounds() {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let members: Vec<_> = (0..MAX_PARTICIPANTS as u64)
                .map(|n| Member {
                    shard: u64::MAX - MAX_PARTICIPANTS as u64 + n,
                    epoch: u64::MAX,
                    mode: [255; 16],
                })
                .collect();
            let domain = Domain {
                role: Role::Coordinator,
                shard: u64::MAX,
                epoch: u64::MAX,
                mode: [255; 16],
                authority: Authority {
                    shard: u64::MAX,
                    mode: [255; 16],
                },
                cohort: Cohort {
                    profile,
                    coordinator_shard: u64::MAX,
                    coordinator_epoch: u64::MAX,
                    members,
                },
            };
            domain.validate().unwrap();
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
            let mut events = vec![
                Command::BindCohort {
                    cohort: domain.cohort.clone(),
                },
                Command::Begin {
                    id: [255; 16],
                    participants: participants.clone(),
                },
                Command::Prepared {
                    id: [255; 16],
                    participant: participants[0].clone(),
                },
                Command::Decide {
                    id: [255; 16],
                    decision: Decision::Commit,
                },
                Command::AdmitRequest {
                    id: [255; 16],
                    request: vec![255; MAX_REQUEST_BYTES],
                    payload: vec![255; MAX_REQUEST_PAYLOAD_BYTES],
                    transaction_id: [255; 16],
                    participants,
                },
            ];
            if profile == CapacityProfile::ByteBound16MiBV1 {
                events.push(Command::ConfirmCohortCapacity {
                    cohort: domain.cohort.clone(),
                    grants: domain
                        .cohort
                        .members
                        .iter()
                        .map(|member| MemberCapacityGrant {
                            member: member.clone(),
                            activation: CapacityActivation {
                                nonce: [255; 32],
                                log_id: LogId::new(
                                    openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
                                    u64::MAX,
                                ),
                            },
                        })
                        .collect(),
                });
            }
            // Exercise the actual writer/loader with the widest legal binding and
            // complete immutable grant, in addition to the conservative shape proof.
            let mut journal = Journal::new(domain.authority.mode).unwrap();
            journal.replay_event(events[0].clone()).unwrap();
            if profile == CapacityProfile::ByteBound16MiBV1 {
                journal
                    .replay_event(events.last().unwrap().clone())
                    .unwrap();
            }
            let mut widest = D1State::new(domain.shard, domain.epoch);
            widest.version = 4;
            widest.managed = Some(crate::D1ManagedMode {
                mode_id: domain.mode,
                activated_sequence: 0,
            });
            widest.coordinator = Some(journal.into());
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let descriptor = persist(&db, &widest, marker(u64::MAX));
            assert_eq!(descriptor.version, profile.storage_format());
            assert_eq!(
                load(&db, &descriptor).unwrap().journal,
                *widest.coordinator.as_ref().unwrap().full().unwrap()
            );
            for event in &events {
                // This width upper bound deliberately ignores Plan validity: no
                // valid event can require a wider integer, array, or payload.
                assert!(event_size(event).unwrap() <= MAX_REQUEST_EVENT_BYTES);
            }
            let row = Row {
                version: domain.cohort.profile.storage_format(),
                domain: domain.clone(),
                first: u64::MAX,
                before_root: [255; 32],
                after_root: [255; 32],
                events: vec![events[3].clone(); MAX_HISTORY_PAGE_EVENTS],
            };
            let command_bytes: usize = row.events.iter().map(|e| event_size(e).unwrap()).sum();
            assert!(
                domain.key(u64::MAX).len() + serde_json::to_vec(&row).unwrap().len()
                    - command_bytes
                    < MAX_ROW_OVERHEAD
            );
            assert_eq!(max_segments(CapacityProfile::LegacyCountV1), 2122);
            assert_eq!(
                max_encoded_bytes(CapacityProfile::LegacyCountV1),
                MAX_JOURNAL_BYTES as u64 + 2122 * MAX_ROW_OVERHEAD as u64
            );
        }
    }

    // Geometry tests isolate canonical serialized lengths from semantic Plan
    // validity. Two syntactically valid commands can reach the exact budget;
    // real admission remains independently enforced by replay in load/stage.
    fn event_with_size(target: usize) -> Command {
        let mut event = Command::AdmitRequest {
            id: [1; 16],
            request: vec![],
            payload: vec![],
            transaction_id: [2; 16],
            participants: vec![],
        };
        let base = serde_json::to_vec(&event).unwrap().len();
        let remaining = target - base + 1;
        if let Command::AdmitRequest { request, .. } = &mut event {
            *request = vec![0; remaining / 2];
            if !remaining.is_multiple_of(2) {
                request[0] = 10;
            }
        }
        assert_eq!(serde_json::to_vec(&event).unwrap().len(), target);
        event
    }
    #[test]
    fn exact_count_and_byte_partition_boundaries_are_greedy() {
        for count in [31, 32, 33] {
            let events = vec![decide(1, Decision::Abort); count];
            assert_eq!(row_end(&events, 0).unwrap().0, count.min(32));
        }
        let large = event_with_size(MAX_REQUEST_EVENT_BYTES);
        let events = vec![large.clone(), large, decide(1, Decision::Abort)];
        assert_eq!(row_end(&events, 0).unwrap(), (2, MAX_HISTORY_PAGE_BYTES));
        assert!(!fits(2, MAX_HISTORY_PAGE_BYTES, 1));
        assert!(fits(2, MAX_HISTORY_PAGE_BYTES - 1, 1));
        let small = event_with_size(MAX_REQUEST_EVENT_BYTES - 1);
        assert_eq!(
            row_end(&[small.clone(), small], 0).unwrap(),
            (2, MAX_HISTORY_PAGE_BYTES - 2)
        );
    }

    #[test]
    fn full_transaction_slots_still_replay_reserved_abort_and_reject_unknown_abort() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let full = history(MAX_TRANSACTIONS as u64 + 1);
        let descriptor = persist(&db, &full, None);
        let loaded = load(&db, &descriptor).unwrap();
        let mut next = full.clone();
        next.coordinator
            .as_mut()
            .unwrap()
            .full_mut()
            .unwrap()
            .replay_event(decide(1, Decision::Abort))
            .unwrap();
        let mut batch = WriteBatch::default();
        let staged = stage(&mut batch, Some(&loaded), Some(&next), None, false)
            .unwrap()
            .unwrap();
        let descriptor = staged.descriptor_for(&next).unwrap();
        db.write(batch).unwrap();
        assert_eq!(
            load(&db, descriptor).unwrap().journal,
            *next.coordinator.as_ref().unwrap().full().unwrap()
        );
        let mut invalid = next
            .coordinator
            .as_ref()
            .unwrap()
            .full()
            .unwrap()
            .events()
            .to_vec();
        invalid.push(decide(MAX_TRANSACTIONS as u64 + 1, Decision::Abort));
        let ends: Vec<_> = (MAX_HISTORY_PAGE_EVENTS..invalid.len())
            .step_by(MAX_HISTORY_PAGE_EVENTS)
            .chain(std::iter::once(invalid.len()))
            .collect();
        let descriptor = forge(&db, &invalid, &ends);
        assert!(load(&db, &descriptor).is_err());
    }
    fn large_admit(n: u64) -> Command {
        let plan = crate::request_plans::Plan {
            version: 1,
            id: id(n),
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: cohort(),
            validation_version: 0,
            parts: vec![crate::request_plans::Part {
                descriptor: participant(),
                request: crate::D1WriteRequest {
                    shard: 7,
                    epoch: 1,
                    request_id: id(n),
                    expected_sequence: Some(0),
                    operations: vec![crate::WriteOp::Put {
                        key: vec![b'x'],
                        value: vec![255; 60 * 1024],
                    }],
                },
            }],
        };
        Command::AdmitRequest {
            id: id(n + 100),
            request: vec![255; MAX_REQUEST_BYTES],
            payload: plan.encode().unwrap(),
            transaction_id: id(n),
            participants: vec![participant()],
        }
    }
    #[test]
    fn active_large_plans_seal_byte_full_tail_without_full_journal() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let mut journal = bound();
        let mut event_bytes = event_size(&journal.events()[0]).unwrap();
        let next_event = loop {
            let event = large_admit(journal.events().len() as u64);
            let size = event_size(&event).unwrap();
            if !fits(journal.events().len(), event_bytes, size) {
                break event;
            }
            event_bytes += size;
            journal.replay_event(event).unwrap();
        };
        assert!(journal.events().len() + 1 < MAX_HISTORY_PAGE_EVENTS);
        assert!(event_bytes + event_size(&next_event).unwrap() > MAX_HISTORY_PAGE_BYTES);
        let old = state(journal.clone());
        let cut = persist_archive(&db, &old, marker(20));
        assert_eq!(cut.descriptor.segment_count, 1);
        let original = rows(&db)[0].clone();
        journal.replay_event(next_event.clone()).unwrap();
        let expected_requests = journal.requests().to_vec();
        let candidate = Arc::new(ActiveCheckpoint::from_journal(&journal).unwrap());
        let expected_dir = tempfile::tempdir().unwrap();
        let expected_db = DB::open_default(expected_dir.path()).unwrap();
        let expected_state = state(journal);
        let expected_cut = persist_archive(&expected_db, &expected_state, marker(21));
        assert_eq!(expected_cut.descriptor.segment_count, 2);
        let mut live = expected_state.clone();
        live.coordinator = Some(crate::coordinator_state::Coordinator::Active(
            candidate.clone(),
        ));
        drop(old);
        drop(expected_state);
        crate::transaction_decisions::without_full_journal(|| {
            let verified = verify_active(&db, &cut).unwrap();
            let suffix = [next_event];
            let mut batch = WriteBatch::default();
            let staged = stage_active(
                &mut batch,
                &verified,
                candidate.as_ref(),
                &suffix,
                marker(21),
            )
            .unwrap();
            assert_eq!(
                staged.descriptor_for(candidate.as_ref()).unwrap(),
                &expected_cut.descriptor
            );
            assert!(staged.drained().is_empty());
            let mut operations = Operations::default();
            batch.iterate(&mut operations);
            assert_eq!(
                operations.puts,
                vec![cut.descriptor.domain.key(cut.descriptor.event_count + 1)]
            );
            assert!(operations.deletes.is_empty());
            for request in &expected_requests {
                assert_eq!(
                    verified
                        .lookup(LookupKey::Request(request.id), &suffix)
                        .unwrap(),
                    LookupResult::Request(Some(request.clone())),
                );
            }
            let manifest = stage_active_terminals(
                &mut batch,
                &verified,
                staged.drained(),
                candidate.resolved_prefix(),
                marker(21),
            );
            let metadata = super::super::state_codec::active_metadata_bytes(
                &live,
                staged.descriptor_for(candidate.as_ref()).unwrap(),
                candidate.as_ref(),
            )
            .unwrap();
            let next_cut = staged
                .published_cut(&candidate, &metadata, &manifest)
                .unwrap();
            batch.put(super::super::D1_KEY, metadata);
            batch.put(
                super::super::APPLIED_KEY,
                serde_json::to_vec(&marker(21)).unwrap(),
            );
            db.write(batch).unwrap();
            assert_eq!(rows(&db), rows(&expected_db));
            assert_eq!(rows(&db)[0], original);
            let published = verify_active(&db, &next_cut).unwrap();
            for request in &expected_requests {
                assert_eq!(
                    published
                        .lookup(LookupKey::Request(request.id), &[])
                        .unwrap(),
                    LookupResult::Request(Some(request.clone())),
                );
            }
        });
    }

    #[test]
    fn real_large_plans_seal_byte_full_tail_without_rewriting_original_requests() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let mut j = bound();
        // Find the first actual byte-driven split, with fewer than 32 events.
        let mut bytes = event_size(&j.events()[0]).unwrap();
        let next_event = loop {
            let event = large_admit(j.events().len() as u64);
            let size = event_size(&event).unwrap();
            if !fits(j.events().len(), bytes, size) {
                break event;
            }
            bytes += size;
            j.replay_event(event).unwrap();
        };
        assert!(j.events().len() < MAX_HISTORY_PAGE_EVENTS);
        let old = state(j);
        let descriptor = persist(&db, &old, None);
        assert_eq!(descriptor.segment_count, 1);
        let before = load(&db, &descriptor).unwrap();
        let old_row = rows(&db)[0].clone();
        let mut next = old.clone();
        next.coordinator
            .as_mut()
            .unwrap()
            .full_mut()
            .unwrap()
            .replay_event(next_event)
            .unwrap();
        let mut batch = WriteBatch::default();
        let staged = stage(&mut batch, Some(&before), Some(&next), None, false)
            .unwrap()
            .unwrap();
        let descriptor = staged.descriptor_for(&next).unwrap();
        let mut operations = Operations::default();
        batch.iterate(&mut operations);
        assert_eq!(
            operations.puts,
            vec![descriptor.domain.key(before.descriptor.event_count + 1)]
        );
        db.write(batch).unwrap();
        assert_eq!(rows(&db)[0], old_row);
        let loaded = load(&db, descriptor).unwrap();
        assert_eq!(
            loaded.journal,
            *next.coordinator.as_ref().unwrap().full().unwrap()
        );
        for request in before.journal.requests() {
            assert_eq!(loaded.journal.request(request.id), Some(request));
        }
    }

    #[test]
    fn original_byte_reservations_reject_peak_hidden_by_future_aborts() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let mut c = cohort();
        c.members = (100..100 + MAX_PARTICIPANTS as u64)
            .map(|shard| Member {
                shard,
                epoch: u64::MAX,
                mode: [255; 16],
            })
            .collect();
        let participants: Vec<_> = c
            .members
            .iter()
            .map(|m| Participant {
                shard: m.shard,
                epoch: m.epoch,
                mode: m.mode,
                intent: [255; 32],
            })
            .collect();
        let begin = |n| Command::Begin {
            id: id(n),
            participants: participants.clone(),
        };
        let mut candidate = Journal::new([9; 16]).unwrap();
        candidate
            .replay_event(Command::BindCohort { cohort: c })
            .unwrap();
        let mut rejected = None;
        for n in 1..=MAX_TRANSACTIONS as u64 {
            match candidate.replay_event(begin(n)) {
                Ok(()) => {}
                Err(crate::transaction_decisions::Error::Capacity) => {
                    rejected = Some(n);
                    break;
                }
                Err(e) => panic!("unexpected replay rejection: {e:?}"),
            }
        }
        let rejected = rejected.expect("byte reservations exhaust before transaction slots");
        // Strict replay's candidate can be mutated on failure. Discard it and
        // reconstruct the accepted original events before performing any I/O.
        let accepted = candidate.events().to_vec();
        drop(candidate);
        let mut journal = Journal::new([9; 16]).unwrap();
        for event in &accepted {
            journal.replay_event(event.clone()).unwrap();
        }
        assert_eq!(journal.transaction_count() as u64, rejected - 1);
        let old = state(journal);
        let descriptor = persist(&db, &old, None);
        let loaded = load(&db, &descriptor).unwrap();
        let mut next = old.clone();
        next.coordinator
            .as_mut()
            .unwrap()
            .full_mut()
            .unwrap()
            .replay_event(decide(rejected - 1, Decision::Abort))
            .unwrap();
        let mut batch = WriteBatch::default();
        let staged = stage(&mut batch, Some(&loaded), Some(&next), None, false)
            .unwrap()
            .unwrap();
        let descriptor = staged.descriptor_for(&next).unwrap();
        db.write(batch).unwrap();
        assert_eq!(
            load(&db, descriptor).unwrap().journal,
            *next.coordinator.as_ref().unwrap().full().unwrap()
        );
        let mut impossible = accepted;
        impossible.push(begin(rejected));
        for n in 1..=rejected {
            impossible.push(decide(n, Decision::Abort));
        }
        assert!(
            impossible
                .iter()
                .map(|e| event_size(e).unwrap())
                .sum::<usize>()
                < MAX_JOURNAL_BYTES
        );
        let ends: Vec<_> = (MAX_HISTORY_PAGE_EVENTS..impossible.len())
            .step_by(MAX_HISTORY_PAGE_EVENTS)
            .chain(std::iter::once(impossible.len()))
            .collect();
        let domain = View::of(&old).unwrap().domain;
        let descriptor = forge_domain(&db, domain, &impossible, &ends);
        assert!(load(&db, &descriptor).is_err());
    }
}
