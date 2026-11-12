//! Sole persistent live Journal authority for fixed-cohort disk formats.
//! Raw commands are replayed with the original admission and reservation rules.
//! The full namespace is closed: no derived index or archived snapshot is a fallback.
use crate::d1::D1State;
use crate::participant_writes::Authority;
use crate::request_index::{RecoveredRequest, RequestLookup};
use crate::target_evidence::{TargetEvidence, TargetRecord};
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

#[path = "ledger_row_witness.rs"]
mod row_witness;
use row_witness::RowWitness;
type RowWitnesses = Option<Arc<[RowWitness]>>;

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
pub(crate) enum Role {
    Coordinator,
    Member,
}
impl Role {
    pub(super) fn decision_role(self) -> crate::decision_segments::Role {
        match self {
            Self::Coordinator => crate::decision_segments::Role::Coordinator,
            Self::Member => crate::decision_segments::Role::Member,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Domain {
    pub(crate) role: Role,
    pub(crate) shard: u64,
    pub(crate) epoch: u64,
    pub(crate) mode: [u8; 16],
    pub(crate) authority: Authority,
    pub(crate) cohort: Cohort,
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
pub(crate) struct Descriptor {
    pub(crate) version: u32,
    pub(crate) domain: Domain,
    pub(crate) event_count: u64,
    pub(crate) segment_count: u64,
    pub(crate) encoded_bytes: u64,
    pub(crate) head_root: [u8; 32],
    #[serde(deserialize_with = "required_applied")]
    pub(crate) applied: Option<LogId<NodeId>>,
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
            None => (Role::Member, state.visibility.as_ref()?.journal.full()?),
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
        #[cfg(all(test, debug_assertions))]
        ROW_DECODES.set(ROW_DECODES.get() + 1);
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
    row_witnesses: RowWitnesses,
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
    row_witnesses: RowWitnesses,
}
impl<'a> Staged<'a> {
    fn new(
        candidate: &'a D1State,
        descriptor: Descriptor,
        row_witnesses: RowWitnesses,
    ) -> anyhow::Result<Self> {
        descriptor.validate()?;
        validate_row_witnesses(&descriptor, &row_witnesses)?;
        Ok(Self {
            candidate,
            descriptor,
            row_witnesses,
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

#[cfg(test)]
thread_local! {
    static RAW_WALKS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    #[cfg(debug_assertions)]
    static ROW_DECODES: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn raw_walks() -> u64 {
    RAW_WALKS.get()
}

/// One structural walker owns all raw namespace, canonical partition, and hash
/// closure checks. Observers may collect semantic replay or one bounded result;
/// neither can return a successful result before the entire scan closes.
fn walk(
    snapshot: &Snapshot<'_>,
    descriptor: &Descriptor,
    observe: impl FnMut(Command, &HistoryCursor, usize) -> anyhow::Result<()>,
) -> anyhow::Result<Tail> {
    walk_rows(snapshot, descriptor, observe, |_, _, _| {})
}

fn walk_rows(
    snapshot: &Snapshot<'_>,
    descriptor: &Descriptor,
    mut observe: impl FnMut(Command, &HistoryCursor, usize) -> anyhow::Result<()>,
    mut observe_row: impl FnMut(u64, &[u8], &[u8]),
) -> anyhow::Result<Tail> {
    #[cfg(test)]
    RAW_WALKS.set(RAW_WALKS.get() + 1);
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
        observe_row(first, &key, &value);
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
    #[cfg(any(test, feature = "test-hooks"))]
    crate::transaction_decisions::assert_full_journal_allowed();
    let snapshot = db.snapshot();
    let mut journal = journal(descriptor.domain.authority.mode)?;
    let mut witnesses = build_row_witnesses(descriptor.domain.role).then(Vec::new);
    let tail = walk_rows(
        &snapshot,
        descriptor,
        |event, _, _| replay(&mut journal, event),
        |first, key, value| {
            if let Some(rows) = &mut witnesses {
                rows.push(RowWitness::new(first, key, value));
            }
        },
    )?;
    anyhow::ensure!(
        journal.cohort() == Some(&descriptor.domain.cohort),
        "ledger replay cohort mismatch"
    );
    Ok(Loaded {
        journal,
        descriptor: descriptor.clone(),
        tail,
        row_witnesses: witnesses.map(Arc::from),
    })
}

/// An integrity expectation minted from one strictly validated, published
/// coordinator or member state. It contains no Journal, events or drained transaction collection,
/// is not serializable, and has no descriptor-only or public constructor.
/// SHA-256 equality proves integrity relative to this cut, not authentication.
#[derive(Debug)]
pub(super) struct PublishedArchiveCut {
    descriptor: Descriptor,
    row_witnesses: RowWitnesses,
    metadata: Vec<u8>,
    decision_manifest: Vec<u8>,
    resolved_prefix: u64,
    terminal_through: u64,
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
        let staged = Staged::new(state, self.descriptor.clone(), self.row_witnesses.clone())?;
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
        let journal = View::of(state)
            .ok_or_else(|| anyhow::anyhow!("missing full archive"))?
            .journal;
        let checkpoint = ActiveCheckpoint::from_journal(journal).map_err(history_error)?;
        if super::state_codec::d1_metadata_bytes(state, Some(self))? != metadata {
            match descriptor.domain.role {
                Role::Coordinator => {
                    super::state_codec::validate_active_metadata(metadata, descriptor, &checkpoint)?
                }
                Role::Member => super::state_codec::validate_active_member_metadata(
                    metadata,
                    descriptor,
                    &checkpoint,
                    state,
                )?,
            }
        }
        anyhow::ensure!(
            crate::decision_segments::manifest_bytes(state, descriptor.applied)?
                == decision_manifest,
            "published archive decision manifest differs from staged state"
        );
        validate_row_witnesses(descriptor, &self.row_witnesses)?;
        Ok(PublishedArchiveCut {
            descriptor: descriptor.clone(),
            row_witnesses: self.row_witnesses.clone(),
            metadata: metadata.to_vec(),
            decision_manifest: decision_manifest.to_vec(),
            terminal_through: state
                .visibility
                .as_ref()
                .map_or(journal.resolved_prefix(), |v| v.applied),
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

fn build_row_witnesses(role: Role) -> bool {
    #[cfg(test)]
    if ORIGINAL_COORDINATOR_VERIFY.get() {
        return false;
    }
    role == Role::Coordinator
}

fn validate_row_witnesses(descriptor: &Descriptor, witnesses: &RowWitnesses) -> anyhow::Result<()> {
    #[cfg(test)]
    if ORIGINAL_COORDINATOR_VERIFY.get() {
        return Ok(());
    }
    match (descriptor.domain.role, witnesses) {
        (Role::Member, None) => Ok(()),
        (Role::Coordinator, Some(rows)) => {
            anyhow::ensure!(
                rows.len() as u64 == descriptor.segment_count
                    && rows.len() as u64 <= max_segments(descriptor.domain.cohort.profile)
                    && rows.first().is_some_and(|row| row.first == 1)
                    && rows.windows(2).all(|pair| pair[0].first < pair[1].first)
                    && rows
                        .last()
                        .is_some_and(|row| row.first <= descriptor.event_count),
                "invalid private row inventory"
            );
            let bytes = rows
                .iter()
                .try_fold(0u64, |total, row| total.checked_add(row.bytes))
                .ok_or_else(|| anyhow::anyhow!("row inventory bytes overflow"))?;
            anyhow::ensure!(
                bytes == descriptor.encoded_bytes,
                "row inventory byte total mismatch"
            );
            Ok(())
        }
        _ => anyhow::bail!("row inventory role mismatch"),
    }
}

#[cfg(test)]
thread_local! { static ORIGINAL_COORDINATOR_VERIFY: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }
#[cfg(test)]
pub(super) fn with_original_coordinator_verify<T>(run: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            ORIGINAL_COORDINATOR_VERIFY.set(self.0);
        }
    }
    let _reset = Reset(ORIGINAL_COORDINATOR_VERIFY.replace(true));
    run()
}

/// Complete byte coverage against a private, strictly minted coordinator witness.
/// Every invocation rereads the namespace and applied marker. Only decoding of
/// byte-identical sealed rows is elided, never their reads or integrity checks.
fn verify_coordinator_rows(
    snapshot: &Snapshot<'_>,
    cut: &PublishedArchiveCut,
) -> anyhow::Result<Tail> {
    #[cfg(test)]
    if ORIGINAL_COORDINATOR_VERIFY.get() {
        return walk(snapshot, &cut.descriptor, |_, _, _| Ok(()));
    }
    #[cfg(test)]
    RAW_WALKS.set(RAW_WALKS.get() + 1);
    let descriptor = &cut.descriptor;
    descriptor.validate()?;
    anyhow::ensure!(
        descriptor.domain.role == Role::Coordinator,
        "row witness role mismatch"
    );
    let expected = cut
        .row_witnesses
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("missing private row witnesses"))?;
    anyhow::ensure!(
        expected.len() as u64 == descriptor.segment_count,
        "row witness count mismatch"
    );
    let applied: Option<LogId<NodeId>> = match snapshot.get(super::APPLIED_KEY)? {
        Some(bytes) => serde_json::from_slice(&bytes)?,
        None => None,
    };
    anyhow::ensure!(
        applied == descriptor.applied,
        "ledger applied marker mismatch"
    );
    let mut count = 0usize;
    let mut encoded = 0u64;
    let mut tail = None;
    for item in snapshot.iterator(IteratorMode::From(PREFIX, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(PREFIX) {
            break;
        }
        let witness = expected
            .get(count)
            .ok_or_else(|| anyhow::anyhow!("extra ledger row"))?;
        anyhow::ensure!(
            key.as_ref() == descriptor.domain.key(witness.first) && witness.matches(&key, &value),
            "ledger row differs from private byte witness"
        );
        encoded = encoded
            .checked_add(witness.bytes)
            .ok_or_else(|| anyhow::anyhow!("ledger row byte overflow"))?;
        count += 1;
        if count == expected.len() {
            let row = Row::decode(&value)?;
            anyhow::ensure!(
                row.domain == descriptor.domain
                    && row.first == witness.first
                    && row.after_root == descriptor.head_root
                    && row
                        .first
                        .checked_add(row.events.len() as u64)
                        .and_then(|n| n.checked_sub(1))
                        == Some(descriptor.event_count),
                "private tail boundary mismatch"
            );
            let before_count = row
                .first
                .checked_sub(1)
                .ok_or_else(|| anyhow::anyhow!("invalid private tail position"))?;
            let mut event_bytes = 0usize;
            for event in &row.events {
                event_bytes = event_bytes
                    .checked_add(event_size(event)?)
                    .ok_or_else(|| anyhow::anyhow!("ledger tail byte overflow"))?;
            }
            anyhow::ensure!(
                event_bytes <= MAX_HISTORY_PAGE_BYTES,
                "private tail byte bound"
            );
            tail = Some(Tail {
                first: row.first,
                before: HistoryCursor {
                    version: descriptor.domain.cohort.profile.history_version(),
                    authority: descriptor.domain.authority.clone(),
                    cohort: descriptor.domain.cohort.clone(),
                    event_count: before_count,
                    root: row.before_root,
                },
                count: row.events.len(),
                events: row.events,
                event_bytes,
                encoded_bytes: witness.bytes,
            });
        }
    }
    anyhow::ensure!(
        count == expected.len() && encoded == descriptor.encoded_bytes,
        "incomplete ledger byte inventory"
    );
    tail.ok_or_else(|| anyhow::anyhow!("missing ledger tail"))
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
    verify_active_inner(db, cut, false).map(|(archive, _)| archive)
}

/// The member apply path also needs the prior partial terminal segment. Retain
/// only those original transactions while performing the same complete checks.
/// This result belongs to this snapshot and is never retained across operations.
pub(super) fn verify_member_apply<'db, 'cut>(
    db: &'db DB,
    cut: &'cut PublishedArchiveCut,
) -> Result<(VerifiedArchive<'db, 'cut>, Vec<Transaction>), D1Error> {
    verify_active_inner(db, cut, true)
}

fn verify_active_inner<'db, 'cut>(
    db: &'db DB,
    cut: &'cut PublishedArchiveCut,
    retain_member_tail: bool,
) -> Result<(VerifiedArchive<'db, 'cut>, Vec<Transaction>), D1Error> {
    let snapshot = cut.snapshot(db)?;
    let mut terminal_tail = Vec::new();
    cut.checkpoint
        .validate()
        .map_err(|_| D1Error::EventLedgerHistory)?;
    let tail = if cut.descriptor.domain.role == Role::Member {
        let domain = &cut.descriptor.domain;
        let identity = crate::decision_segments::CheckpointIdentity {
            role: domain.role.decision_role(),
            authority: domain.authority.clone(),
            shard: domain.shard,
            epoch: domain.epoch,
            mode: domain.mode,
            cohort: domain.cohort.clone(),
        };
        let decision_error = |error: anyhow::Error| {
            if error.is::<rocksdb::Error>() {
                D1Error::DecisionIndexUnavailable
            } else {
                D1Error::DecisionIndexHistory
            }
        };
        let mut rows = crate::decision_segments::CheckpointRows::new(
            &identity,
            cut.terminal_through,
            cut.descriptor.applied,
            &cut.decision_manifest,
            snapshot.iterator(IteratorMode::From(
                crate::decision_segments::PREFIX,
                Direction::Forward,
            )),
        )
        .map_err(decision_error)?;
        // The manifest above validates the cut before interval arithmetic. A
        // sealed boundary has no partial tail, even if R is greater than A.
        let tail_count = if retain_member_tail {
            cut.terminal_through % crate::terminal_prefix::SEGMENT_TRANSACTIONS as u64
        } else {
            0
        };
        let tail_after = cut.terminal_through - tail_count;
        terminal_tail.reserve_exact(tail_count as usize);
        let mut overflowed_tail = false;
        let mut projection = OrderedTerminals::new(domain.cohort.profile);
        let mut failed_decision = None;
        let result = walk(&snapshot, &cut.descriptor, |event, _, _| {
            projection.observe(&event, &mut |tx| {
                rows.observe(&tx).map_err(|error| {
                    failed_decision = Some(error);
                    anyhow::anyhow!("derived decision verification failed")
                })?;
                if tail_after < tx.ordinal && tx.ordinal <= cut.terminal_through {
                    // Never change existing error precedence or retain more
                    // than one partial segment on a malformed projection.
                    if terminal_tail.len() < tail_count as usize {
                        terminal_tail.push(tx);
                    } else {
                        overflowed_tail = true;
                    }
                }
                Ok(())
            })
        });
        if let Some(error) = failed_decision {
            return Err(decision_error(error));
        }
        let tail = result.map_err(super::ledger_read_error)?;
        projection
            .matches(&cut.checkpoint)
            .map_err(super::ledger_read_error)?;
        rows.finish().map_err(decision_error)?;
        if overflowed_tail
            || terminal_tail.len() != tail_count as usize
            || terminal_tail
                .iter()
                .enumerate()
                .any(|(offset, tx)| tx.ordinal != tail_after + offset as u64 + 1)
        {
            return Err(D1Error::EventLedgerHistory);
        }
        tail
    } else {
        verify_coordinator_rows(&snapshot, cut).map_err(super::ledger_read_error)?
    };
    Ok((
        VerifiedArchive {
            snapshot,
            cut,
            tail,
        },
        terminal_tail,
    ))
}

/// Immutable operation-local semantic witness. Only strict original-event
/// extensions can mint a successor; clones share the same accepted state.
#[derive(Clone)]
pub(super) struct MemberOverlay<'operation, 'db, 'cut> {
    archive: &'operation VerifiedArchive<'db, 'cut>,
    checkpoint: Arc<ActiveCheckpoint>,
    commands: Arc<Vec<Command>>,
}
impl<'operation, 'db, 'cut> MemberOverlay<'operation, 'db, 'cut> {
    pub(super) fn checkpoint_arc(&self) -> &Arc<ActiveCheckpoint> {
        &self.checkpoint
    }
    pub(super) fn commands(&self) -> &[Command] {
        self.commands.as_slice()
    }
    pub(super) fn head(&self) -> anyhow::Result<HistoryCursor> {
        self.archive
            .head()
            .extend(self.commands())
            .map_err(history_error)
    }
    pub(super) fn validate_candidate(
        &self,
        archive: &VerifiedArchive<'db, 'cut>,
        checkpoint: &Arc<ActiveCheckpoint>,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            std::ptr::eq(self.archive, archive) && Arc::ptr_eq(&self.checkpoint, checkpoint),
            "member overlay belongs to a different snapshot or checkpoint"
        );
        Ok(())
    }
    pub(super) fn lookup(&self, key: LookupKey) -> anyhow::Result<LookupResult> {
        self.archive.lookup(key, self.commands())
    }
    pub(super) fn transaction_at(&self, ordinal: u64) -> anyhow::Result<Transaction> {
        self.archive
            .transactions(ordinal, ordinal, self.commands())?
            .pop()
            .ok_or_else(|| anyhow::anyhow!("missing retained transaction ordinal"))
    }
    fn append(&self, events: &[Command]) -> anyhow::Result<Self> {
        if events.is_empty() {
            return Ok(self.clone());
        }
        let mut candidate = self.checkpoint.as_ref().clone();
        let mut accepted = self.commands.as_ref().clone();
        for event in events {
            let mut failure = None;
            let result = candidate.apply(event.clone(), &mut |key| {
                self.archive.lookup(key, &accepted).map_err(|error| {
                    failure = Some(error);
                    crate::transaction_decisions::Error::InvalidJournal
                })
            });
            if let Some(error) = failure {
                return Err(error);
            }
            anyhow::ensure!(
                result.map_err(history_error)?.accepted.as_ref() == Some(event),
                "member history does not append an original command"
            );
            accepted.push(event.clone());
        }
        self.archive.check_overlay(&accepted)?;
        Ok(Self {
            archive: self.archive,
            checkpoint: Arc::new(candidate),
            commands: Arc::new(accepted),
        })
    }
    pub(super) fn merge_compatibility_journal(&self, incoming: &Journal) -> anyhow::Result<Self> {
        self.compatible_journal(incoming)?;
        let skip = usize::try_from(self.checkpoint.event_count())
            .map_err(|_| anyhow::anyhow!("local history event offset overflow"))?
            .min(incoming.events().len());
        self.append(&incoming.events()[skip..])
    }

    fn walk_overlay(
        &self,
        mut observe: impl FnMut(&Command, &HistoryCursor) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        walk(
            &self.archive.snapshot,
            &self.archive.cut.descriptor,
            |event, cursor, _| observe(&event, cursor),
        )?;
        let mut cursor = self.archive.head();
        for event in self.commands() {
            cursor = cursor
                .extend(std::slice::from_ref(event))
                .map_err(history_error)?;
            observe(event, &cursor)?;
        }
        Ok(())
    }

    pub(super) fn compatible_journal(&self, incoming: &Journal) -> anyhow::Result<()> {
        let domain = &self.archive.cut.descriptor.domain;
        anyhow::ensure!(
            incoming.authority() == domain.authority.mode
                && incoming.cohort() == Some(&domain.cohort),
            "incompatible full history domain"
        );
        let incoming_cut = incoming.history_cursor().map_err(history_error)?;
        incoming_cut
            .validate_identity(&domain.authority, &domain.cohort)
            .map_err(history_error)?;
        let compare = |event: &Command, cursor: &HistoryCursor| -> anyhow::Result<()> {
            if cursor.event_count <= incoming_cut.event_count {
                let offset = usize::try_from(cursor.event_count - 1)
                    .map_err(|_| anyhow::anyhow!("incoming history offset overflow"))?;
                anyhow::ensure!(
                    incoming.events().get(offset) == Some(event),
                    "full history overlap changed original command"
                );
                if cursor.event_count == incoming_cut.event_count {
                    anyhow::ensure!(cursor == &incoming_cut, "full history root mismatch");
                }
            }
            Ok(())
        };
        self.walk_overlay(compare)
    }

    /// Freeze this target at the exact retained event cut. Later admissions or
    /// decisions cannot supply old authorization, but all later rows still close.
    pub(super) fn target_at_cut(
        &self,
        cut: &HistoryCursor,
        id: [u8; 16],
    ) -> anyhow::Result<Option<TargetRecord>> {
        let domain = &self.archive.cut.descriptor.domain;
        cut.validate_identity(&domain.authority, &domain.cohort)
            .map_err(history_error)?;
        let mut matched = cut == &domain.zero_cursor()?;
        let mut projection = Projection::new(LookupKey::Transaction(id));
        self.walk_overlay(|event, cursor| {
            if cursor.event_count <= cut.event_count {
                projection.observe(event)?;
            }
            if cursor.event_count == cut.event_count {
                anyhow::ensure!(cursor == cut, "target cut differs from retained history");
                matched = true;
            }
            Ok(())
        })?;
        anyhow::ensure!(matched, "target cut is not retained");
        let result = projection.finish_target()?;
        if let Some(record) = &result {
            record
                .validate(&domain.authority, &domain.cohort)
                .map_err(history_error)?;
        }
        Ok(result)
    }

    pub(super) fn request_for_transaction(
        &self,
        id: [u8; 16],
    ) -> anyhow::Result<Option<RequestRecord>> {
        Ok(self
            .target_at_cut(&self.head()?, id)?
            .and_then(|target| target.request))
    }

    /// Freeze the first-target resolution predicate at exactly the supplied
    /// cursor, then finish all later closure before returning any evidence.
    pub(super) fn prove_resolved(&self, cut: &HistoryCursor, target: u64) -> anyhow::Result<bool> {
        let domain = &self.archive.cut.descriptor.domain;
        cut.validate_identity(&domain.authority, &domain.cohort)
            .map_err(history_error)?;
        let zero = domain.zero_cursor()?;
        let mut frozen = (cut.event_count == 0 && cut == &zero).then_some(false);
        let mut admitted = 0u64;
        let mut unresolved = std::collections::BTreeSet::new();
        self.walk_overlay(|event, cursor| {
            if cursor.event_count <= cut.event_count {
                match event {
                    Command::Begin { id, .. }
                    | Command::AdmitRequest {
                        transaction_id: id, ..
                    } => {
                        admitted = admitted
                            .checked_add(1)
                            .ok_or_else(|| anyhow::anyhow!("ordinal overflow"))?;
                        if admitted <= target {
                            anyhow::ensure!(
                                unresolved.len() < domain.cohort.profile.max_transactions(),
                                "frozen proof exceeds transaction bound"
                            );
                            anyhow::ensure!(
                                unresolved.insert(*id),
                                "duplicate unresolved admission"
                            );
                        }
                    }
                    Command::Decide { id, .. } => {
                        unresolved.remove(id);
                    }
                    _ => {}
                }
                if cursor.event_count == cut.event_count {
                    anyhow::ensure!(cursor == cut, "frozen history cut mismatch");
                    frozen = Some(target != 0 && admitted >= target && unresolved.is_empty());
                }
            }
            Ok(())
        })?;
        frozen.ok_or_else(|| anyhow::anyhow!("frozen history cut is not retained"))
    }

    pub(super) fn merge_history_page(&self, page: &HistoryPage) -> anyhow::Result<Self> {
        page.validate().map_err(history_error)?;
        let domain = &self.archive.cut.descriptor.domain;
        page.base
            .validate_identity(&domain.authority, &domain.cohort)
            .map_err(history_error)?;
        anyhow::ensure!(
            page.base.event_count <= self.checkpoint.event_count(),
            "history page base beyond local observation"
        );
        let zero = domain.zero_cursor()?;
        let mut base_seen = page.base == zero;
        let mut end_seen = page.end == zero;
        let mut cut_seen = page.cut == zero;
        self.walk_overlay(|event, cursor| {
            if cursor.event_count == page.base.event_count {
                base_seen = cursor == &page.base;
            }
            if cursor.event_count == page.end.event_count {
                end_seen = cursor == &page.end;
            }
            if cursor.event_count == page.cut.event_count {
                cut_seen = cursor == &page.cut;
            }
            if cursor.event_count > page.base.event_count
                && cursor.event_count <= page.end.event_count
            {
                let index = (cursor.event_count - page.base.event_count - 1) as usize;
                anyhow::ensure!(
                    page.events.get(index) == Some(event),
                    "history overlap changed original command"
                );
            }
            Ok(())
        })?;
        anyhow::ensure!(base_seen, "history page base mismatch");
        if page.end.event_count <= self.checkpoint.event_count() {
            anyhow::ensure!(end_seen, "history page end mismatch");
        }
        if page.cut.event_count <= self.checkpoint.event_count() {
            anyhow::ensure!(cut_seen, "history page cut mismatch");
        }
        let skip = usize::try_from(self.checkpoint.event_count() - page.base.event_count)
            .map_err(|_| anyhow::anyhow!("history page offset overflow"))?
            .min(page.events.len());
        self.append(&page.events[skip..])
    }
}

impl<'db, 'cut> VerifiedArchive<'db, 'cut> {
    pub(super) fn member_overlay(&self) -> MemberOverlay<'_, 'db, 'cut> {
        MemberOverlay {
            archive: self,
            checkpoint: Arc::clone(&self.cut.checkpoint),
            commands: Arc::new(Vec::new()),
        }
    }
    pub(super) fn head(&self) -> HistoryCursor {
        self.cut.head()
    }

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

    pub(super) fn terminal_through(&self) -> u64 {
        self.cut.terminal_through
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

/// A semantic projection of an already published original sequence. Only the
/// unresolved contiguous interval (R,N] and an active-only ID index survive each
/// event. No retired IDs, Plans, requests or Commands are retained.
struct OrderedTerminals {
    active_bytes: usize,
    profile: CapacityProfile,
    admitted: u64,
    resolved: u64,
    order: std::collections::VecDeque<[u8; 16]>,
    active: std::collections::BTreeMap<[u8; 16], Transaction>,
}
impl OrderedTerminals {
    // Borrow every variable-width field so admission is sized before allocating
    // resident participants/prepared bits or inserting either active index entry.
    fn record_bytes(
        id: [u8; 16],
        ordinal: u64,
        participants: &[crate::transaction_decisions::Participant],
        prepared: &[bool],
        decision: Option<crate::transaction_decisions::Decision>,
    ) -> anyhow::Result<usize> {
        #[derive(Serialize)]
        struct Record<'a> {
            id: [u8; 16],
            ordinal: u64,
            participants: &'a [crate::transaction_decisions::Participant],
            prepared: &'a [bool],
            decision: Option<crate::transaction_decisions::Decision>,
        }
        crate::capacity_policy::canonical_size(
            &Record {
                id,
                ordinal,
                participants,
                prepared,
                decision,
            },
            crate::transaction_decisions::checkpoint::MAX_ACTIVE_CHECKPOINT_BYTES,
        )
        .map_err(history_error)?
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("active projection record byte overflow"))
    }
    fn tx_bytes(tx: &Transaction) -> anyhow::Result<usize> {
        Self::record_bytes(
            tx.id,
            tx.ordinal,
            &tx.participants,
            &tx.prepared,
            tx.decision,
        )
    }
    fn replace_bytes(total: usize, old: usize, new: usize) -> anyhow::Result<usize> {
        total
            .checked_sub(old)
            .and_then(|n| n.checked_add(new))
            .filter(|n| *n <= crate::transaction_decisions::checkpoint::MAX_ACTIVE_CHECKPOINT_BYTES)
            .ok_or_else(|| {
                anyhow::anyhow!("active projection canonical bytes exceed checkpoint bound")
            })
    }
    fn new(profile: CapacityProfile) -> Self {
        Self {
            active_bytes: 0,
            profile,
            admitted: 0,
            resolved: 0,
            order: std::collections::VecDeque::new(),
            active: std::collections::BTreeMap::new(),
        }
    }
    fn observe(
        &mut self,
        event: &Command,
        emit: &mut impl FnMut(Transaction) -> anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        match event {
            Command::Begin { id, participants }
            | Command::AdmitRequest {
                transaction_id: id,
                participants,
                ..
            } => {
                anyhow::ensure!(
                    self.admitted < self.profile.max_transactions() as u64
                        && self.order.len() < self.profile.max_transactions()
                        && !participants.is_empty()
                        && participants.len() <= crate::transaction_decisions::MAX_PARTICIPANTS,
                    "ordered projection exceeds original profile bounds"
                );
                anyhow::ensure!(!self.active.contains_key(id), "duplicate active admission");
                let next = self
                    .admitted
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("active admission ordinal overflow"))?;
                let prepared = [false; crate::transaction_decisions::MAX_PARTICIPANTS];
                let bytes = Self::record_bytes(
                    *id,
                    next,
                    participants,
                    &prepared[..participants.len()],
                    None,
                )?;
                self.active_bytes = Self::replace_bytes(self.active_bytes, 0, bytes)?;
                self.admitted = next;
                self.active.insert(
                    *id,
                    Transaction {
                        id: *id,
                        ordinal: self.admitted,
                        participants: participants.clone(),
                        prepared: vec![false; participants.len()],
                        decision: None,
                    },
                );
                self.order.push_back(*id);
            }
            Command::Prepared { id, participant } => {
                let tx = self
                    .active
                    .get_mut(id)
                    .ok_or_else(|| anyhow::anyhow!("prepare outside active interval"))?;
                let old = Self::tx_bytes(tx)?;
                let mut prepared = [false; crate::transaction_decisions::MAX_PARTICIPANTS];
                prepared[..tx.prepared.len()].copy_from_slice(&tx.prepared);
                if let Some(index) = tx.participants.iter().position(|p| p == participant) {
                    prepared[index] = true;
                }
                let new = Self::record_bytes(
                    tx.id,
                    tx.ordinal,
                    &tx.participants,
                    &prepared[..tx.prepared.len()],
                    tx.decision,
                )?;
                let total = Self::replace_bytes(self.active_bytes, old, new)?;
                anyhow::ensure!(
                    tx.record_prepared(participant).map_err(history_error)?,
                    "duplicate retained prepare"
                );
                self.active_bytes = total;
            }
            Command::Decide { id, decision } => {
                let tx = self
                    .active
                    .get_mut(id)
                    .ok_or_else(|| anyhow::anyhow!("decision outside active interval"))?;
                let old = Self::tx_bytes(tx)?;
                let new = Self::record_bytes(
                    tx.id,
                    tx.ordinal,
                    &tx.participants,
                    &tx.prepared,
                    Some(*decision),
                )?;
                let total = Self::replace_bytes(self.active_bytes, old, new)?;
                anyhow::ensure!(
                    tx.record_decision(*decision).map_err(history_error)?,
                    "duplicate retained decision"
                );
                self.active_bytes = total;
            }
            _ => {}
        }
        // Same contiguous-front reduction as ActiveCheckpoint::apply: never
        // jump over a hole, and release a record only after a terminal outcome.
        while let Some(id) = self.order.front().copied() {
            let tx = self
                .active
                .get(&id)
                .ok_or_else(|| anyhow::anyhow!("active projection index mismatch"))?;
            let Some(next) =
                crate::transaction_decisions::checkpoint::next_resolved(self.resolved, tx)
                    .map_err(history_error)?
            else {
                break;
            };
            self.active_bytes = Self::replace_bytes(self.active_bytes, Self::tx_bytes(tx)?, 0)?;
            let tx = self.active.remove(&id).unwrap();
            self.order.pop_front();
            self.resolved = next;
            emit(tx)?;
        }
        Ok(())
    }
    fn matches(&self, checkpoint: &ActiveCheckpoint) -> anyhow::Result<()> {
        anyhow::ensure!(
            self.admitted == checkpoint.transaction_count() as u64
                && self.resolved == checkpoint.resolved_prefix()
                && self.order.len() == checkpoint.active_transactions().len()
                && self
                    .order
                    .iter()
                    .zip(checkpoint.active_transactions())
                    .all(|(id, tx)| self.active.get(id) == Some(tx)),
            "ordered raw projection differs from published checkpoint"
        );
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
                if matches!(&self.key, LookupKey::Request(wanted) if wanted == id)
                    || matches!(&self.key, LookupKey::Transaction(wanted) if wanted == transaction_id)
                {
                    anyhow::ensure!(self.request.is_none(), "duplicate retained request");
                    self.request = Some(RequestRecord {
                        id: *id,
                        request: request.clone(),
                        payload: payload.clone(),
                        transaction_id: *transaction_id,
                    });
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

    fn finish_target(self) -> anyhow::Result<Option<TargetRecord>> {
        match self.transaction {
            Some(transaction) => {
                anyhow::ensure!(
                    transaction.ordinal <= self.ordinal,
                    "target exceeds cut admissions"
                );
                Ok(Some(TargetRecord {
                    transaction,
                    request: self.request,
                }))
            }
            None => {
                anyhow::ensure!(
                    self.request.is_none(),
                    "binding has no retained transaction"
                );
                Ok(None)
            }
        }
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
    base_checkpoint: Arc<ActiveCheckpoint>,
    base_descriptor: Descriptor,
    base_terminal_through: u64,
    descriptor: Descriptor,
    row_witnesses: RowWitnesses,
    // Operation-local output of the exact shared transitions below. These
    // records are handed to terminal staging and never retained by the cut.
    drained: Vec<Transaction>,
}
/// Private operation-local member witness. It owns only newly consumed terminals,
/// not the previously applied history, and borrows the exact metadata candidate.
pub(crate) struct StagedMemberTerminals<'a> {
    state: &'a D1State,
    descriptor: &'a Descriptor,
    previous: u64,
    previous_manifest: Vec<u8>,
    through: u64,
    records: Vec<Transaction>,
}
impl<'a> StagedMemberTerminals<'a> {
    pub(crate) fn state(&self) -> &'a D1State {
        self.state
    }
    pub(crate) fn descriptor(&self) -> &'a Descriptor {
        self.descriptor
    }
    pub(crate) fn previous(&self) -> u64 {
        self.previous
    }
    pub(crate) fn previous_manifest(&self) -> &[u8] {
        &self.previous_manifest
    }
    pub(crate) fn through(&self) -> u64 {
        self.through
    }
    pub(crate) fn records(&self) -> &[Transaction] {
        &self.records
    }
}

impl StagedActive<'_> {
    /// Bind exact original terminal projections to the already staged suffix and
    /// candidate. A caller cannot supply replacement terminal transaction data.
    pub(super) fn member_terminals<'a>(
        &'a self,
        before: &VerifiedArchive<'_, '_>,
        suffix: &[Command],
        state: &'a D1State,
    ) -> anyhow::Result<StagedMemberTerminals<'a>> {
        anyhow::ensure!(
            self.descriptor.domain.role == Role::Member
                && before.cut.descriptor.domain == self.descriptor.domain
                && self.base_descriptor == before.cut.descriptor
                && Arc::ptr_eq(&self.base_checkpoint, &before.cut.checkpoint)
                && self.base_terminal_through == before.cut.terminal_through,
            "member terminal witness has a different prior cut"
        );
        let visibility = state
            .visibility
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing member visibility"))?;
        let active = visibility
            .journal
            .active_arc()
            .ok_or_else(|| anyhow::anyhow!("member witness requires active history"))?;
        self.descriptor_for(active.as_ref())?;
        anyhow::ensure!(
            state.coordinator.is_none()
                && state.shard == self.descriptor.domain.shard
                && state.epoch == self.descriptor.domain.epoch
                && state
                    .managed
                    .as_ref()
                    .is_some_and(|mode| mode.mode_id == self.descriptor.domain.mode),
            "member terminal witness local identity mismatch"
        );
        let previous = before.cut.terminal_through;
        let through = visibility.applied;
        anyhow::ensure!(
            through >= previous
                && through <= self.candidate.resolved_prefix()
                && through <= self.descriptor.domain.cohort.profile.max_transactions() as u64,
            "member terminal witness frontier mismatch"
        );
        before.check_overlay(suffix)?;
        let head = before.head().extend(suffix).map_err(history_error)?;
        anyhow::ensure!(
            head.event_count == self.descriptor.event_count
                && head.root == self.descriptor.head_root,
            "member terminal witness suffix differs from staged raw history"
        );
        let mut records = Vec::new();
        if through > previous {
            let mut projection = OrderedTerminals::new(self.descriptor.domain.cohort.profile);
            let mut emit = |tx: Transaction| -> anyhow::Result<()> {
                if tx.ordinal > previous && tx.ordinal <= through {
                    anyhow::ensure!(
                        tx.ordinal == previous + records.len() as u64 + 1
                            && (records.len() as u64) < through - previous,
                        "member terminal projection ordinal mismatch"
                    );
                    records.push(tx);
                }
                Ok(())
            };
            walk(&before.snapshot, &before.cut.descriptor, |event, _, _| {
                projection.observe(&event, &mut emit)
            })?;
            for event in suffix {
                projection.observe(event, &mut emit)?;
            }
            projection.matches(self.candidate)?;
        }
        anyhow::ensure!(
            records.len() as u64 == through - previous,
            "incomplete member terminal projection"
        );
        Ok(StagedMemberTerminals {
            state,
            descriptor: &self.descriptor,
            previous,
            previous_manifest: before.cut.decision_manifest.clone(),
            through,
            records,
        })
    }

    pub(super) fn published_member_cut(
        &self,
        state: &D1State,
        checkpoint: &Arc<ActiveCheckpoint>,
        metadata: &[u8],
        decision_manifest: &crate::decision_segments::StagedMemberCheckpointManifest<'_>,
    ) -> anyhow::Result<PublishedArchiveCut> {
        let descriptor = self.descriptor_for(checkpoint.as_ref())?;
        anyhow::ensure!(
            std::ptr::eq(state, decision_manifest.state())
                && std::ptr::eq(descriptor, decision_manifest.descriptor()),
            "member terminal manifest belongs to a different state candidate"
        );
        anyhow::ensure!(
            descriptor.domain.role == Role::Member,
            "member archive role mismatch"
        );
        super::state_codec::validate_active_member_metadata(
            metadata, descriptor, checkpoint, state,
        )?;
        let visibility = state
            .visibility
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("missing member visibility"))?;
        let active = visibility
            .journal
            .active_arc()
            .ok_or_else(|| anyhow::anyhow!("member publication requires active history"))?;
        anyhow::ensure!(
            Arc::ptr_eq(active, checkpoint),
            "member publication does not share candidate checkpoint"
        );
        let through = visibility.applied;
        let bytes = decision_manifest.bytes();
        crate::decision_segments::validate_checkpoint_manifest(
            &crate::decision_segments::CheckpointIdentity {
                role: descriptor.domain.role.decision_role(),
                authority: descriptor.domain.authority.clone(),
                shard: descriptor.domain.shard,
                epoch: descriptor.domain.epoch,
                mode: descriptor.domain.mode,
                cohort: descriptor.domain.cohort.clone(),
            },
            through,
            descriptor.applied,
            bytes,
        )?;
        validate_row_witnesses(descriptor, &self.row_witnesses)?;
        Ok(PublishedArchiveCut {
            descriptor: descriptor.clone(),
            row_witnesses: self.row_witnesses.clone(),
            metadata: metadata.to_vec(),
            decision_manifest: bytes.to_vec(),
            terminal_through: through,
            resolved_prefix: checkpoint.resolved_prefix(),
            checkpoint: Arc::clone(checkpoint),
        })
    }

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
                role: descriptor.domain.role.decision_role(),
                authority: descriptor.domain.authority.clone(),
                shard: descriptor.domain.shard,
                epoch: descriptor.domain.epoch,
                mode: descriptor.domain.mode,
                cohort: descriptor.domain.cohort.clone(),
            },
            checkpoint.resolved_prefix(),
            descriptor.applied,
            decision_manifest,
        )?;
        validate_row_witnesses(descriptor, &self.row_witnesses)?;
        Ok(PublishedArchiveCut {
            descriptor: descriptor.clone(),
            row_witnesses: self.row_witnesses.clone(),
            metadata: metadata.to_vec(),
            decision_manifest: decision_manifest.to_vec(),
            terminal_through: checkpoint.resolved_prefix(),
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
            base_checkpoint: Arc::clone(&before.cut.checkpoint),
            base_descriptor: before.cut.descriptor.clone(),
            base_terminal_through: before.cut.terminal_through,
            descriptor: Descriptor {
                applied,
                ..old.clone()
            },
            drained,
            row_witnesses: before.cut.row_witnesses.clone(),
        });
    }
    let mut witnesses = before.cut.row_witnesses.as_ref().map(|rows| rows.to_vec());
    if let Some(rows) = &mut witnesses {
        rows.pop();
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
            let witness = write_active_row(
                batch,
                before,
                &mut cursor,
                &events,
                bytes,
                &mut segments,
                &mut encoded,
            )?;
            if let (Some(rows), Some(witness)) = (&mut witnesses, witness) {
                rows.push(witness);
            }
            events.clear();
            bytes = 0;
        }
        events.push(event.clone());
        bytes = bytes
            .checked_add(size)
            .ok_or_else(|| anyhow::anyhow!("raw tail bytes overflow"))?;
    }
    let witness = write_active_row(
        batch,
        before,
        &mut cursor,
        &events,
        bytes,
        &mut segments,
        &mut encoded,
    )?;
    if let (Some(rows), Some(witness)) = (&mut witnesses, witness) {
        rows.push(witness);
    }
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
        base_checkpoint: Arc::clone(&before.cut.checkpoint),
        base_descriptor: before.cut.descriptor.clone(),
        base_terminal_through: before.cut.terminal_through,
        descriptor,
        drained,
        row_witnesses: witnesses.map(Arc::from),
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
) -> anyhow::Result<Option<RowWitness>> {
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
    let witness = before
        .cut
        .row_witnesses
        .as_ref()
        .map(|_| RowWitness::new(first, &key, &value));
    if first != before.tail.first || events != before.tail.events.as_slice() {
        batch.put(key, value);
    }
    *cursor = after;
    Ok(witness)
}

/// Exact current transaction from the same anchored cut as its E/R metadata.
/// Complete namespace closure precedes both present and absent results.
pub(super) fn lookup_target(
    db: &DB,
    cut: &PublishedArchiveCut,
    id: [u8; 16],
) -> Result<TargetEvidence, D1Error> {
    // Coordinator cuts require raw closure, not member terminal-row replay.
    // Keep the same private metadata/manifest/namespace anchor and perform that
    // closure in the target projection's single walk of this one snapshot.
    if cut.descriptor.domain.role != Role::Coordinator {
        return Err(D1Error::EventLedgerHistory);
    }
    let snapshot = cut.snapshot(db)?;
    cut.checkpoint
        .validate()
        .map_err(|_| D1Error::EventLedgerHistory)?;
    if cut.checkpoint.authority() != cut.descriptor.domain.authority.mode
        || cut.checkpoint.cohort() != Some(&cut.descriptor.domain.cohort)
        || cut.checkpoint.event_count() != cut.descriptor.event_count
        || cut.checkpoint.resolved_prefix() != cut.resolved_prefix
    {
        return Err(D1Error::EventLedgerHistory);
    }
    let mut projection = Projection::new(LookupKey::Transaction(id));
    walk(&snapshot, &cut.descriptor, |event, _, _| {
        projection.observe(&event)
    })
    .map_err(super::ledger_read_error)?;
    if projection.ordinal != cut.checkpoint.transaction_count() as u64
        || cut.resolved_prefix > projection.ordinal
    {
        return Err(D1Error::EventLedgerHistory);
    }
    let result = TargetEvidence {
        version: 1,
        cut: cut.head(),
        resolved_prefix: cut.resolved_prefix,
        id,
        record: projection
            .finish_target()
            .map_err(super::ledger_read_error)?,
    };
    result.validate().map_err(|_| D1Error::EventLedgerHistory)?;
    Ok(result)
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
    let mut projection = Projection::new(LookupKey::Request(id));
    walk(&snapshot, &cut.descriptor, |event, _, _| {
        projection.observe(&event)
    })
    .map_err(super::ledger_read_error)?;
    let record = match projection
        .finish_target()
        .map_err(super::ledger_read_error)?
    {
        Some(found) => Some(RecoveredRequest {
            record: found.request.ok_or(D1Error::RequestIndexHistory)?,
            transaction: found.transaction,
        }),
        None => None,
    };
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
                before.row_witnesses.clone(),
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
    let mut witnesses = build_row_witnesses(view.domain.role).then(|| {
        let mut rows = prior
            .and_then(|loaded| loaded.row_witnesses.as_ref())
            .map_or_else(Vec::new, |rows| rows.to_vec());
        if prior.is_some() {
            rows.pop();
        }
        rows
    });
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
        if let Some(rows) = &mut witnesses {
            rows.push(RowWitness::new(row.first, &key, &bytes));
        }
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
    Staged::new(candidate, descriptor, witnesses.map(Arc::from)).map(Some)
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

    fn tail_member_state(profile: CapacityProfile, count: u64, applied: u64) -> D1State {
        let mut coordinator = if profile.is_legacy() {
            state(bound())
        } else {
            byte_history(0)
        };
        let journal = coordinator
            .coordinator
            .as_mut()
            .unwrap()
            .full_mut()
            .unwrap();
        // Out-of-order completions drain in admission order. Some aborted
        // transactions have prepared bits, and unresolved suffix records remain.
        for n in 1..=count {
            journal.replay_event(begin(n)).unwrap();
            if n % 3 == 0 {
                journal
                    .replay_event(Command::Prepared {
                        id: id(n),
                        participant: participant(),
                    })
                    .unwrap();
            }
        }
        let resolved = count.saturating_sub(2).max(applied);
        for n in (1..=resolved).rev() {
            journal.replay_event(decide(n, Decision::Abort)).unwrap();
        }
        if count > resolved + 1 {
            journal
                .replay_event(decide(count, Decision::Abort))
                .unwrap();
        }
        let journal = coordinator
            .coordinator
            .take()
            .unwrap()
            .full()
            .unwrap()
            .clone();
        let mut member = D1State::new(7, 1);
        member.version = if profile.is_legacy() { 6 } else { 7 };
        member.managed = Some(crate::D1ManagedMode {
            mode_id: [7; 16],
            activated_sequence: 0,
        });
        member.participant = Some(crate::participant_writes::State {
            authority: Authority {
                shard: 9,
                mode: [9; 16],
            },
            activated_sequence: 0,
            records: Vec::new(),
        });
        member.visibility = Some(crate::versioned_reads::State {
            baseline_sequence: 0,
            epoch: 1,
            applied,
            journal: journal.into(),
            commits: Vec::new(),
            activation: (!profile.is_legacy()).then(|| CapacityActivation {
                nonce: [3; 32],
                log_id: marker(1).unwrap(),
            }),
        });
        member
            .visibility
            .as_ref()
            .unwrap()
            .validate(&member)
            .unwrap();
        member
    }

    #[test]
    fn member_tail_fusion_matches_original_projection_at_boundaries() {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            for applied in [0, 1, 31, 32, 33, 63, 64, 65, 1024, 1025] {
                let count = (applied + 7).min(profile.max_transactions() as u64);
                if applied > profile.max_transactions() as u64 {
                    continue;
                }
                let dir = tempfile::tempdir().unwrap();
                let db = DB::open_default(dir.path()).unwrap();
                let state = tail_member_state(profile, count, applied);
                let journal = state.visibility.as_ref().unwrap().journal.full().unwrap();
                let cut = persist_archive(&db, &state, marker(10));
                let expected: Vec<_> = if applied % 32 == 0 {
                    Vec::new()
                } else {
                    (applied / 32 * 32 + 1..=applied)
                        .map(|ordinal| journal.transaction_at(ordinal).unwrap().clone())
                        .collect()
                };
                crate::transaction_decisions::without_full_journal(|| {
                    let before = raw_walks();
                    let ordinary = verify_active(&db, &cut).unwrap();
                    let projected = if expected.is_empty() {
                        Vec::new()
                    } else {
                        ordinary
                            .transactions(applied / 32 * 32 + 1, applied, &[])
                            .unwrap()
                    };
                    assert_eq!(raw_walks() - before, 1 + u64::from(!expected.is_empty()));
                    let before = raw_walks();
                    let (fused, tail) = verify_member_apply(&db, &cut).unwrap();
                    assert_eq!(raw_walks() - before, 1);
                    assert_eq!(tail, expected, "profile={profile:?}, A={applied}");
                    assert_eq!(tail, projected);
                    assert!(tail.len() < 32);
                    assert_eq!(fused.checkpoint(), ordinary.checkpoint());
                    let (_, absent) = verify_active_inner(&db, &cut, false).unwrap();
                    assert!(absent.is_empty());
                });
            }
        }
    }

    #[test]
    fn member_tail_fusion_keeps_maximum_participant_width() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let mut state = tail_member_state(CapacityProfile::LegacyCountV1, 0, 0);
        let mut wide = cohort();
        wide.members = (7..)
            .filter(|shard| *shard != 9)
            .take(crate::transaction_decisions::MAX_PARTICIPANTS)
            .map(|shard| Member {
                shard,
                epoch: 1,
                mode: [shard as u8; 16],
            })
            .collect();
        let participants: Vec<_> = wide
            .members
            .iter()
            .map(|member| Participant {
                shard: member.shard,
                epoch: member.epoch,
                mode: member.mode,
                intent: [8; 32],
            })
            .collect();
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .replay_event(Command::BindCohort { cohort: wide })
            .unwrap();
        for n in 1..=34 {
            journal
                .replay_event(Command::Begin {
                    id: id(n),
                    participants: participants.clone(),
                })
                .unwrap();
            for participant in participants.iter().take((n as usize) % participants.len()) {
                journal
                    .replay_event(Command::Prepared {
                        id: id(n),
                        participant: participant.clone(),
                    })
                    .unwrap();
            }
            journal.replay_event(decide(n, Decision::Abort)).unwrap();
        }
        let expected: Vec<_> = (1..=31)
            .map(|n| journal.transaction_at(n).unwrap().clone())
            .collect();
        state.visibility.as_mut().unwrap().journal = journal.into();
        state.visibility.as_mut().unwrap().applied = 31;
        state.visibility.as_ref().unwrap().validate(&state).unwrap();
        let cut = persist_archive(&db, &state, marker(10));
        let (verified, tail) = verify_member_apply(&db, &cut).unwrap();
        assert_eq!(tail, expected);
        assert_eq!(tail, verified.transactions(1, 31, &[]).unwrap());
        assert_eq!(tail.len(), 31);
        assert!(tail
            .iter()
            .all(|tx| tx.participants.len() == crate::transaction_decisions::MAX_PARTICIPANTS));
    }

    #[test]
    fn member_tail_fusion_preserves_full_validation_and_error_precedence() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let mut state = tail_member_state(CapacityProfile::LegacyCountV1, 80, 33);
        // Resolve an early prefix before later raw rows, so simultaneous faults
        // below really exercise the decision-error side channel before decode.
        let mut journal = bound();
        for n in 1..=80 {
            journal.replay_event(begin(n)).unwrap();
            journal.replay_event(decide(n, Decision::Abort)).unwrap();
        }
        journal.replay_event(begin(81)).unwrap();
        journal.replay_event(begin(82)).unwrap();
        journal.replay_event(decide(82, Decision::Abort)).unwrap();
        state.visibility.as_mut().unwrap().journal = journal.into();
        let cut = persist_archive(&db, &state, marker(10));
        let raw = rows(&db);
        let derived: Vec<_> = db
            .iterator(IteratorMode::From(
                crate::decision_segments::PREFIX,
                Direction::Forward,
            ))
            .map(Result::unwrap)
            .take_while(|(key, _)| key.starts_with(crate::decision_segments::PREFIX))
            .map(|(k, v)| (k.to_vec(), v.to_vec()))
            .collect();
        assert!(raw.len() > 2 && derived.len() == 2);
        let assert_error = |expected: D1Error| {
            assert_eq!(verify_active(&db, &cut).err(), Some(expected.clone()));
            assert_eq!(verify_member_apply(&db, &cut).err(), Some(expected));
        };
        // Both early and late raw rows, both terminal segments, and a missing
        // row must fail even when the requested tail is only ordinal 33.
        for (key, value, expected) in raw
            .iter()
            .map(|(k, v)| (k, v, D1Error::EventLedgerHistory))
            .chain(
                derived
                    .iter()
                    .map(|(k, v)| (k, v, D1Error::DecisionIndexHistory)),
            )
        {
            db.put(key, b"{}").unwrap();
            assert_error(expected.clone());
            db.delete(key).unwrap();
            assert_error(expected);
            db.put(key, value).unwrap();
        }
        for (key, expected) in [
            (super::super::D1_KEY, D1Error::EventLedgerHistory),
            (super::super::APPLIED_KEY, D1Error::EventLedgerHistory),
            (
                crate::decision_segments::MANIFEST_KEY,
                D1Error::DecisionIndexHistory,
            ),
        ] {
            let original = db.get(key).unwrap().unwrap();
            db.put(key, b"{}").unwrap();
            assert_error(expected);
            db.put(key, original).unwrap();
        }
        for (prefix, expected) in [
            (PREFIX, D1Error::EventLedgerHistory),
            (
                crate::decision_segments::PREFIX,
                D1Error::DecisionIndexHistory,
            ),
            (crate::request_index::PREFIX, D1Error::RequestIndexHistory),
        ] {
            let key = [prefix, b"foreign"].concat();
            db.put(&key, b"{}").unwrap();
            assert_error(expected.clone());
            db.delete(key).unwrap();
        }
        // Early derived failure still wins over later raw failure.
        db.put(&derived[0].0, b"{}").unwrap();
        db.put(&raw.last().unwrap().0, b"{}").unwrap();
        assert_error(D1Error::DecisionIndexHistory);
        db.put(&derived[0].0, &derived[0].1).unwrap();
        // Extra derived row is checked at finish, after raw closure.
        let extra = [crate::decision_segments::PREFIX, b"foreign"].concat();
        db.put(&extra, b"{}").unwrap();
        assert_error(D1Error::EventLedgerHistory);
        db.put(&raw.last().unwrap().0, &raw.last().unwrap().1)
            .unwrap();
        assert_error(D1Error::DecisionIndexHistory);
        db.delete(extra).unwrap();
        assert_eq!(verify_member_apply(&db, &cut).unwrap().1.len(), 1);
    }

    #[test]
    fn target_projection_covers_direct_admitted_holes_and_complete_absence() {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let state = archive_state(profile, 3);
            let journal = state.coordinator.as_ref().unwrap().full().unwrap();
            let cut = persist_archive(&db, &state, marker(20));
            let expected: Vec<_> = [id(999), [1; 16], [2; 16], [3; 16], [200; 16]]
                .into_iter()
                .map(|id| TargetEvidence::from_journal(journal, id).unwrap())
                .collect();
            crate::transaction_decisions::without_full_journal(|| {
                for target in &expected {
                    assert_eq!(lookup_target(&db, &cut, target.id).unwrap(), *target);
                }
            });
            assert!(expected[0].record.as_ref().unwrap().request.is_none());
            assert!(expected[1].record.as_ref().unwrap().request.is_some());
            assert_eq!(expected[1].resolved_prefix, 0);
            // Corruption outside the selected target still invalidates positives
            // and absence; no cursor or cached result can replace full closure.
            db.put([PREFIX, b"foreign"].concat(), b"{}").unwrap();
            for target in &expected {
                assert!(lookup_target(&db, &cut, target.id).is_err());
            }
        }
    }

    #[test]
    fn target_old_cut_never_borrows_later_admission_or_decision() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let mut journal = bound();
        let before = journal.history_cursor().unwrap();
        journal.replay_event(begin(1)).unwrap();
        let pending = journal.history_cursor().unwrap();
        let cut = persist_archive(&db, &state(journal.clone()), marker(10));
        let archive = verify_active(&db, &cut).unwrap();
        journal.replay_event(decide(1, Decision::Abort)).unwrap();
        journal.replay_event(begin(2)).unwrap();
        let latest = journal.history_cursor().unwrap();
        let overlay = archive
            .member_overlay()
            .merge_compatibility_journal(&journal)
            .unwrap();
        crate::transaction_decisions::without_full_journal(|| {
            assert!(overlay.target_at_cut(&before, id(1)).unwrap().is_none());
            assert!(overlay.target_at_cut(&pending, id(2)).unwrap().is_none());
            let old = overlay.target_at_cut(&pending, id(1)).unwrap().unwrap();
            assert_eq!(old.transaction.ordinal, 1);
            assert_eq!(old.transaction.decision, None);
            let current = overlay.target_at_cut(&latest, id(1)).unwrap().unwrap();
            assert_eq!(current.transaction.decision, Some(Decision::Abort));
            let mut forged = pending.clone();
            forged.root[0] ^= 1;
            assert!(overlay.target_at_cut(&forged, id(1)).is_err());
            let future = latest.extend(&[begin(3)]).unwrap();
            assert!(overlay.target_at_cut(&future, id(3)).is_err());
        });
    }

    #[test]
    fn ordered_projection_checks_bytes_before_retention_and_releases_only_contiguous_front() {
        let mut projection = OrderedTerminals::new(CapacityProfile::LegacyCountV1);
        projection.active_bytes =
            crate::transaction_decisions::checkpoint::MAX_ACTIVE_CHECKPOINT_BYTES;
        assert!(projection.observe(&begin(1), &mut |_| Ok(())).is_err());
        assert!(projection.order.is_empty());
        assert!(projection.active.is_empty());
        assert_eq!(projection.admitted, 0);
        let mut projection = OrderedTerminals::new(CapacityProfile::LegacyCountV1);
        let mut emitted = Vec::new();
        for n in 1..=33 {
            projection
                .observe(&begin(n), &mut |tx| {
                    emitted.push(tx);
                    Ok(())
                })
                .unwrap();
        }
        for n in 2..=33 {
            projection
                .observe(&decide(n, Decision::Abort), &mut |tx| {
                    emitted.push(tx);
                    Ok(())
                })
                .unwrap();
        }
        assert!(emitted.is_empty());
        assert_eq!(projection.active.len(), 33);
        let expected: usize = projection
            .active
            .values()
            .map(|tx| serde_json::to_vec(tx).unwrap().len() + 1)
            .sum();
        assert_eq!(projection.active_bytes, expected);
        projection
            .observe(
                &Command::Prepared {
                    id: id(1),
                    participant: participant(),
                },
                &mut |_| Ok(()),
            )
            .unwrap();
        let expected: usize = projection
            .active
            .values()
            .map(|tx| serde_json::to_vec(tx).unwrap().len() + 1)
            .sum();
        assert_eq!(projection.active_bytes, expected);
        projection
            .observe(&decide(1, Decision::Abort), &mut |tx| {
                emitted.push(tx);
                Ok(())
            })
            .unwrap();
        assert_eq!(emitted.len(), 33);
        assert_eq!(projection.resolved, 33);
        assert_eq!(projection.active_bytes, 0);
        assert!(projection.active.is_empty());
        assert!(projection.order.is_empty());
    }

    #[test]
    fn frozen_resolution_does_not_borrow_later_hole_closure_or_overlay_decisions() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let mut j = bound();
        j.replay_event(begin(1)).unwrap();
        j.replay_event(begin(2)).unwrap();
        j.replay_event(decide(2, Decision::Abort)).unwrap();
        let before_hole = j.history_cursor().unwrap();
        let old = state(j.clone());
        let cut = persist_archive(&db, &old, marker(10));
        let archive = verify_active(&db, &cut).unwrap();
        j.replay_event(decide(1, Decision::Abort)).unwrap();
        let resolved = j.history_cursor().unwrap();
        let overlay = archive
            .member_overlay()
            .merge_compatibility_journal(&j)
            .unwrap();
        crate::transaction_decisions::without_full_journal(|| {
            assert!(!overlay.prove_resolved(&before_hole, 2).unwrap());
            assert!(!overlay.prove_resolved(&before_hole, 1).unwrap());
            assert!(overlay.prove_resolved(&resolved, 2).unwrap());
            assert!(!overlay.prove_resolved(&resolved, 3).unwrap());
            let mut forged = before_hole.clone();
            forged.root[0] ^= 1;
            assert!(overlay.prove_resolved(&forged, 2).is_err());
            let events = vec![decide(1, Decision::Abort)];
            let end = resolved.extend(&events).unwrap();
            let duplicate = HistoryPage {
                version: resolved.version,
                base: resolved.clone(),
                end: end.clone(),
                cut: end,
                events,
            };
            assert!(overlay.merge_history_page(&duplicate).is_err());
        });
    }

    #[test]
    fn active_page_merge_preserves_newer_observation_and_original_binding() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let old = archive_state(CapacityProfile::LegacyCountV1, 2);
        let journal = old.coordinator.as_ref().unwrap().full().unwrap();
        let base = HistoryCursor::empty(
            Authority {
                shard: 9,
                mode: [9; 16],
            },
            journal.cohort().unwrap().clone(),
        )
        .unwrap();
        let page = journal.history_page(&base, None).unwrap();
        let cut = persist_archive(&db, &old, marker(10));
        let archive = verify_active(&db, &cut).unwrap();
        let mut later = journal.clone();
        later.replay_event(decide(999, Decision::Abort)).unwrap();
        let overlay = archive
            .member_overlay()
            .merge_compatibility_journal(&later)
            .unwrap();
        crate::transaction_decisions::without_full_journal(|| {
            let merged = overlay.merge_history_page(&page).unwrap();
            assert!(Arc::ptr_eq(&merged.checkpoint, &overlay.checkpoint));
            assert!(Arc::ptr_eq(&merged.commands, &overlay.commands));
            let expected = &journal.requests()[0];
            assert_eq!(
                merged
                    .request_for_transaction(expected.transaction_id)
                    .unwrap()
                    .as_ref(),
                Some(expected)
            );
            assert_eq!(
                merged.transaction_at(1).unwrap().decision,
                Some(Decision::Abort)
            );
            let mut changed = page.clone();
            changed.cut.root[0] ^= 1;
            assert!(overlay.merge_history_page(&changed).is_err());
        });
    }

    #[test]
    fn member_overlay_rejects_equal_count_substitution_and_other_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let original = state(bound());
        let cut = persist_archive(&db, &original, marker(10));
        let archive = verify_active(&db, &cut).unwrap();
        let other_snapshot = verify_active(&db, &cut).unwrap();
        let empty = archive.member_overlay();
        empty
            .validate_candidate(&archive, empty.checkpoint_arc())
            .unwrap();
        assert!(empty
            .validate_candidate(&other_snapshot, empty.checkpoint_arc())
            .is_err());
        let other_cut = persist_archive(&db, &original, marker(11));
        let other_archive = verify_active(&db, &other_cut).unwrap();
        assert!(empty
            .validate_candidate(&other_archive, empty.checkpoint_arc())
            .is_err());
        let mut first = bound();
        first.replay_event(begin(1)).unwrap();
        first.replay_event(decide(1, Decision::Abort)).unwrap();
        let mut second = bound();
        second.replay_event(begin(2)).unwrap();
        second.replay_event(decide(2, Decision::Abort)).unwrap();
        let first_active = empty
            .merge_compatibility_journal(
                &first
                    .history_prefix(&first.history_cursor_at(2).unwrap())
                    .unwrap(),
            )
            .unwrap();
        let second_active = empty
            .merge_compatibility_journal(
                &second
                    .history_prefix(&second.history_cursor_at(2).unwrap())
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            first_active.commands().len(),
            second_active.commands().len()
        );
        assert_ne!(
            first_active.checkpoint_arc().as_ref(),
            second_active.checkpoint_arc().as_ref()
        );
        assert!(first_active
            .validate_candidate(&archive, second_active.checkpoint_arc())
            .is_err());
        let first = empty.merge_compatibility_journal(&first).unwrap();
        let second = empty.merge_compatibility_journal(&second).unwrap();
        assert_eq!(first.commands().len(), second.commands().len());
        assert_eq!(
            first.checkpoint_arc().as_ref(),
            second.checkpoint_arc().as_ref()
        );
        assert_ne!(first.commands(), second.commands());
        assert_ne!(first.head().unwrap(), second.head().unwrap());
        assert!(first
            .validate_candidate(&archive, second.checkpoint_arc())
            .is_err());
        let equal_value = Arc::new(first.checkpoint_arc().as_ref().clone());
        assert!(first.validate_candidate(&archive, &equal_value).is_err());
        assert!(empty.commands().is_empty());
        assert_eq!(empty.head().unwrap(), archive.head());
    }

    #[test]
    fn member_overlay_failed_extension_preserves_witness_and_no_tail_shares_arcs() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let original = state(bound());
        let cut = persist_archive(&db, &original, marker(10));
        let archive = verify_active(&db, &cut).unwrap();
        let empty = archive.member_overlay();
        let base = empty.head().unwrap();
        for events in [
            vec![begin(1), begin(1)],
            vec![
                Command::Prepared {
                    id: id(2),
                    participant: participant(),
                },
                begin(2),
            ],
        ] {
            let end = base.extend(&events).unwrap();
            let bad = HistoryPage {
                version: base.version,
                base: base.clone(),
                end: end.clone(),
                cut: end,
                events,
            };
            assert!(empty.merge_history_page(&bad).is_err());
            assert!(empty.commands().is_empty());
            assert_eq!(empty.checkpoint_arc().event_count(), base.event_count);
            assert_eq!(
                empty.lookup(LookupKey::Transaction(id(1))).unwrap(),
                LookupResult::Transaction(None)
            );
        }
        let mut accepted = bound();
        accepted.replay_event(begin(3)).unwrap();
        accepted.replay_event(decide(3, Decision::Abort)).unwrap();
        let successor = empty
            .merge_history_page(&accepted.history_page(&base, None).unwrap())
            .unwrap();
        assert_eq!(successor.transaction_at(1).unwrap().id, id(3));
        let reused = successor.merge_compatibility_journal(&accepted).unwrap();
        assert!(Arc::ptr_eq(&successor.checkpoint, &reused.checkpoint));
        assert!(Arc::ptr_eq(&successor.commands, &reused.commands));
        let older = reused.merge_compatibility_journal(&bound()).unwrap();
        assert!(Arc::ptr_eq(&reused.checkpoint, &older.checkpoint));
        assert!(Arc::ptr_eq(&reused.commands, &older.commands));
        let clone = older.clone();
        assert!(Arc::ptr_eq(&clone.commands, &older.commands));
        assert!(Arc::ptr_eq(&clone.checkpoint, &older.checkpoint));
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
            role: crate::decision_segments::Role::Coordinator,
            authority: descriptor.domain.authority.clone(),
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
                assert_eq!(verify_active(&db, &cut).err(), Some(expected.clone()));
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
            let targets: Vec<_> = [id(999), [1; 16], [2; 16], [3; 16], [4; 16], [200; 16]]
                .into_iter()
                .map(|id| TargetEvidence::from_journal(journal, id).unwrap())
                .collect();
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
                for target in targets {
                    assert_eq!(lookup_target(&db, &reopened, target.id).unwrap(), target);
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
                        lookup_target(&db, &cut, [1; 16]).is_err(),
                        "target fault {fault}"
                    );
                    assert!(
                        lookup_target(&db, &cut, [200; 16]).is_err(),
                        "absent target fault {fault}"
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
            assert!(lookup_target(&db, &cut, [1; 16]).is_err());
            assert!(lookup_target(&db, &next, [1; 16]).is_ok());
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
    #[test]
    fn row_witness_complete_coverage_matches_original_and_decodes_only_tail() {
        for count in [1, 31, 32, 33, 96] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let cut = persist_archive(&db, &history(count + 1), marker(1));
            let old = with_original_coordinator_verify(|| verify_active(&db, &cut)).unwrap();
            #[cfg(debug_assertions)]
            let start = ROW_DECODES.get();
            let new = verify_active(&db, &cut).unwrap();
            #[cfg(debug_assertions)]
            assert_eq!(ROW_DECODES.get() - start, 1);
            assert_eq!(new.tail.events, old.tail.events);
            assert_eq!(new.tail.before, old.tail.before);
            assert_eq!(new.tail.first, old.tail.first);
            assert_eq!(new.tail.count, old.tail.count);
            assert_eq!(new.tail.event_bytes, old.tail.event_bytes);
            assert_eq!(new.tail.encoded_bytes, old.tail.encoded_bytes);
        }
    }

    #[test]
    fn row_witness_rechecks_every_current_byte_and_full_marker() {
        for fault in 0..10 {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let cut = persist_archive(&db, &history(97), marker(1));
            verify_active(&db, &cut).unwrap();
            let pinned = db.snapshot();
            let raw = rows(&db);
            match fault {
                0..=2 => {
                    let i = if fault == 2 { raw.len() - 1 } else { fault };
                    let mut value = raw[i].1.clone();
                    let middle = value.len() / 2;
                    value[middle] ^= 1;
                    db.put(&raw[i].0, value).unwrap();
                }
                3 => db.delete(&raw[1].0).unwrap(),
                4 => db.put([PREFIX, b"extra"].concat(), b"extra").unwrap(),
                5 => {
                    db.delete(&raw[1].0).unwrap();
                    db.put([PREFIX, b"renamed"].concat(), &raw[1].1).unwrap();
                }
                6 => {
                    let mut value = raw[0].1.clone();
                    value.push(b' ');
                    db.put(&raw[0].0, value).unwrap();
                }
                8 => {
                    let mut row = Row::decode(&raw[1].1).unwrap();
                    let Command::Begin { participants, .. } = &mut row.events[0] else {
                        panic!("expected Begin");
                    };
                    participants[0].intent[0] = 9;
                    let value = serde_json::to_vec(&row).unwrap();
                    assert_eq!(value.len(), raw[1].1.len());
                    db.put(&raw[1].0, value).unwrap();
                }
                9 => {
                    db.put(&raw[0].0, &raw[1].1).unwrap();
                    db.put(&raw[1].0, &raw[0].1).unwrap();
                }
                _ => db
                    .put(
                        super::super::APPLIED_KEY,
                        serde_json::to_vec(&Some(LogId::new(
                            openraft::CommittedLeaderId::new(99, 88),
                            1,
                        )))
                        .unwrap(),
                    )
                    .unwrap(),
            }
            let old = with_original_coordinator_verify(|| verify_active(&db, &cut))
                .err()
                .unwrap();
            let new = verify_active(&db, &cut).err().unwrap();
            assert_eq!(old, new, "fault {fault}");
            verify_coordinator_rows(&pinned, &cut).unwrap();
        }
    }

    #[test]
    fn row_witness_bounds_are_constant_per_segment_and_member_has_none() {
        assert_eq!(std::mem::size_of::<RowWitness>(), 48);
        assert_eq!(max_segments(CapacityProfile::LegacyCountV1), 2122);
        assert_eq!(max_segments(CapacityProfile::ByteBound16MiBV1), 7499);
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let state = tail_member_state(CapacityProfile::LegacyCountV1, 4, 2);
        let cut = persist_archive(&db, &state, marker(1));
        assert!(cut.row_witnesses.is_none());
        verify_active(&db, &cut).unwrap();
    }

    #[test]
    #[ignore = "paired real verifier cost, no whole-request speedup claim"]
    fn row_witness_verification_cost() {
        let build = if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        };
        for count in [1, 32, 128, 512] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let state = history(count + 1);
            let cut = persist_archive(&db, &state, marker(1));
            let mut times = [Vec::new(), Vec::new()];
            for round in 0..24 {
                for slot in if round % 2 == 0 { [0, 1] } else { [1, 0] } {
                    let now = std::time::Instant::now();
                    if slot == 0 {
                        with_original_coordinator_verify(|| verify_active(&db, &cut)).unwrap();
                    } else {
                        verify_active(&db, &cut).unwrap();
                    }
                    if round >= 4 {
                        times[slot].push(now.elapsed().as_micros());
                    }
                }
            }
            for t in &mut times {
                t.sort_unstable();
            }
            eprintln!("ROW_WITNESS_VERIFY tx={count} original_median_us={} witness_median_us={} rows={} resident_witness_bytes={} samples=20 build={build}", times[0][10], times[1][10], cut.descriptor.segment_count, cut.row_witnesses.as_ref().unwrap().len()*std::mem::size_of::<RowWitness>());
            eprintln!(
                "ROW_WITNESS_VERIFY_SAMPLES original_us={:?} witness_us={:?}",
                times[0], times[1]
            );
        }
    }
    #[test]
    fn row_witness_cold_staged_and_blank_views_have_exact_provenance() {
        for count in [1, 30, 31, 32, 33, 96] {
            let dir = tempfile::tempdir().unwrap();
            let db = DB::open_default(dir.path()).unwrap();
            let original = history(count + 1);
            let cut = persist_archive(&db, &original, marker(1));
            let loaded = load(&db, &cut.descriptor).unwrap();
            assert_eq!(loaded.row_witnesses, cut.row_witnesses);
            let verified = verify_active(&db, &cut).unwrap();
            let checkpoint = cut.checkpoint.as_ref().clone();
            let mut batch = WriteBatch::default();
            let blank = stage_active(&mut batch, &verified, &checkpoint, &[], marker(2)).unwrap();
            assert!(Arc::ptr_eq(
                blank.row_witnesses.as_ref().unwrap(),
                cut.row_witnesses.as_ref().unwrap()
            ));
            let old_witnesses = cut.row_witnesses.clone();
            let event = begin(count + 1);
            let mut next = checkpoint.clone();
            next.apply(event.clone(), &mut |key| {
                verified
                    .lookup(key, &[])
                    .map_err(|_| crate::transaction_decisions::Error::InvalidJournal)
            })
            .unwrap();
            let changed = stage_active(
                &mut batch,
                &verified,
                &next,
                std::slice::from_ref(&event),
                marker(2),
            )
            .unwrap();
            assert!(!Arc::ptr_eq(
                changed.row_witnesses.as_ref().unwrap(),
                cut.row_witnesses.as_ref().unwrap()
            ));
            assert_eq!(old_witnesses, cut.row_witnesses);
            let mut journal = original
                .coordinator
                .as_ref()
                .unwrap()
                .full()
                .unwrap()
                .clone();
            journal.replay_event(event).unwrap();
            let next_state = state(journal);
            let mut full_batch = WriteBatch::default();
            let full = stage(&mut full_batch, None, Some(&next_state), marker(2), true)
                .unwrap()
                .unwrap();
            assert_eq!(full.row_witnesses, changed.row_witnesses);
        }
    }
}
