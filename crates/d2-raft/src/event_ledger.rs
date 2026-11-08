//! Sole persistent live Journal authority for fixed-cohort disk formats.
//! Raw commands are replayed with the original admission and reservation rules.
//! The full namespace is closed: no derived index or archived snapshot is a fallback.
use crate::d1::D1State;
use crate::participant_writes::Authority;
use crate::transaction_decisions::{
    Cohort, Command, HistoryCursor, Journal, MAX_HISTORY_EVENTS, MAX_HISTORY_PAGE_BYTES,
    MAX_HISTORY_PAGE_EVENTS, MAX_JOURNAL_BYTES, MAX_PARTICIPANTS, MAX_REQUEST_EVENT_BYTES,
};
use crate::NodeId;
use openraft::LogId;
use rocksdb::{Direction, IteratorMode, WriteBatch, DB};
use serde::{de, Deserialize, Deserializer, Serialize};
use std::fmt;

pub(super) const PREFIX: &[u8] = b"sm/ledger/";
const PREFIX_END: &[u8] = b"sm/ledger0";
const VERSION: u32 = 1;
// Every byte-full nonfinal row contains > (4 MiB - 2 MiB) event bytes;
// every other nonfinal row contains 32 events. Both charge existing Journal
// limits, not a new admission quota. The final +1 covers the unsealed tail.
const MAX_SEGMENTS: u64 = MAX_HISTORY_EVENTS.div_ceil(MAX_HISTORY_PAGE_EVENTS as u64)
    + (MAX_JOURNAL_BYTES / (MAX_HISTORY_PAGE_BYTES - MAX_REQUEST_EVENT_BYTES)) as u64
    + 1;
// Worst-width JSON: a Member is <= 160 bytes; 64 members plus commas
// consume <= 10,304. The remaining domain fields are < 512, both root arrays
// <= 258, row fields/32 event separators < 256, and a binary key < 128.
// Their sum < 11,458, so 16 KiB covers key + row overhead for every legal row.
// Tests pin the actual maximum-width representation and all Command variants.
const MAX_ROW_OVERHEAD: usize = 16 * 1024;
const MAX_ROW_BYTES: usize = MAX_HISTORY_PAGE_BYTES + MAX_ROW_OVERHEAD;
const MAX_ENCODED_BYTES: u64 = MAX_JOURNAL_BYTES as u64 + MAX_SEGMENTS * MAX_ROW_OVERHEAD as u64;

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
    #[serde(deserialize_with = "deserialize_cohort")]
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
        key.extend_from_slice(&VERSION.to_be_bytes());
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
        let mut skeleton = journal(self.authority.mode)?;
        replay(
            &mut skeleton,
            Command::BindCohort {
                cohort: self.cohort.clone(),
            },
        )?;
        skeleton.history_cursor_at(0).map_err(history_error)
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
            self.version == VERSION
                && (1..=MAX_HISTORY_EVENTS).contains(&self.event_count)
                && (1..=MAX_SEGMENTS).contains(&self.segment_count)
                && self.segment_count <= self.event_count
                && self.event_count <= self.segment_count * MAX_HISTORY_PAGE_EVENTS as u64
                && self.encoded_bytes > 0
                && self.encoded_bytes <= MAX_ENCODED_BYTES,
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
    pub(super) fn validate_state(&self, state: &D1State) -> anyhow::Result<()> {
        self.validate()?;
        let view = View::of(state).ok_or_else(|| anyhow::anyhow!("ledger state is not bound"))?;
        let cursor = view.journal.history_cursor().map_err(history_error)?;
        anyhow::ensure!(
            self.domain == view.domain
                && self.event_count == cursor.event_count
                && self.head_root == cursor.root,
            "ledger descriptor disagrees with hydrated state"
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
            Some(journal) => (Role::Coordinator, journal),
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
            row.version == VERSION && !row.events.is_empty() && serde_json::to_vec(&row)? == bytes,
            "noncanonical ledger row"
        );
        Ok(row)
    }
}
// Decode bounds are enforced before growing collection allocation, including
// rejecting an extra element without ever materializing its Command/Member.
fn bounded_vec<'de, D, T>(deserializer: D, maximum: usize) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Bounded<T>(usize, std::marker::PhantomData<T>);
    impl<'de, T: Deserialize<'de>> de::Visitor<'de> for Bounded<T> {
        type Value = Vec<T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a bounded ledger sequence")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<T>, A::Error> {
            if seq.size_hint().is_some_and(|size| size > self.0) {
                return Err(de::Error::custom("ledger sequence exceeds bound"));
            }
            let mut items = Vec::new();
            while items.len() < self.0 {
                let Some(item) = seq.next_element()? else {
                    return Ok(items);
                };
                items.push(item);
            }
            struct Reject;
            impl<'de> de::DeserializeSeed<'de> for Reject {
                type Value = ();
                fn deserialize<D: Deserializer<'de>>(self, _: D) -> Result<(), D::Error> {
                    Err(de::Error::custom("ledger sequence exceeds bound"))
                }
            }
            seq.next_element_seed(Reject)?;
            Ok(items)
        }
    }
    deserializer.deserialize_seq(Bounded::<T>(maximum, std::marker::PhantomData))
}
fn deserialize_events<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Command>, D::Error> {
    bounded_vec(d, MAX_HISTORY_PAGE_EVENTS)
}
fn deserialize_cohort<'de, D: Deserializer<'de>>(d: D) -> Result<Cohort, D::Error> {
    use crate::transaction_decisions::Member;
    fn members<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Member>, D::Error> {
        bounded_vec(d, MAX_PARTICIPANTS)
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct BoundCohort {
        coordinator_shard: u64,
        coordinator_epoch: u64,
        #[serde(deserialize_with = "members")]
        members: Vec<Member>,
    }
    let c = BoundCohort::deserialize(d)?;
    Ok(Cohort {
        coordinator_shard: c.coordinator_shard,
        coordinator_epoch: c.coordinator_epoch,
        members: c.members,
    })
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
    first: u64,
    before: HistoryCursor,
    count: usize,
    event_bytes: usize,
    encoded_bytes: u64,
}
pub(super) struct Loaded {
    pub(super) journal: Journal,
    pub(super) descriptor: Descriptor,
    tail: Tail,
}

/// One RocksDB snapshot covers the marker and complete raw namespace scan.
/// Every accepted row is immediately replayed; no full serialized Journal blob
/// is built. Iterator work stops at the first violation of the derived bounds.
pub(super) fn load(db: &DB, descriptor: &Descriptor) -> anyhow::Result<Loaded> {
    descriptor.validate()?;
    let snapshot = db.snapshot();
    let applied: Option<LogId<NodeId>> = match snapshot.get(super::APPLIED_KEY)? {
        Some(bytes) => serde_json::from_slice(&bytes)?,
        None => None,
    };
    anyhow::ensure!(
        applied == descriptor.applied,
        "ledger applied marker mismatch"
    );
    let mut journal = journal(descriptor.domain.authority.mode)?;
    let mut cursor = descriptor.domain.zero_cursor()?;
    let mut count = 0u64;
    let mut encoded_bytes = 0u64;
    let mut tail: Option<Tail> = None;
    for item in snapshot.iterator(IteratorMode::From(PREFIX, Direction::Forward)) {
        let (key, value) = item?;
        if !key.starts_with(PREFIX) {
            break;
        }
        anyhow::ensure!(count < descriptor.segment_count, "extra ledger row");
        anyhow::ensure!(
            key.as_ref() == descriptor.domain.key(cursor.event_count + 1),
            "ledger key gap or foreign domain"
        );
        encoded_bytes = encoded_bytes
            .checked_add((key.len() + value.len()) as u64)
            .ok_or_else(|| anyhow::anyhow!("ledger byte total overflow"))?;
        anyhow::ensure!(
            encoded_bytes <= descriptor.encoded_bytes,
            "extra ledger bytes"
        );
        let row = Row::decode(&value)?;
        anyhow::ensure!(
            row.domain == descriptor.domain
                && row.first == cursor.event_count + 1
                && row.before_root == cursor.root
                && row.events.len() as u64 <= descriptor.event_count - cursor.event_count,
            "ledger row header mismatch"
        );
        let mut bytes = 0;
        for event in &row.events {
            bytes += event_size(event)?;
            anyhow::ensure!(
                bytes <= MAX_HISTORY_PAGE_BYTES,
                "ledger event bytes exceed row bound"
            );
        }
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
        let end = cursor.extend(&row.events).map_err(history_error)?;
        anyhow::ensure!(end.root == row.after_root, "ledger row root mismatch");
        let row_count = row.events.len();
        for event in row.events {
            replay(&mut journal, event)?;
        }
        tail = Some(Tail {
            first: row.first,
            before: cursor,
            count: row_count,
            event_bytes: bytes,
            encoded_bytes: (key.len() + value.len()) as u64,
        });
        cursor = end;
        count += 1;
    }
    anyhow::ensure!(
        count == descriptor.segment_count
            && encoded_bytes == descriptor.encoded_bytes
            && cursor.event_count == descriptor.event_count
            && cursor.root == descriptor.head_root
            && journal.cohort() == Some(&descriptor.domain.cohort),
        "incomplete or substituted ledger history"
    );
    Ok(Loaded {
        journal,
        descriptor: descriptor.clone(),
        tail: tail.ok_or_else(|| anyhow::anyhow!("missing cohort binding event"))?,
    })
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
pub(super) fn stage(
    batch: &mut WriteBatch,
    before: Option<&Loaded>,
    next: Option<&D1State>,
    applied: Option<LogId<NodeId>>,
    replace: bool,
) -> anyhow::Result<Option<Descriptor>> {
    let view = next.and_then(View::of);
    let Some(view) = view else {
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
            return Ok(Some(Descriptor {
                applied,
                ..before.descriptor.clone()
            }));
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
            version: VERSION,
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
        version: VERSION,
        domain: view.domain,
        event_count: cursor.event_count,
        segment_count,
        encoded_bytes,
        head_root: cursor.root,
        applied,
    };
    descriptor.validate()?;
    Ok(Some(descriptor))
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
        Decision, Member, Participant, MAX_REQUEST_BYTES, MAX_REQUEST_PAYLOAD_BYTES,
        MAX_TRANSACTIONS,
    };
    use rocksdb::WriteBatchIterator;

    fn cohort() -> Cohort {
        Cohort {
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
        state.coordinator = Some(journal);
        state
    }
    fn history(count: u64) -> D1State {
        let mut j = bound();
        for n in 1..count {
            j.replay_event(begin(n)).unwrap();
        }
        state(j)
    }
    fn marker(index: u64) -> Option<LogId<NodeId>> {
        Some(LogId::new(openraft::CommittedLeaderId::new(3, 4), index))
    }
    fn persist(db: &DB, state: &D1State, applied: Option<LogId<NodeId>>) -> Descriptor {
        let mut batch = WriteBatch::default();
        let descriptor = stage(&mut batch, None, Some(state), applied, true)
            .unwrap()
            .unwrap();
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
                version: VERSION,
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
            version: VERSION,
            domain,
            event_count: events.len() as u64,
            segment_count: ends.len() as u64,
            encoded_bytes: total,
            head_root: cursor.root,
            applied: None,
        }
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
        assert_eq!(loaded.journal, *bound_state.coordinator.as_ref().unwrap());
        descriptor.validate_state(&bound_state).unwrap();
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
    fn namespace_closure_and_descriptor_source_headers_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let db = DB::open_default(dir.path()).unwrap();
        let state = history(65);
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
        let events = state.coordinator.as_ref().unwrap().events();
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
        for (old, next, first) in [
            (31, 32, Some(1)),
            (32, 33, Some(33)),
            (33, 34, Some(33)),
            (65, 65, None),
        ] {
            let old_state = history(old);
            let descriptor = persist(&db, &old_state, marker(1));
            let loaded = load(&db, &descriptor).unwrap();
            let mut batch = WriteBatch::default();
            let next_state = history(next);
            let next_descriptor = stage(
                &mut batch,
                Some(&loaded),
                Some(&next_state),
                marker(2),
                false,
            )
            .unwrap()
            .unwrap();
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
                load(&db, &next_descriptor).unwrap().journal,
                *next_state.coordinator.as_ref().unwrap()
            );
            loaded
                .journal
                .validate_successor(next_state.coordinator.as_ref().unwrap())
                .unwrap();
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
        let events = vec![
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
        for event in &events {
            // This width upper bound deliberately ignores Plan validity: no
            // valid event can require a wider integer, array, or payload.
            assert!(event_size(event).unwrap() <= MAX_REQUEST_EVENT_BYTES);
        }
        let row = Row {
            version: VERSION,
            domain: domain.clone(),
            first: u64::MAX,
            before_root: [255; 32],
            after_root: [255; 32],
            events: vec![events[3].clone(); MAX_HISTORY_PAGE_EVENTS],
        };
        let command_bytes: usize = row.events.iter().map(|e| event_size(e).unwrap()).sum();
        assert!(
            domain.key(u64::MAX).len() + serde_json::to_vec(&row).unwrap().len() - command_bytes
                < MAX_ROW_OVERHEAD
        );
        assert_eq!(MAX_SEGMENTS, 2122);
        assert_eq!(
            MAX_ENCODED_BYTES,
            MAX_JOURNAL_BYTES as u64 + 2122 * MAX_ROW_OVERHEAD as u64
        );
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
            .replay_event(decide(1, Decision::Abort))
            .unwrap();
        let mut batch = WriteBatch::default();
        let descriptor = stage(&mut batch, Some(&loaded), Some(&next), None, false)
            .unwrap()
            .unwrap();
        db.write(batch).unwrap();
        assert_eq!(
            load(&db, &descriptor).unwrap().journal,
            *next.coordinator.as_ref().unwrap()
        );
        let mut invalid = next.coordinator.as_ref().unwrap().events().to_vec();
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
            .replay_event(next_event)
            .unwrap();
        let mut batch = WriteBatch::default();
        let descriptor = stage(&mut batch, Some(&before), Some(&next), None, false)
            .unwrap()
            .unwrap();
        let mut operations = Operations::default();
        batch.iterate(&mut operations);
        assert_eq!(
            operations.puts,
            vec![descriptor.domain.key(before.descriptor.event_count + 1)]
        );
        db.write(batch).unwrap();
        assert_eq!(rows(&db)[0], old_row);
        let loaded = load(&db, &descriptor).unwrap();
        assert_eq!(loaded.journal, *next.coordinator.as_ref().unwrap());
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
            .replay_event(decide(rejected - 1, Decision::Abort))
            .unwrap();
        let mut batch = WriteBatch::default();
        let descriptor = stage(&mut batch, Some(&loaded), Some(&next), None, false)
            .unwrap()
            .unwrap();
        db.write(batch).unwrap();
        assert_eq!(
            load(&db, &descriptor).unwrap().journal,
            *next.coordinator.as_ref().unwrap()
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
