//! Bounded transfers of exact retained journal events. Cursors are integrity
//! commitments, not authentication. The full replayed Journal remains authority.
use super::{Cohort, Command, Error, Journal, MAX_PARTICIPANTS, MAX_TRANSACTIONS};
use crate::participant_writes::Authority;
use serde::{de, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use std::fmt;
use std::io::{self, Write};

/// Maximum number of original commands in one history page.
pub const MAX_HISTORY_PAGE_EVENTS: usize = 32;
/// Maximum sum of canonical command JSON byte lengths in one history page.
/// The complete transport frame has its own, separate bound.
pub const MAX_HISTORY_PAGE_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_HISTORY_EVENTS: u64 = (MAX_TRANSACTIONS * (MAX_PARTICIPANTS + 2) + 1) as u64;
const VERSION: u32 = 1;
const IDENTITY_DOMAIN: &[u8] = b"d2-raft/journal-history/v1\0";
const EVENT_DOMAIN: &[u8] = b"d2-raft/journal-history/event/v1\0";

/// A checked commitment to an absolute prefix of one full cohort's history.
///
/// Version 1 hashes the canonical compact JSON of `{authority,cohort}` after
/// `IDENTITY_DOMAIN` and its u64 big-endian byte length. Each subsequent root
/// hashes `EVENT_DOMAIN`, the previous root, the u64 big-endian 1-based event
/// index, its u64 big-endian canonical byte length, and the original Command's
/// compact JSON bytes. Journal storage encoding versions are not hash inputs.
/// Public fields permit construction, but every consuming operation validates
/// them. Deserialization also validates identity, version and count bounds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "CursorEncoding")]
pub struct HistoryCursor {
    pub version: u32,
    pub authority: Authority,
    pub cohort: Cohort,
    pub event_count: u64,
    pub root: [u8; 32],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorEncoding {
    version: u32,
    authority: Authority,
    cohort: Cohort,
    event_count: u64,
    root: [u8; 32],
}
impl TryFrom<CursorEncoding> for HistoryCursor {
    type Error = String;
    fn try_from(encoded: CursorEncoding) -> Result<Self, Self::Error> {
        let cursor = Self {
            version: encoded.version,
            authority: encoded.authority,
            cohort: encoded.cohort,
            event_count: encoded.event_count,
            root: encoded.root,
        };
        cursor
            .validate()
            .map_err(|e| format!("invalid history cursor: {e:?}"))?;
        Ok(cursor)
    }
}
impl HistoryCursor {
    /// Validate the format and full identity, not the unavailable history bytes.
    /// The identity-bound empty root can be verified without any history.
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != VERSION || self.event_count > MAX_HISTORY_EVENTS {
            return Err(Error::InvalidJournal);
        }
        if self.authority.mode == [0; 16] || self.authority.shard != self.cohort.coordinator_shard {
            return Err(Error::InvalidIdentity);
        }
        self.cohort.validate()?;
        if self.event_count == 0 && self.root != identity_root(&self.authority, &self.cohort)? {
            return Err(Error::IncompatibleHistory);
        }
        Ok(())
    }

    /// Extend the existing v1 chain once per original event. Semantic replay is
    /// the caller's responsibility; no independently invented ledger hash exists.
    pub(crate) fn extend(&self, events: &[Command]) -> Result<Self, Error> {
        self.validate()?;
        let event_count = self
            .event_count
            .checked_add(events.len() as u64)
            .filter(|count| *count <= MAX_HISTORY_EVENTS)
            .ok_or(Error::InvalidJournal)?;
        let mut root = self.root;
        for (offset, event) in events.iter().enumerate() {
            root = event_root(root, self.event_count + offset as u64 + 1, event)?;
        }
        Ok(Self {
            event_count,
            root,
            ..self.clone()
        })
    }

    /// Check the complete authority and ordered cohort against a trusted target.
    pub fn validate_identity(&self, authority: &Authority, cohort: &Cohort) -> Result<(), Error> {
        self.validate()?;
        if &self.authority != authority || &self.cohort != cohort {
            return Err(Error::IncompatibleHistory);
        }
        Ok(())
    }
}

/// Original commands in `[base.event_count, end.event_count)`, pinned to `cut`.
/// An unseen future cut is a transfer target, never proof of retained history.
/// Empty pages are legal only when base, end and cut are the same cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "PageEncoding")]
pub struct HistoryPage {
    pub version: u32,
    pub base: HistoryCursor,
    pub end: HistoryCursor,
    pub cut: HistoryCursor,
    pub events: Vec<Command>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PageEncoding {
    version: u32,
    base: HistoryCursor,
    end: HistoryCursor,
    cut: HistoryCursor,
    #[serde(deserialize_with = "deserialize_events")]
    events: Vec<Command>,
}
impl TryFrom<PageEncoding> for HistoryPage {
    type Error = String;
    fn try_from(encoded: PageEncoding) -> Result<Self, Self::Error> {
        let page = Self {
            version: encoded.version,
            base: encoded.base,
            end: encoded.end,
            cut: encoded.cut,
            events: encoded.events,
        };
        page.validate()
            .map_err(|e| format!("invalid history page: {e:?}"))?;
        Ok(page)
    }
}
impl HistoryPage {
    /// Validate all headers, bounds, progress and the supplied event hash chain.
    /// This does not authenticate the base, unseen cut or semantic transitions;
    /// `Journal::merge_history_page` checks those against local durable history.
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != VERSION || self.events.len() > MAX_HISTORY_PAGE_EVENTS {
            return Err(Error::InvalidJournal);
        }
        self.base.validate()?;
        self.end
            .validate_identity(&self.base.authority, &self.base.cohort)?;
        self.cut
            .validate_identity(&self.base.authority, &self.base.cohort)?;
        if self.base.event_count > self.end.event_count
            || self.end.event_count > self.cut.event_count
            || self.end.event_count - self.base.event_count != self.events.len() as u64
            || (self.events.is_empty() && self.base != self.cut)
        {
            return Err(Error::InvalidJournal);
        }
        // Count before hashing, without allocating canonical byte buffers.
        let mut bytes = 0;
        for event in &self.events {
            bytes += event_size(event, MAX_HISTORY_PAGE_BYTES - bytes)?;
        }
        let end = self.base.extend(&self.events)?;
        if end != self.end || (self.end.event_count == self.cut.event_count && self.end != self.cut)
        {
            return Err(Error::IncompatibleHistory);
        }
        Ok(())
    }
}

// A rejected 33rd event is never deserialized into a Command. The frame bound
// separately caps allocation while decoding any one Command; each accepted
// command's canonical bytes are counted before it enters the bounded vector.
fn deserialize_events<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<Command>, D::Error> {
    struct Events;
    impl<'de> de::Visitor<'de> for Events {
        type Value = Vec<Command>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("a bounded sequence of original journal commands")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            if seq
                .size_hint()
                .is_some_and(|size| size > MAX_HISTORY_PAGE_EVENTS)
            {
                return Err(de::Error::custom("too many history events"));
            }
            let mut events = Vec::new();
            let mut bytes = 0;
            while events.len() < MAX_HISTORY_PAGE_EVENTS {
                let Some(event) = seq.next_element::<Command>()? else {
                    return Ok(events);
                };
                bytes += event_size(&event, MAX_HISTORY_PAGE_BYTES - bytes)
                    .map_err(|e| de::Error::custom(format!("invalid history event size: {e:?}")))?;
                events.push(event);
            }
            struct RejectExtra;
            impl<'de> de::DeserializeSeed<'de> for RejectExtra {
                type Value = ();
                fn deserialize<D: Deserializer<'de>>(
                    self,
                    _deserializer: D,
                ) -> Result<(), D::Error> {
                    Err(de::Error::custom("too many history events"))
                }
            }
            seq.next_element_seed(RejectExtra)?;
            Ok(events)
        }
    }
    deserializer.deserialize_seq(Events)
}

struct ByteCounter {
    bytes: usize,
    limit: usize,
}
impl Write for ByteCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes {
            return Err(io::Error::other(
                "canonical history event exceeds byte limit",
            ));
        }
        self.bytes += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn event_size(event: &Command, limit: usize) -> Result<usize, Error> {
    let mut counter = ByteCounter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, event).map_err(|_| Error::Capacity)?;
    Ok(counter.bytes)
}
struct HashWriter(Sha256);
impl Write for HashWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
fn identity_root(authority: &Authority, cohort: &Cohort) -> Result<[u8; 32], Error> {
    #[derive(Serialize)]
    struct Identity<'a> {
        authority: &'a Authority,
        cohort: &'a Cohort,
    }
    // Cohort has already been validated, so these bytes are bounded by 64 members.
    let bytes =
        serde_json::to_vec(&Identity { authority, cohort }).map_err(|_| Error::InvalidJournal)?;
    let mut hash = Sha256::new();
    hash.update(IDENTITY_DOMAIN);
    hash.update((bytes.len() as u64).to_be_bytes());
    hash.update(bytes);
    Ok(hash.finalize().into())
}
fn event_root(previous: [u8; 32], index: u64, event: &Command) -> Result<[u8; 32], Error> {
    let bytes = event_size(event, MAX_HISTORY_PAGE_BYTES)?;
    let mut hash = HashWriter(Sha256::new());
    hash.0.update(EVENT_DOMAIN);
    hash.0.update(previous);
    hash.0.update(index.to_be_bytes());
    hash.0.update((bytes as u64).to_be_bytes());
    serde_json::to_writer(&mut hash, event).map_err(|_| Error::InvalidJournal)?;
    Ok(hash.0.finalize().into())
}

impl Journal {
    /// Derive the current position from the full retained, replay-validated events.
    /// This API requires a bound fixed cohort; no separately persisted root exists.
    pub fn history_cursor(&self) -> Result<HistoryCursor, Error> {
        self.history_cursor_at(self.events.len() as u64)
    }

    /// Derive any exact retained prefix, including the identity-bound zero root.
    /// The hash format remains v1 when journal storage changes from v2 to v3.
    pub fn history_cursor_at(&self, count: u64) -> Result<HistoryCursor, Error> {
        if count > MAX_HISTORY_EVENTS || count > self.events.len() as u64 {
            return Err(Error::IncompatibleHistory);
        }
        let cohort = self.cohort.as_ref().ok_or(Error::InvalidIdentity)?;
        let authority = Authority {
            shard: cohort.coordinator_shard,
            mode: self.authority,
        };
        cohort.validate()?;
        let mut root = identity_root(&authority, cohort)?;
        for (index, event) in self.events.iter().take(count as usize).enumerate() {
            root = event_root(root, index as u64 + 1, event)?;
        }
        Ok(HistoryCursor {
            version: VERSION,
            authority,
            cohort: cohort.clone(),
            event_count: count,
            root,
        })
    }

    fn validate_history_cursor(&self, cursor: &HistoryCursor) -> Result<(), Error> {
        let cohort = self.cohort.as_ref().ok_or(Error::InvalidIdentity)?;
        cursor.validate_identity(
            &Authority {
                shard: cohort.coordinator_shard,
                mode: self.authority,
            },
            cohort,
        )?;
        if self.history_cursor_at(cursor.event_count)? != *cursor {
            return Err(Error::IncompatibleHistory);
        }
        Ok(())
    }

    /// Produce a bounded suffix page of original commands. The first page pins
    /// the current cut; every continuation must reuse that exact cut. Appends
    /// after the cut do not change it or make an incomplete page into completion.
    pub fn history_page(
        &self,
        base: &HistoryCursor,
        cut: Option<&HistoryCursor>,
    ) -> Result<HistoryPage, Error> {
        self.validate_history_cursor(base)?;
        let cut = match cut {
            Some(cut) => {
                self.validate_history_cursor(cut)?;
                cut.clone()
            }
            None => self.history_cursor()?,
        };
        if base.event_count > cut.event_count {
            return Err(Error::IncompatibleHistory);
        }
        let mut events = Vec::new();
        let mut bytes = 0;
        let mut root = base.root;
        let start = base.event_count as usize;
        for event in self.events[start..cut.event_count as usize]
            .iter()
            .take(MAX_HISTORY_PAGE_EVENTS)
        {
            let size = event_size(event, MAX_HISTORY_PAGE_BYTES)?;
            if size > MAX_HISTORY_PAGE_BYTES - bytes {
                break;
            }
            bytes += size;
            root = event_root(root, base.event_count + events.len() as u64 + 1, event)?;
            events.push(event.clone());
        }
        let mut end = base.clone();
        end.event_count += events.len() as u64;
        end.root = root;
        let page = HistoryPage {
            version: VERSION,
            base: base.clone(),
            end,
            cut,
            events,
        };
        page.validate()?;
        Ok(page)
    }

    /// Merge on a private candidate, checking the local base and all supplied
    /// overlap exactly. Unknown commands must strictly append through the full
    /// original replay and capacity/reservation rules, never no-op as retries.
    /// The returned journal keeps any local events later than the frozen cut.
    pub fn merge_history_page(&self, page: &HistoryPage) -> Result<Self, Error> {
        page.validate()?;
        self.validate_history_cursor(&page.base)?;
        let mut candidate = self.clone();
        for (offset, event) in page.events.iter().enumerate() {
            let index = page.base.event_count as usize + offset;
            if let Some(existing) = candidate.events.get(index) {
                if existing != event {
                    return Err(Error::IncompatibleHistory);
                }
            } else {
                candidate.replay_event(event.clone())?;
            }
        }
        candidate.validate_history_cursor(&page.end)?;
        if page.cut.event_count <= candidate.events.len() as u64 {
            candidate.validate_history_cursor(&page.cut)?;
        }
        Ok(candidate)
    }

    /// Verify and replay the exact cut's prefix using the original reservation
    /// and semantic rules. This does not discard later history in `self`.
    /// A zero cut returns an empty unbound journal, with no resolved ordinal.
    pub fn history_prefix(&self, cut: &HistoryCursor) -> Result<Self, Error> {
        self.validate_history_cursor(cut)?;
        let mut prefix = Self::new(self.authority)?;
        for event in self.events.iter().take(cut.event_count as usize) {
            prefix.replay_event(event.clone())?;
        }
        Ok(prefix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::{Decision, Member, Participant, MAX_REQUEST_BYTES};

    fn cohort() -> Cohort {
        Cohort {
            coordinator_shard: 9,
            coordinator_epoch: 7,
            members: vec![
                Member {
                    shard: 1,
                    epoch: 2,
                    mode: [3; 16],
                },
                Member {
                    shard: 4,
                    epoch: 5,
                    mode: [6; 16],
                },
            ],
        }
    }
    fn participant() -> Participant {
        Participant {
            shard: 1,
            epoch: 2,
            mode: [3; 16],
            intent: [4; 32],
        }
    }
    fn begin(id: u8) -> Command {
        Command::Begin {
            id: [id; 16],
            participants: vec![participant()],
        }
    }
    fn prepared(id: u8) -> Command {
        Command::Prepared {
            id: [id; 16],
            participant: participant(),
        }
    }
    fn decide(id: u8, decision: Decision) -> Command {
        Command::Decide {
            id: [id; 16],
            decision,
        }
    }
    fn bound() -> Journal {
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .apply(Command::BindCohort { cohort: cohort() })
            .unwrap();
        journal
    }
    fn with_begins(count: u8) -> Journal {
        let mut journal = bound();
        for id in 1..=count {
            journal.apply(begin(id)).unwrap();
        }
        journal
    }
    fn forged(base: &HistoryCursor, events: Vec<Command>) -> HistoryPage {
        let mut end = base.clone();
        for event in &events {
            end.event_count += 1;
            end.root = event_root(end.root, end.event_count, event).unwrap();
        }
        HistoryPage {
            version: VERSION,
            base: base.clone(),
            cut: end.clone(),
            end,
            events,
        }
    }
    fn unchanged_rejection(local: &Journal, page: &HistoryPage) {
        let before = local.encode().unwrap();
        assert!(local.merge_history_page(page).is_err());
        assert_eq!(local.encode().unwrap(), before);
    }

    #[test]
    fn cursor_and_page_golden_vectors_are_exact_and_reopen_derived() {
        let mut journal = bound();
        let base = journal.history_cursor().unwrap();
        journal.apply(begin(1)).unwrap();
        journal.apply(prepared(1)).unwrap();
        journal.apply(decide(1, Decision::Commit)).unwrap();
        assert_eq!(
            serde_json::to_vec(&journal.history_cursor_at(0).unwrap()).unwrap(),
            include_bytes!("../tests/fixtures/history-zero-v1.json")
        );
        assert_eq!(
            serde_json::to_vec(&base).unwrap(),
            include_bytes!("../tests/fixtures/history-bound-v1.json")
        );
        let page = journal.history_page(&base, None).unwrap();
        assert_eq!(
            serde_json::to_vec(&page).unwrap(),
            include_bytes!("../tests/fixtures/history-page-v1.json")
        );
        assert_eq!(
            serde_json::from_slice::<HistoryPage>(include_bytes!(
                "../tests/fixtures/history-page-v1.json"
            ))
            .unwrap(),
            page
        );
        let reopened = Journal::decode(&journal.encode().unwrap()).unwrap();
        assert_eq!(reopened.history_cursor().unwrap(), page.cut);
        assert_eq!(reopened.history_prefix(&base).unwrap(), bound());
        let zero = reopened
            .history_prefix(&reopened.history_cursor_at(0).unwrap())
            .unwrap();
        assert_eq!(zero, Journal::new([9; 16]).unwrap());
        assert!(zero.history_cursor().is_err());
    }

    #[test]
    fn hash_binds_full_identity_command_length_position_and_domain() {
        let journal = with_begins(1);
        let base = journal.history_cursor_at(0).unwrap();
        let expected = base.root;
        for field in [
            "authority_mode",
            "shard",
            "coordinator_epoch",
            "member_shard",
            "member_epoch",
            "member_mode",
        ] {
            let mut cursor = base.clone();
            match field {
                "authority_mode" => cursor.authority.mode[0] += 1,
                "shard" => {
                    cursor.authority.shard += 1;
                    cursor.cohort.coordinator_shard += 1;
                }
                "coordinator_epoch" => cursor.cohort.coordinator_epoch += 1,
                "member_shard" => cursor.cohort.members[1].shard += 1,
                "member_epoch" => cursor.cohort.members[1].epoch += 1,
                "member_mode" => cursor.cohort.members[1].mode[0] += 1,
                _ => unreachable!(),
            }
            assert_ne!(
                identity_root(&cursor.authority, &cursor.cohort).unwrap(),
                expected,
                "{field}"
            );
            cursor.root = identity_root(&cursor.authority, &cursor.cohort).unwrap();
            cursor.validate().unwrap();
            assert!(cursor
                .validate_identity(&base.authority, &base.cohort)
                .is_err());
        }
        let mut reversed = base.cohort.clone();
        reversed.members.reverse();
        assert_ne!(identity_root(&base.authority, &reversed).unwrap(), expected);
        assert!(reversed.validate().is_err());
        let event = begin(1);
        let root = event_root(expected, 1, &event).unwrap();
        assert_ne!(root, event_root(expected, 2, &event).unwrap());
        assert_ne!(root, event_root([8; 32], 1, &event).unwrap());
        assert_ne!(root, event_root(expected, 1, &begin(2)).unwrap());
        assert_ne!(root, expected);
        let bytes = serde_json::to_vec(&event).unwrap();
        let mut wrong_length = Sha256::new();
        wrong_length.update(EVENT_DOMAIN);
        wrong_length.update(expected);
        wrong_length.update(1u64.to_be_bytes());
        wrong_length.update((bytes.len() as u64 + 1).to_be_bytes());
        wrong_length.update(&bytes);
        assert_ne!(root, <[u8; 32]>::from(wrong_length.finalize()));
        let mut wrong_domain = Sha256::new();
        wrong_domain.update(IDENTITY_DOMAIN);
        wrong_domain.update(expected);
        wrong_domain.update(1u64.to_be_bytes());
        wrong_domain.update((bytes.len() as u64).to_be_bytes());
        wrong_domain.update(&bytes);
        assert_ne!(root, <[u8; 32]>::from(wrong_domain.finalize()));
    }

    #[test]
    fn journal_storage_version_change_does_not_change_old_cursor() {
        let journal =
            Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
        let base = journal.history_cursor_at(1).unwrap();
        let before_admit = journal.history_prefix(&base).unwrap();
        assert_eq!(before_admit.encoding_version(), 2);
        assert_eq!(journal.encoding_version(), 3);
        assert_eq!(before_admit.history_cursor().unwrap(), base);
        assert_eq!(
            serde_json::to_vec(&journal.history_cursor().unwrap()).unwrap(),
            include_bytes!("../tests/fixtures/history-admitted-v1.json")
        );
        assert_eq!(
            before_admit
                .merge_history_page(&journal.history_page(&base, None).unwrap())
                .unwrap(),
            journal
        );
    }

    #[test]
    fn pages_obey_31_32_33_event_boundary_and_frozen_cut() {
        let base = bound().history_cursor().unwrap();
        for count in [31, 32, 33] {
            let mut journal = with_begins(count);
            let page = journal.history_page(&base, None).unwrap();
            assert_eq!(
                page.events.len(),
                usize::from(count).min(MAX_HISTORY_PAGE_EVENTS)
            );
            let cut = page.cut.clone();
            journal.apply(begin(99)).unwrap();
            let next = journal.history_page(&page.end, Some(&cut)).unwrap();
            assert_eq!(next.cut, cut);
            assert_eq!(next.end, cut);
            assert_eq!(next.events.len(), usize::from(count.saturating_sub(32)));
            let final_page = journal.history_page(&cut, Some(&cut)).unwrap();
            assert!(final_page.events.is_empty());
            assert_eq!(final_page.base, final_page.end);
            assert!(journal.history_cursor().unwrap().event_count > cut.event_count);
        }
    }

    #[test]
    fn exact_full_and_partial_overlap_retries_preserve_later_history() {
        let source = with_begins(40);
        let base = source.history_cursor_at(1).unwrap();
        let page = source.history_page(&base, None).unwrap();
        let local = source
            .history_prefix(&source.history_cursor_at(20).unwrap())
            .unwrap();
        let synced = local.merge_history_page(&page).unwrap();
        assert_eq!(synced.history_cursor().unwrap(), page.end);
        assert_eq!(synced.merge_history_page(&page).unwrap(), synced);
        let last = source.history_page(&page.end, Some(&page.cut)).unwrap();
        assert_eq!(synced.merge_history_page(&last).unwrap(), source);
        assert_eq!(source.merge_history_page(&page).unwrap(), source);
        assert_eq!(source.merge_history_page(&last).unwrap(), source);
        // The durable journal, not a redundant cursor field, reconstructs a lost-ACK retry.
        let reopened = Journal::decode(&synced.encode().unwrap()).unwrap();
        assert_eq!(reopened.merge_history_page(&page).unwrap(), synced);
    }

    #[test]
    fn supplied_overlap_is_exact_not_semantic_dedup() {
        let source = with_begins(2);
        let base = source.history_cursor_at(1).unwrap();
        for events in [
            vec![begin(2), begin(1)],
            vec![begin(2)],
            vec![begin(1), begin(1)],
            vec![begin(1), begin(2), begin(2)],
        ] {
            let page = forged(&base, events);
            page.validate().unwrap();
            unchanged_rejection(&source, &page);
        }
        let admitted =
            Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
        let base = admitted.history_cursor_at(1).unwrap();
        let mut changed = admitted.events[1].clone();
        if let Command::AdmitRequest { payload, .. } = &mut changed {
            payload.push(0);
        }
        // Original Admit retry semantics accept an exact request ID/bytes. A
        // supplied overlap still has to match the original Plan bytes exactly.
        let mut semantic_retry = admitted.clone();
        semantic_retry.apply(changed.clone()).unwrap();
        assert_eq!(semantic_retry, admitted);
        unchanged_rejection(&admitted, &forged(&base, vec![changed]));
    }

    #[test]
    fn gap_and_same_length_fork_fail_without_publication() {
        let source = with_begins(2);
        let page = source
            .history_page(&source.history_cursor_at(2).unwrap(), None)
            .unwrap();
        unchanged_rejection(&bound(), &page);
        let mut fork = bound();
        fork.apply(begin(3)).unwrap();
        unchanged_rejection(&fork, &page);
        assert!(source
            .history_cursor_at(source.events.len() as u64 + 1)
            .is_err());
        assert!(source.history_cursor_at(u64::MAX).is_err());
        assert!(source
            .history_page(&fork.history_cursor().unwrap(), None)
            .is_err());
        assert!(source
            .history_prefix(&fork.history_cursor().unwrap())
            .is_err());
    }

    #[test]
    fn unknown_noop_events_are_not_new_history() {
        let mut local = with_begins(1);
        local.apply(prepared(1)).unwrap();
        local.apply(decide(1, Decision::Commit)).unwrap();
        for command in [
            Command::BindCohort { cohort: cohort() },
            begin(1),
            prepared(1),
            decide(1, Decision::Commit),
        ] {
            let page = forged(&local.history_cursor().unwrap(), vec![command]);
            page.validate().unwrap();
            assert_eq!(local.merge_history_page(&page), Err(Error::InvalidJournal));
        }
        let admitted =
            Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
        let page = forged(
            &admitted.history_cursor().unwrap(),
            vec![admitted.events[1].clone()],
        );
        assert_eq!(
            admitted.merge_history_page(&page),
            Err(Error::InvalidJournal)
        );
    }

    #[test]
    fn strict_replay_rejects_forged_terminal_prepared_and_admission() {
        let local = with_begins(1);
        for command in [
            decide(1, Decision::Commit),
            prepared(2),
            decide(2, Decision::Abort),
            Command::AdmitRequest {
                id: [3; 16],
                request: vec![1],
                payload: vec![2],
                transaction_id: [2; 16],
                participants: vec![participant()],
            },
        ] {
            let page = forged(&local.history_cursor().unwrap(), vec![command]);
            page.validate().unwrap();
            unchanged_rejection(&local, &page);
        }
        let mut wrong_participant = participant();
        wrong_participant.intent[0] += 1;
        unchanged_rejection(
            &local,
            &forged(
                &local.history_cursor().unwrap(),
                vec![Command::Prepared {
                    id: [1; 16],
                    participant: wrong_participant,
                }],
            ),
        );
        // A valid earlier event is also unpublished if a later transition fails.
        unchanged_rejection(
            &bound(),
            &forged(
                &bound().history_cursor().unwrap(),
                vec![begin(1), decide(1, Decision::Commit)],
            ),
        );
    }

    #[test]
    fn untrusted_cut_is_checked_when_locally_available_not_persisted_as_proof() {
        let source = with_begins(40);
        let mut page = source
            .history_page(&bound().history_cursor().unwrap(), None)
            .unwrap();
        page.cut.root[0] ^= 1;
        page.validate().unwrap();
        let synced = bound().merge_history_page(&page).unwrap();
        assert_eq!(synced.history_cursor().unwrap(), page.end);
        unchanged_rejection(&source, &page);
        let mut next = source.history_page(&page.end, None).unwrap();
        next.cut = page.cut.clone();
        assert!(next.validate().is_err());
        unchanged_rejection(&synced, &next);
        assert!(synced.history_prefix(&page.cut).is_err());
        assert!(source.history_page(&page.end, Some(&page.cut)).is_err());
    }

    #[test]
    fn malformed_headers_roots_identity_counts_and_progress_reject() {
        let source = with_begins(3);
        let page = source
            .history_page(&bound().history_cursor().unwrap(), None)
            .unwrap();
        for field in [
            "page_version",
            "base_version",
            "end_version",
            "cut_version",
            "base_root",
            "end_root",
            "cut_root",
            "base_count",
            "end_count",
            "cut_count",
            "impossible_count",
            "authority",
            "epoch",
            "member_mode",
            "missing",
            "extra",
            "reordered",
            "no_progress",
        ] {
            let mut bad = page.clone();
            match field {
                "page_version" => bad.version += 1,
                "base_version" => bad.base.version += 1,
                "end_version" => bad.end.version += 1,
                "cut_version" => bad.cut.version += 1,
                "base_root" => bad.base.root[0] ^= 1,
                "end_root" => bad.end.root[0] ^= 1,
                "cut_root" => bad.cut.root[0] ^= 1,
                "base_count" => bad.base.event_count = bad.end.event_count + 1,
                "end_count" => bad.end.event_count -= 1,
                "cut_count" => bad.cut.event_count -= 1,
                "impossible_count" => bad.cut.event_count = MAX_HISTORY_EVENTS + 1,
                "authority" => bad.end.authority.mode[0] += 1,
                "epoch" => bad.cut.cohort.coordinator_epoch += 1,
                "member_mode" => bad.base.cohort.members[1].mode[0] += 1,
                "missing" => {
                    bad.events.remove(1);
                }
                "extra" => bad.events.push(begin(4)),
                "reordered" => bad.events.swap(0, 1),
                "no_progress" => {
                    bad.events.clear();
                    bad.end = bad.base.clone();
                }
                _ => unreachable!(),
            }
            assert!(bad.validate().is_err(), "{field}");
            assert!(
                serde_json::from_slice::<HistoryPage>(&serde_json::to_vec(&bad).unwrap()).is_err(),
                "{field}"
            );
            unchanged_rejection(&bound(), &bad);
        }
        let later = source.history_cursor().unwrap();
        assert!(source.history_page(&later, Some(&page.base)).is_err());
    }

    #[test]
    fn cursor_serde_checks_version_identity_zero_root_and_total_count() {
        let cursor = bound().history_cursor().unwrap();
        for field in [
            "version",
            "count",
            "mode",
            "shard",
            "empty_cohort",
            "member_order",
            "zero_root",
        ] {
            let mut bad = cursor.clone();
            match field {
                "version" => bad.version += 1,
                "count" => bad.event_count = MAX_HISTORY_EVENTS + 1,
                "mode" => bad.authority.mode = [0; 16],
                "shard" => bad.authority.shard += 1,
                "empty_cohort" => bad.cohort.members.clear(),
                "member_order" => bad.cohort.members.reverse(),
                "zero_root" => bad.event_count = 0,
                _ => unreachable!(),
            }
            assert!(bad.validate().is_err(), "{field}");
            assert!(
                serde_json::from_slice::<HistoryCursor>(&serde_json::to_vec(&bad).unwrap())
                    .is_err(),
                "{field}"
            );
        }
        let mut boundary = cursor.clone();
        boundary.event_count = MAX_HISTORY_EVENTS;
        boundary.validate().unwrap();
        let mut value = serde_json::to_value(&cursor).unwrap();
        value["extra"] = true.into();
        assert!(serde_json::from_value::<HistoryCursor>(value).is_err());
        let mut value = serde_json::to_value(bound().history_page(&cursor, None).unwrap()).unwrap();
        value["extra"] = true.into();
        assert!(serde_json::from_value::<HistoryPage>(value).is_err());
    }

    #[test]
    fn page_deserializer_rejects_33rd_event_before_materializing_it() {
        let source = with_begins(33);
        let page = source
            .history_page(&bound().history_cursor().unwrap(), None)
            .unwrap();
        let mut value = serde_json::to_value(&page).unwrap();
        value["events"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::Value::Null);
        let error = serde_json::from_slice::<HistoryPage>(&serde_json::to_vec(&value).unwrap())
            .unwrap_err();
        assert!(error.to_string().contains("too many history events"));
        let too_many = forged(&page.base, (1..=33).map(begin).collect());
        assert!(too_many.validate().is_err());
    }

    fn command_with_size(target: usize) -> Command {
        let mut event = Command::AdmitRequest {
            id: [1; 16],
            request: vec![0],
            payload: vec![0],
            transaction_id: [2; 16],
            participants: vec![participant()],
        };
        let initial = serde_json::to_vec(&event).unwrap().len();
        let extra = target - initial;
        if let Command::AdmitRequest { request, .. } = &mut event {
            request.resize(1 + extra / 2, 0);
            if !extra.is_multiple_of(2) {
                request[0] = 10;
            }
        }
        assert_eq!(serde_json::to_vec(&event).unwrap().len(), target);
        event
    }

    #[test]
    fn canonical_byte_budget_accepts_exact_limit_rejects_one_more() {
        let local = bound();
        let base = local.history_cursor().unwrap();
        let event = command_with_size(MAX_HISTORY_PAGE_BYTES);
        assert_eq!(
            event_size(&event, MAX_HISTORY_PAGE_BYTES).unwrap(),
            MAX_HISTORY_PAGE_BYTES
        );
        let page = forged(&base, vec![event]);
        page.validate().unwrap();
        serde_json::from_slice::<HistoryPage>(&serde_json::to_vec(&page).unwrap()).unwrap();
        // The transport shape alone does not bypass the original admission limits.
        unchanged_rejection(&local, &page);
        let mut oversized = page.clone();
        oversized.events[0] = command_with_size(MAX_HISTORY_PAGE_BYTES + 1);
        assert_eq!(oversized.validate(), Err(Error::Capacity));
        assert!(
            serde_json::from_slice::<HistoryPage>(&serde_json::to_vec(&oversized).unwrap())
                .is_err()
        );
        // Sum, not merely each individual event, is bounded.
        oversized.events = vec![command_with_size(MAX_HISTORY_PAGE_BYTES / 2 + 1); 2];
        oversized.end.event_count += 1;
        oversized.cut = oversized.end.clone();
        assert_eq!(oversized.validate(), Err(Error::Capacity));
    }

    #[test]
    fn admitted_large_events_page_by_bytes_without_losing_progress() {
        let fixture =
            Journal::decode(include_bytes!("../tests/fixtures/request-journal-v3.json")).unwrap();
        let base = fixture.history_cursor_at(1).unwrap();
        let mut journal = fixture.history_prefix(&base).unwrap();
        for id in 1..=6u8 {
            let mut event = fixture.events[1].clone();
            if let Command::AdmitRequest {
                id: external,
                request,
                payload,
                transaction_id,
                ..
            } = &mut event
            {
                *external = [id; 16];
                *request = vec![255; MAX_REQUEST_BYTES];
                *transaction_id = [id; 16];
                let mut plan = crate::request_plans::Plan::decode(payload).unwrap();
                plan.id = [id; 16];
                plan.parts[0].request.request_id = [id; 16];
                plan.parts[0].request.operations = vec![crate::WriteOp::Put {
                    key: vec![1],
                    value: vec![255; 50_000],
                }];
                *payload = plan.encode().unwrap();
            }
            journal.apply(event).unwrap();
        }
        let page = journal.history_page(&base, None).unwrap();
        assert!(!page.events.is_empty());
        assert!(page.events.len() < 6);
        let bytes: usize = page
            .events
            .iter()
            .map(|event| event_size(event, MAX_HISTORY_PAGE_BYTES).unwrap())
            .sum();
        assert!(bytes <= MAX_HISTORY_PAGE_BYTES);
        assert!(
            bytes
                + event_size(
                    &journal.events[page.end.event_count as usize],
                    MAX_HISTORY_PAGE_BYTES
                )
                .unwrap()
                > MAX_HISTORY_PAGE_BYTES
        );
        let local = journal
            .history_prefix(&base)
            .unwrap()
            .merge_history_page(&page)
            .unwrap();
        let next = journal.history_page(&page.end, Some(&page.cut)).unwrap();
        assert!(!next.events.is_empty());
        assert_eq!(next.end, page.cut);
        assert_eq!(local.merge_history_page(&next).unwrap(), journal);
    }

    #[test]
    fn observed_terminal_prefix_is_not_an_ordinal_cut_before_resolution() {
        let mut journal = with_begins(2);
        let pending = journal.history_cursor().unwrap();
        journal.apply(decide(2, Decision::Abort)).unwrap();
        let hole = journal.history_cursor().unwrap();
        assert_eq!(journal.history_prefix(&hole).unwrap().resolved_prefix(), 0);
        journal.apply(decide(1, Decision::Abort)).unwrap();
        let complete = journal.history_cursor().unwrap();
        assert_eq!(
            journal.history_prefix(&pending).unwrap().resolved_prefix(),
            0
        );
        assert_eq!(
            journal.history_prefix(&complete).unwrap().resolved_prefix(),
            2
        );
        assert_eq!(journal.history_cursor().unwrap(), complete);
    }
}
