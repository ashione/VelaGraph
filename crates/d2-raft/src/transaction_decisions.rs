//! Deterministic coordinator decision journal, persisted by the dedicated D2 role.
//! Data-shard participant preparation and global visibility are not implemented.
//!
//! A resolved decision prefix is NOT a readable data version. Prepared evidence
//! must eventually come from a fenced participant protocol, not an arbitrary caller.
use crate::capacity_policy::{
    canonical_size, deserialize_bounded_vec, validate_grants, BYTE_MAX_HISTORY_EVENTS,
};
pub use crate::capacity_policy::{CapacityActivation, CapacityProfile, MemberCapacityGrant};
use serde::{Deserialize, Deserializer, Serialize};

#[path = "journal_history.rs"]
mod history;
pub use history::{HistoryCursor, HistoryPage, MAX_HISTORY_PAGE_BYTES, MAX_HISTORY_PAGE_EVENTS};

#[cfg(test)]
#[path = "capacity_proofs.rs"]
mod capacity_proofs;

pub const MAX_TRANSACTIONS: usize = 1024;
pub const MAX_PARTICIPANTS: usize = 64;
pub const MAX_JOURNAL_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_REQUEST_PAYLOAD_BYTES: usize = 256 * 1024;
pub const MAX_REQUESTS: usize = 256;
pub const MAX_REQUEST_EVENT_BYTES: usize = 2 * 1024 * 1024;

fn deserialize_members<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Member>, D::Error> {
    deserialize_bounded_vec(d, MAX_PARTICIPANTS)
}
fn deserialize_grants<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<MemberCapacityGrant>, D::Error> {
    deserialize_bounded_vec(d, MAX_PARTICIPANTS)
}
fn deserialize_journal_events<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Command>, D::Error> {
    deserialize_bounded_vec(d, BYTE_MAX_HISTORY_EVENTS)
}
pub(crate) fn deserialize_participants<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Vec<Participant>, D::Error> {
    deserialize_bounded_vec(d, MAX_PARTICIPANTS)
}
pub(crate) fn deserialize_prepared<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<bool>, D::Error> {
    deserialize_bounded_vec(d, MAX_PARTICIPANTS)
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Participant {
    pub shard: u64,
    pub epoch: u64,
    pub mode: [u8; 16],
    /// Exact future participant intent identity; not an authentication token.
    pub intent: [u8; 32],
}

/// Fixed ownership identities for a graph starting from empty participant baselines.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Member {
    pub shard: u64,
    pub epoch: u64,
    pub mode: [u8; 16],
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cohort {
    pub coordinator_shard: u64,
    pub coordinator_epoch: u64,
    #[serde(deserialize_with = "deserialize_members")]
    pub members: Vec<Member>,
    #[serde(
        default,
        skip_serializing_if = "CapacityProfile::is_legacy",
        deserialize_with = "CapacityProfile::deserialize_cohort_tag"
    )]
    pub profile: CapacityProfile,
}
impl Cohort {
    pub fn contains(&self, participant: &Participant) -> bool {
        self.members.iter().any(|m| {
            m.shard == participant.shard
                && m.epoch == participant.epoch
                && m.mode == participant.mode
        })
    }
    pub fn validate(&self) -> Result<(), Error> {
        if self.members.is_empty()
            || self.members.len() > MAX_PARTICIPANTS
            || self
                .members
                .iter()
                .any(|m| m.mode == [0; 16] || m.shard == self.coordinator_shard)
            || self.members.windows(2).any(|w| w[0].shard >= w[1].shard)
        {
            return Err(Error::InvalidParticipants);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    Commit,
    Abort,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Command {
    /// Atomically bind an external request to its exact durable payload and
    /// create the transaction, including reservation for its final decision.
    AdmitRequest {
        id: [u8; 16],
        request: Vec<u8>,
        payload: Vec<u8>,
        transaction_id: [u8; 16],
        #[serde(deserialize_with = "deserialize_participants")]
        participants: Vec<Participant>,
    },
    BindCohort {
        cohort: Cohort,
    },
    /// Privileged coordinator proposal after every fixed member's durable grant
    /// has been checked. Ordinary managed Command RPCs must reject this variant.
    ConfirmCohortCapacity {
        cohort: Cohort,
        #[serde(deserialize_with = "deserialize_grants")]
        grants: Vec<MemberCapacityGrant>,
    },
    Begin {
        id: [u8; 16],
        #[serde(deserialize_with = "deserialize_participants")]
        participants: Vec<Participant>,
    },
    Prepared {
        id: [u8; 16],
        participant: Participant,
    },
    Decide {
        id: [u8; 16],
        decision: Decision,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Error {
    InvalidIdentity,
    InvalidParticipants,
    IdentityConflict,
    UnknownTransaction,
    UnknownParticipant,
    NotPrepared,
    AlreadyDecided,
    Capacity,
    CapacityNotConfirmed,
    InvalidJournal,
    IncompatibleHistory,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestRecord {
    pub id: [u8; 16],
    pub request: Vec<u8>,
    pub payload: Vec<u8>,
    pub transaction_id: [u8; 16],
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transaction {
    pub id: [u8; 16],
    pub ordinal: u64,
    #[serde(deserialize_with = "deserialize_participants")]
    pub participants: Vec<Participant>,
    #[serde(deserialize_with = "deserialize_prepared")]
    pub prepared: Vec<bool>,
    pub decision: Option<Decision>,
}

impl Transaction {
    /// Shared transition rules for strict replay and one-transaction projection.
    /// A false result is an exact retry, which raw retained history rejects.
    pub(crate) fn record_prepared(&mut self, participant: &Participant) -> Result<bool, Error> {
        let index = self
            .participants
            .iter()
            .position(|p| p == participant)
            .ok_or(Error::UnknownParticipant)?;
        if self.prepared[index] {
            return Ok(false);
        }
        if self.decision.is_some() {
            return Err(Error::AlreadyDecided);
        }
        self.prepared[index] = true;
        Ok(true)
    }

    pub(crate) fn record_decision(&mut self, decision: Decision) -> Result<bool, Error> {
        if let Some(current) = self.decision {
            return if current == decision {
                Ok(false)
            } else {
                Err(Error::AlreadyDecided)
            };
        }
        if decision == Decision::Commit && !self.prepared.iter().all(|p| *p) {
            return Err(Error::NotPrepared);
        }
        self.decision = Some(decision);
        Ok(true)
    }
}

/// Immutable append-only history. The private state cannot be deserialized
/// without replaying every transition. No timeout implicitly aborts a transaction.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(not(test), derive(Clone))]
#[serde(try_from = "Encoding", into = "Encoding")]
pub struct Journal {
    authority: [u8; 16],
    events: Vec<Command>,
    transactions: Vec<Transaction>,
    reserved_bytes: usize,
    // Keep the private binding out of every enclosing Raft command layout.
    // Encoding remains the original event sequence, not this derived field.
    cohort: Option<Box<Cohort>>,
    requests: Vec<RequestRecord>,
    capacity_grants: Option<Vec<MemberCapacityGrant>>,
}

#[cfg(test)]
thread_local! {
    static FULL_JOURNAL_FORBIDDEN: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(test)]
pub(crate) fn assert_full_journal_allowed() {
    FULL_JOURNAL_FORBIDDEN.with(|forbidden| {
        assert!(
            !forbidden.get(),
            "bounded archive reader attempted full Journal access"
        );
    });
}

/// Test-only tripwire: selected readers run after all source Journals are
/// dropped and must not construct, decode, clone or reconstruct a full prefix.
#[cfg(test)]
pub(crate) fn without_full_journal<T>(read: impl FnOnce() -> T) -> T {
    struct Reset(bool);
    impl Drop for Reset {
        fn drop(&mut self) {
            FULL_JOURNAL_FORBIDDEN.with(|forbidden| forbidden.set(self.0));
        }
    }
    let _reset = Reset(FULL_JOURNAL_FORBIDDEN.with(|forbidden| forbidden.replace(true)));
    read()
}

#[cfg(test)]
impl Clone for Journal {
    fn clone(&self) -> Self {
        assert_full_journal_allowed();
        Self {
            authority: self.authority,
            events: self.events.clone(),
            transactions: self.transactions.clone(),
            reserved_bytes: self.reserved_bytes,
            cohort: self.cohort.clone(),
            requests: self.requests.clone(),
            capacity_grants: self.capacity_grants.clone(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Encoding {
    version: u32,
    authority: [u8; 16],
    #[serde(deserialize_with = "deserialize_journal_events")]
    events: Vec<Command>,
}

impl From<Journal> for Encoding {
    fn from(journal: Journal) -> Self {
        Self {
            version: journal.encoding_version(),
            authority: journal.authority,
            events: journal.events,
        }
    }
}
impl TryFrom<Encoding> for Journal {
    type Error = String;
    fn try_from(encoded: Encoding) -> Result<Self, String> {
        let bytes = serde_json::to_vec(&encoded).map_err(|e| e.to_string())?;
        Self::decode(&bytes).map_err(|e| format!("invalid journal: {e:?}"))
    }
}

impl Journal {
    pub fn new(authority: [u8; 16]) -> Result<Self, Error> {
        #[cfg(test)]
        assert_full_journal_allowed();
        if authority == [0; 16] {
            return Err(Error::InvalidIdentity);
        }
        let mut journal = Self {
            authority,
            events: Vec::new(),
            transactions: Vec::new(),
            reserved_bytes: 0,
            cohort: None,
            requests: Vec::new(),
            capacity_grants: None,
        };
        journal.reserved_bytes = journal.encode()?.len();
        Ok(journal)
    }

    fn encoding_version(&self) -> u32 {
        if self.capacity_profile() == CapacityProfile::ByteBound16MiBV1 {
            4
        } else if !self.requests.is_empty() {
            3
        } else if self.cohort.is_some() {
            2
        } else {
            1
        }
    }
    /// Find the durable external request binding for a transaction identity.
    pub fn request_for_transaction(&self, id: [u8; 16]) -> Option<&RequestRecord> {
        self.requests.iter().find(|r| r.transaction_id == id)
    }

    pub fn request(&self, id: [u8; 16]) -> Option<&RequestRecord> {
        self.requests.iter().find(|r| r.id == id)
    }

    /// Borrow original retained commands without serializing an archive.
    pub(crate) fn events(&self) -> &[Command] {
        &self.events
    }

    /// Strict replay on an unpublished candidate. A failure may have changed
    /// the candidate, which must then be discarded. Exact retry no-ops are not
    /// events and are rejected, just like a malformed archived history.
    pub(crate) fn replay_event(&mut self, command: Command) -> Result<(), Error> {
        self.replay_event_with_limit(command, MAX_JOURNAL_BYTES)
    }

    fn replay_event_with_limit(&mut self, command: Command, limit: usize) -> Result<(), Error> {
        if !self.append(command, limit)? {
            return Err(Error::InvalidJournal);
        }
        Ok(())
    }

    pub(crate) fn requests(&self) -> &[RequestRecord] {
        &self.requests
    }

    pub fn cohort(&self) -> Option<&Cohort> {
        self.cohort.as_deref()
    }

    /// The immutable bound policy. Unbound journals retain legacy semantics.
    pub fn capacity_profile(&self) -> CapacityProfile {
        self.cohort
            .as_ref()
            .map_or(CapacityProfile::LegacyCountV1, |c| c.profile)
    }

    /// Complete original grant data reconstructed only by strict event replay.
    pub fn capacity_grants(&self) -> Option<&[MemberCapacityGrant]> {
        self.capacity_grants.as_deref()
    }

    /// Legacy journals need no grant; byte-bound journals need full confirmation.
    pub fn capacity_ready(&self) -> bool {
        self.capacity_profile().is_legacy() || self.capacity_grants.is_some()
    }

    pub fn transaction_count(&self) -> usize {
        self.transactions.len()
    }
    pub fn transaction_at(&self, ordinal: u64) -> Option<&Transaction> {
        let index = usize::try_from(ordinal.checked_sub(1)?).ok()?;
        self.transactions.get(index)
    }

    pub fn authority(&self) -> [u8; 16] {
        self.authority
    }

    pub fn transaction(&self, id: [u8; 16]) -> Option<&Transaction> {
        self.transactions.iter().find(|t| t.id == id)
    }

    /// Includes aborts, stops at the first unresolved transaction, and never
    /// certifies participant application or query visibility.
    pub fn resolved_prefix(&self) -> u64 {
        self.transactions
            .iter()
            .take_while(|t| t.decision.is_some())
            .count() as u64
    }

    /// Rejections and exact retries leave the serialized state unchanged.
    /// Callers must persist the resulting journal atomically with their applied
    /// marker before exposing it. This method performs no I/O itself.
    pub fn apply(&mut self, command: Command) -> Result<(), Error> {
        let mut candidate = self.clone();
        if !candidate.append(command, MAX_JOURNAL_BYTES)? {
            return Ok(());
        }
        *self = candidate;
        Ok(())
    }

    fn event_bytes(command: &Command) -> Result<usize, Error> {
        canonical_size(command, MAX_REQUEST_EVENT_BYTES)
    }

    fn pending_bytes(tx: &Transaction) -> Result<usize, Error> {
        let mut size = Self::event_bytes(&Command::Decide {
            id: tx.id,
            decision: Decision::Commit,
        })?
        .checked_add(1)
        .ok_or(Error::Capacity)?;
        for (participant, prepared) in tx.participants.iter().zip(&tx.prepared) {
            if !prepared {
                size = size
                    .checked_add(Self::event_bytes(&Command::Prepared {
                        id: tx.id,
                        participant: participant.clone(),
                    })?)
                    .and_then(|n| n.checked_add(1))
                    .ok_or(Error::Capacity)?;
            }
        }
        Ok(size)
    }

    // Used on an unpublished candidate or during recovery; every prefix obeys
    // the exact same admission rule. Replay work is linear in bounded events
    // times the bounded participant/transaction lookup sizes, not journal bytes.
    fn append(&mut self, command: Command, limit: usize) -> Result<bool, Error> {
        let released = match &command {
            Command::AdmitRequest { .. }
            | Command::BindCohort { .. }
            | Command::ConfirmCohortCapacity { .. }
            | Command::Begin { .. } => 0,
            Command::Prepared { .. } => Self::event_bytes(&command)?
                .checked_add(1)
                .ok_or(Error::Capacity)?,
            Command::Decide { id, .. } => self
                .transaction(*id)
                .map(Self::pending_bytes)
                .transpose()?
                .unwrap_or(0),
        };
        let old_envelope = self.envelope_bytes()?;
        if !self.transition(&command)? {
            return Ok(false);
        }
        let event_size = Self::event_bytes(&command)?;
        let future = match &command {
            Command::Begin { id, .. } => Self::pending_bytes(self.transaction(*id).unwrap())?,
            Command::AdmitRequest { transaction_id, .. } => {
                Self::pending_bytes(self.transaction(*transaction_id).unwrap())?
            }
            _ => 0,
        };
        // Charge the actual versioned envelope too. Equal digit widths of
        // today's versions are tested, never assumed by the reservation rule.
        let new_envelope = self.envelope_bytes()?;
        self.reserved_bytes = self
            .reserved_bytes
            .checked_sub(released)
            .and_then(|n| n.checked_sub(old_envelope))
            .and_then(|n| n.checked_add(new_envelope))
            .and_then(|n| n.checked_add(event_size))
            .and_then(|n| n.checked_add(usize::from(!self.events.is_empty())))
            .and_then(|n| n.checked_add(future))
            .ok_or(Error::Capacity)?;
        if self.events.len()
            >= usize::try_from(self.capacity_profile().max_history_events())
                .map_err(|_| Error::Capacity)?
        {
            return Err(Error::Capacity);
        }
        if self.reserved_bytes > limit {
            return Err(Error::Capacity);
        }
        self.events.push(command);
        Ok(true)
    }

    fn transition(&mut self, command: &Command) -> Result<bool, Error> {
        match command {
            Command::AdmitRequest {
                id,
                request,
                payload,
                transaction_id,
                participants,
            } => {
                if *id == [0; 16]
                    || request.is_empty()
                    || payload.is_empty()
                    || request.len() > MAX_REQUEST_BYTES
                    || payload.len() > MAX_REQUEST_PAYLOAD_BYTES
                    || self.cohort.is_none()
                {
                    return Err(Error::InvalidIdentity);
                }
                if let Some(existing) = self.request(*id) {
                    return if existing.request == *request {
                        Ok(false)
                    } else {
                        Err(Error::IdentityConflict)
                    };
                }
                if !self.capacity_ready() {
                    return Err(Error::CapacityNotConfirmed);
                }
                let plan = crate::request_plans::Plan::decode(payload)?;
                if plan.id != *transaction_id
                    || plan.authority.mode != self.authority
                    || Some(&plan.cohort) != self.cohort.as_deref()
                    || plan.validation_version > self.resolved_prefix()
                    || plan
                        .parts
                        .iter()
                        .map(|p| p.descriptor.clone())
                        .collect::<Vec<_>>()
                        != *participants
                {
                    return Err(Error::IdentityConflict);
                }
                if self.requests.len() >= self.capacity_profile().max_requests() {
                    return Err(Error::Capacity);
                }
                if self.transaction(*transaction_id).is_some() {
                    return Err(Error::IdentityConflict);
                }
                self.transition(&Command::Begin {
                    id: *transaction_id,
                    participants: participants.clone(),
                })?;
                self.requests.push(RequestRecord {
                    id: *id,
                    request: request.clone(),
                    payload: payload.clone(),
                    transaction_id: *transaction_id,
                });
            }
            Command::BindCohort { cohort } => {
                cohort.validate()?;
                if let Some(existing) = self.cohort.as_deref() {
                    return if existing == cohort {
                        Ok(false)
                    } else {
                        Err(Error::IdentityConflict)
                    };
                }
                if !self.events.is_empty() {
                    return Err(Error::IncompatibleHistory);
                }
                self.cohort = Some(Box::new(cohort.clone()));
            }
            Command::ConfirmCohortCapacity { cohort, grants } => {
                if self.cohort.as_deref() != Some(cohort) {
                    return Err(Error::IdentityConflict);
                }
                validate_grants(cohort, grants)?;
                if let Some(existing) = &self.capacity_grants {
                    return if existing == grants {
                        Ok(false)
                    } else {
                        Err(Error::IdentityConflict)
                    };
                }
                if !self.transactions.is_empty() || !self.requests.is_empty() {
                    return Err(Error::IncompatibleHistory);
                }
                self.capacity_grants = Some(grants.clone());
            }
            Command::Begin { id, participants } => {
                if *id == [0; 16] {
                    return Err(Error::InvalidIdentity);
                }
                if participants.is_empty()
                    || participants.len() > MAX_PARTICIPANTS
                    || participants
                        .iter()
                        .any(|p| p.mode == [0; 16] || p.intent == [0; 32])
                    || participants
                        .windows(2)
                        .any(|pair| pair[0].shard >= pair[1].shard)
                {
                    return Err(Error::InvalidParticipants);
                }
                if self
                    .cohort
                    .as_ref()
                    .is_some_and(|c| participants.iter().any(|p| !c.contains(p)))
                {
                    return Err(Error::UnknownParticipant);
                }
                if let Some(tx) = self.transaction(*id) {
                    return if tx.participants == *participants {
                        Ok(false)
                    } else {
                        Err(Error::IdentityConflict)
                    };
                }
                if !self.capacity_ready() {
                    return Err(Error::CapacityNotConfirmed);
                }
                if self.transactions.len() >= self.capacity_profile().max_transactions() {
                    return Err(Error::Capacity);
                }
                self.transactions.push(Transaction {
                    id: *id,
                    ordinal: u64::try_from(self.transactions.len())
                        .ok()
                        .and_then(|n| n.checked_add(1))
                        .ok_or(Error::Capacity)?,
                    participants: participants.clone(),
                    prepared: vec![false; participants.len()],
                    decision: None,
                });
            }
            Command::Prepared { id, participant } => {
                let tx = self
                    .transactions
                    .iter_mut()
                    .find(|t| t.id == *id)
                    .ok_or(Error::UnknownTransaction)?;
                return tx.record_prepared(participant);
            }
            Command::Decide { id, decision } => {
                let tx = self
                    .transactions
                    .iter_mut()
                    .find(|t| t.id == *id)
                    .ok_or(Error::UnknownTransaction)?;
                return tx.record_decision(*decision);
            }
        }
        Ok(true)
    }

    #[cfg(test)]
    fn reserved_size(&self) -> Result<usize, Error> {
        Ok(self.reserved_bytes)
    }

    fn envelope_bytes(&self) -> Result<usize, Error> {
        canonical_size(
            &Encoding {
                version: self.encoding_version(),
                authority: self.authority,
                events: Vec::new(),
            },
            MAX_JOURNAL_BYTES,
        )
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(&Encoding {
            version: self.encoding_version(),
            authority: self.authority,
            events: self.events.clone(),
        })
        .map_err(|_| Error::InvalidJournal)
    }

    /// Strict structural and semantic replay; serialized derived watermarks are
    /// intentionally absent. Bounded by input bytes and finite unique events.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        #[cfg(test)]
        assert_full_journal_allowed();
        Self::decode_with_limit(bytes, MAX_JOURNAL_BYTES)
    }

    fn decode_with_limit(bytes: &[u8], limit: usize) -> Result<Self, Error> {
        if bytes.len() > limit {
            return Err(Error::Capacity);
        }
        let encoded: Encoding = serde_json::from_slice(bytes).map_err(|_| Error::InvalidJournal)?;
        let profile = if encoded.version == 4 {
            CapacityProfile::ByteBound16MiBV1
        } else {
            CapacityProfile::LegacyCountV1
        };
        if !matches!(encoded.version, 1..=4)
            || u64::try_from(encoded.events.len()).map_err(|_| Error::InvalidJournal)?
                > profile.max_history_events()
        {
            return Err(Error::InvalidJournal);
        }
        let mut result = Self::new(encoded.authority)?;
        for command in encoded.events {
            result.replay_event_with_limit(command, limit)?;
        }
        if encoded.version != result.encoding_version() {
            return Err(Error::InvalidJournal);
        }
        Ok(result)
    }

    /// Equality of the immutable terminal transaction cut, not later journal observations.
    pub fn same_terminal_prefix(&self, other: &Self, version: u64) -> bool {
        self.authority == other.authority
            && self.cohort == other.cohort
            && version <= self.resolved_prefix()
            && version <= other.resolved_prefix()
            && (1..=version).all(|v| self.transaction_at(v) == other.transaction_at(v))
    }

    /// Snapshot replacement may only extend this exact authority's history.
    /// A fresh process still needs an independently trusted durable baseline;
    /// this does not detect restoring an entire old database directory.
    pub fn validate_successor(&self, next: &Self) -> Result<(), Error> {
        if self.authority != next.authority || !next.events.starts_with(&self.events) {
            return Err(Error::IncompatibleHistory);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn participant(shard: u64) -> Participant {
        Participant {
            shard,
            epoch: 1,
            mode: [1; 16],
            intent: [2; 32],
        }
    }
    fn begin(id: u8) -> Command {
        Command::Begin {
            id: [id; 16],
            participants: vec![participant(1), participant(2)],
        }
    }
    fn prepared(id: u8, shard: u64) -> Command {
        Command::Prepared {
            id: [id; 16],
            participant: participant(shard),
        }
    }
    fn decide(id: u8, decision: Decision) -> Command {
        Command::Decide {
            id: [id; 16],
            decision,
        }
    }
    #[test]
    fn replay_rejects_capacity_hole_hidden_by_later_aborts() {
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(begin(1)).unwrap();
        j.apply(begin(2)).unwrap();
        let peak = j.reserved_bytes;
        j.apply(decide(1, Decision::Abort)).unwrap();
        j.apply(decide(2, Decision::Abort)).unwrap();
        let bytes = j.encode().unwrap();
        let limit = bytes.len();
        assert!(limit < peak);
        assert_eq!(
            Journal::decode_with_limit(&bytes, limit),
            Err(Error::Capacity)
        );
    }

    #[test]
    fn cached_reservation_matches_recomputed_bytes_after_every_event() {
        let mut j = Journal::new([9; 16]).unwrap();
        for command in [
            begin(1),
            begin(2),
            prepared(1, 1),
            prepared(1, 1),
            begin(2),
            decide(1, Decision::Abort),
            decide(1, Decision::Abort),
            prepared(2, 1),
            prepared(2, 2),
            decide(2, Decision::Commit),
        ] {
            j.apply(command).unwrap();
            let pending: usize = j
                .transactions
                .iter()
                .filter(|t| t.decision.is_none())
                .map(|t| Journal::pending_bytes(t).unwrap())
                .sum();
            assert_eq!(j.reserved_bytes, j.encode().unwrap().len() + pending);
            assert_eq!(Journal::decode(&j.encode().unwrap()).unwrap(), j);
        }
    }

    #[test]
    fn resolution_capacity_is_reserved_before_begin() {
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(begin(1)).unwrap();
        let reserved = j.reserved_size().unwrap();
        j.apply(prepared(1, 1)).unwrap();
        assert_eq!(j.reserved_size().unwrap(), reserved);
        j.apply(prepared(1, 2)).unwrap();
        assert_eq!(j.reserved_size().unwrap(), reserved);
        j.apply(decide(1, Decision::Commit)).unwrap();
        assert_eq!(j.encode().unwrap().len(), reserved);
        assert_eq!(Journal::decode(&j.encode().unwrap()).unwrap(), j);
    }

    #[test]
    fn format_and_participant_limits_are_strict() {
        let empty = Journal::new([9; 16]).unwrap();
        let expected = format!(
            "{{\"version\":1,\"authority\":[{}],\"events\":[]}}",
            vec!["9"; 16].join(",")
        );
        assert_eq!(empty.encode().unwrap(), expected.as_bytes());
        let mut j = empty;
        assert_eq!(
            j.apply(Command::Begin {
                id: [1; 16],
                participants: (0..=MAX_PARTICIPANTS as u64).map(participant).collect()
            }),
            Err(Error::InvalidParticipants)
        );
        assert_eq!(
            Journal::decode(
                expected
                    .replace("\"version\":1", "\"version\":2")
                    .as_bytes()
            ),
            Err(Error::InvalidJournal)
        );
        assert_eq!(
            Journal::decode(
                expected
                    .replace("\"events\":[]", "\"events\":[],\"prefix\":3")
                    .as_bytes()
            ),
            Err(Error::InvalidJournal)
        );
    }

    #[test]
    fn holes_and_abort_do_not_become_visible_commits() {
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(begin(1)).unwrap();
        j.apply(begin(2)).unwrap();
        j.apply(prepared(2, 1)).unwrap();
        j.apply(prepared(2, 2)).unwrap();
        j.apply(decide(2, Decision::Commit)).unwrap();
        assert_eq!(j.resolved_prefix(), 0);
        let restored = Journal::decode(&j.encode().unwrap()).unwrap();
        assert_eq!(restored, j);
        j.apply(decide(1, Decision::Abort)).unwrap();
        assert_eq!(j.resolved_prefix(), 2);
        assert_eq!(
            j.transaction([1; 16]).unwrap().decision,
            Some(Decision::Abort)
        );
    }
    #[test]
    fn rejections_and_retries_never_mutate_state() {
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(begin(1)).unwrap();
        let bytes = j.encode().unwrap();
        assert_eq!(
            j.apply(decide(1, Decision::Commit)),
            Err(Error::NotPrepared)
        );
        assert_eq!(j.apply(prepared(1, 3)), Err(Error::UnknownParticipant));
        assert_eq!(
            j.apply(decide(2, Decision::Abort)),
            Err(Error::UnknownTransaction)
        );
        j.apply(begin(1)).unwrap();
        assert_eq!(bytes, j.encode().unwrap());
        j.apply(decide(1, Decision::Abort)).unwrap();
        let bytes = j.encode().unwrap();
        assert_eq!(j.apply(prepared(1, 1)), Err(Error::AlreadyDecided));
        assert_eq!(
            j.apply(decide(1, Decision::Commit)),
            Err(Error::AlreadyDecided)
        );
        j.apply(decide(1, Decision::Abort)).unwrap();
        assert_eq!(bytes, j.encode().unwrap());
    }
    #[test]
    fn identity_binds_complete_participant_set() {
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(begin(1)).unwrap();
        let mut changed = participant(1);
        changed.epoch = 2;
        assert_eq!(
            j.apply(Command::Prepared {
                id: [1; 16],
                participant: changed.clone()
            }),
            Err(Error::UnknownParticipant)
        );
        assert_eq!(
            j.apply(Command::Begin {
                id: [1; 16],
                participants: vec![changed]
            }),
            Err(Error::IdentityConflict)
        );
        assert_eq!(
            j.apply(Command::Begin {
                id: [2; 16],
                participants: vec![participant(2), participant(1)]
            }),
            Err(Error::InvalidParticipants)
        );
    }
    #[test]
    fn restore_replays_and_rejects_rewrite_or_rollback() {
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(begin(1)).unwrap();
        let base = j.clone();
        j.apply(decide(1, Decision::Abort)).unwrap();
        base.validate_successor(&j).unwrap();
        assert_eq!(j.validate_successor(&base), Err(Error::IncompatibleHistory));
        let forged = Encoding {
            version: 1,
            authority: [9; 16],
            events: vec![begin(1), decide(1, Decision::Commit)],
        };
        assert_eq!(
            Journal::decode(&serde_json::to_vec(&forged).unwrap()),
            Err(Error::NotPrepared)
        );
        let duplicate = Encoding {
            version: 1,
            authority: [9; 16],
            events: vec![begin(1), begin(1)],
        };
        assert_eq!(
            Journal::decode(&serde_json::to_vec(&duplicate).unwrap()),
            Err(Error::InvalidJournal)
        );
    }
}

#[cfg(test)]
mod cohort_tests {
    use super::*;
    fn binding() -> Cohort {
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
    #[test]
    fn immutable_cohort_canonical_codec_and_begin_admission() {
        let mut j = Journal::new([9; 16]).unwrap();
        let bind = Command::BindCohort { cohort: binding() };
        j.apply(bind.clone()).unwrap();
        let encoded = j.encode().unwrap();
        let nine = format!("[{}]", ["9"; 16].join(","));
        let seven = format!("[{}]", ["7"; 16].join(","));
        let golden = format!(
            r#"{{"version":2,"authority":{nine},"events":[{{"BindCohort":{{"cohort":{{"coordinator_shard":9,"coordinator_epoch":1,"members":[{{"shard":7,"epoch":1,"mode":{seven}}}]}}}}}}]}}"#
        );
        assert_eq!(std::str::from_utf8(&encoded).unwrap(), golden);
        assert_eq!(Journal::decode(&encoded).unwrap(), j);
        j.apply(bind).unwrap();
        assert_eq!(j.encode().unwrap(), encoded);
        let mut wrong_version: serde_json::Value = serde_json::from_slice(&encoded).unwrap();
        wrong_version["version"] = 1.into();
        assert_eq!(
            Journal::decode(&serde_json::to_vec(&wrong_version).unwrap()),
            Err(Error::InvalidJournal)
        );
        for (shard, epoch, mode) in [(8, 1, [7; 16]), (7, 2, [7; 16]), (7, 1, [8; 16])] {
            assert_eq!(
                j.apply(Command::Begin {
                    id: [1; 16],
                    participants: vec![Participant {
                        shard,
                        epoch,
                        mode,
                        intent: [1; 32]
                    }]
                }),
                Err(Error::UnknownParticipant)
            );
            assert_eq!(j.encode().unwrap(), encoded);
        }
        j.apply(Command::Begin {
            id: [1; 16],
            participants: vec![Participant {
                shard: 7,
                epoch: 1,
                mode: [7; 16],
                intent: [1; 32],
            }],
        })
        .unwrap();
        j.apply(Command::Decide {
            id: [1; 16],
            decision: Decision::Abort,
        })
        .unwrap();
        assert_eq!(Journal::decode(&j.encode().unwrap()).unwrap(), j);
        let mut unbound = Journal::new([9; 16]).unwrap();
        unbound
            .apply(Command::Begin {
                id: [1; 16],
                participants: vec![Participant {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                    intent: [1; 32],
                }],
            })
            .unwrap();
        assert_eq!(
            unbound.apply(Command::BindCohort { cohort: binding() }),
            Err(Error::IncompatibleHistory)
        );
        let mut changed = binding();
        changed.coordinator_epoch = 2;
        assert_eq!(
            j.apply(Command::BindCohort { cohort: changed }),
            Err(Error::IdentityConflict)
        );
        assert_eq!(j.reserved_size().unwrap(), j.encode().unwrap().len());
    }
    #[test]
    fn cohort_members_cannot_be_empty_unsorted_duplicate_or_coordinator() {
        for members in [
            vec![],
            vec![Member {
                shard: 9,
                epoch: 1,
                mode: [9; 16],
            }],
            vec![
                Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16]
                };
                2
            ],
            vec![
                Member {
                    shard: 8,
                    epoch: 1,
                    mode: [8; 16],
                },
                Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                },
            ],
        ] {
            let mut j = Journal::new([9; 16]).unwrap();
            assert_eq!(
                j.apply(Command::BindCohort {
                    cohort: Cohort {
                        members,
                        ..binding()
                    }
                }),
                Err(Error::InvalidParticipants)
            );
            assert!(j.cohort().is_none());
        }
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;
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
    fn journal() -> Journal {
        let mut j = Journal::new([9; 16]).unwrap();
        j.apply(Command::BindCohort { cohort: cohort() }).unwrap();
        j
    }
    fn descriptor() -> Participant {
        Participant {
            shard: 7,
            epoch: 1,
            mode: [7; 16],
            intent: [3; 32],
        }
    }
    fn admit(id: u8, tx: u8) -> Command {
        let p = descriptor();
        let payload = crate::request_plans::Plan {
            version: 1,
            id: [tx; 16],
            authority: crate::participant_writes::Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: cohort(),
            validation_version: 0,
            parts: vec![crate::request_plans::Part {
                descriptor: p.clone(),
                request: crate::D1WriteRequest {
                    shard: 7,
                    epoch: 1,
                    request_id: [tx; 16],
                    expected_sequence: Some(0),
                    operations: vec![crate::WriteOp::put("x", "value")],
                },
            }],
        }
        .encode()
        .unwrap();
        Command::AdmitRequest {
            id: [id; 16],
            request: b"typed-request-v1".to_vec(),
            payload,
            transaction_id: [tx; 16],
            participants: vec![p],
        }
    }
    #[test]
    fn duplicate_request_returns_first_plan_without_a_second_begin() {
        let mut j = journal();
        j.apply(admit(1, 2)).unwrap();
        let original = j.encode().unwrap();
        assert_eq!(
            original,
            include_bytes!("../tests/fixtures/request-journal-v3.json")
        );
        assert_eq!(
            j.request([1; 16]).unwrap().payload,
            include_bytes!("../tests/fixtures/request-plan-v1.json")
        );
        j.apply(admit(1, 3)).unwrap();
        assert_eq!(j.encode().unwrap(), original);
        assert_eq!(j.transaction_count(), 1);
        assert!(j.transaction([3; 16]).is_none());
        assert_eq!(j.request([1; 16]).unwrap().transaction_id, [2; 16]);
        let mut different = admit(1, 3);
        if let Command::AdmitRequest { request, .. } = &mut different {
            request.push(1);
        }
        assert_eq!(j.apply(different), Err(Error::IdentityConflict));
        assert_eq!(j.encode().unwrap(), original);
        assert_eq!(j.apply(admit(4, 2)), Err(Error::IdentityConflict));
        assert_eq!(Journal::decode(&original).unwrap(), j);
        let mut encoding: Encoding = serde_json::from_slice(&original).unwrap();
        assert_eq!(encoding.version, 3);
        encoding.events.push(admit(1, 3));
        assert_eq!(
            Journal::decode(&serde_json::to_vec(&encoding).unwrap()),
            Err(Error::InvalidJournal)
        );
        encoding.events.pop();
        encoding.version = 2;
        assert_eq!(
            Journal::decode(&serde_json::to_vec(&encoding).unwrap()),
            Err(Error::InvalidJournal)
        );
    }
    #[test]
    fn admitted_plan_binds_all_execution_identity_and_reserves_resolution() {
        let original = journal();
        let command = admit(1, 2);
        let mut admitted = original.clone();
        admitted.apply(command.clone()).unwrap();
        let exact = admitted.reserved_size().unwrap();
        let mut candidate = original.clone();
        assert!(candidate.append(command.clone(), exact).unwrap());
        let mut candidate = original.clone();
        assert_eq!(
            candidate.append(command.clone(), exact - 1),
            Err(Error::Capacity)
        );
        admitted
            .append(
                Command::Prepared {
                    id: [2; 16],
                    participant: descriptor(),
                },
                exact,
            )
            .unwrap();
        admitted
            .append(
                Command::Decide {
                    id: [2; 16],
                    decision: Decision::Commit,
                },
                exact,
            )
            .unwrap();
        assert!(admitted.encode().unwrap().len() <= exact);
        assert_eq!(
            Journal::decode(&admitted.encode().unwrap()).unwrap(),
            admitted
        );
        for field in ["id", "mode", "cas", "version", "duplicate", "authority"] {
            let mut bad = command.clone();
            if let Command::AdmitRequest { payload, .. } = &mut bad {
                let mut plan: serde_json::Value = serde_json::from_slice(payload).unwrap();
                match field {
                    "id" => plan["parts"][0]["request"]["request_id"][0] = 99.into(),
                    "mode" => plan["parts"][0]["descriptor"]["mode"][0] = 99.into(),
                    "cas" => {
                        plan["parts"][0]["request"]["expected_sequence"] = serde_json::Value::Null
                    }
                    "version" => plan["validation_version"] = 99.into(),
                    "duplicate" => {
                        let part = plan["parts"][0].clone();
                        plan["parts"].as_array_mut().unwrap().push(part);
                    }
                    _ => plan["authority"]["mode"][0] = 99.into(),
                }
                *payload = serde_json::to_vec(&plan).unwrap();
            }
            let mut j = original.clone();
            assert!(j.apply(bad).is_err(), "{field}");
            assert_eq!(j, original);
        }
        let mut unbound = Journal::new([9; 16]).unwrap();
        assert!(unbound.apply(command).is_err());
    }
    #[test]
    fn request_capacity_is_bounded_but_existing_identity_remains_retryable() {
        let mut j = journal();
        for i in 0..MAX_REQUESTS {
            let mut command = admit(1, 2);
            let id = (i as u128 + 1).to_le_bytes();
            let tx = (i as u128 + 1000).to_le_bytes();
            if let Command::AdmitRequest {
                id: external,
                transaction_id,
                payload,
                ..
            } = &mut command
            {
                *external = id;
                *transaction_id = tx;
                let mut p = crate::request_plans::Plan::decode(payload).unwrap();
                p.id = tx;
                p.parts[0].request.request_id = tx;
                *payload = p.encode().unwrap();
            }
            j.apply(command).unwrap();
        }
        let original = j.clone();
        assert_eq!(j.apply(admit(255, 9)), Err(Error::Capacity));
        assert_eq!(j, original);
        // Existing request ID=1 reuses the stored plan even with another proposed plan.
        let mut retry = admit(1, 2);
        if let Command::AdmitRequest { id, .. } = &mut retry {
            *id = 1u128.to_le_bytes();
        }
        j.apply(retry).unwrap();
        assert_eq!(j, original);
    }
}
