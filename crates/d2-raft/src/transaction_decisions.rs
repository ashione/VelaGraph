//! Deterministic coordinator decision journal, persisted by the dedicated D2 role.
//! Data-shard participant preparation and global visibility are not implemented.
//!
//! A resolved decision prefix is NOT a readable data version. Prepared evidence
//! must eventually come from a fenced participant protocol, not an arbitrary caller.
use serde::{Deserialize, Serialize};

pub const MAX_TRANSACTIONS: usize = 1024;
pub const MAX_PARTICIPANTS: usize = 64;
pub const MAX_JOURNAL_BYTES: usize = 16 * 1024 * 1024;

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
    pub members: Vec<Member>,
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
    BindCohort {
        cohort: Cohort,
    },
    Begin {
        id: [u8; 16],
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
    InvalidJournal,
    IncompatibleHistory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    pub id: [u8; 16],
    pub ordinal: u64,
    pub participants: Vec<Participant>,
    pub prepared: Vec<bool>,
    pub decision: Option<Decision>,
}

/// Immutable append-only history. The private state cannot be deserialized
/// without replaying every transition. No timeout implicitly aborts a transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Encoding", into = "Encoding")]
pub struct Journal {
    authority: [u8; 16],
    events: Vec<Command>,
    transactions: Vec<Transaction>,
    reserved_bytes: usize,
    cohort: Option<Cohort>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Encoding {
    version: u32,
    authority: [u8; 16],
    events: Vec<Command>,
}

impl From<Journal> for Encoding {
    fn from(journal: Journal) -> Self {
        Self {
            version: if journal.cohort.is_some() { 2 } else { 1 },
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
        if authority == [0; 16] {
            return Err(Error::InvalidIdentity);
        }
        let mut journal = Self {
            authority,
            events: Vec::new(),
            transactions: Vec::new(),
            reserved_bytes: 0,
            cohort: None,
        };
        journal.reserved_bytes = journal.encode()?.len();
        Ok(journal)
    }

    pub fn cohort(&self) -> Option<&Cohort> {
        self.cohort.as_ref()
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
        serde_json::to_vec(command)
            .map(|bytes| bytes.len())
            .map_err(|_| Error::InvalidJournal)
    }

    fn pending_bytes(tx: &Transaction) -> Result<usize, Error> {
        let mut size = Self::event_bytes(&Command::Decide {
            id: tx.id,
            decision: Decision::Commit,
        })? + 1;
        for (participant, prepared) in tx.participants.iter().zip(&tx.prepared) {
            if !prepared {
                size += Self::event_bytes(&Command::Prepared {
                    id: tx.id,
                    participant: participant.clone(),
                })? + 1;
            }
        }
        Ok(size)
    }

    // Used on an unpublished candidate or during recovery; every prefix obeys
    // the exact same admission rule. Replay work is linear in bounded events
    // times the bounded participant/transaction lookup sizes, not journal bytes.
    fn append(&mut self, command: Command, limit: usize) -> Result<bool, Error> {
        let released = match &command {
            Command::BindCohort { .. } | Command::Begin { .. } => 0,
            Command::Prepared { .. } => Self::event_bytes(&command)? + 1,
            Command::Decide { id, .. } => self
                .transaction(*id)
                .map(Self::pending_bytes)
                .transpose()?
                .unwrap_or(0),
        };
        if !self.transition(&command)? {
            return Ok(false);
        }
        let future = match &command {
            Command::Begin { id, .. } => Self::pending_bytes(self.transaction(*id).unwrap())?,
            _ => 0,
        };
        let size = self.reserved_bytes
            + Self::event_bytes(&command)?
            + usize::from(!self.events.is_empty())
            + future;
        self.reserved_bytes = size.checked_sub(released).ok_or(Error::InvalidJournal)?;
        if self.reserved_bytes > limit {
            return Err(Error::Capacity);
        }
        self.events.push(command);
        Ok(true)
    }

    fn transition(&mut self, command: &Command) -> Result<bool, Error> {
        match command {
            Command::BindCohort { cohort } => {
                cohort.validate()?;
                if let Some(existing) = &self.cohort {
                    return if existing == cohort {
                        Ok(false)
                    } else {
                        Err(Error::IdentityConflict)
                    };
                }
                if !self.events.is_empty() {
                    return Err(Error::IncompatibleHistory);
                }
                self.cohort = Some(cohort.clone());
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
                if self.transactions.len() >= MAX_TRANSACTIONS {
                    return Err(Error::Capacity);
                }
                self.transactions.push(Transaction {
                    id: *id,
                    ordinal: self.transactions.len() as u64 + 1,
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
                let index = tx
                    .participants
                    .iter()
                    .position(|p| p == participant)
                    .ok_or(Error::UnknownParticipant)?;
                if tx.prepared[index] {
                    return Ok(false);
                }
                if tx.decision.is_some() {
                    return Err(Error::AlreadyDecided);
                }
                tx.prepared[index] = true;
            }
            Command::Decide { id, decision } => {
                let tx = self
                    .transactions
                    .iter_mut()
                    .find(|t| t.id == *id)
                    .ok_or(Error::UnknownTransaction)?;
                if let Some(current) = tx.decision {
                    return if current == *decision {
                        Ok(false)
                    } else {
                        Err(Error::AlreadyDecided)
                    };
                }
                if *decision == Decision::Commit && !tx.prepared.iter().all(|p| *p) {
                    return Err(Error::NotPrepared);
                }
                tx.decision = Some(*decision);
            }
        }
        Ok(true)
    }

    #[cfg(test)]
    fn reserved_size(&self) -> Result<usize, Error> {
        Ok(self.reserved_bytes)
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        serde_json::to_vec(&Encoding {
            version: if self.cohort.is_some() { 2 } else { 1 },
            authority: self.authority,
            events: self.events.clone(),
        })
        .map_err(|_| Error::InvalidJournal)
    }

    /// Strict structural and semantic replay; serialized derived watermarks are
    /// intentionally absent. Bounded by input bytes and finite unique events.
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        Self::decode_with_limit(bytes, MAX_JOURNAL_BYTES)
    }

    fn decode_with_limit(bytes: &[u8], limit: usize) -> Result<Self, Error> {
        if bytes.len() > limit {
            return Err(Error::Capacity);
        }
        let encoded: Encoding = serde_json::from_slice(bytes).map_err(|_| Error::InvalidJournal)?;
        if !matches!(encoded.version, 1 | 2)
            || encoded.events.len() > MAX_TRANSACTIONS * (MAX_PARTICIPANTS + 2) + 1
        {
            return Err(Error::InvalidJournal);
        }
        let mut result = Self::new(encoded.authority)?;
        for command in encoded.events {
            if !result.append(command, limit)? {
                return Err(Error::InvalidJournal);
            }
        }
        if encoded.version != if result.cohort.is_some() { 2 } else { 1 } {
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
