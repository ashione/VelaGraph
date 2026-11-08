//! Consumed active coordinator state. Historical identity lookups come only from
//! the validated original-command archive (plus the current batch's suffix).
use super::*;
use crate::capacity_policy::BYTE_MAX_TRANSACTIONS;

pub(crate) const MAX_ACTIVE_CHECKPOINT_BYTES: usize =
    (4 * MAX_JOURNAL_BYTES).div_ceil(3) + 64 * 1024;

/// Shared contiguous-front reduction for compact apply and validated-history
/// projections. A terminal beyond an earlier hole never advances the prefix.
pub(crate) fn next_resolved(resolved: u64, tx: &Transaction) -> Result<Option<u64>, Error> {
    let next = resolved.checked_add(1).ok_or(Error::Capacity)?;
    if tx.ordinal != next {
        return Err(Error::InvalidJournal);
    }
    Ok(tx.decision.is_some().then_some(next))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LookupKey {
    Request([u8; 16]),
    Transaction([u8; 16]),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LookupResult {
    Request(Option<RequestRecord>),
    Transaction(Option<Transaction>),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AppliedTransition {
    /// Exactly the accepted original event, never a retry or synthetic event.
    pub(crate) accepted: Option<Command>,
    /// Newly contiguous terminal records in absolute ordinal order.
    pub(crate) drained: Vec<Transaction>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Header {
    pub(super) authority: [u8; 16],
    pub(super) cohort: Option<Box<Cohort>>,
    #[serde(deserialize_with = "deserialize_optional_grants")]
    pub(super) capacity_grants: Option<Vec<MemberCapacityGrant>>,
    events: u64,
    transactions: u64,
    requests: u64,
    resolved: u64,
    encoding_version: u32,
    pub(super) reserved_bytes: usize,
}
fn deserialize_optional_grants<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<Option<Vec<MemberCapacityGrant>>, D::Error> {
    #[derive(Deserialize)]
    struct Bounded(#[serde(deserialize_with = "deserialize_grants")] Vec<MemberCapacityGrant>);
    Ok(Option::<Bounded>::deserialize(d)?.map(|b| b.0))
}
fn deserialize_active<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Transaction>, D::Error> {
    struct Active;
    impl<'de> serde::de::Visitor<'de> for Active {
        type Value = Vec<Transaction>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("count- and byte-bounded active transactions")
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut seq: A,
        ) -> Result<Self::Value, A::Error> {
            if seq
                .size_hint()
                .is_some_and(|count| count > BYTE_MAX_TRANSACTIONS)
            {
                return Err(serde::de::Error::custom("too many retained items"));
            }
            let mut active = Vec::new();
            let mut bytes = 2usize; // The active array's brackets.
            while active.len() < BYTE_MAX_TRANSACTIONS {
                let Some(tx) = seq.next_element::<Transaction>()? else {
                    return Ok(active);
                };
                // One decoded record is independently bounded by 64 participants.
                // Check its canonical bytes before retaining it. The complete
                // header/grant envelope is checked by validate after decoding.
                let separator = usize::from(!active.is_empty());
                let remaining = MAX_ACTIVE_CHECKPOINT_BYTES
                    .checked_sub(bytes)
                    .and_then(|n| n.checked_sub(separator))
                    .ok_or_else(|| {
                        serde::de::Error::custom("active transaction byte budget exceeded")
                    })?;
                let size = canonical_size(&tx, remaining).map_err(|_| {
                    serde::de::Error::custom("active transaction byte budget exceeded")
                })?;
                bytes = bytes
                    .checked_add(separator)
                    .and_then(|n| n.checked_add(size))
                    .ok_or_else(|| {
                        serde::de::Error::custom("active transaction byte budget exceeded")
                    })?;
                active.push(tx);
            }
            seq.next_element_seed(crate::capacity_policy::RejectExtra)?;
            Ok(active)
        }
    }
    d.deserialize_seq(Active)
}
impl Header {
    pub(super) fn from_journal(j: &Journal) -> Result<Self, Error> {
        Ok(Self {
            authority: j.authority,
            cohort: j.cohort.clone(),
            capacity_grants: j.capacity_grants.clone(),
            events: u64::try_from(j.events.len()).map_err(|_| Error::Capacity)?,
            transactions: u64::try_from(j.transactions.len()).map_err(|_| Error::Capacity)?,
            requests: u64::try_from(j.requests.len()).map_err(|_| Error::Capacity)?,
            resolved: j.resolved_prefix(),
            encoding_version: j.encoding_version(),
            reserved_bytes: j.reserved_bytes,
        })
    }
    fn profile(&self) -> CapacityProfile {
        self.cohort
            .as_ref()
            .map_or(CapacityProfile::LegacyCountV1, |c| c.profile)
    }
    fn ready(&self) -> bool {
        self.profile().is_legacy() || self.capacity_grants.is_some()
    }
    fn version(&self) -> u32 {
        if !self.profile().is_legacy() {
            4
        } else if self.requests != 0 {
            3
        } else if self.cohort.is_some() {
            2
        } else {
            1
        }
    }
    fn envelope_bytes(&self) -> Result<usize, Error> {
        canonical_size(
            &Encoding {
                version: self.version(),
                authority: self.authority,
                events: Vec::new(),
            },
            MAX_JOURNAL_BYTES,
        )
    }
}

/// No events, requests, plans or resolved terminal archive is resident here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ActiveCheckpoint {
    version: u32,
    header: Header,
    history_bytes: usize,
    pending_bytes: usize,
    #[serde(deserialize_with = "deserialize_active")]
    active: Vec<Transaction>,
}

fn event_bytes(command: &Command) -> Result<usize, Error> {
    canonical_size(command, MAX_REQUEST_EVENT_BYTES)
}

pub(super) fn pending_bytes(tx: &Transaction) -> Result<usize, Error> {
    let mut size = event_bytes(&Command::Decide {
        id: tx.id,
        decision: Decision::Commit,
    })?
    .checked_add(1)
    .ok_or(Error::Capacity)?;
    for (participant, prepared) in tx.participants.iter().zip(&tx.prepared) {
        if !prepared {
            size = size
                .checked_add(event_bytes(&Command::Prepared {
                    id: tx.id,
                    participant: participant.clone(),
                })?)
                .and_then(|n| n.checked_add(1))
                .ok_or(Error::Capacity)?;
        }
    }
    Ok(size)
}

pub(super) struct Change {
    pub(super) header: Header,
    pub(super) transaction: Option<Transaction>,
    pub(super) request: Option<RequestRecord>,
    history_added: usize,
    released: usize,
    future: usize,
}
fn transaction(
    lookup: &mut impl FnMut(LookupKey) -> Result<LookupResult, Error>,
    id: [u8; 16],
) -> Result<Option<Transaction>, Error> {
    match lookup(LookupKey::Transaction(id))? {
        LookupResult::Transaction(tx) => Ok(tx),
        _ => Err(Error::InvalidJournal),
    }
}
fn request(
    lookup: &mut impl FnMut(LookupKey) -> Result<LookupResult, Error>,
    id: [u8; 16],
) -> Result<Option<RequestRecord>, Error> {
    match lookup(LookupKey::Request(id))? {
        LookupResult::Request(record) => Ok(record),
        _ => Err(Error::InvalidJournal),
    }
}
fn begin(
    header: &Header,
    id: [u8; 16],
    participants: &[Participant],
    lookup: &mut impl FnMut(LookupKey) -> Result<LookupResult, Error>,
) -> Result<Option<Transaction>, Error> {
    if id == [0; 16] {
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
    if header
        .cohort
        .as_ref()
        .is_some_and(|c| participants.iter().any(|p| !c.contains(p)))
    {
        return Err(Error::UnknownParticipant);
    }
    if let Some(tx) = transaction(lookup, id)? {
        return if tx.participants == participants {
            Ok(None)
        } else {
            Err(Error::IdentityConflict)
        };
    }
    if !header.ready() {
        return Err(Error::CapacityNotConfirmed);
    }
    if header.transactions >= header.profile().max_transactions() as u64 {
        return Err(Error::Capacity);
    }
    Ok(Some(Transaction {
        id,
        ordinal: header.transactions.checked_add(1).ok_or(Error::Capacity)?,
        participants: participants.to_vec(),
        prepared: vec![false; participants.len()],
        decision: None,
    }))
}

/// The one deterministic command grammar, including old precedence and exact
/// reservation arithmetic. It stages only a target transaction and metadata.
/// Neither caller is modified on rejection or retry.
pub(super) fn prepare(
    header: &Header,
    command: &Command,
    limit: usize,
    lookup: &mut impl FnMut(LookupKey) -> Result<LookupResult, Error>,
) -> Result<Option<Change>, Error> {
    // Preserve original pre-transition sizing/lookup precedence.
    let released = match command {
        Command::Prepared { .. } => event_bytes(command)?
            .checked_add(1)
            .ok_or(Error::Capacity)?,
        Command::Decide { id, .. } => transaction(lookup, *id)?
            .as_ref()
            .map(pending_bytes)
            .transpose()?
            .unwrap_or(0),
        _ => 0,
    };
    let old_envelope = header.envelope_bytes()?;
    let mut next = header.clone();
    let mut tx = None;
    let mut record = None;
    match command {
        Command::AdmitRequest {
            id,
            request: bytes,
            payload,
            transaction_id,
            participants,
        } => {
            if *id == [0; 16]
                || bytes.is_empty()
                || payload.is_empty()
                || bytes.len() > MAX_REQUEST_BYTES
                || payload.len() > MAX_REQUEST_PAYLOAD_BYTES
                || header.cohort.is_none()
            {
                return Err(Error::InvalidIdentity);
            }
            if let Some(existing) = request(lookup, *id)? {
                return if existing.request == *bytes {
                    Ok(None)
                } else {
                    Err(Error::IdentityConflict)
                };
            }
            if !header.ready() {
                return Err(Error::CapacityNotConfirmed);
            }
            let plan = crate::request_plans::Plan::decode(payload)?;
            if plan.id != *transaction_id
                || plan.authority.mode != header.authority
                || Some(&plan.cohort) != header.cohort.as_deref()
                || plan.validation_version > header.resolved
                || plan
                    .parts
                    .iter()
                    .map(|p| p.descriptor.clone())
                    .collect::<Vec<_>>()
                    != *participants
            {
                return Err(Error::IdentityConflict);
            }
            if header.requests >= header.profile().max_requests() as u64 {
                return Err(Error::Capacity);
            }
            if transaction(lookup, *transaction_id)?.is_some() {
                return Err(Error::IdentityConflict);
            }
            tx = begin(header, *transaction_id, participants, lookup)?;
            record = Some(RequestRecord {
                id: *id,
                request: bytes.clone(),
                payload: payload.clone(),
                transaction_id: *transaction_id,
            });
            next.requests = next.requests.checked_add(1).ok_or(Error::Capacity)?;
            next.transactions = next.transactions.checked_add(1).ok_or(Error::Capacity)?;
        }
        Command::BindCohort { cohort } => {
            cohort.validate()?;
            if let Some(existing) = header.cohort.as_deref() {
                return if existing == cohort {
                    Ok(None)
                } else {
                    Err(Error::IdentityConflict)
                };
            }
            if header.events != 0 {
                return Err(Error::IncompatibleHistory);
            }
            next.cohort = Some(Box::new(cohort.clone()));
        }
        Command::ConfirmCohortCapacity { cohort, grants } => {
            if header.cohort.as_deref() != Some(cohort) {
                return Err(Error::IdentityConflict);
            }
            validate_grants(cohort, grants)?;
            if let Some(existing) = &header.capacity_grants {
                return if existing == grants {
                    Ok(None)
                } else {
                    Err(Error::IdentityConflict)
                };
            }
            if header.transactions != 0 || header.requests != 0 {
                return Err(Error::IncompatibleHistory);
            }
            next.capacity_grants = Some(grants.clone());
        }
        Command::Begin { id, participants } => {
            tx = begin(header, *id, participants, lookup)?;
            if tx.is_none() {
                return Ok(None);
            }
            next.transactions = next.transactions.checked_add(1).ok_or(Error::Capacity)?;
        }
        Command::Prepared { id, participant } => {
            let mut target = transaction(lookup, *id)?.ok_or(Error::UnknownTransaction)?;
            if !target.record_prepared(participant)? {
                return Ok(None);
            }
            tx = Some(target);
        }
        Command::Decide { id, decision } => {
            let mut target = transaction(lookup, *id)?.ok_or(Error::UnknownTransaction)?;
            if !target.record_decision(*decision)? {
                return Ok(None);
            }
            tx = Some(target);
        }
    }
    let event_size = event_bytes(command)?;
    let future = match command {
        Command::Begin { .. } | Command::AdmitRequest { .. } => {
            pending_bytes(tx.as_ref().ok_or(Error::InvalidJournal)?)?
        }
        _ => 0,
    };
    let new_envelope = next.envelope_bytes()?;
    let history_added = event_size
        .checked_add(usize::from(header.events != 0))
        .ok_or(Error::Capacity)?;
    next.reserved_bytes = header
        .reserved_bytes
        .checked_sub(released)
        .and_then(|n| n.checked_sub(old_envelope))
        .and_then(|n| n.checked_add(new_envelope))
        .and_then(|n| n.checked_add(history_added))
        .and_then(|n| n.checked_add(future))
        .ok_or(Error::Capacity)?;
    if header.events >= next.profile().max_history_events() || next.reserved_bytes > limit {
        return Err(Error::Capacity);
    }
    next.events = next.events.checked_add(1).ok_or(Error::Capacity)?;
    next.encoding_version = next.version();
    Ok(Some(Change {
        header: next,
        transaction: tx,
        request: record,
        history_added,
        released,
        future,
    }))
}

impl ActiveCheckpoint {
    /// Explicit cold/migration boundary. Never used by ordinary active apply.
    pub(crate) fn from_journal(journal: &Journal) -> Result<Self, Error> {
        #[cfg(test)]
        assert_full_journal_allowed();
        let header = Header::from_journal(journal)?;
        let start = usize::try_from(header.resolved).map_err(|_| Error::Capacity)?;
        let active = journal
            .transactions
            .get(start..)
            .ok_or(Error::InvalidJournal)?
            .to_vec();
        let pending_bytes = active
            .iter()
            .filter(|tx| tx.decision.is_none())
            .try_fold(0usize, |n, tx| {
                n.checked_add(pending_bytes(tx)?).ok_or(Error::Capacity)
            })?;
        // This is an explicit cold/full boundary: independently measure the
        // actual original versioned codec, rather than infer history bytes only
        // from its cached reservation. Ordinary active apply never calls here.
        let history_bytes = journal.encode()?.len();
        if history_bytes.checked_add(pending_bytes) != Some(header.reserved_bytes) {
            return Err(Error::InvalidJournal);
        }
        let checkpoint = Self {
            version: 1,
            header,
            history_bytes,
            pending_bytes,
            active,
        };
        checkpoint.validate()?;
        Ok(checkpoint)
    }

    pub(crate) fn authority(&self) -> [u8; 16] {
        self.header.authority
    }
    pub(crate) fn cohort(&self) -> Option<&Cohort> {
        self.header.cohort.as_deref()
    }
    pub(crate) fn capacity_profile(&self) -> CapacityProfile {
        self.header.profile()
    }
    pub(crate) fn transaction_count(&self) -> usize {
        // validate bounds this by the profile's usize count ceiling.
        self.header.transactions as usize
    }
    pub(crate) fn capacity_grants(&self) -> Option<&[MemberCapacityGrant]> {
        self.header.capacity_grants.as_deref()
    }
    #[cfg(test)]
    pub(crate) fn request_count(&self) -> u64 {
        self.header.requests
    }
    pub(crate) fn event_count(&self) -> u64 {
        self.header.events
    }
    pub(crate) fn resolved_prefix(&self) -> u64 {
        self.header.resolved
    }
    #[cfg(test)]
    pub(crate) fn reserved_bytes(&self) -> usize {
        self.header.reserved_bytes
    }
    #[cfg(test)]
    pub(crate) fn history_bytes(&self) -> usize {
        self.history_bytes
    }
    #[cfg(test)]
    pub(crate) fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }
    pub(crate) fn active_transactions(&self) -> &[Transaction] {
        &self.active
    }

    /// Structural closure only. Cold recovery additionally replays the sole
    /// historical authority and compares the complete recomputed checkpoint.
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let h = &self.header;
        if self.version != 1
            || h.authority == [0; 16]
            || h.encoding_version != h.version()
            || h.events > h.profile().max_history_events()
            || h.transactions > h.profile().max_transactions() as u64
            || h.requests > h.profile().max_requests() as u64
            || h.requests > h.transactions
            || h.resolved > h.transactions
            || h.transactions > h.events
            || h.reserved_bytes > MAX_JOURNAL_BYTES
            || self.history_bytes < h.envelope_bytes()?
            || self.history_bytes.checked_add(self.pending_bytes) != Some(h.reserved_bytes)
            || u64::try_from(self.active.len()).map_err(|_| Error::Capacity)?
                != h.transactions - h.resolved
            || self.active.first().is_some_and(|tx| tx.decision.is_some())
        {
            return Err(Error::InvalidJournal);
        }
        if let Some(cohort) = &h.cohort {
            cohort.validate()?;
        }
        if let Some(grants) = &h.capacity_grants {
            validate_grants(h.cohort.as_deref().ok_or(Error::InvalidJournal)?, grants)?;
        }
        if (h.requests != 0 && h.cohort.is_none()) || (h.transactions != 0 && !h.ready()) {
            return Err(Error::InvalidJournal);
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut pending = 0usize;
        for (index, tx) in self.active.iter().enumerate() {
            let ordinal = h
                .resolved
                .checked_add(u64::try_from(index).map_err(|_| Error::Capacity)?)
                .and_then(|n| n.checked_add(1))
                .ok_or(Error::Capacity)?;
            if tx.ordinal != ordinal
                || tx.id == [0; 16]
                || !ids.insert(tx.id)
                || tx.participants.is_empty()
                || tx.participants.len() > MAX_PARTICIPANTS
                || tx.prepared.len() != tx.participants.len()
                || tx
                    .participants
                    .iter()
                    .any(|p| p.mode == [0; 16] || p.intent == [0; 32])
                || tx.participants.windows(2).any(|p| p[0].shard >= p[1].shard)
                || h.cohort
                    .as_ref()
                    .is_some_and(|c| tx.participants.iter().any(|p| !c.contains(p)))
                || (tx.decision == Some(Decision::Commit) && !tx.prepared.iter().all(|p| *p))
            {
                return Err(Error::InvalidJournal);
            }
            if tx.decision.is_none() {
                pending = pending
                    .checked_add(pending_bytes(tx)?)
                    .ok_or(Error::Capacity)?;
            }
        }
        if pending != self.pending_bytes {
            return Err(Error::InvalidJournal);
        }
        canonical_size(self, MAX_ACTIVE_CHECKPOINT_BYTES)?;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| Error::InvalidJournal)
    }
    #[cfg(test)]
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_ACTIVE_CHECKPOINT_BYTES {
            return Err(Error::Capacity);
        }
        let result: Self = serde_json::from_slice(bytes).map_err(|_| Error::InvalidJournal)?;
        result.validate()?;
        if serde_json::to_vec(&result).map_err(|_| Error::InvalidJournal)? != bytes {
            return Err(Error::InvalidJournal);
        }
        Ok(result)
    }

    /// Archive lookups must belong to the exact validated published cut and
    /// current batch overlay. Exact retries never append, charge, or drain.
    pub(crate) fn apply(
        &mut self,
        command: Command,
        lookup: &mut impl FnMut(LookupKey) -> Result<LookupResult, Error>,
    ) -> Result<AppliedTransition, Error> {
        let change = prepare(&self.header, &command, MAX_JOURNAL_BYTES, &mut |key| {
            if let LookupKey::Transaction(id) = key {
                if let Some(tx) = self.active.iter().find(|tx| tx.id == id) {
                    return Ok(LookupResult::Transaction(Some(tx.clone())));
                }
            }
            lookup(key)
        })?;
        let Some(change) = change else {
            return Ok(AppliedTransition {
                accepted: None,
                drained: Vec::new(),
            });
        };
        let mut candidate = self.clone();
        let new_envelope = change.header.envelope_bytes()?;
        candidate.history_bytes = self
            .history_bytes
            .checked_sub(self.header.envelope_bytes()?)
            .and_then(|n| n.checked_add(new_envelope))
            .and_then(|n| n.checked_add(change.history_added))
            .ok_or(Error::Capacity)?;
        candidate.pending_bytes = self
            .pending_bytes
            .checked_sub(change.released)
            .and_then(|n| n.checked_add(change.future))
            .ok_or(Error::Capacity)?;
        candidate.header = change.header;
        if let Some(tx) = change.transaction {
            let index = tx
                .ordinal
                .checked_sub(candidate.header.resolved)
                .and_then(|n| n.checked_sub(1))
                .and_then(|n| usize::try_from(n).ok())
                .ok_or(Error::InvalidJournal)?;
            if index == candidate.active.len() {
                candidate.active.push(tx);
            } else {
                *candidate
                    .active
                    .get_mut(index)
                    .ok_or(Error::InvalidJournal)? = tx;
            }
        }
        let mut count = 0;
        for tx in &candidate.active {
            let Some(next) = next_resolved(candidate.header.resolved, tx)? else {
                break;
            };
            candidate.header.resolved = next;
            count += 1;
        }
        let drained = candidate.active.drain(..count).collect();
        candidate.validate()?;
        *self = candidate;
        Ok(AppliedTransition {
            accepted: Some(command),
            drained,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn participant() -> Participant {
        Participant {
            shard: 1,
            epoch: 1,
            mode: [1; 16],
            intent: [2; 32],
        }
    }
    fn cohort(profile: CapacityProfile) -> Cohort {
        Cohort {
            coordinator_shard: 9,
            coordinator_epoch: 1,
            members: vec![Member {
                shard: 1,
                epoch: 1,
                mode: [1; 16],
            }],
            profile,
        }
    }
    fn grants(cohort: &Cohort) -> Vec<MemberCapacityGrant> {
        cohort
            .members
            .iter()
            .map(|member| MemberCapacityGrant {
                member: member.clone(),
                activation: CapacityActivation {
                    nonce: [1; 32],
                    log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
                },
            })
            .collect()
    }
    fn lookup(journal: &Journal, key: LookupKey) -> Result<LookupResult, Error> {
        Ok(match key {
            LookupKey::Request(id) => LookupResult::Request(journal.request(id).cloned()),
            LookupKey::Transaction(id) => {
                LookupResult::Transaction(journal.transaction(id).cloned())
            }
        })
    }
    fn begin(id: u128) -> Command {
        Command::Begin {
            id: id.to_le_bytes(),
            participants: vec![participant()],
        }
    }
    fn decide(id: u128, decision: Decision) -> Command {
        Command::Decide {
            id: id.to_le_bytes(),
            decision,
        }
    }
    fn prepared(id: u128) -> Command {
        Command::Prepared {
            id: id.to_le_bytes(),
            participant: participant(),
        }
    }
    fn admission(binding: &Cohort, id: u128, value: Vec<u8>) -> Command {
        let id = id.to_le_bytes();
        let plan = crate::request_plans::Plan {
            version: 1,
            id,
            authority: crate::participant_writes::Authority {
                shard: 9,
                mode: [9; 16],
            },
            cohort: binding.clone(),
            validation_version: 0,
            parts: vec![crate::request_plans::Part {
                descriptor: participant(),
                request: crate::D1WriteRequest {
                    shard: 1,
                    epoch: 1,
                    request_id: id,
                    expected_sequence: Some(0),
                    operations: vec![crate::WriteOp::put(vec![0], value)],
                },
            }],
        };
        Command::AdmitRequest {
            id,
            request: vec![1],
            payload: plan.encode().unwrap(),
            transaction_id: id,
            participants: vec![participant()],
        }
    }
    fn step(
        journal: &mut Journal,
        checkpoint: &mut ActiveCheckpoint,
        command: Command,
    ) -> Result<AppliedTransition, Error> {
        let before = checkpoint.clone();
        let old_events = journal.events.len();
        let result = without_full_journal(|| {
            checkpoint.apply(command.clone(), &mut |key| lookup(journal, key))
        });
        let expected = journal.apply(command);
        assert_eq!(result.as_ref().map(|_| ()), expected.as_ref().map(|_| ()));
        if let Ok(applied) = &result {
            assert_eq!(
                applied.accepted.is_some(),
                journal.events.len() > old_events
            );
            assert_eq!(
                applied
                    .drained
                    .iter()
                    .map(|t| t.ordinal)
                    .collect::<Vec<_>>(),
                ((before.resolved_prefix() + 1)..=journal.resolved_prefix()).collect::<Vec<_>>()
            );
            assert_eq!(
                *checkpoint,
                ActiveCheckpoint::from_journal(journal).unwrap()
            );
            assert_eq!(checkpoint.history_bytes(), journal.encode().unwrap().len());
            assert_eq!(
                checkpoint.history_bytes() + checkpoint.pending_bytes(),
                journal.reserved_bytes
            );
            assert_eq!(
                ActiveCheckpoint::decode(&checkpoint.encode().unwrap()).unwrap(),
                *checkpoint
            );
        } else {
            assert_eq!(*checkpoint, before);
        }
        result
    }

    #[test]
    fn shared_rules_match_for_bind_confirm_holes_retries_and_conflicts() {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let binding = cohort(profile);
            let mut journal = Journal::new([9; 16]).unwrap();
            let mut checkpoint = ActiveCheckpoint::from_journal(&journal).unwrap();
            let bind = Command::BindCohort {
                cohort: binding.clone(),
            };
            step(&mut journal, &mut checkpoint, bind.clone()).unwrap();
            step(&mut journal, &mut checkpoint, bind).unwrap();
            let _ = step(&mut journal, &mut checkpoint, begin(1));
            if !profile.is_legacy() {
                let confirm = Command::ConfirmCohortCapacity {
                    cohort: binding.clone(),
                    grants: grants(&binding),
                };
                step(&mut journal, &mut checkpoint, confirm.clone()).unwrap();
                step(&mut journal, &mut checkpoint, confirm).unwrap();
            }
            step(&mut journal, &mut checkpoint, begin(1)).unwrap();
            step(&mut journal, &mut checkpoint, begin(2)).unwrap();
            step(&mut journal, &mut checkpoint, prepared(2)).unwrap();
            step(&mut journal, &mut checkpoint, decide(2, Decision::Commit)).unwrap();
            assert_eq!(checkpoint.resolved_prefix(), 0);
            assert_eq!(checkpoint.active_transactions().len(), 2);
            let result = step(&mut journal, &mut checkpoint, decide(1, Decision::Abort)).unwrap();
            assert_eq!(result.drained.len(), 2);
            assert!(checkpoint.active_transactions().is_empty());
            for command in [
                begin(1),
                begin(2),
                prepared(2),
                decide(1, Decision::Abort),
                decide(2, Decision::Commit),
            ] {
                assert!(step(&mut journal, &mut checkpoint, command)
                    .unwrap()
                    .accepted
                    .is_none());
            }
            for command in [
                prepared(1),
                decide(1, Decision::Commit),
                decide(99, Decision::Abort),
                begin(0),
                Command::Begin {
                    id: 1u128.to_le_bytes(),
                    participants: vec![],
                },
            ] {
                assert!(step(&mut journal, &mut checkpoint, command).is_err());
            }
            let mut changed = participant();
            changed.intent = [3; 32];
            assert!(step(
                &mut journal,
                &mut checkpoint,
                Command::Begin {
                    id: 1u128.to_le_bytes(),
                    participants: vec![changed]
                }
            )
            .is_err());
            let original = admission(&binding, 3, vec![7]);
            step(&mut journal, &mut checkpoint, original.clone()).unwrap();
            step(&mut journal, &mut checkpoint, decide(3, Decision::Abort)).unwrap();
            let mut changed_plan = admission(&binding, 3, vec![8]);
            // The first external request wins even if proposed Plan changes.
            assert!(step(&mut journal, &mut checkpoint, changed_plan.clone())
                .unwrap()
                .accepted
                .is_none());
            if let Command::AdmitRequest { request, .. } = &mut changed_plan {
                request.push(2);
            }
            assert_eq!(
                step(&mut journal, &mut checkpoint, changed_plan),
                Err(Error::IdentityConflict)
            );
            assert!(step(&mut journal, &mut checkpoint, original)
                .unwrap()
                .accepted
                .is_none());
        }
    }

    #[test]
    fn legacy_count_hole_resolves_without_reusing_lifetime_admissions() {
        let mut journal = Journal::new([9; 16]).unwrap();
        for id in 1..=MAX_TRANSACTIONS as u128 {
            journal.replay_event(begin(id)).unwrap();
        }
        for id in 2..=MAX_TRANSACTIONS as u128 {
            journal.replay_event(decide(id, Decision::Abort)).unwrap();
        }
        let mut checkpoint = ActiveCheckpoint::from_journal(&journal).unwrap();
        assert_eq!(checkpoint.active_transactions().len(), MAX_TRANSACTIONS);
        assert_eq!(
            step(
                &mut journal,
                &mut checkpoint,
                begin(MAX_TRANSACTIONS as u128 + 1)
            ),
            Err(Error::Capacity)
        );
        let result = step(&mut journal, &mut checkpoint, decide(1, Decision::Abort)).unwrap();
        assert_eq!(result.drained.len(), MAX_TRANSACTIONS);
        assert_eq!(checkpoint.transaction_count(), MAX_TRANSACTIONS);
        assert!(checkpoint.active_transactions().is_empty());
        assert_eq!(
            step(
                &mut journal,
                &mut checkpoint,
                begin(MAX_TRANSACTIONS as u128 + 1)
            ),
            Err(Error::Capacity)
        );
        assert!(step(&mut journal, &mut checkpoint, begin(1))
            .unwrap()
            .accepted
            .is_none());
    }

    #[test]
    fn full_byte_budget_hole_drains_without_refunding_original_bytes() {
        let binding = cohort(CapacityProfile::ByteBound16MiBV1);
        let mut journal = Journal::new([9; 16]).unwrap();
        journal
            .replay_event(Command::BindCohort {
                cohort: binding.clone(),
            })
            .unwrap();
        journal
            .replay_event(Command::ConfirmCohortCapacity {
                cohort: binding.clone(),
                grants: grants(&binding),
            })
            .unwrap();
        let mut id = 1u128;
        loop {
            let mut next = journal.clone();
            match next.append(admission(&binding, id, vec![0; 100_000]), MAX_JOURNAL_BYTES) {
                Ok(true) => {
                    journal = next;
                    id += 1;
                }
                Err(Error::Capacity) => break,
                result => panic!("unexpected admission: {result:?}"),
            }
        }
        let mut minimum = journal.clone();
        minimum
            .append(admission(&binding, id, vec![]), MAX_JOURNAL_BYTES)
            .unwrap();
        let remaining = MAX_JOURNAL_BYTES - minimum.reserved_bytes;
        let mut final_event = admission(&binding, id, vec![0; (remaining + 3) / 6]);
        let mut next = journal.clone();
        next.append(final_event.clone(), MAX_JOURNAL_BYTES).unwrap();
        let padding = MAX_JOURNAL_BYTES - next.reserved_bytes;
        assert!(padding < 6);
        if let Command::AdmitRequest { request, .. } = &mut final_event {
            request.extend(std::iter::repeat_n(0, padding / 2));
            if !padding.is_multiple_of(2) {
                request[0] = 10;
            }
        }
        let mut checkpoint = ActiveCheckpoint::from_journal(&journal).unwrap();
        let mut over = final_event.clone();
        if let Command::AdmitRequest { request, .. } = &mut over {
            let index = request.iter().position(|byte| *byte < 10).unwrap_or(0);
            request[index] = if request[index] < 10 { 10 } else { 100 };
        }
        assert_eq!(
            step(&mut journal, &mut checkpoint, over),
            Err(Error::Capacity)
        );
        step(&mut journal, &mut checkpoint, final_event.clone()).unwrap();
        assert_eq!(checkpoint.reserved_bytes(), MAX_JOURNAL_BYTES);
        assert!(step(&mut journal, &mut checkpoint, final_event)
            .unwrap()
            .accepted
            .is_none());
        assert_eq!(
            step(&mut journal, &mut checkpoint, begin(id + 1)),
            Err(Error::Capacity)
        );
        for target in (2..=id).chain(std::iter::once(1)) {
            step(&mut journal, &mut checkpoint, prepared(target)).unwrap();
            let result = step(
                &mut journal,
                &mut checkpoint,
                decide(target, Decision::Commit),
            )
            .unwrap();
            assert_eq!(checkpoint.reserved_bytes(), MAX_JOURNAL_BYTES);
            if target != 1 {
                assert!(result.drained.is_empty());
            } else {
                assert_eq!(result.drained.len(), id as usize);
            }
        }
        assert_eq!(checkpoint.history_bytes(), MAX_JOURNAL_BYTES);
        assert_eq!(checkpoint.pending_bytes(), 0);
        assert!(checkpoint.active_transactions().is_empty());
        assert_eq!(
            step(&mut journal, &mut checkpoint, begin(id + 1)),
            Err(Error::Capacity)
        );
        assert!(step(&mut journal, &mut checkpoint, prepared(1))
            .unwrap()
            .accepted
            .is_none());
    }

    #[test]
    fn rejected_lookup_and_invalid_checkpoint_are_not_published() {
        let journal = Journal::new([9; 16]).unwrap();
        let mut checkpoint = ActiveCheckpoint::from_journal(&journal).unwrap();
        let before = checkpoint.clone();
        assert_eq!(
            checkpoint.apply(begin(1), &mut |_| Err(Error::InvalidJournal)),
            Err(Error::InvalidJournal)
        );
        assert_eq!(checkpoint, before);
        assert_eq!(
            checkpoint.apply(begin(1), &mut |_| Ok(LookupResult::Request(None))),
            Err(Error::InvalidJournal)
        );
        assert_eq!(checkpoint, before);
        let bytes = checkpoint.encode().unwrap();
        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        for field in ["history_bytes", "pending_bytes"] {
            let mut invalid = value.clone();
            invalid[field] = 1.into();
            assert!(ActiveCheckpoint::decode(&serde_json::to_vec(&invalid).unwrap()).is_err());
        }
        value["header"]["requests"] = 1.into();
        assert!(ActiveCheckpoint::decode(&serde_json::to_vec(&value).unwrap()).is_err());
        value = serde_json::from_slice(&bytes).unwrap();
        value["future"] = true.into();
        assert!(ActiveCheckpoint::decode(&serde_json::to_vec(&value).unwrap()).is_err());
    }

    #[test]
    fn active_decode_checks_byte_and_item_bounds_before_excess_item() {
        assert_eq!(
            ActiveCheckpoint::decode(&vec![b' '; MAX_ACTIVE_CHECKPOINT_BYTES + 1]),
            Err(Error::Capacity)
        );
        let minimal = Transaction {
            id: [0; 16],
            ordinal: 0,
            participants: vec![],
            prepared: vec![],
            decision: None,
        };
        let item = serde_json::to_string(&minimal).unwrap();
        let text = format!(
            "[{},false]",
            std::iter::repeat_n(item, BYTE_MAX_TRANSACTIONS)
                .collect::<Vec<_>>()
                .join(",")
        );
        let mut decoder = serde_json::Deserializer::from_str(&text);
        let error = deserialize_active(&mut decoder).unwrap_err().to_string();
        assert!(error.contains("too many retained items"), "{error}");
        let binding = cohort(CapacityProfile::ByteBound16MiBV1);
        let item = serde_json::to_string(&grants(&binding)[0]).unwrap();
        let text = format!(
            "[{},false]",
            std::iter::repeat_n(item, MAX_PARTICIPANTS)
                .collect::<Vec<_>>()
                .join(",")
        );
        let mut decoder = serde_json::Deserializer::from_str(&text);
        let error = deserialize_optional_grants(&mut decoder)
            .unwrap_err()
            .to_string();
        assert!(error.contains("too many retained items"), "{error}");
    }

    #[test]
    fn nested_active_decode_rejects_bytes_before_retaining_excess_records() {
        let widest = Transaction {
            id: [255; 16],
            ordinal: u64::MAX,
            participants: vec![
                Participant {
                    shard: u64::MAX,
                    epoch: u64::MAX,
                    mode: [255; 16],
                    intent: [255; 32]
                };
                MAX_PARTICIPANTS
            ],
            prepared: vec![false; MAX_PARTICIPANTS],
            decision: Some(Decision::Commit),
        };
        let item = serde_json::to_string(&widest).unwrap();
        let count = MAX_ACTIVE_CHECKPOINT_BYTES / (item.len() + 1) + 1;
        assert!(count < BYTE_MAX_TRANSACTIONS);
        let text = format!(
            "[{},false]",
            std::iter::repeat_n(item, count)
                .collect::<Vec<_>>()
                .join(",")
        );
        // Exercise the actual nested field decoder: no standalone checkpoint
        // input-size shortcut can hide a count-only allocation regression.
        let mut decoder = serde_json::Deserializer::from_str(&text);
        let error = deserialize_active(&mut decoder).unwrap_err().to_string();
        assert!(
            error.contains("active transaction byte budget exceeded"),
            "{error}"
        );
    }

    #[test]
    fn cold_derivation_checks_actual_original_bytes_against_reservation() {
        let mut journal = Journal::new([9; 16]).unwrap();
        journal.replay_event(begin(1)).unwrap();
        let expected = ActiveCheckpoint::from_journal(&journal).unwrap();
        assert_eq!(expected.history_bytes(), journal.encode().unwrap().len());
        // A corrupted cached charge must not be inherited by a cold checkpoint,
        // even if every count and all active transaction records look valid.
        journal.reserved_bytes += 1;
        assert_eq!(
            ActiveCheckpoint::from_journal(&journal),
            Err(Error::InvalidJournal)
        );
    }

    #[test]
    fn checkpoint_zero_golden_and_strict_header_are_frozen() {
        let checkpoint = ActiveCheckpoint::from_journal(&Journal::new([9; 16]).unwrap()).unwrap();
        let golden = r#"{"version":1,"header":{"authority":[9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9],"cohort":null,"capacity_grants":null,"events":0,"transactions":0,"requests":0,"resolved":0,"encoding_version":1,"reserved_bytes":71},"history_bytes":71,"pending_bytes":0,"active":[]}"#;
        assert_eq!(checkpoint.encode().unwrap(), golden.as_bytes());
        assert_eq!(
            ActiveCheckpoint::decode(golden.as_bytes()).unwrap(),
            checkpoint
        );
        assert!(ActiveCheckpoint::decode(
            golden
                .replace("\"events\":0", "\"events\":0,\"events\":0")
                .as_bytes()
        )
        .is_err());
        assert!(ActiveCheckpoint::decode(
            golden.replace("\"events\":0", "\"future\":0").as_bytes()
        )
        .is_err());
    }

    #[test]
    fn actual_active_codec_and_complete_maximum_width_envelope_fit_bound() {
        fn size(value: &impl Serialize) -> usize {
            serde_json::to_vec(value).unwrap().len()
        }
        fn identity<const N: usize>(wide: bool) -> [u8; N] {
            if wide {
                [255; N]
            } else {
                let mut id = [0; N];
                id[0] = 1;
                id
            }
        }
        // All common decimal fields have identical bytes in Begin and active
        // records. Widening an intermediate value adds equally to both sides,
        // improving this ratio; ordinal and every boolean use maximum widths.
        for count in 1..=MAX_PARTICIPANTS {
            for widths in 0..16 {
                let id = identity(widths & 1 != 0);
                let parts: Vec<_> = (0..count)
                    .map(|i| Participant {
                        shard: if widths & 2 != 0 {
                            u64::MAX - MAX_PARTICIPANTS as u64 + i as u64
                        } else {
                            i as u64
                        },
                        epoch: if widths & 2 != 0 { u64::MAX } else { 0 },
                        mode: identity(widths & 4 != 0),
                        intent: identity(widths & 8 != 0),
                    })
                    .collect();
                let begin_bytes = size(&Command::Begin {
                    id,
                    participants: parts.clone(),
                });
                for decision in [None, Some(Decision::Abort), Some(Decision::Commit)] {
                    for prepared in [false, true] {
                        let tx = Transaction {
                            id,
                            ordinal: u64::MAX,
                            participants: parts.clone(),
                            prepared: vec![prepared; count],
                            decision,
                        };
                        let active_bytes = size(&tx) + 1;
                        // Relative to Begin: remove its 10-byte enum wrapper,
                        // add 31 ordinal bytes, 13 prepared-key/bracket bytes,
                        // (6*n-1) false-array bytes, 1 closing bracket, 20 bytes
                        // for the longest decision, and 1 record separator.
                        // Thus the conservative delta is 55+6*n. This also
                        // covers Commit+false, although validation rejects it.
                        assert!(active_bytes <= begin_bytes + 55 + 6 * count);
                        assert!(3 * active_bytes < 4 * begin_bytes);
                    }
                }
            }
            // The strict ratio margin is 120*n-98 >= 22 for n>=1.
            assert!(3 * (55 + 6 * count) < 67 + 138 * count);
        }
        let members: Vec<_> = (0..MAX_PARTICIPANTS)
            .map(|i| Member {
                shard: u64::MAX - MAX_PARTICIPANTS as u64 + i as u64,
                epoch: u64::MAX,
                mode: [255; 16],
            })
            .collect();
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let binding = Cohort {
                coordinator_shard: u64::MAX,
                coordinator_epoch: u64::MAX,
                members: members.clone(),
                profile,
            };
            let grants = members
                .iter()
                .map(|member| MemberCapacityGrant {
                    member: member.clone(),
                    activation: CapacityActivation {
                        nonce: [255; 32],
                        log_id: openraft::LogId::new(
                            openraft::CommittedLeaderId::new(u64::MAX, u64::MAX),
                            u64::MAX,
                        ),
                    },
                })
                .collect();
            // Synthetic maxima dominate every valid scalar. The actual codec,
            // including all 64 grants and all nesting punctuation, is measured.
            let checkpoint = ActiveCheckpoint {
                version: u32::MAX,
                header: Header {
                    authority: [255; 16],
                    cohort: Some(Box::new(binding)),
                    capacity_grants: Some(grants),
                    events: u64::MAX,
                    transactions: u64::MAX,
                    requests: u64::MAX,
                    resolved: u64::MAX,
                    encoding_version: u32::MAX,
                    reserved_bytes: usize::MAX,
                },
                history_bytes: usize::MAX,
                pending_bytes: usize::MAX,
                active: vec![],
            };
            assert!(size(&checkpoint) <= 64 * 1024);
        }
        assert_eq!(MAX_ACTIVE_CHECKPOINT_BYTES, 22_435_158);
        assert_eq!(BYTE_MAX_TRANSACTIONS, 81_840);
    }
}
