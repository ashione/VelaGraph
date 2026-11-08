//! Immutable fixed-cohort capacity policies. Structural ceilings are derived
//! from retained byte budgets; they do not promise that those counts will fit.
use crate::transaction_decisions::{Cohort, Error, Member, MAX_JOURNAL_BYTES, MAX_PARTICIPANTS};
use crate::NodeId;
use openraft::LogId;
use serde::{de, Deserialize, Deserializer, Serialize};
use std::fmt;
use std::io::{self, Write};
use std::marker::PhantomData;

/// Existing fixed charge for every retained participant intent.
pub const INTENT_FIXED_BYTES: usize = 256;
/// Minimum canonical size of any accepted original Command.
pub(crate) const MIN_EVENT_BYTES: usize = 70;
/// Minimum canonical size of a Begin-equivalent transaction admission.
pub(crate) const MIN_ADMISSION_BYTES: usize = 205;
pub(crate) const BYTE_MAX_TRANSACTIONS: usize = MAX_JOURNAL_BYTES / MIN_ADMISSION_BYTES;
pub(crate) const BYTE_MAX_HISTORY_EVENTS: usize = MAX_JOURNAL_BYTES / MIN_EVENT_BYTES;
/// Full exact terminal records plus their canonical identity/segment envelope.
pub const MAX_BYTE_PROFILE_TERMINAL_BYTES: usize = (4 * MAX_JOURNAL_BYTES).div_ceil(3) + 64 * 1024;

/// A versioned policy, selected once as part of an empty cohort's identity.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CapacityProfile {
    /// Published count limits and byte budgets, also used by unbound state.
    #[default]
    LegacyCountV1,
    /// The unchanged 16 MiB Journal budget, including resolution reservations.
    ByteBound16MiBV1,
}
impl CapacityProfile {
    pub const fn is_legacy(&self) -> bool {
        matches!(self, Self::LegacyCountV1)
    }
    pub const fn max_transactions(self) -> usize {
        match self {
            Self::LegacyCountV1 => crate::transaction_decisions::MAX_TRANSACTIONS,
            Self::ByteBound16MiBV1 => BYTE_MAX_TRANSACTIONS,
        }
    }
    pub const fn max_requests(self) -> usize {
        match self {
            Self::LegacyCountV1 => crate::transaction_decisions::MAX_REQUESTS,
            Self::ByteBound16MiBV1 => BYTE_MAX_TRANSACTIONS,
        }
    }
    pub const fn max_history_events(self) -> u64 {
        match self {
            Self::LegacyCountV1 => {
                (crate::transaction_decisions::MAX_TRANSACTIONS * (MAX_PARTICIPANTS + 2) + 1) as u64
            }
            Self::ByteBound16MiBV1 => BYTE_MAX_HISTORY_EVENTS as u64,
        }
    }
    pub const fn max_intents(self) -> usize {
        match self {
            Self::LegacyCountV1 => crate::participant_writes::MAX_INTENTS,
            Self::ByteBound16MiBV1 => {
                crate::participant_writes::MAX_INTENT_BYTES / INTENT_FIXED_BYTES
            }
        }
    }
    /// Raw-ledger and derived-index format bound to this exact profile.
    pub const fn storage_format(self) -> u32 {
        match self {
            Self::LegacyCountV1 => 1,
            Self::ByteBound16MiBV1 => 2,
        }
    }
    pub const fn history_version(self) -> u32 {
        self.storage_format()
    }
    pub const fn terminal_prefix_version(self) -> u32 {
        self.storage_format()
    }
    pub(crate) fn deserialize_cohort_tag<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Self, D::Error> {
        match Self::deserialize(deserializer)? {
            Self::ByteBound16MiBV1 => Ok(Self::ByteBound16MiBV1),
            Self::LegacyCountV1 => Err(de::Error::custom("legacy cohort profile must be absent")),
        }
    }
}

/// Durable generation fixed by the first successful member activation.
/// The nonce is generated before proposal; deterministic apply fixes this LogId.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityActivation {
    pub nonce: [u8; 32],
    #[serde(deserialize_with = "deserialize_activation_log_id")]
    pub log_id: LogId<NodeId>,
}
fn deserialize_activation_log_id<'de, D: Deserializer<'de>>(
    d: D,
) -> Result<LogId<NodeId>, D::Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Leader {
        term: u64,
        node_id: NodeId,
    }
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Marker {
        leader_id: Leader,
        index: u64,
    }
    let marker = Marker::deserialize(d)?;
    Ok(LogId::new(
        openraft::CommittedLeaderId::new(marker.leader_id.term, marker.leader_id.node_id),
        marker.index,
    ))
}
impl CapacityActivation {
    pub fn validate(&self) -> Result<(), Error> {
        if self.nonce == [0; 32] || self.log_id.index == 0 || self.log_id.leader_id.term == 0 {
            return Err(Error::InvalidIdentity);
        }
        Ok(())
    }
}

/// One immutable durable member generation in a complete-cohort confirmation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemberCapacityGrant {
    pub member: Member,
    pub activation: CapacityActivation,
}

pub(crate) fn validate_grants(
    cohort: &Cohort,
    grants: &[MemberCapacityGrant],
) -> Result<(), Error> {
    cohort.validate()?;
    if cohort.profile != CapacityProfile::ByteBound16MiBV1 || grants.len() != cohort.members.len() {
        return Err(Error::InvalidParticipants);
    }
    for (member, grant) in cohort.members.iter().zip(grants) {
        if member != &grant.member {
            return Err(Error::InvalidParticipants);
        }
        grant.activation.validate()?;
    }
    Ok(())
}

/// Reject the first excess item before deserializing or allocating it. Do not
/// trust a size hint for allocation, even when the input is not a wire frame.
pub(crate) fn deserialize_bounded_vec<'de, D, T>(
    deserializer: D,
    limit: usize,
) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Items<T> {
        limit: usize,
        marker: PhantomData<T>,
    }
    impl<'de, T: Deserialize<'de>> de::Visitor<'de> for Items<T> {
        type Value = Vec<T>;
        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "a sequence of at most {} items", self.limit)
        }
        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            if seq.size_hint().is_some_and(|size| size > self.limit) {
                return Err(de::Error::custom("too many retained items"));
            }
            let mut items = Vec::new();
            while items.len() < self.limit {
                let Some(item) = seq.next_element()? else {
                    return Ok(items);
                };
                items.push(item);
            }
            seq.next_element_seed(RejectExtra)?;
            Ok(items)
        }
    }
    deserializer.deserialize_seq(Items {
        limit,
        marker: PhantomData,
    })
}

pub(crate) struct RejectExtra;
impl<'de> de::DeserializeSeed<'de> for RejectExtra {
    type Value = ();
    fn deserialize<D: Deserializer<'de>>(self, _deserializer: D) -> Result<(), D::Error> {
        Err(de::Error::custom("too many retained items"))
    }
}

/// Count canonical JSON without allocating an encoded archive or proof.
pub(crate) fn canonical_size<T: Serialize + ?Sized>(
    value: &T,
    limit: usize,
) -> Result<usize, Error> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if bytes.len() > self.limit - self.bytes {
                return Err(io::Error::other("canonical JSON exceeds byte limit"));
            }
            self.bytes += bytes.len();
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value).map_err(|_| Error::Capacity)?;
    Ok(counter.bytes)
}
