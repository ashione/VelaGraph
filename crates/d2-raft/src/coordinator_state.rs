//! Shared runtime-only coordinator/member history representation. Logical snapshots still encode a
//! complete Journal and must explicitly materialize archived state first.
use crate::transaction_decisions::checkpoint::ActiveCheckpoint;
use crate::transaction_decisions::{CapacityProfile, Cohort, Journal};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum History {
    Full(Journal),
    Active(Arc<ActiveCheckpoint>),
}

pub(crate) type Coordinator = History;

impl From<Journal> for History {
    fn from(value: Journal) -> Self {
        Self::Full(value)
    }
}

impl Serialize for History {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Full(journal) => journal.serialize(serializer),
            Self::Active(_) => Err(serde::ser::Error::custom(
                "archived history requires explicit complete snapshot materialization",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for History {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Journal::deserialize(deserializer).map(Self::Full)
    }
}

impl History {
    pub(crate) fn full(&self) -> Option<&Journal> {
        match self {
            Self::Full(value) => Some(value),
            Self::Active(_) => None,
        }
    }
    pub(crate) fn full_mut(&mut self) -> Option<&mut Journal> {
        match self {
            Self::Full(value) => Some(value),
            Self::Active(_) => None,
        }
    }
    #[cfg(test)]
    pub(crate) fn into_full(self) -> Option<Journal> {
        match self {
            Self::Full(value) => Some(value),
            Self::Active(_) => None,
        }
    }
    pub(crate) fn active(&self) -> Option<&ActiveCheckpoint> {
        match self {
            Self::Full(_) => None,
            Self::Active(value) => Some(value.as_ref()),
        }
    }
    pub(crate) fn active_arc(&self) -> Option<&Arc<ActiveCheckpoint>> {
        match self {
            Self::Full(_) => None,
            Self::Active(value) => Some(value),
        }
    }
    pub(crate) fn authority(&self) -> [u8; 16] {
        match self {
            Self::Full(value) => value.authority(),
            Self::Active(value) => value.authority(),
        }
    }
    pub(crate) fn cohort(&self) -> Option<&Cohort> {
        match self {
            Self::Full(value) => value.cohort(),
            Self::Active(value) => value.cohort(),
        }
    }
    pub(crate) fn capacity_profile(&self) -> CapacityProfile {
        match self {
            Self::Full(value) => value.capacity_profile(),
            Self::Active(value) => value.capacity_profile(),
        }
    }
    pub(crate) fn capacity_grants(
        &self,
    ) -> Option<&[crate::transaction_decisions::MemberCapacityGrant]> {
        match self {
            Self::Full(value) => value.capacity_grants(),
            Self::Active(value) => value.capacity_grants(),
        }
    }
    pub(crate) fn capacity_ready(&self) -> bool {
        self.capacity_profile() == CapacityProfile::LegacyCountV1
            || self.capacity_grants().is_some()
    }
    pub(crate) fn event_count(&self) -> u64 {
        match self {
            Self::Full(value) => value.events().len() as u64,
            Self::Active(value) => value.event_count(),
        }
    }
    pub(crate) fn resolved_prefix(&self) -> u64 {
        match self {
            Self::Full(value) => value.resolved_prefix(),
            Self::Active(value) => value.resolved_prefix(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn full_history_preserves_published_journal_encoding() {
        let journal = Journal::new([1; 16]).unwrap();
        let bytes = serde_json::to_vec(&journal).unwrap();
        let history = History::Full(journal);
        assert_eq!(serde_json::to_vec(&history).unwrap(), bytes);
        assert_eq!(serde_json::from_slice::<History>(&bytes).unwrap(), history);
    }

    #[test]
    fn active_history_requires_explicit_materialization() {
        let journal = Journal::new([1; 16]).unwrap();
        let checkpoint = ActiveCheckpoint::from_journal(&journal).unwrap();
        let history = History::Active(Arc::new(checkpoint));
        assert!(history.full().is_none());
        assert!(serde_json::to_vec(&history).is_err());
        assert_eq!(history.event_count(), 0);
    }
}
