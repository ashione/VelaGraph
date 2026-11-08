//! Runtime-only coordinator representation. Logical snapshots still encode a
//! complete Journal and must explicitly materialize archived state first.
use crate::transaction_decisions::checkpoint::ActiveCheckpoint;
use crate::transaction_decisions::{CapacityProfile, Cohort, Journal};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Coordinator {
    Full(Journal),
    Active(Arc<ActiveCheckpoint>),
}

impl From<Journal> for Coordinator {
    fn from(value: Journal) -> Self {
        Self::Full(value)
    }
}

impl Serialize for Coordinator {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Full(journal) => journal.serialize(serializer),
            Self::Active(_) => Err(serde::ser::Error::custom(
                "archived coordinator requires explicit complete snapshot materialization",
            )),
        }
    }
}

impl<'de> Deserialize<'de> for Coordinator {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Journal::deserialize(deserializer).map(Self::Full)
    }
}

impl Coordinator {
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
    pub(crate) fn resolved_prefix(&self) -> u64 {
        match self {
            Self::Full(value) => value.resolved_prefix(),
            Self::Active(value) => value.resolved_prefix(),
        }
    }
}
