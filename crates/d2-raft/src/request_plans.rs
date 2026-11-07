//! Versioned exact write plans stored by AdmitRequest. This validates protocol
//! identities and bounds, not application-specific graph semantics.
use crate::participant_writes::Authority;
use crate::transaction_decisions::{
    Cohort, Error, Participant, MAX_PARTICIPANTS, MAX_REQUEST_PAYLOAD_BYTES,
};
use crate::{D1WriteRequest, WriteOp, MAX_BATCH_BYTES, MAX_BATCH_OPERATIONS};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Part {
    pub descriptor: Participant,
    pub request: D1WriteRequest,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub version: u32,
    pub id: [u8; 16],
    pub authority: Authority,
    pub cohort: Cohort,
    pub validation_version: u64,
    pub parts: Vec<Part>,
}
impl Plan {
    pub fn validate(&self) -> Result<(), Error> {
        if self.version != 1
            || self.id == [0; 16]
            || self.authority.mode == [0; 16]
            || self.authority.shard != self.cohort.coordinator_shard
            || self.cohort.validate().is_err()
            || self.parts.is_empty()
            || self.parts.len() > MAX_PARTICIPANTS
            || self
                .parts
                .windows(2)
                .any(|w| w[0].descriptor.shard >= w[1].descriptor.shard)
        {
            return Err(Error::InvalidIdentity);
        }
        let mut operations = 0usize;
        let mut payload = 0usize;
        for p in &self.parts {
            if !self.cohort.contains(&p.descriptor)
                || p.descriptor.intent == [0; 32]
                || p.request.request_id != self.id
                || p.request.shard != p.descriptor.shard
                || p.request.epoch != p.descriptor.epoch
                || p.request.expected_sequence.is_none()
                || p.request.operations.is_empty()
            {
                return Err(Error::InvalidIdentity);
            }
            operations = operations.saturating_add(p.request.operations.len());
            payload = payload.saturating_add(
                p.request
                    .operations
                    .iter()
                    .map(|op| match op {
                        WriteOp::Put { key, value } => key.len() + value.len(),
                        WriteOp::Delete { key } => key.len(),
                    })
                    .sum::<usize>(),
            );
            if operations > MAX_BATCH_OPERATIONS || payload > MAX_BATCH_BYTES {
                return Err(Error::Capacity);
            }
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| Error::InvalidJournal)?;
        if bytes.len() > MAX_REQUEST_PAYLOAD_BYTES {
            return Err(Error::Capacity);
        }
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > MAX_REQUEST_PAYLOAD_BYTES {
            return Err(Error::Capacity);
        }
        let plan: Self = serde_json::from_slice(bytes).map_err(|_| Error::InvalidJournal)?;
        plan.validate()?;
        Ok(plan)
    }
}
