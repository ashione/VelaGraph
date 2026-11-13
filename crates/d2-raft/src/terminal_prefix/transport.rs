//! Typed transport boundary. Parts are untrusted until checked reconstruction.
//! No mutable access to an already validated prefix is exposed.
use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalRecord {
    pub id: [u8; 16],
    pub ordinal: u64,
    pub participants: Vec<Participant>,
    pub prepared: Vec<bool>,
    pub decision: Decision,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TerminalPrefixParts {
    pub format_version: u32,
    pub authority: [u8; 16],
    pub cohort: Cohort,
    pub through: u64,
    pub segments: Vec<Vec<TerminalRecord>>,
}
impl From<TerminalPrefix> for TerminalPrefixParts {
    fn from(value: TerminalPrefix) -> Self {
        Self {
            format_version: value.cohort.profile.terminal_prefix_version(),
            authority: value.authority,
            cohort: value.cohort,
            through: value.through,
            segments: value
                .segments
                .into_iter()
                .map(|segment| {
                    segment
                        .into_iter()
                        .map(|t| TerminalRecord {
                            id: t.id,
                            ordinal: t.ordinal,
                            participants: t.participants,
                            prepared: t.prepared,
                            decision: t.decision,
                        })
                        .collect()
                })
                .collect(),
        }
    }
}
impl TryFrom<TerminalPrefixParts> for TerminalPrefix {
    type Error = &'static str;
    fn try_from(value: TerminalPrefixParts) -> Result<Self, Self::Error> {
        if value.segments.len() > BYTE_MAX_TRANSACTIONS.div_ceil(SEGMENT_TRANSACTIONS) {
            return Err("too many terminal segments");
        }
        let mut bytes = 0;
        let mut segments = Vec::with_capacity(value.segments.len());
        for segment in value.segments {
            if segment.len() > SEGMENT_TRANSACTIONS
                || segment.iter().any(|t| {
                    t.participants.len() > MAX_PARTICIPANTS || t.prepared.len() > MAX_PARTICIPANTS
                })
            {
                return Err("terminal segment count limit exceeded");
            }
            let segment: Vec<Terminal> = segment
                .into_iter()
                .map(|t| Terminal {
                    id: t.id,
                    ordinal: t.ordinal,
                    participants: t.participants,
                    prepared: t.prepared,
                    decision: t.decision,
                })
                .collect();
            // Same cumulative canonical budget as the serde segment visitor.
            bytes += canonical_size(&segment, MAX_BYTE_PROFILE_TERMINAL_BYTES - bytes)
                .map_err(|_| "terminal segment byte budget exceeded")?;
            segments.push(segment);
        }
        Self::try_from(Encoding {
            version: value.format_version,
            authority: value.authority,
            cohort: value.cohort,
            through: value.through,
            segments,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::Member;
    fn parts(count: usize, profile: CapacityProfile) -> TerminalPrefixParts {
        let records: Vec<_> = (1..=count)
            .map(|n| TerminalRecord {
                id: (n as u128).to_le_bytes(),
                ordinal: n as u64,
                participants: vec![Participant {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                    intent: [3; 32],
                }],
                prepared: vec![false],
                decision: Decision::Abort,
            })
            .collect();
        TerminalPrefixParts {
            format_version: profile.terminal_prefix_version(),
            authority: [9; 16],
            cohort: Cohort {
                coordinator_shard: 9,
                coordinator_epoch: 1,
                members: vec![Member {
                    shard: 7,
                    epoch: 1,
                    mode: [7; 16],
                }],
                profile,
            },
            through: count as u64,
            segments: records
                .chunks(SEGMENT_TRANSACTIONS)
                .map(|c| c.to_vec())
                .collect(),
        }
    }
    #[test]
    fn transport_parts_preserve_exact_validated_evidence() {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            for count in [0, 1, 32, 33] {
                let parts = parts(count, profile);
                let proof = TerminalPrefix::try_from(parts.clone()).unwrap();
                assert_eq!(TerminalPrefixParts::from(proof.clone()), parts);
                // Existing disk/legacy wire serde is unchanged, not a transport bridge.
                let bytes = serde_json::to_vec(&proof).unwrap();
                let legacy: TerminalPrefix = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(proof, legacy);
            }
        }
    }
    #[test]
    fn transport_cannot_bypass_checked_prefix_or_decode_bounds() {
        let good = parts(2, CapacityProfile::LegacyCountV1);
        let reject = |p| assert!(TerminalPrefix::try_from(p).is_err());
        let mut p = good.clone();
        p.format_version = 2;
        reject(p);
        let mut p = good.clone();
        p.authority = [0; 16];
        reject(p);
        let mut p = good.clone();
        p.through = 1;
        reject(p);
        let mut p = good.clone();
        p.segments[0][1].ordinal = 1;
        reject(p);
        let mut p = good.clone();
        p.segments[0][1].id = p.segments[0][0].id;
        reject(p);
        let mut p = good.clone();
        p.segments[0][0].decision = Decision::Commit;
        reject(p);
        let mut p = good.clone();
        p.segments[0][0].prepared.clear();
        reject(p);
        let mut p = good.clone();
        p.segments[0][0].participants[0].intent = [0; 32];
        reject(p);
        let mut p = good.clone();
        p.segments[0][0].participants[0].mode = [8; 16];
        reject(p);
        let mut p = good.clone();
        p.segments[0][0].prepared = vec![false; MAX_PARTICIPANTS + 1];
        reject(p);
        let mut p = good.clone();
        p.segments[0] = vec![p.segments[0][0].clone(); SEGMENT_TRANSACTIONS + 1];
        reject(p);
        let mut p = good;
        p.segments = vec![vec![]; BYTE_MAX_TRANSACTIONS.div_ceil(SEGMENT_TRANSACTIONS) + 1];
        reject(p);
    }
}
