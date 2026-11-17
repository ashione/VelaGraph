//! Operation-local output collection; never an authority by itself.
use super::*;
use crate::transaction_decisions::Transaction;

pub(crate) struct Collector {
    authority: [u8; 16],
    cohort: Cohort,
    through: u64,
    segments: Vec<Vec<Terminal>>,
    count: u64,
    record_bytes: usize,
    failed: bool,
}
impl Collector {
    pub(crate) fn new(authority: [u8; 16], cohort: Cohort, through: u64) -> Self {
        let failed = through > cohort.profile.max_transactions() as u64;
        Self {
            authority,
            cohort,
            through,
            segments: Vec::new(),
            count: 0,
            record_bytes: 0,
            failed,
        }
    }

    // A collection error must not short-circuit the caller's strict raw/index
    // closure. It is reported by finish only after that authority is verified.
    pub(crate) fn observe(&mut self, tx: &Transaction) {
        if self.failed || tx.ordinal > self.through {
            return;
        }
        if tx.ordinal != self.count + 1
            || self.count >= self.cohort.profile.max_transactions() as u64
            || tx.participants.len() > MAX_PARTICIPANTS
            || tx.prepared.len() > MAX_PARTICIPANTS
        {
            self.failed = true;
            return;
        }
        let Some(decision) = tx.decision else {
            self.failed = true;
            return;
        };
        // At most one bounded temporary record is allocated before sizing.
        let terminal = Terminal {
            id: tx.id,
            ordinal: tx.ordinal,
            participants: tx.participants.clone(),
            prepared: tx.prepared.clone(),
            decision,
        };
        if self.cohort.profile == CapacityProfile::ByteBound16MiBV1 {
            // Sum of records is a lower bound on the final Encoding size. No
            // repeated envelope/cohort charge can reject a valid old proof.
            let Ok(bytes) = canonical_size(
                &terminal,
                MAX_BYTE_PROFILE_TERMINAL_BYTES - self.record_bytes,
            ) else {
                self.failed = true;
                return;
            };
            self.record_bytes += bytes;
        }
        if self.count.is_multiple_of(SEGMENT_TRANSACTIONS as u64) {
            self.segments.push(Vec::new());
        }
        self.segments.last_mut().unwrap().push(terminal);
        self.count += 1;
    }

    pub(crate) fn finish(self) -> Result<TerminalPrefix, Error> {
        if self.failed || self.count != self.through {
            return Err(Error::InvalidJournal);
        }
        TerminalPrefix::from_segments(self.authority, self.cohort, self.through, self.segments)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_decisions::{CapacityActivation, Command, Member, MemberCapacityGrant};

    fn wide_journal(profile: CapacityProfile) -> Journal {
        let cohort = Cohort {
            profile,
            coordinator_shard: 99,
            coordinator_epoch: 1,
            members: (1..=64)
                .map(|shard| Member {
                    shard,
                    epoch: 1,
                    mode: [shard as u8; 16],
                })
                .collect(),
        };
        let mut journal = Journal::new([99; 16]).unwrap();
        journal
            .apply(Command::BindCohort {
                cohort: cohort.clone(),
            })
            .unwrap();
        if !profile.is_legacy() {
            journal
                .apply(Command::ConfirmCohortCapacity {
                    cohort: cohort.clone(),
                    grants: cohort
                        .members
                        .iter()
                        .map(|member| MemberCapacityGrant {
                            member: member.clone(),
                            activation: CapacityActivation {
                                nonce: [member.shard as u8; 32],
                                log_id: openraft::LogId::new(
                                    openraft::CommittedLeaderId::new(1, member.shard),
                                    1,
                                ),
                            },
                        })
                        .collect(),
                })
                .unwrap();
        }
        let participants: Vec<_> = cohort
            .members
            .iter()
            .map(|member| Participant {
                shard: member.shard,
                epoch: member.epoch,
                mode: member.mode,
                intent: [255; 32],
            })
            .collect();
        for n in 1..=65u8 {
            journal
                .apply(Command::Begin {
                    id: [n; 16],
                    participants: participants.clone(),
                })
                .unwrap();
            journal
                .apply(Command::Decide {
                    id: [n; 16],
                    decision: Decision::Abort,
                })
                .unwrap();
        }
        journal
    }

    #[test]
    fn collector_matches_every_wide_legal_journal_prefix() {
        for profile in [
            CapacityProfile::LegacyCountV1,
            CapacityProfile::ByteBound16MiBV1,
        ] {
            let journal = wide_journal(profile);
            for version in 0..=65 {
                let mut collector = Collector::new(
                    journal.authority(),
                    journal.cohort().unwrap().clone(),
                    version,
                );
                for ordinal in 1..=65 {
                    collector.observe(journal.transaction_at(ordinal).unwrap());
                }
                assert_eq!(
                    collector.finish().unwrap(),
                    journal.terminal_prefix(version).unwrap()
                );
            }
        }
    }

    #[test]
    fn collector_stops_retaining_at_existing_byte_and_count_limits() {
        let journal = wide_journal(CapacityProfile::ByteBound16MiBV1);
        let cohort = journal.cohort().unwrap().clone();
        let mut excessive_count = Collector::new(
            journal.authority(),
            cohort.clone(),
            cohort.profile.max_transactions() as u64 + 1,
        );
        excessive_count.observe(journal.transaction_at(1).unwrap());
        assert!(excessive_count.segments.is_empty());
        assert!(excessive_count.finish().is_err());
        let mut collector = Collector::new(
            journal.authority(),
            cohort.clone(),
            cohort.profile.max_transactions() as u64,
        );
        let mut tx = journal.transaction_at(1).unwrap().clone();
        for ordinal in 1..=cohort.profile.max_transactions() as u64 {
            tx.ordinal = ordinal;
            tx.id[..8].copy_from_slice(&ordinal.to_le_bytes());
            collector.observe(&tx);
            if collector.failed {
                break;
            }
        }
        assert!(
            collector.failed,
            "wide output must exhaust the byte cap before the count cap"
        );
        assert!(collector.record_bytes <= MAX_BYTE_PROFILE_TERMINAL_BYTES);
        let retained = collector.count;
        collector.observe(&tx);
        assert_eq!(collector.count, retained);
        assert!(collector.finish().is_err());
    }

    #[test]
    fn collector_defers_invalid_and_incomplete_output_to_finish() {
        let journal = wide_journal(CapacityProfile::LegacyCountV1);
        for records in [vec![2], vec![1, 1], vec![1]] {
            let mut collector =
                Collector::new(journal.authority(), journal.cohort().unwrap().clone(), 2);
            for ordinal in records {
                collector.observe(journal.transaction_at(ordinal).unwrap());
            }
            assert!(collector.finish().is_err());
        }
    }
}
