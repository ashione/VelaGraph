//! Clock-driven state machine for the opt-in query-owned lifecycle.
//! The supervisor must serialize calls under one short state lock. No method
//! performs I/O, owns graph data, replenishes fragment cost, or changes V.
use std::time::{Duration, Instant};

pub(crate) const MAX_IDLE: Duration = Duration::from_secs(60);
pub(crate) const MAX_EXECUTION: Duration = Duration::from_secs(120);
pub(crate) const MAX_LIFETIME: Duration = Duration::from_secs(300);
pub(crate) const MAX_RENEW_ATTEMPTS: u16 = 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Phase {
    Capturing,
    Running,
    Sealing,
    Ready,
    Cancelled,
    Expired,
    Failed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Error {
    InvalidPolicy,
    InvalidPhase,
    InvalidGeneration,
    Busy,
    Cancelled,
    Expired,
    Failed,
    Capacity,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Grant {
    pub generation: u64,
    pub expires: Instant,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Renewal {
    /// The caller alone owns this round. Never start I/O for Replay or Busy.
    Begin(Grant),
    Replay(Grant),
}
#[derive(Debug)]
pub(crate) struct Lifecycle {
    phase: Phase,
    idle: Duration,
    grant: Grant,
    execution_end: Instant,
    hard_end: Instant,
    attempts: u16,
    pending: Option<Grant>,
}
impl Lifecycle {
    pub fn new(
        now: Instant,
        idle: Duration,
        execution: Duration,
        lifetime: Duration,
    ) -> Result<Self, Error> {
        if idle < Duration::from_secs(1)
            || idle > MAX_IDLE
            || execution.is_zero()
            || execution > MAX_EXECUTION
            || lifetime.is_zero()
            || lifetime > MAX_LIFETIME
            || execution > lifetime
            || idle > lifetime
        {
            return Err(Error::InvalidPolicy);
        }
        let hard_end = now.checked_add(lifetime).ok_or(Error::InvalidPolicy)?;
        Ok(Self {
            phase: Phase::Capturing,
            idle,
            grant: Grant {
                generation: 0,
                expires: now.checked_add(idle).ok_or(Error::InvalidPolicy)?,
            },
            execution_end: now.checked_add(execution).ok_or(Error::InvalidPolicy)?,
            hard_end,
            attempts: 0,
            pending: None,
        })
    }
    pub fn phase(&mut self, now: Instant) -> Phase {
        let _ = self.check(now);
        self.phase
    }
    #[cfg(test)]
    pub fn grant(&mut self, now: Instant) -> Result<Grant, Error> {
        self.check(now)?;
        Ok(self.grant)
    }
    pub fn deadline(&mut self, now: Instant) -> Result<Instant, Error> {
        self.check(now)?;
        Ok(if self.phase == Phase::Ready {
            self.grant.expires.min(self.hard_end)
        } else {
            self.grant
                .expires
                .min(self.hard_end)
                .min(self.execution_end)
        })
    }
    fn check(&mut self, now: Instant) -> Result<(), Error> {
        match self.phase {
            Phase::Cancelled => return Err(Error::Cancelled),
            Phase::Expired => return Err(Error::Expired),
            Phase::Failed => return Err(Error::Failed),
            _ => {}
        }
        if now >= self.grant.expires
            || now >= self.hard_end
            || (self.phase != Phase::Ready && now >= self.execution_end)
        {
            self.phase = Phase::Expired;
            self.pending = None;
            return Err(Error::Expired);
        }
        Ok(())
    }
    fn advance(&mut self, now: Instant, from: Phase, to: Phase) -> Result<(), Error> {
        self.check(now)?;
        if self.phase != from {
            return Err(Error::InvalidPhase);
        }
        self.phase = to;
        Ok(())
    }
    pub fn captured(&mut self, now: Instant) -> Result<(), Error> {
        self.advance(now, Phase::Capturing, Phase::Running)
    }
    pub fn reads_finished(&mut self, now: Instant) -> Result<(), Error> {
        self.advance(now, Phase::Running, Phase::Sealing)
    }
    pub fn publish(&mut self, now: Instant) -> Result<(), Error> {
        self.advance(now, Phase::Sealing, Phase::Ready)
    }
    pub fn cancel(&mut self, now: Instant) {
        if self.check(now).is_ok() {
            self.phase = Phase::Cancelled;
            self.pending = None;
        }
    }
    /// A required node's conservative local grant can expire before the gateway
    /// idle deadline. The owner proves that condition before calling this hook.
    /// Terminal cancellation/failure is never relabelled or resurrected.
    pub fn expire(&mut self, now: Instant) {
        if self.check(now).is_ok() {
            self.phase = Phase::Expired;
            self.pending = None;
        }
    }
    pub fn fail(&mut self, now: Instant) {
        if self.check(now).is_ok() {
            self.phase = Phase::Failed;
            self.pending = None;
        }
    }
    /// `expected` is the last grant observed by the client. Replaying the most
    /// recent accepted request returns its unchanged grant, never `now + idle`.
    pub fn begin_renewal(&mut self, now: Instant, expected: u64) -> Result<Renewal, Error> {
        self.check(now)?;
        if self.phase == Phase::Capturing {
            return Err(Error::Busy);
        }
        if self.attempts == MAX_RENEW_ATTEMPTS {
            self.fail(now);
            return Err(Error::Capacity);
        }
        self.attempts += 1;
        if expected.checked_add(1) == Some(self.grant.generation) {
            return Ok(Renewal::Replay(self.grant));
        }
        if expected != self.grant.generation {
            return Err(Error::InvalidGeneration);
        }
        if self.pending.is_some() {
            return Err(Error::Busy);
        }
        let Some(generation) = expected.checked_add(1) else {
            self.fail(now);
            return Err(Error::Capacity);
        };
        let grant = Grant {
            generation,
            expires: now
                .checked_add(self.idle)
                .ok_or(Error::InvalidPolicy)?
                .min(self.hard_end),
        };
        self.pending = Some(grant);
        Ok(Renewal::Begin(grant))
    }
    /// Completion linearizes with reads_finished, cancellation and expiry.
    /// A pending grant cannot revive an expired committed grant, and its expiry
    /// is never recomputed after slow I/O. Sealing/Ready need no graph grants.
    pub fn finish_renewal(
        &mut self,
        now: Instant,
        proposed: Grant,
        all_nodes_granted: bool,
    ) -> Result<Grant, Error> {
        self.check(now)?;
        if self.pending != Some(proposed) {
            return Err(Error::InvalidGeneration);
        }
        if self.phase == Phase::Running && !all_nodes_granted {
            self.fail(now);
            return Err(Error::Failed);
        }
        self.pending = None;
        self.grant = proposed;
        Ok(proposed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> (Instant, Lifecycle) {
        let now = Instant::now();
        let machine = Lifecycle::new(now, MAX_IDLE, MAX_EXECUTION, MAX_LIFETIME).unwrap();
        (now, machine)
    }
    fn at(now: Instant, seconds: u64) -> Instant {
        now + Duration::from_secs(seconds)
    }
    fn begin(m: &mut Lifecycle, now: Instant, generation: u64) -> Grant {
        match m.begin_renewal(now, generation).unwrap() {
            Renewal::Begin(grant) => grant,
            Renewal::Replay(_) => panic!("unexpected replay"),
        }
    }
    #[test]
    fn invalid_policy_and_exact_boundaries() {
        let now = Instant::now();
        for (idle, execution, hard) in [
            (0, 1, 1),
            (61, 120, 300),
            (1, 121, 300),
            (1, 1, 301),
            (2, 1, 1),
            (1, 3, 2),
        ] {
            assert!(matches!(
                Lifecycle::new(
                    now,
                    Duration::from_secs(idle),
                    Duration::from_secs(execution),
                    Duration::from_secs(hard)
                ),
                Err(Error::InvalidPolicy)
            ));
        }
        let (_, mut m) = fixture();
        let end = m.grant.expires;
        assert!(m.grant(end - Duration::from_nanos(1)).is_ok());
        assert_eq!(m.grant(end), Err(Error::Expired));
        assert_eq!(m.begin_renewal(end, 0), Err(Error::Expired));
    }
    #[test]
    fn capturing_cannot_renew_or_skip_phases() {
        let (t, mut m) = fixture();
        assert_eq!(m.begin_renewal(t, 0), Err(Error::Busy));
        assert_eq!(m.publish(t), Err(Error::InvalidPhase));
        assert_eq!(m.reads_finished(t), Err(Error::InvalidPhase));
        m.captured(t).unwrap();
        assert_eq!(m.captured(t), Err(Error::InvalidPhase));
        m.reads_finished(t).unwrap();
        m.publish(t).unwrap();
        assert_eq!(m.phase(t), Phase::Ready);
        assert_eq!(m.publish(t), Err(Error::InvalidPhase));
    }
    #[test]
    fn replay_lost_ack_never_extends_twice() {
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        let g = begin(&mut m, at(t, 30), 0);
        assert_eq!(m.begin_renewal(at(t, 31), 0), Err(Error::Busy));
        assert_eq!(m.begin_renewal(at(t, 31), 7), Err(Error::InvalidGeneration));
        assert_eq!(m.finish_renewal(at(t, 32), g, true), Ok(g));
        assert_eq!(m.begin_renewal(at(t, 40), 0), Ok(Renewal::Replay(g)));
        assert_eq!(m.grant(at(t, 40)).unwrap().expires, at(t, 90));
        assert_eq!(m.begin_renewal(at(t, 90), 0), Err(Error::Expired));
    }
    #[test]
    fn pending_does_not_protect_expired_old_grant() {
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        let g = begin(&mut m, at(t, 59), 0);
        assert_eq!(g.expires, at(t, 119));
        assert_eq!(m.finish_renewal(at(t, 60), g, true), Err(Error::Expired));
        assert_eq!(m.phase(at(t, 60)), Phase::Expired);
    }
    #[test]
    fn required_node_failure_wins_before_last_read() {
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        let g = begin(&mut m, at(t, 30), 0);
        assert_eq!(m.finish_renewal(at(t, 31), g, false), Err(Error::Failed));
        assert_eq!(m.reads_finished(at(t, 31)), Err(Error::Failed));
        assert_eq!(m.publish(at(t, 31)), Err(Error::Failed));
    }
    #[test]
    fn last_read_wins_before_unneeded_node_failure() {
        for ready in [false, true] {
            let (t, mut m) = fixture();
            m.captured(t).unwrap();
            let g = begin(&mut m, at(t, 30), 0);
            m.reads_finished(at(t, 31)).unwrap();
            if ready {
                m.publish(at(t, 31)).unwrap();
            }
            assert_eq!(m.finish_renewal(at(t, 32), g, false), Ok(g));
            assert_eq!(m.grant(at(t, 32)).unwrap(), g);
        }
    }
    #[test]
    fn cancel_and_expiry_win_after_last_read() {
        for cancel in [false, true] {
            let (t, mut m) = fixture();
            m.captured(t).unwrap();
            let g = begin(&mut m, at(t, 30), 0);
            m.reads_finished(at(t, 31)).unwrap();
            let (now, error) = if cancel {
                m.cancel(at(t, 32));
                (at(t, 33), Error::Cancelled)
            } else {
                (at(t, 60), Error::Expired)
            };
            assert_eq!(m.finish_renewal(now, g, false), Err(error));
            assert_eq!(m.publish(now), Err(error));
        }
    }
    #[test]
    fn hard_and_execution_caps_survive_repeated_renewal() {
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        for (second, generation) in [(30, 0), (60, 1), (90, 2)] {
            let g = begin(&mut m, at(t, second), generation);
            m.finish_renewal(at(t, second), g, true).unwrap();
        }
        assert_eq!(m.deadline(at(t, 119)).unwrap(), at(t, 120));
        assert_eq!(m.publish(at(t, 120)), Err(Error::Expired));
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        m.reads_finished(t).unwrap();
        m.publish(t).unwrap();
        for second in (30..300).step_by(30) {
            let generation = m.grant(at(t, second)).unwrap().generation;
            let g = begin(&mut m, at(t, second), generation);
            m.finish_renewal(at(t, second), g, false).unwrap();
            assert!(g.expires <= at(t, 300));
        }
        assert_eq!(m.deadline(at(t, 299)).unwrap(), at(t, 300));
        assert_eq!(m.phase(at(t, 300)), Phase::Expired);
    }
    #[test]
    fn renewal_attempts_are_bounded_including_replays() {
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        let g = begin(&mut m, t, 0);
        m.finish_renewal(t, g, true).unwrap();
        for _ in 1..MAX_RENEW_ATTEMPTS {
            assert_eq!(m.begin_renewal(t, 0), Ok(Renewal::Replay(g)));
        }
        assert_eq!(m.begin_renewal(t, 0), Err(Error::Capacity));
        assert_eq!(m.phase(t), Phase::Failed);
    }
    #[test]
    fn stale_completion_cannot_commit_another_round() {
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        let first = begin(&mut m, at(t, 10), 0);
        m.finish_renewal(at(t, 10), first, true).unwrap();
        let second = begin(&mut m, at(t, 20), 1);
        assert_eq!(
            m.finish_renewal(at(t, 21), first, true),
            Err(Error::InvalidGeneration)
        );
        assert_eq!(m.finish_renewal(at(t, 22), second, true), Ok(second));
        assert_eq!(m.begin_renewal(at(t, 23), 0), Err(Error::InvalidGeneration));
    }
    #[test]
    fn cancellation_is_idempotent_and_ready_is_releasable() {
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        m.reads_finished(t).unwrap();
        m.publish(t).unwrap();
        m.cancel(t);
        m.cancel(at(t, 1));
        m.fail(at(t, 2));
        assert_eq!(m.phase(at(t, 500)), Phase::Cancelled);
        assert_eq!(m.begin_renewal(at(t, 3), 0), Err(Error::Cancelled));
    }
    #[test]
    fn generation_overflow_fails_without_wrapping() {
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        m.grant.generation = u64::MAX;
        assert_eq!(m.begin_renewal(t, u64::MAX), Err(Error::Capacity));
        assert_eq!(m.phase(t), Phase::Failed);
    }
    #[test]
    fn required_node_expiry_is_terminal_without_overriding_cancel_or_failure() {
        let (t, mut m) = fixture();
        m.captured(t).unwrap();
        let grant = begin(&mut m, at(t, 10), 0);
        m.expire(at(t, 11));
        assert_eq!(m.phase(at(t, 11)), Phase::Expired);
        assert_eq!(
            m.finish_renewal(at(t, 11), grant, true),
            Err(Error::Expired)
        );
        for cancel in [false, true] {
            let (t, mut m) = fixture();
            if cancel {
                m.cancel(t);
            } else {
                m.fail(t);
            }
            m.expire(at(t, 10));
            assert_eq!(
                m.phase(at(t, 10)),
                if cancel {
                    Phase::Cancelled
                } else {
                    Phase::Failed
                }
            );
        }
    }
}
