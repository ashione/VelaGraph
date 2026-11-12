//! Local monotonic lease state; callers serialize registry reachability with mutation.
use super::*;

pub(super) struct NodeLease {
    pub(super) end: Instant,
    hard: Instant,
    generation: u64,
    attempts: u16,
    last: Option<(u64, u64)>, // requested duration, original granted duration
}
impl NodeLease {
    pub(super) fn new(end: Instant, hard: Instant) -> Self {
        Self {
            end: end.min(hard),
            hard,
            generation: 0,
            attempts: 0,
            last: None,
        }
    }
    pub(super) fn renew(
        &mut self,
        now: Instant,
        generation: u64,
        millis: u64,
    ) -> Result<u64, RemoteError> {
        if now >= self.end || now >= self.hard {
            return Err(RemoteError::SessionUnavailable);
        }
        if self.attempts >= 1024 {
            self.end = now;
            return Err(RemoteError::ResourceExhausted);
        }
        self.attempts += 1;
        if !(1..=60_000).contains(&millis) {
            return Err(RemoteError::InvalidArgument);
        }
        if generation == self.generation {
            return match self.last {
                Some((requested, granted)) if requested == millis => Ok(granted),
                _ => Err(RemoteError::InvalidArgument),
            };
        }
        if self.generation.checked_add(1) != Some(generation) {
            return Err(RemoteError::InvalidArgument);
        }
        self.end = (now + Duration::from_millis(millis)).min(self.hard);
        let granted = self.end.saturating_duration_since(now).as_millis() as u64;
        self.generation = generation;
        self.last = Some((millis, granted));
        Ok(granted)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn replay_boundaries_and_attempt_exhaustion() {
        let now = Instant::now();
        let mut lease = NodeLease::new(
            now + Duration::from_secs(60),
            now + Duration::from_secs(120),
        );
        let later = now + Duration::from_secs(50);
        assert_eq!(lease.renew(later, 1, 60_000).unwrap(), 60_000);
        let end = lease.end;
        assert_eq!(
            lease
                .renew(later + Duration::from_secs(1), 1, 60_000)
                .unwrap(),
            60_000
        );
        assert_eq!(lease.end, end);
        assert!(lease.renew(later, 1, 59_000).is_err());
        assert!(lease.renew(later, 3, 60_000).is_err());
        assert!(lease.renew(later, 0, 60_000).is_err());
        assert_eq!(
            lease
                .renew(now + Duration::from_secs(100), 2, 60_000)
                .unwrap(),
            20_000
        );
        assert!(lease
            .renew(now + Duration::from_secs(120), 3, 60_000)
            .is_err());
        let mut lease = NodeLease::new(end, end);
        assert_eq!(lease.renew(later, 1, 1_000).unwrap(), 1_000);
        for _ in 1..1024 {
            lease.renew(later, 1, 1_000).unwrap();
        }
        assert!(matches!(
            lease.renew(later, 1, 1_000),
            Err(RemoteError::ResourceExhausted)
        ));
        assert!(lease.renew(later, 2, 1_000).is_err());
    }
    #[test]
    fn idle_expiry_never_resurrects() {
        let now = Instant::now();
        let end = now + Duration::from_secs(1);
        let mut lease = NodeLease::new(end, now + Duration::from_secs(120));
        assert!(lease.renew(end, 1, 60_000).is_err());
        assert_eq!(lease.end, end);
    }
}
