//! Retry only failed, side-effect-free quorum confirmation, never a graph operation.
use super::{diagnostic, RemoteError};
use d2_raft::TypeConfig;
use openraft::error::{CheckIsLeaderError, RaftError};
use std::{future::Future, time::Duration};
use tokio::time::{timeout_at, Instant};

const MAX_ATTEMPTS: usize = 3;
const RETRY_DELAY: Duration = Duration::from_millis(10);

/// A fresh successful confirmation AND apply wait is mandatory. No cached leader
/// observation, prior failed confirmation, or partial quorum may authorize a read.
pub(super) async fn confirm(
    raft: &openraft::Raft<TypeConfig>,
    end: Instant,
    phase: &'static str,
) -> Result<(), RemoteError> {
    bounded(end, || async {
        raft.ensure_linearizable()
            .await
            .map(|_| ())
            .map_err(|error| {
                diagnostic::failure(format_args!(
                    "phase={phase} reason={}",
                    diagnostic::read_barrier_kind(&error)
                ));
                classify(error)
            })
    })
    .await
}

type ConfirmationError = RaftError<u64, CheckIsLeaderError<u64, openraft::BasicNode>>;
fn classify(error: ConfirmationError) -> (RemoteError, bool) {
    let retryable = matches!(
        &error,
        RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_))
    );
    let mapped = if let Some(hint) = error.forward_to_leader::<openraft::BasicNode>() {
        RemoteError::NotLeader {
            leader: hint.leader_id,
        }
    } else {
        RemoteError::Unavailable
    };
    (mapped, retryable)
}

// Some existing session-open APIs intentionally expose no routing hint.
pub(super) fn without_leader_hint(error: RemoteError) -> RemoteError {
    match error {
        RemoteError::NotLeader { .. } => RemoteError::Unavailable,
        other => other,
    }
}

async fn bounded<F, Fut>(end: Instant, mut confirmation: F) -> Result<(), RemoteError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<(), (RemoteError, bool)>>,
{
    for attempt in 0..MAX_ATTEMPTS {
        // timeout_at may poll a ready future once even when the deadline elapsed.
        if Instant::now() >= end {
            return Err(RemoteError::Timeout { request_id: None });
        }
        let result = timeout_at(end, confirmation())
            .await
            .map_err(|_| RemoteError::Timeout { request_id: None })?;
        if Instant::now() >= end {
            return Err(RemoteError::Timeout { request_id: None });
        }
        match result {
            Ok(()) => return Ok(()),
            Err((error, retryable)) if !retryable || attempt + 1 == MAX_ATTEMPTS => {
                return Err(error)
            }
            Err(_) => {
                timeout_at(end, tokio::time::sleep(RETRY_DELAY))
                    .await
                    .map_err(|_| RemoteError::Timeout { request_id: None })?;
            }
        }
    }
    unreachable!("nonzero bounded confirmation attempts")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn only_typed_quorum_failure_is_retryable() {
        use openraft::error::{Fatal, ForwardToLeader, QuorumNotEnough};
        assert!(
            classify(RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(
                QuorumNotEnough {
                    cluster: String::new(),
                    got: Default::default()
                }
            )))
            .1
        );
        let (mapped, retryable) = classify(RaftError::APIError(
            CheckIsLeaderError::ForwardToLeader(ForwardToLeader {
                leader_id: Some(2),
                leader_node: None,
            }),
        ));
        assert!(!retryable);
        assert!(matches!(mapped, RemoteError::NotLeader { leader: Some(2) }));
        assert!(!classify(RaftError::Fatal(Fatal::Stopped)).1);
        assert!(!classify(RaftError::Fatal(Fatal::Panicked)).1);
    }
    #[tokio::test]
    async fn transient_confirmation_retries_without_replaying_work() {
        let calls = AtomicUsize::new(0);
        bounded(Instant::now() + Duration::from_secs(1), || async {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err((RemoteError::Unavailable, true))
            } else {
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn persistent_failure_is_attempt_bounded() {
        let calls = AtomicUsize::new(0);
        assert!(matches!(
            bounded(Instant::now() + Duration::from_secs(1), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err((RemoteError::Unavailable, true))
            })
            .await,
            Err(RemoteError::Unavailable)
        ));
        assert_eq!(calls.load(Ordering::SeqCst), MAX_ATTEMPTS);
    }
    #[tokio::test]
    async fn not_leader_and_fatal_are_not_retried() {
        for error in [
            RemoteError::NotLeader { leader: Some(2) },
            RemoteError::Unavailable,
        ] {
            let calls = AtomicUsize::new(0);
            bounded(Instant::now() + Duration::from_secs(1), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err((error.clone(), false))
            })
            .await
            .unwrap_err();
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
    }
    #[tokio::test]
    async fn expired_budget_does_not_start_confirmation() {
        bounded(Instant::now(), || async { panic!("must not poll") })
            .await
            .unwrap_err();
    }
    #[tokio::test]
    async fn budget_includes_pending_apply_wait_and_backoff() {
        assert!(matches!(
            bounded(Instant::now() + Duration::from_millis(5), || async {
                std::future::pending().await
            })
            .await,
            Err(RemoteError::Timeout { .. })
        ));
        let calls = AtomicUsize::new(0);
        assert!(matches!(
            bounded(Instant::now() + Duration::from_millis(5), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err((RemoteError::Unavailable, true))
            })
            .await,
            Err(RemoteError::Timeout { .. })
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
