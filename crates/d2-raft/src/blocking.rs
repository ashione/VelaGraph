/// Let the multithread runtime replace a worker during synchronous storage or
/// JSON work. The current call stack keeps its locks and admission permits;
/// work is not moved into a detached task.
/// Current-thread executors preserve the original inline behavior.
pub(crate) fn blocking_section<T>(work: impl FnOnce() -> T) -> T {
    if tokio::runtime::Handle::try_current().is_ok_and(|handle| {
        matches!(
            handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        )
    }) {
        tokio::task::block_in_place(work)
    } else {
        work()
    }
}

#[cfg(test)]
mod blocking_section_tests {
    use super::blocking_section;
    use std::time::Duration;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn synchronous_section_does_not_starve_async_worker() {
        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, blocked) = std::sync::mpsc::channel();
        // Run on the sole worker, not the test driver's block_on thread. Without
        // block_in_place, the release task cannot run until recv_timeout fails.
        let work = tokio::spawn(async move {
            blocking_section(|| {
                entered.send(()).unwrap();
                blocked.recv_timeout(Duration::from_secs(5))
            })
        });
        ready.await.unwrap();
        tokio::spawn(async move { release.send(()).unwrap() })
            .await
            .unwrap();
        assert!(work.await.unwrap().is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn current_thread_executor_keeps_inline_compatibility() {
        assert_eq!(blocking_section(|| 42), 42);
    }

    #[test]
    fn no_runtime_keeps_inline_compatibility() {
        assert_eq!(blocking_section(|| 42), 42);
    }
}
