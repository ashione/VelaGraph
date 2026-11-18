//! Opt-in, per-RPC diagnostic correlation. Never an identity or authorization.
use crate::remote::{diagnostic, RemoteError};
use std::{fmt, future::Future};
use tonic::{metadata::MetadataMap, Status};

const KEY: &str = "x-velagraph-diag-call";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct CallId(pub(super) [u8; 16]);
impl fmt::Display for CallId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}
impl CallId {
    pub(super) fn generate(on: bool) -> Option<Self> {
        if !on {
            return None;
        }
        let mut bytes = [0; 16];
        getrandom::fill(&mut bytes).ok()?;
        Some(Self(bytes))
    }
    fn parse(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 32 {
            return None;
        }
        let mut result = [0; 16];
        fn nibble(b: u8) -> Option<u8> {
            match b {
                b'0'..=b'9' => Some(b - b'0'),
                b'a'..=b'f' => Some(b - b'a' + 10),
                _ => None,
            }
        }
        for (pair, out) in bytes.as_chunks::<2>().0.iter().zip(&mut result) {
            *out = nibble(pair[0])? * 16 + nibble(pair[1])?;
        }
        Some(Self(result))
    }
    pub(super) fn insert(self, metadata: &mut MetadataMap) {
        // The formatter produces exactly 32 ASCII hex characters.
        if let Ok(value) = self.to_string().parse() {
            metadata.insert(KEY, value);
        }
    }
}
pub(super) fn incoming(on: bool, metadata: &MetadataMap) -> Option<CallId> {
    if !on {
        return None;
    }
    let mut values = metadata.get_all(KEY).iter();
    let value = values.next()?;
    if values.next().is_some() {
        return None;
    }
    CallId::parse(value.as_encoded_bytes())
}
tokio::task_local! { static CURRENT: Option<CallId>; }
pub(super) async fn scope<T>(call: Option<CallId>, future: impl Future<Output = T>) -> T {
    // Always scope None too: missing/invalid metadata cannot inherit an outer ID.
    CURRENT.scope(call, future).await
}
pub(super) fn current() -> Option<CallId> {
    CURRENT.try_with(|call| *call).ok().flatten()
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Event {
    Removal {
        expired_us: u128,
    },
    Status {
        code: tonic::Code,
        reason: &'static str,
    },
    Elapsed,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Record {
    pub(super) call: CallId,
    pub(super) event: Event,
}
impl fmt::Display for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "phase=observed_fragment_rpc call_id={} ", self.call)?;
        match self.event {
            Event::Removal { expired_us } => write!(
                f,
                "event=matching_expired_entry_removed result=lookup_absent expired_us={expired_us}"
            ),
            Event::Status { code, reason } => {
                write!(f, "event=remote_status code={code:?} reason={reason}")
            }
            Event::Elapsed => write!(f, "event=local_elapsed"),
        }
    }
}
#[cfg(test)]
pub(super) type Capture = std::sync::Arc<std::sync::Mutex<Vec<Record>>>;
#[cfg(test)]
tokio::task_local! { pub(super) static CAPTURE: Capture; pub(super) static CHECK_UNLOCK: Box<dyn Fn() + Send + Sync>; }
fn emit(call: Option<CallId>, event: Event) {
    let Some(call) = call else {
        return;
    };
    let record = Record { call, event };
    #[cfg(test)]
    let _ = CAPTURE.try_with(|sink| sink.lock().unwrap().push(record));
    diagnostic::failure(format_args!("{record}"));
}
pub(super) fn removed(expired_us: u128) {
    #[cfg(test)]
    let _ = CHECK_UNLOCK.try_with(|check| check());
    emit(current(), Event::Removal { expired_us });
}
pub(super) fn map_status(call: Option<CallId>, status: Status) -> RemoteError {
    let code = status.code();
    let error = crate::remote::paged::grpc::remote_status(status);
    emit(
        call,
        Event::Status {
            code,
            reason: diagnostic::kind(&error),
        },
    );
    error
}
pub(super) fn elapsed(call: Option<CallId>) {
    emit(call, Event::Elapsed);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn strict_metadata_and_disabled_generation() {
        assert_eq!(CallId::generate(false), None);
        let id = CallId([17; 16]);
        let mut metadata = MetadataMap::new();
        id.insert(&mut metadata);
        assert_eq!(incoming(true, &metadata), Some(id));
        assert_eq!(incoming(false, &metadata), None);
        metadata.append(KEY, "11111111111111111111111111111111".parse().unwrap());
        assert_eq!(incoming(true, &metadata), None);
        for value in [
            "",
            "a",
            "1111111111111111111111111111111A",
            "111111111111111111111111111111111",
        ] {
            let mut m = MetadataMap::new();
            m.insert(KEY, value.parse().unwrap());
            assert_eq!(incoming(true, &m), None);
        }
    }
    #[tokio::test]
    async fn scopes_are_concurrent_nested_cancel_safe_and_do_not_cross_spawn() {
        let a = CallId([1; 16]);
        let b = CallId([2; 16]);
        let (x, y) = tokio::join!(
            scope(Some(a), async {
                tokio::task::yield_now().await;
                assert_eq!(current(), Some(a));
                scope(None, async { assert_eq!(current(), None) }).await;
                current()
            }),
            scope(Some(b), async {
                tokio::task::yield_now().await;
                current()
            })
        );
        assert_eq!((x, y), (Some(a), Some(b)));
        assert_eq!(current(), None);
        scope(Some(a), async {
            assert_eq!(tokio::spawn(async { current() }).await.unwrap(), None)
        })
        .await;
        let entered = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dropped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        struct DropFlag(std::sync::Arc<std::sync::atomic::AtomicBool>);
        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let e = entered.clone();
        let d = dropped.clone();
        let mut future = Box::pin(scope(Some(a), async move {
            let _drop = DropFlag(d);
            e.store(true, std::sync::atomic::Ordering::SeqCst);
            std::future::pending::<()>().await
        }));
        std::future::poll_fn(|cx| {
            assert!(future.as_mut().poll(cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        assert!(entered.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
        drop(future);
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(current(), None);
    }
    #[tokio::test]
    async fn actual_formatter_and_mapping_do_not_expose_status_data() {
        let sink = Capture::default();
        let id = CallId([7; 16]);
        CAPTURE
            .scope(sink.clone(), async {
                for number in 0..=16 {
                    let code = tonic::Code::from_i32(number);
                    let original = crate::remote::paged::grpc::remote_status(Status::with_details(
                        code,
                        "private payload",
                        b"private details".to_vec().into(),
                    ));
                    let actual = map_status(
                        Some(id),
                        Status::with_details(
                            code,
                            "private payload",
                            b"private details".to_vec().into(),
                        ),
                    );
                    assert_eq!(
                        serde_json::to_value(&actual).unwrap(),
                        serde_json::to_value(&original).unwrap()
                    );
                }
                elapsed(Some(id));
            })
            .await;
        let records = sink.lock().unwrap();
        assert_eq!(records.len(), 18);
        for record in &*records {
            assert_eq!(record.call, id);
            let text = record.to_string();
            assert!(!text.contains("private"));
            assert!(text.contains(&id.to_string()));
        }
        assert_eq!(records.last().unwrap().event, Event::Elapsed);
    }
}
