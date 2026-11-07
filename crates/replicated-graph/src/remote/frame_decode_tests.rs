//! Boundary and deadline regression tests for the bounded wire decoder.
use super::*;

pub(super) fn padded(bytes: &[u8], size: usize) -> Vec<u8> {
    assert!(bytes.len() <= size);
    let mut bytes = bytes.to_vec();
    bytes.resize(size, b' ');
    bytes
}

// Keep duplicate fields as bytes: a Value round trip would erase the regression.
pub(super) fn invalid_objects(valid: &[u8]) -> Vec<Vec<u8>> {
    let text = std::str::from_utf8(valid).unwrap();
    assert!(text.starts_with('{'));
    vec![
        format!("{{\"unexpected\":true,{}", &text[1..]).into_bytes(),
        format!("{{\"version\":1,{}", &text[1..]).into_bytes(),
        text.replacen("\"version\":1", "\"version\":\"wrong\"", 1)
            .into_bytes(),
        format!("{text}garbage").into_bytes(),
        // Deeply nested and unterminated, rather than merely a wrong scalar.
        format!("{{\"version\":{}", "[".repeat(160)).into_bytes(),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_request_strict_decode_at_byte_boundary() {
    tokio::spawn(async {
        let request = Request {
            version: VERSION,
            identity: Some(RemoteIdentity { group: 7, node: 9 }),
            operation: Operation::Probe { shard: 11 },
        };
        let valid = serde_json::to_vec(&request).unwrap();
        for size in [4095, 4096, 4097] {
            let decoded: Request = decode_frame_json(&padded(&valid, size)).unwrap();
            assert_eq!(decoded.version, VERSION);
            assert_eq!(decoded.identity, request.identity);
            assert!(matches!(decoded.operation, Operation::Probe { shard: 11 }));
            for invalid in invalid_objects(&valid) {
                assert!(decode_frame_json::<Request>(&padded(&invalid, size)).is_err());
            }
        }
    })
    .await
    .unwrap();
}

async fn sockets() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (server, _) = listener.accept().await.unwrap();
    (client, server)
}

static DECODE_STARTED: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
struct LateDecode;
impl<'de> Deserialize<'de> for LateDecode {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let _ = u64::deserialize(d)?;
        DECODE_STARTED.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(60));
        Ok(Self)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_decode_deadline_both_sides_of_boundary() {
    for (index, size) in [4096, 4097].into_iter().enumerate() {
        let (mut client, mut server) = sockets().await;
        let bytes = padded(b"42", size);
        server.write_u32(size as u32).await.unwrap();
        server.write_all(&bytes).await.unwrap();
        tokio::spawn(async move {
            assert!(matches!(
                read_frame::<LateDecode>(&mut client, Instant::now() + Duration::from_millis(20))
                    .await,
                Err(RemoteError::Timeout { .. })
            ));
        })
        .await
        .unwrap();
        assert_eq!(
            DECODE_STARTED.load(std::sync::atomic::Ordering::SeqCst),
            index + 1
        );
    }
}

struct NeedsWorkerProgress;
impl<'de> Deserialize<'de> for NeedsWorkerProgress {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let _ = u64::deserialize(d)?;
        let (send, receive) = std::sync::mpsc::channel();
        tokio::spawn(async move {
            let _ = send.send(());
        });
        receive
            .recv_timeout(Duration::from_secs(5))
            .map_err(serde::de::Error::custom)?;
        Ok(Self)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn large_decode_hands_off_the_only_worker() {
    // Run on a worker, not the runtime's block_on thread. Without block_in_place
    // the spawned sender cannot run until this bounded synchronous wait fails.
    tokio::spawn(async {
        assert!(decode_frame_json::<NeedsWorkerProgress>(&padded(b"42", 4097)).is_ok());
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn complete_nested_json_keeps_recursion_bound_on_both_paths() {
    tokio::spawn(async {
        for depth in [32, 160] {
            // Balanced, complete JSON: failure of the deep case must not be
            // explained by a wrong typed field or missing closing delimiters.
            let json = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            for size in [4095, 4096, 4097] {
                let result = decode_frame_json::<serde_json::Value>(&padded(json.as_bytes(), size));
                if depth == 32 {
                    assert!(result.is_ok());
                } else {
                    assert!(result.unwrap_err().is_syntax());
                }
            }
        }
    })
    .await
    .unwrap();
}
