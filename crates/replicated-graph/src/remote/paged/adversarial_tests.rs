//! Scripted TCP peers exercise the complete downloader, not just validators.
use super::*;

fn descriptor() -> SnapshotDescriptor {
    SnapshotDescriptor {
        version: VERSION,
        identity: RemoteIdentity { group: 0, node: 2 },
        incarnation: [3; 16],
        session: [4; 16],
        shard: 7,
        epoch: 1,
        sequence: 8,
        rows: 2,
        bytes: 4,
    }
}

fn page(d: &SnapshotDescriptor, key: u8, more: bool) -> SnapshotPage {
    SnapshotPage {
        descriptor: d.clone(),
        entries: vec![(vec![key], vec![9])],
        next: more.then_some(SnapshotCursor {
            incarnation: d.incarnation,
            session: d.session,
            position: 1,
            token: [5; 16],
        }),
    }
}

async fn peer(
    d: SnapshotDescriptor,
    pages: Vec<SnapshotPage>,
    delay: Duration,
    expect_close: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let end = Instant::now() + Duration::from_secs(5);
        let (mut stream, _) = listener.accept().await.unwrap();
        let request: Request = read_frame(&mut stream, end).await.unwrap();
        assert!(matches!(
            request.operation,
            Operation::OpenSnapshot { shard: 7 }
        ));
        write_frame(
            &mut stream,
            &Response {
                version: VERSION,
                result: Ok(Reply::OpenSnapshot(d.clone())),
            },
            end,
        )
        .await
        .unwrap();
        for (index, page) in pages.into_iter().enumerate() {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request: Request = read_frame(&mut stream, end).await.unwrap();
            match request.operation {
                Operation::SnapshotPage { descriptor, cursor } => {
                    assert_eq!(descriptor, d);
                    assert_eq!(cursor.map(|c| c.position), (index != 0).then_some(index));
                }
                _ => panic!("expected snapshot page, without retry or reopened session"),
            }
            tokio::time::sleep(delay).await;
            let sent = write_frame(
                &mut stream,
                &Response {
                    version: VERSION,
                    result: Ok(Reply::SnapshotPage(page)),
                },
                end,
            )
            .await;
            if expect_close {
                sent.unwrap();
            }
        }
        if expect_close {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request: Request = read_frame(&mut stream, end).await.unwrap();
            match request.operation {
                Operation::CloseSnapshot { descriptor } => assert_eq!(descriptor, d),
                _ => panic!("protocol errors must close, not return partial data or fetch again"),
            }
            write_frame(
                &mut stream,
                &Response {
                    version: VERSION,
                    result: Ok(Reply::CloseSnapshot),
                },
                end,
            )
            .await
            .unwrap();
        }
    });
    (endpoint, task)
}

#[tokio::test]
async fn malformed_downloads_fail_and_close_without_partial_results() {
    // A boundary duplicate is individually valid on both pages, but invalid as
    // a complete snapshot. Descending boundary keys must likewise fail.
    for last_key in [1, 0] {
        let d = descriptor();
        let pages = vec![page(&d, 1, true), page(&d, last_key, false)];
        let (endpoint, server) = peer(d, pages, Duration::ZERO, true).await;
        assert!(matches!(
            paged_snapshot(endpoint, 7, Duration::from_secs(2)).await,
            Err(RemoteError::Protocol)
        ));
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
    // Individually plausible pages still must exactly match the declared total.
    let mut d = descriptor();
    d.bytes = 5;
    let pages = vec![page(&d, 1, true), page(&d, 2, false)];
    let (endpoint, server) = peer(d, pages, Duration::ZERO, true).await;
    assert!(matches!(
        paged_snapshot(endpoint, 7, Duration::from_secs(2)).await,
        Err(RemoteError::Protocol)
    ));
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();

    // Early metadata, cursor and progress violations must stop immediately.
    for fault in 0..3 {
        let d = descriptor();
        let mut bad = page(&d, 1, true);
        match fault {
            0 => bad.descriptor.sequence += 1,
            1 => bad.next = None,
            _ => bad.entries.clear(),
        }
        let (endpoint, server) = peer(d, vec![bad], Duration::ZERO, true).await;
        assert!(matches!(
            paged_snapshot(endpoint, 7, Duration::from_secs(2)).await,
            Err(RemoteError::Protocol)
        ));
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn successive_pages_share_one_deadline() {
    let d = descriptor();
    let pages = vec![page(&d, 1, true), page(&d, 2, false)];
    // Each individual page fits a 400 ms budget; both together cannot. The
    // server asserts that the second page was requested (rather than a retry).
    let (endpoint, server) = peer(d, pages, Duration::from_millis(250), false).await;
    let started = Instant::now();
    assert!(matches!(
        paged_snapshot(endpoint, 7, Duration::from_millis(400)).await,
        Err(RemoteError::Timeout { request_id: None })
    ));
    assert!(started.elapsed() < Duration::from_millis(900));
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn completed_download_retains_admission_until_close_finishes() {
    let mut d = descriptor();
    d.rows = 100;
    d.bytes = 100 * 1024;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = listener.local_addr().unwrap();
    let (closing, reached_close) = tokio::sync::oneshot::channel();
    let (release_close, released) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let end = Instant::now() + Duration::from_secs(5);
        let (mut stream, _) = listener.accept().await.unwrap();
        let request: Request = read_frame(&mut stream, end).await.unwrap();
        assert!(matches!(
            request.operation,
            Operation::OpenSnapshot { shard: 7 }
        ));
        write_frame(
            &mut stream,
            &Response {
                version: VERSION,
                result: Ok(Reply::OpenSnapshot(d.clone())),
            },
            end,
        )
        .await
        .unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let request: Request = read_frame(&mut stream, end).await.unwrap();
        assert!(matches!(
            request.operation,
            Operation::SnapshotPage { cursor: None, .. }
        ));
        let entries = (0..100u32)
            .map(|n| (n.to_be_bytes().to_vec(), vec![0; 1020]))
            .collect();
        write_frame(
            &mut stream,
            &Response {
                version: VERSION,
                result: Ok(Reply::SnapshotPage(SnapshotPage {
                    descriptor: d.clone(),
                    entries,
                    next: None,
                })),
            },
            end,
        )
        .await
        .unwrap();
        let (mut stream, _) = listener.accept().await.unwrap();
        let request: Request = read_frame(&mut stream, end).await.unwrap();
        assert!(
            matches!(request.operation, Operation::CloseSnapshot { descriptor } if descriptor == d)
        );
        closing.send(()).unwrap();
        released.await.unwrap();
        write_frame(
            &mut stream,
            &Response {
                version: VERSION,
                result: Ok(Reply::CloseSnapshot),
            },
            end,
        )
        .await
        .unwrap();
    });
    let download = tokio::spawn(paged_snapshot(endpoint, 7, Duration::from_secs(3)));
    tokio::time::timeout(Duration::from_secs(2), reached_close)
        .await
        .unwrap()
        .unwrap();
    assert!(!download.is_finished());
    // Other concurrently running tests may hold additional permits, so assert
    // the upper bound rather than an exact global count. The completed result
    // remains owned by download while Close is pending.
    assert!(DOWNLOAD_BYTES.available_permits() <= MAX_RETAINED_BYTES - 100 * 1024);
    assert!(DOWNLOAD_ROWS.available_permits() <= MAX_SESSION_ROWS * MAX_SESSIONS - 100);
    release_close.send(()).unwrap();
    assert_eq!(download.await.unwrap().unwrap().entries.len(), 100);
    server.await.unwrap();
}
