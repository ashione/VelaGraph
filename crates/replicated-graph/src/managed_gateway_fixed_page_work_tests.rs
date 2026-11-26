use super::*;
use crate::remote::{query::QualifiedElement, FixedReadWork};

const PAGE_SIZE: u16 = 3;
const TEXT: &str = "g.V(1,1).out('spread').path()";

async fn send(f: &ScalingFixture, gateway: &Gateway, operation: Operation) -> Reply {
    request(
        gateway.local_addr(),
        f.config.binding.clone(),
        operation,
        DEADLINE,
    )
    .await
    .unwrap()
}

fn edge_write(request: u8, id: u64, target: u32) -> Operation {
    Operation::Submit(Write {
        external_id: [request; 16],
        mutation: Mutation::CreateEdge {
            edge: EdgeData {
                id,
                from: entity(20, 1),
                to: entity(target, 1),
                label: "spread".into(),
                properties: BTreeMap::new(),
            },
        },
    })
}

fn expected(edges: &[(u64, u32)]) -> Vec<QueryRow> {
    (0..2)
        .flat_map(|_| {
            edges.iter().map(|&(id, partition)| {
                QueryRow::Path(vec![
                    QualifiedElement::Vertex {
                        partition: 20,
                        id: 1,
                    },
                    QualifiedElement::Edge { partition: 20, id },
                    QualifiedElement::Vertex { partition, id: 1 },
                ])
            })
        })
        .collect()
}

fn reads(f: &ScalingFixture) -> Vec<FixedReadWork> {
    f.graphs.iter().map(GraphService::fixed_read_work).collect()
}

fn check_page(page: &QueryPage, offset: usize, rows: &[QueryRow]) {
    let end = (offset + usize::from(PAGE_SIZE)).min(rows.len());
    assert_eq!(page.offset as usize, offset);
    assert_eq!(page.total as usize, rows.len());
    assert_eq!(page.data.rows, rows[offset..end]);
    assert_eq!(page.next.is_some(), end < rows.len());
}

async fn open(
    f: &ScalingFixture,
    g: &Gateway,
    version: u64,
    rows: &[QueryRow],
) -> (LocalSession, QueryPage) {
    let before = reads(f);
    let (session, echoed, first) = ready(
        send(
            f,
            g,
            Operation::OpenLocalQuery {
                version: Some(version),
                ttl_millis: 60_000,
                query: query(20, TEXT),
                page_size: PAGE_SIZE,
            },
        )
        .await,
    );
    assert_eq!(session.version, version);
    assert_eq!(echoed, query(20, TEXT));
    check_page(&first, 0, rows);

    let mut gets = 0;
    let mut scans = 0;
    let mut scan_rows = 0;
    let mut replay = 0;
    for ((prev, now), member) in before
        .iter()
        .zip(reads(f))
        .zip(&f.config.binding.cohort.members)
    {
        assert_eq!(now.captures - prev.captures, 1);
        assert!(
            now.gets > prev.gets,
            "target validation on shard {}",
            member.shard
        );
        gets += now.gets - prev.gets;
        scans += now.scans - prev.scans;
        scan_rows += now.scan_rows - prev.scan_rows;
        replay += now.replay_operations - prev.replay_operations;
    }
    // Two explicit roots. Each needs a root and adjacency-owner lookup, then
    // each live edge occurrence checks its record, outgoing index and source
    // vertex on the owner, then the incoming index and target vertex.
    assert!(gets <= 4 + 5 * rows.len(), "{gets} gets at V{version}");
    assert!((1..=2).contains(&scans));
    assert!((1..=rows.len()).contains(&scan_rows));
    // Empty baseline, one key per vertex and three graph records per edge.
    assert_eq!(replay, 3 + 3 * (rows.len() / 2));
    eprintln!(
        "Open V{version}: replay_ops={replay} gets={gets} scans={scans} scan_rows={scan_rows}"
    );
    (session, first)
}

async fn fetch(
    f: &ScalingFixture,
    g: &Gateway,
    session: &LocalSession,
    cursor: Option<PageCursor>,
) -> QueryPage {
    let Reply::FetchLocalQuery {
        session: echoed,
        query: original,
        cursor: returned,
        page_size,
        page,
    } = send(
        f,
        g,
        Operation::FetchLocalQuery {
            session: session.clone(),
            query: query(20, TEXT),
            cursor: cursor.clone(),
            page_size: PAGE_SIZE,
        },
    )
    .await
    else {
        panic!("expected retained query page")
    };
    assert_eq!(
        (&echoed, returned, original, page_size),
        (session, cursor, query(20, TEXT), PAGE_SIZE)
    );
    page
}

async fn verify_retained(
    f: &ScalingFixture,
    g: &Gateway,
    session: LocalSession,
    first: QueryPage,
    rows: &[QueryRow],
) {
    let before = reads(f);
    let mut cursor = None;
    for offset in (0..rows.len()).step_by(PAGE_SIZE.into()) {
        let page = fetch(f, g, &session, cursor).await;
        check_page(&page, offset, rows);
        if offset == 0 {
            assert_eq!(page, first);
        }
        assert_eq!(
            reads(f),
            before,
            "unexpected participant work fetching offset {offset} at V{}",
            session.version
        );
        cursor = page.next;
    }
    send(f, g, Operation::CloseLocalQuery { session }).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_shard_fixed_pages_match_model_and_bound_read_work_during_write() {
    let _gate = TEST_GATE.lock().await;
    let _sessions = crate::remote::distributed::fixed_execution::lock_session_fixtures().await;
    let shards = [20, 21, 22];
    let f = ScalingFixture::for_shards(&shards).await;
    let g = f.gateway(3).await;
    for (i, shard) in shards.into_iter().enumerate() {
        committed(
            &send(
                &f,
                &g,
                Operation::Submit(create(1 + i as u8, shard as u32, 1)),
            )
            .await,
        );
    }
    // Insert out of edge-ID order. Every target has numeric ID 1, including the source.
    for (i, (id, target)) in [(32, 21), (30, 22), (31, 20)].into_iter().enumerate() {
        assert_eq!(
            committed(&send(&f, &g, edge_write(4 + i as u8, id, target)).await).1,
            4 + i as u64
        );
    }
    let old = expected(&[(30, 22), (31, 20), (32, 21)]);
    let (session, first) = open(&f, &g, 6, &old).await;
    let before = reads(&f);
    let (next, written) = tokio::join!(
        fetch(&f, &g, &session, first.next.clone()),
        send(&f, &g, edge_write(7, 29, 22)),
    );
    assert_eq!(committed(&written).1, 7);
    check_page(&next, usize::from(PAGE_SIZE), &old);
    assert_eq!(reads(&f), before);
    verify_retained(&f, &g, session, first, &old).await;

    // Reopening at V6 forces another capture after V7 is visible. The new
    // parallel edge sorts first and moves a path across each V7 page boundary.
    let latest = expected(&[(29, 22), (30, 22), (31, 20), (32, 21)]);
    for (v, rows) in [(6, &old), (7, &latest)] {
        let (session, first) = open(&f, &g, v, rows).await;
        verify_retained(&f, &g, session, first, rows).await;
    }
    g.shutdown().await;
    f.stop().await;
}
