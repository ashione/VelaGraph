use super::*;
use distributed::{
    consistency::{job_with_consistency, query_with_consistency, ReadConsistency::*},
    jobs::{self, Request},
};

fn request() -> DistributedRequest {
    DistributedRequest {
        start_partition: 0,
        text: "g.V(1)".into(),
        timeout_millis: 5000,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consistency_admission_precedes_any_shard_contact() -> anyhow::Result<()> {
    let _serial = PROCESS_TESTS.lock().await;
    // A listening but nonresponding shard makes accidental contact observable.
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let second = TcpListener::bind("127.0.0.1:0").await?;
    let endpoints = [listener.local_addr()?, second.local_addr()?];
    for shard_count in [1, 2] {
        let partitions = (0..shard_count)
            .map(|partition| PartitionRoute {
                partition,
                shard: 120 + u64::from(partition),
                epoch: 1,
                identity: RemoteIdentity {
                    node: 1,
                    group: 120 + u64::from(partition),
                },
                endpoint: endpoints[partition as usize],
            })
            .collect();
        let service =
            CoordinatorService::start("127.0.0.1:0".parse()?, StaticCatalog { partitions }, BUDGET)
                .await?;
        let endpoint = service.local_addr();
        assert!(matches!(
            query_with_consistency(endpoint, request(), GlobalCommittedSnapshot, BUDGET).await?,
            Err(QueryFailure::Unsupported)
        ));
        for job in [
            Request::Start {
                start_partition: 0,
                text: "g.V(1).repeat(__.out()).times(1).count()".into(),
            },
            Request::StartRows {
                start_partition: 0,
                text: "g.V(1).repeat(__.out()).times(1)".into(),
                page_size: 1,
            },
            Request::Advance {
                token: [0; 16],
                sequence: 0,
            },
            Request::Read {
                token: [0; 16],
                cursor: 0,
            },
            Request::Cancel { token: [0; 16] },
        ] {
            assert!(matches!(
                job_with_consistency(endpoint, job, GlobalCommittedSnapshot, BUDGET).await?,
                Err(QueryFailure::Unsupported)
            ));
        }
        assert!(timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err());
        assert!(timeout(Duration::from_millis(30), second.accept())
            .await
            .is_err());
        service.shutdown().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negotiated_queries_keep_legacy_results_and_job_tokens() -> anyhow::Result<()> {
    let _serial = PROCESS_TESTS.lock().await;
    let a = Group::start(0, 120).await?;
    let b = Group::start(1, 130).await?;
    let (ad, bd) = run_blocking(fixture).await??;
    a.seed(ad).await?;
    b.seed(bd).await?;
    let service = CoordinatorService::start(
        "127.0.0.1:0".parse()?,
        StaticCatalog {
            partitions: vec![a.route.clone(), b.route.clone()],
        },
        BUDGET,
    )
    .await?;
    let endpoint = service.local_addr();
    let legacy = distributed::query(endpoint, request(), BUDGET).await??;
    let negotiated =
        query_with_consistency(endpoint, request(), IndependentShardSnapshots, BUDGET).await??;
    assert_eq!(legacy, negotiated);
    assert!(matches!(
        query_with_consistency(endpoint, request(), GlobalCommittedSnapshot, BUDGET).await?,
        Err(QueryFailure::Unsupported)
    ));
    for legacy_start in [true, false] {
        let start = Request::Start {
            start_partition: 0,
            text: "g.V(1).repeat(__.out()).times(1).count()".into(),
        };
        let output = if legacy_start {
            jobs::request(endpoint, start, BUDGET).await??
        } else {
            job_with_consistency(endpoint, start, IndependentShardSnapshots, BUDGET).await??
        };
        let jobs::Output::Progress {
            token,
            next_sequence,
            ..
        } = output
        else {
            anyhow::bail!("expected progress")
        };
        // Strict reads/advances cannot adopt legacy or negotiated weaker tokens.
        for job in [
            Request::Advance {
                token,
                sequence: next_sequence,
            },
            Request::Read { token, cursor: 0 },
            Request::Cancel { token },
        ] {
            assert!(matches!(
                job_with_consistency(endpoint, job, GlobalCommittedSnapshot, BUDGET).await?,
                Err(QueryFailure::Unsupported)
            ));
        }
        assert!(job_with_consistency(
            endpoint,
            Request::Advance {
                token,
                sequence: next_sequence
            },
            IndependentShardSnapshots,
            BUDGET
        )
        .await?
        .is_ok());
        assert!(matches!(
            job_with_consistency(
                endpoint,
                Request::Cancel { token },
                IndependentShardSnapshots,
                BUDGET
            )
            .await??,
            jobs::Output::Cancelled
        ));
    }
    for text in [
        "g.V(1).repeat(__.out()).times(1)",
        "g.V(1).repeat(__.out()).times(1).valueMap('active')",
    ] {
        let mut state = job_with_consistency(
            endpoint,
            Request::StartRows {
                start_partition: 0,
                text: text.into(),
                page_size: 1,
            },
            IndependentShardSnapshots,
            BUDGET,
        )
        .await??;
        let mut advances = 0;
        let token = loop {
            let jobs::Output::Preparing {
                token,
                next_sequence,
                ready,
                ..
            } = state
            else {
                anyhow::bail!("expected preparing rows");
            };
            if ready {
                break token;
            }
            advances += 1;
            ensure!(
                advances <= 100,
                "rows job did not complete within 100 advances"
            );
            state = job_with_consistency(
                endpoint,
                Request::Advance {
                    token,
                    sequence: next_sequence,
                },
                IndependentShardSnapshots,
                BUDGET,
            )
            .await??;
        };
        assert!(matches!(
            job_with_consistency(
                endpoint,
                Request::Read { token, cursor: 0 },
                GlobalCommittedSnapshot,
                BUDGET
            )
            .await?,
            Err(QueryFailure::Unsupported)
        ));
        let read = Request::Read { token, cursor: 0 };
        let explicit =
            job_with_consistency(endpoint, read.clone(), IndependentShardSnapshots, BUDGET)
                .await??;
        let legacy = jobs::request(endpoint, read, BUDGET).await??;
        assert_eq!(
            serde_json::to_value(explicit)?,
            serde_json::to_value(legacy)?
        );
        job_with_consistency(
            endpoint,
            Request::Cancel { token },
            IndependentShardSnapshots,
            BUDGET,
        )
        .await??;
    }
    service.shutdown().await;
    Ok(())
}
