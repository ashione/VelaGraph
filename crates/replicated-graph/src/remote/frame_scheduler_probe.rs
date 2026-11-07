//! Manual, matched-baseline transport probe; no graph or query specialization.
//! Copy this file unchanged and add its cfg(test) module to the baseline.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

const LANES: usize = 8;
const EXCHANGES_PER_LANE: usize = 2000;
const RUN_LIMIT: Duration = Duration::from_secs(30);

fn payload(size: usize, lane: usize, sequence: usize) -> String {
    let mut text = format!("{lane:02}:{sequence:06}:");
    // ASCII without escapes: JSON string quotes are the only encoding overhead.
    text.extend(std::iter::repeat_n('x', size - 2 - text.len()));
    text
}

fn percentile(sorted: &[u64], percent: usize) -> u64 {
    sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "manual matched baseline/candidate scheduler and loopback load probe"]
async fn generic_frame_scheduler_load_probe() {
    for size in [1024, 4096, 8192] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut sockets = Vec::with_capacity(LANES);
        for _ in 0..LANES {
            let client = TcpStream::connect(address).await.unwrap();
            let (server, _) = listener.accept().await.unwrap();
            client.set_nodelay(true).unwrap();
            server.set_nodelay(true).unwrap();
            sockets.push((client, server));
        }
        assert_eq!(
            serde_json::to_vec(&payload(size, 0, 0)).unwrap().len(),
            size
        );
        let completed = Arc::new(AtomicUsize::new(0));
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let (ready, started) = tokio::sync::oneshot::channel();
        let ticker = tokio::spawn(async move {
            let mut stopped = stopped;
            let mut interval = tokio::time::interval(Duration::from_millis(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            interval.tick().await;
            ready.send(()).unwrap();
            let mut samples = Vec::with_capacity(30_001);
            loop {
                tokio::select! {
                    _ = &mut stopped => break,
                    scheduled = interval.tick() => {
                        samples.push(Instant::now().saturating_duration_since(scheduled).as_micros() as u64);
                    }
                }
            }
            samples
        });
        started.await.unwrap();
        let began = Instant::now();
        let end = began + RUN_LIMIT;
        let mut tasks = JoinSet::new();
        for (lane, (mut client, mut server)) in sockets.into_iter().enumerate() {
            let completed = completed.clone();
            tasks.spawn(async move {
                let exchange = async {
                    let send = async {
                        for sequence in 0..EXCHANGES_PER_LANE {
                            let expected = payload(size, lane, sequence);
                            write_frame(&mut client, &expected, end).await?;
                            let actual: String = read_frame(&mut client, end).await?;
                            assert_eq!(actual, expected, "client lane {lane}, sequence {sequence}");
                            completed.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok::<_, RemoteError>(())
                    };
                    let echo = async {
                        for sequence in 0..EXCHANGES_PER_LANE {
                            let actual: String = read_frame(&mut server, end).await?;
                            assert_eq!(
                                actual,
                                payload(size, lane, sequence),
                                "server lane {lane}, sequence {sequence}"
                            );
                            write_frame(&mut server, &actual, end).await?;
                        }
                        Ok::<_, RemoteError>(())
                    };
                    tokio::try_join!(send, echo).map(|_| ())
                };
                match tokio::time::timeout_at(end, exchange).await {
                    Ok(Ok(())) => false,
                    Ok(Err(RemoteError::Timeout { .. })) | Err(_) => true,
                    Ok(Err(error)) => panic!("lane {lane} failed: {error:?}"),
                }
            });
        }
        let mut timeouts = 0;
        while let Some(result) = tasks.join_next().await {
            timeouts += usize::from(result.unwrap());
        }
        let elapsed = began.elapsed();
        // Let a delayed tick be sampled before stopping the ticker.
        tokio::time::sleep(Duration::from_millis(2)).await;
        stop.send(()).unwrap();
        let mut samples = ticker.await.unwrap();
        assert!(!samples.is_empty());
        samples.sort_unstable();
        let completed = completed.load(Ordering::Relaxed);
        println!(
            "{}",
            serde_json::json!({
                "probe": "generic_frame_scheduler_load",
                "body_bytes": size,
                "workers": 2,
                "lanes": LANES,
                "exchanges_per_lane": EXCHANGES_PER_LANE,
                "completed_exchanges": completed,
                "completed_frames": completed * 2,
                "elapsed_seconds": elapsed.as_secs_f64(),
                "frames_per_second": completed as f64 * 2.0 / elapsed.as_secs_f64(),
                "timed_out_lanes": timeouts,
                "ticker_period_us": 1000,
                "ticker_samples": samples.len(),
                "lateness_p95_us": percentile(&samples, 95),
                "lateness_p99_us": percentile(&samples, 99),
                "lateness_max_us": samples.last().unwrap(),
            })
        );
        assert_eq!(timeouts, 0, "bounded load did not complete");
        assert_eq!(completed, LANES * EXCHANGES_PER_LANE);
    }
}
