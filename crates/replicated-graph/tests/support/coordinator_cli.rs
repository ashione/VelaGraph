//! Exercise the shipped CLI through independent client processes, without transport flags.
use anyhow::Context;
use replicated_graph::{remote::distributed::jobs::Output, run_blocking};
use std::{net::SocketAddr, process::Command};

async fn invoke(endpoint: SocketAddr, args: Vec<String>) -> anyhow::Result<std::process::Output> {
    run_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_graph-coordinator"))
            .env("TOKIO_WORKER_THREADS", "2")
            .args(args)
            .args([
                "--endpoint",
                &endpoint.to_string(),
                "--allow-insecure-development",
            ])
            .output()
    })
    .await?
    .context("run coordinator CLI")
}

async fn job(endpoint: SocketAddr, args: Vec<String>) -> anyhow::Result<Output> {
    let result = invoke(endpoint, args).await?;
    anyhow::ensure!(
        result.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&result.stderr)
    );
    Ok(serde_json::from_slice(&result.stdout)?)
}

pub async fn check(
    endpoint: SocketAddr,
    legacy: SocketAddr,
    expected_query: serde_json::Value,
    expected_count: u64,
    expected_rows: Vec<replicated_graph::remote::query::QualifiedElement>,
) -> anyhow::Result<()> {
    let args = vec![
        "query".into(),
        "--partition".into(),
        "0".into(),
        "--query".into(),
        "g.V(1).count()".into(),
    ];
    let result = invoke(endpoint, args.clone()).await?;
    anyhow::ensure!(result.status.success(), "default CLI query failed");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&result.stdout)?,
        expected_query
    );
    let mut wrong_wire = args.clone();
    wrong_wire.extend(["--timeout-ms".into(), "300".into()]);
    assert!(!invoke(legacy, wrong_wire).await?.status.success());
    let mut retired = args;
    retired.push("--grpc".into());
    assert!(!invoke(endpoint, retired).await?.status.success());
    assert!(!invoke(
        endpoint,
        vec!["serve-grpc".into(), "--catalog".into(), "unused".into()]
    )
    .await?
    .status
    .success());

    for rows in [false, true] {
        let mut args = vec![
            if rows {
                "hierarchy-rows-start"
            } else {
                "hierarchy-start"
            }
            .into(),
            "--partition".into(),
            "0".into(),
            "--query".into(),
            if rows {
                "g.V(1).repeat(__.out('knows')).emit().times(2)"
            } else {
                "g.V(1).repeat(__.out('knows')).emit().times(2).count()"
            }
            .into(),
        ];
        if rows {
            args.extend(["--page-size".into(), "1".into()]);
        }
        let mut output = job(endpoint, args).await?;
        let token = match &output {
            Output::Progress { token, .. } | Output::Preparing { token, .. } => token
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            _ => anyhow::bail!("unexpected start response"),
        };
        let mut finished = false;
        for _ in 0..100 {
            let (done, sequence) = match &output {
                Output::Progress {
                    complete,
                    next_sequence,
                    count,
                    ..
                } => {
                    if *complete {
                        assert_eq!(*count, Some(expected_count));
                    }
                    (*complete, *next_sequence)
                }
                Output::Preparing {
                    ready,
                    next_sequence,
                    ..
                } => (*ready, *next_sequence),
                _ => anyhow::bail!("unexpected advance response"),
            };
            if done {
                finished = true;
                break;
            }
            let args = vec![
                "hierarchy-advance".into(),
                "--token".into(),
                token.clone(),
                "--sequence".into(),
                sequence.to_string(),
            ];
            output = job(endpoint, args.clone()).await?;
            assert_eq!(
                serde_json::to_value(&output)?,
                serde_json::to_value(job(endpoint, args).await?)?
            );
        }
        anyhow::ensure!(finished, "CLI job did not finish");
        if rows {
            let mut actual_rows = Vec::new();
            let mut cursor = 0;
            let mut done = false;
            for _ in 0..100 {
                let args = vec![
                    "hierarchy-read".into(),
                    "--token".into(),
                    token.clone(),
                    "--cursor".into(),
                    cursor.to_string(),
                ];
                let page = job(endpoint, args.clone()).await?;
                assert_eq!(
                    serde_json::to_value(&page)?,
                    serde_json::to_value(job(endpoint, args).await?)?
                );
                let Output::Page {
                    done: last,
                    next_cursor,
                    rows,
                    ..
                } = page
                else {
                    anyhow::bail!("unexpected page response");
                };
                assert!(rows.len() <= 1);
                actual_rows.extend(rows);
                if last {
                    done = true;
                    break;
                }
                cursor = next_cursor.context("missing cursor")?;
            }
            anyhow::ensure!(done, "CLI pages did not finish");
            assert_eq!(actual_rows, expected_rows);
        }
        assert!(matches!(
            job(
                endpoint,
                vec!["hierarchy-cancel".into(), "--token".into(), token.clone()]
            )
            .await?,
            Output::Cancelled
        ));
        assert!(!invoke(
            endpoint,
            vec![
                "hierarchy-advance".into(),
                "--token".into(),
                token,
                "--sequence".into(),
                "0".into()
            ]
        )
        .await?
        .status
        .success());
    }
    Ok(())
}
