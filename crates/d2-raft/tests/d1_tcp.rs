//! A typed gRPC control client must not receive success for a rejected D1 write.
use d2_raft::{
    send_control, ControlRequest, ControlResponse, D1Command, D1Result, LogCommand, TcpNode,
    TcpNodeConfig,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn typed_control_write_reports_application_rejection_and_capacity_outcomes(
) -> anyhow::Result<()> {
    timeout(Duration::from_secs(15), async {
        let dir = tempfile::tempdir()?;
        let node = TcpNode::start(TcpNodeConfig {
            id: 1,
            raft_addr: "127.0.0.1:0".into(),
            control_addr: "127.0.0.1:0".into(),
            data_dir: dir.path().to_owned(),
        })
        .await?;
        node.bootstrap(BTreeMap::from([(1, node.raft_addr.clone())]))
            .await?;
        loop {
            if node.raft.ensure_linearizable().await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        let response = node
            .raft
            .client_write(LogCommand::D1V1(D1Command::Activate { shard: 7, epoch: 3 }))
            .await?;
        assert_eq!(
            response.data.d1,
            Some(Ok(D1Result::Activated { shard: 7, epoch: 3 }))
        );
        let result = send_control(
            &node.control_addr,
            ControlRequest::Write {
                key: "forbidden".into(),
                value: "value".into(),
            },
        )
        .await;
        assert!(result.is_err());
        assert_eq!(node.state_machine.get_raw(b"forbidden").await, None);
        assert_eq!(node.state_machine.d1_metadata().await, Some((7, 3, 0)));
        assert!(node.state_machine.applied().await.unwrap().index > response.log_id.index);
        for (shard, epoch) in [(7, 3), (8, 3), (7, 4)] {
            let expected = node.state_machine.d1_capacity(shard, epoch).await;
            let actual = send_control(
                &node.control_addr,
                ControlRequest::D1Capacity { shard, epoch },
            )
            .await?;
            let ControlResponse::D1Capacity(actual) = actual else {
                anyhow::bail!("capacity lost its typed result");
            };
            assert_eq!(actual, expected);
        }
        assert!(matches!(
            send_control(
                &node.control_addr,
                ControlRequest::D1SetCapacity {
                    shard: 7,
                    epoch: 3,
                    expected_bytes: 64 * 1024 * 1024,
                    new_bytes: 128 * 1024 * 1024,
                }
            )
            .await?,
            ControlResponse::D1Admin(Ok(D1Result::DedupCapacity {
                bytes_limit: 134_217_728
            }))
        ));
        assert!(matches!(
            send_control(
                &node.control_addr,
                ControlRequest::D1SetCapacity {
                    shard: 7,
                    epoch: 3,
                    expected_bytes: 128 * 1024 * 1024,
                    new_bytes: 64 * 1024 * 1024,
                }
            )
            .await?,
            ControlResponse::D1Admin(Err(d2_raft::D1Error::InvalidDedupCapacity))
        ));
        node.shutdown().await;
        anyhow::Ok(())
    })
    .await?
}
