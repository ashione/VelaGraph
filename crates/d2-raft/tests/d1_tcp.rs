//! A legacy control client must not receive success for a rejected D1 write.
use d2_raft::{
    send_control, ControlRequest, D1Command, D1Result, LogCommand, TcpNode, TcpNodeConfig,
};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::time::{sleep, timeout};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_control_write_reports_application_rejection() -> anyhow::Result<()> {
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
        node.shutdown().await;
        anyhow::Ok(())
    })
    .await?
}
