//! Restore input-type checks, not protection against concurrent path replacement.
use super::*;

async fn backup_at(root: &Path) -> anyhow::Result<()> {
    let state = StateMachine::open(root.join("source-state"))?;
    let logs = crate::DurableLogStore::open(root.join("source-logs"))?;
    state
        .physical_checkpoint(&logs, &root.join("backup"), 0, 1)
        .await
}

fn rejected_before_destination(backup: &Path, destination: &Path) {
    assert!(restore_physical_checkpoint(backup, destination, 0, 1).is_err());
    assert!(!destination.exists(), "invalid input created a destination");
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_symlinked_backup_components() -> anyhow::Result<()> {
    use std::os::unix::fs::symlink;
    let dir = tempfile::tempdir()?;
    backup_at(dir.path()).await?;
    let backup = dir.path().join("backup");
    let destination = dir.path().join("rejected");
    let link = dir.path().join("backup-link");
    symlink(&backup, &link)?;
    for spelling in [link.clone(), link.join(""), link.join(".")] {
        rejected_before_destination(&spelling, &destination);
    }
    for name in ["raft.db", "state.db", "checkpoint.json"] {
        let path = backup.join(name);
        let saved = dir.path().join(format!("saved-{name}"));
        fs::rename(&path, &saved)?;
        symlink(&saved, &path)?;
        rejected_before_destination(&backup, &destination);
        fs::remove_file(&path)?;
        fs::rename(&saved, &path)?;
    }
    // Every substituted path is restored; the exact genuine backup still works.
    for (index, spelling) in [backup.clone(), backup.join(""), backup.join(".")]
        .into_iter()
        .enumerate()
    {
        restore_physical_checkpoint(&spelling, dir.path().join(format!("healthy-{index}")), 0, 1)?;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejects_non_directory_roots_and_databases_and_directory_manifest() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    backup_at(dir.path()).await?;
    let backup = dir.path().join("backup");
    let destination = dir.path().join("rejected");
    let root_file = dir.path().join("root-file");
    fs::write(&root_file, b"not a directory")?;
    rejected_before_destination(&root_file, &destination);
    for name in ["raft.db", "state.db", "checkpoint.json"] {
        let path = backup.join(name);
        let saved = dir.path().join(format!("saved-{name}"));
        fs::rename(&path, &saved)?;
        if name == "checkpoint.json" {
            fs::create_dir(&path)?;
        } else {
            fs::write(&path, b"not a directory")?;
        }
        rejected_before_destination(&backup, &destination);
        if name == "checkpoint.json" {
            fs::remove_dir(&path)?;
        } else {
            fs::remove_file(&path)?;
        }
        fs::rename(&saved, &path)?;
    }
    restore_physical_checkpoint(&backup, dir.path().join("healthy"), 0, 1)?;
    Ok(())
}

// A synchronous FIFO open cannot be cancelled by a Tokio timeout. Run it in a
// killable child so removing the pre-read type check fails, rather than hangs,
// the ordinary suite. No writer is opened: even trying to read would block.
#[cfg(unix)]
#[test]
fn rejects_fifo_manifest_without_opening_it() -> anyhow::Result<()> {
    use std::{
        process::{Child, Command, Stdio},
        time::{Duration, Instant},
    };
    struct Reap(Child);
    impl Drop for Reap {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let result_dir = tempfile::tempdir()?;
    let result_path = result_dir.path().join("completed");
    let module = module_path!().split_once("::").unwrap().1;
    let mut child = Reap(
        Command::new(std::env::current_exe()?)
            .args(["--ignored", "--exact", &format!("{module}::fifo_child")])
            .env("VELAGRAPH_FIFO_TEST_RESULT", &result_path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .spawn()?,
    );
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.0.try_wait()? {
            anyhow::ensure!(status.success(), "FIFO restore subprocess failed: {status}");
            assert_eq!(fs::read(&result_path)?, b"rejected");
            break;
        }
        anyhow::ensure!(
            Instant::now() < end,
            "restore blocked reading FIFO manifest"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

#[cfg(unix)]
#[test]
#[ignore = "child entrypoint, exercised by the bounded FIFO parent test"]
fn fifo_child() -> anyhow::Result<()> {
    let result_path = std::path::PathBuf::from(
        std::env::var_os("VELAGRAPH_FIFO_TEST_RESULT")
            .ok_or_else(|| anyhow::anyhow!("FIFO child must be launched by its parent test"))?,
    );
    // Parent owns this ancestor so watchdog kill also cleans the child fixture.
    let dir = tempfile::tempdir_in(result_path.parent().unwrap())?;
    let backup = dir.path().join("backup");
    fs::create_dir(&backup)?;
    fs::create_dir(backup.join("raft.db"))?;
    fs::create_dir(backup.join("state.db"))?;
    let status = std::process::Command::new("mkfifo")
        .arg(backup.join("checkpoint.json"))
        .status()?;
    anyhow::ensure!(status.success(), "could not create FIFO fixture");
    rejected_before_destination(&backup, &dir.path().join("rejected"));
    fs::write(&result_path, b"rejected")?;
    Ok(())
}
