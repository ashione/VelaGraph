//! Ordinary-process-test evidence only; no production API or extra subprocesses.
use anyhow::Context;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::ExitStatus,
    time::Instant,
};

pub struct Evidence {
    root: PathBuf,
    test: PathBuf,
    node: PathBuf,
    test_hash: String,
    node_hash: String,
}
fn hash(path: &Path) -> io::Result<String> {
    let mut input = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 65536];
    loop {
        let n = input.read(&mut buffer)?;
        if n == 0 {
            break;
        }
        hash.update(&buffer[..n]);
    }
    Ok(format!("{:x}", hash.finalize()))
}
fn create(path: &Path) -> io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
}
fn save(path: &Path, value: serde_json::Value) -> anyhow::Result<()> {
    serde_json::to_writer(create(path)?, &value)?;
    Ok(())
}
impl Evidence {
    pub fn prepare() -> anyhow::Result<Option<Self>> {
        let Some(base) = std::env::var_os("VELAGRAPH_REMOTE_EVIDENCE_DIR") else {
            return Ok(None);
        };
        let start = Instant::now();
        let root = PathBuf::from(base).join("remote_process");
        fs::create_dir(&root).context("create fresh remote-process evidence directory")?;
        let test = std::env::current_exe()?;
        let node = PathBuf::from(env!("CARGO_BIN_EXE_graph-node"));
        let test_hash = hash(&test)?;
        let node_hash = hash(&node)?;
        for (source, name) in [(&test, "test.elf"), (&node, "graph-node.elf")] {
            io::copy(&mut File::open(source)?, &mut create(&root.join(name))?)?;
        }
        anyhow::ensure!(
            hash(&root.join("test.elf"))? == test_hash
                && hash(&root.join("graph-node.elf"))? == node_hash,
            "executable copy hash mismatch"
        );
        let lock = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../Cargo.lock");
        save(
            &root.join("provenance-before.json"),
            json!({"test_path":test,"test_sha256":test_hash,"baked_graph_node_path":node,"graph_node_sha256":node_hash,"cargo_lock_sha256":hash(&lock)?,"setup_elapsed_ms":start.elapsed().as_millis(),"within_original_outer_timeout":true}),
        )?;
        Ok(Some(Self {
            root,
            test,
            node,
            test_hash,
            node_hash,
        }))
    }
    pub fn streams(&self, index: usize) -> io::Result<(File, File)> {
        Ok((
            create(&self.root.join(format!("node-{index}.stdout.log")))?,
            create(&self.root.join(format!("node-{index}.stderr.log")))?,
        ))
    }
    pub fn record(&self, name: &str, value: serde_json::Value) {
        if save(&self.root.join(name), value).is_err() {
            let _ = writeln!(
                io::stderr(),
                "remote evidence metadata write failed; capture incomplete"
            );
        }
    }
    pub fn verify(&self) -> anyhow::Result<()> {
        let test = hash(&self.test)?;
        let node = hash(&self.node)?;
        let same = test == self.test_hash && node == self.node_hash;
        let missing: Vec<_> = (1..=3)
            .flat_map(|index| {
                [
                    format!("node-{index}.stdout.log"),
                    format!("node-{index}.stderr.log"),
                    format!("node-{index}-launch.json"),
                    format!("node-{index}-pre-cleanup.json"),
                    format!("node-{index}-post-cleanup.json"),
                ]
            })
            .filter(|name| !self.root.join(name).is_file())
            .collect();
        save(
            &self.root.join("capture-status.json"),
            json!({"complete":missing.is_empty(),"missing":missing}),
        )?;

        save(
            &self.root.join("provenance-after.json"),
            json!({"test_sha256":test,"graph_node_sha256":node,"matches_before":same}),
        )?;
        anyhow::ensure!(
            missing.is_empty(),
            "child evidence files missing; see capture-status.json"
        );
        anyhow::ensure!(same, "executed binaries changed during process test");
        Ok(())
    }
}
// Only exit code/signal or error category; no arbitrary error Debug/string.
pub fn status(value: &io::Result<Option<ExitStatus>>) -> serde_json::Value {
    match value {
        Ok(None) => json!({"running":true}),
        Ok(Some(status)) => {
            #[cfg(unix)]
            let signal = {
                use std::os::unix::process::ExitStatusExt;
                status.signal()
            };
            #[cfg(not(unix))]
            let signal: Option<i32> = None;
            json!({"running":false,"code":status.code(),"signal":signal})
        }
        Err(error) => {
            json!({"status_error_kind":format!("{:?}",error.kind()),"os":error.raw_os_error()})
        }
    }
}

trait ChildLifecycle {
    fn pre(&mut self) -> io::Result<Option<ExitStatus>>;
    fn kill(&mut self) -> io::Result<()>;
    fn wait(&mut self) -> io::Result<ExitStatus>;
}
impl ChildLifecycle for std::process::Child {
    fn pre(&mut self) -> io::Result<Option<ExitStatus>> {
        self.try_wait()
    }
    fn kill(&mut self) -> io::Result<()> {
        std::process::Child::kill(self)
    }
    fn wait(&mut self) -> io::Result<ExitStatus> {
        std::process::Child::wait(self)
    }
}
fn cleanup(child: &mut impl ChildLifecycle, mut record: impl FnMut(&str, serde_json::Value)) {
    record("pre-cleanup", status(&child.pre()));
    let _ = child.kill();
    record("post-cleanup", status(&child.wait().map(Some)));
}
pub fn cleanup_observed(child: &mut std::process::Child, index: usize, evidence: &Evidence) {
    cleanup(child, |phase, value| {
        evidence.record(&format!("node-{index}-{phase}.json"), value)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn streaming_hash_and_create_new_preserve_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x");
        create(&path).unwrap().write_all(b"abc").unwrap();
        assert_eq!(
            hash(&path).unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert!(create(&path).is_err());
        assert_eq!(fs::read(path).unwrap(), b"abc");
        assert_eq!(status(&Ok(None)), json!({"running":true}));
        assert!(status(&Err(io::ErrorKind::BrokenPipe.into()))
            .get("status_error_kind")
            .is_some());
    }
    #[test]
    fn cleanup_observes_before_kill_and_wait_even_when_actions_error() {
        use std::{cell::RefCell, rc::Rc};
        struct Fake(Rc<RefCell<Vec<&'static str>>>);
        impl ChildLifecycle for Fake {
            fn pre(&mut self) -> io::Result<Option<ExitStatus>> {
                self.0.borrow_mut().push("pre");
                Ok(None)
            }
            fn kill(&mut self) -> io::Result<()> {
                self.0.borrow_mut().push("kill");
                Err(io::ErrorKind::PermissionDenied.into())
            }
            fn wait(&mut self) -> io::Result<ExitStatus> {
                self.0.borrow_mut().push("wait");
                Err(io::ErrorKind::Other.into())
            }
        }
        let events = Rc::new(RefCell::new(Vec::new()));
        cleanup(&mut Fake(events.clone()), |phase, value| {
            if phase == "pre-cleanup" {
                assert_eq!(value["running"], true);
                events.borrow_mut().push("record_pre");
            } else {
                assert!(value.get("status_error_kind").is_some());
                events.borrow_mut().push("record_post");
            }
        });
        assert_eq!(
            *events.borrow(),
            vec!["pre", "record_pre", "kill", "wait", "record_post"]
        );
    }
    #[test]
    fn missing_child_metadata_is_retained_and_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let executable = dir.path().join("binary");
        fs::write(&executable, b"unchanged").unwrap();
        let evidence = Evidence {
            root: dir.path().to_path_buf(),
            test: executable.clone(),
            node: executable.clone(),
            test_hash: hash(&executable).unwrap(),
            node_hash: hash(&executable).unwrap(),
        };
        assert!(evidence.verify().is_err());
        let capture: serde_json::Value =
            serde_json::from_slice(&fs::read(dir.path().join("capture-status.json")).unwrap())
                .unwrap();
        assert_eq!(capture["complete"], false);
        assert_eq!(capture["missing"].as_array().unwrap().len(), 15);
        assert!(dir.path().join("provenance-after.json").is_file());
    }
}
