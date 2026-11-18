use std::{path::Path, process::Command};
fn invoke(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_checkpoint-verify"))
        .env_clear()
        .args(args)
        .output()
        .unwrap()
}
fn assert_error(output: std::process::Output, code: &str) {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stderr).unwrap(),
        serde_json::json!({"status":"failed","code":code})
    );
}
fn paths<'a>(config: &'a Path, receipt: &'a Path) -> [&'a str; 4] {
    [
        "--config",
        config.to_str().unwrap(),
        "--receipt",
        receipt.to_str().unwrap(),
    ]
}
#[test]
fn arguments_and_help_are_strict() {
    let help = invoke(&["--help"]);
    assert!(help.status.success());
    assert!(help.stderr.is_empty());
    for args in [
        vec![],
        vec!["--unknown"],
        vec!["--help", "--unknown"],
        vec!["--config"],
        vec!["--config", "a", "--config", "b", "--receipt", "c"],
        vec!["--allow-http-loopback", "--allow-http-loopback"],
    ] {
        assert_error(invoke(&args), "arguments");
    }
}
#[test]
fn malformed_bounded_inputs_fail_without_echoing_paths_or_values() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("sensitive-config.json");
    let receipt = dir.path().join("sensitive-receipt.json");
    std::fs::write(&config, "{\"secret\":\"NEVER_ECHO\"}").unwrap();
    std::fs::write(&receipt, "{}").unwrap();
    assert_error(invoke(&paths(&config, &receipt)), "input_json");
    std::fs::write(&config, vec![b'x'; 65537]).unwrap();
    assert_error(invoke(&paths(&config, &receipt)), "input_limit_or_type");
    assert_error(invoke(&paths(dir.path(), &receipt)), "input_limit_or_type");
    assert_error(
        invoke(&paths(&dir.path().join("missing"), &receipt)),
        "input_io",
    );
}
#[cfg(unix)]
#[test]
fn input_symlinks_reject() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("file");
    let link = dir.path().join("link");
    std::fs::write(&file, "{}").unwrap();
    std::os::unix::fs::symlink(&file, &link).unwrap();
    assert_error(invoke(&paths(&link, &file)), "input_limit_or_type");
}
#[test]
fn insecure_endpoint_requires_explicit_loopback() {
    let dir = tempfile::tempdir().unwrap();
    let config = dir.path().join("config");
    let receipt = dir.path().join("receipt");
    std::fs::write(
        &receipt,
        serde_json::to_vec(
            &serde_json::json!({"checkpoint_id":"a","manifest_sha256":"0".repeat(64)}),
        )
        .unwrap(),
    )
    .unwrap();
    for endpoint in [
        "http://127.0.0.1:9000",
        "https://secret@example.test",
        "http://192.0.2.1:9000",
    ] {
        std::fs::write(&config, serde_json::to_vec(&serde_json::json!({"endpoint":endpoint,"region":"us-east-1","bucket":"test","prefix":"backup","limits":{"chunk_bytes":1024,"manifest_bytes":65536,"files":10,"total_bytes":65536,"timeout_millis":100}})).unwrap()).unwrap();
        assert_error(invoke(&paths(&config, &receipt)), "endpoint_or_config");
    }
}

// Synthetic loopback peers expose actual process traffic, not just parser output.
fn peer(
    redirect: Option<String>,
) -> (
    String,
    std::sync::mpsc::Sender<()>,
    std::thread::JoinHandle<Vec<String>>,
) {
    use std::io::{Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let (stop, stopped) = std::sync::mpsc::channel::<()>();
    let thread = std::thread::spawn(move || {
        let mut requests = Vec::new();
        loop {
            let stopping = !matches!(
                stopped.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            );
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_read_timeout(Some(std::time::Duration::from_millis(200)))
                        .unwrap();
                    let mut data = [0; 8192];
                    if let Ok(n) = stream.read(&mut data) {
                        requests.push(
                            String::from_utf8_lossy(&data[..n])
                                .lines()
                                .next()
                                .unwrap_or("")
                                .to_owned(),
                        );
                        let response = match &redirect {
                            Some(target) => format!("HTTP/1.1 302 Found\r\nLocation: {target}/redirected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"),
                            None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".into(),
                        };
                        let _ = stream.write_all(response.as_bytes());
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if stopping {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(5))
                }
                Err(e) => panic!("peer accept: {e}"),
            }
        }
        requests
    });
    (endpoint, stop, thread)
}
fn network_inputs(dir: &Path, endpoint: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let config = dir.join("config");
    let receipt = dir.join("receipt");
    std::fs::write(&config, serde_json::to_vec(&serde_json::json!({"endpoint":endpoint,"region":"us-east-1","bucket":"test","prefix":"backup","limits":{"chunk_bytes":1024,"manifest_bytes":65536,"files":10,"total_bytes":65536,"timeout_millis":10000}})).unwrap()).unwrap();
    std::fs::write(
        &receipt,
        serde_json::to_vec(
            &serde_json::json!({"checkpoint_id":"a","manifest_sha256":"0".repeat(64)}),
        )
        .unwrap(),
    )
    .unwrap();
    (config, receipt)
}
#[test]
fn ambient_endpoint_addressing_and_proxy_settings_do_not_redirect_gets() {
    let (expected, expected_stop, expected_thread) = peer(None);
    let (unwanted, unwanted_stop, unwanted_thread) = peer(None);
    let dir = tempfile::tempdir().unwrap();
    let (config, receipt) = network_inputs(dir.path(), &expected);
    let output = Command::new(env!("CARGO_BIN_EXE_checkpoint-verify"))
        .env_clear()
        .env("AWS_ACCESS_KEY_ID", "synthetic-key")
        .env("AWS_SECRET_ACCESS_KEY", "synthetic-secret")
        .env("AWS_ENDPOINT_URL_S3", &unwanted)
        .env("AWS_ENDPOINT", &unwanted)
        .env("AWS_BUCKET", "wrong-bucket")
        .env("AWS_VIRTUAL_HOSTED_STYLE_REQUEST", "true")
        .env("HTTP_PROXY", &unwanted)
        .args(paths(&config, &receipt))
        .arg("--allow-http-loopback")
        .output()
        .unwrap();
    drop(expected_stop);
    drop(unwanted_stop);
    assert_error(output, "object_store");
    assert_eq!(
        expected_thread.join().unwrap(),
        vec!["GET /test/backup/checkpoints/a/checkpoint.json HTTP/1.1"]
    );
    assert!(unwanted_thread.join().unwrap().is_empty());
}
#[test]
fn redirects_are_errors_not_permission_to_contact_another_endpoint() {
    let (target, target_stop, target_thread) = peer(None);
    let (source, source_stop, source_thread) = peer(Some(target));
    let dir = tempfile::tempdir().unwrap();
    let (config, receipt) = network_inputs(dir.path(), &source);
    let output = Command::new(env!("CARGO_BIN_EXE_checkpoint-verify"))
        .env_clear()
        .env("AWS_ACCESS_KEY_ID", "synthetic-key")
        .env("AWS_SECRET_ACCESS_KEY", "synthetic-secret")
        .args(paths(&config, &receipt))
        .arg("--allow-http-loopback")
        .output()
        .unwrap();
    drop(source_stop);
    drop(target_stop);
    assert_error(output, "object_store");
    assert_eq!(
        source_thread.join().unwrap(),
        vec!["GET /test/backup/checkpoints/a/checkpoint.json HTTP/1.1"]
    );
    assert!(target_thread.join().unwrap().is_empty());
}

#[tokio::test]
async fn complete_verification_process_issues_only_receipt_bound_gets() {
    use futures::TryStreamExt;
    use object_store::{ObjectStore, ObjectStoreExt};
    use std::io::{Read, Write};
    mod common {
        include!("common/mod.rs");
    }
    let dir = tempfile::tempdir().unwrap();
    let source = dir.path().join("source");
    common::fixture(&source, b"data");
    let backend = std::sync::Arc::new(object_store::memory::InMemory::new());
    let writer = checkpoint_object_store::CheckpointStore::new(
        backend.clone(),
        "backup".into(),
        checkpoint_object_store::Limits::default(),
        checkpoint_object_store::CreateCapability::AtomicCreateVerified,
    )
    .unwrap();
    let receipt_value = writer.upload(&source, "a").await.unwrap();
    let objects: Vec<_> = backend.list(None).try_collect().await.unwrap();
    let mut bodies = std::collections::BTreeMap::new();
    for object in objects {
        bodies.insert(
            format!("/test/{}", object.location),
            backend
                .get(&object.location)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap()
                .to_vec(),
        );
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let (stop, stopped) = std::sync::mpsc::channel::<()>();
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        loop {
            let stopping = !matches!(
                stopped.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            );
            match listener.accept() {
                Ok((mut stream, _)) => {
                    stream
                        .set_read_timeout(Some(std::time::Duration::from_millis(200)))
                        .unwrap();
                    let mut data = [0; 8192];
                    let n = stream.read(&mut data).unwrap();
                    let line = String::from_utf8_lossy(&data[..n])
                        .lines()
                        .next()
                        .unwrap()
                        .to_owned();
                    let parts: Vec<_> = line.split_whitespace().collect();
                    assert_eq!(parts[0], "GET");
                    let body = bodies
                        .get(parts[1])
                        .expect("only known receipt-bound objects");
                    let headers = format!("HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"test-etag\"\r\nLast-Modified: Mon, 16 Nov 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n", body.len());
                    stream.write_all(headers.as_bytes()).unwrap();
                    stream.write_all(body).unwrap();
                    requests.push(line);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if stopping {
                        break;
                    }
                    std::thread::sleep(std::time::Duration::from_millis(5))
                }
                Err(e) => panic!("peer accept: {e}"),
            }
        }
        requests
    });
    let (config, receipt) = network_inputs(dir.path(), &endpoint);
    std::fs::write(&receipt, serde_json::to_vec(&receipt_value).unwrap()).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_checkpoint-verify"))
        .env_clear()
        .env("AWS_ACCESS_KEY_ID", "synthetic-key")
        .env("AWS_SECRET_ACCESS_KEY", "synthetic-secret")
        .args(paths(&config, &receipt))
        .arg("--allow-http-loopback")
        .output()
        .unwrap();
    drop(stop);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({"status":"verified","files":2,"bytes":7,"chunks":2})
    );
    let requests = server.join().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(
        requests[0],
        "GET /test/backup/checkpoints/a/checkpoint.json HTTP/1.1"
    );
    assert_eq!(
        std::fs::read_dir(dir.path()).unwrap().count(),
        3,
        "no staging directory"
    );
}
