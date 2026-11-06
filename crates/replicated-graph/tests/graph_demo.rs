//! Real CLI demonstration against three independent single-voter OS processes.
#[test]
fn graph_client_demo_qualified_observed_path() {
    let script =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/graph_client_demo.py");
    let result = std::process::Command::new("python3")
        .arg(script)
        .args([
            "--allow-insecure-development",
            "--node",
            env!("CARGO_BIN_EXE_graph-node"),
            "--client",
            env!("CARGO_BIN_EXE_graph-client"),
        ])
        .output()
        .expect("Python 3 is required for the graph CLI demo integration test");
    assert!(
        result.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    assert!(String::from_utf8_lossy(&result.stdout).contains("Demo verified:"));
}

#[test]
fn graph_client_demo_driver_safety_tests() {
    let scripts = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts");
    let result = std::process::Command::new("python3")
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .args(["-m", "unittest", "discover", "-s"])
        .arg(scripts)
        .args(["-p", "test_graph_client_demo.py"])
        .output()
        .expect("Python 3 is required for the graph demo safety tests");
    assert!(
        result.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}
