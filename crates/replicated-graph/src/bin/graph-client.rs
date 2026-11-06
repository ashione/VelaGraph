//! One-shot development client. Prepared writes never survive this process.
mod graph_client;

#[tokio::main(worker_threads = 2)]
async fn main() {
    let result = graph_client::run(std::env::args().skip(1).collect()).await;
    let (output, code) = match result {
        Ok(value) => (value, 0),
        Err(error) => {
            eprintln!("graph-client: {error}");
            (graph_client::error_json(&error), 1)
        }
    };
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if serde_json::to_writer(&mut out, &output).is_err()
        || writeln!(out).is_err()
        || out.flush().is_err()
    {
        eprintln!("graph-client: cannot write JSON response");
        std::process::exit(1);
    }
    if code != 0 {
        std::process::exit(code);
    }
}
