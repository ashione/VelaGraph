//! Candidate-only offline codec/projection diagnostic; no network or writes.
use anyhow::{anyhow, Context};
use d2_raft::{terminal_prefix::TerminalPrefix, transaction_decisions::Journal};
use std::{fs, time::Instant};
fn main() -> anyhow::Result<()> {
    let path = std::env::args()
        .nth(1)
        .context("usage: proof_sizes EXPLICIT_JOURNAL_JSON")?;
    let input = fs::read(&path)?;
    let journal = Journal::decode(&input).map_err(|e| anyhow!("journal decode: {e:?}"))?;
    assert_eq!(journal.resolved_prefix(), 149);
    let mut full = Vec::new();
    let mut samples = Vec::new();
    for repeat in 1..=3 {
        let start = Instant::now();
        let decoded = Journal::decode(&input).map_err(|e| anyhow!("journal decode: {e:?}"))?;
        let decode_ms = start.elapsed().as_secs_f64() * 1000.;
        let start = Instant::now();
        let encoded = decoded
            .encode()
            .map_err(|e| anyhow!("journal encode: {e:?}"))?;
        let encode_ms = start.elapsed().as_secs_f64() * 1000.;
        assert_eq!(decoded, journal);
        full.push(serde_json::json!({"repeat":repeat,"input_bytes":input.len(),"encoded_bytes":encoded.len(),"decode_validate_replay_ms":decode_ms,"encode_ms":encode_ms}));
        for version in [0, 1, 128, 148, 149] {
            let start = Instant::now();
            let proof = journal
                .terminal_prefix(version)
                .map_err(|e| anyhow!("projection: {e:?}"))?;
            let projection_ms = start.elapsed().as_secs_f64() * 1000.;
            assert_eq!(proof.version(), version);
            let start = Instant::now();
            let bytes = serde_json::to_vec(&proof)?;
            let encode_ms = start.elapsed().as_secs_f64() * 1000.;
            let start = Instant::now();
            let decoded: TerminalPrefix = serde_json::from_slice(&bytes)?;
            let decode_validation_ms = start.elapsed().as_secs_f64() * 1000.;
            let start = Instant::now();
            assert_eq!(proof, decoded);
            let exact_equality_ms = start.elapsed().as_secs_f64() * 1000.;
            assert_eq!(serde_json::to_vec(&decoded)?, bytes);
            samples.push(serde_json::json!({"repeat":repeat,"version":version,"encoded_bytes":bytes.len(),"projection_ms":projection_ms,"encode_ms":encode_ms,"decode_validation_ms":decode_validation_ms,"exact_equality_ms":exact_equality_ms}));
        }
    }
    println!(
        "{}",
        serde_json::to_string_pretty(
            &serde_json::json!({"source_journal":path,"full_journal":full,"terminal_prefix":samples,"scope":"offline exact deterministic projection, serde validation and structural equality; not authentication"})
        )?
    );
    Ok(())
}
