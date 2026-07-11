//! Runs the reusable `faucet-conformance` battery against the real JSONL sink.
//!
//! JSONL is an append-only file sink — it advertises no idempotency mechanism,
//! so the battery exercises the **honest branch**:
//! - check 1 `assert_config_schema_valid` (value form, for sinks);
//! - check 5 `assert_capabilities_truthful` — Append works, and the sink does
//!   *not* claim idempotent/keyed dedup (so the pipeline correctly refuses
//!   `delivery: exactly_once` for it).
//!
//! check 4 (`assert_idempotent_replay`) does not apply — see the SQLite sink's
//! conformance test for the real effectively-once path.

use faucet_core::Sink;
use faucet_sink_jsonl::{JsonlSink, JsonlSinkConfig};

fn count_lines(path: &std::path::Path) -> usize {
    std::fs::read_to_string(path)
        .map(|s| s.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(0)
}

#[test]
fn conformance_config_schema_valid() {
    let sink = JsonlSink::new(JsonlSinkConfig::new("/tmp/does-not-matter.jsonl"));
    faucet_conformance::assert_config_schema_valid_value(
        &sink.config_schema(),
        sink.connector_name(),
    );
}

#[tokio::test]
async fn conformance_capabilities_truthful() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("out.jsonl");
    let sink = JsonlSink::new(JsonlSinkConfig::new(&path));
    let sink_ref = &sink;

    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let path = path.clone();
        async move {
            // The sink buffers; flush so the durable line count reflects writes.
            sink_ref.flush().await.expect("flush");
            count_lines(&path)
        }
    })
    .await;

    // The honest branch must have left the append-only sink non-idempotent.
    assert!(!sink.supports_idempotent_writes());
    assert!(!sink.dedups_by_key());
}
