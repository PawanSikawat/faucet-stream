//! Proves `faucet-source-singer` upholds the shared connector contract by
//! invoking the reusable `faucet-conformance` battery (checks 1, 2, 3 & 6),
//! driving the dependency-free fake tap.
//!
//! The Singer bridge is single-stream output; it is a source (not a sink), so
//! checks 4/5 do not apply.

use faucet_source_singer::{SingerSource, SingerSourceConfig};

fn fake_tap() -> String {
    format!("{}/tests/fake_taps/fake_tap.sh", env!("CARGO_MANIFEST_DIR"))
}

#[test]
fn conformance_config_schema_valid() {
    let source = SingerSource::new(SingerSourceConfig::new(fake_tap(), "s"));
    faucet_conformance::assert_config_schema_valid(&source);
}

#[tokio::test]
async fn conformance_bounded_memory() {
    let total = 2_000;
    let batch = 100;
    // Fake tap emits `total` records; the bridge pages at the stream_pages hint.
    let source = SingerSource::new(SingerSourceConfig {
        args: vec![
            "--stream".into(),
            "s".into(),
            "--total".into(),
            total.to_string(),
        ],
        state_key: Some("singer_conf".into()),
        ..SingerSourceConfig::new(fake_tap(), "s")
    });
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;
}

#[tokio::test]
async fn conformance_bookmark_roundtrip() {
    // The fake tap emits a final STATE {"last_id": total} on clean completion,
    // and on resume re-emits only the boundary record — so a round-trip through
    // apply_start_bookmark yields strictly fewer records the second time.
    let total = 6;
    let source = SingerSource::new(SingerSourceConfig {
        args: vec![
            "--stream".into(),
            "s".into(),
            "--total".into(),
            total.to_string(),
        ],
        state_key: Some("singer_conf_bookmark".into()),
        ..SingerSourceConfig::new(fake_tap(), "s")
    });
    faucet_conformance::assert_bookmark_roundtrip(&source).await;
}

#[tokio::test]
async fn conformance_errors_not_panics() {
    // A tap executable that does not exist must surface a typed FaucetError when
    // the bridge tries to spawn it — never a panic.
    let source = SingerSource::new(SingerSourceConfig::new(
        "/nonexistent/faucet-conformance-tap-binary",
        "s",
    ));
    faucet_conformance::assert_errors_not_panics(&source).await;
}
