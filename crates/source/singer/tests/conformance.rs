//! Proves `faucet-source-singer` upholds the shared connector contract by
//! invoking the reusable `faucet-conformance` battery (checks 1 & 2), driving
//! the dependency-free fake tap.

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
