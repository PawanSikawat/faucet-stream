//! `faucet-conformance` battery for the Amazon Redshift sink.
//!
//! Check 1 — the config JSON Schema is a valid, well-formed value. Redshift has
//! no local container image and the sink both loads over the PG wire and stages
//! to S3, so end-to-end load coverage lives in the `#[ignore]`d
//! `tests/integration.rs` (a live cluster + S3 bucket in CI).

use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_redshift::RedshiftSinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "faucet-sink-redshift");
}
