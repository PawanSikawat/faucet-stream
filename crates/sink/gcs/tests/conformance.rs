//! `faucet-conformance` battery — Check 1 (config schema validity).
//! Passing this battery in CI is the Tier-1 (supported) criterion.
use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_gcs::GcsSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "gcs");
}
