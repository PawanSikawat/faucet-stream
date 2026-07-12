//! `faucet-conformance` battery for the Iceberg sink.
//!
//! Only **check 1** (`assert_config_schema_valid_value`) runs here, so the
//! Iceberg sink stays **Tier-2** for now. The effectively-once check (check 4)
//! does not fit this sink cleanly on `iceberg-rust` 0.9.1: the sink is
//! append-only and only materialises rows on `flush()` (it buffers in
//! `write_batch_idempotent`), while the battery drives an interleaved
//! write→count→write→count sequence and never flushes. Bridging that with a
//! flush-inside-count shim makes the cumulative `total-records` snapshot summary
//! non-monotonic across the two commits, so the "forward progress after a new
//! token" assertion cannot be satisfied deterministically. Promoting Iceberg to
//! Tier-1 is tracked separately (needs an upstream append/commit model that the
//! watermark-replay check can observe without a terminal flush).
use faucet_conformance::assert_config_schema_valid_value;

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(
        faucet_sink_iceberg::IcebergSinkConfig
    ))
    .unwrap();
    assert_config_schema_valid_value(&schema, "iceberg");
}
