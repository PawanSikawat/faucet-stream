//! `faucet-conformance` battery for the Azure Blob source.
//!
//! Check 1 (config-schema validity) is pure and offline and always runs.
//!
//! Check 6 (errors, not panics) points the source at an unreachable endpoint
//! and verifies the read paths surface typed `FaucetError`s rather than
//! panicking. The `object_store` builder is lazy (no I/O at build time), so no
//! container is needed — only the failure path is exercised, which an
//! unreachable host reproduces deterministically.

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_source_azure_blob::{AzureBlobSource, AzureBlobSourceConfig, AzureCredentials};

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(AzureBlobSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-azure-blob");
}

// ── Check 10: connector_name is non-empty (offline, lazy build) ──────────────
/// The `object_store` Azure builder is lazy, so this runs unconditionally with
/// no container.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_connector_name_nonempty() {
    let config = AzureBlobSourceConfig::new("does-not-exist")
        .account("devstoreaccount1")
        .auth(AzureCredentials::AccountKey {
            account_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
        })
        .endpoint("http://127.0.0.1:1")
        .allow_http(true);
    let source = AzureBlobSource::new(config)
        .await
        .expect("source builds lazily");
    faucet_conformance::assert_connector_name_nonempty(&source);
}

// ── Check 6: errors, not panics (no container) ──────────────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    // `http://127.0.0.1:1` refuses connections immediately. Anonymous-style
    // build stays lazy, so `new()` succeeds; the first list/get RPC fails with
    // a typed `FaucetError` on both `fetch_all` and `stream_pages`.
    let config = AzureBlobSourceConfig::new("does-not-exist")
        .account("devstoreaccount1")
        .prefix("data/")
        .auth(AzureCredentials::AccountKey {
            account_key: "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".into(),
        })
        .endpoint("http://127.0.0.1:1")
        .allow_http(true)
        .with_batch_size(250);
    let source = AzureBlobSource::new(config)
        .await
        .expect("source builds lazily");
    assert_errors_not_panics(&source).await;
}
