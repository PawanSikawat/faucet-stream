//! `faucet-conformance` Tier-1 battery for the Spanner source.
//!
//! Check 1 (config-schema validity) is pure and offline. Checks 2 (bounded
//! memory), 3 (bookmark round-trip), and 6 (errors, not panics) run against
//! the Cloud Spanner emulator via testcontainers and skip cleanly when Docker
//! is unavailable.

mod support;

use faucet_conformance::{
    assert_bookmark_roundtrip, assert_bounded_memory, assert_config_schema_valid_value,
    assert_connector_name_nonempty, assert_errors_not_panics,
};
use faucet_source_spanner::{SpannerReplication, SpannerSource, SpannerSourceConfig};

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SpannerSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-spanner");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// Seed `n` rows into `nums(id INT64, v STRING)` via batched DML.
async fn seed_nums(client: &gcloud_spanner::client::Client, n: usize) {
    for chunk_start in (0..n).step_by(500) {
        let values: Vec<String> = (chunk_start..(chunk_start + 500).min(n))
            .map(|i| format!("({i}, 'v{i}')"))
            .collect();
        support::execute_dml(
            client,
            &format!("INSERT INTO nums (id, v) VALUES {}", values.join(", ")),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "conformance",
        &["CREATE TABLE nums (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "conformance").await;
    seed_nums(&client, 5_000).await;

    let mut cfg = SpannerSourceConfig::new(
        support::PROJECT,
        support::INSTANCE,
        "conformance",
        "SELECT * FROM nums",
    );
    cfg.connection = support::connection(&emu.host, "conformance");
    cfg.batch_size = 500;
    let source = SpannerSource::new(cfg).await.expect("source");

    // Check 10: connector_name() is non-empty (reuses this live instance).
    assert_connector_name_nonempty(&source);

    assert_bounded_memory(&source, 500, 5_000).await;
}

// ── Check 3: bookmark round-trip (Docker) ───────────────────────────────────

/// Drive an incremental source to completion once (capturing the max-`id`
/// bookmark), feed it back via `apply_start_bookmark`, then re-drive with no
/// new rows. The resumed run must replay strictly fewer records (zero — the
/// `@bookmark` pushdown filters them all), proving the bookmark is honoured.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_bookmark_roundtrip() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(
        &emu.host,
        "resume",
        &["CREATE TABLE nums (id INT64 NOT NULL, v STRING(MAX)) PRIMARY KEY (id)"],
    )
    .await;
    let client = support::raw_client(&emu.host, "resume").await;
    seed_nums(&client, 500).await;

    let mut cfg = SpannerSourceConfig::new(
        support::PROJECT,
        support::INSTANCE,
        "resume",
        "SELECT * FROM nums WHERE id > @bookmark",
    );
    cfg.connection = support::connection(&emu.host, "resume");
    cfg.replication = SpannerReplication::Incremental {
        column: "id".into(),
        initial_value: serde_json::json!(-1),
    };
    let source = SpannerSource::new(cfg).await.expect("source");

    assert_bookmark_roundtrip(&source).await;
}

// ── Check 6: errors, not panics (Docker) ────────────────────────────────────

/// Query a table that does not exist — both `fetch_all` and `stream_pages`
/// must surface a typed `FaucetError`, never a panic.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let Some(emu) = support::start_emulator().await else {
        return;
    };
    support::create_database(&emu.host, "errors", &[]).await;

    let mut cfg = SpannerSourceConfig::new(
        support::PROJECT,
        support::INSTANCE,
        "errors",
        "SELECT * FROM does_not_exist",
    );
    cfg.connection = support::connection(&emu.host, "errors");
    let source = SpannerSource::new(cfg).await.expect("source builds");

    assert_errors_not_panics(&source).await;
}
