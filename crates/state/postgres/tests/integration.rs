//! Integration tests for `PostgresStateStore` against a real Postgres instance
//! via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.

use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_core::state::StateStore;
use faucet_state_postgres::PostgresStateStore;
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// Start a Postgres container and return the handle (keeps it alive) plus a
/// connection URL.
async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default().with_tag("16-alpine");
    let container = image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

#[tokio::test(flavor = "multi_thread")]
async fn full_lifecycle_get_put_overwrite_delete() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");

    // ensure_table must be idempotent — calling it twice is a no-op.
    store.ensure_table().await.expect("ensure_table #1");
    store.ensure_table().await.expect("ensure_table #2");

    // A key that was never written reads back as None.
    assert_eq!(store.get("missing").await.expect("get missing"), None);

    // Write then read back the exact value.
    let v1 = json!({"page": 1, "cursor": "abc"});
    store.put("bookmark", &v1).await.expect("put v1");
    assert_eq!(
        store.get("bookmark").await.expect("get v1"),
        Some(v1),
        "get must return the value that was put"
    );

    // Overwriting an existing key (UPSERT) replaces the value.
    let v2 = json!({"page": 2, "cursor": "def"});
    store.put("bookmark", &v2).await.expect("put v2");
    assert_eq!(
        store.get("bookmark").await.expect("get v2"),
        Some(v2),
        "second put must overwrite the first via ON CONFLICT DO UPDATE"
    );

    // Delete removes the key; subsequent get is None.
    store.delete("bookmark").await.expect("delete");
    assert_eq!(
        store.get("bookmark").await.expect("get after delete"),
        None,
        "get after delete must return None"
    );

    // Deleting an absent key is a no-op (no error).
    store.delete("bookmark").await.expect("delete idempotent");
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_json_value_roundtrips_exactly() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");
    store.ensure_table().await.expect("ensure_table");

    let nested = json!({
        "lsn": "0/1A2B3C",
        "tables": ["public.users", "public.orders"],
        "meta": {"committed": true, "rows": 42, "ratio": 0.75, "note": null}
    });
    store.put("cdc", &nested).await.expect("put nested");
    assert_eq!(
        store.get("cdc").await.expect("get nested"),
        Some(nested),
        "nested JSON must survive a JSONB round-trip byte-for-byte"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn check_probe_passes_against_live_db() {
    let (_container, url) = start_postgres().await;
    let store = PostgresStateStore::connect(&url).await.expect("connect");
    store.ensure_table().await.expect("ensure_table");

    let report = store
        .check(&CheckContext::default())
        .await
        .expect("check returns Ok");
    assert_eq!(report.failed_count(), 0, "sentinel probe should pass");
    assert_eq!(report.probes.len(), 1);
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "expected a passing probe, got {:?}",
        report.probes[0].status
    );

    // The sentinel round-trip must leave no residue behind.
    assert_eq!(
        store
            .get(faucet_core::state::DOCTOR_SENTINEL_KEY)
            .await
            .expect("get sentinel"),
        None,
        "check() must clean up its sentinel key"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn from_pool_with_custom_table_name() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    let store =
        PostgresStateStore::from_pool(pool, "custom_state").expect("from_pool with valid name");
    assert_eq!(store.table(), "custom_state");

    store.ensure_table().await.expect("ensure custom table");
    let v = json!({"ok": true});
    store.put("k", &v).await.expect("put");
    assert_eq!(store.get("k").await.expect("get"), Some(v));
}
