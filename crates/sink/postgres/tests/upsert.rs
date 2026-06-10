//! Integration tests for [`PostgresSink`]'s write-mode upsert/delete path
//! against a real Postgres instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.

use faucet_core::{DeleteMarker, Sink, WriteMode, WriteSpec};
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::json;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

/// Start a Postgres container and return both the container handle and a
/// connection URL.
async fn start_postgres() -> (ContainerAsync<Postgres>, String) {
    let image = Postgres::default().with_tag("16-alpine");
    let container: ContainerAsync<Postgres> =
        image.start().await.expect("postgres container start");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("postgres port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    (container, url)
}

/// Create the keyed `kv` table with a PRIMARY KEY on `id`.
async fn create_kv_table(url: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
}

async fn row_count(url: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM kv")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    count
}

async fn name_for_id(url: &str, id: i32) -> Option<String> {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let name: Option<String> = sqlx::query_scalar("SELECT name FROM kv WHERE id = $1")
        .bind(id)
        .fetch_optional(&pool)
        .await
        .expect("read back");
    pool.close().await;
    name
}

fn upsert_sink_config(url: &str) -> PostgresSinkConfig {
    let mut config = PostgresSinkConfig::new(url, "kv")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".into()],
        delete_marker: None,
    };
    config
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_insert_then_update_same_key_keeps_one_row_with_latest_value() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;

    let sink = PostgresSink::new(upsert_sink_config(&url))
        .await
        .expect("sink new");

    // First write inserts the row.
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "alice"})])
        .await
        .expect("first upsert");
    assert_eq!(n, 1);
    assert_eq!(row_count(&url).await, 1);
    assert_eq!(name_for_id(&url, 1).await.as_deref(), Some("alice"));

    // Second write updates the same key in place.
    sink.write_batch(&[json!({"id": 1, "name": "alice2"})])
        .await
        .expect("second upsert");
    assert_eq!(row_count(&url).await, 1, "same key must not create a new row");
    assert_eq!(
        name_for_id(&url, 1).await.as_deref(),
        Some("alice2"),
        "upsert must overwrite with the latest value"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_with_delete_marker_removes_the_row() {
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;

    let mut config = PostgresSinkConfig::new(&url, "kv")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0);
    config.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".into()],
        delete_marker: Some(DeleteMarker {
            field: "__op".into(),
            values: vec!["d".into()],
        }),
    };
    let sink = PostgresSink::new(config).await.expect("sink new");

    // Upsert the row (marker value "u" → not a delete; marker field is stripped).
    sink.write_batch(&[json!({"id": 1, "name": "x", "__op": "u"})])
        .await
        .expect("upsert with marker");
    assert_eq!(row_count(&url).await, 1);
    assert_eq!(name_for_id(&url, 1).await.as_deref(), Some("x"));

    // Marker value "d" → delete the row by key.
    sink.write_batch(&[json!({"id": 1, "__op": "d"})])
        .await
        .expect("delete via marker");
    assert_eq!(row_count(&url).await, 0, "delete-marker must remove the row");
}

#[tokio::test(flavor = "multi_thread")]
async fn upsert_duplicate_keys_in_one_batch_collapse_last_write_wins() {
    // Two records with the same key in ONE batch must collapse to a single
    // upsert (planner dedup), so the ON CONFLICT target is never hit twice in
    // one statement — which Postgres would reject with "ON CONFLICT DO UPDATE
    // command cannot affect row a second time".
    let (_container, url) = start_postgres().await;
    create_kv_table(&url).await;

    let sink = PostgresSink::new(upsert_sink_config(&url))
        .await
        .expect("sink new");

    sink.write_batch(&[
        json!({"id": 1, "name": "old"}),
        json!({"id": 1, "name": "new"}),
    ])
    .await
    .expect("duplicate-key batch upsert");

    assert_eq!(row_count(&url).await, 1);
    assert_eq!(
        name_for_id(&url, 1).await.as_deref(),
        Some("new"),
        "last write within the batch wins"
    );
}
