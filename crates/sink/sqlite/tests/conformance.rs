//! Runs the reusable `faucet-conformance` battery against the real SQLite sink,
//! configured for keyed upsert — the load-bearing proof that the battery's
//! effectively-once check (check 4) exercises a genuine idempotent sink, not
//! just a double.
//!
//! - check 1 `assert_config_schema_valid`
//! - check 4 `assert_idempotent_replay` — the atomic-watermark path
//!   (`write_batch_idempotent` + `last_committed_token`) leaves no duplicates.
//! - check 5 `assert_capabilities_truthful` — the advertised idempotent /
//!   upsert / schema-evolution capabilities all hold.
//!
//! Runs against a tempfile SQLite database — no Docker required.

use faucet_core::{Sink, WriteMode, WriteSpec};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

/// A fresh temp database with a keyed table `t(id PK, v)` and an upsert-mode
/// SQLite sink pointed at it.
async fn fresh_sink() -> (TempDir, String, SqliteSink) {
    let dir = TempDir::new().expect("tempdir");
    let url = format!("sqlite://{}?mode=rwc", dir.path().join("t.db").display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let cfg = SqliteSinkConfig {
        database_url: url.clone(),
        table_name: "t".to_string(),
        column_mapping: SqliteColumnMapping::AutoMap,
        batch_size: 1000,
        max_connections: 1,
        write: WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".to_string()],
            delete_marker: None,
        },
    };
    let sink = SqliteSink::new(cfg).await.expect("sink");
    (dir, url, sink)
}

async fn count_rows(url: &str) -> usize {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let row = sqlx::query("SELECT COUNT(*) AS n FROM t")
        .fetch_one(&pool)
        .await
        .expect("count");
    let n: i64 = row.get("n");
    pool.close().await;
    n as usize
}

#[tokio::test]
async fn conformance_config_schema_valid() {
    let (_dir, _url, sink) = fresh_sink().await;
    faucet_conformance::assert_config_schema_valid_value(
        &sink.config_schema(),
        sink.connector_name(),
    );
}

#[tokio::test]
async fn conformance_idempotent_replay() {
    let (_dir, url, sink) = fresh_sink().await;
    faucet_conformance::assert_idempotent_replay(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
}

#[tokio::test]
async fn conformance_capabilities_truthful() {
    let (_dir, url, sink) = fresh_sink().await;
    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
}
