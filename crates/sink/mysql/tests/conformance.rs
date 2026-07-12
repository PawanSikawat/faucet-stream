//! Runs the reusable `faucet-conformance` battery against the real MySQL sink.
//!
//! - check 1 `assert_config_schema_valid_value` — static, no Docker; passing it
//!   is the Tier-1 (supported) criterion.
//! - check 4 `assert_idempotent_replay` — the atomic-watermark path
//!   (`write_batch_idempotent` + `last_committed_token`) leaves no duplicates.
//! - check 5 `assert_capabilities_truthful` — the advertised idempotent /
//!   upsert / schema-evolution capabilities all hold.
//!
//! Checks 4 and 5 boot a keyed upsert sink against a real MySQL container, so
//! they require Docker.

use faucet_conformance::assert_config_schema_valid_value;
use faucet_core::{WriteMode, WriteSpec};
use faucet_sink_mysql::{MysqlColumnMapping, MysqlSink, MysqlSinkConfig};
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

#[test]
fn conformance_config_schema_valid() {
    let schema =
        serde_json::to_value(schemars::schema_for!(faucet_sink_mysql::MysqlSinkConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "mysql");
}

/// Bounds concurrent MySQL container startups (see `exactly_once_upsert.rs`).
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

/// A fresh container with a keyed table `t(id PK, v)` and an upsert-mode MySQL
/// sink pointed at it. Holds the startup permit for the container's lifetime by
/// returning it alongside the container.
async fn fresh_sink() -> (
    ContainerAsync<Mysql>,
    tokio::sync::SemaphorePermit<'static>,
    String,
    MysqlSink,
) {
    let permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");
    let image = Mysql::default();
    let container: ContainerAsync<Mysql> = image.start().await.expect("mysql container start");
    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");
    let url = format!("mysql://root@127.0.0.1:{port}/test");

    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE t (id BIGINT PRIMARY KEY, v TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let mut cfg = MysqlSinkConfig::new(&url, "t").column_mapping(MysqlColumnMapping::AutoMap);
    cfg.write = WriteSpec {
        write_mode: WriteMode::Upsert,
        key: vec!["id".to_string()],
        delete_marker: None,
    };
    let sink = MysqlSink::new(cfg).await.expect("sink");
    (container, permit, url, sink)
}

async fn count_rows(url: &str) -> usize {
    use sqlx::Row;
    let pool = sqlx::MySqlPool::connect(url).await.expect("pool connect");
    let row = sqlx::query("SELECT COUNT(*) AS n FROM t")
        .fetch_one(&pool)
        .await
        .expect("count");
    let n: i64 = row.get("n");
    pool.close().await;
    n as usize
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_idempotent_replay() {
    let (_container, _permit, url, sink) = fresh_sink().await;
    faucet_conformance::assert_idempotent_replay(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_capabilities_truthful() {
    let (_container, _permit, url, sink) = fresh_sink().await;
    faucet_conformance::assert_capabilities_truthful(&sink, || {
        let url = url.clone();
        async move { count_rows(&url).await }
    })
    .await;
}
