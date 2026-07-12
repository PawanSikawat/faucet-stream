//! `faucet-conformance` battery for the MySQL source.
//!
//! Check 1 (config-schema validity) is pure and offline. Check 2
//! (bounded-memory streaming) boots a real MySQL via testcontainers and so
//! requires Docker — it runs in CI alongside the other integration tests.

use faucet_conformance::assert_config_schema_valid_value;
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use std::sync::OnceLock;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(MysqlSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-mysql");
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

/// Bounds concurrent MySQL container startups across all tests in this
/// binary. MySQL 8.x init is heavy (~2-3 GB RSS per container during
/// startup) and starting several in parallel exhausts memory on Colima/Docker
/// Desktop, surfacing as random "Failed to start mysqld daemon" errors. We
/// allow at most two simultaneous startups; once a container is running it
/// is steady-state cheap, so the cap only serialises the spin-up window.
fn startup_limit() -> &'static Semaphore {
    static SEM: OnceLock<Semaphore> = OnceLock::new();
    SEM.get_or_init(|| Semaphore::new(2))
}

/// Start a MySQL container and return both the container handle and a
/// connection URL. The container is kept alive by the returned handle; drop
/// it to stop the container.
async fn start_mysql() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
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
    (container, url)
}

/// Create a single-column `events` table and insert `n` rows of `(id)` with
/// values `1..=n`. Uses a recursive CTE for a fast bulk insert — avoids
/// per-row round trips.
async fn seed_events(url: &str, n: i64) {
    use sqlx::Connection;

    let mut conn = sqlx::MySqlConnection::connect(url)
        .await
        .expect("connect for seed");
    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY)")
        .execute(&mut conn)
        .await
        .expect("create table");
    // MySQL 8 supports recursive CTEs; bump cte_max_recursion_depth so large
    // n works. The SET SESSION must run on the same connection as the
    // INSERT, hence the explicit single-connection seeding.
    sqlx::query("SET SESSION cte_max_recursion_depth = 1000000")
        .execute(&mut conn)
        .await
        .expect("set cte depth");
    sqlx::query(
        "INSERT INTO events (id) \
         WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < ?) \
         SELECT n FROM seq",
    )
    .bind(n)
    .execute(&mut conn)
    .await
    .expect("insert rows");
    conn.close().await.expect("close conn");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let (_container, url) = start_mysql().await;
    seed_events(&url, 5_000).await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(250);
    let source = MysqlSource::new(config).await.expect("source new");

    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;
    // _container stays alive to here
}

// ── Check 6: errors, not panics (Docker) ────────────────────────────────────

/// The source builds against a live container (so `new()` — which eagerly
/// connects the pool — succeeds), but the configured query targets a table that
/// does not exist, so the failure surfaces at read time. The battery asserts
/// both `fetch_all` and `stream_pages` return a typed `FaucetError` rather than
/// panicking.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let (_container, url) = start_mysql().await;

    let config = MysqlSourceConfig::new(url, "SELECT id FROM missing_table");
    let source = MysqlSource::new(config)
        .await
        .expect("builds; query fails at read");

    faucet_conformance::assert_errors_not_panics(&source).await;
    // _container stays alive to here
}
