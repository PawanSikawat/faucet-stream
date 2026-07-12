//! Runs the reusable `faucet-conformance` battery against the real PostgreSQL
//! query source. Checks 2 & 6 boot a Postgres container via testcontainers
//! (require Docker); check 1 is offline.
//!
//! PostgreSQL query is full-table (no bookmark), so check 3 does not apply;
//! checks 4/5 are sink-only.

use faucet_source_postgres::{PostgresSource, PostgresSourceConfig};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;

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

async fn seed(url: &str, n: i64) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY, name TEXT)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query(
        "INSERT INTO events (id, name) SELECT g, 'row-' || g FROM generate_series(1, $1) g",
    )
    .bind(n)
    .execute(&pool)
    .await
    .expect("insert rows");
    pool.close().await;
}

#[test]
fn conformance_config_schema_valid() {
    // Offline: the schema is static, so no container is needed.
    let schema = serde_json::to_value(schemars::schema_for!(PostgresSourceConfig)).unwrap();
    faucet_conformance::assert_config_schema_valid_value(&schema, "postgres");
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let total = 5_000;
    let batch = 500;
    let (_container, url) = start_postgres().await;
    seed(&url, total as i64).await;

    let source = PostgresSource::new(
        PostgresSourceConfig::new(url, "SELECT id, name FROM events ORDER BY id")
            .with_batch_size(batch),
    )
    .await
    .expect("source new");
    faucet_conformance::assert_bounded_memory(&source, batch, total).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let (_container, url) = start_postgres().await;
    // Valid connection, but the query hits a table that does not exist — the
    // read path must surface a typed FaucetError, never a panic. (Construction
    // connects eagerly and succeeds; the query only fails at read time.)
    let source = PostgresSource::new(PostgresSourceConfig::new(
        url,
        "SELECT * FROM missing_table",
    ))
    .await
    .expect("source builds; the query only fails at read time");
    faucet_conformance::assert_errors_not_panics(&source).await;
}
