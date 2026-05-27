//! Integration tests for [`PostgresSink`]'s `batch_size` chunking against a
//! real Postgres instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container so they are
//! fully isolated and safe to run in parallel.
//!
//! Postgres has no per-request observability hook the way wiremock-backed
//! sinks do. Instead, every test installs a **statement-level `AFTER INSERT`
//! trigger** that increments a counter row in `insert_calls`. Postgres fires
//! statement-level triggers exactly once per `INSERT` statement regardless
//! of how many rows the statement touches, so the counter is a precise
//! proxy for "number of multi-row `INSERT` statements the sink issued".

use faucet_core::Sink;
use faucet_sink_postgres::{PostgresColumnMapping, PostgresSink, PostgresSinkConfig};
use serde_json::{Value, json};
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

/// Install the statement-counter trigger on `target_table`. Each
/// `INSERT INTO target_table ...` statement bumps `insert_calls.calls` by 1
/// (statement-level triggers fire once per statement regardless of row
/// count — exactly what we want to count multi-row INSERTs).
async fn install_insert_counter(pool: &sqlx::PgPool, target_table: &str) {
    sqlx::query("CREATE TABLE insert_calls (calls BIGINT NOT NULL)")
        .execute(pool)
        .await
        .expect("create counter table");
    sqlx::query("INSERT INTO insert_calls (calls) VALUES (0)")
        .execute(pool)
        .await
        .expect("seed counter");
    sqlx::query(
        "CREATE OR REPLACE FUNCTION bump_insert_calls() RETURNS TRIGGER AS $$ \
         BEGIN UPDATE insert_calls SET calls = calls + 1; RETURN NULL; END; \
         $$ LANGUAGE plpgsql",
    )
    .execute(pool)
    .await
    .expect("create trigger fn");
    sqlx::query(&format!(
        "CREATE TRIGGER count_inserts AFTER INSERT ON \"{target_table}\" \
         FOR EACH STATEMENT EXECUTE FUNCTION bump_insert_calls()"
    ))
    .execute(pool)
    .await
    .expect("attach trigger");
}

/// Create the JSONB events table and attach the per-statement INSERT
/// counter.
async fn prepare_jsonb_table(url: &str) {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (data JSONB NOT NULL)")
        .execute(&pool)
        .await
        .expect("create table");
    install_insert_counter(&pool, "events").await;
    pool.close().await;
}

/// Read the current INSERT-statement count.
async fn insert_call_count(url: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT calls FROM insert_calls")
        .fetch_one(&pool)
        .await
        .expect("query counter");
    pool.close().await;
    count
}

/// Build N JSONB records.
fn records(n: usize) -> Vec<Value> {
    (0..n).map(|i| json!({"id": i, "name": "row"})).collect()
}

/// Total rows currently in `events`.
async fn row_count(url: &str) -> i64 {
    let pool = sqlx::PgPool::connect(url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM events")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    count
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_re_chunks_when_input_exceeds_batch_size() {
    let (_container, url) = start_postgres().await;
    prepare_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(1000);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&records(2_500)).await.expect("write");
    assert_eq!(written, 2_500);
    assert_eq!(row_count(&url).await, 2_500);

    // 2_500 / 1_000 = 3 statements (1000 + 1000 + 500).
    let calls = insert_call_count(&url).await;
    assert_eq!(
        calls, 3,
        "write_batch(2_500) with batch_size=1000 must issue exactly 3 INSERT statements; \
         observed {calls}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_single_chunk_when_input_smaller_than_batch_size() {
    let (_container, url) = start_postgres().await;
    prepare_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(1000);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&records(250)).await.expect("write");
    assert_eq!(written, 250);
    assert_eq!(row_count(&url).await, 250);

    let calls = insert_call_count(&url).await;
    assert_eq!(
        calls, 1,
        "write_batch(250) with batch_size=1000 must issue exactly 1 INSERT statement; \
         observed {calls}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_zero_sentinel_sends_whole_slice_in_one_insert() {
    let (_container, url) = start_postgres().await;
    prepare_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(0);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&records(2_500)).await.expect("write");
    assert_eq!(written, 2_500);
    assert_eq!(row_count(&url).await, 2_500);

    let calls = insert_call_count(&url).await;
    assert_eq!(
        calls, 1,
        "batch_size=0 must drain the upstream slice in one INSERT statement; \
         observed {calls}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_empty_input_is_a_noop() {
    let (_container, url) = start_postgres().await;
    prepare_jsonb_table(&url).await;

    let config = PostgresSinkConfig::new(&url, "events").with_batch_size(1000);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&[]).await.expect("write");
    assert_eq!(written, 0);
    assert_eq!(row_count(&url).await, 0);
    assert_eq!(insert_call_count(&url).await, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_auto_map_re_chunks_when_input_exceeds_batch_size() {
    let (_container, url) = start_postgres().await;
    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    // AutoMap binds every value as JSONB, so the table columns must be
    // JSONB too (the sink does not coerce per-column).
    sqlx::query("CREATE TABLE events (id JSONB, name JSONB)")
        .execute(&pool)
        .await
        .expect("create table");
    install_insert_counter(&pool, "events").await;
    pool.close().await;

    let config = PostgresSinkConfig::new(&url, "events")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(1000);
    let sink = PostgresSink::new(config).await.expect("sink new");

    let written = sink.write_batch(&records(2_500)).await.expect("write");
    assert_eq!(written, 2_500);
    assert_eq!(row_count(&url).await, 2_500);

    // 2_500 / 1_000 = 3 multi-row INSERT statements. AutoMap builds a
    // distinct VALUES clause per chunk size (1000-row vs 500-row), but
    // each is still exactly one INSERT statement.
    let calls = insert_call_count(&url).await;
    assert_eq!(
        calls, 3,
        "AutoMap write_batch(2_500) with batch_size=1000 must issue exactly 3 INSERT statements; \
         observed {calls}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_map_chunks_to_respect_postgres_param_limit() {
    // Regression for #78/#21: Postgres caps bind parameters at 65535. A wide
    // table at a large batch (70 cols × 1000 rows = 70_000 binds) in a single
    // INSERT would fail; the sink must sub-chunk and still land every row.
    let (_container, url) = start_postgres().await;
    let cols: Vec<String> = (0..70).map(|i| format!("c{i}")).collect();
    let create = format!(
        "CREATE TABLE wide ({})",
        cols.iter()
            .map(|c| format!("{c} JSONB"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    {
        let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
        sqlx::query(&create)
            .execute(&pool)
            .await
            .expect("create wide table");
        pool.close().await;
    }

    let config = PostgresSinkConfig::new(&url, "wide")
        .column_mapping(PostgresColumnMapping::AutoMap)
        .with_batch_size(0); // one slice → exercises the inner param-limit chunking
    let sink = PostgresSink::new(config).await.expect("sink new");

    let recs: Vec<Value> = (0..1_000)
        .map(|r| {
            let mut m = serde_json::Map::new();
            for (i, c) in cols.iter().enumerate() {
                m.insert(c.clone(), json!(r * 100 + i as i64));
            }
            Value::Object(m)
        })
        .collect();

    let written = sink.write_batch(&recs).await.expect("write");
    assert_eq!(written, 1_000);

    let pool = sqlx::PgPool::connect(&url).await.expect("pool connect");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*)::BIGINT FROM wide")
        .fetch_one(&pool)
        .await
        .expect("count");
    pool.close().await;
    assert_eq!(count, 1_000);
}
