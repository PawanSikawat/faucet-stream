//! Integration tests for `MysqlSource::stream_pages` against a real MySQL
//! instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own table so they are fully isolated and safe to run in parallel.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_mysql::{MysqlSource, MysqlSourceConfig};
use futures::StreamExt;
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Instant;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;
use tokio::sync::Semaphore;

/// Bounds concurrent MySQL container startups across all tests in this
/// binary. MySQL 8.x init is heavy (~2-3 GB RSS per container during
/// startup) and starting 6 in parallel exhausts memory on Colima/Docker
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
async fn stream_pages_chunks_rows_into_batch_sized_pages() {
    let (_container, url) = start_mysql().await;
    seed_events(&url, 10_000).await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = MysqlSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut page_count = 0;
    let mut total_rows = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        page_count += 1;
        total_rows += page.records.len();
        assert_eq!(
            page.records.len(),
            1000,
            "every page must be exactly batch_size rows when total is a multiple"
        );
        assert!(
            page.bookmark.is_none(),
            "mysql source has no incremental mode yet; bookmark must be None"
        );
    }

    assert_eq!(page_count, 10, "10_000 / 1000 = 10 pages");
    assert_eq!(total_rows, 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page() {
    let (_container, url) = start_mysql().await;
    seed_events(&url, 2_500).await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = MysqlSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(
        sizes,
        vec![1000, 1000, 500],
        "partial trailing page must hold the remainder"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_emits_single_page() {
    let (_container, url) = start_mysql().await;
    seed_events(&url, 10_000).await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id FROM events ORDER BY id").with_batch_size(0);
    let source = MysqlSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut collected = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        collected.push(page.records.len());
    }
    assert_eq!(
        collected,
        vec![10_000],
        "batch_size = 0 must drain the cursor and emit exactly one page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_empty_result_yields_no_pages() {
    let (_container, url) = start_mysql().await;
    // Create the table but insert no rows.
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE events (id BIGINT PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id FROM events").with_batch_size(DEFAULT_BATCH_SIZE);
    let source = MysqlSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(
        page_count, 0,
        "empty result with no bookmark must yield zero pages"
    );
}

/// Catches the "buffered-then-chunked" anti-pattern.
///
/// MySQL's wire protocol sends all rows from a simple `SELECT` in a single
/// response (without using a server-side cursor), so a `SLEEP`-style
/// server-side timing test would always look identical regardless of
/// client-side streaming.
///
/// Instead, we test the *client-side* signal: the default `stream_pages`
/// impl calls `fetch_with_context_incremental` which materialises every row
/// into a `Vec<Value>` before any page is yielded, while the true-streaming
/// impl parses rows from the wire and yields after `batch_size` are buffered.
///
/// For a large result, the parse-and-buffer cost dominates and the
/// difference is observable: dropping the stream after the first page in the
/// streaming impl avoids parsing the remaining ~99% of rows.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_first_page_completes_without_parsing_full_result() {
    let (_container, url) = start_mysql().await;
    seed_events(&url, 200_000).await;

    // Time a full drain so we have a reference for "parse all rows".
    let config_full =
        MysqlSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = MysqlSource::new(config_full).await.expect("source new");
    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let start = Instant::now();
    let mut full_pages = source.stream_pages(&ctx, 1000);
    while let Some(page) = full_pages.next().await {
        let _ = page.expect("page ok");
    }
    let full_elapsed = start.elapsed();
    drop(full_pages);
    drop(source);

    // Now grab just the first page and drop the stream.
    let config_first =
        MysqlSourceConfig::new(&url, "SELECT id FROM events ORDER BY id").with_batch_size(1000);
    let source = MysqlSource::new(config_first).await.expect("source new");
    let start = Instant::now();
    let mut first_pages = source.stream_pages(&ctx, 1000);
    let first_page = first_pages
        .next()
        .await
        .expect("first page exists")
        .expect("page ok");
    let first_elapsed = start.elapsed();
    drop(first_pages);
    assert_eq!(first_page.records.len(), 1000);

    // First page should arrive in well under half the full-drain time.
    // The default (buffer-then-chunk) impl would parse all 200k rows before
    // the first page, making first_elapsed ≈ full_elapsed.
    assert!(
        first_elapsed * 2 < full_elapsed,
        "first page should arrive without parsing the full result; \
         first page took {first_elapsed:?}, full drain took {full_elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_row_contents() {
    let (_container, url) = start_mysql().await;
    let pool = sqlx::MySqlPool::connect(&url).await.expect("pool connect");
    sqlx::query("CREATE TABLE items (id BIGINT PRIMARY KEY, name VARCHAR(64) NOT NULL)")
        .execute(&pool)
        .await
        .expect("create table");
    sqlx::query("INSERT INTO items (id, name) VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')")
        .execute(&pool)
        .await
        .expect("insert");
    pool.close().await;

    let config =
        MysqlSourceConfig::new(url, "SELECT id, name FROM items ORDER BY id").with_batch_size(2);
    let source = MysqlSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);

    let mut all_records = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        all_records.extend(page.records);
    }

    assert_eq!(all_records.len(), 3);
    assert_eq!(all_records[0]["id"], 1);
    assert_eq!(all_records[0]["name"], "alpha");
    assert_eq!(all_records[2]["name"], "gamma");
}
