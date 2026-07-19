//! Integration tests against a real ClickHouse server in Docker.
//!
//! These **auto-start** a `clickhouse/clickhouse-server` container via
//! `testcontainers` (no env var, not `#[ignore]`d), so they run in CI wherever
//! Docker is present and count toward patch coverage. They exercise the HTTP
//! I/O paths in `src/sink.rs` — the `INSERT … FORMAT JSONEachRow` body builder,
//! the async-insert query-param toggle, `batch_size` re-chunking, `flush`, and
//! the live `check()` probe — that the pure unit tests can't reach. Mirrors the
//! postgres/mssql integration-test pattern.
//!
//! Run explicitly with:
//! `cargo test -p faucet-sink-clickhouse --test integration`.

use faucet_core::Sink as _;
use faucet_core::check::{CheckContext, ProbeStatus};
use faucet_sink_clickhouse::{ClickHouseSink, ClickHouseSinkConfig};
use serde_json::{Value, json};
use testcontainers_modules::clickhouse::ClickHouse;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

// `cargo test` runs a binary's tests in parallel; serialize so at most one
// container runs at a time on a small CI runner. Mirrors the mssql/postgres
// integration suites.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_clickhouse() -> (ContainerAsync<ClickHouse>, String) {
    let container = ClickHouse::default()
        .start()
        .await
        .expect("start clickhouse container");
    let port = container
        .get_host_port_ipv4(8123)
        .await
        .expect("clickhouse host port");
    let base = format!("http://127.0.0.1:{port}");
    (container, base)
}

/// POST a statement over the HTTP interface, asserting a 2xx. Used to run DDL
/// out-of-band from the sink under test.
async fn http_exec(base: &str, sql: &str) {
    let resp = reqwest::Client::new()
        .post(base)
        .body(sql.to_string())
        .send()
        .await
        .expect("http exec send");
    let status = resp.status();
    let body = resp.text().await.unwrap_or_default();
    assert!(
        status.is_success(),
        "statement failed ({status}): {sql}\n{body}"
    );
}

/// Read rows back with `FORMAT JSONEachRow` and decode each line into a
/// [`Value`]. The independent read path confirms the sink's writes landed.
async fn read_rows(base: &str, sql: &str) -> Vec<Value> {
    let body = reqwest::Client::new()
        .post(base)
        .body(format!("{sql} FORMAT JSONEachRow"))
        .send()
        .await
        .expect("query send")
        .text()
        .await
        .expect("query body");
    body.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("decode JSONEachRow line"))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_inserts_rows_and_rechunks() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;

    http_exec(
        &base,
        "CREATE TABLE events (id UInt32, name String, score Float64) \
         ENGINE = MergeTree ORDER BY id",
    )
    .await;

    // Single-chunk write (default batch_size) exercises the JSONEachRow body
    // builder + a plain (non-async) INSERT request, and type round-tripping.
    let sink = ClickHouseSink::new(ClickHouseSinkConfig::new(&base, "events")).expect("sink");
    let rows = vec![
        json!({"id": 1, "name": "alice", "score": 1.5}),
        json!({"id": 2, "name": "bob", "score": 2.25}),
        json!({"id": 3, "name": "carol", "score": 3.0}),
    ];
    let written = sink.write_batch(&rows).await.expect("write_batch");
    assert_eq!(written, 3);
    sink.flush().await.expect("flush");

    let back = read_rows(&base, "SELECT id, name, score FROM events ORDER BY id").await;
    assert_eq!(back.len(), 3);
    assert_eq!(back[0]["id"], json!(1));
    assert_eq!(back[0]["name"], json!("alice"));
    assert_eq!(back[0]["score"], json!(1.5));
    assert_eq!(back[2]["name"], json!("carol"));

    // batch_size = 2 over 5 rows splits into requests of 2 + 2 + 1; the return
    // value is the total, and every row must land.
    http_exec(
        &base,
        "CREATE TABLE nums (n UInt32) ENGINE = MergeTree ORDER BY n",
    )
    .await;
    let sink2 = ClickHouseSink::new(ClickHouseSinkConfig::new(&base, "nums").with_batch_size(2))
        .expect("sink");
    let nums: Vec<Value> = (1..=5).map(|n| json!({ "n": n })).collect();
    assert_eq!(sink2.write_batch(&nums).await.expect("chunked write"), 5);
    let back = read_rows(&base, "SELECT n FROM nums ORDER BY n").await;
    let got: Vec<i64> = back.iter().map(|r| r["n"].as_i64().unwrap()).collect();
    assert_eq!(got, vec![1, 2, 3, 4, 5]);

    // An empty page is a no-op that issues no request.
    assert_eq!(sink.write_batch(&[]).await.expect("empty"), 0);

    // Write modes + a live connect probe.
    assert_eq!(
        sink.supported_write_modes(),
        &[faucet_core::WriteMode::Append]
    );
    let ctx = CheckContext {
        timeout: std::time::Duration::from_secs(5),
    };
    let report = sink.check(&ctx).await.expect("check");
    assert!(
        matches!(report.probes[0].status, ProbeStatus::Pass),
        "connect probe against a live server must pass: {:?}",
        report.probes[0].status
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn write_batch_with_async_insert_lands_rows() {
    let _serial = SERIAL.lock().await;
    let (_c, base) = start_clickhouse().await;

    http_exec(
        &base,
        "CREATE TABLE async_events (id UInt32, name String) ENGINE = MergeTree ORDER BY id",
    )
    .await;

    // async_insert = 1 with wait_for_async_insert = 1 (the default) preserves
    // at-least-once durability, so the rows are queryable immediately after the
    // acknowledged write. Exercises the async-insert query-param branch.
    let sink = ClickHouseSink::new(
        ClickHouseSinkConfig::new(&base, "async_events").with_async_insert(true),
    )
    .expect("sink");
    let rows = vec![
        json!({"id": 10, "name": "x"}),
        json!({"id": 20, "name": "y"}),
    ];
    assert_eq!(sink.write_batch(&rows).await.expect("async write"), 2);

    let back = read_rows(&base, "SELECT id, name FROM async_events ORDER BY id").await;
    let ids: Vec<i64> = back.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![10, 20]);
    assert_eq!(back[0]["name"], json!("x"));
}
