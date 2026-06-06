//! Integration tests for `MysqlCdcSource` against a real MySQL 8 instance
//! via testcontainers.
//!
//! These tests require Docker (matching the kafka / postgres-cdc convention).
//! MySQL is started with `binlog_row_metadata=FULL` (required for column names
//! in the binlog). MySQL 8 already defaults `log_bin=ON`, `binlog_format=ROW`,
//! and `binlog_row_image=FULL`, so only `binlog_row_metadata` needs an
//! explicit flag.
//!
//! The test opens a binlog stream, then a concurrent writer performs
//! INSERT / UPDATE / DELETE after a warm-up delay (so `start_position =
//! current` is ahead of those writes when the stream opens), and asserts the
//! emitted CDC envelopes. It then resumes from the captured bookmark and
//! asserts that a subsequent write — and only that write — is delivered (no
//! replay).

use faucet_core::Source;
use faucet_source_mysql_cdc::{MysqlCdcSource, MysqlCdcSourceConfig};
use futures::StreamExt;
use mysql_async::{Conn, Opts, prelude::Queryable};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::mysql::Mysql;

/// Bounds concurrent MySQL container startups across all tests in this binary.
/// MySQL 8.x init is heavy (~2-3 GB RSS per container during startup) and
/// starting multiple in parallel can exhaust memory on Colima/Docker Desktop.
/// We allow at most two simultaneous startups; once a container is running it
/// is steady-state cheap, so the cap only serialises the spin-up window.
fn startup_limit() -> &'static tokio::sync::Semaphore {
    static SEM: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    SEM.get_or_init(|| tokio::sync::Semaphore::new(2))
}

/// Start a MySQL 8 container with binlog options required for CDC and return
/// both the container handle and a connection URL.
///
/// Passes the binlog flags as CMD args. The official `mysql` Docker image
/// prepends `mysqld` to any `--`-prefixed args found in CMD, so passing
/// `["--server-id=1", "--log-bin=mysql-bin", ...]` causes the entrypoint
/// to run `mysqld --server-id=1 --log-bin=mysql-bin ...`.
///
/// The default `Mysql` image creates database `test` with the root user
/// having no password, so the connection URL is
/// `mysql://root@127.0.0.1:<port>/test`.
async fn start_mysql_cdc() -> (ContainerAsync<Mysql>, String) {
    let _permit = startup_limit()
        .acquire()
        .await
        .expect("startup semaphore closed");

    let container = Mysql::default()
        .with_cmd([
            "--server-id=1",
            "--log-bin=mysql-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--binlog-row-metadata=FULL",
        ])
        .start()
        .await
        .expect("mysql CDC container start");

    let port = container
        .get_host_port_ipv4(3306)
        .await
        .expect("mysql port");

    let url = format!("mysql://root@127.0.0.1:{port}/test");
    (container, url)
}

/// Build the CDC source config.
fn build_config(url: &str) -> MysqlCdcSourceConfig {
    serde_json::from_value(json!({
        "connection_url": url,
        "server_id": 1001,
        "start_position": { "type": "current" },
        "idle_timeout": 5,
        "batch_size": 0
    }))
    .expect("config")
}

/// Open a fresh `mysql_async` connection to the given URL.
async fn connect(url: &str) -> Conn {
    Conn::new(Opts::from_url(url).expect("parse URL"))
        .await
        .expect("connect")
}

/// Drain a single binlog fetch cycle into a flat `Vec` of records plus the
/// bookmark of the last page that carried one. The cycle ends after the
/// source's `idle_timeout` (5 s) of quiet.
async fn drain(source: &MysqlCdcSource) -> (Vec<Value>, Option<Value>) {
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);
    let mut records = Vec::new();
    let mut bookmark = None;
    while let Some(page) = pages.next().await {
        let page = page.expect("page");
        records.extend(page.records);
        if page.bookmark.is_some() {
            bookmark = page.bookmark;
        }
    }
    (records, bookmark)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cdc_captures_crud_then_resumes_without_replay() {
    let (_container, url) = start_mysql_cdc().await;

    // Pre-create the table BEFORE building the source so that `start_position =
    // current` is positioned after the DDL and won't capture it.
    {
        let mut conn = connect(&url).await;
        conn.query_drop("CREATE TABLE test.users (id INT PRIMARY KEY, name VARCHAR(64))")
            .await
            .expect("create table");
    }

    // Build the source — opens a throwaway connection for preflight checks and
    // records the current binlog position as the stream start.
    let source = MysqlCdcSource::new(build_config(&url))
        .await
        .expect("source new");

    // Concurrent writer: wait ~2 s for the stream to open, then INSERT / UPDATE /
    // DELETE on id=1.
    let writer_url = url.clone();
    let writer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut conn = connect(&writer_url).await;
        conn.query_drop("INSERT INTO test.users (id, name) VALUES (1, 'alice')")
            .await
            .expect("insert");
        conn.query_drop("UPDATE test.users SET name = 'bob' WHERE id = 1")
            .await
            .expect("update");
        conn.query_drop("DELETE FROM test.users WHERE id = 1")
            .await
            .expect("delete");
    });

    let (records, bookmark) = drain(&source).await;
    writer.await.expect("writer task");

    // We must have observed the create, update, and delete for id=1.
    let ops: Vec<&str> = records
        .iter()
        .map(|r| r["op"].as_str().unwrap_or(""))
        .collect();
    assert!(ops.contains(&"c"), "expected a create op, got {ops:?}");
    assert!(ops.contains(&"u"), "expected an update op, got {ops:?}");
    assert!(ops.contains(&"d"), "expected a delete op, got {ops:?}");

    // The create envelope must carry the correct namespace, column values, and LSN.
    let create = records
        .iter()
        .find(|r| r["op"] == "c")
        .expect("create record");
    assert_eq!(create["schema"], "test", "schema must be 'test'");
    assert_eq!(create["table"], "users", "table must be 'users'");
    assert_eq!(
        create["after"]["name"], "alice",
        "after.name must be 'alice'; envelope: {create:?}"
    );
    assert!(
        create["lsn"]["file"].is_string(),
        "lsn.file must be a string; envelope: {create:?}"
    );
    assert!(
        create["lsn"]["pos"].is_number(),
        "lsn.pos must be a number; envelope: {create:?}"
    );

    let bookmark = bookmark.expect("cycle 1 must produce a bookmark");

    // Apply the bookmark and drain cycle 2 — only the id=2 insert must appear.
    source
        .apply_start_bookmark(bookmark)
        .await
        .expect("apply bookmark");

    let writer2_url = url.clone();
    let writer2 = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        let mut conn = connect(&writer2_url).await;
        conn.query_drop("INSERT INTO test.users (id, name) VALUES (2, 'carol')")
            .await
            .expect("insert2");
    });

    let (records2, _bm2) = drain(&source).await;
    writer2.await.expect("writer2 task");

    // Resume must not replay id=1's events; only the id=2 insert appears.
    assert!(
        !records2.is_empty(),
        "expected the post-bookmark insert to be delivered"
    );
    for r in &records2 {
        let id = &r["after"]["id"];
        assert_eq!(id, &json!(2), "resume replayed a pre-bookmark event: {r:?}");
    }
    assert!(
        records2.iter().any(|r| r["op"] == "c"),
        "expected a create op in cycle 2, got: {records2:?}"
    );
}
