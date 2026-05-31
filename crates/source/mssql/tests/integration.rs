//! Integration tests against a real Microsoft SQL Server in Docker.
//!
//! Requires Docker (the `mcr.microsoft.com/mssql/server` image). Run with:
//! `cargo test -p faucet-source-mssql --test integration`. These are skipped by
//! a plain `cargo test --lib` (unit tests), mirroring the postgres/kafka pattern.

use faucet_core::Source;
use faucet_mssql_common::{MssqlConnectionConfig, MssqlTls, MssqlTlsMode, build_pool};
use faucet_source_mssql::{MssqlReplication, MssqlSource, MssqlSourceConfig};
use futures::StreamExt;
use serde_json::Value;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

/// `yourStrong(!)Password` percent-encoded for a URL userinfo segment.
const ENCODED_PW: &str = "yourStrong%28%21%29Password";

// SQL Server containers need ~2 GB RAM each. `cargo test` runs a binary's tests
// in parallel; serialize them so at most one container runs at a time and the
// CI runner doesn't run out of memory.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn start_mssql() -> (ContainerAsync<MssqlServer>, u16) {
    let container = MssqlServer::default()
        .with_accept_eula()
        .start()
        .await
        .expect("start mssql container");
    let port = container
        .get_host_port_ipv4(1433)
        .await
        .expect("mssql host port");
    (container, port)
}

fn conn_cfg(port: u16) -> MssqlConnectionConfig {
    MssqlConnectionConfig {
        connection_url: Some(format!("mssql://sa:{ENCODED_PW}@127.0.0.1:{port}/master")),
        connection_string: None,
        tls: MssqlTls {
            mode: MssqlTlsMode::TrustServerCertificate,
            ca_cert_path: None,
        },
    }
}

async fn exec(pool: &faucet_mssql_common::MssqlPool, sql: &str) {
    let mut conn = pool.get().await.expect("checkout");
    conn.execute(sql, &[]).await.expect("execute setup sql");
}

#[tokio::test(flavor = "multi_thread")]
async fn decodes_column_types_and_streams_pages() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.types_test (
            id INT NOT NULL,
            name NVARCHAR(50),
            price DECIMAL(10,2),
            active BIT,
            created DATETIME2,
            uid UNIQUEIDENTIFIER,
            payload VARBINARY(8),
            big BIGINT,
            ratio FLOAT,
            maybe INT
        )",
    )
    .await;
    exec(
        &pool,
        "INSERT INTO dbo.types_test
            (id, name, price, active, created, uid, payload, big, ratio, maybe)
         VALUES
            (1, N'alice', 19.99, 1, '2024-01-02T03:04:05',
             '00000000-0000-0000-0000-000000000001', 0x010203, 9000000000, 1.5, NULL)",
    )
    .await;

    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT id, name, price, active, created, uid, payload, big, ratio, maybe FROM dbo.types_test",
    );
    scfg.connection.tls = cfg.tls.clone();
    let source = MssqlSource::new(scfg).await.expect("source");

    let rows = source.fetch_all().await.expect("fetch");
    assert_eq!(rows.len(), 1);
    let r = &rows[0];
    assert_eq!(r["id"], Value::from(1));
    assert_eq!(r["name"], Value::from("alice"));
    assert_eq!(r["price"], Value::from("19.99")); // DECIMAL -> string
    assert_eq!(r["active"], Value::from(true)); // BIT -> bool
    assert!(
        r["created"]
            .as_str()
            .unwrap()
            .starts_with("2024-01-02T03:04:05"),
        "datetime2 -> {}",
        r["created"]
    );
    assert_eq!(
        r["uid"],
        Value::from("00000000-0000-0000-0000-000000000001")
    );
    assert_eq!(r["payload"], Value::from("AQID")); // 0x010203 -> base64
    assert_eq!(r["big"], Value::from(9_000_000_000_i64));
    assert_eq!(r["ratio"], Value::from(1.5));
    assert_eq!(r["maybe"], Value::Null);

    // Streaming: 5 rows at batch_size 2 -> pages of 2,2,1.
    exec(&pool, "CREATE TABLE dbo.nums (n INT)").await;
    for n in 1..=5 {
        exec(&pool, &format!("INSERT INTO dbo.nums (n) VALUES ({n})")).await;
    }
    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT n FROM dbo.nums ORDER BY n",
    );
    scfg.connection.tls = cfg.tls.clone();
    scfg.batch_size = 2;
    let source = MssqlSource::new(scfg).await.expect("source");

    let ctx = std::collections::HashMap::new();
    let pages: Vec<_> = source
        .stream_pages(&ctx, 2)
        .map(|p| p.expect("page"))
        .collect()
        .await;
    let sizes: Vec<usize> = pages.iter().map(|p| p.records.len()).collect();
    assert_eq!(sizes, vec![2, 2, 1], "batch_size=2 over 5 rows");
    let total: usize = pages.iter().map(|p| p.records.len()).sum();
    assert_eq!(total, 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn incremental_resumes_without_duplicates() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.events (id INT, updated_at NVARCHAR(30))",
    )
    .await;
    for (id, ts) in [(1, "2024-01-01"), (2, "2024-02-01"), (3, "2024-03-01")] {
        exec(
            &pool,
            &format!("INSERT INTO dbo.events (id, updated_at) VALUES ({id}, '{ts}')"),
        )
        .await;
    }

    let mut scfg = MssqlSourceConfig::new(
        cfg.connection_url.clone().unwrap(),
        "SELECT id, updated_at FROM dbo.events WHERE updated_at > @bookmark ORDER BY updated_at",
    );
    scfg.connection.tls = cfg.tls.clone();
    scfg.replication = MssqlReplication::Incremental {
        column: "updated_at".into(),
        initial_value: Value::from("2024-01-15"),
    };
    let source = MssqlSource::new(scfg).await.expect("source");

    // Run 1: initial_value=2024-01-15 -> rows 2024-02-01 and 2024-03-01.
    let (rows, bookmark) = source.fetch_all_incremental().await.expect("run 1");
    let ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids, vec![2, 3], "run 1 honours initial_value");
    assert_eq!(bookmark, Some(Value::from("2024-03-01")));

    // Persist the bookmark, add a newer row, run again.
    source
        .apply_start_bookmark(bookmark.unwrap())
        .await
        .unwrap();
    exec(
        &pool,
        "INSERT INTO dbo.events (id, updated_at) VALUES (4, '2024-04-01')",
    )
    .await;

    // Run 2: only the strictly-newer row, no duplicates of 2/3.
    let (rows2, bookmark2) = source.fetch_all_incremental().await.expect("run 2");
    let ids2: Vec<i64> = rows2.iter().map(|r| r["id"].as_i64().unwrap()).collect();
    assert_eq!(ids2, vec![4], "run 2 resumes from bookmark, no duplicates");
    assert_eq!(bookmark2, Some(Value::from("2024-04-01")));
}
