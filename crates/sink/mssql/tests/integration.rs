//! Integration tests against a real Microsoft SQL Server in Docker.
//!
//! Requires Docker (the `mcr.microsoft.com/mssql/server` image). Run with:
//! `cargo test -p faucet-sink-mssql --test integration`.

use faucet_core::Sink;
use faucet_mssql_common::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig};
use serde_json::{Value, json};
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";

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

async fn exec(pool: &MssqlPool, sql: &str) {
    let mut conn = pool.get().await.expect("checkout");
    conn.execute(sql, &[]).await.expect("execute setup sql");
}

async fn count(pool: &MssqlPool, table: &str) -> i32 {
    let mut conn = pool.get().await.expect("checkout");
    let rows = conn
        .query(format!("SELECT COUNT(*) AS c FROM {table}"), &[])
        .await
        .expect("count query")
        .into_first_result()
        .await
        .expect("count result");
    rows[0].get::<i32, _>("c").expect("count value")
}

fn sink_cfg(cfg: &MssqlConnectionConfig, table: &str) -> MssqlSinkConfig {
    let mut s = MssqlSinkConfig::new(cfg.connection_url.clone().unwrap(), table);
    s.connection.tls = cfg.tls.clone();
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn auto_columns_bulk_write_splits_param_limit() {
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(&pool, "CREATE TABLE dbo.bulk (id INT, name NVARCHAR(50))").await;

    let mut scfg = sink_cfg(&cfg, "dbo.bulk");
    scfg.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: faucet_sink_mssql::OnUnknownField::Warn,
    };
    // batch_size 0 forces the whole 5000-row page through one write_batch, so
    // the 2100-parameter auto-split (5000 rows * 2 cols = 10000 params) is hit.
    scfg.batch_size = 0;
    let sink = MssqlSink::new(scfg).await.expect("sink");

    let records: Vec<Value> = (1..=5000)
        .map(|i| json!({"id": i, "name": format!("user-{i}")}))
        .collect();
    let written = sink.write_batch(&records).await.expect("write");
    assert_eq!(written, 5000);
    assert_eq!(count(&pool, "dbo.bulk").await, 5000);
}

#[tokio::test(flavor = "multi_thread")]
async fn row_isolation_routes_only_the_bad_row() {
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(
        &pool,
        "CREATE TABLE dbo.strict (id INT NOT NULL, n INT NOT NULL)",
    )
    .await;

    let mut scfg = sink_cfg(&cfg, "dbo.strict");
    scfg.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: faucet_sink_mssql::OnUnknownField::Warn,
    };
    let sink = MssqlSink::new(scfg).await.expect("sink");

    // Row 3 is missing `n` -> binds NULL -> violates NOT NULL.
    let records = vec![
        json!({"id": 1, "n": 10}),
        json!({"id": 2, "n": 20}),
        json!({"id": 3}),
    ];
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("partial write");
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes[0].is_ok(), "row 1 ok");
    assert!(outcomes[1].is_ok(), "row 2 ok");
    assert!(outcomes[2].is_err(), "row 3 (NULL into NOT NULL) -> DLQ");

    // Only the two good rows persisted.
    assert_eq!(count(&pool, "dbo.strict").await, 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn json_column_with_create_table() {
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    let mut scfg = sink_cfg(&cfg, "dbo.payloads");
    scfg.column_mapping = MssqlColumnMapping::JsonColumn {
        column: "body".into(),
    };
    scfg.create_table = true;
    // new() should create the table (id IDENTITY + body NVARCHAR(MAX)).
    let sink = MssqlSink::new(scfg).await.expect("sink creates table");

    let written = sink
        .write_batch(&[json!({"a": 1, "nested": {"x": true}}), json!({"b": 2})])
        .await
        .expect("write");
    assert_eq!(written, 2);
    assert_eq!(count(&pool, "dbo.payloads").await, 2);

    // The body column holds the serialized JSON.
    let mut conn = pool.get().await.unwrap();
    let rows = conn
        .query("SELECT body FROM dbo.payloads ORDER BY id", &[])
        .await
        .unwrap()
        .into_first_result()
        .await
        .unwrap();
    let first: &str = rows[0].get("body").unwrap();
    let parsed: Value = serde_json::from_str(first).unwrap();
    assert_eq!(parsed["a"], json!(1));
    assert_eq!(parsed["nested"]["x"], json!(true));
}
