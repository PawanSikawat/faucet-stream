//! Integration tests for MSSQL sink schema introspection + evolution (#194).
//!
//! Requires Docker (the `mcr.microsoft.com/mssql/server` image). Run with:
//! `cargo test -p faucet-sink-mssql --test schema_evolution`.

use faucet_common_mssql::{MssqlConnectionConfig, MssqlPool, MssqlTls, MssqlTlsMode, build_pool};
use faucet_core::{ColumnChange, SchemaEvolution, Sink};
use serde_json::json;
use testcontainers_modules::mssql_server::MssqlServer;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::runners::AsyncRunner;

const ENCODED_PW: &str = "yourStrong%28%21%29Password";

// SQL Server containers need ~2 GB RAM each; serialize so parallel tests don't
// start several containers at once and exhaust the runner.
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

async fn exec(pool: &MssqlPool, sql: &str) {
    let mut conn = pool.get().await.expect("checkout");
    conn.execute(sql, &[]).await.expect("execute setup sql");
}

fn sink_cfg(cfg: &MssqlConnectionConfig, table: &str) -> MssqlSinkConfig {
    let mut s = MssqlSinkConfig::new(cfg.connection_url.clone().unwrap(), table);
    s.connection.tls = cfg.tls.clone();
    s.column_mapping = MssqlColumnMapping::AutoColumns {
        on_unknown_field: faucet_sink_mssql::OnUnknownField::Warn,
    };
    s
}

use faucet_sink_mssql::{MssqlColumnMapping, MssqlSink, MssqlSinkConfig};

#[tokio::test(flavor = "multi_thread")]
async fn current_schema_and_evolve_add_and_widen() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);
    let pool = build_pool(&cfg, 4).await.expect("pool");

    exec(&pool, "CREATE TABLE dbo.t (id BIGINT)").await;

    let sink = MssqlSink::new(sink_cfg(&cfg, "dbo.t")).await.expect("sink");

    // current_schema: id is integer, nullable (no NOT NULL declared).
    let schema = sink
        .current_schema()
        .await
        .expect("current_schema")
        .expect("table exists");
    let id_ty = &schema["properties"]["id"]["type"];
    // BIGINT → integer; the column allows NULL so the type widens to an array.
    assert!(
        id_ty == &json!("integer") || id_ty == &json!(["integer", "null"]),
        "id should be integer (got {id_ty})"
    );

    // Evolve: add `email` (NVARCHAR(MAX) ← Text) and widen `id` → FLOAT (number).
    let evolution = SchemaEvolution {
        additions: vec![ColumnChange {
            name: "email".into(),
            from: None,
            to: json!({"type": "string"}),
        }],
        widenings: vec![ColumnChange {
            name: "id".into(),
            from: Some(json!({"type": "integer"})),
            to: json!({"type": "number"}),
        }],
        relax_nullability: vec![],
    };
    sink.evolve_schema(&evolution).await.expect("evolve");

    // Re-query: email present (string), id now number.
    let schema = sink
        .current_schema()
        .await
        .expect("current_schema")
        .expect("table exists");
    let email_ty = &schema["properties"]["email"]["type"];
    assert!(
        email_ty == &json!("string") || email_ty == &json!(["string", "null"]),
        "email should be string (got {email_ty})"
    );
    let id_ty = &schema["properties"]["id"]["type"];
    assert!(
        id_ty == &json!("number") || id_ty == &json!(["number", "null"]),
        "id should be widened to number (got {id_ty})"
    );

    // Idempotent re-run: the guarded ADD + ALTER are no-ops and must not error.
    sink.evolve_schema(&evolution)
        .await
        .expect("idempotent re-run");

    // A write that uses the new column must succeed (cache was invalidated).
    let written = sink
        .write_batch(&[json!({"id": 1, "email": "a@b.c"})])
        .await
        .expect("write after evolve");
    assert_eq!(written, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn current_schema_returns_none_for_missing_table() {
    let _serial = SERIAL.lock().await;
    let (_c, port) = start_mssql().await;
    let cfg = conn_cfg(port);

    let sink = MssqlSink::new(sink_cfg(&cfg, "dbo.does_not_exist"))
        .await
        .expect("sink");
    assert!(
        sink.current_schema()
            .await
            .expect("current_schema")
            .is_none(),
        "missing table → None"
    );
}
