//! Integration tests for the BigQuery `write_mode: overwrite` lifecycle (#492):
//! bucket-free staging via the query API + the atomic `TRUNCATE`+`INSERT … SELECT`
//! swap. Driven against a wiremock BigQuery so the DDL/query bodies can be
//! asserted without a real project.

use faucet_core::{OverwriteScope, Sink, WriteMode};
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde::Serialize;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PROJECT_ID: &str = "p";
const DATASET_ID: &str = "d";
const TABLE_ID: &str = "t";
const AUTH_TOKEN_PATH: &str = "/:o/oauth2/token";
const AUTH_SCOPE_BASE: &str = "/auth/bigquery";

#[derive(Serialize)]
struct FakeToken {
    access_token: &'static str,
    token_type: &'static str,
    expires_in: u32,
}

fn fake_token() -> FakeToken {
    FakeToken {
        access_token: "fake-token",
        token_type: "bearer",
        expires_in: 9_999_999,
    }
}

fn dummy_service_account_json(oauth_server: &str) -> serde_json::Value {
    let token_uri = format!("{oauth_server}{AUTH_TOKEN_PATH}");
    json!({
        "type": "service_account",
        "project_id": "dummy",
        "private_key_id": "dummy",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
        "client_email": "dummy@developer.gserviceaccount.com",
        "client_id": "dummy",
        "auth_uri": format!("{oauth_server}/o/oauth2/auth"),
        "token_uri": token_uri,
        "auth_provider_x509_cert_url": format!("{oauth_server}/oauth2/v1/certs"),
        "client_x509_cert_url": format!("{oauth_server}/robot/v1/metadata/x509/dummy"),
    })
}

async fn mount_token_endpoint(server: &MockServer) {
    Mock::given(method("POST"))
        .and(path(AUTH_TOKEN_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(fake_token()))
        .mount(server)
        .await;
}

fn config_overwrite() -> BigQuerySinkConfig {
    let mut c = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    c.write.write_mode = WriteMode::Overwrite;
    c
}

async fn build_sink(
    server: &MockServer,
    config: BigQuerySinkConfig,
) -> (BigQuerySink, tempfile::NamedTempFile) {
    let sa_json = dummy_service_account_json(&server.uri());
    let sa_file = tempfile::NamedTempFile::new().expect("create sa tempfile");
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&sa_json).unwrap(),
    )
    .expect("write sa");
    let client = ClientBuilder::new()
        .with_auth_base_url(format!("{}{AUTH_SCOPE_BASE}", server.uri()))
        .with_v2_base_url(server.uri())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("build client");
    (BigQuerySink::from_parts(config, client), sa_file)
}

async fn mount_table_schema(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "schema": {"fields": [
                {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "name", "type": "STRING", "mode": "NULLABLE"}
            ]}
        })))
        .mount(server)
        .await;
}

/// `tables.get` returns 404 (table not found) — the shape the BigQuery client
/// maps to a not-found error. `up_to` bounds how many times it answers before a
/// later, higher-numbered-priority mock (e.g. a 200 schema) takes over.
async fn mount_table_missing(server: &MockServer, up_to: Option<u64>) {
    let mut m = Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "error": {"code": 404, "message": "Not found: Table",
                      "errors": [{"reason": "notFound", "message": "Not found: Table"}]}
        })))
        .with_priority(1);
    if let Some(n) = up_to {
        m = m.up_to_n_times(n);
    }
    m.mount(server).await;
}

async fn mount_query_and_job(server: &MockServer, job_id: &str) {
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "kind": "bigquery#queryResponse",
            "jobComplete": true,
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id}
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/projects/{PROJECT_ID}/jobs/{job_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": PROJECT_ID, "jobId": job_id},
            "status": {"state": "DONE"}
        })))
        .mount(server)
        .await;
}

async fn queries(server: &MockServer) -> Vec<String> {
    server
        .received_requests()
        .await
        .expect("recording enabled")
        .into_iter()
        .filter(|r| r.url.path().ends_with("/queries"))
        .filter_map(|r| {
            serde_json::from_slice::<serde_json::Value>(&r.body)
                .ok()
                .and_then(|b| b["query"].as_str().map(str::to_string))
        })
        .collect()
}

#[tokio::test]
async fn overwrite_advertised_and_flagged() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    let (sink, _sa) = build_sink(&server, config_overwrite()).await;
    assert!(sink.is_overwrite());
    assert!(sink.supported_write_modes().contains(&WriteMode::Overwrite));
}

#[tokio::test]
async fn overwrite_lifecycle_posts_bucket_free_swap() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-o").await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;

    // begin → CREATE OR REPLACE TABLE temp LIKE target
    sink.begin_overwrite().await.expect("begin");
    // write → the page is loaded into the staging table via the query API
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .expect("write");
    assert_eq!(n, 2);
    // also exercise the partial path (overwrite is insert-shaped, all Ok)
    let outcomes = sink
        .write_batch_partial(&[json!({"id": 3, "name": "c"})])
        .await
        .expect("partial");
    assert_eq!(outcomes.len(), 1);
    assert!(outcomes[0].is_ok());
    // commit → transactional TRUNCATE + INSERT … SELECT swap, then DROP temp
    sink.commit_overwrite().await.expect("commit");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.contains("CREATE OR REPLACE TABLE `p.d.t__faucet_ovw` LIKE `p.d.t`")),
        "begin DDL missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.contains("INSERT INTO `p.d.t__faucet_ovw`")),
        "staging load missing: {qs:?}"
    );
    assert!(
        qs.iter().any(|q| q.contains("BEGIN TRANSACTION")
            && q.contains("TRUNCATE TABLE `p.d.t`")
            && q.contains("INSERT INTO `p.d.t` SELECT * FROM `p.d.t__faucet_ovw`")),
        "commit swap missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.contains("DROP TABLE IF EXISTS `p.d.t__faucet_ovw`")),
        "drop temp missing: {qs:?}"
    );
}

#[tokio::test]
async fn scoped_overwrite_commit_deletes_in_window_not_truncate() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-s").await;

    let mut config = config_overwrite();
    config.scope = Some(OverwriteScope::Window {
        column: "posting_date".into(),
        from: json!("2024-06-01"),
        to: json!("2024-07-01"),
    });
    let (sink, _sa) = build_sink(&server, config).await;

    sink.begin_overwrite().await.expect("begin");
    sink.write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write");
    sink.commit_overwrite().await.expect("commit");

    let qs = queries(&server).await;
    assert!(
        qs.iter().any(|q| q.contains("BEGIN TRANSACTION")
            && q.contains("DELETE FROM `p.d.t` WHERE `posting_date` >= '2024-06-01' AND `posting_date` < '2024-07-01'")
            && q.contains("INSERT INTO `p.d.t` SELECT * FROM `p.d.t__faucet_ovw`")),
        "scoped commit swap missing: {qs:?}"
    );
    assert!(
        !qs.iter().any(|q| q.contains("TRUNCATE TABLE `p.d.t`")),
        "scoped overwrite must not truncate: {qs:?}"
    );
}

#[tokio::test]
async fn overwrite_abort_drops_staging_without_swap() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // `begin_overwrite` now probes table existence first; the target exists, so
    // it clones staging via `LIKE` and abort drops that staging.
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-a").await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;
    sink.begin_overwrite().await.expect("begin");
    sink.abort_overwrite().await.expect("abort");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.contains("DROP TABLE IF EXISTS `p.d.t__faucet_ovw`")),
        "abort must drop staging: {qs:?}"
    );
    assert!(
        !qs.iter().any(|q| q.contains("TRUNCATE TABLE")),
        "abort must not swap into the target: {qs:?}"
    );
}

#[tokio::test]
async fn overwrite_staging_insert_failure_surfaces_error() {
    // A rejected staging-page load must surface as an error, not silently drop
    // the page — otherwise the commit swap would replace the destination with an
    // incomplete dataset. Covers the `insert_overwrite_page` query-failure path.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_schema(&server).await;
    Mock::given(method("POST"))
        .and(path(format!("/projects/{PROJECT_ID}/queries")))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({
            "error": {"code": 500, "message": "backend error"}
        })))
        .mount(&server)
        .await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;
    let err = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect_err("a failed staging load must surface as an error");
    assert!(
        err.to_string().contains("overwrite page insert"),
        "error should name the overwrite page insert: {err}"
    );
}

#[tokio::test]
async fn overwrite_creates_dataset_table_and_staging_when_missing() {
    // create_table defaults to true: a first-ever overwrite sync must create the
    // dataset + target table (schema inferred from the first page) and the
    // staging clone, then swap — instead of 404ing on the missing target.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // tables.get: 404 for the begin-time existence probe, then 200 once created
    // (the deferred create re-reads the schema to build the staging load).
    mount_table_missing(&server, Some(1)).await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-c").await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;

    sink.begin_overwrite().await.expect("begin");
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"}), json!({"id": 2, "name": "b"})])
        .await
        .expect("write");
    assert_eq!(n, 2);
    sink.commit_overwrite().await.expect("commit");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.contains("CREATE SCHEMA IF NOT EXISTS `p`.`d`")),
        "dataset create missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.starts_with("CREATE OR REPLACE TABLE `p.d.t` (")
                && q.contains("`id` INT64")
                && q.contains("`name` STRING")),
        "target table create missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.contains("CREATE OR REPLACE TABLE `p.d.t__faucet_ovw` LIKE `p.d.t`")),
        "staging clone missing: {qs:?}"
    );
    assert!(
        qs.iter()
            .any(|q| q.contains("INSERT INTO `p.d.t__faucet_ovw`")),
        "staging load missing: {qs:?}"
    );
    assert!(
        qs.iter().any(|q| q.contains("TRUNCATE TABLE `p.d.t`")
            && q.contains("INSERT INTO `p.d.t` SELECT * FROM `p.d.t__faucet_ovw`")),
        "commit swap missing: {qs:?}"
    );
}

#[tokio::test]
async fn overwrite_begin_errors_when_target_missing_and_create_disabled() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;

    let mut config = config_overwrite();
    config.create_table = false;
    let (sink, _sa) = build_sink(&server, config).await;

    let err = sink
        .begin_overwrite()
        .await
        .expect_err("missing target + create_table disabled must error");
    assert!(
        err.to_string().contains("create_table` is disabled"),
        "error should name the disabled create_table: {err}"
    );
}

#[tokio::test]
async fn append_creates_table_when_missing_by_default() {
    // The append path also honors create_table: a missing table is created from
    // the first page's inferred schema before the streaming insert.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;
    mount_query_and_job(&server, "job-ap").await;
    Mock::given(method("POST"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}/insertAll"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
        .mount(&server)
        .await;

    // Default config = append mode, create_table defaults true.
    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    );
    let (sink, _sa) = build_sink(&server, config).await;

    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write");
    assert_eq!(n, 1);

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.starts_with("CREATE OR REPLACE TABLE `p.d.t` (")),
        "append path must create the missing table: {qs:?}"
    );
}

#[tokio::test]
async fn overwrite_recreates_schemaless_target() {
    // A table created by a bare `bq mk` (no schema) exists but has no fields, so
    // `CREATE … LIKE` / typed inserts fail against it. create_table (default on)
    // must (re)create it from the first page's schema, then swap.
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    // schema probe: empty-schema 200 for the begin-time check, then a real
    // schema once the table has been (re)created.
    Mock::given(method("GET"))
        .and(path(format!(
            "/projects/{PROJECT_ID}/datasets/{DATASET_ID}/tables/{TABLE_ID}"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tableReference": {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": TABLE_ID},
            "schema": {"fields": []}
        })))
        .up_to_n_times(1)
        .with_priority(1)
        .mount(&server)
        .await;
    mount_table_schema(&server).await;
    mount_query_and_job(&server, "job-sl").await;

    let (sink, _sa) = build_sink(&server, config_overwrite()).await;
    sink.begin_overwrite().await.expect("begin");
    let n = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect("write");
    assert_eq!(n, 1);
    sink.commit_overwrite().await.expect("commit");

    let qs = queries(&server).await;
    assert!(
        qs.iter()
            .any(|q| q.starts_with("CREATE OR REPLACE TABLE `p.d.t` (")),
        "schemaless target must be re-created: {qs:?}"
    );
    assert!(
        qs.iter().any(|q| q.contains("TRUNCATE TABLE `p.d.t`")),
        "commit swap missing: {qs:?}"
    );
}

#[tokio::test]
async fn append_errors_when_missing_and_create_disabled() {
    let server = MockServer::start().await;
    mount_token_endpoint(&server).await;
    mount_table_missing(&server, None).await;

    let config = BigQuerySinkConfig::new(
        PROJECT_ID,
        DATASET_ID,
        TABLE_ID,
        BigQueryCredentials::ApplicationDefault,
    )
    .with_create_table(false);
    let (sink, _sa) = build_sink(&server, config).await;

    let err = sink
        .write_batch(&[json!({"id": 1, "name": "a"})])
        .await
        .expect_err("missing table + create_table disabled must error");
    assert!(
        err.to_string().contains("create_table` is disabled"),
        "error should name the disabled create_table: {err}"
    );
}
