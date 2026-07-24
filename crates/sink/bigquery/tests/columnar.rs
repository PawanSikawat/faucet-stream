//! Arrow columnar load-job path (#380): assert the sink advertises the
//! columnar fast path exactly when a `bulk_load` staging config is present and
//! the write mode is `append` — the "columnar path is taken" acceptance
//! criterion. No network: `supports_columnar` is pure, and the offline client
//! is never driven.
#![cfg(feature = "arrow")]

use faucet_core::Sink;
use faucet_sink_bigquery::config::BigQueryLoadConfig;
use faucet_sink_bigquery::{BigQueryCredentials, BigQuerySink, BigQuerySinkConfig};
use gcp_bigquery_client::client_builder::ClientBuilder;
use serde_json::json;

/// Throwaway service-account JSON — enough for the client builder to assemble
/// an authenticator offline (it is never used to make a call here).
fn dummy_sa_json() -> serde_json::Value {
    json!({
        "type": "service_account",
        "project_id": "dummy",
        "private_key_id": "dummy",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDNk6cKkWP/4NMu\nWb3s24YHfM639IXzPtTev06PUVVQnyHmT1bZgQ/XB6BvIRaReqAqnQd61PAGtX3e\n8XocTw+u/ZfiPJOf+jrXMkRBpiBh9mbyEIqBy8BC20OmsUc+O/YYh/qRccvRfPI7\n3XMabQ8eFWhI6z/t35oRpvEVFJnSIgyV4JR/L/cjtoKnxaFwjBzEnxPiwtdy4olU\nKO/1maklXexvlO7onC7CNmPAjuEZKzdMLzFszikCDnoKJC8k6+2GZh0/JDMAcAF4\nwxlKNQ89MpHVRXZ566uKZg0MqZqkq5RXPn6u7yvNHwZ0oahHT+8ixPPrAEjuPEKM\nUPzVRz71AgMBAAECggEAfdbVWLW5Befkvam3hea2+5xdmeN3n3elrJhkiXxbAhf3\nE1kbq9bCEHmdrokNnI34vz0SWBFCwIiWfUNJ4UxQKGkZcSZto270V8hwWdNMXUsM\npz6S2nMTxJkdp0s7dhAUS93o9uE2x4x5Z0XecJ2ztFGcXY6Lupu2XvnW93V9109h\nkY3uICLdbovJq7wS/fO/AL97QStfEVRWW2agIXGvoQG5jOwfPh86GZZRYP9b8VNw\ntkAUJe4qpzNbWs9AItXOzL+50/wsFkD/iWMGWFuU8DY5ZwsL434N+uzFlaD13wtZ\n63D+tNAxCSRBfZGQbd7WxJVFfZe/2vgjykKWsdyNAQKBgQDnEBgSI836HGSRk0Ub\nDwiEtdfh2TosV+z6xtyU7j/NwjugTOJEGj1VO/TMlZCEfpkYPLZt3ek2LdNL66n8\nDyxwzTT5Q3D/D0n5yE3mmxy13Qyya6qBYvqqyeWNwyotGM7hNNOix1v9lEMtH5Rd\nUT0gkThvJhtrV663bcAWCALmtQKBgQDjw2rYlMUp2TUIa2/E7904WOnSEG85d+nc\norhzthX8EWmPgw1Bbfo6NzH4HhebTw03j3NjZdW2a8TG/uEmZFWhK4eDvkx+rxAa\n6EwamS6cmQ4+vdep2Ac4QCSaTZj02YjHb06Be3gptvpFaFrotH2jnpXxggdiv8ul\n6x+ooCffQQKBgQCR3ykzGoOI6K/c75prELyR+7MEk/0TzZaAY1cSdq61GXBHLQKT\nd/VMgAN1vN51pu7DzGBnT/dRCvEgNvEjffjSZdqRmrAVdfN/y6LSeQ5RCfJgGXSV\nJoWVmMxhCNrxiX3h01Xgp/c9SYJ3VD54AzeR/dwg32/j/oEAsDraLciXGQKBgQDF\nMNc8k/DvfmJv27R06Ma6liA6AoiJVMxgfXD8nVUDW3/tBCVh1HmkFU1p54PArvxe\nchAQqoYQ3dUMBHeh6ZRJaYp2ATfxJlfnM99P1/eHFOxEXdBt996oUMBf53bZ5cyJ\n/lAVwnQSiZy8otCyUDHGivJ+mXkTgcIq8BoEwERFAQKBgQDmImBaFqoMSVihqHIf\nDa4WZqwM7ODqOx0JnBKrKO8UOc51J5e1vpwP/qRpNhUipoILvIWJzu4efZY7GN5C\nImF9sN3PP6Sy044fkVPyw4SYEisxbvp9tfw8Xmpj/pbmugkB2ut6lz5frmEBoJSN\n3osZlZTgx+pM3sO6ITV6U4ID2Q==\n-----END PRIVATE KEY-----\n",
        "client_email": "dummy@developer.gserviceaccount.com",
        "client_id": "dummy",
        "auth_uri": "https://example.invalid/o/oauth2/auth",
        "token_uri": "https://example.invalid/token",
    })
}

async fn offline_client() -> (gcp_bigquery_client::Client, tempfile::NamedTempFile) {
    let sa_file = tempfile::NamedTempFile::new().expect("sa tempfile");
    std::fs::write(
        sa_file.path(),
        serde_json::to_string_pretty(&dummy_sa_json()).unwrap(),
    )
    .expect("write sa");
    let client = ClientBuilder::new()
        .with_v2_base_url("https://example.invalid".into())
        .build_from_service_account_key_file(sa_file.path().to_str().unwrap())
        .await
        .expect("offline client");
    (client, sa_file)
}

fn load_cfg() -> BigQueryLoadConfig {
    BigQueryLoadConfig {
        staging_bucket: "stage-bucket".into(),
        staging_prefix: "faucet-bq-load/".into(),
        gcs_auth: Default::default(),
        write_disposition: "WRITE_APPEND".into(),
        storage_host: None,
    }
}

#[tokio::test]
async fn columnar_taken_only_with_bulk_load_and_append() {
    let (client, _sa) = offline_client().await;

    // No bulk_load → row (insertAll) path only.
    let plain = BigQuerySink::from_parts(
        BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault),
        client.clone(),
    );
    assert!(!plain.supports_columnar());

    // bulk_load + append → columnar fast path advertised.
    let staged = BigQuerySink::from_parts(
        BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault)
            .with_bulk_load(load_cfg()),
        client.clone(),
    );
    assert!(staged.supports_columnar());

    // bulk_load + upsert → NOT columnar (load jobs are append/truncate only).
    let mut upsert_cfg =
        BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault)
            .with_bulk_load(load_cfg());
    upsert_cfg.write.write_mode = faucet_core::WriteMode::Upsert;
    upsert_cfg.write.key = vec!["id".into()];
    let upsert = BigQuerySink::from_parts(upsert_cfg, client);
    assert!(!upsert.supports_columnar());
}
