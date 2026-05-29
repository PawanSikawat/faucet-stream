//! End-to-end REST → JSONL test using wiremock. Verifies that the YAML
//! source config — including the tagged Auth and pagination enums — round-trips
//! through serde into a working RestStream.

#![cfg(feature = "source-rest")]
#![cfg(feature = "sink-jsonl")]

use assert_cmd::Command;
use predicates::str::contains;
use std::fs;
use tempfile::TempDir;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test(flavor = "multi_thread")]
async fn rest_to_jsonl_round_trip() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/things"))
        .and(header("x-api-key", "secret-xyz"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"id": 1, "name": "thing one"},
            {"id": 2, "name": "thing two"},
        ])))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let out = dir.path().join("out.jsonl");
    let cfg = dir.path().join("pipeline.yaml");
    let yaml = format!(
        r#"version: 1
name: rest_smoke
pipeline:
  source:
    type: rest
    config:
      base_url: {base}
      path: /things
      method: GET
      auth:
        type: api_key
        config:
          header: X-Api-Key
          value: ${{env:FAUCET_TEST_API_KEY}}
      query_params: {{}}
      pagination:
        type: None
      max_retries: 0
      retry_backoff: 0
      tolerated_http_errors: []
      replication_method:
        type: FullTable
      primary_keys: []
      partitions: []
      schema_sample_size: 0
  sink:
    type: jsonl
    config:
      path: {out}
"#,
        base = server.uri(),
        out = out.display(),
    );
    fs::write(&cfg, yaml).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .env("FAUCET_TEST_API_KEY", "secret-xyz")
        .args(["run"])
        .arg(&cfg)
        .assert()
        .success()
        .stdout(contains("wrote 2 records"));

    let body = fs::read_to_string(&out).unwrap();
    assert_eq!(body.lines().count(), 2);
    assert!(body.contains("\"thing one\""));
}
