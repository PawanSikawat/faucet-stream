//! `faucet doctor` end-to-end: a reachable source probes green, an unreachable
//! one probes red with a non-zero exit, and `--json` emits a parseable summary.

use assert_cmd::Command;
use predicates::str::contains;
use std::fs;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A complete, valid REST source config pointed at `base`, paired with a stdout
/// sink. `pagination: None` so the read probe pulls exactly one page.
fn rest_to_stdout(base: &str) -> String {
    format!(
        r#"version: 1
name: doctor_smoke
pipeline:
  source:
    type: rest
    config:
      base_url: {base}
      path: /things
      method: GET
      auth:
        type: none
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
    type: stdout
    config: {{}}
"#
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_passes_for_reachable_source() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/things"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([{"id": 1}])))
        .mount(&server)
        .await;

    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, rest_to_stdout(&server.uri())).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["doctor"])
        .arg(&cfg)
        .args(["--timeout-secs", "10"])
        .assert()
        .success()
        .stdout(contains("source"))
        .stdout(contains("read"))
        .stdout(contains("0 failed"));
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_fails_for_unreachable_source() {
    // Port 9 (discard) on loopback: connection refused, fast and deterministic.
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, rest_to_stdout("http://127.0.0.1:9")).unwrap();

    Command::cargo_bin("faucet")
        .unwrap()
        .args(["doctor"])
        .arg(&cfg)
        .args(["--timeout-secs", "5"])
        .assert()
        .failure()
        .stdout(contains("✗"))
        .stdout(contains("read"));
}

#[tokio::test(flavor = "multi_thread")]
async fn doctor_json_emits_parseable_summary() {
    let dir = TempDir::new().unwrap();
    let cfg = dir.path().join("pipeline.yaml");
    fs::write(&cfg, rest_to_stdout("http://127.0.0.1:9")).unwrap();

    let output = Command::cargo_bin("faucet")
        .unwrap()
        .args(["doctor"])
        .arg(&cfg)
        .args(["--timeout-secs", "5", "--json"])
        .assert()
        .failure()
        .get_output()
        .stdout
        .clone();

    let v: serde_json::Value =
        serde_json::from_slice(&output).expect("doctor --json emits valid JSON");
    assert!(v["summary"]["failed"].as_u64().unwrap() >= 1);
    assert_eq!(v["invocations"][0]["probes"][0]["role"], "source");
}
