//! RBAC + audit-log integration tests for `faucet serve` (#205). Boots a real
//! server with a multi-principal `--auth-config` and asserts role enforcement
//! (`403` for a viewer's write, `200` for its read) and that mutating / denied
//! actions land in the admin-only audit log. Requires the `serve` feature.
#![cfg(feature = "serve")]

use faucet_cli::cli::ServeArgs;
use faucet_cli::serve::ServeConfig;
use std::time::Duration;

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Two principals: `admin-tok` → admin, `viewer-tok` → viewer.
const AUTH_CONFIG: &str = "principals:\n\
    \x20 - name: alice\n\
    \x20   token: admin-tok\n\
    \x20   role: admin\n\
    \x20 - name: bob\n\
    \x20   token: viewer-tok\n\
    \x20   role: viewer\n";

fn args_with_auth_config(port: u16, auth_config: std::path::PathBuf) -> ServeArgs {
    ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        auth_config: Some(auth_config),
        no_auth: false,
        max_concurrent_runs: Some(4),
        max_queued_runs: Some(16),
        default_config: None,
        history: None,
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        log_retention_secs: 604_800,
        log_max_lines_per_run: 100_000,
        local_output_retention_days: 7,
        local_output_in_flight_grace_secs: 60,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: false,
        cluster: false,
        cluster_poll_secs: 2,
        cluster_max_attempts: 3,
        triggers: None,
        callback_allow_host: Vec::new(),
        mcp: false,
        mcp_allow_mutations: false,
    }
}

/// Boot a server whose auth is the two-principal RBAC config. Returns the
/// tempdir (kept alive for the server's lifetime — it holds the auth file).
async fn spawn_rbac_server(port: u16) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let auth_path = dir.path().join("auth.yaml");
    std::fs::write(&auth_path, AUTH_CONFIG).unwrap();
    let mut config = ServeConfig::from_args(args_with_auth_config(port, auth_path)).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config, Default::default()).await;
    });
    let client = reqwest::Client::new();
    for _ in 0..100 {
        if client
            .get(format!("http://127.0.0.1:{port}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            return dir;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("server did not become healthy on port {port}");
}

fn csv_to_jsonl_yaml(input: &std::path::Path, output: &std::path::Path) -> String {
    format!(
        "version: 1\npipeline:\n  source: {{ type: csv, config: {{ path: \"{}\" }} }}\n  sink: {{ type: jsonl, config: {{ path: \"{}\" }} }}\n",
        input.display(),
        output.display()
    )
}

/// A viewer is denied `POST /v1/runs` (403) but allowed `GET /v1/runs` (200);
/// an admin can submit (202). Covers the core acceptance criterion.
#[tokio::test(flavor = "multi_thread")]
async fn viewer_is_readonly_admin_can_write() {
    let port = free_port();
    let _dir = spawn_rbac_server(port).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    // No token → 401.
    assert_eq!(
        client
            .get(format!("{base}/v1/runs"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );

    // Viewer can read.
    assert_eq!(
        client
            .get(format!("{base}/v1/runs"))
            .bearer_auth("viewer-tok")
            .send()
            .await
            .unwrap()
            .status(),
        200,
        "viewer must be allowed GET /v1/runs"
    );

    // Viewer cannot write.
    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "name\nalice\n").unwrap();
    let cfg = csv_to_jsonl_yaml(&input, &dir.path().join("out.jsonl"));
    let denied = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("viewer-tok")
        .json(&serde_json::json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(denied.status(), 403, "viewer must be denied POST /v1/runs");
    let err: serde_json::Value = denied.json().await.unwrap();
    assert_eq!(err["error"]["code"], "forbidden");

    // Admin can write.
    let accepted = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("admin-tok")
        .json(&serde_json::json!({ "config": cfg }))
        .send()
        .await
        .unwrap();
    assert_eq!(
        accepted.status(),
        202,
        "admin must be allowed POST /v1/runs"
    );
}

/// The audit log is admin-only (viewer → 403) and records both a successful
/// mutating action (admin's `run.submit`, result `ok`) and a denied one
/// (viewer's `run.submit`, result `denied`).
#[tokio::test(flavor = "multi_thread")]
async fn audit_log_records_actions_and_is_admin_only() {
    let port = free_port();
    let _dir = spawn_rbac_server(port).await;
    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "name\nalice\n").unwrap();
    let cfg = csv_to_jsonl_yaml(&input, &dir.path().join("out.jsonl"));
    let body = serde_json::json!({ "config": cfg });

    // Admin submit (recorded ok) and a viewer submit (denied, recorded).
    let submit: serde_json::Value = client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("admin-tok")
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let run_id = submit["run_id"].as_str().unwrap().to_string();

    client
        .post(format!("{base}/v1/runs"))
        .bearer_auth("viewer-tok")
        .json(&body)
        .send()
        .await
        .unwrap();

    // Viewer is denied the audit log.
    assert_eq!(
        client
            .get(format!("{base}/v1/audit"))
            .bearer_auth("viewer-tok")
            .send()
            .await
            .unwrap()
            .status(),
        403,
        "viewer must be denied GET /v1/audit"
    );

    // Admin reads the audit log.
    let audit: serde_json::Value = client
        .get(format!("{base}/v1/audit"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let entries = audit["entries"].as_array().expect("entries array");

    // A successful run.submit by alice, carrying the run_id + a fingerprint.
    let submit_ok = entries
        .iter()
        .find(|e| e["action"] == "run.submit" && e["result"] == "ok" && e["principal"] == "alice");
    let submit_ok = submit_ok.unwrap_or_else(|| panic!("no admin run.submit entry in {audit:#}"));
    assert_eq!(submit_ok["run_id"], run_id);
    assert!(
        submit_ok["config_fingerprint"].is_string(),
        "submit audit must carry a config fingerprint"
    );

    // A denied run.submit by bob.
    assert!(
        entries.iter().any(|e| {
            e["action"] == "run.submit" && e["result"] == "denied" && e["principal"] == "bob"
        }),
        "denied viewer submit must be audited; got {audit:#}"
    );

    // Filtering by principal narrows the result set.
    let bob_only: serde_json::Value = client
        .get(format!("{base}/v1/audit?principal=bob"))
        .bearer_auth("admin-tok")
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        bob_only["entries"]
            .as_array()
            .unwrap()
            .iter()
            .all(|e| e["principal"] == "bob"),
        "principal filter must only return that principal's entries"
    );
}
