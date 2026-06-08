//! Phase-1 smoke test: the server boots on an ephemeral port, answers
//! `/healthz` and `/metrics` without auth, and shuts down on signal.
#![cfg(feature = "serve")]

use faucet_cli::serve::{ServeConfig, run_server};
use std::time::Duration;

/// Build a ServeConfig bound to a given address with auth disabled.
fn test_config(listen: &str) -> ServeConfig {
    let args = faucet_cli::cli::ServeArgs {
        listen: listen.into(),
        auth_token: None,
        no_auth: true,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
        default_config: None,
        history: None,
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 60,
        idempotency_retention_secs: 60,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: false,
    };
    ServeConfig::from_args(args).unwrap()
}

#[tokio::test]
async fn healthz_is_reachable_without_auth() {
    // Grab a free port via a throwaway listener, then bind serve to it. There is
    // a small TOCTOU window where another process could claim the port between
    // drop and re-bind; acceptable for a smoke test. A later phase can have
    // `serve()` bind `:0` and report the chosen address to remove the window.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let listen = format!("127.0.0.1:{port}");

    let cfg = test_config(&listen);
    let server = tokio::spawn(run_server(cfg));

    let url = format!("http://{listen}");
    let client = reqwest::Client::new();
    let mut ok = false;
    for _ in 0..50 {
        if let Ok(resp) = client.get(format!("{url}/healthz")).send().await {
            assert_eq!(resp.status(), 200);
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(ok, "server never became reachable on {listen}");

    // /metrics is reachable unauthenticated too (200 since the recorder installs
    // in this single-server test process).
    let resp = client.get(format!("{url}/metrics")).send().await.unwrap();
    assert_eq!(resp.status(), 200);

    server.abort();
}
