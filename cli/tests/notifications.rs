#![cfg(feature = "notify")]
//! Integration tests for the notification delivery path (#280) — real HTTP via
//! wiremock. The unit tests in `src/notify/` cover the pure matching /
//! rendering / coalesce decisions; these cover the wire: Slack / webhook /
//! PagerDuty delivery, HMAC signing, PagerDuty trigger→resolve pairing,
//! leading-edge coalescing, and failure-is-swallowed.

use assert_cmd::Command;
use faucet_cli::notify::{
    ChannelSpec, EventKind, NotificationSpec, Notifier, NotifyEvent, PagerdutyConfig, Severity,
    SlackConfig, WebhookConfig,
};
use serde_json::Value;
use wiremock::matchers::{header_exists, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn rule(name: &str, on: Vec<EventKind>, channel: ChannelSpec) -> NotificationSpec {
    NotificationSpec {
        name: name.into(),
        on,
        min_severity: Severity::Info,
        dedupe_window_secs: None,
        dlq_threshold: None,
        channel,
    }
}

#[tokio::test]
async fn slack_delivery_posts_block_kit_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let spec = rule(
        "slack",
        vec![EventKind::RunFailure],
        ChannelSpec::Slack(SlackConfig {
            webhook_url: format!("{}/hook", server.uri()),
            channel: Some("#ops".into()),
            username: None,
        }),
    );
    let n = Notifier::from_specs(&[spec]).unwrap().unwrap();
    n.emit(NotifyEvent::run_failure("orders", "", "sink", "boom"))
        .await;

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let body: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert!(body["text"].as_str().unwrap().contains("failed"));
    assert_eq!(body["channel"], "#ops");
    assert!(body["blocks"].is_array());
}

#[tokio::test]
async fn webhook_delivery_signs_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .and(header_exists("X-Faucet-Signature"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let spec = rule(
        "wh",
        vec![],
        ChannelSpec::Webhook(WebhookConfig {
            url: format!("{}/hook", server.uri()),
            method: "POST".into(),
            headers: Default::default(),
            hmac_secret: Some("s3cr3t".into()),
            signature_header: "X-Faucet-Signature".into(),
        }),
    );
    let n = Notifier::from_specs(&[spec]).unwrap().unwrap();
    n.emit(NotifyEvent::dlq_threshold("p", "", 5)).await;

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let sig = reqs[0]
        .headers
        .get("X-Faucet-Signature")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(sig.len(), 64, "hex HMAC-SHA256 is 64 chars");
    let body: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    assert_eq!(body["event"], "dlq_threshold");
}

#[tokio::test]
async fn pagerduty_trigger_then_resolve_pairing() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/enqueue"))
        .respond_with(ResponseTemplate::new(202))
        .mount(&server)
        .await;

    let spec = rule(
        "pd",
        vec![EventKind::RunFailure],
        ChannelSpec::Pagerduty(PagerdutyConfig {
            routing_key: "rk".into(),
            source: None,
            endpoint: Some(format!("{}/enqueue", server.uri())),
        }),
    );
    let n = Notifier::from_specs(&[spec]).unwrap().unwrap();
    // Failure opens the incident (trigger); success resolves it.
    n.emit(NotifyEvent::run_failure("orders", "r1", "sink", "boom"))
        .await;
    n.emit(NotifyEvent::run_success("orders", "r1", 3)).await;

    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 2, "one trigger + one resolve");
    let first: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    let second: Value = serde_json::from_slice(&reqs[1].body).unwrap();
    assert_eq!(first["event_action"], "trigger");
    assert_eq!(second["event_action"], "resolve");
    assert_eq!(first["dedup_key"], second["dedup_key"]); // same incident
}

#[tokio::test]
async fn coalescing_drops_repeat_within_window() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .expect(1) // only the first of two identical events is delivered
        .mount(&server)
        .await;

    let mut spec = rule(
        "slack",
        vec![EventKind::RunFailure],
        ChannelSpec::Slack(SlackConfig {
            webhook_url: format!("{}/hook", server.uri()),
            channel: None,
            username: None,
        }),
    );
    spec.dedupe_window_secs = Some(3600);
    let n = Notifier::from_specs(&[spec]).unwrap().unwrap();
    n.emit(NotifyEvent::run_failure("p", "", "sink", "boom"))
        .await;
    n.emit(NotifyEvent::run_failure("p", "", "sink", "boom"))
        .await;

    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn delivery_failure_is_swallowed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let spec = rule(
        "slack",
        vec![],
        ChannelSpec::Slack(SlackConfig {
            webhook_url: format!("{}/hook", server.uri()),
            channel: None,
            username: None,
        }),
    );
    let n = Notifier::from_specs(&[spec]).unwrap().unwrap();
    // Must not panic despite the 500; delivery is fire-and-forget.
    n.emit(NotifyEvent::run_failure("p", "", "sink", "boom"))
        .await;
    // Retried, so at least one request reached the server.
    assert!(!server.received_requests().await.unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn end_to_end_run_notifies_on_success() {
    // Drives the real `faucet run` binary so the executor's notify emit path
    // (run_success + the run.rs wiring) is exercised end-to-end.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let dir = tempfile::tempdir().unwrap();
    let input = dir.path().join("in.csv");
    std::fs::write(&input, "name\nalice\nbob\n").unwrap();
    let out = dir.path().join("out.jsonl");
    let cfg = format!(
        "version: 1\nname: notif_e2e\npipeline:\n  source:\n    type: csv\n    config:\n      path: \"{}\"\n  sink:\n    type: jsonl\n    config:\n      path: \"{}\"\nnotifications:\n  - name: slack\n    on: [run_success]\n    channel:\n      type: slack\n      config:\n        webhook_url: \"{}/hook\"\n",
        input.display(),
        out.display(),
        server.uri()
    );
    let cfg_path = dir.path().join("p.yaml");
    std::fs::write(&cfg_path, cfg).unwrap();

    // `.assert()` blocks until the child exits; run it off the async workers.
    let cfg_path2 = cfg_path.clone();
    tokio::task::spawn_blocking(move || {
        Command::cargo_bin("faucet")
            .unwrap()
            .args(["run"])
            .arg(&cfg_path2)
            .assert()
            .success();
    })
    .await
    .unwrap();

    // The notify emit is awaited inside the run, so by process exit it landed.
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn severity_floor_filters_delivery() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/hook"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;

    let mut spec = rule(
        "slack",
        vec![],
        ChannelSpec::Slack(SlackConfig {
            webhook_url: format!("{}/hook", server.uri()),
            channel: None,
            username: None,
        }),
    );
    spec.min_severity = Severity::Error;
    let n = Notifier::from_specs(&[spec]).unwrap().unwrap();
    // run_success is Info < Error → gated (no request); circuit_open is Critical → delivered.
    n.emit(NotifyEvent::run_success("p", "", 1)).await;
    assert!(server.received_requests().await.unwrap().is_empty());
    n.emit(NotifyEvent::circuit_open("p", "", 3, 30)).await;
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}
