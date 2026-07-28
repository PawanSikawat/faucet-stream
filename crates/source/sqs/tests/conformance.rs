//! `faucet-conformance` battery for the SQS source.
//!
//! Check 1 (config-schema validity) and check 6 (errors, not panics) are pure /
//! offline and always run. Check 2 (bounded-memory streaming) boots LocalStack
//! via testcontainers and so requires Docker — it runs in CI alongside the
//! other integration tests.
//!
//! Check 3 (bookmark round-trip) does not apply: the SQS source drains the
//! queue with no resumable bookmark (`bookmark: None` on every page).

use faucet_conformance::{assert_config_schema_valid_value, assert_errors_not_panics};
use faucet_source_sqs::{SqsCredentials, SqsSource, SqsSourceConfig};
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::localstack::LocalStack;

// ── Check 1: config schema ──────────────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(SqsSourceConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-sqs");
}

// ── Check 6: errors, not panics (offline) ───────────────────────────────────

/// Point the source at an unreachable endpoint (`http://127.0.0.1:1`, which
/// refuses connections immediately). `new()` stays lazy — no container needed —
/// and the first `ReceiveMessage` fails with a typed `FaucetError` on both the
/// `fetch_all` and `stream_pages` paths, never a panic.
#[tokio::test(flavor = "multi_thread")]
async fn conformance_errors_not_panics() {
    let mut cfg = SqsSourceConfig::new("https://sqs.us-east-1.amazonaws.com/1/does-not-exist");
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some("http://127.0.0.1:1".into());
    cfg.credentials = test_credentials();
    cfg.wait_time_seconds = 0;
    // A terminating run is required by config validation.
    cfg.idle_timeout_secs = Some(1);
    cfg.max_messages = Some(10);

    let source = SqsSource::new(cfg).await.expect("source builds lazily");
    assert_errors_not_panics(&source).await;
}

// ── Check 2: bounded-memory streaming (Docker) ──────────────────────────────

async fn start_localstack() -> (ContainerAsync<LocalStack>, String) {
    use testcontainers::ImageExt;
    let image = LocalStack::default().with_env_var("SERVICES", "sqs");
    let container = image.start().await.expect("localstack start");
    let port = container
        .get_host_port_ipv4(4566)
        .await
        .expect("localstack port");
    (container, format!("http://127.0.0.1:{port}"))
}

fn test_credentials() -> SqsCredentials {
    SqsCredentials::AccessKey {
        access_key_id: "test".into(),
        secret_access_key: "test".into(),
        session_token: None,
    }
}

async fn raw_client(endpoint: &str) -> aws_sdk_sqs::Client {
    faucet_source_sqs::build_client(Some("us-east-1"), Some(endpoint), &test_credentials())
        .await
        .expect("client")
}

/// Create a queue (large visibility timeout so nothing is redelivered mid-run)
/// and return its URL. Retries until LocalStack's SQS endpoint is ready.
async fn create_queue(client: &aws_sdk_sqs::Client, name: &str) -> String {
    use aws_sdk_sqs::types::QueueAttributeName;
    for _ in 0..120 {
        match client
            .create_queue()
            .queue_name(name)
            .attributes(QueueAttributeName::VisibilityTimeout, "300")
            .send()
            .await
        {
            Ok(out) => return out.queue_url().expect("queue url").to_string(),
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
    panic!("localstack sqs never became ready");
}

/// Send `n` JSON messages in batches of 10.
async fn seed(client: &aws_sdk_sqs::Client, queue_url: &str, n: usize) {
    use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
    let mut i = 0usize;
    while i < n {
        let end = (i + 10).min(n);
        let entries: Vec<SendMessageBatchRequestEntry> = (i..end)
            .map(|j| {
                SendMessageBatchRequestEntry::builder()
                    .id(format!("m{j}"))
                    .message_body(format!("{{\"i\":{j}}}"))
                    .build()
                    .expect("entry")
            })
            .collect();
        let out = client
            .send_message_batch()
            .queue_url(queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .expect("send_message_batch");
        assert!(out.failed().is_empty(), "seeding must not fail");
        i = end;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let (_container, endpoint) = start_localstack().await;
    let client = raw_client(&endpoint).await;
    let queue_url = create_queue(&client, "conformance").await;
    seed(&client, &queue_url, 5_000).await;

    let mut cfg = SqsSourceConfig::new(&queue_url);
    cfg.region = Some("us-east-1".into());
    cfg.endpoint_url = Some(endpoint.clone());
    cfg.credentials = test_credentials();
    cfg.wait_time_seconds = 1;
    cfg.idle_timeout_secs = Some(15);
    cfg.batch_size = 250;

    let source = SqsSource::new(cfg).await.expect("source");
    faucet_conformance::assert_bounded_memory(&source, 250, 5_000).await;
    // _container stays alive to here
}
