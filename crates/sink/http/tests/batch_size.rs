//! Integration tests for the HTTP sink's `batch_size` re-chunking
//! behaviour in Array mode. Uses wiremock to capture the number of
//! outbound POST requests for a single `write_batch` call.
//!
//! `batch_size` is a no-op for the wire in Individual mode (the sink
//! already issues one request per record) — those tests live in
//! `src/sink.rs`. Here we focus on the Array-mode re-chunking semantics
//! that Plan 18 adds.

use faucet_core::Sink;
use faucet_sink_http::{HttpBatchMode, HttpSink, HttpSinkConfig};
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn make_records(n: usize) -> Vec<Value> {
    (0..n).map(|i| json!({"id": i, "name": "row"})).collect()
}

async fn mock_server_with_success() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    server
}

fn url(server: &MockServer) -> String {
    format!("{}/ingest", server.uri())
}

#[tokio::test]
async fn array_mode_rechunks_into_batch_size_requests() {
    // 1500 records with batch_size = 500 → 3 POSTs (500, 500, 500).
    let server = mock_server_with_success().await;
    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Array)
        .with_batch_size(500);
    let sink = HttpSink::new(config);

    let written = sink.write_batch(&make_records(1_500)).await.unwrap();
    assert_eq!(written, 1_500);

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        3,
        "expected exactly 3 POSTs (1500 records / batch_size=500)"
    );

    // Each request body must be a JSON array of <= 500 records.
    for req in &requests {
        let body: Value = serde_json::from_slice(&req.body).unwrap();
        let arr = body.as_array().expect("body is JSON array");
        assert!(arr.len() <= 500);
    }
}

#[tokio::test]
async fn array_mode_rechunks_uneven_remainder() {
    // 1200 records with batch_size = 500 → 3 POSTs (500, 500, 200).
    let server = mock_server_with_success().await;
    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Array)
        .with_batch_size(500);
    let sink = HttpSink::new(config);

    sink.write_batch(&make_records(1_200)).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);

    let last_body: Value = serde_json::from_slice(&requests[2].body).unwrap();
    assert_eq!(
        last_body.as_array().unwrap().len(),
        200,
        "tail chunk must hold the remaining 200 records"
    );
}

#[tokio::test]
async fn array_mode_sentinel_zero_sends_single_request() {
    // batch_size = 0 → forward the whole slice as a single POST body, no
    // matter how large.
    let server = mock_server_with_success().await;
    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Array)
        .with_batch_size(0);
    let sink = HttpSink::new(config);

    sink.write_batch(&make_records(2_500)).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        1,
        "batch_size = 0 must forward the whole slice in a single POST"
    );
    let body: Value = serde_json::from_slice(&requests[0].body).unwrap();
    assert_eq!(body.as_array().unwrap().len(), 2_500);
}

#[tokio::test]
async fn array_mode_smaller_than_batch_size_makes_one_request() {
    let server = mock_server_with_success().await;
    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Array)
        .with_batch_size(500);
    let sink = HttpSink::new(config);

    sink.write_batch(&make_records(42)).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
}

#[tokio::test]
async fn array_mode_empty_records_makes_no_requests() {
    let server = mock_server_with_success().await;
    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Array)
        .with_batch_size(500);
    let sink = HttpSink::new(config);

    let written = sink.write_batch(&[]).await.unwrap();
    assert_eq!(written, 0);

    let requests = server.received_requests().await.unwrap();
    assert!(requests.is_empty());
}

#[tokio::test]
async fn individual_mode_ignores_batch_size_for_wire_framing() {
    // In Individual mode batch_size has no effect — the sink still issues
    // one request per record, regardless of batch_size.
    let server = mock_server_with_success().await;
    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Individual)
        .with_batch_size(500)
        .concurrency(16);
    let sink = HttpSink::new(config);

    sink.write_batch(&make_records(10)).await.unwrap();

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        10,
        "Individual mode sends one request per record regardless of batch_size"
    );
}

/// Regression for #59 — the previous Individual-mode implementation used a
/// `Semaphore` and collected futures in a `Vec`, but never started any
/// future until after the for-loop finished. That deadlocked at the
/// `(concurrency + 1)`th `acquire_owned().await` call because no permit had
/// ever been released. The fix uses `buffer_unordered(concurrency)` instead,
/// which actually polls in-flight futures concurrently.
///
/// Asserts: 100 records with `concurrency = 4` completes (would hang in the
/// old impl). The mock returns instantly so completion is the test.
#[tokio::test]
async fn individual_mode_does_not_deadlock_when_records_exceed_concurrency() {
    let server = mock_server_with_success().await;
    let config = HttpSinkConfig::new(url(&server))
        .batch_mode(HttpBatchMode::Individual)
        .concurrency(4);
    let sink = HttpSink::new(config);

    // Wrap in a timeout so a regression here surfaces as a test failure
    // rather than a hung test runner.
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        sink.write_batch(&make_records(100)),
    )
    .await
    .expect("Individual mode write_batch deadlocked (regression of #59)");
    result.expect("Individual mode write_batch returned an error");

    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 100);
}
