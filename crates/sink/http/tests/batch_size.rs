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
    //
    // Note: `concurrency` is set >= record count to side-step a separate
    // pre-existing deadlock in `write_batch` Individual mode tracked in
    // #59 — the semaphore acquire happens in a sequential loop before any
    // future runs, so `records.len() > concurrency` blocks forever waiting
    // for a permit that never gets released.
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
