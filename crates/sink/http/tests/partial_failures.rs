//! Integration tests for the HTTP sink's `write_batch_partial` (#146 M14).
//!
//! In Individual mode each record is an independent POST, so a partial
//! failure must be reported per-row — only the genuinely failed record is
//! dead-lettered, not the whole batch (which, under `dlq_all`, would duplicate
//! the already-delivered rows against a non-idempotent endpoint). In Array
//! mode a single array POST cannot attribute a failure to specific rows, so the
//! whole batch surfaces as an outer error (the default all-or-nothing path).

use faucet_core::Sink;
use faucet_sink_http::{HttpBatchMode, HttpSink, HttpSinkConfig};
use serde_json::json;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn individual_mode_write_batch_partial_reports_only_the_failed_row() {
    let server = MockServer::start().await;
    // Exact per-record body matchers (mutually exclusive) so each POST matches
    // exactly one mock. Record id=1 gets a non-retriable 400; the rest 200.
    for id in [0_u64, 2, 3] {
        Mock::given(method("POST"))
            .and(path("/ingest"))
            .and(body_json(json!({ "id": id })))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .and(body_json(json!({ "id": 1 })))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;

    let config = HttpSinkConfig::new(format!("{}/ingest", server.uri()))
        .batch_mode(HttpBatchMode::Individual)
        .concurrency(4);
    let sink = HttpSink::new(config);

    let records: Vec<_> = (0..4).map(|i| json!({ "id": i })).collect();
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("partial write must not surface an outer error in Individual mode");

    assert_eq!(outcomes.len(), 4, "one outcome per record");
    assert!(outcomes[0].is_ok());
    assert!(
        outcomes[1].is_err(),
        "only the 400 record (id=1) is a failure"
    );
    assert!(outcomes[2].is_ok());
    assert!(outcomes[3].is_ok());

    // All four records were actually POSTed (the failure did not short-circuit
    // the siblings the way the first-error `?` in write_batch does).
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 4, "every record is POSTed exactly once");
}

#[tokio::test]
async fn array_mode_write_batch_partial_surfaces_outer_error() {
    // A single array POST can't attribute a failure to specific rows, so the
    // whole batch fails as an outer error (the router then applies on_batch_error).
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;

    let config = HttpSinkConfig::new(format!("{}/ingest", server.uri()))
        .batch_mode(HttpBatchMode::Array)
        .with_batch_size(0);
    let sink = HttpSink::new(config);

    let records: Vec<_> = (0..3).map(|i| json!({ "id": i })).collect();
    let result = sink.write_batch_partial(&records).await;
    assert!(
        result.is_err(),
        "array-mode failure must surface as an outer error, not per-row outcomes"
    );
}
