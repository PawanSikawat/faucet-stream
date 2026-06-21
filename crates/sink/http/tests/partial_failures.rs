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

#[tokio::test]
async fn array_mode_multi_chunk_only_failed_chunk_rows_reported_failed() {
    // F15 / audit #264: a page that spans MULTIPLE batch_size chunks must NOT
    // dead-letter already-delivered chunks when a LATER chunk fails. The first
    // chunk [0,1] is delivered (200); the second chunk [2,3] fails (400). Only
    // the second chunk's rows may be reported failed — the first chunk's rows
    // were already delivered to the live endpoint and must stay Ok, or the DLQ
    // would silently duplicate them downstream.
    let server = MockServer::start().await;

    // First chunk: exactly [{id:0},{id:1}] → 200 (delivered).
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .and(body_json(json!([{ "id": 0 }, { "id": 1 }])))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    // Second chunk: exactly [{id:2},{id:3}] → 400 (non-retriable failure).
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .and(body_json(json!([{ "id": 2 }, { "id": 3 }])))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;

    let config = HttpSinkConfig::new(format!("{}/ingest", server.uri()))
        .batch_mode(HttpBatchMode::Array)
        .with_batch_size(2);
    let sink = HttpSink::new(config);

    let records: Vec<_> = (0..4).map(|i| json!({ "id": i })).collect();
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("a later-chunk failure must report per-row outcomes, not an outer error");

    assert_eq!(outcomes.len(), 4, "one outcome per record");
    assert!(
        outcomes[0].is_ok(),
        "record 0 was in the delivered first chunk"
    );
    assert!(
        outcomes[1].is_ok(),
        "record 1 was in the delivered first chunk"
    );
    assert!(
        outcomes[2].is_err(),
        "record 2 was in the failed second chunk"
    );
    assert!(
        outcomes[3].is_err(),
        "record 3 was in the failed second chunk"
    );

    // Both chunks were actually POSTed (the failure is observed, not skipped).
    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        2,
        "exactly two chunk POSTs (2 records each)"
    );
}

#[tokio::test]
async fn array_mode_multi_chunk_first_chunk_failure_surfaces_outer_error() {
    // When the FIRST chunk fails (nothing delivered yet) the original
    // all-or-nothing contract is preserved: an outer error so the router's
    // `on_batch_error` policy still governs a wholly-undelivered page.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;

    let config = HttpSinkConfig::new(format!("{}/ingest", server.uri()))
        .batch_mode(HttpBatchMode::Array)
        .with_batch_size(2);
    let sink = HttpSink::new(config);

    let records: Vec<_> = (0..4).map(|i| json!({ "id": i })).collect();
    let result = sink.write_batch_partial(&records).await;
    assert!(
        result.is_err(),
        "first-chunk failure (nothing delivered) must surface as an outer error"
    );
}

#[tokio::test]
async fn array_mode_multi_chunk_unsent_chunks_after_failure_reported_failed() {
    // Three chunks of size 1: chunk 0 (id=0) delivered, chunk 1 (id=1) fails,
    // chunk 2 (id=2) is never sent. The delivered row stays Ok; the failed and
    // the never-sent rows are both reported failed so the DLQ captures every
    // undelivered row, and the never-sent chunk is NOT POSTed.
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .and(body_json(json!([{ "id": 0 }])))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/ingest"))
        .and(body_json(json!([{ "id": 1 }])))
        .respond_with(ResponseTemplate::new(400))
        .mount(&server)
        .await;
    // No mock for [{id:2}] — asserting it is never sent.

    let config = HttpSinkConfig::new(format!("{}/ingest", server.uri()))
        .batch_mode(HttpBatchMode::Array)
        .with_batch_size(1);
    let sink = HttpSink::new(config);

    let records: Vec<_> = (0..3).map(|i| json!({ "id": i })).collect();
    let outcomes = sink
        .write_batch_partial(&records)
        .await
        .expect("per-row outcomes once an earlier chunk was delivered");

    assert_eq!(outcomes.len(), 3, "one outcome per record");
    assert!(outcomes[0].is_ok(), "record 0 delivered");
    assert!(outcomes[1].is_err(), "record 1 failed");
    assert!(
        outcomes[2].is_err(),
        "record 2 never sent → reported failed"
    );

    let requests = server.received_requests().await.unwrap();
    assert_eq!(
        requests.len(),
        2,
        "only chunk 0 and chunk 1 are POSTed; chunk 2 is short-circuited"
    );
}
