//! Integration tests for [`ElasticsearchSink::write_batch_partial`].
//!
//! Each test stands up a wiremock server, mounts a `_bulk` endpoint, and
//! drives the sink — asserting on per-row [`faucet_core::RowOutcome`]s.

use faucet_core::{FaucetError, Sink};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn errors_false_returns_all_ok() {
    let server = MockServer::start().await;
    let body = json!({
        "errors": false,
        "items": [
            { "index": { "status": 201 } },
            { "index": { "status": 201 } }
        ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let cfg = ElasticsearchSinkConfig::new(server.uri(), "idx");
    let sink = ElasticsearchSink::new(cfg);
    let outcomes = sink
        .write_batch_partial(&[json!({"a": 1}), json!({"a": 2})])
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes.iter().all(|o| o.is_ok()));
}

#[tokio::test]
async fn item_level_errors_route_by_index() {
    let server = MockServer::start().await;
    let body = json!({
        "errors": true,
        "items": [
            { "index": { "status": 201 } },
            { "index": { "status": 400, "error": { "reason": "mapping" } } },
            { "index": { "status": 201 } }
        ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let cfg = ElasticsearchSinkConfig::new(server.uri(), "idx");
    let sink = ElasticsearchSink::new(cfg);
    let outcomes = sink
        .write_batch_partial(&[json!({"a": 1}), json!({"a": 2}), json!({"a": 3})])
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 3);
    assert!(outcomes[0].is_ok());
    assert!(outcomes[1].is_err());
    assert!(outcomes[2].is_ok());
}

#[tokio::test]
async fn http_failure_bubbles_outer_err() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .mount(&server)
        .await;
    let cfg = ElasticsearchSinkConfig::new(server.uri(), "idx");
    let sink = ElasticsearchSink::new(cfg);
    let result = sink.write_batch_partial(&[json!({"a": 1})]).await;
    assert!(matches!(result, Err(FaucetError::HttpStatus { .. })));
}

#[tokio::test]
async fn truncated_response_pads_with_failures() {
    let server = MockServer::start().await;
    let body = json!({
        "errors": false,
        "items": [ { "index": { "status": 201 } } ]
    });
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;
    let cfg = ElasticsearchSinkConfig::new(server.uri(), "idx");
    let sink = ElasticsearchSink::new(cfg);
    let outcomes = sink
        .write_batch_partial(&[json!({"a": 1}), json!({"a": 2})])
        .await
        .unwrap();
    assert_eq!(outcomes.len(), 2);
    assert!(outcomes[0].is_ok());
    assert!(outcomes[1].is_err());
}
