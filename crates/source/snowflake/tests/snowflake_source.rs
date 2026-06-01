//! Integration tests for the Snowflake source.
//!
//! Drives [`SnowflakeSource`] against a wiremock server mocking the SQL REST
//! API. Verifies that the source:
//!
//! - sends the right auth headers and request body shape,
//! - paginates across all `partitionInfo` partitions,
//! - re-frames partitions into pages of `batch_size`,
//! - surfaces Snowflake error codes as `FaucetError::Source`.

use std::collections::HashMap;

use faucet_core::Source;
use faucet_source_snowflake::{SnowflakeAuth, SnowflakeSource, SnowflakeSourceConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn cfg() -> SnowflakeSourceConfig {
    SnowflakeSourceConfig::new(
        "xy12345",
        "WH",
        "DB",
        "PUBLIC",
        SnowflakeAuth::OAuth { token: "t".into() },
        "SELECT id, name, active FROM events",
    )
    .with_role("ANALYST")
    .with_batch_size(10)
    .with_statement_timeout(std::time::Duration::from_secs(5))
}

fn build_source(cfg: SnowflakeSourceConfig, server: &MockServer) -> SnowflakeSource {
    SnowflakeSource::new(cfg)
        .unwrap()
        .with_endpoint_base(server.uri())
}

fn metadata(num_partitions: usize) -> Value {
    let partition_info: Vec<Value> = (0..num_partitions)
        .map(|_| json!({"rowCount": 5}))
        .collect();
    json!({
        "rowType": [
            {"name": "ID", "type": "fixed"},
            {"name": "NAME", "type": "text"},
            {"name": "ACTIVE", "type": "boolean"}
        ],
        "partitionInfo": partition_info,
        "format": "jsonv2",
        "numRows": (num_partitions * 5) as u64,
    })
}

fn rows_for_partition(p: usize) -> Vec<Vec<Value>> {
    (0..5)
        .map(|i| {
            let row_id = p * 5 + i;
            vec![
                json!(row_id.to_string()),
                json!(format!("name-{row_id}")),
                json!(if row_id.is_multiple_of(2) {
                    "true"
                } else {
                    "false"
                }),
            ]
        })
        .collect()
}

#[tokio::test]
async fn check_probes_with_select_1_not_the_configured_query() {
    use faucet_core::check::{CheckContext, ProbeStatus};
    use wiremock::matchers::body_partial_json;

    let server = MockServer::start().await;
    // Mock matches ONLY a `SELECT 1` statement body. If `check()` submitted the
    // configured query ("SELECT id, name, active FROM events") — i.e. a billed
    // execution — this mock would not match, wiremock would 404, and the probe
    // would fail. A passing probe therefore proves the cheap `SELECT 1` is used.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_partial_json(json!({"statement": "SELECT 1"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"code": "090001"})))
        .mount(&server)
        .await;

    let source = build_source(cfg(), &server);
    let report = source.check(&CheckContext::default()).await.unwrap();
    assert!(
        report
            .probes
            .iter()
            .all(|p| matches!(p.status, ProbeStatus::Pass)),
        "doctor probe must pass against the SELECT 1 mock (proving it didn't run \
         the configured query): {report:?}"
    );
}

#[tokio::test]
async fn fetch_all_returns_first_partition_when_no_partitions() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Snowflake Token=\"t\""))
        .and(header("X-Snowflake-Authorization-Token-Type", "OAUTH"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "handle-1",
            "resultSetMetaData": metadata(1),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 5);
    assert_eq!(rows[0]["ID"], 0);
    assert_eq!(rows[0]["NAME"], "name-0");
    assert_eq!(rows[0]["ACTIVE"], true);
    assert_eq!(rows[1]["ACTIVE"], false);
}

#[tokio::test]
async fn fetch_all_paginates_across_partitions() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "handle-1",
            "resultSetMetaData": metadata(3),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/handle-1"))
        .and(query_param("partition", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": rows_for_partition(1),
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/handle-1"))
        .and(query_param("partition", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": rows_for_partition(2),
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let rows = src.fetch_all().await.unwrap();
    assert_eq!(rows.len(), 15);
    assert_eq!(rows[0]["ID"], 0);
    assert_eq!(rows[14]["ID"], 14);
}

#[tokio::test]
async fn stream_pages_chunks_by_batch_size() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h",
            "resultSetMetaData": metadata(3),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;
    for p in 1..3 {
        Mock::given(method("GET"))
            .and(path("/api/v2/statements/h"))
            .and(query_param("partition", p.to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "code": "090001",
                "data": rows_for_partition(p),
            })))
            .mount(&server)
            .await;
    }

    // batch_size = 4 → 15 rows should yield ⌈15/4⌉ = 4 pages of [4, 4, 4, 3].
    let mut c = cfg();
    c.batch_size = 4;
    let src = build_source(c, &server);
    let ctx = HashMap::new();
    let mut pages = src.stream_pages(&ctx, 0);
    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        assert!(page.bookmark.is_none());
        sizes.push(page.records.len());
    }
    assert_eq!(sizes, vec![4, 4, 4, 3]);
}

#[tokio::test]
async fn stream_pages_batch_size_zero_emits_one_page() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h",
            "resultSetMetaData": metadata(2),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/h"))
        .and(query_param("partition", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "data": rows_for_partition(1),
        })))
        .mount(&server)
        .await;

    let mut c = cfg();
    c.batch_size = 0;
    let src = build_source(c, &server);
    let ctx = HashMap::new();
    let pages: Vec<_> = src.stream_pages(&ctx, 0).collect().await;
    assert_eq!(pages.len(), 1);
    assert_eq!(pages[0].as_ref().unwrap().records.len(), 10);
}

#[tokio::test]
async fn submits_bindings_for_configured_params_and_context() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "090001",
            "statementHandle": "h",
            "resultSetMetaData": metadata(1),
            "data": rows_for_partition(0),
        })))
        .mount(&server)
        .await;

    let mut c = cfg();
    c.query = "SELECT * FROM events WHERE region = ? AND id > {parent.min_id}".into();
    c.params = vec![json!("us-east")];
    let src = build_source(c, &server);

    let mut ctx = HashMap::new();
    ctx.insert("parent.min_id".to_string(), json!(100));
    src.fetch_with_context(&ctx).await.unwrap();

    let reqs = server.received_requests().await.unwrap();
    let body: Value = serde_json::from_slice(&reqs[0].body).unwrap();
    // Query should have the `{parent.min_id}` token replaced by `?`.
    assert_eq!(
        body["statement"],
        "SELECT * FROM events WHERE region = ? AND id > ?"
    );
    let bindings = &body["bindings"];
    assert_eq!(bindings["1"]["value"], "us-east");
    assert_eq!(bindings["2"]["value"], "100");
    assert_eq!(body["role"], "ANALYST");
    assert_eq!(body["timeout"], 5);
}

#[tokio::test]
async fn surfaces_non_success_code_as_source_error() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "code": "000604",
            "message": "SQL execution canceled",
        })))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        faucet_core::FaucetError::Source(msg) => {
            assert!(msg.contains("000604"));
            assert!(msg.contains("canceled"));
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}

#[tokio::test]
async fn http_error_surfaces_as_source_error_with_status() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .mount(&server)
        .await;

    let src = build_source(cfg(), &server);
    let err = src.fetch_all().await.unwrap_err();
    match err {
        faucet_core::FaucetError::Source(msg) => {
            assert!(msg.contains("401"));
        }
        other => panic!("expected Source error, got {other:?}"),
    }
}
