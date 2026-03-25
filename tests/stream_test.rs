use faucet_stream::{
    Auth, FaucetError, PaginationStyle, RecordTransform, ReplicationMethod, RestStream,
    RestStreamConfig,
};
use futures::StreamExt;
use serde_json::json;
use std::collections::HashMap;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn test_single_page_fetch() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/users")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["name"], "Alice");
}

#[tokio::test]
async fn test_cursor_pagination() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1}, {"id": 2}],
            "next_cursor": "page2"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("cursor", "page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 3}],
            "next_cursor": null
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/items")
            .records_path("$.items[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.next_cursor".into(),
                param_name: "cursor".into(),
            }),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
}

#[tokio::test]
async fn test_typed_deserialization() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "name": "Alice", "email": "alice@example.com"},
            ]
        })))
        .mount(&server)
        .await;

    #[derive(Debug, serde::Deserialize, PartialEq)]
    struct User {
        id: u64,
        name: String,
        email: String,
    }

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/users").records_path("$.data[*]"),
    )
    .unwrap();

    let users: Vec<User> = stream.fetch_all_as().await.unwrap();
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].name, "Alice");
}

#[tokio::test]
async fn test_link_header_pagination() {
    let server = MockServer::start().await;
    let page2_url = format!("{}/api/items?page=2", server.uri());

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"items": [{"id": 1}, {"id": 2}]}))
                .append_header("link", format!(r#"<{page2_url}>; rel="next""#).as_str()),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": [{"id": 3}]})))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/items")
            .records_path("$.items[*]")
            .pagination(PaginationStyle::LinkHeader),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[2]["id"], 3);
}

#[tokio::test]
async fn test_next_link_in_body_pagination() {
    let server = MockServer::start().await;
    let page2_url = format!("{}/api/workers?page=2", server.uri());

    Mock::given(method("GET"))
        .and(path("/api/workers"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id": 1}, {"id": 2}],
            "next_link": page2_url,
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/workers"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id": 3}],
            "next_link": null,
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/workers")
            .records_path("$.results[*]")
            .pagination(PaginationStyle::NextLinkInBody {
                next_link_path: "$.next_link".into(),
            }),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[2]["id"], 3);
}

#[tokio::test]
async fn test_max_pages_enforced_for_cursor_pagination() {
    let server = MockServer::start().await;

    // Page 1 (no cursor param) → returns cursor "page2"
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1}],
            "next_cursor": "page2"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Page 2 → returns cursor "page3"
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("cursor", "page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 2}],
            "next_cursor": "page3"
        })))
        .mount(&server)
        .await;

    // Page 3 → returns cursor "page4" (but max_pages will stop here)
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("cursor", "page3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 3}],
            "next_cursor": "page4"
        })))
        .mount(&server)
        .await;

    // Page 4 should never be fetched.
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("cursor", "page4"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 4}],
            "next_cursor": null
        })))
        .expect(0)
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/items")
            .records_path("$.items[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.next_cursor".into(),
                param_name: "cursor".into(),
            })
            .max_pages(3),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    // max_pages(3) → exactly 3 pages fetched, each with 1 record.
    assert_eq!(records.len(), 3);
}

#[tokio::test]
async fn test_bearer_auth_sent() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/secure"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": []})))
        .expect(1)
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/secure")
            .auth(Auth::Bearer("my-secret-token".into()))
            .records_path("$.data[*]"),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert!(records.is_empty());
}

#[tokio::test]
async fn test_stream_pages_yields_per_page() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1}, {"id": 2}],
            "next_cursor": "page2"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("cursor", "page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 3}],
            "next_cursor": null
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/items")
            .records_path("$.items[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.next_cursor".into(),
                param_name: "cursor".into(),
            }),
    )
    .unwrap();

    let mut pages = stream.stream_pages();

    let page1 = pages.next().await.unwrap().unwrap();
    assert_eq!(page1.len(), 2);
    assert_eq!(page1[0]["id"], 1);

    let page2 = pages.next().await.unwrap().unwrap();
    assert_eq!(page2.len(), 1);
    assert_eq!(page2[0]["id"], 3);

    assert!(pages.next().await.is_none());
}

// ── Incremental replication ───────────────────────────────────────────────────

#[tokio::test]
async fn test_incremental_replication_filters_old_records() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {"id": 1, "updated_at": "2024-01-01"},
                {"id": 2, "updated_at": "2024-06-01"},
                {"id": 3, "updated_at": "2024-12-01"},
            ]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/events")
            .records_path("$.items[*]")
            .replication_method(ReplicationMethod::Incremental)
            .replication_key("updated_at")
            .start_replication_value(json!("2024-06-01")),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    // Records at or before "2024-06-01" are filtered out; only id=3 remains.
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 3);
}

#[tokio::test]
async fn test_fetch_all_incremental_returns_bookmark() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {"id": 1, "updated_at": "2024-06-15"},
                {"id": 2, "updated_at": "2024-11-30"},
                {"id": 3, "updated_at": "2024-08-01"},
            ]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/events")
            .records_path("$.items[*]")
            .replication_key("updated_at"),
    )
    .unwrap();

    let (records, bookmark) = stream.fetch_all_incremental().await.unwrap();
    assert_eq!(records.len(), 3);
    // Bookmark is the maximum replication key value seen.
    assert_eq!(bookmark.unwrap(), json!("2024-11-30"));
}

#[tokio::test]
async fn test_full_table_mode_does_not_filter() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {"id": 1, "updated_at": "2023-01-01"},
                {"id": 2, "updated_at": "2024-01-01"},
            ]
        })))
        .mount(&server)
        .await;

    // FullTable with replication_key + start_value set: no filtering should occur.
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/events")
            .records_path("$.items[*]")
            .replication_method(ReplicationMethod::FullTable)
            .replication_key("updated_at")
            .start_replication_value(json!("2023-06-01")),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
}

// ── Partitions ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_partitions_fetch_each_context() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/orgs/acme/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "users": [{"id": 1, "org": "acme"}]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/orgs/beta/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "users": [{"id": 2, "org": "beta"}, {"id": 3, "org": "beta"}]
        })))
        .mount(&server)
        .await;

    let mut p1 = HashMap::new();
    p1.insert("org_id".to_string(), json!("acme"));

    let mut p2 = HashMap::new();
    p2.insert("org_id".to_string(), json!("beta"));

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/orgs/{org_id}/users")
            .records_path("$.users[*]")
            .add_partition(p1)
            .add_partition(p2),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["org"], "acme");
    assert_eq!(records[1]["org"], "beta");
}

// ── HTTP 429 / Retry-After ────────────────────────────────────────────────────

#[tokio::test]
async fn test_429_retries_after_header_delay() {
    let server = MockServer::start().await;

    // First call: 429 with Retry-After: 1
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(429).append_header("retry-after", "1"))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Second call: success
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"items": [{"id": 1}]})))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/items")
            .records_path("$.items[*]")
            .max_retries(3),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
}

// ── Tolerated HTTP errors ─────────────────────────────────────────────────────

#[tokio::test]
async fn test_tolerated_http_error_returns_empty_page() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/missing"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/missing").tolerate_http_error(404),
    )
    .unwrap();

    // 404 is tolerated: should return empty vec, not an error.
    let records = stream.fetch_all().await.unwrap();
    assert!(records.is_empty());
}

#[tokio::test]
async fn test_untolerated_http_error_propagates() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/missing"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let stream =
        RestStream::new(RestStreamConfig::new(&server.uri(), "/api/missing").max_retries(0))
            .unwrap();

    // 404 not tolerated: should propagate as an error.
    assert!(stream.fetch_all().await.is_err());
}

// ── Metadata fields (compile-time / builder checks) ───────────────────────────

#[test]
fn test_metadata_fields_builder() {
    let cfg = RestStreamConfig::new("https://api.example.com", "/users")
        .name("users")
        .primary_keys(vec!["id".to_string()])
        .schema(json!({
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"}
            }
        }));

    assert_eq!(cfg.name.as_deref(), Some("users"));
    assert_eq!(cfg.primary_keys, vec!["id"]);
    assert!(cfg.schema.is_some());
}

// ── Schema inference ──────────────────────────────────────────────────────────

#[tokio::test]
async fn test_infer_schema_from_api_response() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "name": "Alice", "email": "alice@example.com", "score": 9.5},
                {"id": 2, "name": "Bob",   "score": 8.0},
            ]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/users").records_path("$.data[*]"),
    )
    .unwrap();

    let schema = stream.infer_schema().await.unwrap();

    assert_eq!(schema["type"], "object");
    let props = &schema["properties"];
    assert_eq!(props["id"]["type"], "integer");
    assert_eq!(props["name"]["type"], "string");
    assert_eq!(props["score"]["type"], "number");
    // email is absent from Bob's record → nullable
    let email_type = &props["email"]["type"];
    assert!(
        email_type == &json!(["null", "string"]) || email_type == &json!(["string", "null"]),
        "expected nullable string for email, got {email_type}"
    );
}

#[tokio::test]
async fn test_infer_schema_returns_existing_schema_without_request() {
    // No mock server needed — infer_schema should return the pre-set schema
    // without making any HTTP requests.
    let explicit_schema = json!({
        "type": "object",
        "properties": {"id": {"type": "integer"}}
    });

    let stream = RestStream::new(
        RestStreamConfig::new("http://localhost:19999", "/api/never-called")
            .schema(explicit_schema.clone()),
    )
    .unwrap();

    let result = stream.infer_schema().await.unwrap();
    assert_eq!(result, explicit_schema);
}

#[tokio::test]
async fn test_infer_schema_sample_size_limits_requests() {
    let server = MockServer::start().await;

    // Page 1: 3 records
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                {"id": 1, "updated_at": "2024-01-01"},
                {"id": 2, "updated_at": "2024-02-01"},
                {"id": 3, "updated_at": "2024-03-01"},
            ],
            "next_cursor": "page2"
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    // Page 2 is registered but should never be hit (sample_size = 2).
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("cursor", "page2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 4, "updated_at": "2024-04-01"}],
            "next_cursor": null
        })))
        .expect(0) // must not be called
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/items")
            .records_path("$.items[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.next_cursor".into(),
                param_name: "cursor".into(),
            })
            .schema_sample_size(2),
    )
    .unwrap();

    let schema = stream.infer_schema().await.unwrap();
    assert_eq!(schema["type"], "object");
    assert_eq!(schema["properties"]["id"]["type"], "integer");
}

// ── Record transforms (integration) ──────────────────────────────────────────

#[cfg(feature = "transform-flatten")]
#[tokio::test]
async fn test_flatten_transform_applied_to_records() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {"id": 1, "address": {"city": "NYC", "zip": "10001"}},
                {"id": 2, "address": {"city": "LA",  "zip": "90001"}},
            ]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/users")
            .records_path("$.data[*]")
            .add_transform(RecordTransform::Flatten {
                separator: "__".into(),
            }),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[0]["address__city"], "NYC");
    assert_eq!(records[0]["address__zip"], "10001");
    assert!(
        records[0].get("address").is_none(),
        "nested key should be gone"
    );
}

#[cfg(feature = "transform-snake-case")]
#[tokio::test]
async fn test_keys_to_snake_case_transform() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"First Name": "Alice", "Last Name": "Smith", "price ($)": 9.99}]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/users")
            .records_path("$.data[*]")
            .add_transform(RecordTransform::KeysToSnakeCase),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records[0]["first_name"], "Alice");
    assert_eq!(records[0]["last_name"], "Smith");
    assert_eq!(records[0]["price"], 9.99);
}

#[cfg(feature = "transform-rename-keys")]
#[tokio::test]
async fn test_rename_keys_transform() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/events"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"_sdc_id": 1, "_sdc_name": "event_one"}]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/events")
            .records_path("$.data[*]")
            .add_transform(RecordTransform::RenameKeys {
                pattern: r"^_sdc_".into(),
                replacement: "".into(),
            }),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records[0]["id"], 1);
    assert_eq!(records[0]["name"], "event_one");
}

#[cfg(all(feature = "transform-snake-case", feature = "transform-flatten"))]
#[tokio::test]
async fn test_chained_transforms_snake_case_then_flatten() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/data"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"User Info": {"First Name": "Alice"}}]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/data")
            .records_path("$.data[*]")
            .add_transform(RecordTransform::KeysToSnakeCase)
            .add_transform(RecordTransform::Flatten {
                separator: "_".into(),
            }),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    // snake_case: {"user_info": {"first_name": "Alice"}}
    // flatten with "_": {"user_info_first_name": "Alice"}
    assert_eq!(records[0]["user_info_first_name"], "Alice");
}

#[cfg(feature = "transform-rename-keys")]
#[test]
fn test_invalid_regex_errors_at_construction() {
    let result = RestStream::new(
        RestStreamConfig::new("http://localhost", "/api").add_transform(
            RecordTransform::RenameKeys {
                pattern: "[invalid".into(),
                replacement: "".into(),
            },
        ),
    );
    assert!(result.is_err());
    assert!(matches!(
        result,
        Err(faucet_stream::FaucetError::Transform(_))
    ));
}

#[tokio::test]
async fn test_custom_transform_applied_to_records() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1, "value": 10}, {"id": 2, "value": 20}]
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/items")
            .records_path("$.data[*]")
            // Double the "value" field and inject a "_source" tag.
            .add_transform(RecordTransform::custom(|mut record| {
                if let serde_json::Value::Object(ref mut m) = record {
                    if let Some(v) = m.get("value").and_then(|v| v.as_i64()) {
                        m.insert("value".to_string(), json!(v * 2));
                    }
                    m.insert("_source".to_string(), json!("test-api"));
                }
                record
            })),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records[0]["value"], 20);
    assert_eq!(records[1]["value"], 40);
    assert_eq!(records[0]["_source"], "test-api");
    assert_eq!(records[1]["_source"], "test-api");
}

// ── ApiKeyQuery ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_api_key_query_sent_as_param() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/items"))
        .and(query_param("api_key", "my-secret"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{"id": 1}]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/items")
            .records_path("$.data[*]")
            .auth(Auth::ApiKeyQuery {
                param: "api_key".into(),
                value: "my-secret".into(),
            }),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
}

// ── HttpStatus error with body ────────────────────────────────────────────────

#[tokio::test]
async fn test_http_error_includes_response_body() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/fail"))
        .respond_with(
            ResponseTemplate::new(422)
                .set_body_string(r#"{"error": "validation failed", "field": "email"}"#),
        )
        .mount(&server)
        .await;

    let stream =
        RestStream::new(RestStreamConfig::new(&server.uri(), "/api/fail").max_retries(0)).unwrap();

    let err = stream.fetch_all().await.unwrap_err();
    match &err {
        FaucetError::HttpStatus { status, body, url } => {
            assert_eq!(*status, 422);
            assert!(body.contains("validation failed"));
            assert!(url.contains("/api/fail"));
        }
        other => panic!("expected HttpStatus, got: {other:?}"),
    }
}

// ── 5xx retry behavior (integration) ──────────────────────────────────────────

#[tokio::test]
async fn test_5xx_retries_then_succeeds() {
    let server = MockServer::start().await;

    // First two calls: 500
    Mock::given(method("GET"))
        .and(path("/api/flaky"))
        .respond_with(ResponseTemplate::new(500).set_body_string("server error"))
        .up_to_n_times(2)
        .mount(&server)
        .await;

    // Third call: success
    Mock::given(method("GET"))
        .and(path("/api/flaky"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}]})))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/flaky")
            .records_path("$.data[*]")
            .max_retries(3)
            .retry_backoff(std::time::Duration::from_millis(1)),
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
}

#[tokio::test]
async fn test_4xx_does_not_retry() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/bad"))
        .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
        .expect(1) // exactly 1 call — no retries
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/bad")
            .max_retries(3)
            .retry_backoff(std::time::Duration::from_millis(1)),
    )
    .unwrap();

    assert!(stream.fetch_all().await.is_err());
}

// ── Cursor loop detection (integration) ───────────────────────────────────────

#[tokio::test]
async fn test_cursor_loop_detection_stops_fetching() {
    let server = MockServer::start().await;

    // Every page returns the same cursor — should be detected as a loop.
    Mock::given(method("GET"))
        .and(path("/api/stuck"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1}],
            "cursor": "same-token"
        })))
        .mount(&server)
        .await;

    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/api/stuck")
            .records_path("$.items[*]")
            .pagination(PaginationStyle::Cursor {
                next_token_path: "$.cursor".into(),
                param_name: "cursor".into(),
            })
            .max_pages(100), // high limit — loop detection should kick in first
    )
    .unwrap();

    let records = stream.fetch_all().await.unwrap();
    // Should get records from first page + the duplicate page, then stop.
    // First page: cursor "same-token" (new, accepted).
    // Second page: cursor "same-token" (duplicate, loop detected → stop).
    assert_eq!(records.len(), 2);
}
