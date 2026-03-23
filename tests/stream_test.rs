use serde_json::json;
use faucet_stream::{Auth, PaginationStyle, RestStream, RestStreamConfig};
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
        RestStreamConfig::new(&server.uri(), "/api/users")
            .records_path("$.data[*]"),
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
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"items": [{"id": 3}]})),
        )
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
async fn test_max_pages_enforced_for_cursor_pagination() {
    let server = MockServer::start().await;

    // Every request returns a next_cursor — pagination would be infinite without max_pages.
    Mock::given(method("GET"))
        .and(path("/api/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{"id": 1}],
            "next_cursor": "always-has-next"
        })))
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
