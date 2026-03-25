use faucet_stream::PaginationStyle;
use faucet_stream::pagination::PaginationState;
use reqwest::header::HeaderMap;
use serde_json::json;
use std::collections::HashMap;

/// Convenience: an empty HeaderMap for tests that don't need response headers.
fn no_headers() -> HeaderMap {
    HeaderMap::new()
}

#[test]
fn cursor_pagination_extracts_token() {
    let style = PaginationStyle::Cursor {
        next_token_path: "$.meta.cursor".into(),
        param_name: "cursor".into(),
    };

    let body = json!({"data": [], "meta": {"cursor": "abc123"}});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(has_next);
    assert_eq!(state.next_token, Some("abc123".into()));

    let mut params = HashMap::new();
    style.apply_params(&mut params, &state);
    assert_eq!(params.get("cursor").unwrap(), "abc123");
}

#[test]
fn cursor_pagination_stops_on_null() {
    let style = PaginationStyle::Cursor {
        next_token_path: "$.meta.cursor".into(),
        param_name: "cursor".into(),
    };

    let body = json!({"data": [], "meta": {"cursor": null}});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 0).unwrap();
    assert!(!has_next);
    assert!(state.next_token.is_none());
}

#[test]
fn page_number_increments() {
    let style = PaginationStyle::PageNumber {
        param_name: "page".into(),
        start_page: 1,
        page_size: Some(25),
        page_size_param: Some("per_page".into()),
    };

    let mut state = PaginationState::default();
    let body = json!({});

    let has_next = style.advance(&body, &no_headers(), &mut state, 25).unwrap();
    assert!(has_next);
    assert_eq!(state.page, 1);

    let mut params = HashMap::new();
    style.apply_params(&mut params, &state);
    assert_eq!(params.get("page").unwrap(), "2");
    assert_eq!(params.get("per_page").unwrap(), "25");
}

#[test]
fn page_number_stops_on_empty() {
    let style = PaginationStyle::PageNumber {
        param_name: "page".into(),
        start_page: 1,
        page_size: None,
        page_size_param: None,
    };

    let mut state = PaginationState::default();
    let body = json!({});

    let has_next = style.advance(&body, &no_headers(), &mut state, 0).unwrap();
    assert!(!has_next);
}

#[test]
fn offset_pagination_advances() {
    let style = PaginationStyle::Offset {
        offset_param: "offset".into(),
        limit_param: "limit".into(),
        limit: 50,
        total_path: Some("$.total".into()),
    };

    let body = json!({"total": 120});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 50).unwrap();
    assert!(has_next);
    assert_eq!(state.offset, 50);

    let has_next = style.advance(&body, &no_headers(), &mut state, 50).unwrap();
    assert!(has_next);
    assert_eq!(state.offset, 100);

    let has_next = style.advance(&body, &no_headers(), &mut state, 20).unwrap();
    assert!(!has_next);
    assert_eq!(state.offset, 120);
}

#[test]
fn link_header_extracts_next_link() {
    use reqwest::header::HeaderValue;

    let style = PaginationStyle::LinkHeader;
    let body = json!({});
    let mut state = PaginationState::default();

    let mut headers = HeaderMap::new();
    headers.insert(
        "link",
        HeaderValue::from_static(
            r#"<https://api.example.com/items?page=2>; rel="next", <https://api.example.com/items?page=5>; rel="last""#,
        ),
    );

    let has_next = style.advance(&body, &headers, &mut state, 10).unwrap();
    assert!(has_next);
    assert_eq!(
        state.next_link,
        Some("https://api.example.com/items?page=2".into())
    );
}

#[test]
fn link_header_stops_when_no_next() {
    let style = PaginationStyle::LinkHeader;
    let body = json!({});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 5).unwrap();
    assert!(!has_next);
    assert!(state.next_link.is_none());
}

#[test]
fn next_link_in_body_extracts_url() {
    let style = PaginationStyle::NextLinkInBody {
        next_link_path: "$.next_link".into(),
    };
    let body = json!({"results": [], "next_link": "https://api.example.com/workers?page=2"});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(has_next);
    assert_eq!(
        state.next_link,
        Some("https://api.example.com/workers?page=2".into())
    );
}

#[test]
fn next_link_in_body_stops_on_null() {
    let style = PaginationStyle::NextLinkInBody {
        next_link_path: "$.next_link".into(),
    };
    let body = json!({"results": [], "next_link": null});
    let mut state = PaginationState {
        next_link: Some("stale".into()),
        ..Default::default()
    };

    let has_next = style.advance(&body, &no_headers(), &mut state, 0).unwrap();
    assert!(!has_next);
    assert!(state.next_link.is_none());
}

#[test]
fn next_link_in_body_stops_when_field_absent() {
    let style = PaginationStyle::NextLinkInBody {
        next_link_path: "$.next_link".into(),
    };
    let body = json!({"results": []});
    let mut state = PaginationState::default();

    let has_next = style.advance(&body, &no_headers(), &mut state, 0).unwrap();
    assert!(!has_next);
    assert!(state.next_link.is_none());
}

// ── Loop detection ────────────────────────────────────────────────────────────

#[test]
fn cursor_loop_detection_stops_on_duplicate_token() {
    let style = PaginationStyle::Cursor {
        next_token_path: "$.cursor".into(),
        param_name: "cursor".into(),
    };

    let mut state = PaginationState::default();

    // First advance: cursor "abc" — should succeed.
    let body = json!({"cursor": "abc"});
    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(has_next);
    assert_eq!(state.next_token, Some("abc".into()));

    // Second advance: same cursor "abc" — loop detected, should stop.
    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(!has_next, "expected loop detection to stop pagination");
}

#[test]
fn cursor_loop_detection_allows_distinct_tokens() {
    let style = PaginationStyle::Cursor {
        next_token_path: "$.cursor".into(),
        param_name: "cursor".into(),
    };

    let mut state = PaginationState::default();

    let body1 = json!({"cursor": "page2"});
    assert!(
        style
            .advance(&body1, &no_headers(), &mut state, 10)
            .unwrap()
    );

    let body2 = json!({"cursor": "page3"});
    assert!(
        style
            .advance(&body2, &no_headers(), &mut state, 10)
            .unwrap()
    );

    let body3 = json!({"cursor": null});
    assert!(
        !style
            .advance(&body3, &no_headers(), &mut state, 10)
            .unwrap()
    );
}

#[test]
fn link_header_loop_detection_stops_on_duplicate() {
    use reqwest::header::HeaderValue;

    let style = PaginationStyle::LinkHeader;
    let body = json!({});
    let mut state = PaginationState::default();

    let mut headers = HeaderMap::new();
    headers.insert(
        "link",
        HeaderValue::from_static(r#"<https://api.example.com/items?page=2>; rel="next""#),
    );

    // First advance: new link — succeeds.
    let has_next = style.advance(&body, &headers, &mut state, 10).unwrap();
    assert!(has_next);

    // Second advance: same link — loop detected.
    let has_next = style.advance(&body, &headers, &mut state, 10).unwrap();
    assert!(!has_next, "expected loop detection to stop pagination");
}

#[test]
fn next_link_body_loop_detection_stops_on_duplicate() {
    let style = PaginationStyle::NextLinkInBody {
        next_link_path: "$.next_link".into(),
    };

    let mut state = PaginationState::default();
    let body = json!({"results": [], "next_link": "https://api.example.com/page=2"});

    // First advance: new link — succeeds.
    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(has_next);

    // Second advance: same link — loop detected.
    let has_next = style.advance(&body, &no_headers(), &mut state, 10).unwrap();
    assert!(!has_next, "expected loop detection to stop pagination");
}
