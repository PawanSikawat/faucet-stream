//! Regression: max_pages-truncated incremental streams must still emit the
//! consolidated bookmark so the next run resumes from where we left off.
//!
//! Previously `stream_pages_inner` only emitted the trailing checkpoint page
//! when ZERO pages had been fetched. This meant a run cut short by `max_pages`
//! would silently drop the cumulative `running_max`, causing the next run to
//! re-fetch from the same start bookmark.

use faucet_core::{ReplicationMethod, Source};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use futures::StreamExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn max_pages_truncation_still_emits_bookmark() {
    let server = MockServer::start().await;

    // Page 1: 2 records. next_page_token is set, so upstream has more pages.
    // max_pages = 1 means we never fetch page 2.
    let page1 = json!({
        "data": [
            {"id": 1, "updated_at": "2026-01-01T00:00:00Z"},
            {"id": 2, "updated_at": "2026-01-02T00:00:00Z"},
        ],
        "next_page_token": "page-2"
    });

    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&page1))
        .mount(&server)
        .await;

    let config = RestStreamConfig::new(&server.uri(), "/items")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .pagination(PaginationStyle::Cursor {
            next_token_path: "$.next_page_token".into(),
            param_name: "page_token".into(),
        })
        .max_pages(1);

    let stream = RestStream::new(config).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    // Use Source trait's stream_pages which yields StreamPage (records + bookmark)
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);

    let mut last_bookmark: Option<Value> = None;
    let mut records_seen = 0usize;
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        records_seen += page.records.len();
        if let Some(bm) = page.bookmark {
            last_bookmark = Some(bm);
        }
    }

    assert_eq!(records_seen, 2, "should have seen both records from page 1");
    assert_eq!(
        last_bookmark,
        Some(json!("2026-01-02T00:00:00Z")),
        "max_pages-truncated stream must emit the consolidated bookmark so the \
         next run resumes from the last seen record"
    );
}

/// Guard: when pagination ends naturally (no truncation), the bookmark is still
/// correctly emitted on the final page (no regression on the happy path).
#[tokio::test]
async fn natural_pagination_end_still_emits_bookmark() {
    let server = MockServer::start().await;

    // Only one page — next_page_token is null so pagination ends naturally.
    let page1 = json!({
        "data": [
            {"id": 1, "updated_at": "2026-01-01T00:00:00Z"},
            {"id": 2, "updated_at": "2026-01-02T00:00:00Z"},
        ],
        "next_page_token": null
    });

    Mock::given(method("GET"))
        .and(path("/items"))
        .respond_with(ResponseTemplate::new(200).set_body_json(&page1))
        .mount(&server)
        .await;

    let config = RestStreamConfig::new(&server.uri(), "/items")
        .records_path("$.data[*]")
        .replication_method(ReplicationMethod::Incremental)
        .replication_key("updated_at")
        .pagination(PaginationStyle::Cursor {
            next_token_path: "$.next_page_token".into(),
            param_name: "page_token".into(),
        });

    let stream = RestStream::new(config).unwrap();
    let ctx: HashMap<String, Value> = HashMap::new();
    let mut pages = <RestStream as Source>::stream_pages(&stream, &ctx, 1000);

    let mut last_bookmark: Option<Value> = None;
    let mut records_seen = 0usize;
    while let Some(page) = pages.next().await {
        let page = page.unwrap();
        records_seen += page.records.len();
        if let Some(bm) = page.bookmark {
            last_bookmark = Some(bm);
        }
    }

    assert_eq!(records_seen, 2);
    assert_eq!(
        last_bookmark,
        Some(json!("2026-01-02T00:00:00Z")),
        "natural end must still emit the bookmark on the final page"
    );
}
