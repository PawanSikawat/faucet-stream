//! Integration tests for `MongoSource::stream_pages` against a real MongoDB
//! instance via testcontainers.
//!
//! These tests require Docker. Each test boots its own container and seeds
//! its own collection so they are fully isolated and safe to run in
//! parallel.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_mongodb::{MongoSource, MongoSourceConfig};
use futures::StreamExt;
use mongodb::Client;
use mongodb::bson::{Document, doc};
use std::collections::HashMap;
use std::time::Instant;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::mongo::Mongo;

/// Start a MongoDB container and return both the container handle and a
/// connection URI. The container is kept alive by the returned handle.
async fn start_mongo() -> (ContainerAsync<Mongo>, String) {
    let container: ContainerAsync<Mongo> = Mongo::default()
        .start()
        .await
        .expect("mongo container start");
    let port = container
        .get_host_port_ipv4(27017)
        .await
        .expect("mongo port");
    let uri = format!("mongodb://127.0.0.1:{port}");
    (container, uri)
}

/// Insert `n` documents of the form `{ id: i }` for `i = 1..=n` into
/// `db.collection`.
async fn seed_docs(uri: &str, db: &str, collection: &str, n: i64) {
    let client = Client::with_uri_str(uri).await.expect("client");
    let coll = client.database(db).collection::<Document>(collection);
    let docs: Vec<Document> = (1..=n).map(|i| doc! { "id": i }).collect();
    coll.insert_many(docs).await.expect("insert_many");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_documents_into_batch_sized_pages() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 10_000).await;

    let config = MongoSourceConfig::new(uri, "testdb", "events").with_batch_size(1000);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut page_count = 0;
    let mut total = 0;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        page_count += 1;
        total += page.records.len();
        assert_eq!(
            page.records.len(),
            1000,
            "every page must hold exactly batch_size docs when total is a multiple"
        );
        assert!(
            page.bookmark.is_none(),
            "mongodb source has no incremental mode yet; bookmark must be None"
        );
    }
    assert_eq!(page_count, 10);
    assert_eq!(total, 10_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_partial_final_page() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 2_500).await;

    let config = MongoSourceConfig::new(uri, "testdb", "events").with_batch_size(1000);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        sizes.push(page.expect("page ok").records.len());
    }
    assert_eq!(sizes, vec![1000, 1000, 500]);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_emits_single_page() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 10_000).await;

    let config = MongoSourceConfig::new(uri, "testdb", "events").with_batch_size(0);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut collected = Vec::new();
    while let Some(page) = pages.next().await {
        collected.push(page.expect("page ok").records.len());
    }
    assert_eq!(
        collected,
        vec![10_000],
        "batch_size = 0 must drain the cursor and emit exactly one page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_empty_result_yields_no_pages() {
    let (_container, uri) = start_mongo().await;
    // Don't seed — the collection is empty.

    let config = MongoSourceConfig::new(uri, "testdb", "empty").with_batch_size(DEFAULT_BATCH_SIZE);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(page_count, 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_document_contents() {
    let (_container, uri) = start_mongo().await;
    let client = Client::with_uri_str(&uri).await.expect("client");
    let coll = client.database("testdb").collection::<Document>("items");
    coll.insert_many(vec![
        doc! { "id": 1, "name": "alpha" },
        doc! { "id": 2, "name": "beta" },
        doc! { "id": 3, "name": "gamma" },
    ])
    .await
    .expect("insert_many");

    let config = MongoSourceConfig::new(uri, "testdb", "items")
        .sort(serde_json::json!({"id": 1}))
        .with_batch_size(2);
    let source = MongoSource::new(config).await.expect("source new");

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);

    let mut all = Vec::new();
    while let Some(page) = pages.next().await {
        all.extend(page.expect("page ok").records);
    }
    assert_eq!(all.len(), 3);
    assert_eq!(all[0]["id"], 1);
    assert_eq!(all[0]["name"], "alpha");
    assert_eq!(all[2]["name"], "gamma");
}

/// Catches the "buffered-then-chunked" anti-pattern.
///
/// The MongoDB driver's cursor naturally streams documents in batches from
/// the server. The true-streaming impl yields a `StreamPage` after parsing
/// `batch_size` documents off the cursor; the default trait impl materialises
/// every document into a `Vec<Value>` before any page is yielded.
///
/// For a large result, the parse-and-buffer cost dominates and the
/// difference is observable: dropping the stream after the first page in the
/// streaming impl avoids parsing the remaining ~99% of documents.
#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_first_page_completes_without_parsing_full_result() {
    let (_container, uri) = start_mongo().await;
    seed_docs(&uri, "testdb", "events", 100_000).await;

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();

    // Full drain for reference.
    let config_full = MongoSourceConfig::new(&uri, "testdb", "events").with_batch_size(1000);
    let source = MongoSource::new(config_full).await.expect("source new");
    let start = Instant::now();
    let mut pages = source.stream_pages(&ctx, 1000);
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
    }
    let full_elapsed = start.elapsed();
    drop(pages);
    drop(source);

    // First page only.
    let config_first = MongoSourceConfig::new(&uri, "testdb", "events").with_batch_size(1000);
    let source = MongoSource::new(config_first).await.expect("source new");
    let start = Instant::now();
    let mut pages = source.stream_pages(&ctx, 1000);
    let first = pages
        .next()
        .await
        .expect("first page exists")
        .expect("page ok");
    let first_elapsed = start.elapsed();
    drop(pages);
    assert_eq!(first.records.len(), 1000);

    assert!(
        first_elapsed * 2 < full_elapsed,
        "first page should arrive without parsing the full result; \
         first page took {first_elapsed:?}, full drain took {full_elapsed:?}"
    );
}
