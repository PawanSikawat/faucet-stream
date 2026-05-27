//! Integration tests for `XmlStream::stream_pages`.
//!
//! These exercise the event-driven parser end-to-end against a wiremock
//! HTTP server, asserting that records are emitted in `batch_size`-sized
//! pages without materialising the whole document tree.

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_source_xml::{XmlStream, XmlStreamConfig};
use futures::StreamExt;
use std::collections::HashMap;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Build an `<root><item id="i"><name>item-i</name></item>...</root>`
/// document with `n` items.
fn build_doc(n: usize) -> String {
    let mut s = String::with_capacity(n * 64 + 32);
    s.push_str("<root>");
    for i in 0..n {
        s.push_str(&format!(
            "<item id=\"{i}\"><name>item-{i}</name><qty>{}</qty></item>",
            i * 2
        ));
    }
    s.push_str("</root>");
    s
}

/// Serve a fixed XML body at GET `/feed.xml` and return the running mock.
async fn serve_xml(body: String) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(body),
        )
        .mount(&server)
        .await;
    server
}

#[tokio::test(flavor = "multi_thread")]
async fn retries_transient_5xx_then_succeeds() {
    // Regression for #78/#16: the XML source previously did a single send with
    // no retry, so any transient 5xx failed the run. Two 503s then a 200 must
    // now succeed.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(ResponseTemplate::new(503))
        .up_to_n_times(2)
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(build_doc(3)),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml").records_element_path("root.item");
    let source = XmlStream::new(config);
    let records = source
        .fetch_all()
        .await
        .expect("should succeed after retries");
    assert_eq!(records.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_chunks_records_into_batch_sized_pages() {
    let server = serve_xml(build_doc(10_000)).await;
    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .with_batch_size(1000);
    let source = XmlStream::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut page_count = 0;
    let mut total = 0usize;
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        assert!(
            page.records.len() <= 1000,
            "page must respect batch_size cap"
        );
        assert!(page.bookmark.is_none(), "XML source emits no bookmarks");
        page_count += 1;
        total += page.records.len();
    }

    assert_eq!(total, 10_000, "all matched elements must be emitted");
    assert_eq!(
        page_count, 10,
        "10_000 / 1000 = 10 pages with no partial remainder"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_emits_partial_trailing_page() {
    let server = serve_xml(build_doc(2_500)).await;
    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .with_batch_size(1000);
    let source = XmlStream::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 1000);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(
        sizes,
        vec![1000, 1000, 500],
        "trailing partial page must hold the remainder"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_batch_size_zero_emits_single_page() {
    let server = serve_xml(build_doc(5_000)).await;
    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .with_batch_size(0);
    let source = XmlStream::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 0);

    let mut sizes = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
    }
    assert_eq!(
        sizes,
        vec![5_000],
        "batch_size = 0 must drain the doc into a single page"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_empty_document_yields_no_pages() {
    // No matched elements: the doc is well-formed but contains nothing at
    // the configured path.
    let server = serve_xml("<root></root>".to_string()).await;
    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .with_batch_size(DEFAULT_BATCH_SIZE);
    let source = XmlStream::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);

    let mut page_count = 0;
    while let Some(page) = pages.next().await {
        let _ = page.expect("page ok");
        page_count += 1;
    }
    assert_eq!(
        page_count, 0,
        "no matches and no bookmark = no pages emitted"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_preserves_element_contents() {
    let xml = r#"<root>
        <item id="1"><name>alpha</name><tags><tag>x</tag><tag>y</tag></tags></item>
        <item id="2"><name>beta</name><tags><tag>z</tag></tags></item>
    </root>"#
        .to_string();
    let server = serve_xml(xml).await;
    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        .with_batch_size(10);
    let source = XmlStream::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 10);

    let mut all = Vec::new();
    while let Some(page) = pages.next().await {
        all.extend(page.expect("page ok").records);
    }
    assert_eq!(all.len(), 2);
    assert_eq!(all[0]["@id"], "1");
    assert_eq!(all[0]["name"], "alpha");
    let tags = all[0]["tags"]["tag"].as_array().expect("repeated tags");
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0], "x");
    assert_eq!(all[1]["@id"], "2");
    assert_eq!(all[1]["tags"]["tag"], "z");
}

#[tokio::test(flavor = "multi_thread")]
async fn stream_pages_handles_soap_envelope_with_namespaces() {
    let xml = r#"<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
        <soap:Body>
            <GetUsersResponse>
                <User><Name>Alice</Name></User>
                <User><Name>Bob</Name></User>
                <User><Name>Carol</Name></User>
            </GetUsersResponse>
        </soap:Body>
    </soap:Envelope>"#
        .to_string();
    let server = serve_xml(xml).await;
    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("soap:Envelope.soap:Body.GetUsersResponse.User")
        .with_batch_size(2);
    let source = XmlStream::new(config);

    let ctx: HashMap<String, serde_json::Value> = HashMap::new();
    let mut pages = source.stream_pages(&ctx, 2);

    let mut sizes = Vec::new();
    let mut names = Vec::new();
    while let Some(page) = pages.next().await {
        let page = page.expect("page ok");
        sizes.push(page.records.len());
        for r in &page.records {
            names.push(r["Name"].as_str().unwrap().to_string());
        }
    }
    assert_eq!(sizes, vec![2, 1], "3 users at batch_size=2 -> 2 + 1");
    assert_eq!(names, vec!["Alice", "Bob", "Carol"]);
}

// NOTE: A time-to-first-page test like the one used for postgres / parquet /
// csv intentionally does not apply here. The XML source buffers each HTTP
// response body via `execute_request` before handing it to the event-driven
// extractor, so within a single response there is no meaningful first-page
// vs. full-drain asymmetry. The streaming win is across HTTP pages (a small
// response yields its records as soon as it lands, instead of the source
// waiting for every paginated response to be fetched). The multi-page
// pagination tests above already cover that property.
