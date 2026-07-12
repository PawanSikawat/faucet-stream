//! `faucet-conformance` battery for the XML source.
//!
//! Check 1: the connector's config JSON Schema is a well-formed schema value.
//! Check 2: `stream_pages` bounds peak memory — the event-driven parser chunks
//! a 6_000-record document into `batch_size`-sized pages (peak page ≤ the batch
//! cap and strictly < the total), never materialising the whole result set.

use faucet_conformance::{
    assert_bounded_memory, assert_config_schema_valid_value, assert_errors_not_panics,
};
use faucet_source_xml::{XmlStream, XmlStreamConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const TOTAL: usize = 6_000;
const BATCH: usize = 250;

// ── Check 1: config schema validity ──────────────────────────────────────────

#[test]
fn conformance_config_schema_valid() {
    let schema = serde_json::to_value(schemars::schema_for!(XmlStreamConfig)).unwrap();
    assert_config_schema_valid_value(&schema, "faucet-source-xml");
}

// ── Check 2: bounded-memory streaming ────────────────────────────────────────

/// Build a `<root><item id="i"><name>item-i</name></item>...</root>` document
/// with `n` items.
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

#[tokio::test(flavor = "multi_thread")]
async fn conformance_bounded_memory() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(build_doc(TOTAL)),
        )
        .mount(&server)
        .await;

    let config = XmlStreamConfig::new(server.uri(), "/feed.xml")
        .records_element_path("root.item")
        // Config batch_size is the authoritative knob for the overriding source.
        .with_batch_size(BATCH);
    let source = XmlStream::new(config);

    // 6_000 records at batch_size 250 stream as 24 pages of 250 — peak page =
    // 250 ≤ BATCH < TOTAL.
    assert_bounded_memory(&source, BATCH, TOTAL).await;
}

// ── Check 6: errors, not panics ──────────────────────────────────────────────

#[tokio::test]
async fn conformance_errors_not_panics() {
    // Unreachable endpoint: `new()` builds (lazy HTTP client), the first read
    // errors with a typed `FaucetError` (connection refused) without panicking.
    // Port 1 refuses connections immediately on all platforms.
    let source = XmlStream::new(XmlStreamConfig::new("http://127.0.0.1:1", "/feed.xml"));
    assert_errors_not_panics(&source).await;
}
