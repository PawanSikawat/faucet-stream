//! Integration tests: download a file from an authed URL (wiremock) and parse it.

use faucet_core::{AuthSpec, Source};
#[cfg(feature = "excel")]
use faucet_source_http_file::FileFormat;
use faucet_source_http_file::{HttpFileAuth, HttpFileSource, HttpFileSourceConfig};
use futures::StreamExt;
use std::collections::HashMap;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const CSV: &str = "id,name,city\n1,Alice,NYC\n2,Bob,LA\n";

#[derive(Debug)]
struct StubProvider(faucet_core::Credential);

#[faucet_core::async_trait]
impl faucet_core::AuthProvider for StubProvider {
    fn provider_name(&self) -> &'static str {
        "stub"
    }
    async fn credential(&self) -> Result<faucet_core::Credential, faucet_core::FaucetError> {
        Ok(self.0.clone())
    }
}

fn bearer_cfg(url: &str) -> HttpFileSourceConfig {
    let mut c = HttpFileSourceConfig::new(url);
    c.auth = AuthSpec::Inline(HttpFileAuth::Bearer {
        token: "secret-token".into(),
    });
    c
}

#[tokio::test]
async fn fetches_and_parses_csv_with_bearer_auth() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/export.csv"))
        .and(header("authorization", "Bearer secret-token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CSV))
        .mount(&server)
        .await;

    let cfg = bearer_cfg(&format!("{}/export.csv", server.uri()));
    let source = HttpFileSource::new(cfg).unwrap();
    let recs = source.fetch_with_context(&HashMap::new()).await.unwrap();

    assert_eq!(recs.len(), 2);
    assert_eq!(recs[0]["name"], "Alice");
    assert_eq!(recs[0]["city"], "NYC");
    assert_eq!(recs[1]["name"], "Bob");
}

#[tokio::test]
async fn missing_auth_header_is_rejected_by_server() {
    // The mock only matches when the bearer header is present; without it the
    // server 404s, which the source surfaces as an HttpStatus error.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/export.csv"))
        .and(header("authorization", "Bearer secret-token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CSV))
        .mount(&server)
        .await;

    // No auth configured → request goes out unauth'd → no mock matches → 404.
    let cfg = HttpFileSourceConfig::new(format!("{}/export.csv", server.uri()));
    let source = HttpFileSource::new(cfg).unwrap();
    let err = source
        .fetch_with_context(&HashMap::new())
        .await
        .unwrap_err();
    assert!(
        matches!(err, faucet_core::FaucetError::HttpStatus { status, .. } if status == 404),
        "{err}"
    );
}

#[tokio::test]
async fn api_key_header_auth_and_context_substitution() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/files/42/content"))
        .and(header("x-api-key", "abc123"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CSV))
        .mount(&server)
        .await;

    let mut cfg = HttpFileSourceConfig::new(format!("{}/files/{{item_id}}/content", server.uri()));
    cfg.auth = AuthSpec::Inline(HttpFileAuth::ApiKey {
        header: "X-Api-Key".into(),
        value: "abc123".into(),
    });
    let source = HttpFileSource::new(cfg).unwrap();

    let mut ctx = HashMap::new();
    ctx.insert("item_id".to_string(), serde_json::json!("42"));
    let recs = source.fetch_with_context(&ctx).await.unwrap();
    assert_eq!(recs.len(), 2);
}

#[tokio::test]
async fn shared_auth_provider_takes_precedence() {
    use std::sync::Arc;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/export.csv"))
        .and(header("authorization", "Bearer from-provider"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CSV))
        .mount(&server)
        .await;

    // Inline auth says one thing; the shared provider must win.
    let mut cfg = bearer_cfg(&format!("{}/export.csv", server.uri()));
    cfg.auth = AuthSpec::Inline(HttpFileAuth::Bearer {
        token: "inline-ignored".into(),
    });
    let source = HttpFileSource::new(cfg)
        .unwrap()
        .with_auth_provider(Arc::new(StubProvider(faucet_core::Credential::Bearer(
            "from-provider".into(),
        ))));
    let recs = source.fetch_with_context(&HashMap::new()).await.unwrap();
    assert_eq!(recs.len(), 2);
}

#[tokio::test]
async fn auth_ref_without_wired_provider_sends_unauthenticated() {
    // A `{ ref }` config with no provider injected (e.g. driven directly, not
    // via the CLI catalog) falls back to an unauthenticated request.
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/export.csv"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CSV))
        .mount(&server)
        .await;
    let mut cfg = HttpFileSourceConfig::new(format!("{}/export.csv", server.uri()));
    cfg.auth = AuthSpec::Reference(faucet_core::AuthReference {
        name: "graph".into(),
    });
    let source = HttpFileSource::new(cfg).unwrap();
    assert_eq!(
        source
            .fetch_with_context(&HashMap::new())
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn inline_basic_auth() {
    let server = MockServer::start().await;
    // base64("u:p") == "dTpw"
    Mock::given(method("GET"))
        .and(path("/export.csv"))
        .and(header("authorization", "Basic dTpw"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CSV))
        .mount(&server)
        .await;
    let mut cfg = HttpFileSourceConfig::new(format!("{}/export.csv", server.uri()));
    cfg.auth = AuthSpec::Inline(HttpFileAuth::Basic {
        username: "u".into(),
        password: "p".into(),
    });
    let source = HttpFileSource::new(cfg).unwrap();
    assert_eq!(
        source
            .fetch_with_context(&HashMap::new())
            .await
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn streams_in_pages() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/export.csv"))
        .and(header("authorization", "Bearer secret-token"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CSV))
        .mount(&server)
        .await;

    let mut cfg = bearer_cfg(&format!("{}/export.csv", server.uri()));
    cfg.batch_size = 1;
    let source = HttpFileSource::new(cfg).unwrap();

    let ctx = HashMap::new();
    let mut s = source.stream_pages(&ctx, 1);
    let mut pages = 0;
    let mut total = 0;
    while let Some(page) = s.next().await {
        let page = page.unwrap();
        pages += 1;
        total += page.records.len();
        assert!(page.records.len() <= 1);
    }
    assert_eq!(pages, 2);
    assert_eq!(total, 2);
}

#[tokio::test]
async fn server_error_surfaces_http_status() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/export.csv"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let cfg = bearer_cfg(&format!("{}/export.csv", server.uri()));
    let source = HttpFileSource::new(cfg).unwrap();
    let err = source
        .fetch_with_context(&HashMap::new())
        .await
        .unwrap_err();
    assert!(
        matches!(err, faucet_core::FaucetError::HttpStatus { status, .. } if status == 500),
        "{err}"
    );
}

#[cfg(feature = "excel")]
#[tokio::test]
async fn fetches_and_parses_xlsx() {
    let xlsx = include_bytes!("fixtures/sample.xlsx");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/book.xlsx"))
        .and(header("authorization", "Bearer secret-token"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(xlsx.to_vec()))
        .mount(&server)
        .await;

    // format: auto → inferred Excel from the `.xlsx` extension.
    let cfg = bearer_cfg(&format!("{}/book.xlsx", server.uri()));
    assert_eq!(cfg.resolved_format(), FileFormat::Excel);
    let source = HttpFileSource::new(cfg).unwrap();
    let recs = source.fetch_with_context(&HashMap::new()).await.unwrap();

    assert_eq!(recs.len(), 2);
    // Excel stores all numbers as floats, so calamine yields JSON numbers.
    assert_eq!(recs[0]["id"], 1.0);
    assert_eq!(recs[0]["name"], "Alice");
    assert_eq!(recs[0]["active"], true);
    assert_eq!(recs[0]["score"], 9.5);
    assert_eq!(recs[1]["name"], "Bob");
    assert_eq!(recs[1]["active"], false);
}

#[cfg(feature = "excel")]
#[tokio::test]
async fn selects_named_worksheet() {
    let xlsx = include_bytes!("fixtures/sample.xlsx");
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/book.xlsx"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(xlsx.to_vec()))
        .mount(&server)
        .await;

    let mut cfg = HttpFileSourceConfig::new(format!("{}/book.xlsx", server.uri()));
    cfg.format = FileFormat::Excel;
    cfg.sheet = Some("Extra".into());
    let source = HttpFileSource::new(cfg).unwrap();
    let recs = source.fetch_with_context(&HashMap::new()).await.unwrap();
    assert_eq!(recs.len(), 1);
    assert_eq!(recs[0]["k"], "x");
    assert_eq!(recs[0]["v"], 42.0);
}
