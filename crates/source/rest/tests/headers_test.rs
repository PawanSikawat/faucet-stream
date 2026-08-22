//! Integration tests for settable custom request headers (#539):
//! a `headers:` config map is sent on every request, and an auth provider's
//! header of the same name overrides a config header.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::{AuthProvider, Credential, FaucetError, SharedAuthProvider};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// One page of data, then an empty page (so `PageNumber` pagination stops).
struct OnePageThenEmpty(Arc<AtomicUsize>);
impl Respond for OnePageThenEmpty {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        let body = if n == 0 {
            json!({ "data": [{ "id": 1 }] })
        } else {
            json!({ "data": [] })
        };
        ResponseTemplate::new(200).set_body_json(body)
    }
}

/// A shared provider that places a fixed header credential.
#[derive(Debug)]
struct HeaderProvider {
    name: &'static str,
    value: &'static str,
}
#[async_trait::async_trait]
impl AuthProvider for HeaderProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Header {
            name: self.name.to_string(),
            value: self.value.to_string(),
        })
    }
    fn provider_name(&self) -> &'static str {
        "header-provider"
    }
}

#[tokio::test]
async fn config_headers_sent_on_every_request() {
    let server = MockServer::start().await;
    let counter = Arc::new(AtomicUsize::new(0));

    // The mock only matches when BOTH config headers are present — so a request
    // that dropped a header would 404 and fail the run.
    Mock::given(method("GET"))
        .and(path("/data"))
        .and(header("x-custom", "hello"))
        .and(header("prefer", "transient"))
        .respond_with(OnePageThenEmpty(counter.clone()))
        .mount(&server)
        .await;

    let mut cfg = RestStreamConfig::new(&server.uri(), "/data")
        .records_path("$.data[*]")
        .pagination(PaginationStyle::PageNumber {
            param_name: "page".into(),
            start_page: 1,
            page_size: None,
            page_size_param: None,
        });
    cfg.headers.insert("X-Custom".into(), "hello".into());
    cfg.headers.insert("Prefer".into(), "transient".into());

    let records = RestStream::new(cfg).unwrap().fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    // Two requests (page 1 with data, page 2 empty → stop); both matched the
    // header-requiring mock.
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn auth_provider_header_overrides_config_header() {
    let server = MockServer::start().await;

    // Matches only when Authorization is the PROVIDER's value (not the config
    // one) and the non-conflicting config header still rides along.
    Mock::given(method("GET"))
        .and(path("/data"))
        .and(header("authorization", "provider-wins"))
        .and(header("x-custom", "hello"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [{ "id": 1 }] })))
        .mount(&server)
        .await;

    let mut cfg = RestStreamConfig::new(&server.uri(), "/data")
        .records_path("$.data[*]")
        .pagination(PaginationStyle::None);
    // A config Authorization header that must lose to the provider, plus a
    // second config header that must survive.
    cfg.headers
        .insert("Authorization".into(), "config-loses".into());
    cfg.headers.insert("X-Custom".into(), "hello".into());

    let provider: SharedAuthProvider = Arc::new(HeaderProvider {
        name: "Authorization",
        value: "provider-wins",
    });
    let records = RestStream::new(cfg)
        .unwrap()
        .with_auth_provider(provider)
        .fetch_all()
        .await
        .unwrap();
    assert_eq!(records.len(), 1);
}
