//! Integration tests for the shared `AuthProvider` injection path on `XmlStream`.

use std::sync::Arc;

use faucet_core::{AuthProvider, AuthReference, AuthSpec, Credential, FaucetError};
use faucet_source_xml::{XmlStream, XmlStreamConfig};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[derive(Debug)]
struct FixedBearer(&'static str);

#[async_trait::async_trait]
impl AuthProvider for FixedBearer {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        Ok(Credential::Bearer(self.0.to_string()))
    }
    fn provider_name(&self) -> &'static str {
        "fixed-bearer"
    }
}

/// Build a minimal XML response with `n` `<item>` records.
fn xml_response(n: usize) -> String {
    let mut s = String::new();
    s.push_str("<root>");
    for i in 0..n {
        s.push_str(&format!("<item><id>{i}</id></item>"));
    }
    s.push_str("</root>");
    s
}

#[tokio::test]
async fn injected_provider_supplies_bearer_token() {
    let server = MockServer::start().await;
    // The mock only matches when the injected provider's token is sent.
    Mock::given(method("GET"))
        .and(path("/feed.xml"))
        .and(header("authorization", "Bearer INJECTED"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(xml_response(2)),
        )
        .mount(&server)
        .await;

    let provider = Arc::new(FixedBearer("INJECTED"));
    let stream = XmlStream::new(
        XmlStreamConfig::new(server.uri(), "/feed.xml")
            .records_element_path("root.item"),
    )
    .with_auth_provider(provider);

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 2, "injected provider token must be sent");
}

#[tokio::test]
async fn one_provider_shared_across_two_streams() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer SHARED"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Content-Type", "application/xml")
                .set_body_string(xml_response(1)),
        )
        .mount(&server)
        .await;

    let provider = Arc::new(FixedBearer("SHARED"));
    let a = XmlStream::new(
        XmlStreamConfig::new(server.uri(), "/a.xml")
            .records_element_path("root.item"),
    )
    .with_auth_provider(provider.clone());
    let b = XmlStream::new(
        XmlStreamConfig::new(server.uri(), "/b.xml")
            .records_element_path("root.item"),
    )
    .with_auth_provider(provider.clone());

    assert_eq!(a.fetch_all().await.unwrap().len(), 1);
    assert_eq!(b.fetch_all().await.unwrap().len(), 1);
}

#[tokio::test]
async fn unresolved_auth_reference_errors() {
    let server = MockServer::start().await;
    let mut config =
        XmlStreamConfig::new(server.uri(), "/feed.xml").records_element_path("root.item");
    // A reference with no provider supplied must error at request time.
    config.auth = AuthSpec::Reference(AuthReference {
        name: "missing".into(),
    });
    let stream = XmlStream::new(config);
    let err = stream.fetch_all().await.unwrap_err();
    assert!(matches!(err, FaucetError::Auth(_)), "got {err:?}");
}
