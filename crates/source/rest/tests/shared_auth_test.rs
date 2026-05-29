//! Integration tests for the shared `AuthProvider` injection path.

use std::sync::Arc;

use faucet_core::{AuthProvider, AuthReference, AuthSpec, Credential, FaucetError, SharedAuthProvider};
use faucet_source_rest::{PaginationStyle, RestStream, RestStreamConfig};
use serde_json::json;
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

#[tokio::test]
async fn injected_provider_supplies_bearer_token() {
    let server = MockServer::start().await;
    // The mock only matches when the injected provider's token is sent.
    Mock::given(method("GET"))
        .and(path("/data"))
        .and(header("authorization", "Bearer INJECTED"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}]})))
        .mount(&server)
        .await;

    let provider: SharedAuthProvider = Arc::new(FixedBearer("INJECTED"));
    let stream = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/data")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap()
    .with_auth_provider(provider);

    let records = stream.fetch_all().await.unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["id"], 1);
}

#[tokio::test]
async fn one_provider_shared_across_two_streams() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(header("authorization", "Bearer SHARED"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": 1}]})))
        .mount(&server)
        .await;

    let provider: SharedAuthProvider = Arc::new(FixedBearer("SHARED"));
    let a = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/a")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap()
    .with_auth_provider(provider.clone());
    let b = RestStream::new(
        RestStreamConfig::new(&server.uri(), "/b")
            .records_path("$.data[*]")
            .pagination(PaginationStyle::None),
    )
    .unwrap()
    .with_auth_provider(provider.clone());

    assert_eq!(a.fetch_all().await.unwrap().len(), 1);
    assert_eq!(b.fetch_all().await.unwrap().len(), 1);
}

#[tokio::test]
async fn unresolved_auth_reference_errors() {
    let server = MockServer::start().await;
    let mut config = RestStreamConfig::new(&server.uri(), "/data").pagination(PaginationStyle::None);
    // A reference with no provider supplied must error at request time.
    config.auth = AuthSpec::Reference(AuthReference {
        name: "missing".into(),
    });
    let stream = RestStream::new(config).unwrap();
    let err = stream.fetch_all().await.unwrap_err();
    assert!(matches!(err, FaucetError::Auth(_)), "got {err:?}");
}
