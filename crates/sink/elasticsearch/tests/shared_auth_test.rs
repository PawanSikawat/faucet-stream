//! Integration tests for the shared `AuthProvider` injection path on
//! [`ElasticsearchSink`].

use std::sync::Arc;

use faucet_core::{AuthProvider, AuthReference, AuthSpec, Credential, FaucetError, Sink};
use faucet_sink_elasticsearch::{ElasticsearchSink, ElasticsearchSinkConfig};
use serde_json::json;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A fixed-credential provider that always returns the same [`Credential`].
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

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn injected_provider_supplies_bearer_token() {
    let server = MockServer::start().await;

    // The mock only matches when the injected provider's token is present.
    Mock::given(method("POST"))
        .and(path("/_bulk"))
        .and(header("authorization", "Bearer SINK_TOKEN"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "errors": false,
            "items": [{ "index": { "status": 201 } }]
        })))
        .mount(&server)
        .await;

    let provider: faucet_core::SharedAuthProvider = Arc::new(FixedBearer("SINK_TOKEN"));
    let sink = ElasticsearchSink::new(ElasticsearchSinkConfig::new(server.uri(), "my_index"))
        .unwrap()
        .with_auth_provider(provider);

    let written = sink.write_batch(&[json!({"id": 1})]).await.unwrap();
    assert_eq!(written, 1);
}

#[tokio::test]
async fn unresolved_auth_reference_errors() {
    let server = MockServer::start().await;

    let mut config = ElasticsearchSinkConfig::new(server.uri(), "my_index");
    // A reference with no provider supplied must error at request time.
    config.auth = AuthSpec::Reference(AuthReference {
        name: "missing-provider".into(),
    });
    let sink = ElasticsearchSink::new(config).unwrap();

    let err = sink.write_batch(&[json!({"id": 1})]).await.unwrap_err();
    assert!(
        matches!(err, FaucetError::Auth(_)),
        "expected Auth error, got {err:?}"
    );
}
