//! End-to-end test: a top-level `auth:` catalog provider shared across matrix
//! rows via `auth: { ref }`, driven through the real CLI run path.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

/// Token endpoint that counts how many times it is hit.
struct CountingToken(Arc<AtomicUsize>);
impl Respond for CountingToken {
    fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
        self.0.fetch_add(1, Ordering::SeqCst);
        ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "access_token": "SHARED_TOKEN",
            "expires_in": 3600
        }))
    }
}

#[tokio::test]
async fn auth_catalog_shares_one_token_across_matrix_rows() {
    let server = MockServer::start().await;
    let token_hits = Arc::new(AtomicUsize::new(0));

    // Token endpoint (oauth2 client_credentials) — counts fetches.
    Mock::given(method("POST"))
        .and(path("/token"))
        .respond_with(CountingToken(token_hits.clone()))
        .mount(&server)
        .await;

    // Two data endpoints, both require the shared bearer token.
    for p in ["/a", "/b"] {
        Mock::given(method("GET"))
            .and(path(p))
            .and(header("authorization", "Bearer SHARED_TOKEN"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "data": [{"id": 1}]
            })))
            .mount(&server)
            .await;
    }

    let out = tempfile::tempdir().unwrap();
    let out_a = out.path().join("a.jsonl");
    let out_b = out.path().join("b.jsonl");

    let yaml = format!(
        r#"
version: 1
name: shared_auth
auth:
  api:
    type: oauth2
    config:
      token_url: "{base}/token"
      client_id: id
      client_secret: secret
pipeline:
  sources:
    ep:
      type: rest
      config:
        base_url: "{base}"
        path: /a
        method: GET
        auth: {{ ref: api }}
        query_params: {{}}
        pagination: {{ type: None }}
        records_path: "$.data[*]"
        max_retries: 0
        retry_backoff: 0
        tolerated_http_errors: []
        replication_method: {{ type: FullTable }}
        primary_keys: []
        partitions: []
        schema_sample_size: 0
  sinks:
    out:
      type: jsonl
      config:
        path: "{a}"
matrix:
  - id: row-a
    source: {{ ref: ep, config: {{ path: /a }} }}
    sink: {{ ref: out, config: {{ path: "{a}" }} }}
  - id: row-b
    source: {{ ref: ep, config: {{ path: /b }} }}
    sink: {{ ref: out, config: {{ path: "{b}" }} }}
"#,
        base = server.uri(),
        a = out_a.display(),
        b = out_b.display(),
    );

    faucet_cli::run_from_yaml_str(&yaml)
        .await
        .expect("pipeline with shared auth should succeed");

    // Both rows authenticated with the shared token, and the token endpoint was
    // hit exactly once (single-flight, shared Arc) despite two rows.
    assert_eq!(
        token_hits.load(Ordering::SeqCst),
        1,
        "expected exactly one token fetch shared across both matrix rows"
    );
    assert!(out_a.exists() && out_b.exists());
}
