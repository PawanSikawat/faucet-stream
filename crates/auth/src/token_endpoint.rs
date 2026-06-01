//! Generic token-endpoint provider: fetch a token from an arbitrary HTTP
//! endpoint and extract it from the JSON response via JSONPath.

use async_trait::async_trait;
use faucet_core::{AuthProvider, Credential, FaucetError};
use jsonpath_rust::JsonPath;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::expiry_instant;

#[derive(Default)]
struct CachedToken {
    token: Option<String>,
    expires_at: Option<Instant>,
}

/// Fetches a token from an arbitrary endpoint, extracts it via `token_path`
/// (JSONPath), and caches it with optional expiry tracking. Single-flight
/// refresh via an internal [`Mutex`].
pub struct TokenEndpointProvider {
    http: Client,
    url: String,
    method: reqwest::Method,
    body: Option<Value>,
    token_path: String,
    expiry_path: Option<String>,
    expiry_ratio: f64,
    state: Mutex<CachedToken>,
}

// Hand-written so `{:?}` never prints the cached token in `state` or the request
// `body` (which can carry a `client_secret`). `finish_non_exhaustive` omits both.
impl std::fmt::Debug for TokenEndpointProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TokenEndpointProvider")
            .field("url", &self.url)
            .field("method", &self.method)
            .field("token_path", &self.token_path)
            .field("expiry_path", &self.expiry_path)
            .field("expiry_ratio", &self.expiry_ratio)
            .finish_non_exhaustive()
    }
}

impl TokenEndpointProvider {
    /// Build from a config object with `url`, optional `method` (default `POST`),
    /// optional `body`, `token_path` (JSONPath), optional `expiry_path`, and
    /// optional `expiry_ratio`.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        let url = config
            .get("url")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                FaucetError::Config("token_endpoint auth provider: missing `url`".into())
            })?
            .to_string();
        let method = config
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("POST")
            .parse::<reqwest::Method>()
            .map_err(|e| FaucetError::Config(format!("token_endpoint: invalid method: {e}")))?;
        let token_path = config
            .get("token_path")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                FaucetError::Config("token_endpoint auth provider: missing `token_path`".into())
            })?
            .to_string();
        Ok(Self {
            http: crate::auth_http_client(),
            url,
            method,
            body: config.get("body").cloned().filter(|v| !v.is_null()),
            token_path,
            expiry_path: config
                .get("expiry_path")
                .and_then(Value::as_str)
                .map(str::to_string),
            expiry_ratio: crate::parse_expiry_ratio(config)?,
            state: Mutex::new(CachedToken::default()),
        })
    }

    async fn fetch(&self) -> Result<(String, Option<u64>), FaucetError> {
        let mut req = self.http.request(self.method.clone(), &self.url);
        if let Some(body) = &self.body {
            req = req.json(body);
        }
        let resp = req.send().await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(FaucetError::Auth(format!(
                "token endpoint request failed (HTTP {status}): {body}"
            )));
        }
        let body: Value = resp.json().await?;
        let token = extract_string(&body, &self.token_path).ok_or_else(|| {
            FaucetError::Auth(format!(
                "token_path '{}' did not match a string value in the response",
                self.token_path
            ))
        })?;
        let expires_in = self
            .expiry_path
            .as_deref()
            .and_then(|p| extract_u64(&body, p));
        Ok((token, expires_in))
    }
}

#[async_trait]
impl AuthProvider for TokenEndpointProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        let still_valid = match (&state.token, state.expires_at) {
            (Some(_), Some(exp)) => Instant::now() < exp,
            (Some(_), None) => true,
            _ => false,
        };
        if still_valid {
            return Ok(Credential::Bearer(state.token.clone().unwrap()));
        }
        let (token, expires_in) = self.fetch().await?;
        state.token = Some(token.clone());
        state.expires_at = expiry_instant(expires_in, self.expiry_ratio);
        Ok(Credential::Bearer(token))
    }

    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        // CAS: if the cache already holds a *different* still-valid token, a
        // concurrent caller already refreshed after the same 401 — hand that
        // back instead of fetching again (single-flight). Only refetch when the
        // cached token is the stale one (or itself expired). Without this
        // override the default `invalidate` just returns `credential()`, which
        // serves the still-cached stale token straight back, so a connector that
        // hit a 401 can never force a refresh (#146 M15).
        let current_valid = match (&state.token, state.expires_at) {
            (Some(t), Some(exp)) if Instant::now() < exp => Some(t.clone()),
            (Some(t), None) => Some(t.clone()),
            _ => None,
        };
        if let (Some(cur), Credential::Bearer(stale_tok)) = (&current_valid, stale)
            && cur != stale_tok
        {
            return Ok(Credential::Bearer(cur.clone()));
        }
        let (token, expires_in) = self.fetch().await?;
        state.token = Some(token.clone());
        state.expires_at = expiry_instant(expires_in, self.expiry_ratio);
        Ok(Credential::Bearer(token))
    }

    fn provider_name(&self) -> &'static str {
        "token_endpoint"
    }
}

fn extract_string(body: &Value, path: &str) -> Option<String> {
    let results = body.query(path).ok()?;
    match results.first()? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn extract_u64(body: &Value, path: &str) -> Option<u64> {
    let results = body.query(path).ok()?;
    results.first()?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    struct Counting(Arc<AtomicUsize>);
    impl Respond for Counting {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            let n = self.0.fetch_add(1, Ordering::SeqCst) + 1;
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "auth": { "access_token": format!("tok{n}") },
                "ttl": 3600
            }))
        }
    }

    #[tokio::test]
    async fn extracts_token_via_jsonpath_and_single_flights() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(Counting(hits.clone()))
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "token_path": "$.auth.access_token",
            "expiry_path": "$.ttl",
        }))
        .unwrap();
        let results = futures::future::join_all((0..3).map(|_| p.credential())).await;
        for r in &results {
            assert_eq!(r.as_ref().unwrap(), &Credential::Bearer("tok1".into()));
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn provider_debug_does_not_leak_body_secrets() {
        // The request `body` may carry a client secret; a `{:?}` of the provider
        // (held as `Arc<dyn AuthProvider>`) must not print it.
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": "https://idp.example/token",
            "token_path": "$.access_token",
            "body": { "client_secret": "topsecretbody" },
        }))
        .unwrap();
        let s = format!("{p:?}");
        assert!(
            !s.contains("topsecretbody"),
            "request body secret leaked: {s}"
        );
        assert!(s.contains("token_path"), "non-secret fields should remain: {s}");
    }

    #[test]
    fn missing_url_errors() {
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({"token_path": "$.t"})).is_err()
        );
    }

    #[tokio::test]
    async fn invalidate_forces_a_refresh_of_the_stale_token() {
        // M15 (#146): a connector that hit a 401 calls invalidate(stale) and
        // must get a freshly-fetched token — not the cached stale one back.
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(Counting(hits.clone()))
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "token_path": "$.auth.access_token",
            "expiry_path": "$.ttl",
        }))
        .unwrap();

        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("tok1".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        // invalidate(tok1) must refetch → tok2.
        assert_eq!(
            p.invalidate(&Credential::Bearer("tok1".into()))
                .await
                .unwrap(),
            Credential::Bearer("tok2".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 2);

        // The refreshed token is now cached — no extra fetch.
        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("tok2".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invalidate_short_circuits_when_token_already_rotated() {
        // CAS: if the cache already holds a token different from the stale one,
        // a concurrent caller already refreshed — return it without refetching.
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(Counting(hits.clone()))
            .mount(&server)
            .await;
        let p = TokenEndpointProvider::from_config(&serde_json::json!({
            "url": server.uri(),
            "token_path": "$.auth.access_token",
            "expiry_path": "$.ttl",
        }))
        .unwrap();

        assert_eq!(
            p.credential().await.unwrap(),
            Credential::Bearer("tok1".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);
        // Invalidating an already-superseded token returns cached tok1, no fetch.
        assert_eq!(
            p.invalidate(&Credential::Bearer("old-token".into()))
                .await
                .unwrap(),
            Credential::Bearer("tok1".into())
        );
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn rejects_out_of_range_expiry_ratio() {
        // M16 (#146): an out-of-range expiry_ratio breaks caching — reject it.
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({
                "url": "http://x", "token_path": "$.t", "expiry_ratio": 0
            }))
            .is_err()
        );
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({
                "url": "http://x", "token_path": "$.t", "expiry_ratio": 1.5
            }))
            .is_err()
        );
        // A valid ratio still constructs.
        assert!(
            TokenEndpointProvider::from_config(&serde_json::json!({
                "url": "http://x", "token_path": "$.t", "expiry_ratio": 0.5
            }))
            .is_ok()
        );
    }
}
