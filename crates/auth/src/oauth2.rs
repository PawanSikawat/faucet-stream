//! OAuth2 providers: `client_credentials` and `refresh_token` (with rotation).
//!
//! Both hold a single [`Mutex`]-guarded cache and perform the token-endpoint
//! call **with the lock held**, so concurrent callers during a refresh await the
//! one in-flight fetch (single-flight). The refresh provider captures a rotated
//! `refresh_token` from each response in place, so a single active access token
//! plus a rotating refresh token can be shared across many connectors without
//! racing.

use async_trait::async_trait;
use faucet_core::{AuthProvider, Credential, FaucetError};
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::{DEFAULT_EXPIRY_RATIO, expiry_instant};

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    token_type: Option<String>,
}

#[derive(Debug, Default)]
struct CachedToken {
    access_token: Option<String>,
    expires_at: Option<Instant>,
}

impl CachedToken {
    fn valid(&self) -> Option<&str> {
        match (&self.access_token, self.expires_at) {
            (Some(tok), Some(exp)) if Instant::now() < exp => Some(tok),
            (Some(tok), None) => Some(tok),
            _ => None,
        }
    }
}

/// OAuth2 `client_credentials` grant provider.
#[derive(Debug)]
pub struct OAuth2ClientCredentialsProvider {
    http: Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    scopes: Vec<String>,
    expiry_ratio: f64,
    state: Mutex<CachedToken>,
}

impl OAuth2ClientCredentialsProvider {
    /// Build from a config object with `token_url`, `client_id`,
    /// `client_secret`, optional `scopes` and `expiry_ratio`.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        Ok(Self {
            http: crate::auth_http_client(),
            token_url: required_str(config, "token_url")?,
            client_id: required_str(config, "client_id")?,
            client_secret: required_str(config, "client_secret")?,
            scopes: string_array(config, "scopes"),
            expiry_ratio: config
                .get("expiry_ratio")
                .and_then(Value::as_f64)
                .unwrap_or(DEFAULT_EXPIRY_RATIO),
            state: Mutex::new(CachedToken::default()),
        })
    }

    async fn fetch(&self) -> Result<TokenResponse, FaucetError> {
        let resp = self
            .http
            .post(&self.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("scope", &self.scopes.join(" ")),
            ])
            .send()
            .await?;
        parse_token_response(resp).await
    }
}

#[async_trait]
impl AuthProvider for OAuth2ClientCredentialsProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        if let Some(tok) = state.valid() {
            return Ok(Credential::Bearer(tok.to_string()));
        }
        let body = self.fetch().await?;
        state.access_token = Some(body.access_token.clone());
        state.expires_at = expiry_instant(body.expires_in, self.expiry_ratio);
        Ok(Credential::Bearer(body.access_token))
    }

    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        // CAS: only refresh if the cache still holds the stale token.
        if let (Some(cur), Credential::Bearer(stale_tok)) = (state.valid(), stale)
            && cur != stale_tok
        {
            return Ok(Credential::Bearer(cur.to_string()));
        }
        let body = self.fetch().await?;
        state.access_token = Some(body.access_token.clone());
        state.expires_at = expiry_instant(body.expires_in, self.expiry_ratio);
        Ok(Credential::Bearer(body.access_token))
    }

    fn provider_name(&self) -> &'static str {
        "oauth2"
    }
}

#[derive(Debug, Default)]
struct RefreshState {
    access_token: Option<String>,
    expires_at: Option<Instant>,
    refresh_token: String,
}

/// OAuth2 `refresh_token` grant provider with refresh-token rotation capture.
#[derive(Debug)]
pub struct OAuth2RefreshProvider {
    http: Client,
    token_url: String,
    client_id: String,
    client_secret: String,
    expiry_ratio: f64,
    state: Mutex<RefreshState>,
}

impl OAuth2RefreshProvider {
    /// Build from a config object with `token_url`, `client_id`,
    /// `client_secret`, `refresh_token`, and optional `expiry_ratio`.
    pub fn from_config(config: &Value) -> Result<Self, FaucetError> {
        let refresh_token = required_str(config, "refresh_token")?;
        Ok(Self {
            http: crate::auth_http_client(),
            token_url: required_str(config, "token_url")?,
            client_id: required_str(config, "client_id")?,
            client_secret: required_str(config, "client_secret")?,
            expiry_ratio: config
                .get("expiry_ratio")
                .and_then(Value::as_f64)
                .unwrap_or(DEFAULT_EXPIRY_RATIO),
            state: Mutex::new(RefreshState {
                refresh_token,
                ..Default::default()
            }),
        })
    }

    /// Refresh using the *current* refresh token and capture rotation in place.
    async fn refresh(&self, state: &mut RefreshState) -> Result<String, FaucetError> {
        let resp = self
            .http
            .post(&self.token_url)
            .form(&[
                ("grant_type", "refresh_token"),
                ("refresh_token", &state.refresh_token),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
            ])
            .send()
            .await?;
        let body = parse_token_response(resp).await?;
        state.access_token = Some(body.access_token.clone());
        state.expires_at = expiry_instant(body.expires_in, self.expiry_ratio);
        if let Some(rotated) = body.refresh_token {
            state.refresh_token = rotated; // capture rotation centrally
        }
        Ok(body.access_token)
    }
}

#[async_trait]
impl AuthProvider for OAuth2RefreshProvider {
    async fn credential(&self) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        if let (Some(tok), Some(exp)) = (&state.access_token, state.expires_at)
            && Instant::now() < exp
        {
            return Ok(Credential::Bearer(tok.clone()));
        }
        let token = self.refresh(&mut state).await?;
        Ok(Credential::Bearer(token))
    }

    async fn invalidate(&self, stale: &Credential) -> Result<Credential, FaucetError> {
        let mut state = self.state.lock().await;
        // CAS: another connector may have already refreshed; if the cached token
        // no longer equals the stale one, hand back the fresh token.
        if let (Some(cur), Credential::Bearer(stale_tok)) = (&state.access_token, stale)
            && cur != stale_tok
        {
            return Ok(Credential::Bearer(cur.clone()));
        }
        let token = self.refresh(&mut state).await?;
        Ok(Credential::Bearer(token))
    }

    fn provider_name(&self) -> &'static str {
        "oauth2_refresh"
    }
}

fn required_str(config: &Value, key: &str) -> Result<String, FaucetError> {
    config
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| FaucetError::Config(format!("oauth2 auth provider: missing `{key}`")))
}

fn string_array(config: &Value, key: &str) -> Vec<String> {
    config
        .get(key)
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

async fn parse_token_response(resp: reqwest::Response) -> Result<TokenResponse, FaucetError> {
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(FaucetError::Auth(format!(
            "OAuth2 token request failed (HTTP {status}): {body}"
        )));
    }
    resp.json::<TokenResponse>().await.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, Respond, ResponseTemplate};

    struct CountingToken {
        hits: Arc<AtomicUsize>,
        token_prefix: &'static str,
    }
    impl Respond for CountingToken {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            let n = self.hits.fetch_add(1, Ordering::SeqCst) + 1;
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "access_token": format!("{}{n}", self.token_prefix),
                "expires_in": 3600,
                "refresh_token": format!("rt{n}"),
            }))
        }
    }

    #[tokio::test]
    async fn refresh_provider_single_flight_one_fetch_for_concurrent_calls() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(CountingToken {
                hits: hits.clone(),
                token_prefix: "A",
            })
            .mount(&server)
            .await;

        let provider = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
        }))
        .unwrap();

        let results = futures::future::join_all((0..4).map(|_| provider.credential())).await;
        for r in &results {
            assert_eq!(r.as_ref().unwrap(), &Credential::Bearer("A1".into()));
        }
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "expected exactly one token fetch"
        );
    }

    #[tokio::test]
    async fn refresh_provider_invalidate_cas_refetches_once() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(CountingToken {
                hits: hits.clone(),
                token_prefix: "A",
            })
            .mount(&server)
            .await;
        let provider = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
        }))
        .unwrap();

        let first = provider.credential().await.unwrap();
        assert_eq!(first, Credential::Bearer("A1".into()));
        // Invalidate the token we hold → one more fetch, rotated refresh token used.
        let second = provider.invalidate(&first).await.unwrap();
        assert_eq!(second, Credential::Bearer("A2".into()));
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        // Invalidating a *stale* token that no longer matches → no fetch.
        let again = provider.invalidate(&first).await.unwrap();
        assert_eq!(again, Credential::Bearer("A2".into()));
        assert_eq!(hits.load(Ordering::SeqCst), 2, "stale CAS must not refetch");
    }

    #[tokio::test]
    async fn client_credentials_single_flight() {
        let server = MockServer::start().await;
        let hits = Arc::new(AtomicUsize::new(0));
        Mock::given(method("POST"))
            .respond_with(CountingToken {
                hits: hits.clone(),
                token_prefix: "C",
            })
            .mount(&server)
            .await;
        let provider = OAuth2ClientCredentialsProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "scopes": ["read"],
        }))
        .unwrap();
        let results = futures::future::join_all((0..4).map(|_| provider.credential())).await;
        for r in &results {
            assert_eq!(r.as_ref().unwrap(), &Credential::Bearer("C1".into()));
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn token_endpoint_failure_surfaces_auth_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("nope"))
            .mount(&server)
            .await;
        let provider = OAuth2RefreshProvider::from_config(&serde_json::json!({
            "token_url": server.uri(),
            "client_id": "id",
            "client_secret": "secret",
            "refresh_token": "rt0",
        }))
        .unwrap();
        assert!(matches!(
            provider.credential().await,
            Err(FaucetError::Auth(_))
        ));
    }
}
