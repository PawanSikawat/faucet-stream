//! OAuth2 client credentials flow with token caching.

use crate::error::FaucetError;
use reqwest::Client;
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[allow(dead_code)]
    #[serde(default)]
    token_type: Option<String>,
}

/// Default fraction of `expires_in` after which the token is refreshed.
pub const DEFAULT_EXPIRY_RATIO: f64 = 0.9;

/// Cached OAuth2 token with expiration tracking.
#[derive(Debug, Clone)]
struct CachedToken {
    access_token: String,
    /// Instant at which the token should be considered expired. Computed as
    /// `now + expires_in * expiry_ratio` at fetch time.  `None` means no
    /// expiry info was provided by the server — the token is treated as valid
    /// indefinitely (until a 401 forces a refresh).
    expires_at: Option<tokio::time::Instant>,
}

impl CachedToken {
    fn is_valid(&self) -> bool {
        match self.expires_at {
            Some(exp) => tokio::time::Instant::now() < exp,
            None => true,
        }
    }
}

/// Thread-safe token cache shared across requests within a single `RestStream`.
#[derive(Debug, Clone, Default)]
pub struct TokenCache(Arc<Mutex<Option<CachedToken>>>);

impl TokenCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    /// Return a valid cached token or fetch a new one.
    ///
    /// `expiry_ratio` is the fraction of the server-reported `expires_in`
    /// lifetime after which the token is proactively refreshed. For example,
    /// `0.9` means a token with `expires_in = 3600` is refreshed after 3240 s.
    pub async fn get_or_refresh(
        &self,
        client: &Client,
        token_url: &str,
        client_id: &str,
        client_secret: &str,
        scopes: &[String],
        expiry_ratio: f64,
    ) -> Result<String, FaucetError> {
        let mut guard = self.0.lock().await;
        if let Some(cached) = guard.as_ref() {
            if cached.is_valid() {
                return Ok(cached.access_token.clone());
            }
            tracing::debug!("OAuth2 token expired; refreshing");
        }

        let (token, expires_in) = fetch_oauth2_token_inner_with_client(
            client,
            token_url,
            client_id,
            client_secret,
            scopes,
        )
        .await?;

        let expires_at = expires_in.map(|secs| {
            let effective = (secs as f64 * expiry_ratio) as u64;
            tokio::time::Instant::now() + std::time::Duration::from_secs(effective)
        });

        *guard = Some(CachedToken {
            access_token: token.clone(),
            expires_at,
        });

        Ok(token)
    }
}

/// Fetch an OAuth2 token using the client credentials grant.
///
/// Prefer using [`TokenCache::get_or_refresh`] to avoid fetching a new token
/// on every request.
pub async fn fetch_oauth2_token(
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &[String],
) -> Result<String, FaucetError> {
    let (token, _) = fetch_oauth2_token_inner(token_url, client_id, client_secret, scopes).await?;
    Ok(token)
}

async fn fetch_oauth2_token_inner(
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &[String],
) -> Result<(String, Option<u64>), FaucetError> {
    let client = Client::new();
    fetch_oauth2_token_inner_with_client(&client, token_url, client_id, client_secret, scopes).await
}

async fn fetch_oauth2_token_inner_with_client(
    client: &Client,
    token_url: &str,
    client_id: &str,
    client_secret: &str,
    scopes: &[String],
) -> Result<(String, Option<u64>), FaucetError> {
    let resp = client
        .post(token_url)
        .form(&[
            ("grant_type", "client_credentials"),
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("scope", &scopes.join(" ")),
        ])
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(FaucetError::Auth(format!(
            "OAuth2 token request failed (HTTP {status}): {body}"
        )));
    }

    let token_resp: TokenResponse = resp.json().await?;
    Ok((token_resp.access_token, token_resp.expires_in))
}
