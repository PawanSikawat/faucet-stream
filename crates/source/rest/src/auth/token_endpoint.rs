//! Generic token-endpoint authentication with caching.
//!
//! Fetches a token from an arbitrary HTTP endpoint, extracts it from the
//! response via JSONPath, and caches it with optional expiry tracking.

use faucet_core::FaucetError;
use jsonpath_rust::JsonPath;
use reqwest::Client;
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::fmt;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Optional callback to decide whether the token endpoint response is
/// successful.
///
/// Receives the HTTP status code and returns `true` if the response should
/// be treated as successful.  When not provided, the default check is
/// `status.is_success()` (i.e. 2xx).
///
/// # Example
///
/// ```
/// use faucet_source_rest::ResponseValidator;
///
/// // Accept 200 and 201 only:
/// let validator = ResponseValidator::new(|status| status == 200 || status == 201);
///
/// // Accept anything below 400:
/// let validator = ResponseValidator::new(|status| status < 400);
/// ```
#[derive(Clone)]
pub struct ResponseValidator(Arc<dyn Fn(u16) -> bool + Send + Sync>);

impl ResponseValidator {
    /// Create a new response validator from a closure.
    ///
    /// The closure receives the HTTP status code as a `u16` and must
    /// return `true` if the response should be considered successful.
    pub fn new(f: impl Fn(u16) -> bool + Send + Sync + 'static) -> Self {
        Self(Arc::new(f))
    }

    /// Evaluate the validator against a status code.
    pub(crate) fn is_success(&self, status: u16) -> bool {
        (self.0)(status)
    }
}

impl fmt::Debug for ResponseValidator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ResponseValidator(<fn>)")
    }
}

/// Default fraction of `expires_in` after which the token is refreshed.
pub const DEFAULT_TOKEN_ENDPOINT_EXPIRY_RATIO: f64 = 0.9;

/// Cached token with expiration tracking.
#[derive(Debug, Clone)]
struct CachedToken {
    token: String,
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

/// Thread-safe token cache for `Auth::TokenEndpoint`.
#[derive(Debug, Clone, Default)]
pub struct TokenEndpointCache(Arc<Mutex<Option<CachedToken>>>);

impl TokenEndpointCache {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(None)))
    }

    /// Drop any cached token so the next [`get_or_refresh`](Self::get_or_refresh)
    /// fetches a fresh one. Called when the API rejects the current token with a
    /// 401 — a server-side expiry the time-based `is_valid` check cannot detect
    /// (F57).
    pub async fn invalidate(&self) {
        *self.0.lock().await = None;
    }

    /// Return a valid cached token or fetch a new one from the endpoint.
    #[allow(clippy::too_many_arguments)]
    pub async fn get_or_refresh(
        &self,
        client: &Client,
        url: &str,
        method: &reqwest::Method,
        headers: &HeaderMap,
        body: Option<&Value>,
        token_path: &str,
        expiry_path: Option<&str>,
        expiry_ratio: f64,
        response_validator: Option<&ResponseValidator>,
    ) -> Result<String, FaucetError> {
        let mut guard = self.0.lock().await;
        if let Some(cached) = guard.as_ref() {
            if cached.is_valid() {
                return Ok(cached.token.clone());
            }
            tracing::debug!("TokenEndpoint token expired; refreshing");
        }

        let (token, expires_in) = fetch_token(
            client,
            url,
            method,
            headers,
            body,
            token_path,
            expiry_path,
            response_validator,
        )
        .await?;

        let expires_at = expires_in.map(|secs| {
            let effective = (secs as f64 * expiry_ratio) as u64;
            tokio::time::Instant::now() + std::time::Duration::from_secs(effective)
        });

        *guard = Some(CachedToken {
            token: token.clone(),
            expires_at,
        });

        Ok(token)
    }
}

/// Fetch a token from the given endpoint and extract it using JSONPath.
///
/// This is the public one-shot variant for callers who want to fetch a token
/// without caching (e.g. for use with `Auth::Bearer`).
pub async fn fetch_token_from_endpoint(
    url: &str,
    method: &reqwest::Method,
    headers: &HeaderMap,
    body: Option<&Value>,
    token_path: &str,
    response_validator: Option<&ResponseValidator>,
) -> Result<String, FaucetError> {
    let client = Client::new();
    let (token, _) = fetch_token(
        &client,
        url,
        method,
        headers,
        body,
        token_path,
        None,
        response_validator,
    )
    .await?;
    Ok(token)
}

#[allow(clippy::too_many_arguments)]
async fn fetch_token(
    client: &Client,
    url: &str,
    method: &reqwest::Method,
    headers: &HeaderMap,
    body: Option<&Value>,
    token_path: &str,
    expiry_path: Option<&str>,
    response_validator: Option<&ResponseValidator>,
) -> Result<(String, Option<u64>), FaucetError> {
    let mut req = client.request(method.clone(), url).headers(headers.clone());
    if let Some(b) = body {
        req = req.json(b);
    }

    let resp = req.send().await?;

    let status = resp.status();
    let is_success = match response_validator {
        Some(v) => v.is_success(status.as_u16()),
        None => status.is_success(),
    };
    if !is_success {
        let status_code = status.as_u16();
        let body_text = resp.text().await.unwrap_or_default();
        return Err(FaucetError::Auth(format!(
            "token endpoint request failed (HTTP {status_code}): {body_text}"
        )));
    }

    let resp_body: Value = resp.json().await?;

    let token = extract_string(&resp_body, token_path).ok_or_else(|| {
        FaucetError::Auth(format!(
            "token_path '{token_path}' did not match a string value in the response"
        ))
    })?;

    let expires_in = expiry_path.and_then(|ep| extract_u64(&resp_body, ep));

    Ok((token, expires_in))
}

/// Extract a single string value from a JSON body using a JSONPath expression.
fn extract_string(body: &Value, path: &str) -> Option<String> {
    let results = body.query(path).ok()?;
    match results.first()? {
        Value::String(s) => Some(s.clone()),
        // Accept numbers/bools as tokens by converting to string.
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Extract a single u64 value from a JSON body using a JSONPath expression.
fn extract_u64(body: &Value, path: &str) -> Option<u64> {
    let results = body.query(path).ok()?;
    results.first()?.as_u64()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn extract_string_from_nested_json() {
        let body = json!({"auth": {"token": "abc123"}});
        assert_eq!(extract_string(&body, "$.auth.token"), Some("abc123".into()));
    }

    #[test]
    fn extract_string_returns_none_for_missing_path() {
        let body = json!({"auth": {}});
        assert_eq!(extract_string(&body, "$.auth.token"), None);
    }

    #[test]
    fn extract_string_converts_number_to_string() {
        let body = json!({"token": 12345});
        assert_eq!(extract_string(&body, "$.token"), Some("12345".into()));
    }

    #[test]
    fn extract_u64_from_json() {
        let body = json!({"expires_in": 3600});
        assert_eq!(extract_u64(&body, "$.expires_in"), Some(3600));
    }

    #[test]
    fn extract_u64_returns_none_for_string() {
        let body = json!({"expires_in": "not a number"});
        assert_eq!(extract_u64(&body, "$.expires_in"), None);
    }

    #[test]
    fn extract_u64_returns_none_for_missing() {
        let body = json!({});
        assert_eq!(extract_u64(&body, "$.expires_in"), None);
    }

    // ── ResponseValidator tests ──────────────────────────────────────────────

    #[test]
    fn response_validator_accepts_matching_status() {
        let v = ResponseValidator::new(|s| s == 200);
        assert!(v.is_success(200));
        assert!(!v.is_success(201));
    }

    #[test]
    fn response_validator_range_check() {
        let v = ResponseValidator::new(|s| s < 400);
        assert!(v.is_success(200));
        assert!(v.is_success(301));
        assert!(v.is_success(399));
        assert!(!v.is_success(400));
        assert!(!v.is_success(500));
    }

    #[test]
    fn response_validator_debug_format() {
        let v = ResponseValidator::new(|_| true);
        assert_eq!(format!("{v:?}"), "ResponseValidator(<fn>)");
    }

    #[test]
    fn response_validator_clone() {
        let v = ResponseValidator::new(|s| s == 200);
        let cloned = v.clone();
        assert!(cloned.is_success(200));
        assert!(!cloned.is_success(404));
    }

    // ── CachedToken tests ────────────────────────────────────────────────────

    #[test]
    fn cached_token_without_expiry_is_always_valid() {
        let token = CachedToken {
            token: "abc".into(),
            expires_at: None,
        };
        assert!(token.is_valid());
    }

    #[test]
    fn cached_token_with_future_expiry_is_valid() {
        let token = CachedToken {
            token: "abc".into(),
            expires_at: Some(tokio::time::Instant::now() + std::time::Duration::from_secs(3600)),
        };
        assert!(token.is_valid());
    }

    // ── extract edge cases ───────────────────────────────────────────────────

    #[test]
    fn extract_string_from_array_path() {
        let body = json!({"tokens": ["first", "second"]});
        assert_eq!(extract_string(&body, "$.tokens[0]"), Some("first".into()));
    }

    #[test]
    fn extract_string_returns_none_for_object() {
        let body = json!({"token": {"nested": "value"}});
        assert_eq!(extract_string(&body, "$.token"), None);
    }

    #[test]
    fn extract_string_returns_none_for_null() {
        let body = json!({"token": null});
        assert_eq!(extract_string(&body, "$.token"), None);
    }

    #[test]
    fn extract_u64_returns_none_for_negative() {
        let body = json!({"expires_in": -1});
        assert_eq!(extract_u64(&body, "$.expires_in"), None);
    }

    #[test]
    fn extract_u64_returns_none_for_float() {
        let body = json!({"expires_in": 3600.5});
        assert_eq!(extract_u64(&body, "$.expires_in"), None);
    }
}
