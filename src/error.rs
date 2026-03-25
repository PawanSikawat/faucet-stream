//! Error types for faucet-stream.

use std::time::Duration;
use thiserror::Error;

/// All possible errors returned by faucet-stream.
#[derive(Debug, Error)]
pub enum FaucetError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// An HTTP response with a non-success status code.
    ///
    /// Contains the status code, URL, and (truncated) response body for
    /// debugging.  Whether this error is retriable depends on the status code
    /// — see [`FaucetError::is_retriable`].
    #[error("HTTP {status} from {url}: {body}")]
    HttpStatus {
        status: u16,
        url: String,
        body: String,
    },

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("JSONPath error: {0}")]
    JsonPath(String),

    #[error("Auth error: {0}")]
    Auth(String),

    /// The server responded with HTTP 429 Too Many Requests.
    /// The inner value is the duration to wait before retrying,
    /// parsed from the `Retry-After` response header (default: 60 s).
    #[error("Rate limited: retry after {0:?}")]
    RateLimited(Duration),

    /// A URL could not be constructed or parsed.
    #[error("URL error: {0}")]
    Url(String),

    /// A record transform could not be compiled or applied (e.g. invalid regex).
    #[error("Transform error: {0}")]
    Transform(String),
}

impl FaucetError {
    /// Whether this error is transient and the request should be retried.
    ///
    /// Retriable errors:
    /// - Network / connection errors (`Http` from reqwest)
    /// - Server errors (5xx status codes)
    /// - Rate limiting (429 — handled separately with `Retry-After`)
    ///
    /// Non-retriable errors:
    /// - Client errors (4xx except 429)
    /// - JSON parse / JSONPath / auth / transform errors
    pub fn is_retriable(&self) -> bool {
        match self {
            // reqwest errors: connection timeouts, DNS failures, etc. are retriable.
            FaucetError::Http(e) => {
                // If it's a status error that leaked through, check the code.
                if let Some(status) = e.status() {
                    status.is_server_error()
                } else {
                    // Connection errors, timeouts, etc.
                    true
                }
            }
            FaucetError::HttpStatus { status, .. } => *status >= 500,
            FaucetError::RateLimited(_) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_status_5xx_is_retriable() {
        let err = FaucetError::HttpStatus {
            status: 500,
            url: "https://example.com".into(),
            body: "Internal Server Error".into(),
        };
        assert!(err.is_retriable());

        let err = FaucetError::HttpStatus {
            status: 503,
            url: "https://example.com".into(),
            body: "".into(),
        };
        assert!(err.is_retriable());
    }

    #[test]
    fn http_status_4xx_is_not_retriable() {
        let err = FaucetError::HttpStatus {
            status: 400,
            url: "https://example.com".into(),
            body: "Bad Request".into(),
        };
        assert!(!err.is_retriable());

        let err = FaucetError::HttpStatus {
            status: 404,
            url: "https://example.com".into(),
            body: "".into(),
        };
        assert!(!err.is_retriable());
    }

    #[test]
    fn rate_limited_is_retriable() {
        let err = FaucetError::RateLimited(Duration::from_secs(30));
        assert!(err.is_retriable());
    }

    #[test]
    fn json_error_is_not_retriable() {
        let serde_err = serde_json::from_str::<serde_json::Value>("not json").unwrap_err();
        let err = FaucetError::Json(serde_err);
        assert!(!err.is_retriable());
    }

    #[test]
    fn jsonpath_error_is_not_retriable() {
        let err = FaucetError::JsonPath("bad path".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn auth_error_is_not_retriable() {
        let err = FaucetError::Auth("invalid token".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn url_error_is_not_retriable() {
        let err = FaucetError::Url("bad url".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn transform_error_is_not_retriable() {
        let err = FaucetError::Transform("bad regex".into());
        assert!(!err.is_retriable());
    }

    #[test]
    fn http_status_display_includes_url_and_body() {
        let err = FaucetError::HttpStatus {
            status: 422,
            url: "https://api.example.com/test".into(),
            body: "Unprocessable Entity".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("422"));
        assert!(msg.contains("https://api.example.com/test"));
        assert!(msg.contains("Unprocessable Entity"));
    }
}
