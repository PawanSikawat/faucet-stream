//! HTTP sink configuration.

use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Authentication method for the HTTP sink.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum HttpSinkAuth {
    /// No authentication.
    None,
    /// Bearer token in the Authorization header.
    Bearer {
        /// The token value (sent as `Authorization: Bearer <token>`).
        token: String,
    },
    /// HTTP Basic authentication.
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// Custom headers for authentication (e.g. API keys).
    Custom {
        /// Header name → value map applied to every request.
        headers: HashMap<String, String>,
    },
}

impl std::fmt::Debug for HttpSinkAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Bearer { .. } => f.debug_tuple("Bearer").field(&"***").finish(),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Self::Custom { .. } => f.debug_tuple("Custom").field(&"***").finish(),
        }
    }
}

/// How records are sent in HTTP requests.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum HttpBatchMode {
    /// Send one HTTP request per record.
    Individual,
    /// Send all records as a JSON array in a single request.
    Array,
}

/// Configuration for the HTTP sink connector.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct HttpSinkConfig {
    /// Target endpoint URL.
    pub url: String,
    /// HTTP method (default: POST).
    #[serde(with = "crate::serde_helpers::http_method")]
    #[schemars(with = "String")]
    pub method: reqwest::Method,
    /// Additional request headers.
    #[serde(skip, default)]
    pub headers: HeaderMap,
    /// Authentication method.
    pub auth: HttpSinkAuth,
    /// How to batch records in requests.
    pub batch_mode: HttpBatchMode,
    /// Number of retries on transient failures (default: 0).
    pub max_retries: usize,
    /// Maximum number of concurrent requests in Individual mode (default: 10).
    pub concurrency: usize,
}

impl std::fmt::Debug for HttpSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSinkConfig")
            .field("url", &self.url)
            .field("method", &self.method)
            .field("headers", &self.headers)
            .field("auth", &self.auth)
            .field("batch_mode", &self.batch_mode)
            .field("max_retries", &self.max_retries)
            .field("concurrency", &self.concurrency)
            .finish()
    }
}

impl HttpSinkConfig {
    /// Create a new config with the given URL and sensible defaults.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            method: reqwest::Method::POST,
            headers: HeaderMap::new(),
            auth: HttpSinkAuth::None,
            batch_mode: HttpBatchMode::Individual,
            max_retries: 0,
            concurrency: 10,
        }
    }

    /// Set the HTTP method.
    pub fn method(mut self, method: reqwest::Method) -> Self {
        self.method = method;
        self
    }

    /// Set additional request headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Set the authentication method.
    pub fn auth(mut self, auth: HttpSinkAuth) -> Self {
        self.auth = auth;
        self
    }

    /// Set the batch mode.
    pub fn batch_mode(mut self, mode: HttpBatchMode) -> Self {
        self.batch_mode = mode;
        self
    }

    /// Set the maximum number of retries.
    pub fn max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the maximum number of concurrent requests in Individual mode.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = HttpSinkConfig::new("https://api.example.com/ingest");
        assert_eq!(config.url, "https://api.example.com/ingest");
        assert_eq!(config.method, reqwest::Method::POST);
        assert_eq!(config.max_retries, 0);
        assert!(matches!(config.auth, HttpSinkAuth::None));
        assert!(matches!(config.batch_mode, HttpBatchMode::Individual));
    }

    #[test]
    fn builder_methods() {
        let config = HttpSinkConfig::new("https://api.example.com/ingest")
            .method(reqwest::Method::PUT)
            .auth(HttpSinkAuth::Bearer {
                token: "token123".into(),
            })
            .batch_mode(HttpBatchMode::Array)
            .max_retries(3);
        assert_eq!(config.method, reqwest::Method::PUT);
        assert_eq!(config.max_retries, 3);
        assert!(matches!(config.batch_mode, HttpBatchMode::Array));
    }

    #[test]
    fn auth_debug_masks_secrets() {
        let bearer = HttpSinkAuth::Bearer {
            token: "secret-token".into(),
        };
        let debug = format!("{bearer:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret-token"));

        let basic = HttpSinkAuth::Basic {
            username: "user".into(),
            password: "secret-pass".into(),
        };
        let debug = format!("{basic:?}");
        assert!(debug.contains("user"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret-pass"));

        let custom = HttpSinkAuth::Custom {
            headers: HashMap::new(),
        };
        let debug = format!("{custom:?}");
        assert!(debug.contains("***"));
    }
}
