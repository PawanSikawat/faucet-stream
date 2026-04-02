//! HTTP sink configuration.

use reqwest::header::HeaderMap;

/// Authentication method for the HTTP sink.
#[derive(Clone)]
pub enum HttpSinkAuth {
    /// No authentication.
    None,
    /// Bearer token in the Authorization header.
    Bearer(String),
    /// HTTP Basic authentication.
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// Custom headers for authentication (e.g. API keys).
    Custom(HeaderMap),
}

impl std::fmt::Debug for HttpSinkAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Bearer(_) => f.debug_tuple("Bearer").field(&"***").finish(),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Self::Custom(_) => f.debug_tuple("Custom").field(&"***").finish(),
        }
    }
}

/// How records are sent in HTTP requests.
#[derive(Debug, Clone)]
pub enum HttpBatchMode {
    /// Send one HTTP request per record.
    Individual,
    /// Send all records as a JSON array in a single request.
    Array,
}

/// Configuration for the HTTP sink connector.
#[derive(Clone)]
pub struct HttpSinkConfig {
    /// Target endpoint URL.
    pub url: String,
    /// HTTP method (default: POST).
    pub method: reqwest::Method,
    /// Additional request headers.
    pub headers: HeaderMap,
    /// Authentication method.
    pub auth: HttpSinkAuth,
    /// How to batch records in requests.
    pub batch_mode: HttpBatchMode,
    /// Number of retries on transient failures (default: 0).
    pub max_retries: usize,
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
            .auth(HttpSinkAuth::Bearer("token123".into()))
            .batch_mode(HttpBatchMode::Array)
            .max_retries(3);
        assert_eq!(config.method, reqwest::Method::PUT);
        assert_eq!(config.max_retries, 3);
        assert!(matches!(config.batch_mode, HttpBatchMode::Array));
    }

    #[test]
    fn auth_debug_masks_secrets() {
        let bearer = HttpSinkAuth::Bearer("secret-token".into());
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

        let custom = HttpSinkAuth::Custom(HeaderMap::new());
        let debug = format!("{custom:?}");
        assert!(debug.contains("***"));
    }
}
