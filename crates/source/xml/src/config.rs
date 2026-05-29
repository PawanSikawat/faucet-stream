//! XML source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use reqwest::header::HeaderMap;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Authentication for XML API endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "config", rename_all = "snake_case")]
pub enum XmlAuth {
    /// No authentication.
    None,
    /// Bearer token.
    Bearer { token: String },
    /// Basic authentication.
    Basic { username: String, password: String },
    /// Custom headers (e.g. SOAP action headers, API keys).
    Custom { headers: HashMap<String, String> },
}

/// Pagination configuration for XML APIs.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum XmlPagination {
    /// Page-number pagination with a query parameter.
    PageNumber {
        param_name: String,
        start_page: usize,
        page_size: Option<usize>,
        page_size_param: Option<String>,
    },
    /// Offset/limit pagination.
    Offset {
        offset_param: String,
        limit_param: String,
        limit: usize,
    },
}

/// Configuration for the XML source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct XmlStreamConfig {
    /// Base URL of the API.
    pub base_url: String,
    /// Request path (appended to base_url).
    pub path: String,
    /// HTTP method (GET or POST for SOAP).
    #[serde(with = "crate::serde_helpers::http_method")]
    #[schemars(with = "String")]
    pub method: reqwest::Method,
    /// Authentication method.
    pub auth: XmlAuth,
    /// Additional request headers.
    #[serde(skip, default)]
    pub headers: HeaderMap,
    /// Optional request body (e.g. SOAP envelope).
    pub body: Option<String>,
    /// Dot-separated path to the repeating element in the XML response
    /// (e.g. `"Envelope.Body.GetUsersResponse.Users.User"`).
    pub records_element_path: Option<String>,
    /// Pagination configuration.
    pub pagination: Option<XmlPagination>,
    /// Maximum number of pages to fetch.
    pub max_pages: Option<usize>,
    /// Query parameters to include in every request.
    pub query_params: std::collections::HashMap<String, String>,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). The
    /// event-driven XML parser accumulates matched subtrees into a buffer
    /// and yields whenever the buffer reaches this size. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the document is
    /// drained end-to-end and the entire result set is emitted in a single
    /// page. Useful for small lookup payloads or for sinks (e.g. SQL `COPY`,
    /// BigQuery load jobs) that prefer one large request to many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl XmlStreamConfig {
    /// Create a new config with required fields.
    pub fn new(base_url: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            path: path.into(),
            method: reqwest::Method::GET,
            auth: XmlAuth::None,
            headers: HeaderMap::new(),
            body: None,
            records_element_path: None,
            pagination: None,
            max_pages: None,
            query_params: std::collections::HashMap::new(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the HTTP method (default: GET).
    pub fn method(mut self, method: reqwest::Method) -> Self {
        self.method = method;
        self
    }

    /// Set the authentication method.
    pub fn auth(mut self, auth: XmlAuth) -> Self {
        self.auth = auth;
        self
    }

    /// Set additional headers.
    pub fn headers(mut self, headers: HeaderMap) -> Self {
        self.headers = headers;
        self
    }

    /// Set a SOAP or XML request body.
    pub fn body(mut self, body: impl Into<String>) -> Self {
        self.body = Some(body.into());
        self
    }

    /// Set the dot-separated path to the repeating element.
    pub fn records_element_path(mut self, path: impl Into<String>) -> Self {
        self.records_element_path = Some(path.into());
        self
    }

    /// Set pagination configuration.
    pub fn pagination(mut self, pagination: XmlPagination) -> Self {
        self.pagination = Some(pagination);
        self
    }

    /// Set the maximum number of pages.
    pub fn max_pages(mut self, max: usize) -> Self {
        self.max_pages = Some(max);
        self
    }

    /// Add a query parameter.
    pub fn query_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query_params.insert(key.into(), value.into());
        self
    }

    /// Set the per-page record count for
    /// [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire document is drained and
    /// emitted in a single [`StreamPage`](faucet_core::StreamPage).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users");
        assert_eq!(config.base_url, "https://api.example.com");
        assert_eq!(config.path, "/users");
        assert_eq!(config.method, reqwest::Method::GET);
        assert!(config.records_element_path.is_none());
    }

    #[test]
    fn soap_config() {
        let config = XmlStreamConfig::new("https://api.example.com", "/soap")
            .method(reqwest::Method::POST)
            .body("<Envelope><Body><GetUsers/></Body></Envelope>")
            .records_element_path("Envelope.Body.GetUsersResponse.Users.User");
        assert_eq!(config.method, reqwest::Method::POST);
        assert!(config.body.is_some());
        assert_eq!(
            config.records_element_path.unwrap(),
            "Envelope.Body.GetUsersResponse.Users.User"
        );
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = XmlStreamConfig::new("https://api.example.com", "/users")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "base_url": "https://api.example.com",
            "path": "/users.xml",
            "method": "GET",
            "auth": { "type": "none" },
            "body": null,
            "records_element_path": "root.user",
            "pagination": null,
            "max_pages": null,
            "query_params": {},
            "batch_size": 250
        }"#;
        let config: XmlStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_missing_from_json() {
        // The `#[serde(default = "default_batch_size")]` attribute is the
        // user-facing contract — older configs without `batch_size` must
        // continue to deserialize and adopt the library default.
        let json = r#"{
            "base_url": "https://api.example.com",
            "path": "/users.xml",
            "method": "GET",
            "auth": { "type": "none" },
            "body": null,
            "records_element_path": null,
            "pagination": null,
            "max_pages": null,
            "query_params": {}
        }"#;
        let config: XmlStreamConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
