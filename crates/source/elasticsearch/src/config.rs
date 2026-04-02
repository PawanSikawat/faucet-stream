//! Elasticsearch source configuration.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Authentication method for Elasticsearch.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum ElasticsearchAuth {
    /// No authentication.
    None,
    /// HTTP Basic authentication.
    Basic { username: String, password: String },
    /// Bearer token authentication.
    Bearer(String),
    /// API key authentication (sent as `ApiKey` in the `Authorization` header).
    ApiKey(String),
}

impl std::fmt::Debug for ElasticsearchAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Basic { username, .. } => f
                .debug_struct("Basic")
                .field("username", username)
                .field("password", &"***")
                .finish(),
            Self::Bearer(_) => write!(f, "Bearer(***)"),
            Self::ApiKey(_) => write!(f, "ApiKey(***)"),
        }
    }
}

/// Configuration for the Elasticsearch search source.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchSourceConfig {
    /// Base URL of the Elasticsearch cluster (e.g. `"http://localhost:9200"`).
    pub base_url: String,
    /// Index name to search.
    pub index: String,
    /// Elasticsearch query DSL. Defaults to `{"match_all": {}}`.
    pub query: Value,
    /// Scroll context timeout (e.g. `"1m"`). Defaults to `"1m"`.
    pub scroll_timeout: String,
    /// Number of documents per scroll page. Defaults to `1000`.
    pub scroll_size: usize,
    /// Authentication method.
    pub auth: ElasticsearchAuth,
    /// Maximum number of scroll pages to fetch. `None` means no limit.
    pub max_pages: Option<usize>,
}

impl ElasticsearchSourceConfig {
    /// Create a new config with the required fields and sensible defaults.
    pub fn new(base_url: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            index: index.into(),
            query: json!({"match_all": {}}),
            scroll_timeout: "1m".to_string(),
            scroll_size: 1000,
            auth: ElasticsearchAuth::None,
            max_pages: None,
        }
    }

    /// Set the Elasticsearch query DSL.
    pub fn query(mut self, q: Value) -> Self {
        self.query = q;
        self
    }

    /// Set the scroll context timeout (e.g. `"5m"`).
    pub fn scroll_timeout(mut self, t: impl Into<String>) -> Self {
        self.scroll_timeout = t.into();
        self
    }

    /// Set the number of documents per scroll page.
    pub fn scroll_size(mut self, n: usize) -> Self {
        self.scroll_size = n;
        self
    }

    /// Set the authentication method.
    pub fn auth(mut self, a: ElasticsearchAuth) -> Self {
        self.auth = a;
        self
    }

    /// Set the maximum number of scroll pages to fetch.
    pub fn max_pages(mut self, n: usize) -> Self {
        self.max_pages = Some(n);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = ElasticsearchSourceConfig::new("http://localhost:9200", "my_index");
        assert_eq!(config.base_url, "http://localhost:9200");
        assert_eq!(config.index, "my_index");
        assert_eq!(config.query, json!({"match_all": {}}));
        assert_eq!(config.scroll_timeout, "1m");
        assert_eq!(config.scroll_size, 1000);
        assert!(config.max_pages.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = ElasticsearchSourceConfig::new("http://es:9200/", "idx")
            .query(json!({"term": {"status": "active"}}))
            .scroll_timeout("5m")
            .scroll_size(500)
            .max_pages(10)
            .auth(ElasticsearchAuth::Bearer("tok".into()));
        assert_eq!(config.base_url, "http://es:9200");
        assert_eq!(config.scroll_timeout, "5m");
        assert_eq!(config.scroll_size, 500);
        assert_eq!(config.max_pages, Some(10));
    }

    #[test]
    fn auth_debug_masks_credentials() {
        let none = ElasticsearchAuth::None;
        assert_eq!(format!("{none:?}"), "None");

        let basic = ElasticsearchAuth::Basic {
            username: "user".into(),
            password: "secret".into(),
        };
        let debug = format!("{basic:?}");
        assert!(debug.contains("user"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));

        let bearer = ElasticsearchAuth::Bearer("my-token".into());
        let debug = format!("{bearer:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-token"));

        let api_key = ElasticsearchAuth::ApiKey("my-key".into());
        let debug = format!("{api_key:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-key"));
    }
}
