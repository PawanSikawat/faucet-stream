//! Elasticsearch sink configuration.

use serde::{Deserialize, Serialize};

/// Authentication method for the Elasticsearch sink.
#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum ElasticsearchSinkAuth {
    /// No authentication.
    None,
    /// HTTP Basic authentication.
    Basic { username: String, password: String },
    /// Bearer token authentication.
    Bearer(String),
    /// API key authentication (sent as `ApiKey` in the `Authorization` header).
    ApiKey(String),
}

impl std::fmt::Debug for ElasticsearchSinkAuth {
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

/// Configuration for the Elasticsearch bulk index sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchSinkConfig {
    /// Base URL of the Elasticsearch cluster (e.g. `"http://localhost:9200"`).
    pub base_url: String,
    /// Target index name.
    pub index: String,
    /// Authentication method.
    pub auth: ElasticsearchSinkAuth,
    /// Maximum number of documents per `_bulk` request. Defaults to `500`.
    pub batch_size: usize,
    /// Optional JSON field name to use as the document `_id`.
    /// If `None`, Elasticsearch auto-generates IDs.
    pub id_field: Option<String>,
}

impl ElasticsearchSinkConfig {
    /// Create a new config with the required fields and sensible defaults.
    pub fn new(base_url: impl Into<String>, index: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            index: index.into(),
            auth: ElasticsearchSinkAuth::None,
            batch_size: 500,
            id_field: None,
        }
    }

    /// Set the authentication method.
    pub fn auth(mut self, a: ElasticsearchSinkAuth) -> Self {
        self.auth = a;
        self
    }

    /// Set the maximum batch size for bulk requests.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    /// Set the JSON field to use as the document `_id`.
    pub fn id_field(mut self, field: impl Into<String>) -> Self {
        self.id_field = Some(field.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = ElasticsearchSinkConfig::new("http://localhost:9200", "my_index");
        assert_eq!(config.base_url, "http://localhost:9200");
        assert_eq!(config.index, "my_index");
        assert_eq!(config.batch_size, 500);
        assert!(config.id_field.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = ElasticsearchSinkConfig::new("http://es:9200/", "idx")
            .batch_size(100)
            .id_field("doc_id")
            .auth(ElasticsearchSinkAuth::Bearer("tok".into()));
        assert_eq!(config.base_url, "http://es:9200");
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.id_field.as_deref(), Some("doc_id"));
    }

    #[test]
    fn auth_debug_masks_credentials() {
        let none = ElasticsearchSinkAuth::None;
        assert_eq!(format!("{none:?}"), "None");

        let basic = ElasticsearchSinkAuth::Basic {
            username: "user".into(),
            password: "secret".into(),
        };
        let debug = format!("{basic:?}");
        assert!(debug.contains("user"));
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));

        let bearer = ElasticsearchSinkAuth::Bearer("my-token".into());
        let debug = format!("{bearer:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-token"));

        let api_key = ElasticsearchSinkAuth::ApiKey("my-key".into());
        let debug = format!("{api_key:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("my-key"));
    }
}
