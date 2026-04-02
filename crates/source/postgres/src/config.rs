//! PostgreSQL source configuration.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Configuration for the PostgreSQL query source.
#[derive(Clone, Serialize, Deserialize)]
pub struct PostgresSourceConfig {
    /// PostgreSQL connection URL (e.g. `postgres://user:pass@host/db`).
    pub connection_url: String,
    /// SQL query to execute.
    pub query: String,
    /// Bind parameters for the query. Defaults to empty.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Maximum number of connections in the pool. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_max_connections() -> u32 {
    10
}

impl std::fmt::Debug for PostgresSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSourceConfig")
            .field("connection_url", &"***")
            .field("query", &self.query)
            .field("params", &self.params)
            .field("max_connections", &self.max_connections)
            .finish()
    }
}

impl PostgresSourceConfig {
    /// Create a new config with the required connection URL and query.
    pub fn new(connection_url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            connection_url: connection_url.into(),
            query: query.into(),
            params: Vec::new(),
            max_connections: 10,
        }
    }

    /// Set bind parameters for the query.
    pub fn params(mut self, params: Vec<Value>) -> Self {
        self.params = params;
        self
    }

    /// Set the maximum number of connections in the pool.
    pub fn with_max_connections(mut self, max_connections: u32) -> Self {
        self.max_connections = max_connections;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn default_config() {
        let config = PostgresSourceConfig::new("postgres://localhost/test", "SELECT * FROM events");
        assert_eq!(config.query, "SELECT * FROM events");
        assert!(config.params.is_empty());
    }

    #[test]
    fn builder_with_params() {
        let config = PostgresSourceConfig::new(
            "postgres://localhost/test",
            "SELECT * FROM events WHERE id = $1",
        )
        .params(vec![json!(42)]);
        assert_eq!(config.params.len(), 1);
        assert_eq!(config.params[0], json!(42));
    }

    #[test]
    fn debug_masks_connection_url() {
        let config = PostgresSourceConfig::new("postgres://secret:pass@host/db", "SELECT 1");
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("pass"));
    }
}
