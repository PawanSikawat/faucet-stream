//! SQLite source configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the SQLite query source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SqliteSourceConfig {
    /// SQLite database URL (file path or `sqlite::memory:`).
    pub database_url: String,
    /// SQL query to execute.
    pub query: String,
    /// Maximum number of connections in the pool. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

fn default_max_connections() -> u32 {
    10
}

impl SqliteSourceConfig {
    /// Create a new config with the required database URL and query.
    pub fn new(database_url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            query: query.into(),
            max_connections: 10,
        }
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

    #[test]
    fn default_config() {
        let config = SqliteSourceConfig::new("sqlite:test.db", "SELECT * FROM events");
        assert_eq!(config.database_url, "sqlite:test.db");
        assert_eq!(config.query, "SELECT * FROM events");
    }

    #[test]
    fn memory_database() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1");
        assert_eq!(config.database_url, "sqlite::memory:");
    }
}
