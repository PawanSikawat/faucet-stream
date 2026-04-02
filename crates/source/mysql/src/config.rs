//! MySQL source configuration.

/// Configuration for the MySQL query source.
#[derive(Clone)]
pub struct MysqlSourceConfig {
    /// MySQL connection URL (e.g. `mysql://user:pass@host/db`).
    pub connection_url: String,
    /// SQL query to execute.
    pub query: String,
    /// Maximum number of connections in the pool. Defaults to 10.
    pub max_connections: u32,
}

impl std::fmt::Debug for MysqlSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlSourceConfig")
            .field("connection_url", &"***")
            .field("query", &self.query)
            .field("max_connections", &self.max_connections)
            .finish()
    }
}

impl MysqlSourceConfig {
    /// Create a new config with the required connection URL and query.
    pub fn new(connection_url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            connection_url: connection_url.into(),
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
        let config = MysqlSourceConfig::new("mysql://localhost/test", "SELECT * FROM events");
        assert_eq!(config.query, "SELECT * FROM events");
    }

    #[test]
    fn debug_masks_connection_url() {
        let config = MysqlSourceConfig::new("mysql://secret:pass@host/db", "SELECT 1");
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("pass"));
    }
}
