//! SQLite source configuration.

/// Configuration for the SQLite query source.
#[derive(Debug, Clone)]
pub struct SqliteSourceConfig {
    /// SQLite database URL (file path or `sqlite::memory:`).
    pub database_url: String,
    /// SQL query to execute.
    pub query: String,
}

impl SqliteSourceConfig {
    /// Create a new config with the required database URL and query.
    pub fn new(database_url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            query: query.into(),
        }
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
