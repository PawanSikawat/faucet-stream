//! SQLite sink configuration.

/// How to map JSON records to table columns.
#[derive(Debug, Clone)]
pub enum SqliteColumnMapping {
    /// Insert each record as a single JSON text column. The column name
    /// defaults to `"data"` but can be overridden.
    Json { column: String },
    /// Map top-level JSON keys directly to table columns.
    /// Only keys that match existing columns are inserted; extra keys are ignored.
    AutoMap,
}

impl Default for SqliteColumnMapping {
    fn default() -> Self {
        Self::Json {
            column: "data".into(),
        }
    }
}

/// Configuration for the SQLite sink.
#[derive(Debug, Clone)]
pub struct SqliteSinkConfig {
    /// SQLite database URL (file path or `:memory:`).
    pub database_url: String,
    /// Target table name.
    pub table_name: String,
    /// How to map JSON records to columns.
    pub column_mapping: SqliteColumnMapping,
    /// Maximum number of rows per INSERT batch. Defaults to 500.
    pub batch_size: usize,
    /// Maximum number of connections in the pool. Defaults to 5.
    pub max_connections: u32,
}

impl SqliteSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(database_url: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            table_name: table_name.into(),
            column_mapping: SqliteColumnMapping::default(),
            batch_size: 500,
            max_connections: 5,
        }
    }

    /// Set the column mapping strategy.
    pub fn column_mapping(mut self, mapping: SqliteColumnMapping) -> Self {
        self.column_mapping = mapping;
        self
    }

    /// Set the batch size for INSERT statements.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }

    /// Set the maximum number of connections in the pool.
    pub fn max_connections(mut self, n: u32) -> Self {
        self.max_connections = n;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events");
        assert_eq!(config.table_name, "events");
        assert_eq!(config.batch_size, 500);
        assert!(matches!(
            config.column_mapping,
            SqliteColumnMapping::Json { ref column } if column == "data"
        ));
    }

    #[test]
    fn builder_methods() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events")
            .column_mapping(SqliteColumnMapping::AutoMap)
            .batch_size(100);
        assert_eq!(config.batch_size, 100);
        assert!(matches!(
            config.column_mapping,
            SqliteColumnMapping::AutoMap
        ));
    }

    #[test]
    fn json_custom_column() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events").column_mapping(
            SqliteColumnMapping::Json {
                column: "payload".into(),
            },
        );
        assert!(matches!(
            config.column_mapping,
            SqliteColumnMapping::Json { ref column } if column == "payload"
        ));
    }

    #[test]
    fn config_with_file_path() {
        let config = SqliteSinkConfig::new("/tmp/test.db", "events");
        assert_eq!(config.database_url, "/tmp/test.db");
    }
}
