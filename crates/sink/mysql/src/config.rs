//! MySQL sink configuration.

/// How to map JSON records to table columns.
#[derive(Debug, Clone)]
pub enum MysqlColumnMapping {
    /// Insert each record as a single JSON column. The column name
    /// defaults to `"data"` but can be overridden.
    Json { column: String },
    /// Map top-level JSON keys directly to table columns.
    /// Only keys that match existing columns are inserted; extra keys are ignored.
    AutoMap,
}

impl Default for MysqlColumnMapping {
    fn default() -> Self {
        Self::Json {
            column: "data".into(),
        }
    }
}

/// Configuration for the MySQL sink.
#[derive(Clone)]
pub struct MysqlSinkConfig {
    /// MySQL connection URL (e.g. `mysql://user:pass@host/db`).
    pub connection_url: String,
    /// Target table name.
    pub table_name: String,
    /// How to map JSON records to columns.
    pub column_mapping: MysqlColumnMapping,
    /// Maximum number of rows per INSERT statement. Defaults to 500.
    pub batch_size: usize,
}

impl std::fmt::Debug for MysqlSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlSinkConfig")
            .field("connection_url", &"***")
            .field("table_name", &self.table_name)
            .field("column_mapping", &self.column_mapping)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl MysqlSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(connection_url: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            connection_url: connection_url.into(),
            table_name: table_name.into(),
            column_mapping: MysqlColumnMapping::default(),
            batch_size: 500,
        }
    }

    /// Set the column mapping strategy.
    pub fn column_mapping(mut self, mapping: MysqlColumnMapping) -> Self {
        self.column_mapping = mapping;
        self
    }

    /// Set the batch size for INSERT statements.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events");
        assert_eq!(config.table_name, "events");
        assert_eq!(config.batch_size, 500);
        assert!(matches!(
            config.column_mapping,
            MysqlColumnMapping::Json { ref column } if column == "data"
        ));
    }

    #[test]
    fn builder_methods() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events")
            .column_mapping(MysqlColumnMapping::AutoMap)
            .batch_size(100);
        assert_eq!(config.batch_size, 100);
        assert!(matches!(config.column_mapping, MysqlColumnMapping::AutoMap));
    }

    #[test]
    fn json_custom_column() {
        let config = MysqlSinkConfig::new("mysql://localhost/test", "events").column_mapping(
            MysqlColumnMapping::Json {
                column: "payload".into(),
            },
        );
        assert!(matches!(
            config.column_mapping,
            MysqlColumnMapping::Json { ref column } if column == "payload"
        ));
    }

    #[test]
    fn debug_masks_connection_url() {
        let config = MysqlSinkConfig::new("mysql://secret:pass@host/db", "events");
        let debug = format!("{config:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret"));
        assert!(!debug.contains("pass"));
    }
}
