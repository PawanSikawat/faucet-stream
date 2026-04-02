//! PostgreSQL sink configuration.

/// How to map JSON records to table columns.
#[derive(Debug, Clone)]
pub enum PostgresColumnMapping {
    /// Insert each record as a single `jsonb` column. The column name
    /// defaults to `"data"` but can be overridden.
    Jsonb { column: String },
    /// Map top-level JSON keys directly to table columns.
    /// Only keys that match existing columns are inserted; extra keys are ignored.
    AutoMap,
}

impl Default for PostgresColumnMapping {
    fn default() -> Self {
        Self::Jsonb {
            column: "data".into(),
        }
    }
}

/// Configuration for the PostgreSQL sink.
#[derive(Debug, Clone)]
pub struct PostgresSinkConfig {
    /// PostgreSQL connection URL (e.g. `postgres://user:pass@host/db`).
    pub connection_url: String,
    /// Target table name.
    pub table_name: String,
    /// How to map JSON records to columns.
    pub column_mapping: PostgresColumnMapping,
    /// Maximum number of rows per INSERT statement. Defaults to 500.
    pub batch_size: usize,
}

impl PostgresSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(connection_url: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            connection_url: connection_url.into(),
            table_name: table_name.into(),
            column_mapping: PostgresColumnMapping::default(),
            batch_size: 500,
        }
    }

    /// Set the column mapping strategy.
    pub fn column_mapping(mut self, mapping: PostgresColumnMapping) -> Self {
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
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events");
        assert_eq!(config.table_name, "events");
        assert_eq!(config.batch_size, 500);
        assert!(matches!(
            config.column_mapping,
            PostgresColumnMapping::Jsonb { ref column } if column == "data"
        ));
    }

    #[test]
    fn builder_methods() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events")
            .column_mapping(PostgresColumnMapping::AutoMap)
            .batch_size(100);
        assert_eq!(config.batch_size, 100);
        assert!(matches!(
            config.column_mapping,
            PostgresColumnMapping::AutoMap
        ));
    }

    #[test]
    fn jsonb_custom_column() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events").column_mapping(
            PostgresColumnMapping::Jsonb {
                column: "payload".into(),
            },
        );
        assert!(matches!(
            config.column_mapping,
            PostgresColumnMapping::Jsonb { ref column } if column == "payload"
        ));
    }
}
