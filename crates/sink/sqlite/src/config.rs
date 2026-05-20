//! SQLite sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to map JSON records to table columns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
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
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SqliteSinkConfig {
    /// SQLite database URL (file path or `:memory:`).
    pub database_url: String,
    /// Target table name.
    pub table_name: String,
    /// How to map JSON records to columns.
    pub column_mapping: SqliteColumnMapping,
    /// Maximum number of rows per multi-row INSERT. Defaults to
    /// [`DEFAULT_BATCH_SIZE`] (1000); the recommended value for SQLite.
    ///
    /// When the upstream `StreamPage` carries more records than `batch_size`,
    /// the sink slices the page into `batch_size`-row chunks and issues one
    /// multi-row INSERT per chunk, each wrapped in its own `BEGIN`/`COMMIT`
    /// transaction. When `batch_size = 0`, the entire slice is written as a
    /// single multi-row INSERT inside one transaction — useful when the
    /// source already emits pages sized for SQLite (e.g. a Postgres source
    /// configured with `batch_size: 1000`).
    ///
    /// `batch_size = 0` is the "no batching" sentinel: upstream framing is
    /// preserved exactly, no internal re-chunking is performed.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum number of connections in the pool. Defaults to 5.
    pub max_connections: u32,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl SqliteSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(database_url: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            database_url: database_url.into(),
            table_name: table_name.into(),
            column_mapping: SqliteColumnMapping::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_connections: 5,
        }
    }

    /// Set the column mapping strategy.
    pub fn column_mapping(mut self, mapping: SqliteColumnMapping) -> Self {
        self.column_mapping = mapping;
        self
    }

    /// Set the maximum number of rows per multi-row INSERT.
    ///
    /// Pass `0` to opt out of re-chunking — the entire records slice handed
    /// to `write_batch` is written in a single multi-row INSERT inside one
    /// transaction, preserving upstream `StreamPage` framing.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
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
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert!(matches!(
            config.column_mapping,
            SqliteColumnMapping::Json { ref column } if column == "data"
        ));
    }

    #[test]
    fn builder_methods() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events")
            .column_mapping(SqliteColumnMapping::AutoMap)
            .with_batch_size(100);
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

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events").with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = SqliteSinkConfig::new("sqlite::memory:", "events")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "database_url": "sqlite::memory:",
            "table_name": "events",
            "column_mapping": {"json": {"column": "data"}},
            "batch_size": 250,
            "max_connections": 5
        }"#;
        let config: SqliteSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_in_json() {
        let json = r#"{
            "database_url": "sqlite::memory:",
            "table_name": "events",
            "column_mapping": {"json": {"column": "data"}},
            "max_connections": 5
        }"#;
        let config: SqliteSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
