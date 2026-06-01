//! PostgreSQL sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to map JSON records to table columns.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
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
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct PostgresSinkConfig {
    /// PostgreSQL connection URL (e.g. `postgres://user:pass@host/db`).
    pub connection_url: String,
    /// Target table name.
    pub table_name: String,
    /// Optional schema (namespace) qualifying [`table_name`](Self::table_name).
    ///
    /// When set, both the AutoMap column-discovery probe and the `INSERT`
    /// target `schema.table_name` explicitly. When unset (the default), the
    /// table resolves against the connection's `search_path`, and column
    /// discovery is scoped to whichever schema the `INSERT` actually resolves
    /// to — so a same-named table in another schema no longer pollutes the
    /// AutoMap column set (#146 M13).
    #[serde(default)]
    pub schema: Option<String>,
    /// How to map JSON records to columns.
    pub column_mapping: PostgresColumnMapping,
    /// Maximum rows per multi-row `INSERT` statement. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// When the upstream `StreamPage` carries more records than `batch_size`,
    /// the sink slices the page into `batch_size`-row chunks and issues one
    /// multi-row `INSERT` per chunk. When `batch_size = 0`, the entire slice
    /// is sent in a single `INSERT` — useful when the source already chunks
    /// to a Postgres-friendly size.
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the entire upstream
    /// page is forwarded in one statement, subject to Postgres' natural
    /// per-statement bind-parameter limit of 65 535. AutoMap mode binds one
    /// parameter per column per row, so the safe ceiling is roughly
    /// `65_535 / num_columns` rows per call; JSONB mode binds a single
    /// array parameter and has no such ceiling. Keep the default unless the
    /// upstream page size is already tuned for Postgres.
    ///
    /// **Recommended value: ~1000** — Postgres' multi-row `INSERT` sweet
    /// spot. Larger chunks rarely add throughput and risk hitting the
    /// 65 535-parameter ceiling in AutoMap mode.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum number of connections in the pool. Defaults to 5.
    pub max_connections: u32,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl std::fmt::Debug for PostgresSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresSinkConfig")
            .field("connection_url", &"***")
            .field("table_name", &self.table_name)
            .field("schema", &self.schema)
            .field("column_mapping", &self.column_mapping)
            .field("batch_size", &self.batch_size)
            .field("max_connections", &self.max_connections)
            .finish()
    }
}

impl PostgresSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(connection_url: impl Into<String>, table_name: impl Into<String>) -> Self {
        Self {
            connection_url: connection_url.into(),
            table_name: table_name.into(),
            schema: None,
            column_mapping: PostgresColumnMapping::default(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_connections: 5,
        }
    }

    /// Set the schema (namespace) that qualifies the table. When unset, the
    /// table resolves against the connection's `search_path`.
    pub fn with_schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    /// Set the column mapping strategy.
    pub fn column_mapping(mut self, mapping: PostgresColumnMapping) -> Self {
        self.column_mapping = mapping;
        self
    }

    /// Set the per-statement row count for multi-row `INSERT`.
    ///
    /// Pass `0` to opt out of re-chunking — the sink forwards each upstream
    /// [`StreamPage`](faucet_core::StreamPage) as a single `INSERT`
    /// statement. Postgres' multi-row `INSERT` sweet spot is ~1000 rows.
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
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events");
        assert_eq!(config.table_name, "events");
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert!(matches!(
            config.column_mapping,
            PostgresColumnMapping::Jsonb { ref column } if column == "data"
        ));
    }

    #[test]
    fn builder_methods() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events")
            .column_mapping(PostgresColumnMapping::AutoMap)
            .with_batch_size(100);
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

    #[test]
    fn with_batch_size_overrides_default() {
        let config =
            PostgresSinkConfig::new("postgres://localhost/test", "events").with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config =
            PostgresSinkConfig::new("postgres://localhost/test", "events").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events")
            .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "connection_url": "postgres://localhost/test",
            "table_name": "events",
            "column_mapping": {"jsonb": {"column": "data"}},
            "batch_size": 250,
            "max_connections": 5
        }"#;
        let config: PostgresSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_in_json() {
        let json = r#"{
            "connection_url": "postgres://localhost/test",
            "table_name": "events",
            "column_mapping": {"jsonb": {"column": "data"}},
            "max_connections": 5
        }"#;
        let config: PostgresSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn config_builder_chaining() {
        let config = PostgresSinkConfig::new("postgres://localhost/test", "events")
            .with_batch_size(100)
            .with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }
}
