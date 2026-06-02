//! Configuration for the MSSQL sink.

use faucet_common_mssql::MssqlConnectionConfig;
use faucet_core::{FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

fn default_batch_size() -> usize {
    500
}
fn default_max_connections() -> u32 {
    5
}
fn default_statement_timeout_secs() -> u64 {
    300
}
fn default_true() -> bool {
    true
}

/// What to do with record keys that don't match a table column (`auto_columns`).
#[derive(Clone, Copy, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnUnknownField {
    /// Log a one-shot warning and drop the unknown keys (default).
    #[default]
    Warn,
    /// Silently drop the unknown keys.
    Drop,
    /// Fail the write with [`FaucetError::Sink`].
    Error,
}

/// How records map onto table columns.
///
/// Serializes as `{ type: json_column, column: "data" }` (default) or
/// `{ type: auto_columns, on_unknown_field: warn }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MssqlColumnMapping {
    /// Map top-level JSON keys to same-named table columns. IDENTITY columns are
    /// skipped (the server generates them).
    AutoColumns {
        #[serde(default)]
        on_unknown_field: OnUnknownField,
    },
    /// Serialize each record to a JSON string inserted into a single
    /// `NVARCHAR(MAX)` (or native `JSON`) column.
    JsonColumn { column: String },
}

impl Default for MssqlColumnMapping {
    fn default() -> Self {
        Self::JsonColumn {
            column: "data".into(),
        }
    }
}

/// Configuration for [`MssqlSink`](crate::MssqlSink).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct MssqlSinkConfig {
    /// Connection + TLS settings (`connection_url` or `connection_string`).
    #[serde(flatten)]
    pub connection: MssqlConnectionConfig,
    /// Target table, optionally schema-qualified (e.g. `dbo.events`).
    pub table: String,
    /// How records map onto columns. Defaults to a single `data` JSON column.
    #[serde(default)]
    pub column_mapping: MssqlColumnMapping,
    /// Rows per multi-row `INSERT`. Auto-split further so `rows * columns` stays
    /// within MSSQL's 2100-parameter limit. `0` sends the whole page (still
    /// param-split). Defaults to 500.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum pooled connections. Defaults to 5.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Wrap each batch's INSERTs in `BEGIN TRAN` / `COMMIT TRAN`. Defaults to true.
    #[serde(default = "default_true")]
    pub transaction_per_batch: bool,
    /// On a batch failure, retry row-by-row to isolate the offender so good rows
    /// still land and only the bad row is DLQ-routed. When false, one bad row
    /// fails the whole batch (fewer round-trips). Defaults to true.
    #[serde(default = "default_true")]
    pub isolate_row_failures: bool,
    /// Per-statement timeout in seconds (`0` disables). Defaults to 300.
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    /// In `json_column` mode only, create the table if absent as
    /// `(id BIGINT IDENTITY PRIMARY KEY, <column> NVARCHAR(MAX))`. Rejected with
    /// `auto_columns` (schema inference is unsafe for MSSQL types). Defaults to false.
    #[serde(default)]
    pub create_table: bool,
}

impl std::fmt::Debug for MssqlSinkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlSinkConfig")
            .field("connection", &"***")
            .field("table", &self.table)
            .field("column_mapping", &self.column_mapping)
            .field("batch_size", &self.batch_size)
            .field("max_connections", &self.max_connections)
            .field("transaction_per_batch", &self.transaction_per_batch)
            .field("isolate_row_failures", &self.isolate_row_failures)
            .field("statement_timeout_secs", &self.statement_timeout_secs)
            .field("create_table", &self.create_table)
            .finish()
    }
}

impl MssqlSinkConfig {
    /// Build a config from a connection URL and table, with defaults elsewhere.
    pub fn new(connection_url: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            connection: MssqlConnectionConfig {
                connection_url: Some(connection_url.into()),
                ..Default::default()
            },
            table: table.into(),
            column_mapping: MssqlColumnMapping::default(),
            batch_size: default_batch_size(),
            max_connections: default_max_connections(),
            transaction_per_batch: true,
            isolate_row_failures: true,
            statement_timeout_secs: default_statement_timeout_secs(),
            create_table: false,
        }
    }

    /// Validate connection source, batch size, table, and mode combination.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_batch_size(self.batch_size)?;
        if self.table.trim().is_empty() {
            return Err(FaucetError::Config("MSSQL sink requires a `table`".into()));
        }
        if self.create_table
            && matches!(self.column_mapping, MssqlColumnMapping::AutoColumns { .. })
        {
            return Err(FaucetError::Config(
                "MSSQL sink `create_table` is only supported with `json_column` mode \
                 (schema inference for auto_columns is unsafe — create the table first)"
                    .into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn json_column_is_default() {
        assert_eq!(
            MssqlColumnMapping::default(),
            MssqlColumnMapping::JsonColumn {
                column: "data".into()
            }
        );
    }

    #[test]
    fn column_mapping_round_trips() {
        let auto: MssqlColumnMapping =
            serde_json::from_value(json!({"type": "auto_columns", "on_unknown_field": "error"}))
                .unwrap();
        assert_eq!(
            auto,
            MssqlColumnMapping::AutoColumns {
                on_unknown_field: OnUnknownField::Error
            }
        );

        let jc: MssqlColumnMapping =
            serde_json::from_value(json!({"type": "json_column", "column": "payload"})).unwrap();
        assert_eq!(
            jc,
            MssqlColumnMapping::JsonColumn {
                column: "payload".into()
            }
        );
    }

    #[test]
    fn auto_columns_defaults_unknown_field_to_warn() {
        let auto: MssqlColumnMapping =
            serde_json::from_value(json!({"type": "auto_columns"})).unwrap();
        assert_eq!(
            auto,
            MssqlColumnMapping::AutoColumns {
                on_unknown_field: OnUnknownField::Warn
            }
        );
    }

    #[test]
    fn config_defaults() {
        let cfg: MssqlSinkConfig = serde_json::from_value(json!({
            "connection_url": "mssql://sa:pw@h/db",
            "table": "dbo.events",
        }))
        .unwrap();
        assert_eq!(cfg.batch_size, 500);
        assert_eq!(cfg.max_connections, 5);
        assert!(cfg.transaction_per_batch);
        assert!(cfg.isolate_row_failures);
        assert_eq!(cfg.statement_timeout_secs, 300);
        assert!(!cfg.create_table);
    }

    #[test]
    fn validate_rejects_auto_columns_with_create_table() {
        let cfg = MssqlSinkConfig {
            column_mapping: MssqlColumnMapping::AutoColumns {
                on_unknown_field: OnUnknownField::Warn,
            },
            create_table: true,
            ..MssqlSinkConfig::new("mssql://sa:pw@h/db", "dbo.events")
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_empty_table() {
        let cfg = MssqlSinkConfig::new("mssql://sa:pw@h/db", "  ");
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn debug_masks_connection() {
        let cfg = MssqlSinkConfig::new("mssql://sa:secret@h/db", "dbo.t");
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("secret"));
    }
}
