//! Configuration for the MSSQL query source.

use faucet_common_mssql::MssqlConnectionConfig;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError, validate_batch_size};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_max_connections() -> u32 {
    10
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_statement_timeout_secs() -> u64 {
    300
}

/// How the source replicates rows across runs.
///
/// Serializes as `{ type: full }` or
/// `{ type: incremental, column: "...", initial_value: ... }`.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema, Default, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MssqlReplication {
    /// Every run fetches the full result set (default).
    #[default]
    Full,
    /// Only rows whose `column` is strictly greater than the stored bookmark
    /// (or `initial_value` on the first run) are emitted.
    ///
    /// The bookmark is applied two ways: if the query contains the literal
    /// token `@bookmark`, it is bound as a parameter so the server filters
    /// (efficient); the source *also* filters client-side as a correctness
    /// backstop. The new maximum of `column` is persisted on the final page.
    Incremental {
        /// Column whose value is the replication cursor (e.g. `updated_at`).
        column: String,
        /// Lower bound used on the first run, before any bookmark is stored.
        initial_value: Value,
    },
}

/// Configuration for [`MssqlSource`](crate::MssqlSource).
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct MssqlSourceConfig {
    /// Connection + TLS settings (`connection_url` or `connection_string`).
    #[serde(flatten)]
    pub connection: MssqlConnectionConfig,
    /// SQL query to run. Use `@P1`, `@P2`, … for [`params`](Self::params), and
    /// the literal `@bookmark` token to bind the incremental cursor server-side.
    pub query: String,
    /// Positional bind parameters for the query (`@P1`…`@Pn`). Defaults to empty.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Maximum pooled connections. Defaults to 10.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). `0` emits the
    /// whole result set as a single page. Defaults to [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Per-query timeout in seconds (`0` disables). Defaults to 300.
    #[serde(default = "default_statement_timeout_secs")]
    pub statement_timeout_secs: u64,
    /// Replication mode. Defaults to [`MssqlReplication::Full`].
    #[serde(default)]
    pub replication: MssqlReplication,
    /// Explicit state-store key for the bookmark. When unset, a key is derived
    /// from the connection host and a query fingerprint.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_key: Option<String>,
}

impl std::fmt::Debug for MssqlSourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MssqlSourceConfig")
            .field("connection", &"***")
            .field("query", &self.query)
            .field("params", &self.params)
            .field("max_connections", &self.max_connections)
            .field("batch_size", &self.batch_size)
            .field("statement_timeout_secs", &self.statement_timeout_secs)
            .field("replication", &self.replication)
            .field("state_key", &self.state_key)
            .finish()
    }
}

impl MssqlSourceConfig {
    /// Build a config from a connection URL and query, with defaults elsewhere.
    pub fn new(connection_url: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            connection: MssqlConnectionConfig {
                connection_url: Some(connection_url.into()),
                ..Default::default()
            },
            query: query.into(),
            params: Vec::new(),
            max_connections: default_max_connections(),
            batch_size: default_batch_size(),
            statement_timeout_secs: default_statement_timeout_secs(),
            replication: MssqlReplication::Full,
            state_key: None,
        }
    }

    /// Validate connection source, batch size, and replication settings.
    pub fn validate(&self) -> Result<(), FaucetError> {
        self.connection.validate()?;
        validate_batch_size(self.batch_size)?;
        if let MssqlReplication::Incremental { column, .. } = &self.replication
            && column.trim().is_empty()
        {
            return Err(FaucetError::Config(
                "MSSQL incremental replication requires a non-empty `column`".into(),
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
    fn replication_full_is_default_and_round_trips() {
        let r: MssqlReplication = serde_json::from_value(json!({"type": "full"})).unwrap();
        assert_eq!(r, MssqlReplication::Full);
        assert_eq!(MssqlReplication::default(), MssqlReplication::Full);
    }

    #[test]
    fn replication_incremental_parses_column_and_initial_value() {
        let r: MssqlReplication = serde_json::from_value(json!({
            "type": "incremental",
            "column": "updated_at",
            "initial_value": "1970-01-01T00:00:00Z"
        }))
        .unwrap();
        assert_eq!(
            r,
            MssqlReplication::Incremental {
                column: "updated_at".into(),
                initial_value: json!("1970-01-01T00:00:00Z"),
            }
        );
    }

    #[test]
    fn config_flattens_connection_fields() {
        let cfg: MssqlSourceConfig = serde_json::from_value(json!({
            "connection_url": "mssql://sa:pw@host:1433/db",
            "query": "SELECT 1",
        }))
        .unwrap();
        assert_eq!(
            cfg.connection.connection_url.as_deref(),
            Some("mssql://sa:pw@host:1433/db")
        );
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(cfg.max_connections, 10);
        assert_eq!(cfg.statement_timeout_secs, 300);
    }

    #[test]
    fn validate_rejects_incremental_without_column() {
        let cfg = MssqlSourceConfig {
            replication: MssqlReplication::Incremental {
                column: "  ".into(),
                initial_value: json!(0),
            },
            ..MssqlSourceConfig::new("mssql://sa:pw@h/db", "SELECT 1")
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn validate_rejects_bad_batch_size() {
        let cfg = MssqlSourceConfig {
            batch_size: faucet_core::MAX_BATCH_SIZE + 1,
            ..MssqlSourceConfig::new("mssql://sa:pw@h/db", "SELECT 1")
        };
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn debug_masks_connection() {
        let cfg = MssqlSourceConfig::new("mssql://sa:secret@h/db", "SELECT 1");
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("***"));
        assert!(!dbg.contains("secret"));
    }
}
