//! Snowflake sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;

// Re-export the shared auth type so end-user imports remain stable
// (`use faucet_sink_snowflake::SnowflakeAuth;` keeps working).
pub use faucet_snowflake_common::SnowflakeAuth;

/// Configuration for the Snowflake sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SnowflakeSinkConfig {
    /// Snowflake account identifier (e.g. `"xy12345.us-east-1"`).
    pub account: String,
    /// Warehouse to use for the session.
    pub warehouse: String,
    /// Database name.
    pub database: String,
    /// Schema name.
    pub schema: String,
    /// Target table name.
    pub table: String,
    /// Authentication credentials.
    pub auth: SnowflakeAuth,
    /// Maximum number of records sent per Snowflake SQL REST API request.
    /// Defaults to [`DEFAULT_BATCH_SIZE`] (1000), which matches the
    /// documented sweet spot for the SQL REST API.
    ///
    /// When `write_batch` is handed a slice larger than `batch_size`, the
    /// sink re-chunks it into `batch_size` slices and issues one INSERT per
    /// chunk. `batch_size = 0` is the **"no batching" sentinel** — the
    /// records slice is forwarded as a single INSERT, no matter how large,
    /// so upstream `StreamPage` framing flows through untouched.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum wall-clock time to wait for an asynchronously-executed
    /// INSERT to finish. Snowflake's SQL REST API answers an accepted but
    /// not-yet-finished statement with HTTP 202 and a `statementHandle`;
    /// the sink polls `GET /api/v2/statements/{handle}` until the statement
    /// reports success before counting the rows as written. Without this
    /// the sink would report success the moment Snowflake *accepted* the
    /// statement, losing durability. Defaults to 300 seconds. Set to `0`
    /// to poll indefinitely.
    #[serde(
        default = "default_poll_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub poll_timeout: Duration,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_poll_timeout() -> Duration {
    Duration::from_secs(300)
}

impl SnowflakeSinkConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(
        account: impl Into<String>,
        warehouse: impl Into<String>,
        database: impl Into<String>,
        schema: impl Into<String>,
        table: impl Into<String>,
        auth: SnowflakeAuth,
    ) -> Self {
        Self {
            account: account.into(),
            warehouse: warehouse.into(),
            database: database.into(),
            schema: schema.into(),
            table: table.into(),
            auth,
            batch_size: DEFAULT_BATCH_SIZE,
            poll_timeout: default_poll_timeout(),
        }
    }

    /// Set the maximum wall-clock time spent polling an asynchronously
    /// executed INSERT for completion. Pass `Duration::ZERO` to poll forever.
    pub fn with_poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Set the maximum number of records per Snowflake SQL REST API request.
    ///
    /// Pass `0` to opt out of re-chunking — the entire records slice handed
    /// to `write_batch` is sent in a single INSERT request, preserving
    /// upstream `StreamPage` framing.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_auth() -> SnowflakeAuth {
        SnowflakeAuth::OAuth {
            token: "tok".into(),
        }
    }

    fn sample_config() -> SnowflakeSinkConfig {
        SnowflakeSinkConfig::new(
            "xy12345",
            "COMPUTE_WH",
            "MY_DB",
            "PUBLIC",
            "events",
            sample_auth(),
        )
    }

    #[test]
    fn default_config() {
        let config = sample_config();
        assert_eq!(config.account, "xy12345");
        assert_eq!(config.warehouse, "COMPUTE_WH");
        assert_eq!(config.database, "MY_DB");
        assert_eq!(config.schema, "PUBLIC");
        assert_eq!(config.table, "events");
        assert_eq!(config.poll_timeout, Duration::from_secs(300));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = sample_config();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = sample_config().with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = sample_config().with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config = sample_config().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "account": "xy12345",
            "warehouse": "COMPUTE_WH",
            "database": "MY_DB",
            "schema": "PUBLIC",
            "table": "events",
            "auth": {"type": "oauth", "config": {"token": "tok"}},
            "batch_size": 250
        }"#;
        let config: SnowflakeSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_from_json() {
        let json = r#"{
            "account": "xy12345",
            "warehouse": "COMPUTE_WH",
            "database": "MY_DB",
            "schema": "PUBLIC",
            "table": "events",
            "auth": {"type": "oauth", "config": {"token": "tok"}}
        }"#;
        let config: SnowflakeSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
