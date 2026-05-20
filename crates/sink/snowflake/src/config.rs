//! Snowflake sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Authentication method for Snowflake.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
pub enum SnowflakeAuth {
    /// JWT key-pair authentication.
    ///
    /// Uses an RSA private key (PEM-encoded) to generate JWT tokens
    /// for the Snowflake SQL REST API.
    KeyPair {
        /// The Snowflake user account name.
        user: String,
        /// PEM-encoded RSA private key.
        private_key_pem: String,
    },
    /// OAuth2 bearer token (e.g. from an external identity provider).
    OAuth { token: String },
}

impl std::fmt::Debug for SnowflakeAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyPair { user, .. } => f
                .debug_struct("KeyPair")
                .field("user", user)
                .field("private_key_pem", &"***")
                .finish(),
            Self::OAuth { .. } => f.debug_struct("OAuth").field("token", &"***").finish(),
        }
    }
}

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
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
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
        }
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
            "auth": {"type": "OAuth", "token": "tok"},
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
            "auth": {"type": "OAuth", "token": "tok"}
        }"#;
        let config: SnowflakeSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
