//! Snowflake sink configuration.

use serde::{Deserialize, Serialize};

/// Authentication method for Snowflake.
#[derive(Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    /// Maximum number of rows per INSERT statement. Defaults to 500.
    pub batch_size: usize,
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
            batch_size: 500,
        }
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
        let config = SnowflakeSinkConfig::new(
            "xy12345",
            "COMPUTE_WH",
            "MY_DB",
            "PUBLIC",
            "events",
            SnowflakeAuth::OAuth {
                token: "tok".into(),
            },
        );
        assert_eq!(config.account, "xy12345");
        assert_eq!(config.warehouse, "COMPUTE_WH");
        assert_eq!(config.database, "MY_DB");
        assert_eq!(config.schema, "PUBLIC");
        assert_eq!(config.table, "events");
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_builder() {
        let config = SnowflakeSinkConfig::new(
            "acct",
            "wh",
            "db",
            "schema",
            "tbl",
            SnowflakeAuth::OAuth { token: "t".into() },
        )
        .batch_size(100);
        assert_eq!(config.batch_size, 100);
    }
}
