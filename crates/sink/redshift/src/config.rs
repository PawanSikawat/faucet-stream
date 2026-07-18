//! Amazon Redshift sink configuration.

use faucet_common_redshift::RedshiftConnection;
use faucet_core::{DEFAULT_BATCH_SIZE, FaucetError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How the sink loads rows into Redshift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RedshiftWriteStrategy {
    /// Stage each page to S3 and bulk-load it with `COPY … FROM 's3://…'` — the
    /// default and by far the fastest path for Redshift (the recommended way to
    /// load data). Requires `staging_bucket` and `iam_role`.
    #[default]
    Copy,
    /// Multi-row `INSERT INTO … VALUES (…), (…)`. Portable and needs no S3, but
    /// much slower than `COPY` for anything beyond small batches. Redshift does
    /// not recommend row-by-row inserts for bulk data.
    Insert,
}

impl RedshiftWriteStrategy {
    /// Lower-case wire name, for error messages.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Copy => "copy",
            Self::Insert => "insert",
        }
    }
}

/// Format of the staged file that `COPY` reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RedshiftCopyFormat {
    /// Newline-delimited JSON objects, loaded with `FORMAT AS JSON 'auto'`.
    /// Maps by column **name** (order-independent) and handles NULLs and typed
    /// columns cleanly — the default.
    #[default]
    Jsonl,
    /// RFC-4180 CSV, loaded with `FORMAT AS CSV`. Column order is taken from the
    /// destination table's schema and passed explicitly in the `COPY` column
    /// list.
    Csv,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

fn default_max_connections() -> u32 {
    5
}

/// Configuration for the Amazon Redshift sink.
#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub struct RedshiftSinkConfig {
    /// Connection block (host / port / database / user / credentials / tls),
    /// flattened to the config top level.
    #[serde(flatten)]
    pub connection: RedshiftConnection,
    /// Target table name.
    pub table_name: String,
    /// Optional schema (namespace) qualifying [`table_name`](Self::table_name).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<String>,
    /// How rows are loaded. Defaults to [`RedshiftWriteStrategy::Copy`].
    #[serde(default)]
    pub write_strategy: RedshiftWriteStrategy,
    /// Staged-file format for the `COPY` path. Defaults to
    /// [`RedshiftCopyFormat::Jsonl`]. Ignored by the `insert` strategy.
    #[serde(default)]
    pub copy_format: RedshiftCopyFormat,
    /// S3 bucket used to stage `COPY` files. **Required** when
    /// `write_strategy: copy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub staging_bucket: Option<String>,
    /// Key prefix for staged objects (e.g. `redshift-staging/`). Defaults to
    /// empty.
    #[serde(default)]
    pub staging_prefix: String,
    /// IAM role ARN Redshift assumes to read the staged file
    /// (`COPY … IAM_ROLE '<arn>'`). **Required** when `write_strategy: copy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iam_role: Option<String>,
    /// AWS region of the staging bucket (used for both the S3 client and the
    /// `COPY … REGION '<region>'` clause). `None` uses the SDK default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Custom endpoint URL for S3-compatible services (e.g. MinIO) — testing
    /// aid; production loads use real S3.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_url: Option<String>,
    /// Rows per load unit. For `insert`, the per-statement multi-row chunk size;
    /// for `copy`, the number of rows per staged S3 object. Defaults to
    /// [`DEFAULT_BATCH_SIZE`]. `0` = one unit for the whole page.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Maximum number of connections in the pool. Defaults to 5.
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
}

impl RedshiftSinkConfig {
    /// Validate the config. Enforces that the `copy` strategy has a staging
    /// bucket and IAM role.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.table_name.trim().is_empty() {
            return Err(FaucetError::Config(
                "redshift sink: `table_name` must not be empty".into(),
            ));
        }
        if self.write_strategy == RedshiftWriteStrategy::Copy {
            let bucket_ok = self
                .staging_bucket
                .as_ref()
                .is_some_and(|b| !b.trim().is_empty());
            if !bucket_ok {
                return Err(FaucetError::Config(
                    "redshift sink: write_strategy: copy requires a non-empty `staging_bucket`"
                        .into(),
                ));
            }
            let role_ok = self.iam_role.as_ref().is_some_and(|r| !r.trim().is_empty());
            if !role_ok {
                return Err(FaucetError::Config(
                    "redshift sink: write_strategy: copy requires a non-empty `iam_role`".into(),
                ));
            }
        }
        faucet_core::validate_batch_size(self.batch_size)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_redshift::RedshiftConnection;

    fn base() -> RedshiftSinkConfig {
        RedshiftSinkConfig {
            connection: RedshiftConnection::new("host", "db", "user", "pw"),
            table_name: "events".into(),
            schema: None,
            write_strategy: RedshiftWriteStrategy::Copy,
            copy_format: RedshiftCopyFormat::Jsonl,
            staging_bucket: Some("stage".into()),
            staging_prefix: String::new(),
            iam_role: Some("arn:aws:iam::123:role/redshift".into()),
            region: None,
            endpoint_url: None,
            batch_size: DEFAULT_BATCH_SIZE,
            max_connections: default_max_connections(),
        }
    }

    #[test]
    fn valid_copy_config_passes() {
        base().validate().unwrap();
    }

    #[test]
    fn valid_insert_config_needs_no_bucket() {
        let mut c = base();
        c.write_strategy = RedshiftWriteStrategy::Insert;
        c.staging_bucket = None;
        c.iam_role = None;
        c.validate().unwrap();
    }

    #[test]
    fn copy_requires_bucket() {
        let mut c = base();
        c.staging_bucket = None;
        match c.validate() {
            Err(FaucetError::Config(m)) => assert!(m.contains("staging_bucket"), "got: {m}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn copy_requires_iam_role() {
        let mut c = base();
        c.iam_role = Some("  ".into());
        match c.validate() {
            Err(FaucetError::Config(m)) => assert!(m.contains("iam_role"), "got: {m}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_empty_table_name() {
        let mut c = base();
        c.table_name = " ".into();
        assert!(c.validate().is_err());
    }

    #[test]
    fn rejects_oversized_batch() {
        let mut c = base();
        c.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        assert!(c.validate().is_err());
    }

    #[test]
    fn defaults_copy_and_jsonl() {
        let json = r#"{
            "host": "h", "database": "db", "user": "u",
            "credentials": {"type": "password", "config": {"password": "pw"}},
            "table_name": "t",
            "staging_bucket": "b",
            "iam_role": "arn:x"
        }"#;
        let c: RedshiftSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.write_strategy, RedshiftWriteStrategy::Copy);
        assert_eq!(c.copy_format, RedshiftCopyFormat::Jsonl);
        assert_eq!(c.max_connections, 5);
        assert_eq!(c.batch_size, DEFAULT_BATCH_SIZE);
        c.validate().unwrap();
    }

    #[test]
    fn write_strategy_round_trips() {
        assert_eq!(RedshiftWriteStrategy::Copy.as_str(), "copy");
        assert_eq!(RedshiftWriteStrategy::Insert.as_str(), "insert");
    }
}
