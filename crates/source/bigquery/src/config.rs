//! BigQuery source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

// Re-export the shared credentials type so end-user imports remain stable.
pub use faucet_common_bigquery::BigQueryCredentials;

fn default_use_legacy_sql() -> bool {
    false
}

fn default_max_results_per_page() -> i32 {
    1000
}

fn default_statement_timeout() -> Duration {
    Duration::from_secs(60)
}

fn default_poll_timeout() -> Duration {
    Duration::from_secs(300)
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

/// Configuration for the BigQuery query source.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
pub struct BigQuerySourceConfig {
    /// GCP project ID against which the query is billed and run.
    pub project_id: String,
    /// Authentication — the `auth` field, consistent with every other connector.
    pub auth: BigQueryCredentials,
    /// SQL statement to execute. May contain `${field.path}` placeholders that
    /// are resolved against the parent-record context at runtime as
    /// positional `?` markers; matched values are appended to
    /// [`params`](Self::params) when the query is sent.
    pub query: String,
    /// Whether to use BigQuery's legacy SQL dialect. Defaults to `false`
    /// (Standard SQL). Set to `true` only for tables that use legacy
    /// `[project:dataset.table]` references.
    #[serde(default = "default_use_legacy_sql")]
    pub use_legacy_sql: bool,
    /// Optional location override for non-`US` jobs (`"EU"`, `"asia-east1"`).
    /// When `None`, BigQuery uses the default location for the queried tables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    /// Maximum rows per page when calling `jobs.getQueryResults`. Smaller
    /// values trade more HTTP round-trips for lower memory; larger values
    /// trade memory for fewer requests. Defaults to 1000.
    #[serde(default = "default_max_results_per_page")]
    pub max_results_per_page: i32,
    /// Positional bind parameters for the query, sent as
    /// [`POSITIONAL`](https://cloud.google.com/bigquery/docs/parameterized-queries)
    /// query parameters in declaration order before any context-derived
    /// values. Each value is shipped as a STRING parameter; BigQuery casts
    /// as needed at execution time.
    #[serde(default)]
    pub params: Vec<Value>,
    /// Per-statement server-side timeout. Forwarded to the
    /// `timeoutMs` field on `jobs.query`. Defaults to 60 seconds. If
    /// BigQuery does not finish the query within this window it responds
    /// with `jobComplete=false`; the source then polls
    /// `jobs.getQueryResults` until the job completes.
    #[serde(
        default = "default_statement_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub statement_timeout: Duration,
    /// Maximum wall-clock time the source will spend polling
    /// `jobs.getQueryResults` for a job that keeps reporting
    /// `jobComplete=false`, before giving up with
    /// [`FaucetError::Source`](faucet_core::FaucetError::Source).
    /// Without this cap a job that never completes would loop forever.
    /// Defaults to 300 seconds. Set to `0` to disable the cap and poll
    /// indefinitely. Only the *completion* wait is bounded; once the job is
    /// complete, ordinary `pageToken` paging is unaffected.
    #[serde(
        default = "default_poll_timeout",
        with = "faucet_core::config::duration_secs"
    )]
    #[schemars(with = "u64")]
    pub poll_timeout: Duration,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Rows
    /// returned by BigQuery are re-framed into pages of this size — every
    /// time the buffer reaches `batch_size`, a page is yielded. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the **"no batching" sentinel**: the entire
    /// result set is buffered and emitted in a single page. Useful for
    /// small lookup tables, or for sinks that prefer one large request to
    /// many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

impl std::fmt::Debug for BigQuerySourceConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQuerySourceConfig")
            .field("project_id", &self.project_id)
            .field("auth", &self.auth)
            .field("query", &self.query)
            .field("use_legacy_sql", &self.use_legacy_sql)
            .field("location", &self.location)
            .field("max_results_per_page", &self.max_results_per_page)
            .field("params", &self.params)
            .field("statement_timeout", &self.statement_timeout)
            .field("poll_timeout", &self.poll_timeout)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl BigQuerySourceConfig {
    /// Create a new config with required fields and sensible defaults.
    pub fn new(
        project_id: impl Into<String>,
        credentials: BigQueryCredentials,
        query: impl Into<String>,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            auth: credentials,
            query: query.into(),
            use_legacy_sql: default_use_legacy_sql(),
            location: None,
            max_results_per_page: default_max_results_per_page(),
            params: Vec::new(),
            statement_timeout: default_statement_timeout(),
            poll_timeout: default_poll_timeout(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Enable BigQuery's legacy SQL dialect.
    pub fn with_use_legacy_sql(mut self, use_legacy: bool) -> Self {
        self.use_legacy_sql = use_legacy;
        self
    }

    /// Pin the job to a specific location (e.g. `"EU"`).
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Set the maximum row count per `getQueryResults` page.
    pub fn with_max_results_per_page(mut self, max_results: i32) -> Self {
        self.max_results_per_page = max_results;
        self
    }

    /// Set the positional bind parameters for the query.
    pub fn with_params(mut self, params: Vec<Value>) -> Self {
        self.params = params;
        self
    }

    /// Set the per-statement server-side timeout.
    pub fn with_statement_timeout(mut self, timeout: Duration) -> Self {
        self.statement_timeout = timeout;
        self
    }

    /// Set the maximum wall-clock time spent polling for job completion
    /// before giving up. Pass `Duration::ZERO` to poll forever.
    pub fn with_poll_timeout(mut self, timeout: Duration) -> Self {
        self.poll_timeout = timeout;
        self
    }

    /// Set the records-per-page hint for [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire result set is emitted
    /// in a single page.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> BigQuerySourceConfig {
        BigQuerySourceConfig::new(
            "my-project",
            BigQueryCredentials::ApplicationDefault,
            "SELECT id FROM events",
        )
    }

    #[test]
    fn default_config() {
        let c = sample();
        assert_eq!(c.project_id, "my-project");
        assert!(!c.use_legacy_sql);
        assert!(c.location.is_none());
        assert_eq!(c.max_results_per_page, 1000);
        assert!(c.params.is_empty());
        assert_eq!(c.statement_timeout, Duration::from_secs(60));
        assert_eq!(c.poll_timeout, Duration::from_secs(300));
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn builder_chaining() {
        let c = sample()
            .with_use_legacy_sql(true)
            .with_location("EU")
            .with_max_results_per_page(500)
            .with_params(vec![json!("us-east")])
            .with_statement_timeout(Duration::from_secs(30))
            .with_batch_size(250);
        assert!(c.use_legacy_sql);
        assert_eq!(c.location.as_deref(), Some("EU"));
        assert_eq!(c.max_results_per_page, 500);
        assert_eq!(c.params, vec![json!("us-east")]);
        assert_eq!(c.statement_timeout, Duration::from_secs(30));
        assert_eq!(c.batch_size, 250);
    }

    #[test]
    fn deserializes_minimal_json() {
        let json = r#"{
            "project_id": "my-project",
            "auth": {"type": "application_default"},
            "query": "SELECT 1"
        }"#;
        let c: BigQuerySourceConfig = serde_json::from_str(json).unwrap();
        assert!(!c.use_legacy_sql);
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert_eq!(c.statement_timeout, Duration::from_secs(60));
        assert_eq!(c.max_results_per_page, 1000);
    }

    #[test]
    fn deserializes_all_fields() {
        let json = r#"{
            "project_id": "p",
            "auth": {"type": "application_default"},
            "query": "SELECT 1",
            "use_legacy_sql": true,
            "location": "EU",
            "max_results_per_page": 500,
            "params": ["us-east"],
            "statement_timeout": 30,
            "batch_size": 250
        }"#;
        let c: BigQuerySourceConfig = serde_json::from_str(json).unwrap();
        assert!(c.use_legacy_sql);
        assert_eq!(c.location.as_deref(), Some("EU"));
        assert_eq!(c.max_results_per_page, 500);
        assert_eq!(c.statement_timeout, Duration::from_secs(30));
        assert_eq!(c.batch_size, 250);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let c = sample().with_batch_size(0);
        assert!(faucet_core::validate_batch_size(c.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let c = sample().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(c.batch_size).is_err());
    }

    #[test]
    fn debug_masks_inline_credentials() {
        let c = BigQuerySourceConfig::new(
            "p",
            BigQueryCredentials::ServiceAccountKey {
                json: "secret".into(),
            },
            "SELECT 1",
        );
        let dbg = format!("{c:?}");
        assert!(!dbg.contains("secret"));
        assert!(dbg.contains("***"));
    }
}
