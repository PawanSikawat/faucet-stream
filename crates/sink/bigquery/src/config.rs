//! BigQuery sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// How to authenticate with Google BigQuery.
#[derive(Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", content = "value")]
pub enum BigQueryCredentials {
    /// Path to a service account JSON key file.
    ServiceAccountKeyPath(String),
    /// Inline service account JSON key content.
    ServiceAccountKey(String),
    /// Use application default credentials (e.g. workload identity, `gcloud auth`).
    ApplicationDefault,
}

impl std::fmt::Debug for BigQueryCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ServiceAccountKeyPath(path) => {
                f.debug_tuple("ServiceAccountKeyPath").field(path).finish()
            }
            Self::ServiceAccountKey(_) => write!(f, "ServiceAccountKey(***)"),
            Self::ApplicationDefault => write!(f, "ApplicationDefault"),
        }
    }
}

/// Configuration for the BigQuery streaming insert sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BigQuerySinkConfig {
    /// GCP project ID.
    pub project_id: String,
    /// BigQuery dataset ID.
    pub dataset_id: String,
    /// BigQuery table ID.
    pub table_id: String,
    /// Authentication credentials.
    pub credentials: BigQueryCredentials,
    /// Maximum rows per `tabledata.insertAll` request. Defaults to
    /// [`DEFAULT_BATCH_SIZE`].
    ///
    /// When the upstream `StreamPage` carries more records than `batch_size`,
    /// the sink slices the page into `batch_size`-row chunks and issues one
    /// `insertAll` HTTP call per chunk. When `batch_size = 0`, the page is
    /// sent as a single request — useful when the source already chunks to
    /// BigQuery's preferred size (e.g. ~500 rows for streaming inserts).
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the entire upstream
    /// page is forwarded in one `insertAll` call, subject to BigQuery's
    /// natural per-request limits (~10MB body, ~500 rows recommended).
    /// Larger pages may exceed those limits — keep the default unless the
    /// upstream `StreamPage` size is already tuned for BigQuery.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl BigQuerySinkConfig {
    /// Create a new config with the required fields and sensible defaults.
    pub fn new(
        project_id: impl Into<String>,
        dataset_id: impl Into<String>,
        table_id: impl Into<String>,
        credentials: BigQueryCredentials,
    ) -> Self {
        Self {
            project_id: project_id.into(),
            dataset_id: dataset_id.into(),
            table_id: table_id.into(),
            credentials,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the per-request row count for `tabledata.insertAll`.
    ///
    /// Pass `0` to opt out of re-chunking — the sink forwards each upstream
    /// [`StreamPage`](faucet_core::StreamPage) as a single `insertAll` call.
    /// BigQuery's streaming-insert sweet spot is ~500 rows per request.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = BigQuerySinkConfig::new(
            "my-project",
            "my_dataset",
            "my_table",
            BigQueryCredentials::ApplicationDefault,
        );
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config =
            BigQuerySinkConfig::new("proj", "ds", "tbl", BigQueryCredentials::ApplicationDefault)
                .with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn config_stores_all_fields() {
        let config = BigQuerySinkConfig::new(
            "my-project",
            "my_dataset",
            "my_table",
            BigQueryCredentials::ServiceAccountKeyPath("/path/to/key.json".into()),
        );
        assert_eq!(config.project_id, "my-project");
        assert_eq!(config.dataset_id, "my_dataset");
        assert_eq!(config.table_id, "my_table");
        assert!(matches!(
            config.credentials,
            BigQueryCredentials::ServiceAccountKeyPath(_)
        ));
    }

    #[test]
    fn config_with_inline_key() {
        let config = BigQuerySinkConfig::new(
            "proj",
            "ds",
            "tbl",
            BigQueryCredentials::ServiceAccountKey(r#"{"type":"service_account"}"#.into()),
        );
        if let BigQueryCredentials::ServiceAccountKey(json) = &config.credentials {
            assert!(json.contains("service_account"));
        } else {
            panic!("expected ServiceAccountKey");
        }
    }

    #[test]
    fn config_builder_chaining() {
        let config =
            BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault)
                .with_batch_size(100)
                .with_batch_size(250);
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn credentials_debug_masks_secrets() {
        let creds = BigQueryCredentials::ApplicationDefault;
        assert_eq!(format!("{creds:?}"), "ApplicationDefault");

        let creds = BigQueryCredentials::ServiceAccountKey("secret-json".into());
        let debug = format!("{creds:?}");
        assert!(debug.contains("***"));
        assert!(!debug.contains("secret-json"));

        let creds = BigQueryCredentials::ServiceAccountKeyPath("/path/to/key.json".into());
        let debug = format!("{creds:?}");
        assert!(debug.contains("/path/to/key.json"));
    }

    #[test]
    fn config_clone() {
        let config =
            BigQuerySinkConfig::new("proj", "ds", "tbl", BigQueryCredentials::ApplicationDefault)
                .with_batch_size(42);
        let cloned = config.clone();
        assert_eq!(cloned.project_id, "proj");
        assert_eq!(cloned.batch_size, 42);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config =
            BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault)
                .with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config =
            BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault)
                .with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "project_id": "p",
            "dataset_id": "d",
            "table_id": "t",
            "credentials": {"type": "ApplicationDefault"},
            "batch_size": 250
        }"#;
        let config: BigQuerySinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_absent_in_json() {
        let json = r#"{
            "project_id": "p",
            "dataset_id": "d",
            "table_id": "t",
            "credentials": {"type": "ApplicationDefault"}
        }"#;
        let config: BigQuerySinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
