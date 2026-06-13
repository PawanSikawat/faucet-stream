//! BigQuery sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// Re-export the shared credentials type so end-user imports remain stable
// (`use faucet_sink_bigquery::BigQueryCredentials;` keeps working).
pub use faucet_common_bigquery::BigQueryCredentials;

/// Configuration for the BigQuery streaming insert sink.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct BigQuerySinkConfig {
    /// GCP project ID.
    pub project_id: String,
    /// BigQuery dataset ID.
    pub dataset_id: String,
    /// BigQuery table ID.
    pub table_id: String,
    /// Authentication credentials. YAML/JSON key is `auth` for consistency with
    /// every other connector's auth block.
    pub auth: BigQueryCredentials,
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
    /// Optional record field whose value is sent as the BigQuery streaming
    /// `insertId` for each row. BigQuery uses `insertId` for best-effort
    /// de-duplication over a short window, so a stable per-row key here makes
    /// streaming inserts resilient to transport retries (which are otherwise
    /// at-least-once and can produce duplicate rows) (#78/#31). When `None`
    /// (the default) no `insertId` is sent. A row missing the field is
    /// inserted without an `insertId` (no dedup for that row).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insert_id_field: Option<String>,
    /// Write mode (append / upsert / delete) plus the `key` columns and optional
    /// `delete_marker`. Flattened, so `write_mode` / `key` / `delete_marker`
    /// appear at the config top level. Defaults to append (every existing
    /// config keeps working). Upsert/delete merge by `key` in place via a
    /// BigQuery `MERGE` over the page (no staging table); `key` must be real
    /// column(s) of the target table.
    #[serde(flatten)]
    pub write: faucet_core::WriteSpec,
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
            auth: credentials,
            batch_size: DEFAULT_BATCH_SIZE,
            insert_id_field: None,
            write: faucet_core::WriteSpec::default(),
        }
    }

    /// Set the record field used as the per-row BigQuery streaming `insertId`
    /// for best-effort de-duplication on retry.
    pub fn with_insert_id_field(mut self, field: impl Into<String>) -> Self {
        self.insert_id_field = Some(field.into());
        self
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
            BigQueryCredentials::ServiceAccountKeyPath {
                path: "/path/to/key.json".into(),
            },
        );
        assert_eq!(config.project_id, "my-project");
        assert_eq!(config.dataset_id, "my_dataset");
        assert_eq!(config.table_id, "my_table");
        assert!(matches!(
            config.auth,
            BigQueryCredentials::ServiceAccountKeyPath { .. }
        ));
    }

    #[test]
    fn config_with_inline_key() {
        let config = BigQuerySinkConfig::new(
            "proj",
            "ds",
            "tbl",
            BigQueryCredentials::ServiceAccountKey {
                json: r#"{"type":"service_account"}"#.into(),
            },
        );
        if let BigQueryCredentials::ServiceAccountKey { json } = &config.auth {
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
    fn insert_id_field_defaults_none_and_builder_sets_it() {
        let config =
            BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault);
        assert!(config.insert_id_field.is_none());
        let config = config.with_insert_id_field("event_id");
        assert_eq!(config.insert_id_field.as_deref(), Some("event_id"));
    }

    #[test]
    fn insert_id_field_deserializes_from_json() {
        let json = r#"{
            "project_id": "p",
            "dataset_id": "d",
            "table_id": "t",
            "auth": {"type": "application_default"},
            "insert_id_field": "id"
        }"#;
        let config: BigQuerySinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.insert_id_field.as_deref(), Some("id"));
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "project_id": "p",
            "dataset_id": "d",
            "table_id": "t",
            "auth": {"type": "application_default"},
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
            "auth": {"type": "application_default"}
        }"#;
        let config: BigQuerySinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn write_mode_defaults_to_append() {
        let config =
            BigQuerySinkConfig::new("p", "d", "t", BigQueryCredentials::ApplicationDefault);
        assert_eq!(config.write.write_mode, faucet_core::WriteMode::Append);
        assert!(config.write.key.is_empty());
    }

    #[test]
    fn write_spec_deserializes_flattened() {
        let json = r#"{
            "project_id": "p",
            "dataset_id": "d",
            "table_id": "t",
            "auth": {"type": "application_default"},
            "write_mode": "upsert",
            "key": ["id"],
            "delete_marker": {"field": "__op", "values": ["d"]}
        }"#;
        let config: BigQuerySinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.write.write_mode, faucet_core::WriteMode::Upsert);
        assert_eq!(config.write.key, vec!["id".to_string()]);
        let dm = config.write.delete_marker.expect("delete_marker");
        assert_eq!(dm.field, "__op");
        assert_eq!(dm.values, vec!["d".to_string()]);
    }
}
