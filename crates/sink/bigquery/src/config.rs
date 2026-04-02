//! BigQuery sink configuration.

/// How to authenticate with Google BigQuery.
#[derive(Clone)]
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
#[derive(Debug, Clone)]
pub struct BigQuerySinkConfig {
    /// GCP project ID.
    pub project_id: String,
    /// BigQuery dataset ID.
    pub dataset_id: String,
    /// BigQuery table ID.
    pub table_id: String,
    /// Authentication credentials.
    pub credentials: BigQueryCredentials,
    /// Maximum number of rows per `insertAll` request. Defaults to 500.
    pub batch_size: usize,
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
            batch_size: 500,
        }
    }

    /// Set the maximum batch size for streaming inserts.
    pub fn batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_batch_size_is_500() {
        let config = BigQuerySinkConfig::new(
            "my-project",
            "my_dataset",
            "my_table",
            BigQueryCredentials::ApplicationDefault,
        );
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_builder() {
        let config =
            BigQuerySinkConfig::new("proj", "ds", "tbl", BigQueryCredentials::ApplicationDefault)
                .batch_size(1000);
        assert_eq!(config.batch_size, 1000);
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
                .batch_size(100)
                .batch_size(250);
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
                .batch_size(42);
        let cloned = config.clone();
        assert_eq!(cloned.project_id, "proj");
        assert_eq!(cloned.batch_size, 42);
    }
}
