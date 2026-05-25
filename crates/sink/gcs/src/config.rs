//! GCS sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use faucet_gcs_common::GcsCredentials;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the GCS sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GcsSinkConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// Object-name prefix for written files.
    pub prefix: String,
    /// Credential source.
    #[serde(default)]
    pub credentials: GcsCredentials,
    /// File extension for written objects (default `.jsonl`).
    #[serde(default = "default_file_extension")]
    pub file_extension: String,
    /// Hard cap on records per uploaded object. `None` means a single
    /// object per `write_batch` call (still subject to `batch_size`).
    pub max_records_per_file: Option<usize>,
    /// Maximum number of concurrent uploads (default 10).
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per uploaded object from a single `write_batch` call.
    /// `batch_size = 0` writes whatever upstream hands the sink as one
    /// object. Recommended value for GCS is `0` — many tiny objects is
    /// a well-known anti-pattern.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional storage-host override (integration-test escape hatch).
    pub storage_host: Option<String>,
}

fn default_file_extension() -> String {
    ".jsonl".to_string()
}
fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_concurrency() -> usize {
    10
}

impl GcsSinkConfig {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: String::new(),
            credentials: GcsCredentials::default(),
            file_extension: default_file_extension(),
            max_records_per_file: None,
            concurrency: default_concurrency(),
            batch_size: default_batch_size(),
            storage_host: None,
        }
    }

    pub fn prefix(mut self, p: impl Into<String>) -> Self {
        self.prefix = p.into();
        self
    }
    pub fn credentials(mut self, c: GcsCredentials) -> Self {
        self.credentials = c;
        self
    }
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }
    pub fn max_records_per_file(mut self, n: usize) -> Self {
        self.max_records_per_file = Some(n);
        self
    }
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
        self
    }
    pub fn with_batch_size(mut self, n: usize) -> Self {
        self.batch_size = n;
        self
    }
    pub fn storage_host(mut self, h: impl Into<String>) -> Self {
        self.storage_host = Some(h.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let c = GcsSinkConfig::new("b");
        assert_eq!(c.bucket, "b");
        assert_eq!(c.prefix, "");
        assert!(matches!(c.credentials, GcsCredentials::ApplicationDefault));
        assert_eq!(c.file_extension, ".jsonl");
        assert!(c.max_records_per_file.is_none());
        assert_eq!(c.concurrency, 10);
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert!(c.storage_host.is_none());
    }

    #[test]
    fn builder_methods() {
        let c = GcsSinkConfig::new("b")
            .prefix("out/")
            .file_extension(".ndjson")
            .max_records_per_file(500)
            .concurrency(4)
            .with_batch_size(0)
            .storage_host("http://localhost:4443");
        assert_eq!(c.prefix, "out/");
        assert_eq!(c.file_extension, ".ndjson");
        assert_eq!(c.max_records_per_file, Some(500));
        assert_eq!(c.concurrency, 4);
        assert_eq!(c.batch_size, 0);
        assert_eq!(c.storage_host.as_deref(), Some("http://localhost:4443"));
    }

    #[test]
    fn batch_size_sentinel_accepted_and_above_max_rejected() {
        assert!(faucet_core::validate_batch_size(0).is_ok());
        assert!(faucet_core::validate_batch_size(faucet_core::MAX_BATCH_SIZE + 1).is_err());
    }

    #[test]
    fn batch_size_defaults_when_omitted_from_json() {
        let json = r#"{
            "bucket": "b",
            "prefix": "p/",
            "max_records_per_file": null,
            "concurrency": 10,
            "storage_host": null
        }"#;
        let c: GcsSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(c.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
