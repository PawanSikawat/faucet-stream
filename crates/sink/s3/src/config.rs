//! S3 sink configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the S3 sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct S3SinkConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Key prefix for written objects.
    pub prefix: String,
    /// AWS region. `None` uses the SDK default.
    pub region: Option<String>,
    /// Custom endpoint URL for S3-compatible services (e.g. MinIO).
    pub endpoint_url: Option<String>,
    /// File extension for written objects (default: `.jsonl`).
    pub file_extension: String,
    /// Maximum records per file. `None` writes all records to a single file.
    pub max_records_per_file: Option<usize>,
    /// Maximum number of concurrent file uploads (default: 10).
    pub concurrency: usize,
    /// Records per S3 object written by a single
    /// [`Sink::write_batch`](faucet_core::Sink::write_batch) call. When a call
    /// hands the sink `N` records with `batch_size = M > 0`, the sink writes
    /// `ceil(N / M)` objects, each containing at most `M` records (the final
    /// object holds the remainder). Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the sink writes
    /// whatever upstream hands it without re-chunking (still honouring
    /// `max_records_per_file` if set). Recommended for S3 — most callers
    /// should leave this at `0` and let the source's `batch_size` drive
    /// object sizing, because many tiny S3 objects are a well-known
    /// anti-pattern (per-request overhead, slower downstream reads,
    /// LIST/PUT cost).
    ///
    /// When both `batch_size > 0` and `max_records_per_file` are set, the
    /// effective per-object cap is `min(batch_size, max_records_per_file)`.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl S3SinkConfig {
    /// Create a new config with the required bucket name and sensible defaults.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: String::new(),
            region: None,
            endpoint_url: None,
            file_extension: ".jsonl".to_string(),
            max_records_per_file: None,
            concurrency: 10,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the key prefix for written objects.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }

    /// Set the AWS region.
    pub fn region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    /// Set a custom endpoint URL for S3-compatible services.
    pub fn endpoint_url(mut self, url: impl Into<String>) -> Self {
        self.endpoint_url = Some(url.into());
        self
    }

    /// Set the file extension for written objects.
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }

    /// Set the maximum number of records per file.
    pub fn max_records_per_file(mut self, max: usize) -> Self {
        self.max_records_per_file = Some(max);
        self
    }

    /// Set the maximum number of concurrent file uploads.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the per-object record count for
    /// [`Sink::write_batch`](faucet_core::Sink::write_batch).
    ///
    /// Pass `0` to opt out of write-side re-chunking — the sink writes
    /// whatever upstream hands it as a single object (still honouring
    /// `max_records_per_file` if set). `0` is the recommended value for S3
    /// because writing many small objects is an anti-pattern.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = S3SinkConfig::new("my-bucket");
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, "");
        assert!(config.region.is_none());
        assert!(config.endpoint_url.is_none());
        assert_eq!(config.file_extension, ".jsonl");
        assert!(config.max_records_per_file.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = S3SinkConfig::new("my-bucket")
            .prefix("output/")
            .region("eu-west-1")
            .endpoint_url("http://localhost:9000")
            .file_extension(".json")
            .max_records_per_file(1000);

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix, "output/");
        assert_eq!(config.region.as_deref(), Some("eu-west-1"));
        assert_eq!(
            config.endpoint_url.as_deref(),
            Some("http://localhost:9000")
        );
        assert_eq!(config.file_extension, ".json");
        assert_eq!(config.max_records_per_file, Some(1000));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = S3SinkConfig::new("my-bucket");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = S3SinkConfig::new("my-bucket").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = S3SinkConfig::new("my-bucket").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config =
            S3SinkConfig::new("my-bucket").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": "",
            "region": null,
            "endpoint_url": null,
            "file_extension": ".jsonl",
            "max_records_per_file": null,
            "concurrency": 10,
            "batch_size": 250
        }"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[test]
    fn batch_size_defaults_when_omitted_from_json() {
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": "",
            "region": null,
            "endpoint_url": null,
            "file_extension": ".jsonl",
            "max_records_per_file": null,
            "concurrency": 10
        }"#;
        let config: S3SinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
