//! GCS source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use faucet_gcs_common::GcsCredentials;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Format of files stored in GCS.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GcsFileFormat {
    /// Each line in the file is a separate JSON record.
    #[default]
    JsonLines,
    /// The entire file is a JSON array of records.
    JsonArray,
    /// Each file becomes a single record with `"key"` and `"content"` fields.
    RawText,
}

/// Configuration for the GCS source connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct GcsSourceConfig {
    /// GCS bucket name.
    pub bucket: String,
    /// Object name prefix filter. Ignored when `object_keys` is set.
    pub prefix: Option<String>,
    /// Explicit object names. When set, listing is skipped and `prefix`
    /// is ignored.
    pub object_keys: Option<Vec<String>>,
    /// Credential source.
    #[serde(default)]
    pub credentials: GcsCredentials,
    /// File format.
    #[serde(default)]
    pub file_format: GcsFileFormat,
    /// Hard cap on the number of objects read (after listing).
    pub max_objects: Option<usize>,
    /// Maximum concurrent object reads (default: 10).
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Records per emitted `StreamPage`. See "Streaming and batching"
    /// in the README. `batch_size = 0` is the "no batching" sentinel and
    /// emits one page per object.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Optional storage-host override (e.g. `http://localhost:4443` for
    /// fake-gcs-server). Production users should leave this unset.
    pub storage_host: Option<String>,
    /// Compression codec applied to each downloaded object. Defaults to
    /// [`CompressionConfig::Auto`](faucet_core::CompressionConfig::Auto) —
    /// the codec is resolved per-object-key, so a single source can read a
    /// mix of compressed and uncompressed objects. Requires the
    /// crate-local `compression` feature.
    #[cfg(feature = "compression")]
    #[serde(default)]
    pub compression: faucet_core::CompressionConfig,
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}
fn default_concurrency() -> usize {
    10
}

impl GcsSourceConfig {
    /// Create a new config with the required bucket name and sensible defaults.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: None,
            object_keys: None,
            credentials: GcsCredentials::default(),
            file_format: GcsFileFormat::default(),
            max_objects: None,
            concurrency: default_concurrency(),
            batch_size: default_batch_size(),
            storage_host: None,
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::default(),
        }
    }

    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    pub fn object_keys(mut self, keys: Vec<String>) -> Self {
        self.object_keys = Some(keys);
        self
    }

    pub fn credentials(mut self, creds: GcsCredentials) -> Self {
        self.credentials = creds;
        self
    }

    pub fn file_format(mut self, format: GcsFileFormat) -> Self {
        self.file_format = format;
        self
    }

    pub fn max_objects(mut self, max: usize) -> Self {
        self.max_objects = Some(max);
        self
    }

    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    pub fn storage_host(mut self, host: impl Into<String>) -> Self {
        self.storage_host = Some(host.into());
        self
    }

    /// Set the compression codec. Available only with the `compression` feature.
    #[cfg(feature = "compression")]
    pub fn compression(mut self, c: faucet_core::CompressionConfig) -> Self {
        self.compression = c;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = GcsSourceConfig::new("my-bucket");
        assert_eq!(config.bucket, "my-bucket");
        assert!(config.prefix.is_none());
        assert!(config.object_keys.is_none());
        assert!(matches!(
            config.credentials,
            GcsCredentials::ApplicationDefault
        ));
        assert!(matches!(config.file_format, GcsFileFormat::JsonLines));
        assert!(config.max_objects.is_none());
        assert_eq!(config.concurrency, 10);
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
        assert!(config.storage_host.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = GcsSourceConfig::new("my-bucket")
            .prefix("data/")
            .file_format(GcsFileFormat::JsonArray)
            .max_objects(5)
            .concurrency(20)
            .with_batch_size(250)
            .storage_host("http://localhost:4443");

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix.as_deref(), Some("data/"));
        assert!(matches!(config.file_format, GcsFileFormat::JsonArray));
        assert_eq!(config.max_objects, Some(5));
        assert_eq!(config.concurrency, 20);
        assert_eq!(config.batch_size, 250);
        assert_eq!(
            config.storage_host.as_deref(),
            Some("http://localhost:4443")
        );
    }

    #[test]
    fn file_format_default_is_json_lines() {
        assert!(matches!(GcsFileFormat::default(), GcsFileFormat::JsonLines));
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = GcsSourceConfig::new("b").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected() {
        let config = GcsSourceConfig::new("b").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = GcsSourceConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }

    #[test]
    fn batch_size_defaults_when_omitted_from_json() {
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": null,
            "object_keys": null,
            "file_format": "json_lines",
            "max_objects": null,
            "concurrency": 10,
            "storage_host": null
        }"#;
        let config: GcsSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }
}
