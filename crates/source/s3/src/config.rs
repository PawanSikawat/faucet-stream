//! S3 source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Format of files stored in S3.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum S3FileFormat {
    /// Each line in the file is a separate JSON record.
    #[default]
    JsonLines,
    /// The entire file is a JSON array of records.
    JsonArray,
    /// Each file becomes a single record with `"key"` and `"content"` fields.
    RawText,
}

/// Configuration for the S3 source connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct S3SourceConfig {
    /// S3 bucket name.
    pub bucket: String,
    /// Object key prefix filter.
    pub prefix: Option<String>,
    /// AWS region. `None` uses the SDK default.
    pub region: Option<String>,
    /// Custom endpoint URL for S3-compatible services (e.g. MinIO).
    pub endpoint_url: Option<String>,
    /// Format of the files to read.
    pub file_format: S3FileFormat,
    /// Maximum number of objects to read.
    pub max_objects: Option<usize>,
    /// Maximum number of concurrent object reads (default: 10).
    pub concurrency: usize,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). For
    /// `JsonLines` and `RawText` formats, the object body is decoded
    /// line-by-line via [`tokio::io::AsyncBufReadExt`] and a page is yielded
    /// whenever the buffer reaches this size; multi-object scans flatten so
    /// a single page may contain lines from any object. For `JsonArray`,
    /// each object is buffered fully before its records are chunked into
    /// pages of this size (see the README "Streaming and batching" section
    /// for the caveat). Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: every page is one
    /// complete object — no within-object chunking. Useful for small
    /// lookup files, or for sinks (e.g. SQL `COPY`, BigQuery load jobs)
    /// that prefer one large request per file to many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
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

impl S3SourceConfig {
    /// Create a new config with the required bucket name and sensible defaults.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: None,
            region: None,
            endpoint_url: None,
            file_format: S3FileFormat::default(),
            max_objects: None,
            concurrency: 10,
            batch_size: DEFAULT_BATCH_SIZE,
            #[cfg(feature = "compression")]
            compression: faucet_core::CompressionConfig::default(),
        }
    }

    /// Set the object key prefix filter.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
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

    /// Set the file format.
    pub fn file_format(mut self, format: S3FileFormat) -> Self {
        self.file_format = format;
        self
    }

    /// Set the maximum number of objects to read.
    pub fn max_objects(mut self, max: usize) -> Self {
        self.max_objects = Some(max);
        self
    }

    /// Set the maximum number of concurrent object reads.
    pub fn concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = concurrency;
        self
    }

    /// Set the per-page record count for [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of within-object chunking — every emitted
    /// [`StreamPage`](faucet_core::StreamPage) corresponds to exactly one
    /// S3 object.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
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
        let config = S3SourceConfig::new("my-bucket");
        assert_eq!(config.bucket, "my-bucket");
        assert!(config.prefix.is_none());
        assert!(config.region.is_none());
        assert!(config.endpoint_url.is_none());
        assert!(matches!(config.file_format, S3FileFormat::JsonLines));
        assert!(config.max_objects.is_none());
    }

    #[test]
    fn builder_methods() {
        let config = S3SourceConfig::new("my-bucket")
            .prefix("data/")
            .region("us-west-2")
            .endpoint_url("http://localhost:9000")
            .file_format(S3FileFormat::JsonArray)
            .max_objects(10);

        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.prefix.as_deref(), Some("data/"));
        assert_eq!(config.region.as_deref(), Some("us-west-2"));
        assert_eq!(
            config.endpoint_url.as_deref(),
            Some("http://localhost:9000")
        );
        assert!(matches!(config.file_format, S3FileFormat::JsonArray));
        assert_eq!(config.max_objects, Some(10));
    }

    #[test]
    fn file_format_default_is_json_lines() {
        let format = S3FileFormat::default();
        assert!(matches!(format, S3FileFormat::JsonLines));
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = S3SourceConfig::new("my-bucket");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = S3SourceConfig::new("my-bucket").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = S3SourceConfig::new("my-bucket").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config =
            S3SourceConfig::new("my-bucket").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "bucket": "my-bucket",
            "prefix": null,
            "region": null,
            "endpoint_url": null,
            "file_format": "json_lines",
            "max_objects": null,
            "concurrency": 10,
            "batch_size": 250
        }"#;
        let config: S3SourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compression_default_is_auto() {
        let cfg = S3SourceConfig::new("bucket");
        assert_eq!(cfg.compression, faucet_core::CompressionConfig::Auto);
    }
}
