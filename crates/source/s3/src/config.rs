//! S3 source configuration.

/// Format of files stored in S3.
#[derive(Debug, Clone, Default)]
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
#[derive(Debug, Clone)]
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
}
