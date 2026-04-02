//! S3 sink configuration.

/// Configuration for the S3 sink connector.
#[derive(Debug, Clone)]
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
}
