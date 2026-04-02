//! S3 sink executor.

use crate::config::S3SinkConfig;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use faucet_core::FaucetError;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde_json::Value;

/// A sink that writes JSON records to S3 as JSON Lines files.
pub struct S3Sink {
    config: S3SinkConfig,
    client: Client,
}

impl S3Sink {
    /// Create a new S3 sink from the given configuration.
    ///
    /// Builds the S3 client eagerly so it is reused across calls.
    pub async fn new(config: S3SinkConfig) -> Result<Self, FaucetError> {
        let client = Self::build_client(&config).await?;
        Ok(Self { config, client })
    }

    /// Build an S3 client from the configuration.
    async fn build_client(config: &S3SinkConfig) -> Result<Client, FaucetError> {
        let mut config_loader = aws_config::defaults(aws_config::BehaviorVersion::latest());

        if let Some(ref region) = config.region {
            config_loader = config_loader.region(aws_config::Region::new(region.clone()));
        }

        if let Some(ref endpoint) = config.endpoint_url {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        let sdk_config = config_loader.load().await;
        let client = Client::new(&sdk_config);
        Ok(client)
    }

    /// Serialize a slice of records as a JSON Lines string.
    fn serialize_jsonl(records: &[Value]) -> Result<String, FaucetError> {
        let mut buf = String::new();
        for record in records {
            let line = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
            buf.push_str(&line);
            buf.push('\n');
        }
        Ok(buf)
    }

    /// Generate a unique S3 key for a file.
    fn generate_key(&self) -> String {
        let id = uuid::Uuid::new_v4();
        format!("{}{}{}", self.config.prefix, id, self.config.file_extension)
    }

    /// Upload a single JSONL file to S3.
    async fn upload_file(&self, key: &str, body: String) -> Result<(), FaucetError> {
        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(body.into_bytes().into())
            .content_type("application/x-ndjson")
            .send()
            .await
            .map_err(|e| FaucetError::Sink(format!("S3 put object error for key '{key}': {e}")))?;

        tracing::debug!(key = %key, "Uploaded S3 object");
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for S3Sink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let chunks: Vec<&[Value]> = match self.config.max_records_per_file {
            Some(max) if max > 0 => records.chunks(max).collect(),
            _ => vec![records],
        };

        let total_files = chunks.len();
        let concurrency = self.config.concurrency.max(1);

        // Pre-serialize each chunk and generate keys before uploading.
        let prepared: Vec<(String, String)> = chunks
            .iter()
            .map(|chunk| {
                let body = Self::serialize_jsonl(chunk)?;
                let key = self.generate_key();
                Ok((key, body))
            })
            .collect::<Result<Vec<_>, FaucetError>>()?;

        stream::iter(prepared)
            .map(|(key, body)| async move { self.upload_file(&key, body).await })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<()>>()
            .await?;

        tracing::info!(
            records = records.len(),
            files = total_files,
            "S3 batch write complete"
        );
        Ok(records.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::S3SinkConfig;
    use serde_json::json;

    /// Helper to build an S3Sink synchronously for tests that never make network calls.
    fn test_sink(config: S3SinkConfig) -> S3Sink {
        let sdk_config = aws_config::SdkConfig::builder()
            .behavior_version(aws_config::BehaviorVersion::latest())
            .build();
        let client = Client::new(&sdk_config);
        S3Sink { config, client }
    }

    #[test]
    fn serialize_jsonl_produces_newline_delimited() {
        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        let result = S3Sink::serialize_jsonl(&records).unwrap();
        let lines: Vec<&str> = result.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[test]
    fn serialize_jsonl_empty() {
        let result = S3Sink::serialize_jsonl(&[]).unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn generate_key_uses_prefix_and_extension() {
        let sink = test_sink(
            S3SinkConfig::new("bucket")
                .prefix("data/")
                .file_extension(".jsonl"),
        );
        let key = sink.generate_key();
        assert!(key.starts_with("data/"));
        assert!(key.ends_with(".jsonl"));
        // UUID is 36 chars
        assert!(key.len() > "data/".len() + ".jsonl".len());
    }

    #[test]
    fn generate_key_no_prefix() {
        let sink = test_sink(S3SinkConfig::new("bucket"));
        let key = sink.generate_key();
        assert!(key.ends_with(".jsonl"));
        // No prefix means key starts with UUID
        assert!(!key.starts_with('/'));
    }
}
