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
        faucet_core::validate_batch_size(config.batch_size)?;
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

    /// Serialize a slice of records as JSON Lines bytes.
    fn serialize_jsonl(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
        let mut buf: Vec<u8> = Vec::new();
        for record in records {
            let line = serde_json::to_vec(record)
                .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
            buf.extend_from_slice(&line);
            buf.push(b'\n');
        }
        Ok(buf)
    }

    /// Generate a unique S3 key for a file.
    fn generate_key(&self) -> String {
        let id = uuid::Uuid::new_v4();
        format!("{}{}{}", self.config.prefix, id, self.config.file_extension)
    }

    /// Upload a single JSONL file to S3.
    async fn upload_file(&self, key: &str, body: Vec<u8>) -> Result<(), FaucetError> {
        #[cfg(feature = "compression")]
        let body = {
            let codec = self.config.compression.resolve(&self.config.file_extension);
            faucet_core::compression::warn_mismatch(&self.config.file_extension, codec);
            faucet_core::compression::compress_buf(&body, codec)?
        };

        self.client
            .put_object()
            .bucket(&self.config.bucket)
            .key(key)
            .body(body.into())
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
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(S3SinkConfig)).expect("schema serialization")
    }

    /// Preflight probe: confirm the configured bucket is reachable and the
    /// credentials work via a non-mutating `HeadBucket` call. Uploads nothing.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe = match tokio::time::timeout(
            ctx.timeout,
            self.client.head_bucket().bucket(&self.config.bucket).send(),
        )
        .await
        {
            Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e.to_string(),
                "check bucket name, credentials, and network",
            ),
            Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
        };
        Ok(CheckReport::single(probe))
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Effective per-object cap is the smaller of `batch_size` (when set)
        // and `max_records_per_file` (when set). When neither caps the chunk,
        // the whole slice is written as a single object.
        let chunk_cap: Option<usize> =
            match (self.config.batch_size, self.config.max_records_per_file) {
                (0, None) => None,
                (0, Some(0)) => None,
                (0, Some(max)) => Some(max),
                (bs, None) => Some(bs),
                (bs, Some(0)) => Some(bs),
                (bs, Some(max)) => Some(bs.min(max)),
            };

        let chunks: Vec<&[Value]> = match chunk_cap {
            Some(cap) => records.chunks(cap).collect(),
            None => vec![records],
        };

        let total_files = chunks.len();
        let concurrency = self.config.concurrency.max(1);

        // Pre-serialize each chunk and generate keys before uploading.
        let prepared: Vec<(String, Vec<u8>)> = chunks
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
        let text = String::from_utf8(result).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[test]
    fn serialize_jsonl_empty() {
        let result = S3Sink::serialize_jsonl(&[]).unwrap();
        assert!(result.is_empty());
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

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = S3SinkConfig::new("bucket");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match S3Sink::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compress_buf_used_for_gzip_extension() {
        // White-box: confirm the codec resolved from file_extension is Gzip
        // and that compress_buf produces a gzip-magic-prefixed buffer.
        let cfg = S3SinkConfig::new("bucket").file_extension(".jsonl.gz");
        let codec = cfg.compression.resolve(&cfg.file_extension);
        assert_eq!(codec, faucet_core::Compression::Gzip);
        let compressed = faucet_core::compression::compress_buf(b"hello\n", codec).unwrap();
        // gzip magic bytes.
        assert_eq!(&compressed[..2], b"\x1f\x8b");
    }
}
