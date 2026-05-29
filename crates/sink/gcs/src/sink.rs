//! GCS sink executor.

use crate::config::GcsSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_gcs_common::build_storage;
use futures::stream::{self, StreamExt, TryStreamExt};
use google_cloud_storage::client::Storage;
use serde_json::Value;

/// A sink that writes JSON records to GCS as JSON Lines files.
pub struct GcsSink {
    config: GcsSinkConfig,
    storage: Storage,
}

impl GcsSink {
    pub async fn new(config: GcsSinkConfig) -> Result<Self, FaucetError> {
        let storage = build_storage(&config.auth, config.storage_host.as_deref()).await?;
        Ok(Self { config, storage })
    }

    /// Bucket as a GCS resource path: `projects/_/buckets/{bucket}`.
    fn bucket_path(&self) -> String {
        format!("projects/_/buckets/{}", self.config.bucket)
    }

    /// Serialize a slice of records as a JSON Lines byte buffer.
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

    /// Generate a time-sortable UUIDv7 object name.
    fn generate_key(&self) -> String {
        generate_object_key(&self.config.prefix, &self.config.file_extension)
    }

    /// Upload a single JSONL file to GCS.
    async fn upload_file(&self, key: &str, body: Vec<u8>) -> Result<(), FaucetError> {
        #[cfg(feature = "compression")]
        let body = {
            let codec = self.config.compression.resolve(&self.config.file_extension);
            faucet_core::compression::warn_mismatch(&self.config.file_extension, codec);
            faucet_core::compression::compress_buf(&body, codec)?
        };

        let payload = bytes::Bytes::from(body);
        self.storage
            .write_object(self.bucket_path(), key.to_string(), payload)
            .set_content_type("application/x-ndjson")
            .send_unbuffered()
            .await
            .map_err(|e| FaucetError::Sink(format!("GCS put object error for key '{key}': {e}")))?;
        tracing::debug!(key = %key, "Uploaded GCS object");
        Ok(())
    }

    /// Compute the effective chunk size combining `batch_size` and
    /// `max_records_per_file`. `batch_size = 0` removes the batch-size
    /// limit; `max_records_per_file = None` removes the file-rollover
    /// limit. When both are unlimited, returns `usize::MAX` (single chunk).
    fn effective_chunk_size(&self) -> usize {
        resolve_effective_chunk_size(&self.config)
    }
}

#[async_trait]
impl faucet_core::Sink for GcsSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let chunk = self.effective_chunk_size();
        let concurrency = self.config.concurrency.max(1);

        let uploads: Vec<(String, Vec<u8>)> = records
            .chunks(chunk)
            .map(|slice| {
                let body = Self::serialize_jsonl(slice)?;
                Ok::<(String, Vec<u8>), FaucetError>((self.generate_key(), body))
            })
            .collect::<Result<_, _>>()?;

        let written = records.len();
        stream::iter(uploads)
            .map(|(key, body)| async move { self.upload_file(&key, body).await })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<()>>()
            .await?;

        Ok(written)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(GcsSinkConfig)).expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "gcs"
    }
}

/// Pure helper for chunk-size resolution — used by `write_batch` and unit
/// tested directly so the test surface doesn't need a `Storage` stub.
fn resolve_effective_chunk_size(config: &GcsSinkConfig) -> usize {
    let bs = if config.batch_size == 0 {
        usize::MAX
    } else {
        config.batch_size
    };
    let mr = config.max_records_per_file.unwrap_or(usize::MAX);
    bs.min(mr)
}

/// Pure helper for object-key generation — used by `write_batch` and unit
/// tested directly. UUIDv7 makes keys time-sortable so a listing of the
/// destination bucket returns objects in write order.
fn generate_object_key(prefix: &str, file_extension: &str) -> String {
    format!("{prefix}{}{file_extension}", uuid::Uuid::now_v7())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serialize_jsonl_two_records() {
        let body = GcsSink::serialize_jsonl(&[json!({"a": 1}), json!({"b": 2})]).unwrap();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            "{\"a\":1}\n{\"b\":2}\n"
        );
    }

    #[test]
    fn serialize_jsonl_empty_is_empty() {
        let body = GcsSink::serialize_jsonl(&[]).unwrap();
        assert!(body.is_empty());
    }

    #[test]
    fn effective_chunk_size_unlimited_when_both_unset() {
        let cfg = GcsSinkConfig::new("b").with_batch_size(0);
        assert_eq!(resolve_effective_chunk_size(&cfg), usize::MAX);
    }

    #[test]
    fn effective_chunk_size_takes_smaller_limit() {
        let cfg = GcsSinkConfig::new("b")
            .with_batch_size(500)
            .max_records_per_file(100);
        assert_eq!(resolve_effective_chunk_size(&cfg), 100);
    }

    #[test]
    fn effective_chunk_size_uses_batch_size_when_smaller() {
        let cfg = GcsSinkConfig::new("b")
            .with_batch_size(50)
            .max_records_per_file(500);
        assert_eq!(resolve_effective_chunk_size(&cfg), 50);
    }

    #[test]
    fn generate_key_uses_prefix_and_extension() {
        let key = generate_object_key("out/", ".ndjson");
        assert!(key.starts_with("out/"));
        assert!(key.ends_with(".ndjson"));
    }

    #[test]
    fn generate_key_yields_distinct_time_ordered_keys() {
        let a = generate_object_key("p/", ".jsonl");
        let b = generate_object_key("p/", ".jsonl");
        assert_ne!(a, b);
        // UUIDv7 keys are lexically comparable by time within the same
        // process: the second key generated should compare greater.
        assert!(a < b, "expected UUIDv7 keys to sort by generation order");
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compress_buf_used_for_zstd_extension() {
        let cfg = GcsSinkConfig::new("bucket").file_extension(".jsonl.zst");
        let codec = cfg.compression.resolve(&cfg.file_extension);
        assert_eq!(codec, faucet_core::Compression::Zstd);
        let compressed = faucet_core::compression::compress_buf(b"hello\n", codec).unwrap();
        // zstd magic bytes: 0x28 B5 2F FD.
        assert_eq!(&compressed[..4], b"\x28\xb5\x2f\xfd");
    }
}
