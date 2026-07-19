//! Azure Blob sink executor.

use std::sync::Arc;

use async_trait::async_trait;
use faucet_common_azure::build_store;
use faucet_core::FaucetError;
use futures::stream::{self, StreamExt, TryStreamExt};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use serde_json::Value;

use crate::config::AzureBlobSinkConfig;

/// A sink that writes JSON records to Azure Blob as JSON Lines objects.
pub struct AzureBlobSink {
    config: AzureBlobSinkConfig,
    store: Arc<dyn ObjectStore>,
}

impl AzureBlobSink {
    /// Construct the sink, building the object store eagerly so it is reused
    /// across calls.
    pub async fn new(config: AzureBlobSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        let store = build_store(&config.connection)?;
        Ok(Self { config, store })
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

    /// Upload a single JSONL object.
    async fn upload_file(&self, key: &str, body: Vec<u8>) -> Result<(), FaucetError> {
        #[cfg(feature = "compression")]
        let body = {
            let codec = self.config.compression.resolve(&self.config.file_extension);
            faucet_core::compression::warn_mismatch(&self.config.file_extension, codec);
            faucet_core::compression::compress_buf(&body, codec)?
        };

        let path = ObjectPath::from(key);
        let payload = bytes::Bytes::from(body);
        self.store.put(&path, payload.into()).await.map_err(|e| {
            FaucetError::Sink(format!("azure put object error for key '{key}': {e}"))
        })?;
        tracing::debug!(key = %key, "Uploaded Azure object");
        Ok(())
    }

    /// Effective per-object chunk size (smaller of `batch_size` and
    /// `max_records_per_file`).
    fn effective_chunk_size(&self) -> usize {
        resolve_effective_chunk_size(&self.config)
    }
}

#[async_trait]
impl faucet_core::Sink for AzureBlobSink {
    fn dataset_uri(&self) -> String {
        format!("az://{}/{}", self.config.container(), self.config.prefix)
    }

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
        let files = uploads.len();
        stream::iter(uploads)
            .map(|(key, body)| async move { self.upload_file(&key, body).await })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<()>>()
            .await?;

        tracing::info!(records = written, files, "Azure batch write complete");
        Ok(written)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(AzureBlobSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "azure-blob"
    }

    /// Preflight probe: confirm the container is reachable and the credentials
    /// work via a non-mutating listing capped at a single item. Uploads
    /// nothing.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, async {
            let mut listing = self.store.list(None);
            listing.next().await
        })
        .await
        {
            Ok(None) | Ok(Some(Ok(_))) => Probe::pass("auth", started.elapsed()),
            Ok(Some(Err(e))) => Probe::fail_hint(
                "auth",
                started.elapsed(),
                e.to_string(),
                "check account, container, credentials, and network",
            ),
            Err(_) => Probe::fail("network", started.elapsed(), "timed out"),
        };
        Ok(CheckReport::single(probe))
    }
}

/// Pure chunk-size resolution — unit tested directly so the test surface needs
/// no object store.
fn resolve_effective_chunk_size(config: &AzureBlobSinkConfig) -> usize {
    let bs = if config.batch_size == 0 {
        usize::MAX
    } else {
        config.batch_size
    };
    let mr = config.max_records_per_file.unwrap_or(usize::MAX);
    bs.min(mr)
}

/// Pure object-key generation — UUIDv7 makes keys time-sortable so a listing of
/// the destination returns objects in write order.
fn generate_object_key(prefix: &str, file_extension: &str) -> String {
    format!("{prefix}{}{file_extension}", uuid::Uuid::now_v7())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = AzureBlobSinkConfig::new("cont");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match AzureBlobSink::new(config).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("batch_size"), "got: {m}"),
            Ok(_) => panic!("expected a batch_size Config error, got Ok(sink)"),
            Err(e) => panic!("expected a batch_size Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_rejects_empty_container() {
        let config = AzureBlobSinkConfig::new("   ");
        match AzureBlobSink::new(config).await {
            Err(FaucetError::Config(m)) => assert!(m.contains("container"), "got: {m}"),
            Ok(_) => panic!("expected a container Config error, got Ok(sink)"),
            Err(e) => panic!("expected a container Config error, got {e:?}"),
        }
    }

    #[tokio::test]
    async fn new_builds_lazily_with_emulator() {
        use faucet_core::Sink as _;
        let config = AzureBlobSinkConfig::new("cont")
            .prefix("out/")
            .use_emulator(true)
            .allow_http(true);
        let sink = AzureBlobSink::new(config).await.unwrap();
        assert_eq!(sink.connector_name(), "azure-blob");
        assert_eq!(sink.dataset_uri(), "az://cont/out/");
    }

    #[test]
    fn serialize_jsonl_two_records() {
        let body = AzureBlobSink::serialize_jsonl(&[json!({"a": 1}), json!({"b": 2})]).unwrap();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            "{\"a\":1}\n{\"b\":2}\n"
        );
    }

    #[test]
    fn serialize_jsonl_empty_is_empty() {
        let body = AzureBlobSink::serialize_jsonl(&[]).unwrap();
        assert!(body.is_empty());
    }

    #[test]
    fn effective_chunk_size_unlimited_when_both_unset() {
        let cfg = AzureBlobSinkConfig::new("c").with_batch_size(0);
        assert_eq!(resolve_effective_chunk_size(&cfg), usize::MAX);
    }

    #[test]
    fn effective_chunk_size_takes_smaller_limit() {
        let cfg = AzureBlobSinkConfig::new("c")
            .with_batch_size(500)
            .max_records_per_file(100);
        assert_eq!(resolve_effective_chunk_size(&cfg), 100);
    }

    #[test]
    fn effective_chunk_size_uses_batch_size_when_smaller() {
        let cfg = AzureBlobSinkConfig::new("c")
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
        assert!(a < b, "expected UUIDv7 keys to sort by generation order");
    }

    #[cfg(feature = "compression")]
    #[test]
    fn compress_buf_used_for_gzip_extension() {
        let cfg = AzureBlobSinkConfig::new("c").file_extension(".jsonl.gz");
        let codec = cfg.compression.resolve(&cfg.file_extension);
        assert_eq!(codec, faucet_core::Compression::Gzip);
        let compressed = faucet_core::compression::compress_buf(b"hello\n", codec).unwrap();
        assert_eq!(&compressed[..2], b"\x1f\x8b");
    }
}
