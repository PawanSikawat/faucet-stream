//! S3 sink executor.

use crate::config::S3SinkConfig;
#[cfg(feature = "arrow")]
use crate::config::S3SinkFormat;
use async_trait::async_trait;
use aws_sdk_s3::Client;
use faucet_core::FaucetError;
use futures::stream::{self, StreamExt, TryStreamExt};
use serde_json::Value;

/// A sink that writes JSON records to S3 as JSON Lines files (or, with the
/// `arrow` feature, self-contained Parquet objects).
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

    /// The effective per-object record cap, combining `batch_size` (write-side
    /// re-chunking) and `max_records_per_file`. `None` means "one object for
    /// the whole call". Shared by the JSONL and Parquet write paths.
    fn effective_chunk_cap(&self) -> Option<usize> {
        match (self.config.batch_size, self.config.max_records_per_file) {
            (0, None) => None,
            (0, Some(0)) => None,
            (0, Some(max)) => Some(max),
            (bs, None) => Some(bs),
            (bs, Some(0)) => Some(bs),
            (bs, Some(max)) => Some(bs.min(max)),
        }
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

    /// Upload pre-encoded Parquet objects concurrently. Parquet carries its own
    /// internal compression, so the crate-local `compression` wrapper is
    /// deliberately **not** applied here; the content type advertises Parquet.
    #[cfg(feature = "arrow")]
    async fn upload_parquet_objects(
        &self,
        prepared: Vec<(String, Vec<u8>)>,
    ) -> Result<(), FaucetError> {
        let concurrency = self.config.concurrency.max(1);
        stream::iter(prepared)
            .map(|(key, body)| async move {
                self.client
                    .put_object()
                    .bucket(&self.config.bucket)
                    .key(&key)
                    .body(body.into())
                    .content_type("application/vnd.apache.parquet")
                    .send()
                    .await
                    .map_err(|e| {
                        FaucetError::Sink(format!("S3 put object error for key '{key}': {e}"))
                    })?;
                tracing::debug!(key = %key, "Uploaded S3 parquet object");
                Ok::<(), FaucetError>(())
            })
            .buffer_unordered(concurrency)
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for S3Sink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(S3SinkConfig)).expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!("s3://{}/{}", self.config.bucket, self.config.prefix)
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

        let chunks: Vec<&[Value]> = match self.effective_chunk_cap() {
            Some(cap) => records.chunks(cap).collect(),
            None => vec![records],
        };

        // Parquet path: encode each chunk as a self-contained Parquet object.
        #[cfg(feature = "arrow")]
        if matches!(self.config.format, S3SinkFormat::Parquet) {
            let prepared: Vec<(String, Vec<u8>)> = chunks
                .iter()
                .map(|chunk| {
                    let batch = faucet_core::columnar::values_to_record_batch_inferred(chunk)?;
                    let body = encode_parquet(&batch)?;
                    Ok((self.generate_key(), body))
                })
                .collect::<Result<Vec<_>, FaucetError>>()?;
            self.upload_parquet_objects(prepared).await?;
            tracing::info!(
                records = records.len(),
                files = chunks.len(),
                "S3 parquet batch write complete"
            );
            return Ok(records.len());
        }

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

    /// The S3 sink consumes Arrow `RecordBatch`es natively **only** when
    /// configured for the [`Parquet`](S3SinkFormat::Parquet) format; the JSONL
    /// format has no columnar representation and stays on the row path
    /// (RFC 0002 / #375).
    #[cfg(feature = "arrow")]
    fn supports_columnar(&self) -> bool {
        matches!(self.config.format, S3SinkFormat::Parquet)
    }

    /// Write an Arrow `RecordBatch` as one or more self-contained Parquet
    /// objects (sliced by the effective per-object cap), skipping the
    /// `Value` round-trip. Falls back to the row path for a non-Parquet
    /// format (which `supports_columnar` prevents the pipeline from reaching,
    /// but a direct caller might).
    #[cfg(feature = "arrow")]
    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        if batch.num_rows() == 0 {
            return Ok(0);
        }
        if !matches!(self.config.format, S3SinkFormat::Parquet) {
            let rows = faucet_core::columnar::record_batch_to_values(batch)?;
            return self.write_batch(&rows).await;
        }

        let n = batch.num_rows();
        let cap = self.effective_chunk_cap().unwrap_or(n).max(1);
        let mut prepared: Vec<(String, Vec<u8>)> = Vec::new();
        let mut offset = 0usize;
        while offset < n {
            let len = cap.min(n - offset);
            let slice = batch.slice(offset, len);
            let body = encode_parquet(&slice)?;
            prepared.push((self.generate_key(), body));
            offset += len;
        }

        let files = prepared.len();
        self.upload_parquet_objects(prepared).await?;
        tracing::info!(records = n, files, "S3 parquet columnar write complete");
        Ok(n)
    }
}

/// Encode an Arrow `RecordBatch` into a complete, self-contained Parquet file
/// (ZSTD-compressed) in memory.
#[cfg(feature = "arrow")]
fn encode_parquet(batch: &arrow::array::RecordBatch) -> Result<Vec<u8>, FaucetError> {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, ZstdLevel};
    use parquet::file::properties::WriterProperties;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))
            .map_err(|e| FaucetError::Sink(format!("parquet writer init failed: {e}")))?;
        writer
            .write(batch)
            .map_err(|e| FaucetError::Sink(format!("parquet write failed: {e}")))?;
        writer
            .close()
            .map_err(|e| FaucetError::Sink(format!("parquet finalize failed: {e}")))?;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::S3SinkConfig;
    use faucet_core::Sink as _;
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
    fn dataset_uri_includes_bucket_and_prefix() {
        let sink = test_sink(S3SinkConfig::new("my-bucket").prefix("data/events/"));
        assert_eq!(sink.dataset_uri(), "s3://my-bucket/data/events/");
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

    // ── Parquet columnar path (feature `arrow`) ──────────────────────────────

    #[cfg(feature = "arrow")]
    #[test]
    fn supports_columnar_only_for_parquet_format() {
        let parquet_sink = test_sink(S3SinkConfig::new("b").format(S3SinkFormat::Parquet));
        assert!(faucet_core::Sink::supports_columnar(&parquet_sink));
        let json_sink = test_sink(S3SinkConfig::new("b"));
        assert!(!faucet_core::Sink::supports_columnar(&json_sink));
    }

    #[cfg(feature = "arrow")]
    #[test]
    fn encode_parquet_round_trips_via_reader() {
        use arrow::array::{Int32Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();

        let bytes = encode_parquet(&batch).unwrap();
        // Parquet magic header/footer.
        assert_eq!(&bytes[..4], b"PAR1");

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let total: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(total, 3);
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
