//! JSON Lines file sink.

use crate::config::JsonlSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// A sink that writes JSON records to a file in JSON Lines format.
///
/// Each record is written as a single line of JSON followed by a newline.
/// The file is opened lazily on the first `write_batch` call.
///
/// With the `compression` feature, the writer transparently wraps the file
/// with a gzip / zstd encoder based on the `compression` config field.
/// [`Sink::flush`] finalises the encoder (writes the trailer) and clears the
/// writer slot — a subsequent `write_batch` reopens the file in append mode
/// and starts a fresh encoder, producing a multi-member compressed file that
/// decoders read back correctly. Pipelines call `flush` exactly once at
/// end-of-run, so the multi-member case only arises for direct library users
/// who interleave `flush` and `write_batch`.
pub struct JsonlSink {
    config: JsonlSinkConfig,
    /// Mutex-protected writer for thread-safe concurrent writes.
    writer: Mutex<Option<std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>>>,
}

impl JsonlSink {
    /// Create a new JSON Lines sink. The file is opened on first write.
    pub fn new(config: JsonlSinkConfig) -> Self {
        Self {
            config,
            writer: Mutex::new(None),
        }
    }

    /// Ensure the file is open and return a mutable reference to the writer.
    async fn ensure_open(
        &self,
    ) -> Result<
        tokio::sync::MutexGuard<
            '_,
            Option<std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>>>,
        >,
        FaucetError,
    > {
        let mut guard = self.writer.lock().await;
        if guard.is_none() {
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(self.config.append)
                .truncate(!self.config.append)
                .open(&self.config.path)
                .await
                .map_err(|e| {
                    FaucetError::Sink(format!(
                        "failed to open {}: {e}",
                        self.config.path.display()
                    ))
                })?;
            let buffered = tokio::io::BufWriter::new(file);
            #[cfg(feature = "compression")]
            let writer: std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>> = {
                let path_str = self.config.path.to_string_lossy();
                let codec = self.config.compression.resolve(&path_str);
                faucet_core::compression::warn_mismatch(&path_str, codec);
                faucet_core::compression::wrap_async_writer(buffered, codec)
            };
            #[cfg(not(feature = "compression"))]
            let writer: std::pin::Pin<Box<dyn tokio::io::AsyncWrite + Send + Unpin>> =
                Box::pin(buffered);
            *guard = Some(writer);
        }
        Ok(guard)
    }
}

#[async_trait]
impl faucet_core::Sink for JsonlSink {
    fn connector_name(&self) -> &'static str {
        "jsonl"
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(JsonlSinkConfig))
            .expect("schema serialization")
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut guard = self.ensure_open().await?;
        let writer = guard.as_mut().expect("writer opened in ensure_open");

        for record in records {
            let line = if self.config.pretty {
                serde_json::to_string_pretty(record)
            } else {
                serde_json::to_string(record)
            }
            .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;

            writer
                .write_all(line.as_bytes())
                .await
                .map_err(|e| FaucetError::Sink(format!("write failed: {e}")))?;
            writer
                .write_all(b"\n")
                .await
                .map_err(|e| FaucetError::Sink(format!("write failed: {e}")))?;
        }

        tracing::debug!(records = records.len(), "JSONL batch written");
        Ok(records.len())
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        let mut guard = self.writer.lock().await;
        if let Some(mut writer) = guard.take() {
            use tokio::io::AsyncWriteExt;
            writer
                .shutdown()
                .await
                .map_err(|e| FaucetError::Sink(format!("flush failed: {e}")))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn writes_jsonl_records() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));

        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        let count = sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 2);
        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[tokio::test]
    async fn append_mode() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();

        // Write first batch.
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));
        sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        sink.flush().await.unwrap();
        drop(sink);

        // Write second batch in append mode.
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path).append(true));
        sink.write_batch(&[json!({"id": 2})]).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
    }

    #[tokio::test]
    async fn empty_batch_returns_zero() {
        let tmp = NamedTempFile::new().unwrap();
        let sink = JsonlSink::new(JsonlSinkConfig::new(tmp.path()));
        let count = sink.write_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn flush_without_write_is_noop() {
        let tmp = NamedTempFile::new().unwrap();
        let sink = JsonlSink::new(JsonlSinkConfig::new(tmp.path()));
        assert!(sink.flush().await.is_ok());
    }

    #[tokio::test]
    async fn multiple_batches_accumulate() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(JsonlSinkConfig::new(&path));

        sink.write_batch(&[json!({"a": 1})]).await.unwrap();
        sink.write_batch(&[json!({"b": 2}), json!({"c": 3})])
            .await
            .unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn jsonl_sink_connector_name_is_jsonl() {
        use faucet_core::Sink;
        let tmp = NamedTempFile::new().unwrap();
        let sink = JsonlSink::new(JsonlSinkConfig::new(tmp.path()));
        assert_eq!(sink.connector_name(), "jsonl");
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_gzip() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".jsonl.gz").unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(
            JsonlSinkConfig::new(&path).compression(CompressionConfig::Auto),
        );

        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        // Read raw bytes, decompress via faucet_core, parse JSONL.
        let bytes = tokio::fs::read(&path).await.unwrap();
        use tokio::io::AsyncReadExt;
        let mut decoded = Vec::new();
        let mut r = faucet_core::compression::wrap_async_reader(
            tokio::io::BufReader::new(&bytes[..]),
            faucet_core::Compression::Gzip,
        );
        r.read_to_end(&mut decoded).await.unwrap();
        let text = String::from_utf8(decoded).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_zstd() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".jsonl.zst").unwrap();
        let path = tmp.path().to_path_buf();
        let sink = JsonlSink::new(
            JsonlSinkConfig::new(&path).compression(CompressionConfig::Auto),
        );
        sink.write_batch(&[json!({"x": 42})]).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use tokio::io::AsyncReadExt;
        let mut decoded = Vec::new();
        let mut r = faucet_core::compression::wrap_async_reader(
            tokio::io::BufReader::new(&bytes[..]),
            faucet_core::Compression::Zstd,
        );
        r.read_to_end(&mut decoded).await.unwrap();
        let text = String::from_utf8(decoded).unwrap();
        let v: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(v["x"], 42);
    }
}
