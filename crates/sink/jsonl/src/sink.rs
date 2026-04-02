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
pub struct JsonlSink {
    config: JsonlSinkConfig,
    /// Mutex-protected writer for thread-safe concurrent writes.
    writer: Mutex<Option<tokio::io::BufWriter<tokio::fs::File>>>,
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
        tokio::sync::MutexGuard<'_, Option<tokio::io::BufWriter<tokio::fs::File>>>,
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
            *guard = Some(tokio::io::BufWriter::new(file));
        }
        Ok(guard)
    }
}

#[async_trait]
impl faucet_core::Sink for JsonlSink {
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
        if let Some(writer) = guard.as_mut() {
            writer
                .flush()
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
}
