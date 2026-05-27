//! CSV file sink.

use crate::config::CsvSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use std::fs::OpenOptions;
use std::sync::Mutex;

/// The inner writer the CSV serializer writes into. With compression enabled
/// it is a [`SyncCompressWriter`](faucet_core::compression::SyncCompressWriter)
/// that retains the concrete encoder so `finish()` errors surface on flush
/// (#78/#41); otherwise it is the raw file.
#[cfg(feature = "compression")]
type SinkWriter = faucet_core::compression::SyncCompressWriter<std::fs::File>;
#[cfg(not(feature = "compression"))]
type SinkWriter = std::fs::File;

/// State for the CSV writer, including the determined column order.
struct WriterState {
    writer: csv::Writer<SinkWriter>,
    columns: Vec<String>,
}

/// A sink that writes JSON records to a CSV file.
///
/// Column order is determined from the keys of the first record in the first
/// `write_batch` call. Subsequent records use the same column order; missing
/// fields are written as empty strings.
///
/// [`Sink::flush`] finalises the encoder (writes the trailer) and clears the
/// writer slot — a subsequent `write_batch` reopens the file in append mode
/// (independent of `config.append`) and starts a fresh encoder. This makes
/// the per-page `flush` the pipeline emits for bookmarked pages safe for CDC
/// sources — every transaction appends rather than truncates.
pub struct CsvSink {
    config: CsvSinkConfig,
    state: Mutex<Option<WriterState>>,
    /// Tracks whether the file has been opened at least once.
    /// On re-opens (after `flush()` clears the writer), we always use
    /// append mode regardless of `config.append` so the new gzip / zstd
    /// member appends instead of truncating the file. Without this, the
    /// pipeline's per-bookmark flush would silently lose data when
    /// `config.append = false` (the default).
    opened_once: std::sync::atomic::AtomicBool,
}

impl CsvSink {
    /// Create a new CSV sink. The file is opened on the first `write_batch` call.
    pub fn new(config: CsvSinkConfig) -> Self {
        Self {
            config,
            state: Mutex::new(None),
            opened_once: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Convert a JSON value to a string suitable for a CSV field.
    fn value_to_csv_field(value: &Value) -> String {
        match value {
            Value::Null => String::new(),
            Value::String(s) => s.clone(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => n.to_string(),
            // For nested objects/arrays, serialize as JSON.
            other => other.to_string(),
        }
    }
}

#[async_trait]
impl faucet_core::Sink for CsvSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(CsvSinkConfig)).expect("schema serialization")
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let config = self.config.clone();
        let records: Vec<Value> = records.to_vec();

        // Extract state from the mutex before entering the blocking task.
        // This avoids holding the MutexGuard across an await point.
        let current_state = {
            let mut guard = self
                .state
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            guard.take()
        };

        let opened_before = self.opened_once.load(std::sync::atomic::Ordering::Relaxed);

        let result = tokio::task::spawn_blocking(move || {
            write_csv_blocking(config, current_state, &records, opened_before)
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("CSV write task failed: {e}")))?;

        let (new_state, count) = result?;

        // Mark opened. From now on, re-opens (after flush) use append mode.
        self.opened_once
            .store(true, std::sync::atomic::Ordering::Relaxed);

        // Put the state back.
        {
            let mut guard = self
                .state
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            *guard = Some(new_state);
        }

        Ok(count)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        // Take the state out of the mutex so we can move it into a blocking
        // task. Replacing it with None means the next write_batch reopens
        // the file in append mode — for compressed output this starts a
        // fresh gzip/zstd member, which decoders read back transparently.
        let state = {
            let mut guard = self
                .state
                .lock()
                .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
            guard.take()
        };
        if let Some(state) = state {
            tokio::task::spawn_blocking(move || -> Result<(), FaucetError> {
                let WriterState { writer, .. } = state;
                // Flush the csv serializer's buffer and recover the inner
                // writer so the compression encoder can be finalised with its
                // error captured, rather than swallowed on drop (#78/#41).
                let inner = writer
                    .into_inner()
                    .map_err(|e| FaucetError::Sink(format!("CSV flush failed: {e}")))?;
                #[cfg(feature = "compression")]
                {
                    // Writes the gzip/zstd trailer and surfaces any I/O error.
                    inner.finish().map_err(|e| {
                        FaucetError::Sink(format!("CSV compression finalise failed: {e}"))
                    })?;
                }
                #[cfg(not(feature = "compression"))]
                {
                    let mut f = inner;
                    std::io::Write::flush(&mut f)
                        .map_err(|e| FaucetError::Sink(format!("CSV flush failed: {e}")))?;
                }
                Ok(())
            })
            .await
            .map_err(|e| FaucetError::Sink(format!("CSV flush task failed: {e}")))??;
        }
        Ok(())
    }
}

/// Synchronous CSV writing logic, run inside `spawn_blocking`.
fn write_csv_blocking(
    config: CsvSinkConfig,
    existing_state: Option<WriterState>,
    records: &[Value],
    opened_before: bool,
) -> Result<(WriterState, usize), FaucetError> {
    let mut state = match existing_state {
        Some(s) => s,
        None => {
            // Determine columns from the first record.
            let columns: Vec<String> = match &records[0] {
                Value::Object(map) => map.keys().cloned().collect(),
                _ => {
                    return Err(FaucetError::Sink(
                        "CSV sink expects JSON objects, got non-object record".into(),
                    ));
                }
            };

            // First open obeys `config.append`. Re-opens (after flush()
            // cleared the writer) always append, so flush-then-write
            // sequences do not truncate previously-written data.
            let (append, truncate) = if opened_before {
                (true, false)
            } else {
                (config.append, !config.append)
            };

            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(append)
                .truncate(truncate)
                .open(&config.path)
                .map_err(|e| {
                    FaucetError::Sink(format!("failed to open CSV file '{}': {e}", config.path))
                })?;

            #[cfg(feature = "compression")]
            let inner: SinkWriter = {
                let codec = config.compression.resolve(&config.path);
                faucet_core::compression::warn_mismatch(&config.path, codec);
                faucet_core::compression::sync_compress_writer(file, codec)
            };
            #[cfg(not(feature = "compression"))]
            let inner: SinkWriter = file;

            let mut writer = csv::WriterBuilder::new()
                .delimiter(config.delimiter)
                .from_writer(inner);

            // Write header row if configured and this is the first open.
            if config.write_headers && !append {
                writer
                    .write_record(&columns)
                    .map_err(|e| FaucetError::Sink(format!("failed to write CSV headers: {e}")))?;
            }

            WriterState { writer, columns }
        }
    };

    let mut count = 0;
    for record in records {
        let row: Vec<String> = state
            .columns
            .iter()
            .map(|col| {
                record
                    .get(col)
                    .map(CsvSink::value_to_csv_field)
                    .unwrap_or_default()
            })
            .collect();

        state
            .writer
            .write_record(&row)
            .map_err(|e| FaucetError::Sink(format!("CSV write error: {e}")))?;
        count += 1;
    }

    tracing::debug!(records = count, path = %config.path, "CSV batch written");

    Ok((state, count))
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Sink;
    use serde_json::json;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn writes_csv_records() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        let records = vec![
            json!({"id": 1, "name": "Alice"}),
            json!({"id": 2, "name": "Bob"}),
        ];
        let count = sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        assert_eq!(count, 2);

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 2 data rows.
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn writes_csv_without_headers() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).write_headers(false));

        let records = vec![json!({"a": "1", "b": "2"})];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // No header, just 1 data row.
        assert_eq!(lines.len(), 1);
    }

    #[tokio::test]
    async fn empty_batch_returns_zero() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        let count = sink.write_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn multiple_batches_accumulate() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        sink.write_batch(&[json!({"x": "1"})]).await.unwrap();
        sink.write_batch(&[json!({"x": "2"}), json!({"x": "3"})])
            .await
            .unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 3 data rows.
        assert_eq!(lines.len(), 4);
    }

    #[tokio::test]
    async fn missing_fields_written_as_empty() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        let records = vec![
            json!({"a": "1", "b": "2"}),
            json!({"a": "3"}), // missing "b"
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        assert_eq!(lines.len(), 3); // header + 2 rows
    }

    #[tokio::test]
    async fn value_to_csv_field_handles_types() {
        assert_eq!(CsvSink::value_to_csv_field(&json!(null)), "");
        assert_eq!(CsvSink::value_to_csv_field(&json!("hello")), "hello");
        assert_eq!(CsvSink::value_to_csv_field(&json!(42)), "42");
        assert_eq!(CsvSink::value_to_csv_field(&json!(true)), "true");
        assert_eq!(CsvSink::value_to_csv_field(&json!(2.72)), "2.72");
    }

    #[tokio::test]
    async fn flush_without_write_is_noop() {
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));
        assert!(sink.flush().await.is_ok());
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_gzip() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.gz").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).compression(CompressionConfig::Auto));

        let records = vec![
            json!({"id": "1", "name": "Alice"}),
            json!({"id": "2", "name": "Bob"}),
        ];
        sink.write_batch(&records).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use std::io::Read;
        let mut r =
            faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Gzip);
        let mut text = String::new();
        r.read_to_string(&mut text).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        // Header + 2 rows.
        assert_eq!(lines.len(), 3);
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_zstd() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.zst").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).compression(CompressionConfig::Auto));

        sink.write_batch(&[json!({"x": "42"})]).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use std::io::Read;
        let mut r =
            faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Zstd);
        let mut text = String::new();
        r.read_to_string(&mut text).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        // Header + 1 row.
        assert_eq!(lines.len(), 2);
    }

    #[tokio::test]
    async fn write_flush_write_does_not_truncate() {
        // Regression: flush() clears the writer; the next write_batch
        // must reopen in append mode regardless of config.append (which
        // defaults to false). Without the opened_once guard, the second
        // open would truncate and lose the first batch's records.
        let tmp = NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path));

        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": "2"})]).await.unwrap();
        sink.flush().await.unwrap();

        let content = tokio::fs::read_to_string(&path).await.unwrap();
        let lines: Vec<&str> = content.trim().split('\n').collect();
        // Header + 2 data rows (header is written only on the first open).
        assert_eq!(
            lines.len(),
            3,
            "both batches must survive the mid-stream flush"
        );
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn write_flush_write_produces_multi_member_gzip_csv() {
        // With compression, flush() finalises one gzip member; the
        // next write_batch starts a fresh member appended after it.
        // The decoder reads both members back correctly.
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.gz").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let sink = CsvSink::new(CsvSinkConfig::new(&path).compression(CompressionConfig::Auto));
        sink.write_batch(&[json!({"id": "1"})]).await.unwrap();
        sink.flush().await.unwrap();
        sink.write_batch(&[json!({"id": "2"})]).await.unwrap();
        sink.flush().await.unwrap();

        let bytes = tokio::fs::read(&path).await.unwrap();
        use std::io::Read;
        let mut r =
            faucet_core::compression::wrap_sync_reader(&bytes[..], faucet_core::Compression::Gzip);
        let mut text = String::new();
        r.read_to_string(&mut text).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        // Header (from first open) + 2 data rows. The re-open uses
        // append=true so no second header is written.
        assert_eq!(lines.len(), 3);
    }
}
