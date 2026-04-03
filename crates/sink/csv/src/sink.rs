//! CSV file sink.

use crate::config::CsvSinkConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use std::fs::{File, OpenOptions};
use std::sync::Mutex;

/// State for the CSV writer, including the determined column order.
struct WriterState {
    writer: csv::Writer<File>,
    columns: Vec<String>,
}

/// A sink that writes JSON records to a CSV file.
///
/// Column order is determined from the keys of the first record in the first
/// `write_batch` call. Subsequent records use the same column order; missing
/// fields are written as empty strings.
pub struct CsvSink {
    config: CsvSinkConfig,
    state: Mutex<Option<WriterState>>,
}

impl CsvSink {
    /// Create a new CSV sink. The file is opened on the first `write_batch` call.
    pub fn new(config: CsvSinkConfig) -> Self {
        Self {
            config,
            state: Mutex::new(None),
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

        let result = tokio::task::spawn_blocking(move || {
            write_csv_blocking(config, current_state, &records)
        })
        .await
        .map_err(|e| FaucetError::Sink(format!("CSV write task failed: {e}")))?;

        let (new_state, count) = result?;

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
        let mut guard = self
            .state
            .lock()
            .map_err(|e| FaucetError::Sink(format!("CSV sink lock poisoned: {e}")))?;
        if let Some(ref mut state) = *guard {
            state
                .writer
                .flush()
                .map_err(|e| FaucetError::Sink(format!("CSV flush failed: {e}")))?;
        }
        Ok(())
    }
}

/// Synchronous CSV writing logic, run inside `spawn_blocking`.
fn write_csv_blocking(
    config: CsvSinkConfig,
    existing_state: Option<WriterState>,
    records: &[Value],
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

            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .append(config.append)
                .truncate(!config.append)
                .open(&config.path)
                .map_err(|e| {
                    FaucetError::Sink(format!("failed to open CSV file '{}': {e}", config.path))
                })?;

            let mut writer = csv::WriterBuilder::new()
                .delimiter(config.delimiter)
                .from_writer(file);

            // Write header row if configured.
            if config.write_headers && !config.append {
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
}
