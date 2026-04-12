//! CSV file source.

use crate::config::CsvSourceConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::{Map, Value};

/// A source that reads records from a CSV file.
///
/// Each row is returned as a JSON object. If the file has headers, the header
/// names are used as keys. Otherwise, generated names (`column_0`, `column_1`,
/// etc.) are used.
pub struct CsvSource {
    config: CsvSourceConfig,
}

impl CsvSource {
    /// Create a new CSV source from the given configuration.
    pub fn new(config: CsvSourceConfig) -> Self {
        Self { config }
    }
}

#[async_trait]
impl faucet_core::Source for CsvSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let mut config = self.config.clone();

        if !context.is_empty() {
            config.path = faucet_core::util::substitute_context(&config.path, context);
        }

        // CSV reading is synchronous I/O — run on a blocking thread to avoid
        // starving the async runtime.
        tokio::task::spawn_blocking(move || read_csv(&config))
            .await
            .map_err(|e| FaucetError::Config(format!("CSV read task failed: {e}")))?
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(CsvSourceConfig))
            .expect("schema serialization")
    }
}

/// Synchronous CSV reading logic.
fn read_csv(config: &CsvSourceConfig) -> Result<Vec<Value>, FaucetError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(config.has_headers)
        .delimiter(config.delimiter)
        .quote(config.quote)
        .from_path(&config.path)
        .map_err(|e| {
            FaucetError::Config(format!("failed to open CSV file '{}': {e}", config.path))
        })?;

    // Determine column names.
    let headers: Vec<String> = if config.has_headers {
        reader
            .headers()
            .map_err(|e| FaucetError::Config(format!("failed to read CSV headers: {e}")))?
            .iter()
            .map(|h| h.to_string())
            .collect()
    } else {
        Vec::new()
    };

    let mut records = Vec::new();

    for (row_idx, result) in reader.records().enumerate() {
        let record = result.map_err(|e| {
            FaucetError::Config(format!("CSV parse error at row {}: {e}", row_idx + 1))
        })?;

        let mut obj = Map::new();

        for (col_idx, field) in record.iter().enumerate() {
            let key = if col_idx < headers.len() {
                headers[col_idx].clone()
            } else {
                format!("column_{col_idx}")
            };
            obj.insert(key, Value::String(field.to_string()));
        }

        records.push(Value::Object(obj));
    }

    tracing::debug!(records = records.len(), path = %config.path, "CSV file read complete");

    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn reads_csv_with_headers() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name,age").unwrap();
        writeln!(tmp, "1,Alice,30").unwrap();
        writeln!(tmp, "2,Bob,25").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], "1");
        assert_eq!(records[0]["name"], "Alice");
        assert_eq!(records[0]["age"], "30");
        assert_eq!(records[1]["name"], "Bob");
    }

    #[tokio::test]
    async fn reads_csv_without_headers() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "Alice,30").unwrap();
        writeln!(tmp, "Bob,25").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).has_headers(false);
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["column_0"], "Alice");
        assert_eq!(records[0]["column_1"], "30");
    }

    #[tokio::test]
    async fn reads_tsv_with_custom_delimiter() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id\tname").unwrap();
        writeln!(tmp, "1\tAlice").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap()).delimiter(b'\t');
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["id"], "1");
        assert_eq!(records[0]["name"], "Alice");
    }

    #[tokio::test]
    async fn empty_csv_returns_empty_vec() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "id,name").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn missing_file_returns_error() {
        let config = CsvSourceConfig::new("/nonexistent/path/data.csv");
        let source = CsvSource::new(config);
        let result = source.fetch_all().await;

        assert!(result.is_err());
    }
}
