//! CSV file source.

use crate::config::CsvSourceConfig;
use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use serde_json::{Map, Value};
use std::pin::Pin;
use tokio::io::AsyncBufReadExt;

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

    /// Stream rows from the CSV file without buffering the full result set.
    ///
    /// The file is opened via [`tokio::fs::File`] + [`tokio::io::BufReader`]
    /// and consumed line-by-line via [`AsyncBufReadExt::lines`]. Each line is
    /// parsed as a single CSV record using `csv::ReaderBuilder::from_reader`
    /// so quoted fields containing the delimiter (e.g. `"hello, world"`) are
    /// handled correctly. Each emitted [`StreamPage`] holds up to
    /// [`CsvSourceConfig::batch_size`] rows.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README documents,
    /// and routing the pipeline-supplied hint through it would silently
    /// override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire file into a single page. The CSV
    /// source has no incremental-replication mode, so every emitted page
    /// carries `bookmark: None`.
    ///
    /// ## Multi-line records
    ///
    /// `AsyncBufReadExt::lines` splits on raw `\n`, so quoted fields that
    /// contain embedded newlines are **not** supported by the streaming path
    /// — they will be parsed as broken individual lines. Use `fetch_all` /
    /// `fetch_with_context` for files that need multi-line records.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let mut config = self.config.clone();
            if !context.is_empty() {
                config.path = faucet_core::util::substitute_context(&config.path, context);
            }

            let file = tokio::fs::File::open(&config.path).await.map_err(|e| {
                FaucetError::Config(format!(
                    "failed to open CSV file '{}': {e}",
                    config.path
                ))
            })?;
            let reader = tokio::io::BufReader::new(file);
            let mut lines = reader.lines();

            // Read the header line first if configured, so we can key records
            // by header name. Skip blank leading lines (defensive — most CSVs
            // start with content).
            let headers: Vec<String> = if config.has_headers {
                match next_nonempty_line(&mut lines).await? {
                    Some(line) => parse_csv_line(&line, &config)?,
                    None => Vec::new(),
                }
            } else {
                Vec::new()
            };

            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;
            let mut row_idx = 0usize;

            while let Some(line) = lines.next_line().await.map_err(|e| {
                FaucetError::Config(format!(
                    "CSV read error in '{}': {e}",
                    config.path
                ))
            })? {
                // Skip stray blank lines so a trailing newline at EOF does
                // not produce a phantom empty record.
                if line.is_empty() {
                    continue;
                }

                let fields = parse_csv_line(&line, &config).map_err(|e| match e {
                    FaucetError::Config(msg) => FaucetError::Config(format!(
                        "CSV parse error at row {}: {msg}",
                        row_idx + 1
                    )),
                    other => other,
                })?;

                let mut obj = Map::new();
                for (col_idx, field) in fields.into_iter().enumerate() {
                    let key = if col_idx < headers.len() {
                        headers[col_idx].clone()
                    } else {
                        format!("column_{col_idx}")
                    };
                    obj.insert(key, Value::String(field));
                }
                buffer.push(Value::Object(obj));
                row_idx += 1;

                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(initial_capacity));
                    total += page.len();
                    yield StreamPage { records: page, bookmark: None };
                }
            }

            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                rows = total,
                batch_size,
                path = %config.path,
                "CSV source stream complete",
            );
        })
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(CsvSourceConfig))
            .expect("schema serialization")
    }
}

/// Pull the next non-empty line from the async line iterator. Returns `None`
/// when EOF is reached without finding one.
async fn next_nonempty_line<R>(
    lines: &mut tokio::io::Lines<R>,
) -> Result<Option<String>, FaucetError>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    loop {
        match lines
            .next_line()
            .await
            .map_err(|e| FaucetError::Config(format!("CSV read error: {e}")))?
        {
            Some(line) if line.is_empty() => continue,
            Some(line) => return Ok(Some(line)),
            None => return Ok(None),
        }
    }
}

/// Parse a single CSV line into a vector of field strings using a per-line
/// `csv::ReaderBuilder` so quoting/escaping rules are honoured.
fn parse_csv_line(line: &str, config: &CsvSourceConfig) -> Result<Vec<String>, FaucetError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(config.delimiter)
        .quote(config.quote)
        // A single line cannot contain a flexible-width record set, but
        // tolerating uneven field counts matches the lenient behaviour of
        // the spawn_blocking path.
        .flexible(true)
        .from_reader(line.as_bytes());

    let mut iter = reader.records();
    match iter.next() {
        Some(Ok(record)) => Ok(record.iter().map(|f| f.to_string()).collect()),
        Some(Err(e)) => Err(FaucetError::Config(format!("CSV parse error: {e}"))),
        None => Ok(Vec::new()),
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
