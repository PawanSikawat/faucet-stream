//! CSV file source.

use crate::config::CsvSourceConfig;
use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use serde_json::{Map, Value};
use std::pin::Pin;

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
        use futures::StreamExt;
        let mut all = Vec::new();
        let mut s = self.stream_pages(context, self.config.batch_size);
        while let Some(page) = s.next().await {
            let page = page?;
            all.extend(page.records);
        }
        Ok(all)
    }

    /// Stream rows from the CSV file without buffering the full result set.
    ///
    /// The file is opened via [`tokio::fs::File`] and parsed by `csv-async`,
    /// a streaming RFC-4180 reader. Because it tracks quote state across
    /// physical lines, **quoted fields containing embedded newlines are parsed
    /// correctly** — the file written by `faucet-sink-csv` round-trips back
    /// losslessly (#78/#40). Each emitted [`StreamPage`] holds up to
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
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            use futures::StreamExt as _;

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
            #[cfg(feature = "compression")]
            let reader = {
                let codec = config.compression.resolve(&config.path);
                faucet_core::compression::warn_mismatch(&config.path, codec);
                faucet_core::compression::wrap_async_reader(reader, codec)
            };

            // `csv-async` tracks quote state across physical lines, so a
            // record whose field contains an embedded newline is read as one
            // record. Headers are handled manually below, so the underlying
            // reader is configured header-less.
            let mut csv_reader = csv_async::AsyncReaderBuilder::new()
                .has_headers(false)
                .delimiter(config.delimiter)
                .quote(config.quote)
                // Tolerate uneven field counts (the connector has always been
                // lenient — uneven rows must not abort the run).
                .flexible(true)
                .create_reader(reader);

            let mut records = csv_reader.records();

            // The first record is the header row when configured.
            let headers: Vec<String> = if config.has_headers {
                match records.next().await {
                    Some(rec) => {
                        let rec = rec.map_err(|e| FaucetError::Config(format!(
                            "CSV header parse error in '{}': {e}", config.path
                        )))?;
                        rec.iter().map(|f| f.to_string()).collect()
                    }
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

            while let Some(rec) = records.next().await {
                let record = rec.map_err(|e| FaucetError::Config(format!(
                    "CSV parse error at row {} in '{}': {e}",
                    row_idx + 1,
                    config.path
                )))?;

                let mut obj = Map::new();
                for (col_idx, field) in record.iter().enumerate() {
                    let key = if col_idx < headers.len() {
                        headers[col_idx].clone()
                    } else {
                        format!("column_{col_idx}")
                    };
                    obj.insert(key, Value::String(field.to_string()));
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
    async fn reads_quoted_field_with_embedded_newline() {
        // Regression for #78/#40: a quoted field spanning multiple physical
        // lines (as the CSV sink emits) must round-trip as one record.
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "id,note\n1,\"line one\nline two\"\n2,\"plain\"\n").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], "1");
        assert_eq!(records[0]["note"], "line one\nline two");
        assert_eq!(records[1]["note"], "plain");
    }

    #[tokio::test]
    async fn reads_quoted_field_with_embedded_delimiter() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "id,name\n1,\"Doe, John\"\n").unwrap();
        tmp.flush().unwrap();

        let config = CsvSourceConfig::new(tmp.path().to_str().unwrap());
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["name"], "Doe, John");
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

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_gzip_via_stream_pages() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.gz").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let plain = b"id,name\n1,Alice\n2,Bob\n";
        let compressed =
            faucet_core::compression::compress_buf(plain, faucet_core::Compression::Gzip).unwrap();
        tokio::fs::write(&path, &compressed).await.unwrap();

        let config = CsvSourceConfig::new(&path).compression(CompressionConfig::Auto);
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["name"], "Alice");
        assert_eq!(records[1]["name"], "Bob");
    }

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn roundtrip_zstd_via_stream_pages() {
        use faucet_core::CompressionConfig;
        let tmp = NamedTempFile::with_suffix(".csv.zst").unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        let plain = b"id,name\n1,Carol\n";
        let compressed =
            faucet_core::compression::compress_buf(plain, faucet_core::Compression::Zstd).unwrap();
        tokio::fs::write(&path, &compressed).await.unwrap();

        let config = CsvSourceConfig::new(&path).compression(CompressionConfig::Auto);
        let source = CsvSource::new(config);
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["name"], "Carol");
    }
}
