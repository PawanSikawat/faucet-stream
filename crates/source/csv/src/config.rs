//! CSV source configuration.

use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the CSV file source.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CsvSourceConfig {
    /// Path to the CSV file.
    pub path: String,
    /// Whether the file has a header row. Defaults to `true`.
    #[serde(default = "default_true")]
    pub has_headers: bool,
    /// Field delimiter byte. Defaults to `b','`.
    #[serde(default = "default_delimiter")]
    pub delimiter: u8,
    /// Quote character byte. Defaults to `b'"'`.
    #[serde(default = "default_quote")]
    pub quote: u8,
    /// Records per emitted [`StreamPage`](faucet_core::StreamPage). Rows are
    /// parsed line-by-line from a tokio `BufReader` and yielded whenever the
    /// buffer reaches this size. Defaults to [`DEFAULT_BATCH_SIZE`].
    ///
    /// `batch_size = 0` is the "no batching" sentinel: the file is fully
    /// drained and the entire result set is emitted in a single page. Useful
    /// for small lookup tables or for sinks (e.g. SQL `COPY`, BigQuery load
    /// jobs) that prefer one large request to many small ones.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_true() -> bool {
    true
}

fn default_delimiter() -> u8 {
    b','
}

fn default_quote() -> u8 {
    b'"'
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl CsvSourceConfig {
    /// Create a new config with the required file path and sensible defaults.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            has_headers: true,
            delimiter: b',',
            quote: b'"',
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set whether the file has a header row.
    pub fn has_headers(mut self, v: bool) -> Self {
        self.has_headers = v;
        self
    }

    /// Set the field delimiter byte.
    pub fn delimiter(mut self, d: u8) -> Self {
        self.delimiter = d;
        self
    }

    /// Set the quote character byte.
    pub fn quote(mut self, q: u8) -> Self {
        self.quote = q;
        self
    }

    /// Set the per-page row count for [`Source::stream_pages`](faucet_core::Source::stream_pages).
    ///
    /// Pass `0` to opt out of batching — the entire file is emitted in a
    /// single [`StreamPage`](faucet_core::StreamPage).
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = CsvSourceConfig::new("/tmp/data.csv");
        assert_eq!(config.path, "/tmp/data.csv");
        assert!(config.has_headers);
        assert_eq!(config.delimiter, b',');
        assert_eq!(config.quote, b'"');
    }

    #[test]
    fn builder_methods() {
        let config = CsvSourceConfig::new("/tmp/data.tsv")
            .has_headers(false)
            .delimiter(b'\t')
            .quote(b'\'');
        assert!(!config.has_headers);
        assert_eq!(config.delimiter, b'\t');
        assert_eq!(config.quote, b'\'');
    }

    #[test]
    fn batch_size_defaults_to_default_batch_size() {
        let config = CsvSourceConfig::new("/tmp/data.csv");
        assert_eq!(config.batch_size, faucet_core::DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn with_batch_size_overrides_default() {
        let config = CsvSourceConfig::new("/tmp/data.csv").with_batch_size(500);
        assert_eq!(config.batch_size, 500);
    }

    #[test]
    fn batch_size_zero_is_accepted_as_no_batching_sentinel() {
        let config = CsvSourceConfig::new("/tmp/data.csv").with_batch_size(0);
        assert_eq!(config.batch_size, 0);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_ok());
    }

    #[test]
    fn batch_size_above_max_is_rejected_by_validate_batch_size() {
        let config =
            CsvSourceConfig::new("/tmp/data.csv").with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(faucet_core::validate_batch_size(config.batch_size).is_err());
    }

    #[test]
    fn batch_size_deserializes_from_json() {
        let json = r#"{
            "path": "/tmp/data.csv",
            "batch_size": 250
        }"#;
        let config: CsvSourceConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.batch_size, 250);
    }
}
