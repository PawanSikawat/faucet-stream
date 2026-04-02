//! CSV source configuration.

use serde::{Deserialize, Serialize};

/// Configuration for the CSV file source.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl CsvSourceConfig {
    /// Create a new config with the required file path and sensible defaults.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            has_headers: true,
            delimiter: b',',
            quote: b'"',
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
}
