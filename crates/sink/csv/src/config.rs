//! CSV sink configuration.

/// Configuration for the CSV file sink.
#[derive(Debug, Clone)]
pub struct CsvSinkConfig {
    /// Path to the output CSV file.
    pub path: String,
    /// Field delimiter byte. Defaults to `b','`.
    pub delimiter: u8,
    /// Whether to write a header row. Defaults to `true`.
    pub write_headers: bool,
    /// Whether to append to an existing file. Defaults to `false` (truncates).
    pub append: bool,
}

impl CsvSinkConfig {
    /// Create a new config with the required file path and sensible defaults.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            delimiter: b',',
            write_headers: true,
            append: false,
        }
    }

    /// Set the field delimiter byte.
    pub fn delimiter(mut self, d: u8) -> Self {
        self.delimiter = d;
        self
    }

    /// Set whether to write a header row.
    pub fn write_headers(mut self, v: bool) -> Self {
        self.write_headers = v;
        self
    }

    /// Set whether to append to an existing file.
    pub fn append(mut self, v: bool) -> Self {
        self.append = v;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = CsvSinkConfig::new("/tmp/out.csv");
        assert_eq!(config.path, "/tmp/out.csv");
        assert_eq!(config.delimiter, b',');
        assert!(config.write_headers);
        assert!(!config.append);
    }

    #[test]
    fn builder_methods() {
        let config = CsvSinkConfig::new("/tmp/out.tsv")
            .delimiter(b'\t')
            .write_headers(false)
            .append(true);
        assert_eq!(config.delimiter, b'\t');
        assert!(!config.write_headers);
        assert!(config.append);
    }
}
