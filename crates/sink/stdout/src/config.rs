//! Stdout/stderr sink configuration.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Which standard stream to write records to.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum StdStream {
    /// Standard output (default). Honors shell redirection.
    #[default]
    Stdout,
    /// Standard error. Useful when stdout is reserved for piping pipeline output.
    Stderr,
}

/// How each record should be serialized before writing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StdoutFormat {
    /// One compact JSON object per line (default — matches JSONL format).
    #[default]
    JsonLines,
    /// Indented JSON, separated by newlines. Easier to read but not a single-line format.
    PrettyJson,
    /// Tab-separated values, with each record's keys sorted alphabetically.
    /// Scalars are emitted as-is; nested objects/arrays are emitted as compact JSON.
    Tsv,
}

/// Configuration for the stdout/stderr sink.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct StdoutSinkConfig {
    /// Which standard stream to write to.
    #[serde(default)]
    pub destination: StdStream,
    /// Output format.
    #[serde(default)]
    pub format: StdoutFormat,
    /// Flush the underlying writer after every record instead of at batch boundaries.
    /// Tradeoff: lower latency for live preview, slightly lower throughput.
    #[serde(default)]
    pub flush_per_record: bool,
    /// Stop writing after this many records have been emitted. Subsequent
    /// `write_batch` calls become no-ops. `None` means unlimited.
    #[serde(default)]
    pub max_records: Option<usize>,
}

impl StdoutSinkConfig {
    /// Create a new config with all defaults (stdout, JSON Lines, no limit).
    pub fn new() -> Self {
        Self::default()
    }

    /// Send records to the given standard stream.
    pub fn destination(mut self, destination: StdStream) -> Self {
        self.destination = destination;
        self
    }

    /// Choose the output format.
    pub fn format(mut self, format: StdoutFormat) -> Self {
        self.format = format;
        self
    }

    /// Flush after every record.
    pub fn flush_per_record(mut self, flush_per_record: bool) -> Self {
        self.flush_per_record = flush_per_record;
        self
    }

    /// Stop after writing `n` records total.
    pub fn max_records(mut self, max_records: usize) -> Self {
        self.max_records = Some(max_records);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults() {
        let c = StdoutSinkConfig::new();
        assert_eq!(c.destination, StdStream::Stdout);
        assert_eq!(c.format, StdoutFormat::JsonLines);
        assert!(!c.flush_per_record);
        assert!(c.max_records.is_none());
    }

    #[test]
    fn builder_chains() {
        let c = StdoutSinkConfig::new()
            .destination(StdStream::Stderr)
            .format(StdoutFormat::PrettyJson)
            .flush_per_record(true)
            .max_records(10);
        assert_eq!(c.destination, StdStream::Stderr);
        assert_eq!(c.format, StdoutFormat::PrettyJson);
        assert!(c.flush_per_record);
        assert_eq!(c.max_records, Some(10));
    }

    #[test]
    fn serde_round_trip() {
        let c = StdoutSinkConfig::new()
            .destination(StdStream::Stderr)
            .format(StdoutFormat::Tsv);
        let json = serde_json::to_string(&c).unwrap();
        let back: StdoutSinkConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.destination, StdStream::Stderr);
        assert_eq!(back.format, StdoutFormat::Tsv);
    }

    #[test]
    fn deserialize_from_minimal_json() {
        let c: StdoutSinkConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(c.destination, StdStream::Stdout);
        assert_eq!(c.format, StdoutFormat::JsonLines);
    }
}
