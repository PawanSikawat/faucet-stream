//! JSON Lines sink configuration.

use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Configuration for the JSON Lines file sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonlSinkConfig {
    /// Path to the output file.
    pub path: PathBuf,
    /// Whether to append to an existing file (default: false, truncates).
    #[serde(default)]
    pub append: bool,
    /// Whether to pretty-print each JSON record (default: false, compact).
    #[serde(default)]
    pub pretty: bool,
}

impl JsonlSinkConfig {
    /// Create a new config with the required file path and sensible defaults.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            append: false,
            pretty: false,
        }
    }

    /// Append to an existing file instead of truncating.
    pub fn append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }

    /// Pretty-print each JSON record (one record still per logical entry,
    /// but with indentation). Note: this breaks strict JSONL format.
    pub fn pretty(mut self, pretty: bool) -> Self {
        self.pretty = pretty;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let config = JsonlSinkConfig::new("/tmp/out.jsonl");
        assert_eq!(config.path, PathBuf::from("/tmp/out.jsonl"));
        assert!(!config.append);
        assert!(!config.pretty);
    }

    #[test]
    fn builder_methods() {
        let config = JsonlSinkConfig::new("/tmp/out.jsonl")
            .append(true)
            .pretty(true);
        assert!(config.append);
        assert!(config.pretty);
    }
}
