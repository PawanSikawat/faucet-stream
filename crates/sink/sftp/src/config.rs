//! SFTP sink configuration.

use faucet_common_sftp::SftpConnectionConfig;
use faucet_core::DEFAULT_BATCH_SIZE;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Configuration for the SFTP sink connector.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SftpSinkConfig {
    /// Shared SFTP connection settings (host, port, username, auth, host-key
    /// policy). Flattened, so its fields sit at the top level of the config.
    #[serde(flatten)]
    pub connection: SftpConnectionConfig,
    /// Remote directory prefix under which JSON Lines objects are written.
    pub path: String,
    /// File extension for written objects (default: `.jsonl`).
    #[serde(default = "default_file_extension")]
    pub file_extension: String,
    /// Records per written object. When a `write_batch` call hands the sink
    /// `N` records with `batch_size = M > 0`, the sink writes `ceil(N / M)`
    /// objects. `batch_size = 0` writes whatever upstream hands it as a single
    /// object. Defaults to [`DEFAULT_BATCH_SIZE`].
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_file_extension() -> String {
    ".jsonl".to_string()
}

fn default_batch_size() -> usize {
    DEFAULT_BATCH_SIZE
}

impl SftpSinkConfig {
    /// Build a sink config from a connection and a remote directory prefix.
    pub fn new(connection: SftpConnectionConfig, path: impl Into<String>) -> Self {
        Self {
            connection,
            path: path.into(),
            file_extension: default_file_extension(),
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }

    /// Set the file extension for written objects.
    pub fn file_extension(mut self, ext: impl Into<String>) -> Self {
        self.file_extension = ext.into();
        self
    }

    /// Set the per-object record count.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_sftp::SftpConnectionConfig;

    fn conn() -> SftpConnectionConfig {
        SftpConnectionConfig::with_password("h", "u", "p")
    }

    #[test]
    fn defaults() {
        let cfg = SftpSinkConfig::new(conn(), "/out");
        assert_eq!(cfg.path, "/out");
        assert_eq!(cfg.file_extension, ".jsonl");
        assert_eq!(cfg.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn deserializes_flat_shape() {
        let json = r#"{
            "host": "sftp.example.com",
            "username": "user",
            "type": "password",
            "config": { "password": "secret" },
            "path": "/upload",
            "batch_size": 0
        }"#;
        let cfg: SftpSinkConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.connection.host, "sftp.example.com");
        assert_eq!(cfg.path, "/upload");
        assert_eq!(cfg.batch_size, 0);
        assert_eq!(cfg.file_extension, ".jsonl");
    }

    #[test]
    fn batch_size_zero_is_valid_sentinel() {
        let cfg = SftpSinkConfig::new(conn(), "/o").with_batch_size(0);
        assert!(faucet_core::validate_batch_size(cfg.batch_size).is_ok());
    }
}
