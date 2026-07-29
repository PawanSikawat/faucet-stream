//! SFTP sink executor.
//!
//! Writes each `write_batch` chunk as a JSON Lines object under a remote
//! directory. Writes are **atomic**: each object is uploaded to a hidden
//! temporary name and then renamed to its final name, so a consumer watching
//! the directory never observes a partially-written file. Construction is lazy
//! — [`SftpSink::new`] performs no I/O; the SSH transport is opened on the
//! first `write_batch` and reused for the life of the sink.

use crate::config::SftpSinkConfig;
use async_trait::async_trait;
use faucet_common_sftp::{SftpSession, connect};
use faucet_core::FaucetError;
use russh_sftp::protocol::OpenFlags;
use serde_json::Value;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// A sink that writes JSON records to an SFTP server as JSON Lines objects.
pub struct SftpSink {
    config: SftpSinkConfig,
    /// Reused SFTP session, opened on first write. Behind a `Mutex` so writes
    /// share one SSH connection instead of reconnecting per page.
    session: Mutex<Option<SftpSession>>,
}

impl SftpSink {
    /// Create a new SFTP sink. Lazy: performs no I/O and never connects here.
    /// The batch size is validated up front so a bad config fails fast.
    pub fn new(config: SftpSinkConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;
        Ok(Self {
            config,
            session: Mutex::new(None),
        })
    }

    /// Serialize a slice of records as JSON Lines bytes.
    fn serialize_jsonl(records: &[Value]) -> Result<Vec<u8>, FaucetError> {
        let mut buf: Vec<u8> = Vec::new();
        for record in records {
            let line = serde_json::to_vec(record)
                .map_err(|e| FaucetError::Sink(format!("JSON serialization failed: {e}")))?;
            buf.extend_from_slice(&line);
            buf.push(b'\n');
        }
        Ok(buf)
    }

    /// Join the configured directory prefix with a file name using POSIX `/`.
    fn join_path(&self, name: &str) -> String {
        let dir = &self.config.path;
        if dir.is_empty() {
            name.to_string()
        } else if dir.ends_with('/') {
            format!("{dir}{name}")
        } else {
            format!("{dir}/{name}")
        }
    }

    /// Generate the final object key: `{path}/{uuid}{ext}`.
    fn final_key(&self) -> String {
        let id = uuid::Uuid::new_v4();
        self.join_path(&format!("{id}{}", self.config.file_extension))
    }

    /// Upload `body` to `final_key` atomically: write a temporary object and
    /// rename it into place, so consumers never see a partial file.
    async fn upload_atomic(
        sftp: &SftpSession,
        final_key: &str,
        body: &[u8],
    ) -> Result<(), FaucetError> {
        let temp_key = format!("{final_key}.tmp");

        let mut file = sftp
            .open_with_flags(
                temp_key.as_str(),
                OpenFlags::CREATE | OpenFlags::WRITE | OpenFlags::TRUNCATE,
            )
            .await
            .map_err(|e| {
                FaucetError::Sink(format!("SFTP open '{temp_key}' for write failed: {e}"))
            })?;

        file.write_all(body)
            .await
            .map_err(|e| FaucetError::Sink(format!("SFTP write to '{temp_key}' failed: {e}")))?;
        file.flush()
            .await
            .map_err(|e| FaucetError::Sink(format!("SFTP flush of '{temp_key}' failed: {e}")))?;
        file.shutdown()
            .await
            .map_err(|e| FaucetError::Sink(format!("SFTP close of '{temp_key}' failed: {e}")))?;

        sftp.rename(temp_key.as_str(), final_key)
            .await
            .map_err(|e| {
                FaucetError::Sink(format!(
                    "SFTP rename '{temp_key}' -> '{final_key}' failed: {e}"
                ))
            })?;

        tracing::debug!(key = %final_key, "wrote SFTP object");
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for SftpSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut guard = self.session.lock().await;
        if guard.is_none() {
            let sftp = connect(&self.config.connection).await?;
            // Best-effort: ensure the target directory exists. Ignore errors
            // (it usually already exists; a real permission/path problem
            // surfaces on the first write with a clear message).
            if let Err(e) = sftp.create_dir(self.config.path.as_str()).await {
                tracing::debug!(path = %self.config.path, error = %e, "SFTP create_dir (best-effort)");
            }
            *guard = Some(sftp);
        }
        let sftp = guard.as_ref().expect("session initialized above");

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let files = chunks.len();
        for chunk in chunks {
            let body = Self::serialize_jsonl(chunk)?;
            let key = self.final_key();
            Self::upload_atomic(sftp, &key, &body).await?;
        }

        tracing::info!(records = records.len(), files, "SFTP batch write complete");
        Ok(records.len())
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(SftpSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "sftp"
    }

    fn dataset_uri(&self) -> String {
        format!(
            "sftp://{}:{}/{}",
            self.config.connection.host,
            self.config.connection.port,
            self.config.path.trim_start_matches('/')
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_common_sftp::SftpConnectionConfig;
    use faucet_core::Sink;

    fn cfg() -> SftpSinkConfig {
        SftpSinkConfig::new(SftpConnectionConfig::with_password("h", "u", "p"), "/out")
    }

    #[test]
    fn new_is_lazy_and_validates_batch_size() {
        assert!(SftpSink::new(cfg()).is_ok());
        let bad = cfg().with_batch_size(faucet_core::MAX_BATCH_SIZE + 1);
        assert!(matches!(SftpSink::new(bad), Err(FaucetError::Config(_))));
    }

    #[test]
    fn serialize_jsonl_is_newline_delimited() {
        let records = vec![
            serde_json::json!({"id": 1, "name": "Alice"}),
            serde_json::json!({"id": 2, "name": "Bob"}),
        ];
        let bytes = SftpSink::serialize_jsonl(&records).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let lines: Vec<&str> = text.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);
        let first: Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["id"], 1);
    }

    #[test]
    fn serialize_jsonl_empty() {
        assert!(SftpSink::serialize_jsonl(&[]).unwrap().is_empty());
    }

    #[test]
    fn join_path_handles_trailing_slash() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert_eq!(sink.join_path("f.jsonl"), "/out/f.jsonl");

        let sink2 = SftpSink::new(SftpSinkConfig::new(
            SftpConnectionConfig::with_password("h", "u", "p"),
            "/out/",
        ))
        .unwrap();
        assert_eq!(sink2.join_path("f.jsonl"), "/out/f.jsonl");
    }

    #[test]
    fn final_key_uses_prefix_and_extension() {
        let sink = SftpSink::new(cfg()).unwrap();
        let key = sink.final_key();
        assert!(key.starts_with("/out/"), "got {key}");
        assert!(key.ends_with(".jsonl"), "got {key}");
    }

    #[test]
    fn connector_name_is_sftp() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert_eq!(sink.connector_name(), "sftp");
    }

    #[test]
    fn dataset_uri_has_no_credentials() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert_eq!(sink.dataset_uri(), "sftp://h:22/out");
    }

    #[test]
    fn append_only_capabilities() {
        let sink = SftpSink::new(cfg()).unwrap();
        assert!(!sink.supports_idempotent_writes());
        assert!(!sink.dedups_by_key());
        assert!(
            sink.supported_write_modes()
                .contains(&faucet_core::write_mode::WriteMode::Append)
        );
    }
}
