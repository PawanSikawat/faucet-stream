//! File transport — append each event as one JSON line.

use super::Transport;
use async_trait::async_trait;
use faucet_core::FaucetError;
use std::path::PathBuf;
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

pub struct FileTransport {
    path: PathBuf,
    lock: Mutex<()>,
}

impl FileTransport {
    pub fn new(path: PathBuf) -> Self {
        Self { path, lock: Mutex::new(()) }
    }
}

#[async_trait]
impl Transport for FileTransport {
    async fn send(&self, event_json: Vec<u8>) -> Result<(), FaucetError> {
        let _g = self.lock.lock().await;
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| FaucetError::Custom(Box::new(e)))?;
        }
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|e| FaucetError::Custom(Box::new(e)))?;
        let mut line = event_json;
        line.push(b'\n');
        f.write_all(&line).await.map_err(|e| FaucetError::Custom(Box::new(e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn appends_one_json_line_per_event() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ol.jsonl");
        let t = FileTransport::new(path.clone());
        t.send(b"{\"a\":1}".to_vec()).await.unwrap();
        t.send(b"{\"a\":2}".to_vec()).await.unwrap();
        let body = std::fs::read_to_string(&path).unwrap();
        assert_eq!(body.lines().count(), 2);
        assert_eq!(body.lines().next().unwrap(), "{\"a\":1}");
    }

    #[tokio::test]
    async fn creates_parent_dirs() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nested/deep/ol.jsonl");
        FileTransport::new(path.clone()).send(b"{}".to_vec()).await.unwrap();
        assert!(path.exists());
    }
}
