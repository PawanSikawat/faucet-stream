//! Pluggable state store for incremental replication bookmarks.
//!
//! Sources that support incremental replication need to remember where they
//! left off across runs. This module defines the [`StateStore`] trait that
//! the pipeline orchestrator uses to read and persist that progress, plus two
//! ready-to-use implementations:
//!
//! - [`MemoryStateStore`] — in-process map. Useful for tests and for runs
//!   that intentionally start fresh each time.
//! - [`FileStateStore`] — one JSON file per key, written via atomic rename
//!   so a crash mid-update never leaves the bookmark torn.
//!
//! Heavier backends (Redis, PostgreSQL) live in their own crates so
//! `faucet-core` stays dependency-light. They implement the same trait.

use crate::error::FaucetError;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

/// Persistent key/value store for replication bookmarks and pipeline checkpoints.
///
/// Implementations must be safe to call from multiple tasks at once. The
/// trait is intentionally minimal — three operations cover every bookmark
/// flow the pipeline orchestrator needs.
#[async_trait]
pub trait StateStore: Send + Sync {
    /// Return the value stored under `key`, or `None` if no entry exists.
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError>;

    /// Store `value` under `key`, replacing any previous entry.
    ///
    /// Implementations should make the update durable before returning so a
    /// crash immediately after `put` does not lose the bookmark.
    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError>;

    /// Remove the entry for `key`. A missing key is not an error.
    async fn delete(&self, key: &str) -> Result<(), FaucetError>;
}

/// Reject keys that could escape the storage namespace or break filename
/// rules on common filesystems. Allowed: ASCII letters, digits, `_`, `-`,
/// `:`, `.`. Empty keys are rejected.
pub fn validate_state_key(key: &str) -> Result<(), FaucetError> {
    if key.is_empty() {
        return Err(FaucetError::State("state key must not be empty".into()));
    }
    if key.len() > 256 {
        return Err(FaucetError::State(format!(
            "state key '{key}' exceeds 256 characters"
        )));
    }
    for (i, c) in key.char_indices() {
        let ok = c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | ':' | '.');
        if !ok {
            return Err(FaucetError::State(format!(
                "state key '{key}' contains illegal character {c:?} at byte {i}"
            )));
        }
    }
    if key == "." || key == ".." || key.starts_with('.') {
        return Err(FaucetError::State(format!(
            "state key '{key}' must not begin with a dot"
        )));
    }
    Ok(())
}

// ── MemoryStateStore ────────────────────────────────────────────────────────

/// In-memory `StateStore` for tests and ephemeral pipelines.
#[derive(Default)]
pub struct MemoryStateStore {
    inner: Mutex<HashMap<String, Value>>,
}

impl MemoryStateStore {
    /// Create an empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl StateStore for MemoryStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        validate_state_key(key)?;
        Ok(self.inner.lock().await.get(key).cloned())
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        self.inner
            .lock()
            .await
            .insert(key.to_owned(), value.clone());
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        self.inner.lock().await.remove(key);
        Ok(())
    }
}

// ── FileStateStore ──────────────────────────────────────────────────────────

/// File-backed `StateStore`. Each key maps to a JSON file at
/// `{root}/{key}.json`, written via atomic rename.
///
/// Each `put` writes a temp file, fsyncs it, renames it over the final path,
/// and (on Unix) fsyncs the parent directory — so once `put` returns the
/// bookmark is durable across a crash or power loss, not merely sitting in the
/// page cache.
///
/// The store creates its root directory on first use. All writes are
/// serialized through a per-store mutex so two concurrent `put`s for the
/// same key cannot race on the temp filename. Different processes pointing
/// at the same directory rely on the filesystem's atomic rename guarantee.
pub struct FileStateStore {
    root: PathBuf,
    write_lock: Mutex<()>,
}

impl FileStateStore {
    /// Open or create a file-backed state store rooted at `root`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            write_lock: Mutex::new(()),
        }
    }

    fn entry_path(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.json"))
    }

    fn temp_path(&self, key: &str) -> PathBuf {
        self.root.join(format!("{key}.json.tmp"))
    }

    async fn ensure_root(&self) -> Result<(), FaucetError> {
        tokio::fs::create_dir_all(&self.root).await.map_err(|e| {
            FaucetError::State(format!(
                "failed to create state dir {}: {e}",
                self.root.display()
            ))
        })
    }

    /// Returns the root directory this store writes into.
    pub fn root(&self) -> &Path {
        &self.root
    }
}

#[async_trait]
impl StateStore for FileStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        validate_state_key(key)?;
        let path = self.entry_path(key);
        match tokio::fs::read(&path).await {
            Ok(bytes) => {
                let value: Value = serde_json::from_slice(&bytes).map_err(|e| {
                    FaucetError::State(format!(
                        "failed to parse state file {}: {e}",
                        path.display()
                    ))
                })?;
                Ok(Some(value))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(FaucetError::State(format!(
                "failed to read state file {}: {e}",
                path.display()
            ))),
        }
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        let _guard = self.write_lock.lock().await;
        self.ensure_root().await?;
        let bytes = serde_json::to_vec(value).map_err(|e| {
            FaucetError::State(format!("failed to serialize state for key '{key}': {e}"))
        })?;
        let final_path = self.entry_path(key);
        let tmp_path = self.temp_path(key);

        // Write the temp file and fsync it BEFORE the rename. `fs::write`
        // alone leaves the bytes in the page cache: a crash after `put`
        // returns could surface a zero-length or stale file, breaking the
        // durability guarantee this store documents (#78/#8). `sync_all`
        // flushes the file's data and metadata to disk.
        {
            let mut file = tokio::fs::File::create(&tmp_path).await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to create temp state file {}: {e}",
                    tmp_path.display()
                ))
            })?;
            file.write_all(&bytes).await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to write temp state file {}: {e}",
                    tmp_path.display()
                ))
            })?;
            file.sync_all().await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to fsync temp state file {}: {e}",
                    tmp_path.display()
                ))
            })?;
        }

        tokio::fs::rename(&tmp_path, &final_path)
            .await
            .map_err(|e| {
                FaucetError::State(format!(
                    "failed to commit state file {}: {e}",
                    final_path.display()
                ))
            })?;

        // fsync the parent directory so the rename itself is durable — an
        // atomic rename can still be lost on crash if the directory entry was
        // never flushed. Directory fsync is a POSIX concept; on platforms that
        // don't allow opening a directory as a file (e.g. Windows) it is
        // skipped.
        #[cfg(unix)]
        {
            let dir = tokio::fs::File::open(&self.root).await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to open state dir {} for fsync: {e}",
                    self.root.display()
                ))
            })?;
            dir.sync_all().await.map_err(|e| {
                FaucetError::State(format!(
                    "failed to fsync state dir {}: {e}",
                    self.root.display()
                ))
            })?;
        }

        tracing::debug!(
            key,
            path = %final_path.display(),
            "state file written"
        );
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        validate_state_key(key)?;
        let path = self.entry_path(key);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(FaucetError::State(format!(
                "failed to delete state file {}: {e}",
                path.display()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;
    use tempfile::TempDir;

    // ── key validation ─────────────────────────────────────────────────────

    #[test]
    fn rejects_empty_key() {
        let err = validate_state_key("").unwrap_err();
        assert!(matches!(err, FaucetError::State(_)));
    }

    #[test]
    fn rejects_path_traversal_segments() {
        for k in ["../etc/passwd", "a/b", "a\\b", "..", "."] {
            assert!(validate_state_key(k).is_err(), "expected reject for {k:?}");
        }
    }

    #[test]
    fn rejects_leading_dot() {
        assert!(validate_state_key(".hidden").is_err());
    }

    #[test]
    fn rejects_over_long_key() {
        let k = "a".repeat(257);
        assert!(validate_state_key(&k).is_err());
    }

    #[test]
    fn accepts_typical_keys() {
        for k in [
            "github_issues",
            "pipeline:rest:issues",
            "with.dot",
            "with-dash_and_underscore",
            "lower-Case_99",
        ] {
            validate_state_key(k).unwrap_or_else(|e| panic!("expected ok for {k:?}: {e}"));
        }
    }

    // ── MemoryStateStore ────────────────────────────────────────────────────

    #[tokio::test]
    async fn memory_get_returns_none_for_missing_key() {
        let s = MemoryStateStore::new();
        assert!(s.get("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn memory_put_then_get_round_trips() {
        let s = MemoryStateStore::new();
        s.put("k", &json!({"cursor": "abc", "n": 7})).await.unwrap();
        let got = s.get("k").await.unwrap().unwrap();
        assert_eq!(got["cursor"], "abc");
        assert_eq!(got["n"], 7);
    }

    #[tokio::test]
    async fn memory_put_overwrites_previous_value() {
        let s = MemoryStateStore::new();
        s.put("k", &json!(1)).await.unwrap();
        s.put("k", &json!(2)).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), json!(2));
    }

    #[tokio::test]
    async fn memory_delete_makes_get_return_none() {
        let s = MemoryStateStore::new();
        s.put("k", &json!("v")).await.unwrap();
        s.delete("k").await.unwrap();
        assert!(s.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn memory_delete_missing_key_is_ok() {
        let s = MemoryStateStore::new();
        s.delete("absent").await.unwrap();
    }

    #[tokio::test]
    async fn memory_rejects_invalid_keys() {
        let s = MemoryStateStore::new();
        assert!(s.get("a/b").await.is_err());
        assert!(s.put("a/b", &json!(1)).await.is_err());
        assert!(s.delete("a/b").await.is_err());
    }

    // ── FileStateStore ──────────────────────────────────────────────────────

    #[tokio::test]
    async fn file_get_returns_none_for_missing_key() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        assert!(s.get("nope").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn file_put_creates_root_directory_lazily() {
        let dir = TempDir::new().unwrap();
        let root = dir.path().join("nested/state");
        let s = FileStateStore::new(&root);
        s.put("k", &json!("v")).await.unwrap();
        assert!(root.is_dir(), "root dir should be created on first put");
    }

    #[tokio::test]
    async fn file_put_then_get_round_trips() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        let value = json!({"cursor": "abc", "n": 42, "nested": {"flag": true}});
        s.put("github_issues", &value).await.unwrap();
        let got = s.get("github_issues").await.unwrap().unwrap();
        assert_eq!(got, value);
    }

    #[tokio::test]
    async fn file_put_overwrites_previous_value_atomically() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        s.put("k", &json!({"v": 1})).await.unwrap();
        s.put("k", &json!({"v": 2})).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), json!({"v": 2}));
        // The temp path must not be left behind.
        let leftover = dir.path().join("k.json.tmp");
        assert!(!leftover.exists());
    }

    #[tokio::test]
    async fn file_put_writes_complete_durable_file_with_no_temp_residue() {
        // Regression for #78/#8. `put` must produce a fully-written, parseable
        // file and leave no temp file behind. (The fsync that makes this
        // durable across a power loss can't be observed on a healthy
        // filesystem, but a regression in the write/rename path — truncation,
        // a leftover .tmp, or an unwritten file — is caught here.) A large
        // payload makes a partial/unflushed write detectable on read-back.
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        let big: Vec<Value> = (0..1_000)
            .map(|i| json!({"i": i, "s": "x".repeat(20)}))
            .collect();
        let value = json!({"cursor": "abc", "rows": big});

        s.put("github_issues", &value).await.unwrap();

        // Read the raw file directly (bypassing get) to confirm it is complete.
        let raw = tokio::fs::read(dir.path().join("github_issues.json"))
            .await
            .expect("state file must exist after put");
        assert!(!raw.is_empty(), "state file must not be zero-length");
        let parsed: Value = serde_json::from_slice(&raw).expect("state file must be valid JSON");
        assert_eq!(parsed, value);

        // No temp file left behind.
        assert!(!dir.path().join("github_issues.json.tmp").exists());
    }

    #[tokio::test]
    async fn file_delete_removes_file() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        s.put("k", &json!("v")).await.unwrap();
        s.delete("k").await.unwrap();
        assert!(s.get("k").await.unwrap().is_none());
        assert!(!dir.path().join("k.json").exists());
    }

    #[tokio::test]
    async fn file_delete_missing_key_is_ok() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        s.delete("absent").await.unwrap();
    }

    #[tokio::test]
    async fn file_get_returns_error_for_corrupt_json() {
        let dir = TempDir::new().unwrap();
        let s = FileStateStore::new(dir.path());
        tokio::fs::create_dir_all(dir.path()).await.unwrap();
        tokio::fs::write(dir.path().join("bad.json"), b"not json")
            .await
            .unwrap();
        let err = s.get("bad").await.unwrap_err();
        match err {
            FaucetError::State(msg) => assert!(msg.contains("bad.json")),
            other => panic!("expected State error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn file_concurrent_puts_do_not_corrupt_or_leak_temp() {
        let dir = TempDir::new().unwrap();
        let s = Arc::new(FileStateStore::new(dir.path()));
        let mut handles = vec![];
        for i in 0..50 {
            let s = Arc::clone(&s);
            handles.push(tokio::spawn(async move {
                s.put("k", &json!({"i": i})).await.unwrap();
            }));
        }
        for h in handles {
            h.await.unwrap();
        }
        // The final value must be one of the 50 we wrote.
        let got = s.get("k").await.unwrap().unwrap();
        let i = got["i"].as_i64().unwrap();
        assert!((0..50).contains(&i));
        // And no temp file should remain.
        assert!(!dir.path().join("k.json.tmp").exists());
    }

    #[tokio::test]
    async fn file_store_works_through_trait_object() {
        let dir = TempDir::new().unwrap();
        let s: Box<dyn StateStore> = Box::new(FileStateStore::new(dir.path()));
        s.put("k", &json!(1)).await.unwrap();
        assert_eq!(s.get("k").await.unwrap().unwrap(), json!(1));
    }
}
