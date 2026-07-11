//! Synthetic `Source` / `Sink` doubles the battery drives (and that connector
//! authors can reuse in their own tests).
//!
//! The doubles come in **conformant** and deliberately **non-conformant**
//! flavours. The non-conformant ones (`FailingSource`, `PanickingSource`,
//! `LyingIdempotentSink`, `LyingKeyedSink`) exist so the battery's own unit
//! tests can prove each check actually *fails* when the contract is violated —
//! a check that can never fail is worthless.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use faucet_core::write_mode::WriteMode;
use faucet_core::{FaucetError, Sink, Source, StreamPage, Value, async_trait};
use futures_core::Stream;
use serde_json::json;

/// A source that lazily emits `total` synthetic records (`{"n": i}`) in pages of
/// its configured `batch` (or the `stream_pages` hint), **without** buffering
/// the whole set — so it exercises the bounded-memory contract genuinely.
///
/// It also honours incremental resume: after `stream_pages` runs to completion
/// it emits a `{"n": total}` bookmark; feeding that back via
/// [`apply_start_bookmark`](Source::apply_start_bookmark) makes the next run
/// start at that offset (so a fully-consumed source resumes to zero records).
/// Construct with [`CountingSource::non_resumable`] to model a source that
/// *ignores* the bookmark — used to prove the bookmark-roundtrip check fails.
pub struct CountingSource {
    total: usize,
    batch: usize,
    resumable: bool,
    start: Arc<Mutex<usize>>,
}

impl CountingSource {
    /// `total` records, chunked into pages of `batch` (0 = one page). Resumable.
    pub fn new(total: usize, batch: usize) -> Self {
        Self {
            total,
            batch,
            resumable: true,
            start: Arc::new(Mutex::new(0)),
        }
    }

    /// Like [`new`](Self::new) but ignores any applied bookmark — a source that
    /// silently restarts from the beginning on resume (contract violation).
    pub fn non_resumable(total: usize, batch: usize) -> Self {
        Self {
            total,
            batch,
            resumable: false,
            start: Arc::new(Mutex::new(0)),
        }
    }
}

#[async_trait]
impl Source for CountingSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let start = *self.start.lock().unwrap();
        Ok((start..self.total).map(|i| json!({ "n": i })).collect())
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        // Like real sources, the double treats its own configured `batch` as
        // authoritative and ignores the pipeline hint. `batch == 0` is the
        // "no batching" sentinel — emit the whole set as one page (useful for
        // exercising the bounded-memory check's failure path).
        let batch = if self.batch == 0 {
            self.total.max(1)
        } else {
            self.batch
        };
        let total = self.total;
        let start = (*self.start.lock().unwrap()).min(total);
        Box::pin(async_stream::try_stream! {
            let mut n = start;
            if n >= total {
                // Fully consumed on resume: still emit one empty page carrying
                // the terminal bookmark so the pipeline advances its checkpoint.
                yield StreamPage { records: Vec::new(), bookmark: Some(json!({ "n": total })) };
                return;
            }
            while n < total {
                let end = (n + batch).min(total);
                let records: Vec<Value> = (n..end).map(|i| json!({ "n": i })).collect();
                n = end;
                let bookmark = if n >= total { Some(json!({ "n": total })) } else { None };
                yield StreamPage { records, bookmark };
            }
        })
    }

    fn connector_name(&self) -> &'static str {
        "counting-source"
    }

    fn state_key(&self) -> Option<String> {
        Some("conformance:counting".to_string())
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        if !self.resumable {
            return Ok(());
        }
        if let Some(n) = bookmark.get("n").and_then(|v| v.as_u64()) {
            *self.start.lock().unwrap() = n as usize;
        }
        Ok(())
    }
}

/// A source whose read path always returns a typed [`FaucetError`] — models an
/// unreachable endpoint / bad credentials. Used to prove the
/// `errors-not-panics` check passes on a well-behaved failure.
pub struct FailingSource;

#[async_trait]
impl Source for FailingSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Err(FaucetError::Source(
            "unreachable endpoint (test double)".to_string(),
        ))
    }

    fn connector_name(&self) -> &'static str {
        "failing-source"
    }
}

/// A source whose read path **panics** — models a buggy connector that unwraps
/// on unexpected input. Used to prove the `errors-not-panics` check *fails*
/// (catches the unwind) rather than letting the panic escape silently.
pub struct PanickingSource;

#[async_trait]
impl Source for PanickingSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        panic!("connector bug: unwrap() on a None value");
    }

    fn connector_name(&self) -> &'static str {
        "panicking-source"
    }
}

/// A sink that records everything written, optionally deduplicating by a key
/// field (upsert), optionally advertising the atomic-watermark idempotent path.
///
/// Modes:
/// - [`TestSink::new`] — append-only, non-idempotent.
/// - [`TestSink::keyed`] — dedups by key on `write_batch` (keyed-upsert /
///   `dedups_by_key`), advertises `Upsert`/`Delete`.
/// - [`TestSink::idempotent`] — additionally advertises
///   `supports_idempotent_writes` and stores a per-scope commit token, so the
///   atomic-watermark path can be exercised.
#[derive(Clone, Default)]
pub struct TestSink {
    key_field: Option<String>,
    idempotent: bool,
    keyed: Arc<Mutex<HashMap<String, Value>>>,
    appended: Arc<Mutex<Vec<Value>>>,
    tokens: Arc<Mutex<HashMap<String, String>>>,
    write_calls: Arc<Mutex<usize>>,
}

impl TestSink {
    /// An append-only recording sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// An upsert sink that dedups by `key_field` in `write_batch`.
    pub fn keyed(key_field: impl Into<String>) -> Self {
        Self {
            key_field: Some(key_field.into()),
            ..Self::default()
        }
    }

    /// An upsert sink that also commits an atomic watermark token per scope,
    /// so it advertises (and honours) `supports_idempotent_writes`.
    pub fn idempotent(key_field: impl Into<String>) -> Self {
        Self {
            key_field: Some(key_field.into()),
            idempotent: true,
            ..Self::default()
        }
    }

    /// Number of distinct rows currently stored (keyed) or appended.
    pub fn len(&self) -> usize {
        if self.key_field.is_some() {
            self.keyed.lock().unwrap().len()
        } else {
            self.appended.lock().unwrap().len()
        }
    }

    /// Whether the sink holds no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Total number of records passed to `write_batch` across all calls
    /// (counts re-delivered duplicates).
    pub fn total_written(&self) -> usize {
        *self.write_calls.lock().unwrap()
    }
}

#[async_trait]
impl Sink for TestSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        *self.write_calls.lock().unwrap() += records.len();
        match &self.key_field {
            Some(field) => {
                let mut map = self.keyed.lock().unwrap();
                for r in records {
                    let key = r.get(field).map(|v| v.to_string()).ok_or_else(|| {
                        FaucetError::Sink(format!("record missing key `{field}`"))
                    })?;
                    map.insert(key, r.clone());
                }
            }
            None => self
                .appended
                .lock()
                .unwrap()
                .extend(records.iter().cloned()),
        }
        Ok(records.len())
    }

    fn supports_idempotent_writes(&self) -> bool {
        self.idempotent
    }

    fn dedups_by_key(&self) -> bool {
        self.key_field.is_some()
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        if self.key_field.is_some() {
            &[WriteMode::Append, WriteMode::Upsert, WriteMode::Delete]
        } else {
            &[WriteMode::Append]
        }
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        // Store the token opaquely (last-write-wins). Monotonicity is enforced
        // by the pipeline via `last_committed_token`, not by the sink — the
        // double models a real atomic-watermark commit faithfully.
        self.tokens
            .lock()
            .unwrap()
            .insert(scope.to_string(), token.to_string());
        self.write_batch(records).await
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        Ok(self.tokens.lock().unwrap().get(scope).cloned())
    }

    fn connector_name(&self) -> &'static str {
        "test-sink"
    }
}

/// A sink that **claims** `supports_idempotent_writes` but does not actually
/// store a commit token (it just appends). Used to prove the idempotent-replay
/// and capabilities checks *fail* against a lying sink.
#[derive(Clone, Default)]
pub struct LyingIdempotentSink {
    appended: Arc<Mutex<Vec<Value>>>,
}

impl LyingIdempotentSink {
    /// A fresh lying sink.
    pub fn new() -> Self {
        Self::default()
    }
    /// Rows appended so far.
    pub fn len(&self) -> usize {
        self.appended.lock().unwrap().len()
    }
    /// Whether the sink holds no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl Sink for LyingIdempotentSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.appended
            .lock()
            .unwrap()
            .extend(records.iter().cloned());
        Ok(records.len())
    }

    fn supports_idempotent_writes(&self) -> bool {
        true // the lie — it never persists a token (default methods apply).
    }

    fn connector_name(&self) -> &'static str {
        "lying-idempotent-sink"
    }
}

/// A sink that **claims** to dedup by key (`dedups_by_key` + `Upsert` in
/// `supported_write_modes`) but actually appends duplicates. Used to prove the
/// keyed-convergence branch of the idempotent-replay check *fails*.
#[derive(Clone, Default)]
pub struct LyingKeyedSink {
    appended: Arc<Mutex<Vec<Value>>>,
}

impl LyingKeyedSink {
    /// A fresh lying keyed sink.
    pub fn new() -> Self {
        Self::default()
    }
    /// Rows appended so far.
    pub fn len(&self) -> usize {
        self.appended.lock().unwrap().len()
    }
    /// Whether the sink holds no rows.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[async_trait]
impl Sink for LyingKeyedSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.appended
            .lock()
            .unwrap()
            .extend(records.iter().cloned());
        Ok(records.len())
    }

    fn dedups_by_key(&self) -> bool {
        true // the lie — it never dedups.
    }

    fn supported_write_modes(&self) -> &'static [WriteMode] {
        &[WriteMode::Append, WriteMode::Upsert]
    }

    fn connector_name(&self) -> &'static str {
        "lying-keyed-sink"
    }
}
