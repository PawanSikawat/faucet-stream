//! Synthetic `Source` / `Sink` doubles the battery drives (and that connector
//! authors can reuse in their own tests).

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use faucet_core::{FaucetError, Sink, Source, StreamPage, Value, async_trait};
use futures_core::Stream;
use serde_json::json;

/// A source that lazily emits `total` synthetic records (`{"n": i}`) in pages of
/// its configured `batch` (or the `stream_pages` hint), **without** buffering
/// the whole set — so it exercises the bounded-memory contract genuinely.
pub struct CountingSource {
    total: usize,
    batch: usize,
}

impl CountingSource {
    /// `total` records, chunked into pages of `batch` (0 = one page).
    pub fn new(total: usize, batch: usize) -> Self {
        Self { total, batch }
    }
}

#[async_trait]
impl Source for CountingSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok((0..self.total).map(|i| json!({ "n": i })).collect())
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
        Box::pin(async_stream::try_stream! {
            let mut n = 0usize;
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
}

/// A sink that records everything written, deduplicating by an optional key
/// field so it can stand in for an upsert destination.
#[derive(Clone, Default)]
pub struct TestSink {
    key_field: Option<String>,
    keyed: Arc<Mutex<HashMap<String, Value>>>,
    appended: Arc<Mutex<Vec<Value>>>,
    write_calls: Arc<Mutex<usize>>,
}

impl TestSink {
    /// An append-only recording sink.
    pub fn new() -> Self {
        Self::default()
    }

    /// An upsert sink that dedups by `key_field`.
    pub fn keyed(key_field: impl Into<String>) -> Self {
        Self {
            key_field: Some(key_field.into()),
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

    fn connector_name(&self) -> &'static str {
        "test-sink"
    }
}
