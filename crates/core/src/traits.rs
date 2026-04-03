//! Shared traits for faucet sources and sinks.

use crate::error::FaucetError;
use async_trait::async_trait;
use serde_json::Value;

/// A source fetches records from an external system.
#[async_trait]
pub trait Source: Send + Sync {
    /// Primary fetch method. Receives context from a parent source's records.
    ///
    /// An empty context map means this is a root source (no parent).
    /// Connectors that support being a child should use
    /// [`substitute_context()`](crate::util::substitute_context) to resolve
    /// `{placeholder}` tokens in their URL path, query parameters, headers,
    /// or body. Connectors that don't need parent context ignore the map.
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError>;

    /// Convenience: fetch with no parent context.
    async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
        self.fetch_with_context(&std::collections::HashMap::new())
            .await
    }

    /// Incremental fetch with parent context support.
    ///
    /// Returns the records and an optional bookmark value for incremental
    /// replication. The default delegates to `fetch_with_context` and
    /// returns `None` for the bookmark.
    async fn fetch_with_context_incremental(
        &self,
        context: &std::collections::HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let records = self.fetch_with_context(context).await?;
        Ok((records, None))
    }

    /// Convenience: incremental fetch with no parent context.
    async fn fetch_all_incremental(&self) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.fetch_with_context_incremental(&std::collections::HashMap::new())
            .await
    }

    /// Return a JSON Schema describing the configuration this source accepts.
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object", "properties": {}})
    }
}

/// A sink writes records to an external system.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Write a batch of records to the destination.
    ///
    /// Returns the number of records successfully written.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError>;

    /// Flush any buffered data to the destination.
    ///
    /// The default implementation is a no-op (suitable for sinks that
    /// write immediately in `write_batch`).
    async fn flush(&self) -> Result<(), FaucetError> {
        Ok(())
    }

    /// Return a JSON Schema describing the configuration this sink accepts.
    ///
    /// The schema is auto-generated from the config struct using `schemars`.
    /// Callers can inspect it to discover required fields, types, defaults,
    /// and descriptions before constructing the sink.
    ///
    /// The default returns an empty object schema.
    fn config_schema(&self) -> Value {
        serde_json::json!({"type": "object", "properties": {}})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── Mock Source ──────────────────────────────────────────────────────────

    struct MockSource {
        records: Vec<Value>,
    }

    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
    }

    struct IncrementalSource {
        records: Vec<Value>,
        bookmark: Value,
    }

    #[async_trait]
    impl Source for IncrementalSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }

        async fn fetch_with_context_incremental(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), Some(self.bookmark.clone())))
        }
    }

    struct FailingSource;

    #[async_trait]
    impl Source for FailingSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Err(FaucetError::Auth("no credentials".into()))
        }
    }

    // ── Mock Sink ───────────────────────────────────────────────────────────

    struct MockSink {
        written: std::sync::Mutex<Vec<Value>>,
    }

    impl MockSink {
        fn new() -> Self {
            Self {
                written: std::sync::Mutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Sink for MockSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            let mut w = self.written.lock().unwrap();
            w.extend(records.iter().cloned());
            Ok(records.len())
        }
    }

    struct FailingSink;

    #[async_trait]
    impl Sink for FailingSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            Err(FaucetError::Sink("write failed".into()))
        }
    }

    // ── Source tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn source_fetch_all_returns_records() {
        let source = MockSource {
            records: vec![json!({"id": 1}), json!({"id": 2})],
        };
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[tokio::test]
    async fn source_fetch_all_empty() {
        let source = MockSource { records: vec![] };
        let records = source.fetch_all().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn source_default_incremental_returns_none_bookmark() {
        let source = MockSource {
            records: vec![json!({"id": 1})],
        };
        let (records, bookmark) = source.fetch_all_incremental().await.unwrap();
        assert_eq!(records.len(), 1);
        assert!(bookmark.is_none());
    }

    #[tokio::test]
    async fn source_custom_incremental_returns_bookmark() {
        let source = IncrementalSource {
            records: vec![json!({"id": 1})],
            bookmark: json!("2024-12-01"),
        };
        let (records, bookmark) = source.fetch_all_incremental().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(bookmark, Some(json!("2024-12-01")));
    }

    #[tokio::test]
    async fn source_error_propagates() {
        let source = FailingSource;
        let result = source.fetch_all().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Auth(_))));
    }

    #[tokio::test]
    async fn source_as_trait_object() {
        let source: Box<dyn Source> = Box::new(MockSource {
            records: vec![json!({"id": 42})],
        });
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records[0]["id"], 42);
    }

    // ── Sink tests ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn sink_write_batch_returns_count() {
        let sink = MockSink::new();
        let records = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
        let count = sink.write_batch(&records).await.unwrap();
        assert_eq!(count, 3);
    }

    #[tokio::test]
    async fn sink_write_batch_empty() {
        let sink = MockSink::new();
        let count = sink.write_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn sink_accumulates_records() {
        let sink = MockSink::new();
        sink.write_batch(&[json!({"a": 1})]).await.unwrap();
        sink.write_batch(&[json!({"b": 2})]).await.unwrap();
        let written = sink.written.lock().unwrap();
        assert_eq!(written.len(), 2);
    }

    #[tokio::test]
    async fn sink_default_flush_is_noop() {
        let sink = MockSink::new();
        assert!(sink.flush().await.is_ok());
    }

    #[tokio::test]
    async fn sink_error_propagates() {
        let sink = FailingSink;
        let result = sink.write_batch(&[json!({"id": 1})]).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(FaucetError::Sink(_))));
    }

    #[tokio::test]
    async fn sink_as_trait_object() {
        let sink: Box<dyn Sink> = Box::new(MockSink::new());
        let count = sink.write_batch(&[json!({"id": 1})]).await.unwrap();
        assert_eq!(count, 1);
    }
}
