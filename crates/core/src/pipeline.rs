//! Source-to-sink pipeline orchestration.
//!
//! The [`Pipeline`] struct connects any [`Source`](crate::Source) to any
//! [`Sink`](crate::Sink) and handles moving data between them.
//!
//! # Batch mode
//!
//! Fetches all records from the source, then writes them to the sink in one
//! shot.  Supports incremental replication (returns a bookmark for the next
//! run).
//!
//! ```rust,no_run
//! use faucet_core::{Pipeline, Source, Sink};
//! # async fn example(source: impl Source, sink: impl Sink) -> Result<(), faucet_core::FaucetError> {
//! let result = Pipeline::new(&source, &sink).run().await?;
//! println!("wrote {} records", result.records_written);
//! // Persist result.bookmark for the next incremental run
//! # Ok(())
//! # }
//! ```
//!
//! # Streaming mode
//!
//! Writes records page-by-page as they arrive from a source's
//! [`stream_pages`](crate::Source::stream_pages) implementation, keeping
//! memory usage bounded.  [`Pipeline::run`] uses this internally; callers
//! that have already assembled a [`StreamPage`] stream can drive it directly
//! via [`run_stream`].
//!
//! ```rust,no_run
//! use faucet_core::{run_stream, Sink, StreamPage, FaucetError};
//! use futures_core::Stream;
//! # async fn example(
//! #     pages: impl Stream<Item = Result<StreamPage, FaucetError>> + Unpin,
//! #     sink: impl Sink,
//! # ) -> Result<(), FaucetError> {
//! let result = run_stream(pages, &sink, None, None).await?;
//! # Ok(())
//! # }
//! ```

use crate::error::FaucetError;
use crate::state::{StateStore, validate_state_key};
use crate::traits::{Sink, Source};
use futures_core::Stream;
use serde_json::Value;
use std::pin::Pin;
use std::sync::Arc;

/// Default page size used when a caller does not specify one.
///
/// Sources are free to override this from their own config when implementing
/// [`Source::stream_pages`](crate::Source::stream_pages); the value passed
/// from the pipeline acts as a hint when no source-side preference exists.
pub const DEFAULT_BATCH_SIZE: usize = 1000;

/// Hard upper bound on `batch_size`. Values above this (other than the
/// special `0` "no batching" sentinel) are rejected at config validation
/// time to prevent accidental O(total) buffering in the default
/// implementation of [`Source::stream_pages`].
pub const MAX_BATCH_SIZE: usize = 1_000_000;

/// Validate a `batch_size` value against the global constraints.
///
/// `batch_size = 0` is the **opt-out-of-batching sentinel**: sources and
/// sinks should treat it as "emit / accept the entire result set in one
/// page." This is useful for small lookup tables or for sinks (e.g. SQL
/// `COPY`, BigQuery load jobs) that prefer one large request to many small
/// ones. Any non-zero value above [`MAX_BATCH_SIZE`] is rejected to prevent
/// accidental unbounded buffering through a typo.
///
/// Returns the unchanged value on success. Returns `FaucetError::Config`
/// only for values strictly greater than [`MAX_BATCH_SIZE`].
pub fn validate_batch_size(batch_size: usize) -> Result<usize, FaucetError> {
    if batch_size > MAX_BATCH_SIZE {
        return Err(FaucetError::Config(format!(
            "batch_size {batch_size} exceeds maximum {MAX_BATCH_SIZE} \
             (use 0 to opt out of batching entirely)"
        )));
    }
    Ok(batch_size)
}

/// One page emitted by [`Source::stream_pages`](crate::Source::stream_pages).
///
/// `records` is the chunk of records for this page. `bookmark` is `Some` only
/// when the source has a durable checkpoint to advance — most sources emit
/// `Some` only on the final page (max-replication-value semantics); CDC-style
/// sources emit `Some` per committed transaction. The pipeline flushes the
/// sink and persists the bookmark every time a page carries one, so a
/// mid-stream crash never advances past records the sink has not durably
/// written.
#[derive(Debug, Clone, Default)]
pub struct StreamPage {
    /// Records to write to the sink for this page.
    pub records: Vec<Value>,
    /// Optional bookmark to checkpoint after this page is durably written.
    pub bookmark: Option<Value>,
}

/// Result of a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    /// Total number of records written to the sink.
    pub records_written: usize,
    /// Bookmark value for incremental replication.
    ///
    /// `Some(value)` when the source returned a bookmark on its final
    /// (or, for streaming CDC sources, most recent) page. Persist this and
    /// pass it back as `start_replication_value` on the next run; this is
    /// handled automatically when a [`StateStore`] is attached via
    /// [`Pipeline::with_state_store`].
    pub bookmark: Option<Value>,
}

/// A pipeline that moves data from a [`Source`] to a [`Sink`].
///
/// The pipeline is generic over the source and sink types — any combination
/// of connectors works as long as they implement the respective traits.
pub struct Pipeline<'a, So: Source + ?Sized, Si: Sink + ?Sized> {
    source: &'a So,
    sink: &'a Si,
    state_store: Option<Arc<dyn StateStore>>,
}

impl<'a, So: Source + ?Sized, Si: Sink + ?Sized> Pipeline<'a, So, Si> {
    /// Create a new pipeline from a source and a sink.
    pub fn new(source: &'a So, sink: &'a Si) -> Self {
        Self {
            source,
            sink,
            state_store: None,
        }
    }

    /// Attach a [`StateStore`] for persistent incremental-replication bookmarks.
    ///
    /// When configured, `run()` will:
    /// 1. Read any previously stored bookmark at the source's
    ///    [`state_key`](Source::state_key) and call
    ///    [`apply_start_bookmark`](Source::apply_start_bookmark) on the source
    ///    so it can resume from that point.
    /// 2. Run the fetch + write as usual.
    /// 3. Persist the new bookmark **only after** the sink confirms the
    ///    batch was written and flushed.
    ///
    /// Sources that do not return a [`state_key`](Source::state_key) are
    /// unaffected — the store is consulted only when the source opts in.
    pub fn with_state_store(mut self, store: Arc<dyn StateStore>) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Run the pipeline in streaming mode.
    ///
    /// 1. Loads the stored bookmark and pushes it to the source (if a state
    ///    store is configured and the source returns a `state_key`).
    /// 2. Drives [`Source::stream_pages`] with [`DEFAULT_BATCH_SIZE`],
    ///    writing each page to the sink as it arrives via
    ///    [`Sink::write_batch`].
    /// 3. Whenever a page carries `Some(bookmark)`, flushes the sink and
    ///    persists the bookmark to the state store before polling the next
    ///    page. This makes per-page CDC checkpointing automatic.
    /// 4. Flushes the sink one final time after the stream completes.
    /// 5. Returns a [`PipelineResult`] with the total count and the last
    ///    bookmark observed.
    pub async fn run(&self) -> Result<PipelineResult, FaucetError> {
        let state_key = self.source.state_key();
        if let (Some(store), Some(key)) = (self.state_store.as_ref(), state_key.as_ref()) {
            validate_state_key(key)?;
            if let Some(prior) = store.get(key).await? {
                self.source.apply_start_bookmark(prior).await?;
            }
        }

        let ctx = std::collections::HashMap::new();
        let pages = self.source.stream_pages(&ctx, DEFAULT_BATCH_SIZE);
        run_stream(pages, self.sink, self.state_store.clone(), state_key).await
    }
}

/// Run a streaming pipeline, writing each [`StreamPage`] to the sink as it
/// arrives and persisting bookmarks per page.
///
/// This keeps memory usage bounded — only one page of records is held at a
/// time. The stream comes from [`Source::stream_pages`] (or any
/// `Stream<Item = Result<StreamPage, FaucetError>>` a caller assembles
/// directly).
///
/// Bookmark semantics: whenever a page carries `Some(bookmark)`, the sink is
/// flushed and the bookmark is persisted (when `state_store` and `state_key`
/// are both `Some`) before the next page is polled. Sources that only know
/// their bookmark after seeing every record emit `Some` on the final page;
/// CDC-style sources emit `Some` per committed transaction and get
/// per-transaction durability automatically.
///
/// Returns the cumulative [`PipelineResult`] — `records_written` is the sum
/// across all pages and `bookmark` is the last per-page bookmark observed.
pub async fn run_stream<S, Si>(
    mut pages: S,
    sink: &Si,
    state_store: Option<Arc<dyn StateStore>>,
    state_key: Option<String>,
) -> Result<PipelineResult, FaucetError>
where
    S: Stream<Item = Result<StreamPage, FaucetError>> + Unpin,
    Si: Sink + ?Sized,
{
    if let Some(key) = state_key.as_ref() {
        validate_state_key(key)?;
    }

    let mut records_written = 0usize;
    let mut last_bookmark: Option<Value> = None;

    loop {
        let page = std::future::poll_fn(|cx| Pin::new(&mut pages).poll_next(cx)).await;
        match page {
            Some(Ok(page)) => {
                if !page.records.is_empty() {
                    records_written += sink.write_batch(&page.records).await?;
                }
                if let Some(bookmark) = page.bookmark {
                    sink.flush().await?;
                    if let (Some(store), Some(key)) = (state_store.as_ref(), state_key.as_ref()) {
                        store.put(key, &bookmark).await?;
                    }
                    last_bookmark = Some(bookmark);
                }
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }

    sink.flush().await?;

    tracing::info!(
        records_written,
        has_bookmark = last_bookmark.is_some(),
        persisted = state_store.is_some() && state_key.is_some() && last_bookmark.is_some(),
        "pipeline streaming run complete"
    );

    Ok(PipelineResult {
        records_written,
        bookmark: last_bookmark,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;

    // ── Mock Source ──────────────────────────────────────────────────────────

    struct MockSource(Vec<Value>);

    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _context: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
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

    struct MockSink(std::sync::Mutex<Vec<Value>>);

    impl MockSink {
        fn new() -> Self {
            Self(std::sync::Mutex::new(Vec::new()))
        }
        fn written(&self) -> Vec<Value> {
            self.0.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Sink for MockSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.lock().unwrap().extend(records.iter().cloned());
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

    // ── StreamPage / batch_size tests ───────────────────────────────────────

    #[test]
    fn stream_page_constructs() {
        let page = StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: Some(json!("2026-05-18")),
        };
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.bookmark, Some(json!("2026-05-18")));
    }

    #[test]
    fn validate_batch_size_accepts_zero_as_no_batching_sentinel() {
        // 0 means "do not batch — emit/accept the whole result set in one page".
        assert_eq!(validate_batch_size(0).unwrap(), 0);
    }

    #[test]
    fn validate_batch_size_rejects_too_large() {
        let err = validate_batch_size(MAX_BATCH_SIZE + 1).unwrap_err();
        assert!(matches!(err, FaucetError::Config(_)));
    }

    #[test]
    fn validate_batch_size_accepts_one() {
        assert_eq!(validate_batch_size(1).unwrap(), 1);
    }

    #[test]
    fn validate_batch_size_accepts_max() {
        assert_eq!(validate_batch_size(MAX_BATCH_SIZE).unwrap(), MAX_BATCH_SIZE);
    }

    // Compile-time invariant: DEFAULT_BATCH_SIZE must be within [1, MAX_BATCH_SIZE].
    const _: () = {
        assert!(DEFAULT_BATCH_SIZE >= 1);
        assert!(DEFAULT_BATCH_SIZE <= MAX_BATCH_SIZE);
    };

    // ── Batch mode tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn batch_pipeline_writes_all_records() {
        let source = MockSource(vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})]);
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        assert_eq!(result.records_written, 3);
        assert!(result.bookmark.is_none());
        assert_eq!(sink.written().len(), 3);
    }

    #[tokio::test]
    async fn batch_pipeline_returns_bookmark() {
        let source = IncrementalSource {
            records: vec![json!({"id": 1, "ts": "2024-12-01"})],
            bookmark: json!("2024-12-01"),
        };
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        assert_eq!(result.records_written, 1);
        assert_eq!(result.bookmark, Some(json!("2024-12-01")));
    }

    #[tokio::test]
    async fn batch_pipeline_empty_source() {
        let source = MockSource(vec![]);
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        assert_eq!(result.records_written, 0);
        assert!(sink.written().is_empty());
    }

    #[tokio::test]
    async fn batch_pipeline_source_error_propagates() {
        let source = FailingSource;
        let sink = MockSink::new();

        let result = Pipeline::new(&source, &sink).run().await;
        assert!(result.is_err());
        assert!(sink.written().is_empty());
    }

    #[tokio::test]
    async fn batch_pipeline_sink_error_propagates() {
        let source = MockSource(vec![json!({"id": 1})]);
        let sink = FailingSink;

        let result = Pipeline::new(&source, &sink).run().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn batch_pipeline_with_trait_objects() {
        let source: Box<dyn Source> = Box::new(MockSource(vec![json!({"id": 1})]));
        let sink: Box<dyn Sink> = Box::new(MockSink::new());

        let result = Pipeline::new(source.as_ref(), sink.as_ref())
            .run()
            .await
            .unwrap();

        assert_eq!(result.records_written, 1);
    }

    // ── Streaming mode tests ────────────────────────────────────────────────

    #[tokio::test]
    async fn stream_pipeline_writes_pages() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1}), json!({"id": 2})],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 3})],
                bookmark: None,
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, None, None).await.unwrap();

        assert_eq!(result.records_written, 3);
        assert!(result.bookmark.is_none());
        assert_eq!(sink.written().len(), 3);
    }

    #[tokio::test]
    async fn stream_pipeline_empty() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, None, None).await.unwrap();

        assert_eq!(result.records_written, 0);
    }

    #[tokio::test]
    async fn stream_pipeline_skips_empty_pages() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: None,
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, None, None).await.unwrap();

        assert_eq!(result.records_written, 2);
    }

    #[tokio::test]
    async fn stream_pipeline_error_in_page_propagates() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: None,
            }),
            Err(FaucetError::HttpStatus {
                status: 500,
                url: "https://example.com".into(),
                body: "Internal Server Error".into(),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink, None, None).await;
        assert!(result.is_err());
        // First page was written before the error
        assert_eq!(sink.written().len(), 1);
    }

    #[tokio::test]
    async fn stream_pipeline_sink_error_propagates() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let sink = FailingSink;

        let result = run_stream(stream, &sink, None, None).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stream_pipeline_with_trait_object_sink() {
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
            records: vec![json!({"id": 1})],
            bookmark: None,
        })];
        let stream = futures::stream::iter(pages);
        let sink: Box<dyn Sink> = Box::new(MockSink::new());

        let result = run_stream(stream, sink.as_ref(), None, None).await.unwrap();
        assert_eq!(result.records_written, 1);
    }

    #[tokio::test]
    async fn stream_pipeline_persists_bookmark_when_page_carries_one() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: None,
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: Some(json!("checkpoint-final")),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(
            stream,
            &sink,
            Some(Arc::clone(&store)),
            Some("k".to_string()),
        )
        .await
        .unwrap();

        assert_eq!(result.records_written, 2);
        assert_eq!(result.bookmark, Some(json!("checkpoint-final")));
        assert_eq!(
            store.get("k").await.unwrap(),
            Some(json!("checkpoint-final"))
        );
    }

    #[tokio::test]
    async fn stream_pipeline_persists_per_page_bookmarks() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let pages: Vec<Result<StreamPage, FaucetError>> = vec![
            Ok(StreamPage {
                records: vec![json!({"id": 1})],
                bookmark: Some(json!("tx-1")),
            }),
            Ok(StreamPage {
                records: vec![json!({"id": 2})],
                bookmark: Some(json!("tx-2")),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        run_stream(
            stream,
            &sink,
            Some(Arc::clone(&store)),
            Some("k".to_string()),
        )
        .await
        .unwrap();

        // Latest per-page bookmark wins.
        assert_eq!(store.get("k").await.unwrap(), Some(json!("tx-2")));
    }

    // ── State-store integration tests ───────────────────────────────────────

    use crate::state::{FileStateStore, MemoryStateStore, StateStore};
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Source that opts into state persistence. It records the bookmark it
    /// received via `apply_start_bookmark` so tests can verify the pipeline
    /// pushed the stored value back into it on resume.
    struct StatefulSource {
        key: String,
        records: Vec<Value>,
        new_bookmark: Value,
        seen_bookmark: std::sync::Mutex<Option<Value>>,
    }

    impl StatefulSource {
        fn new(key: &str, records: Vec<Value>, new_bookmark: Value) -> Self {
            Self {
                key: key.into(),
                records,
                new_bookmark,
                seen_bookmark: std::sync::Mutex::new(None),
            }
        }
        fn observed_start(&self) -> Option<Value> {
            self.seen_bookmark.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Source for StatefulSource {
        async fn fetch_with_context(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn fetch_with_context_incremental(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), Some(self.new_bookmark.clone())))
        }
        fn state_key(&self) -> Option<String> {
            Some(self.key.clone())
        }
        async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
            *self.seen_bookmark.lock().unwrap() = Some(bookmark);
            Ok(())
        }
    }

    #[tokio::test]
    async fn pipeline_with_state_store_persists_bookmark_after_sink() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let source = StatefulSource::new(
            "github_issues",
            vec![json!({"id": 1, "ts": "2026-05-01"})],
            json!("2026-05-01"),
        );
        let sink = MockSink::new();
        let result = Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();

        assert_eq!(result.records_written, 1);
        assert_eq!(result.bookmark, Some(json!("2026-05-01")));
        // Stored value matches what the source returned.
        let stored = store.get("github_issues").await.unwrap();
        assert_eq!(stored, Some(json!("2026-05-01")));
    }

    #[tokio::test]
    async fn pipeline_with_state_store_resumes_from_stored_bookmark() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        store
            .put("github_issues", &json!("2026-04-30"))
            .await
            .unwrap();

        let source =
            StatefulSource::new("github_issues", vec![json!({"id": 2})], json!("2026-05-01"));
        let sink = MockSink::new();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();

        // The pipeline pushed the previously-stored bookmark back into the source.
        assert_eq!(source.observed_start(), Some(json!("2026-04-30")));
        // And then overwrote it with the new value from this run.
        assert_eq!(
            store.get("github_issues").await.unwrap(),
            Some(json!("2026-05-01"))
        );
    }

    #[tokio::test]
    async fn pipeline_with_state_store_does_not_persist_when_sink_fails() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let source = StatefulSource::new("k", vec![json!({"id": 1})], json!("2026-05-01"));
        let sink = FailingSink;

        let result = Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await;
        assert!(result.is_err());
        assert!(store.get("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pipeline_with_state_store_no_state_key_means_no_persist() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let source = IncrementalSource {
            records: vec![json!({"id": 1})],
            bookmark: json!("ignored"),
        };
        let sink = MockSink::new();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        // IncrementalSource doesn't override state_key, so nothing was persisted.
        // Cross-check that no keys exist by trying a likely one.
        assert!(store.get("anything").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn pipeline_with_state_store_skips_persist_when_bookmark_is_none() {
        let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        struct NoBookmarkSource;
        #[async_trait]
        impl Source for NoBookmarkSource {
            async fn fetch_with_context(
                &self,
                _ctx: &std::collections::HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(vec![json!({"id": 1})])
            }
            fn state_key(&self) -> Option<String> {
                Some("k".into())
            }
        }
        let source = NoBookmarkSource;
        let sink = MockSink::new();
        Pipeline::new(&source, &sink)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        assert!(store.get("k").await.unwrap().is_none());
    }

    // ── Pipeline::run drives stream_pages ──────────────────────────────────

    /// A source with a custom `stream_pages` impl that yields three pages.
    /// Used to prove `Pipeline::run` drives the streaming path.
    struct PagedSource;

    #[async_trait]
    impl Source for PagedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            // Should never be called when stream_pages is overridden.
            unreachable!("Pipeline::run must drive stream_pages, not fetch_with_context");
        }
        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a std::collections::HashMap<String, Value>,
            _batch_size: usize,
        ) -> std::pin::Pin<
            Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>,
        > {
            Box::pin(async_stream::try_stream! {
                yield StreamPage { records: vec![json!({"i": 1})], bookmark: None };
                yield StreamPage { records: vec![json!({"i": 2})], bookmark: None };
                yield StreamPage { records: vec![json!({"i": 3})], bookmark: Some(json!("final")) };
            })
        }
    }

    /// Sink that counts how many distinct write_batch calls happen.
    struct CountingSink {
        calls: std::sync::Mutex<Vec<usize>>,
    }

    impl CountingSink {
        fn new() -> Self {
            Self {
                calls: std::sync::Mutex::new(Vec::new()),
            }
        }
        fn call_count(&self) -> usize {
            self.calls.lock().unwrap().len()
        }
    }

    #[async_trait]
    impl Sink for CountingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.calls.lock().unwrap().push(records.len());
            Ok(records.len())
        }
    }

    #[tokio::test]
    async fn pipeline_run_drives_stream_pages() {
        let source = PagedSource;
        let sink = CountingSink::new();

        let result = Pipeline::new(&source, &sink).run().await.unwrap();

        // Three pages of one record each → three sink calls, three records.
        assert_eq!(sink.call_count(), 3);
        assert_eq!(result.records_written, 3);
        assert_eq!(result.bookmark, Some(json!("final")));
    }

    #[tokio::test]
    async fn pipeline_with_file_state_store_round_trips_across_runs() {
        let dir = TempDir::new().unwrap();
        let store: Arc<dyn StateStore> = Arc::new(FileStateStore::new(dir.path()));

        // Run 1: nothing stored yet, persist new bookmark.
        let s1 = StatefulSource::new("k", vec![json!({"i": 1})], json!("v1"));
        let sink1 = MockSink::new();
        Pipeline::new(&s1, &sink1)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        assert_eq!(s1.observed_start(), None);
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v1")));

        // Run 2: resume from v1, persist v2.
        let s2 = StatefulSource::new("k", vec![json!({"i": 2})], json!("v2"));
        let sink2 = MockSink::new();
        Pipeline::new(&s2, &sink2)
            .with_state_store(Arc::clone(&store))
            .run()
            .await
            .unwrap();
        assert_eq!(s2.observed_start(), Some(json!("v1")));
        assert_eq!(store.get("k").await.unwrap(), Some(json!("v2")));
    }
}
