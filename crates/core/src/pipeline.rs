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
//! Writes records page-by-page as they arrive from a stream, keeping memory
//! usage bounded.  Use [`run_stream`] with any `Stream<Item =
//! Result<Vec<Value>, FaucetError>>` (e.g.
//! [`RestStream::stream_pages()`](https://docs.rs/faucet-source-rest)).
//!
//! ```rust,no_run
//! use faucet_core::{run_stream, Sink};
//! use futures_core::Stream;
//! use serde_json::Value;
//! # async fn example(
//! #     pages: impl Stream<Item = Result<Vec<Value>, faucet_core::FaucetError>> + Unpin,
//! #     sink: impl Sink,
//! # ) -> Result<(), faucet_core::FaucetError> {
//! let result = run_stream(pages, &sink).await?;
//! # Ok(())
//! # }
//! ```

use crate::error::FaucetError;
use crate::traits::{Sink, Source};
use futures_core::Stream;
use serde_json::Value;
use std::pin::Pin;

/// Result of a pipeline run.
#[derive(Debug, Clone)]
pub struct PipelineResult {
    /// Total number of records written to the sink.
    pub records_written: usize,
    /// Bookmark value for incremental replication.
    ///
    /// `Some(value)` when the source supports incremental replication and
    /// returned a bookmark.  Persist this and pass it back as
    /// `start_replication_value` on the next run.
    ///
    /// Always `None` in streaming mode — use batch mode for bookmark support.
    pub bookmark: Option<Value>,
}

/// A pipeline that moves data from a [`Source`] to a [`Sink`].
///
/// The pipeline is generic over the source and sink types — any combination
/// of connectors works as long as they implement the respective traits.
pub struct Pipeline<'a, So: Source + ?Sized, Si: Sink + ?Sized> {
    source: &'a So,
    sink: &'a Si,
}

impl<'a, So: Source + ?Sized, Si: Sink + ?Sized> Pipeline<'a, So, Si> {
    /// Create a new pipeline from a source and a sink.
    pub fn new(source: &'a So, sink: &'a Si) -> Self {
        Self { source, sink }
    }

    /// Run the pipeline in batch mode.
    ///
    /// 1. Calls [`Source::fetch_all_incremental`] to get all records and an
    ///    optional bookmark.
    /// 2. Writes the records to the sink via [`Sink::write_batch`].
    /// 3. Calls [`Sink::flush`] to ensure all data is committed.
    /// 4. Returns a [`PipelineResult`] with the total count and bookmark.
    pub async fn run(&self) -> Result<PipelineResult, FaucetError> {
        let (records, bookmark) = self.source.fetch_all_incremental().await?;

        let records_written = if records.is_empty() {
            0
        } else {
            self.sink.write_batch(&records).await?
        };

        self.sink.flush().await?;

        tracing::info!(
            records_written,
            has_bookmark = bookmark.is_some(),
            "pipeline batch run complete"
        );

        Ok(PipelineResult {
            records_written,
            bookmark,
        })
    }
}

/// Run a streaming pipeline, writing each page to the sink as it arrives.
///
/// This keeps memory usage bounded — only one page of records is held at a
/// time.  The stream can come from any source that supports page-by-page
/// iteration (e.g. `RestStream::stream_pages()`).
///
/// Returns a [`PipelineResult`] with the total number of records written.
/// The `bookmark` field is always `None` in streaming mode — use batch mode
/// ([`Pipeline::run`]) when you need incremental replication bookmarks.
pub async fn run_stream<S, Si>(mut pages: S, sink: &Si) -> Result<PipelineResult, FaucetError>
where
    S: Stream<Item = Result<Vec<Value>, FaucetError>> + Unpin,
    Si: Sink + ?Sized,
{
    let mut records_written = 0usize;

    loop {
        let page = std::future::poll_fn(|cx| Pin::new(&mut pages).poll_next(cx)).await;
        match page {
            Some(Ok(records)) => {
                if !records.is_empty() {
                    records_written += sink.write_batch(&records).await?;
                }
            }
            Some(Err(e)) => return Err(e),
            None => break,
        }
    }

    sink.flush().await?;

    tracing::info!(records_written, "pipeline streaming run complete");

    Ok(PipelineResult {
        records_written,
        bookmark: None,
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
        async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }

    struct IncrementalSource {
        records: Vec<Value>,
        bookmark: Value,
    }

    #[async_trait]
    impl Source for IncrementalSource {
        async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn fetch_all_incremental(&self) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), Some(self.bookmark.clone())))
        }
    }

    struct FailingSource;

    #[async_trait]
    impl Source for FailingSource {
        async fn fetch_all(&self) -> Result<Vec<Value>, FaucetError> {
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
        let pages: Vec<Result<Vec<Value>, FaucetError>> = vec![
            Ok(vec![json!({"id": 1}), json!({"id": 2})]),
            Ok(vec![json!({"id": 3})]),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink).await.unwrap();

        assert_eq!(result.records_written, 3);
        assert!(result.bookmark.is_none());
        assert_eq!(sink.written().len(), 3);
    }

    #[tokio::test]
    async fn stream_pipeline_empty() {
        let pages: Vec<Result<Vec<Value>, FaucetError>> = vec![];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink).await.unwrap();

        assert_eq!(result.records_written, 0);
    }

    #[tokio::test]
    async fn stream_pipeline_skips_empty_pages() {
        let pages: Vec<Result<Vec<Value>, FaucetError>> = vec![
            Ok(vec![json!({"id": 1})]),
            Ok(vec![]),
            Ok(vec![json!({"id": 2})]),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink).await.unwrap();

        assert_eq!(result.records_written, 2);
    }

    #[tokio::test]
    async fn stream_pipeline_error_in_page_propagates() {
        let pages: Vec<Result<Vec<Value>, FaucetError>> = vec![
            Ok(vec![json!({"id": 1})]),
            Err(FaucetError::HttpStatus {
                status: 500,
                url: "https://example.com".into(),
                body: "Internal Server Error".into(),
            }),
        ];
        let stream = futures::stream::iter(pages);
        let sink = MockSink::new();

        let result = run_stream(stream, &sink).await;
        assert!(result.is_err());
        // First page was written before the error
        assert_eq!(sink.written().len(), 1);
    }

    #[tokio::test]
    async fn stream_pipeline_sink_error_propagates() {
        let pages: Vec<Result<Vec<Value>, FaucetError>> = vec![Ok(vec![json!({"id": 1})])];
        let stream = futures::stream::iter(pages);
        let sink = FailingSink;

        let result = run_stream(stream, &sink).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn stream_pipeline_with_trait_object_sink() {
        let pages: Vec<Result<Vec<Value>, FaucetError>> = vec![Ok(vec![json!({"id": 1})])];
        let stream = futures::stream::iter(pages);
        let sink: Box<dyn Sink> = Box::new(MockSink::new());

        let result = run_stream(stream, sink.as_ref()).await.unwrap();
        assert_eq!(result.records_written, 1);
    }
}
