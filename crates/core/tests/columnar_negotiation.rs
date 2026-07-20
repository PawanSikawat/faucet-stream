//! Proves the pipeline actually negotiates the Arrow columnar fast path
//! (feature `arrow`, RFC 0002 / #375) — not just that it compiles.
//!
//! The mock source/sink are **columnar-only**: their `Value` methods
//! (`stream_pages` / `write_batch`) return errors, while their columnar methods
//! (`stream_batches` / `write_batch_columnar`) work. So a successful
//! `Pipeline::run` is only possible if the pipeline drove the columnar path;
//! if it fell back to the `Value` path the run would error.
#![cfg(feature = "arrow")]

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use faucet_core::columnar::{ColumnarPage, values_to_record_batch_inferred};
use faucet_core::{FaucetError, Pipeline, Sink, Source, Stream, StreamPage, async_trait};
use serde_json::{Value, json};

/// A source that ONLY works columnar: `stream_pages` errors, `stream_batches`
/// yields one batch built from `rows`.
struct ColumnarOnlySource {
    rows: Vec<Value>,
}

#[async_trait]
impl Source for ColumnarOnlySource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Err(FaucetError::Source("value path must not be used".into()))
    }

    fn stream_pages<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(futures::stream::once(async {
            Err(FaucetError::Source(
                "stream_pages must not be called on the columnar path".into(),
            ))
        }))
    }

    fn connector_name(&self) -> &'static str {
        "columnar-only-source"
    }

    fn supports_columnar(&self) -> bool {
        true
    }

    fn stream_batches<'a>(
        &'a self,
        _context: &'a HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<ColumnarPage, FaucetError>> + Send + 'a>> {
        let batch = values_to_record_batch_inferred(&self.rows).unwrap();
        Box::pin(futures::stream::once(async move {
            Ok(ColumnarPage::new(batch, Some(json!({"done": true}))))
        }))
    }
}

/// A sink that ONLY works columnar: `write_batch` errors, `write_batch_columnar`
/// records the row count.
struct ColumnarOnlySink {
    rows: Arc<AtomicUsize>,
    columnar_calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Sink for ColumnarOnlySink {
    async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
        Err(FaucetError::Sink("value path must not be used".into()))
    }

    fn connector_name(&self) -> &'static str {
        "columnar-only-sink"
    }

    fn supports_columnar(&self) -> bool {
        true
    }

    async fn write_batch_columnar(
        &self,
        batch: &arrow::array::RecordBatch,
    ) -> Result<usize, FaucetError> {
        self.columnar_calls.fetch_add(1, Ordering::SeqCst);
        let n = batch.num_rows();
        self.rows.fetch_add(n, Ordering::SeqCst);
        Ok(n)
    }
}

#[tokio::test]
async fn pipeline_negotiates_columnar_path_when_both_sides_support_it() {
    let source = ColumnarOnlySource {
        rows: vec![
            json!({"id": 1, "region": "NA"}),
            json!({"id": 2, "region": "EU"}),
            json!({"id": 3, "region": "APAC"}),
        ],
    };
    let rows = Arc::new(AtomicUsize::new(0));
    let columnar_calls = Arc::new(AtomicUsize::new(0));
    let sink = ColumnarOnlySink {
        rows: Arc::clone(&rows),
        columnar_calls: Arc::clone(&columnar_calls),
    };

    // If the pipeline used the Value path, stream_pages / write_batch would
    // error and this would be `Err`.
    let result = Pipeline::new(&source, &sink)
        .run()
        .await
        .expect("columnar path should succeed");

    assert_eq!(result.records_written, 3, "all rows written via columnar path");
    assert_eq!(rows.load(Ordering::SeqCst), 3);
    assert_eq!(
        columnar_calls.load(Ordering::SeqCst),
        1,
        "exactly one columnar write for the single batch"
    );
    assert_eq!(
        result.bookmark,
        Some(json!({"done": true})),
        "the columnar page's bookmark propagates to the result"
    );
}

/// When the sink does NOT support columnar, the pipeline must fall back to the
/// `Value` path — verified here by a columnar-only *source* (whose `stream_pages`
/// errors) paired with a value-only sink, so the run fails rather than silently
/// mis-routing. This pins the negotiation predicate (both sides required).
#[tokio::test]
async fn pipeline_falls_back_when_sink_lacks_columnar() {
    struct ValueOnlySink;
    #[async_trait]
    impl Sink for ValueOnlySink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "value-only-sink"
        }
        // supports_columnar() defaults to false.
    }

    let source = ColumnarOnlySource {
        rows: vec![json!({"id": 1})],
    };
    let sink = ValueOnlySink;

    // Sink is not columnar → Value path → source.stream_pages errors.
    let result = Pipeline::new(&source, &sink).run().await;
    assert!(
        result.is_err(),
        "with a non-columnar sink the pipeline must use the Value path (which this source rejects)"
    );
}
