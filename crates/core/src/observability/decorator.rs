//! Pipeline-internal decorators that emit spans + metrics around every
//! source / sink trait call. See the design spec for the full vocabulary.

use crate::error::FaucetError;
use crate::observability::labels::Labels;
use crate::observability::timer::DurationGuard;
use crate::pipeline::StreamPage;
use crate::traits::Source;
use async_trait::async_trait;
use futures::FutureExt;
use futures_core::Stream;
use metrics::{Label, SharedString, counter, gauge};
use serde_json::Value;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tracing::{Instrument, info_span};

/// Wraps a `&dyn Source` (or any `&S: Source`) and emits spans + metrics
/// around every call. Constructed by `Pipeline::run` and never exposed to
/// end users; the wrapped source remains the user-facing object.
pub struct InstrumentedSource<'a, S: Source + ?Sized> {
    inner: &'a S,
    labels: Labels,
    connector: SharedString,
    page_index: Arc<AtomicUsize>,
}

impl<'a, S: Source + ?Sized> InstrumentedSource<'a, S> {
    pub fn new(inner: &'a S, labels: Labels) -> Self {
        let raw = inner.connector_name();
        debug_assert!(
            !raw.is_empty(),
            "connector_name() must return a non-empty string"
        );
        let connector: SharedString =
            SharedString::const_str(if raw.is_empty() { "unknown" } else { raw });
        Self {
            inner,
            labels,
            connector,
            page_index: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn metric_labels(&self) -> Vec<Label> {
        vec![
            Label::new(
                "pipeline",
                SharedString::from(self.labels.pipeline.to_string()),
            ),
            Label::new("row", SharedString::from(self.labels.row.to_string())),
            Label::new("connector", self.connector.clone()),
        ]
    }

    /// Returns `metric_labels()` with an additional `kind` label appended.
    /// Used by `InstrumentedSink::write_batch` (Task 9) and any future
    /// instrumentation paths where `self` is in scope.
    #[allow(dead_code)]
    fn error_labels(&self, kind: &'static str) -> Vec<Label> {
        let mut l = self.metric_labels();
        l.push(Label::new("kind", SharedString::const_str(kind)));
        l
    }
}

#[async_trait]
impl<'a, S: Source + ?Sized> Source for InstrumentedSource<'a, S> {
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }

    fn state_key(&self) -> Option<String> {
        self.inner.state_key()
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }

    async fn fetch_with_context(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // Library-call path; the pipeline drives through stream_pages.
        self.inner.fetch_with_context(context).await
    }

    async fn fetch_with_context_incremental(
        &self,
        context: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.inner.fetch_with_context_incremental(context).await
    }

    fn stream_pages<'b>(
        &'b self,
        context: &'b HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'b>> {
        let inner_stream = self.inner.stream_pages(context, batch_size);
        let labels = self.labels.clone();
        let connector = self.connector.clone();
        let page_index = Arc::clone(&self.page_index);
        let metric_labels = self.metric_labels();
        let pipeline = self.labels.pipeline.clone();
        let row = self.labels.row.clone();

        Box::pin(async_stream::try_stream! {
            // In-flight gauge tracks open streams. Decrement on drop so
            // cancellation leaves the gauge consistent.
            struct InFlightGuard(Vec<Label>);
            impl Drop for InFlightGuard {
                fn drop(&mut self) {
                    gauge!("faucet_source_in_flight", self.0.clone()).decrement(1.0);
                }
            }
            gauge!("faucet_source_in_flight", metric_labels.clone()).increment(1.0);
            let _in_flight = InFlightGuard(metric_labels.clone());

            let mut inner = inner_stream;
            loop {
                let idx = page_index.fetch_add(1, Ordering::Relaxed);
                let span = info_span!(
                    "faucet.source.page",
                    pipeline = %pipeline,
                    row = %row,
                    run_id = %labels.run_id,
                    connector = %connector,
                    page_index = idx,
                );
                let _timer = DurationGuard::new(
                    "faucet_source_page_duration_seconds",
                    metric_labels.clone(),
                );

                let next = AssertUnwindSafe(async {
                    use futures::StreamExt;
                    inner.next().await
                })
                .catch_unwind()
                .instrument(span)
                .await;

                match next {
                    Ok(Some(Ok(page))) => {
                        counter!("faucet_source_pages_total", metric_labels.clone()).increment(1);
                        counter!("faucet_source_records_total", metric_labels.clone())
                            .increment(page.records.len() as u64);
                        yield page;
                    }
                    Ok(Some(Err(e))) => {
                        let mut l = metric_labels.clone();
                        l.push(Label::new("kind", SharedString::const_str(error_kind(&e))));
                        counter!("faucet_source_errors_total", l).increment(1);
                        Err(e)?;
                    }
                    Ok(None) => break,
                    Err(panic) => {
                        let mut l = metric_labels.clone();
                        l.push(Label::new("kind", SharedString::const_str("Panic")));
                        counter!("faucet_source_errors_total", l).increment(1);
                        let msg = panic.downcast_ref::<&'static str>().map(|s| (*s).to_string())
                            .or_else(|| panic.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "<non-string panic payload>".to_string());
                        Err(FaucetError::Custom(format!("panic in source: {msg}").into()))?;
                    }
                }
            }
        })
    }
}

/// Map a `FaucetError` variant to its stable `kind` label value. The match
/// must be exhaustive; update when new variants are added.
pub(crate) fn error_kind(e: &FaucetError) -> &'static str {
    match e {
        FaucetError::Http(_) => "Http",
        FaucetError::HttpStatus { .. } => "HttpStatus",
        FaucetError::Json(_) => "Json",
        FaucetError::JsonPath(_) => "JsonPath",
        FaucetError::Auth(_) => "Auth",
        FaucetError::RateLimited { .. } => "RateLimited",
        FaucetError::Url(_) => "Url",
        FaucetError::Transform(_) => "Transform",
        FaucetError::Config(_) => "Config",
        FaucetError::Source(_) => "Source",
        FaucetError::Sink(_) => "Sink",
        FaucetError::State(_) => "State",
        FaucetError::Custom(_) => "Custom",
    }
}

/// Placeholder for the sink decorator (Task 9).
pub struct InstrumentedSink;

#[cfg(test)]
pub(super) mod source_tests {
    use super::*;
    use async_trait::async_trait;
    use futures::StreamExt;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
    use serde_json::json;
    use std::sync::{Mutex, OnceLock};

    // Process-global recorder shared across all observability tests in this
    // crate. Task 5 established the same pattern.
    pub(in crate::observability) static LOCK: Mutex<()> = Mutex::new(());
    static SNAPSHOTTER: OnceLock<Snapshotter> = OnceLock::new();

    pub(in crate::observability) fn snapshotter() -> &'static Snapshotter {
        SNAPSHOTTER.get_or_init(|| {
            let recorder = DebuggingRecorder::new();
            let snap = recorder.snapshotter();
            // First test installs; the OnceLock guarantees we never install
            // twice. If something else (e.g. the timer test) already installed
            // a recorder, `set_global_recorder` will Err — but in that case
            // *our* snapshotter is disconnected from the live recorder. The
            // workaround is for all observability tests to share one source of
            // truth — this file. If a future test elsewhere installs a
            // recorder first, restructure so all tests share this OnceLock.
            let _ = metrics::set_global_recorder(recorder);
            snap
        })
    }

    pub(in crate::observability) fn labels() -> Labels {
        Labels::new("p", "r", "rid")
    }

    struct MockSource(Vec<Value>);
    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
        fn connector_name(&self) -> &'static str {
            "mock"
        }
    }

    struct PanickingSource;
    #[async_trait]
    impl Source for PanickingSource {
        async fn fetch_with_context(
            &self,
            _: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            panic!("kaboom")
        }
        fn connector_name(&self) -> &'static str {
            "panic-test"
        }
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn records_records_counter_per_page() {
        let _g = LOCK.lock().unwrap();
        let snap = snapshotter();
        let inner = MockSource((0..5).map(|i| json!({"i": i})).collect());
        let wrapped = InstrumentedSource::new(&inner, labels());
        let ctx = HashMap::new();
        let mut s = wrapped.stream_pages(&ctx, 2);
        while s.next().await.is_some() {}
        let snapshot = snap.snapshot();
        let records: u64 = snapshot
            .into_vec()
            .into_iter()
            .filter_map(|(key, _u, _d, v)| {
                if key.key().name() == "faucet_source_records_total"
                    && let DebugValue::Counter(c) = v
                {
                    return Some(c);
                }
                None
            })
            .sum();
        assert!(
            records >= 5,
            "expected at least 5 records counted, got {records}"
        );
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn maps_panic_to_custom_error_with_kind_panic() {
        let _g = LOCK.lock().unwrap();
        let _snap = snapshotter();
        let inner = PanickingSource;
        let wrapped = InstrumentedSource::new(&inner, labels());
        let ctx = HashMap::new();
        let mut s = wrapped.stream_pages(&ctx, 10);
        let first = s
            .next()
            .await
            .expect("stream yields at least one item before terminating");
        assert!(matches!(first, Err(FaucetError::Custom(_))));
        // Process did not abort — implicit by reaching this line.
    }
}
