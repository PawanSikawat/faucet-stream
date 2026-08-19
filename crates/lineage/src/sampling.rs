//! Sink/source wrappers that sample records for schema inference and count
//! throughput for RUNNING heartbeats. Installed only when the corresponding
//! lineage facets/events are enabled, so they add zero overhead otherwise.

use async_trait::async_trait;
use faucet_core::{FaucetError, RowOutcome, Sink, Source, StreamPage};
use futures::{Stream, StreamExt};
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::lifecycle::InferredSchema;

/// Shared sampling/counter state for one dataset side.
pub struct SampleState {
    cap: usize,
    count: AtomicU64,
    sample: Mutex<Vec<Value>>,
}

impl SampleState {
    pub fn new(cap: usize) -> Self {
        Self {
            cap,
            count: AtomicU64::new(0),
            sample: Mutex::new(Vec::new()),
        }
    }
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }
    fn observe(&self, records: &[Value]) {
        self.count
            .fetch_add(records.len() as u64, Ordering::Relaxed);
        if self.cap == 0 {
            return;
        }
        let mut s = self.sample.lock().unwrap();
        for r in records {
            if s.len() >= self.cap {
                break;
            }
            s.push(r.clone());
        }
    }
    /// A copy of the sampled records (bounded by the construction-time cap).
    /// Used by consumers that run their own schema inference over the sample —
    /// e.g. the CLI's Data Movement Catalog (#279), which feeds them to
    /// `faucet_core::schema::infer_schema` for a drift-comparable shape.
    pub fn samples(&self) -> Vec<Value> {
        self.sample.lock().unwrap().clone()
    }

    /// Infer an ordered (name, OL-type) schema from the sampled records.
    pub fn inferred_schema(&self) -> InferredSchema {
        let sample = self.sample.lock().unwrap();
        if sample.is_empty() {
            return InferredSchema::default();
        }
        // Preserve first-seen field order across the sample.
        let mut order: Vec<String> = Vec::new();
        let mut types: HashMap<String, String> = HashMap::new();
        for rec in sample.iter() {
            if let Value::Object(map) = rec {
                for (k, v) in map {
                    if !types.contains_key(k) {
                        order.push(k.clone());
                    }
                    types
                        .entry(k.clone())
                        .or_insert_with(|| ol_type_of(v).to_string());
                }
            }
        }
        InferredSchema {
            fields: order
                .into_iter()
                .map(|k| {
                    let t = types.remove(&k).unwrap_or_else(|| "string".into());
                    (k, t)
                })
                .collect(),
        }
    }
}

fn ol_type_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(n) if n.is_i64() || n.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Wraps a sink, sampling written records and counting throughput.
pub struct SamplingSink {
    inner: Box<dyn Sink>,
    state: std::sync::Arc<SampleState>,
}

impl SamplingSink {
    pub fn new(inner: Box<dyn Sink>, state: std::sync::Arc<SampleState>) -> Self {
        Self { inner, state }
    }
}

#[async_trait]
impl Sink for SamplingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let n = self.inner.write_batch(records).await?;
        self.state.observe(records);
        Ok(n)
    }
    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        let outcomes = self.inner.write_batch_partial(records).await?;
        self.state.observe(records);
        Ok(outcomes)
    }
    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    // Capability + exactly-once passthroughs. Without these the wrapper's
    // trait defaults would mask the inner sink's capabilities whenever lineage
    // (or catalog) sampling is active — e.g. an exactly-once run would be
    // rejected as "sink is not idempotent" purely because sampling was on.
    fn supports_idempotent_writes(&self) -> bool {
        self.inner.supports_idempotent_writes()
    }
    fn sink_guarantee(&self) -> faucet_core::SinkGuarantee {
        self.inner.sink_guarantee()
    }
    fn dedups_by_key(&self) -> bool {
        self.inner.dedups_by_key()
    }
    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        self.inner.supported_write_modes()
    }
    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let n = self
            .inner
            .write_batch_idempotent(records, scope, token)
            .await?;
        self.state.observe(records);
        Ok(n)
    }
    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.inner.last_committed_token(scope).await
    }
    async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.current_schema().await
    }
    fn supports_schema_evolution(&self) -> bool {
        self.inner.supports_schema_evolution()
    }
    async fn evolve_schema(
        &self,
        evolution: &faucet_core::SchemaEvolution,
    ) -> Result<(), FaucetError> {
        self.inner.evolve_schema(evolution).await
    }
    fn is_overwrite(&self) -> bool {
        self.inner.is_overwrite()
    }
    async fn begin_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.begin_overwrite().await
    }
    async fn commit_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.commit_overwrite().await
    }
    async fn abort_overwrite(&self) -> Result<(), FaucetError> {
        self.inner.abort_overwrite().await
    }
}

/// Wraps a source, sampling the records it yields (pre-transform input schema).
pub struct SamplingSource {
    inner: Box<dyn Source>,
    state: std::sync::Arc<SampleState>,
}

impl SamplingSource {
    pub fn new(inner: Box<dyn Source>, state: std::sync::Arc<SampleState>) -> Self {
        Self { inner, state }
    }
}

#[async_trait]
impl Source for SamplingSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        // Required method; sampling happens in `stream_pages` (the path the
        // pipeline actually drives). Delegate so the trait is complete.
        self.inner.fetch_with_context(ctx).await
    }
    /// Override `stream_pages` (NOT `fetch_*`) so native-streaming sources keep
    /// their bounded-memory page stream — we tap the pages as they flow rather
    /// than forcing the buffering default path.
    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let state = std::sync::Arc::clone(&self.state);
        let inner = self.inner.stream_pages(ctx, batch_size);
        Box::pin(faucet_core::async_stream::try_stream! {
            let mut inner = inner;
            while let Some(page) = inner.next().await {
                let page = page?;
                state.observe(&page.records);
                yield page;
            }
        })
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn dataset_uri(&self) -> String {
        self.inner.dataset_uri()
    }
    fn state_key(&self) -> Option<String> {
        self.inner.state_key()
    }
    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }
    // Bookmark + capability passthroughs. `fetch_with_context_incremental`
    // matters even though the pipeline drives `stream_pages`: an *outer*
    // wrapper's default `stream_pages` builds on it, and the trait default
    // would silently drop the inner source's bookmark (breaking incremental
    // resume whenever sampling is active).
    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.inner.fetch_with_context_incremental(ctx).await
    }
    fn supports_exactly_once(&self) -> bool {
        self.inner.supports_exactly_once()
    }
    fn replay_guarantee(&self) -> faucet_core::ReplayGuarantee {
        self.inner.replay_guarantee()
    }
    async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
        self.inner.capture_resume_position().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use faucet_core::{FaucetError, Sink};
    use serde_json::{Value, json};
    use std::sync::Arc;

    struct CollectSink(std::sync::Mutex<Vec<Value>>);
    #[async_trait]
    impl Sink for CollectSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.lock().unwrap().extend(records.iter().cloned());
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "collect"
        }
    }

    #[tokio::test]
    async fn sink_counts_and_samples_first_n() {
        let shared = Arc::new(SampleState::new(2));
        let inner: Box<dyn Sink> = Box::new(CollectSink(Default::default()));
        let s = SamplingSink::new(inner, Arc::clone(&shared));
        s.write_batch(&[json!({"id":1,"name":"a"})]).await.unwrap();
        s.write_batch(&[json!({"id":2}), json!({"id":3})])
            .await
            .unwrap();
        assert_eq!(shared.count(), 3);
        // only the first 2 records were retained for schema inference
        let schema = shared.inferred_schema();
        let names: Vec<&str> = schema.fields.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"name"));
    }

    #[tokio::test]
    async fn sampling_sink_forwards_overwrite_lifecycle() {
        // The sampling wrapper must forward the overwrite lifecycle so an
        // overwrite run still stages/swaps when lineage/catalog sampling is on.
        struct OvwSink(Arc<std::sync::Mutex<Vec<&'static str>>>);
        #[async_trait]
        impl Sink for OvwSink {
            async fn write_batch(&self, r: &[Value]) -> Result<usize, FaucetError> {
                Ok(r.len())
            }
            fn is_overwrite(&self) -> bool {
                true
            }
            async fn begin_overwrite(&self) -> Result<(), FaucetError> {
                self.0.lock().unwrap().push("begin");
                Ok(())
            }
            async fn commit_overwrite(&self) -> Result<(), FaucetError> {
                self.0.lock().unwrap().push("commit");
                Ok(())
            }
            async fn abort_overwrite(&self) -> Result<(), FaucetError> {
                self.0.lock().unwrap().push("abort");
                Ok(())
            }
        }
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner: Box<dyn Sink> = Box::new(OvwSink(Arc::clone(&log)));
        let s = SamplingSink::new(inner, Arc::new(SampleState::new(2)));
        assert!(s.is_overwrite());
        // A write through the wrapper must forward to the inner sink too.
        assert_eq!(s.write_batch(&[json!({"id": 1})]).await.unwrap(), 1);
        s.begin_overwrite().await.unwrap();
        s.commit_overwrite().await.unwrap();
        s.abort_overwrite().await.unwrap();
        assert_eq!(*log.lock().unwrap(), vec!["begin", "commit", "abort"]);
    }

    struct TwoRowSource;
    #[async_trait]
    impl faucet_core::Source for TwoRowSource {
        async fn fetch_with_context(
            &self,
            _: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![json!({"id": 1}), json!({"id": 2})])
        }
        fn connector_name(&self) -> &'static str {
            "tworow"
        }
    }

    #[tokio::test]
    async fn source_samples_streamed_pages_without_buffering_override() {
        use faucet_core::Source as _;
        use futures::StreamExt as _;
        let shared = Arc::new(SampleState::new(10));
        let s = SamplingSource::new(Box::new(TwoRowSource), Arc::clone(&shared));
        let ctx = std::collections::HashMap::new();
        let mut pages = s.stream_pages(&ctx, 1000);
        while let Some(p) = pages.next().await {
            let _ = p.unwrap();
        }
        assert_eq!(shared.count(), 2);
        let schema = shared.inferred_schema();
        let names: Vec<&str> = schema.fields.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"id"));
    }

    /// An idempotent, upsert-capable sink: the sampler must forward every
    /// capability + exactly-once method instead of masking them with the trait
    /// defaults (which would make an exactly-once run fail as "sink is not
    /// idempotent" whenever lineage/catalog sampling is active).
    struct IdemSink;
    #[async_trait]
    impl Sink for IdemSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        fn connector_name(&self) -> &'static str {
            "idem"
        }
        fn supports_idempotent_writes(&self) -> bool {
            true
        }
        fn dedups_by_key(&self) -> bool {
            true
        }
        fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
            &[
                faucet_core::WriteMode::Append,
                faucet_core::WriteMode::Upsert,
            ]
        }
        async fn write_batch_idempotent(
            &self,
            records: &[Value],
            _scope: &str,
            _token: &str,
        ) -> Result<usize, FaucetError> {
            Ok(records.len())
        }
        async fn last_committed_token(&self, _scope: &str) -> Result<Option<String>, FaucetError> {
            Ok(Some("tok".into()))
        }
        fn supports_schema_evolution(&self) -> bool {
            true
        }
        async fn current_schema(&self) -> Result<Option<Value>, FaucetError> {
            Ok(Some(json!({"type": "object", "properties": {}})))
        }
    }

    #[tokio::test]
    async fn sink_forwards_capabilities_and_samples_idempotent_writes() {
        let shared = Arc::new(SampleState::new(10));
        let s = SamplingSink::new(Box::new(IdemSink), Arc::clone(&shared));
        assert!(s.supports_idempotent_writes());
        assert!(s.dedups_by_key());
        assert_eq!(
            s.sink_guarantee(),
            faucet_core::SinkGuarantee::AtomicWatermark
        );
        assert!(
            s.supported_write_modes()
                .contains(&faucet_core::WriteMode::Upsert)
        );
        assert!(s.supports_schema_evolution());
        assert!(s.current_schema().await.unwrap().is_some());
        assert_eq!(
            s.last_committed_token("k").await.unwrap(),
            Some("tok".into())
        );
        // Idempotent writes are observed by the sampler like plain writes.
        s.write_batch_idempotent(&[json!({"id": 1})], "k", "t")
            .await
            .unwrap();
        assert_eq!(shared.count(), 1);
    }

    struct BookmarkedSource;
    #[async_trait]
    impl faucet_core::Source for BookmarkedSource {
        async fn fetch_with_context(
            &self,
            _: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![json!({"id": 1})])
        }
        async fn fetch_with_context_incremental(
            &self,
            _: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((vec![json!({"id": 1})], Some(json!("bm"))))
        }
        fn supports_exactly_once(&self) -> bool {
            true
        }
        async fn capture_resume_position(&self) -> Result<Option<Value>, FaucetError> {
            Ok(Some(json!("pos")))
        }
        fn connector_name(&self) -> &'static str {
            "bookmarked"
        }
    }

    #[tokio::test]
    async fn source_forwards_bookmarks_and_capabilities() {
        use faucet_core::Source as _;
        let shared = Arc::new(SampleState::new(10));
        let s = SamplingSource::new(Box::new(BookmarkedSource), Arc::clone(&shared));
        // The incremental fetch must surface the inner bookmark — the trait
        // default would silently drop it (breaking incremental resume when an
        // outer wrapper's default stream_pages builds on this method).
        let (_, bm) = s
            .fetch_with_context_incremental(&std::collections::HashMap::new())
            .await
            .unwrap();
        assert_eq!(bm, Some(json!("bm")));
        assert!(s.supports_exactly_once());
        assert_eq!(
            s.replay_guarantee(),
            faucet_core::ReplayGuarantee::Deterministic
        );
        assert_eq!(
            s.capture_resume_position().await.unwrap(),
            Some(json!("pos"))
        );
    }
}
