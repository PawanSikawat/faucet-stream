//! Wrap any [`Source`] with a fixed list of [`TransformStage`]s applied to
//! every emitted record. The canonical way for library callers to attach
//! stages (transforms wrapped via [`TransformStage::Map`], plus `Filter` /
//! `Explode` / `Custom`); the CLI uses this same type internally.

use crate::error::FaucetError;
use crate::observability::{Labels, instrumented_apply_stages};
use crate::pipeline::StreamPage;
use crate::stage::{CompiledStage, TransformStage, compile_stage};
use crate::traits::Source;
use async_trait::async_trait;
use futures::StreamExt;
use futures_core::Stream;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;

/// Source decorator that applies a fixed list of compiled stages to every
/// record. Emits `faucet_transform_*` metrics per page via
/// [`instrumented_apply_stages`].
///
/// # Example
///
/// ```no_run
/// use faucet_core::{RecordTransform, Source, TransformingSource};
/// use faucet_core::observability::Labels;
/// use faucet_core::stage::TransformStage;
/// use faucet_core::transform::KeyCaseMode;
///
/// # fn build_inner() -> Box<dyn Source> { unimplemented!() }
/// let inner: Box<dyn Source> = build_inner();
/// let wrapped = TransformingSource::new(
///     inner,
///     vec![TransformStage::Map(RecordTransform::KeysCase { mode: KeyCaseMode::Snake })],
///     Labels::for_named("rest"),
/// ).unwrap();
/// ```
pub struct TransformingSource {
    inner: Box<dyn Source>,
    stages: Vec<CompiledStage>,
    labels: Labels,
}

impl TransformingSource {
    /// Compile `stages` and wrap `inner`. Returns
    /// [`FaucetError::Transform`] if any stage's compilation fails (e.g.
    /// invalid regex in `RenameKeys`).
    pub fn new(
        inner: Box<dyn Source>,
        stages: Vec<TransformStage>,
        labels: Labels,
    ) -> Result<Self, FaucetError> {
        let compiled = stages
            .iter()
            .map(compile_stage)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            inner,
            stages: compiled,
            labels,
        })
    }
}

#[async_trait]
impl Source for TransformingSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let records = self.inner.fetch_with_context(ctx).await?;
        instrumented_apply_stages(records, &self.stages, &self.labels)
    }

    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let (records, bookmark) = self.inner.fetch_with_context_incremental(ctx).await?;
        let transformed = instrumented_apply_stages(records, &self.stages, &self.labels)?;
        Ok((transformed, bookmark))
    }

    fn stream_pages<'a>(
        &'a self,
        ctx: &'a HashMap<String, Value>,
        batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        Box::pin(async_stream::try_stream! {
            let mut inner_stream = self.inner.stream_pages(ctx, batch_size);
            while let Some(page) = inner_stream.next().await {
                let page = page?;
                let transformed = instrumented_apply_stages(
                    page.records,
                    &self.stages,
                    &self.labels,
                )?;
                yield StreamPage { records: transformed, bookmark: page.bookmark };
            }
        })
    }

    fn state_key(&self) -> Option<String> {
        self.inner.state_key()
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }

    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stage::TransformStage;
    use crate::transform::{KeyCaseMode, RecordTransform};
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct MockSource(Vec<Value>);

    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn fetch_with_context_transforms_records() {
        let inner: Box<dyn Source> = Box::new(MockSource(vec![json!({"FooBar": 1})]));
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
            })],
            Labels::for_named("test"),
        )
        .expect("compile succeeds");
        let out = wrapped.fetch_with_context(&HashMap::new()).await.unwrap();
        assert_eq!(out, vec![json!({"foo_bar": 1})]);
    }

    struct IncrementalSource {
        records: Vec<Value>,
        bookmark: Value,
    }

    #[async_trait]
    impl Source for IncrementalSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }

        async fn fetch_with_context_incremental(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), Some(self.bookmark.clone())))
        }
    }

    #[tokio::test]
    async fn fetch_with_context_incremental_transforms_and_preserves_bookmark() {
        let inner: Box<dyn Source> = Box::new(IncrementalSource {
            records: vec![json!({"FooBar": 1})],
            bookmark: json!("2026-05-28T00:00:00Z"),
        });
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
            })],
            Labels::for_named("test"),
        )
        .unwrap();
        let (records, bookmark) = wrapped
            .fetch_with_context_incremental(&HashMap::new())
            .await
            .unwrap();
        assert_eq!(records, vec![json!({"foo_bar": 1})]);
        assert_eq!(bookmark, Some(json!("2026-05-28T00:00:00Z")));
    }

    /// Emits records as N predetermined pages with the bookmark only on the last.
    /// Overrides `stream_pages` directly so the test catches whether the wrapper
    /// delegates to the native streaming path (correct) or falls back to the
    /// chunk-the-buffer default (wrong — the bug we're fixing).
    struct PagedSource {
        pages: Vec<Vec<Value>>,
        final_bookmark: Value,
    }

    #[async_trait]
    impl Source for PagedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.pages.iter().flatten().cloned().collect())
        }

        fn stream_pages<'a>(
            &'a self,
            _ctx: &'a HashMap<String, Value>,
            _batch_size: usize,
        ) -> Pin<Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>
        {
            let pages = self.pages.clone();
            let bookmark = self.final_bookmark.clone();
            Box::pin(async_stream::try_stream! {
                let n = pages.len();
                for (i, records) in pages.into_iter().enumerate() {
                    let bm = if i + 1 == n { Some(bookmark.clone()) } else { None };
                    yield StreamPage { records, bookmark: bm };
                }
            })
        }
    }

    #[tokio::test]
    async fn stream_pages_transforms_each_page_and_preserves_bookmarks() {
        let inner: Box<dyn Source> = Box::new(PagedSource {
            pages: vec![
                vec![json!({"FooBar": 1})],
                vec![json!({"FooBar": 2})],
                vec![json!({"FooBar": 3})],
            ],
            final_bookmark: json!("v1"),
        });
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
            })],
            Labels::for_named("test"),
        )
        .unwrap();

        let ctx = HashMap::new();
        let mut stream = wrapped.stream_pages(&ctx, 1000);
        let mut collected: Vec<StreamPage> = Vec::new();
        while let Some(page) = stream.next().await {
            collected.push(page.unwrap());
        }

        assert_eq!(collected.len(), 3);
        assert_eq!(collected[0].records, vec![json!({"foo_bar": 1})]);
        assert!(collected[0].bookmark.is_none());
        assert_eq!(collected[1].records, vec![json!({"foo_bar": 2})]);
        assert!(collected[1].bookmark.is_none());
        assert_eq!(collected[2].records, vec![json!({"foo_bar": 3})]);
        assert_eq!(collected[2].bookmark, Some(json!("v1")));
    }

    #[tokio::test]
    async fn stream_pages_passes_through_empty_records_page_with_bookmark() {
        struct EmptyWithBookmark;
        #[async_trait]
        impl Source for EmptyWithBookmark {
            async fn fetch_with_context(
                &self,
                _ctx: &HashMap<String, Value>,
            ) -> Result<Vec<Value>, FaucetError> {
                Ok(Vec::new())
            }
            fn stream_pages<'a>(
                &'a self,
                _ctx: &'a HashMap<String, Value>,
                _batch_size: usize,
            ) -> Pin<
                Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>,
            > {
                Box::pin(async_stream::try_stream! {
                    yield StreamPage { records: Vec::new(), bookmark: Some(json!("v1")) };
                })
            }
        }
        let wrapped = TransformingSource::new(
            Box::new(EmptyWithBookmark),
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
            })],
            Labels::for_named("test"),
        )
        .unwrap();
        let ctx = HashMap::new();
        let mut stream = wrapped.stream_pages(&ctx, 1000);
        let page = stream.next().await.unwrap().unwrap();
        assert!(page.records.is_empty());
        assert_eq!(page.bookmark, Some(json!("v1")));
        assert!(stream.next().await.is_none());
    }

    struct InstrumentedSource {
        started: Arc<AtomicBool>,
    }

    #[async_trait]
    impl Source for InstrumentedSource {
        async fn fetch_with_context(
            &self,
            _ctx: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(vec![])
        }
        fn connector_name(&self) -> &'static str {
            "instrumented"
        }
        fn state_key(&self) -> Option<String> {
            Some("instrumented::key".to_string())
        }
        async fn apply_start_bookmark(&self, _bookmark: Value) -> Result<(), FaucetError> {
            self.started.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn connector_name_state_key_and_start_bookmark_delegate_to_inner() {
        let started = Arc::new(AtomicBool::new(false));
        let inner = InstrumentedSource {
            started: started.clone(),
        };
        let wrapped = TransformingSource::new(
            Box::new(inner),
            vec![TransformStage::Map(RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
            })],
            Labels::for_named("test"),
        )
        .unwrap();
        assert_eq!(wrapped.connector_name(), "instrumented");
        assert_eq!(wrapped.state_key(), Some("instrumented::key".to_string()));
        wrapped.apply_start_bookmark(json!("bm")).await.unwrap();
        assert!(started.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn new_fails_fast_on_invalid_regex() {
        let inner: Box<dyn Source> = Box::new(MockSource(vec![]));
        let result = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::RenameKeys {
                pattern: "[invalid".to_string(),
                replacement: "x".to_string(),
            })],
            Labels::for_named("test"),
        );
        let err = match result {
            Ok(_) => panic!("invalid regex must fail at new()"),
            Err(e) => e,
        };
        assert!(matches!(err, FaucetError::Transform(_)));
    }

    #[tokio::test]
    async fn custom_closure_transform_runs() {
        let inner: Box<dyn Source> = Box::new(MockSource(vec![json!({"x": 1})]));
        let wrapped = TransformingSource::new(
            inner,
            vec![TransformStage::Map(RecordTransform::custom(|mut record| {
                if let Some(obj) = record.as_object_mut() {
                    obj.insert("added".to_string(), json!(true));
                }
                record
            }))],
            Labels::for_named("test"),
        )
        .unwrap();
        let out = wrapped.fetch_with_context(&HashMap::new()).await.unwrap();
        assert_eq!(out, vec![json!({"x": 1, "added": true})]);
    }

    #[tokio::test]
    async fn usable_as_boxed_dyn_source() {
        let inner: Box<dyn Source> = Box::new(MockSource(vec![json!({"FooBar": 1})]));
        let wrapped: Box<dyn Source> = Box::new(
            TransformingSource::new(
                inner,
                vec![TransformStage::Map(RecordTransform::KeysCase {
                    mode: KeyCaseMode::Snake,
                })],
                Labels::for_named("test"),
            )
            .unwrap(),
        );
        let out = wrapped.fetch_with_context(&HashMap::new()).await.unwrap();
        assert_eq!(out, vec![json!({"foo_bar": 1})]);
    }
}
