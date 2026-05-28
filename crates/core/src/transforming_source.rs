//! Wrap any [`Source`] with a fixed list of [`RecordTransform`]s applied to
//! every emitted record. The canonical way for library callers to attach
//! transforms; the CLI uses this same type internally.

use crate::error::FaucetError;
use crate::observability::{Labels, instrumented_apply_all};
use crate::pipeline::StreamPage;
use crate::traits::Source;
use crate::transform::{CompiledTransform, RecordTransform, compile};
use async_trait::async_trait;
use futures::StreamExt;
use futures_core::Stream;
use serde_json::Value;
use std::collections::HashMap;
use std::pin::Pin;

/// Source decorator that applies a fixed list of compiled transforms to every
/// record. Emits `faucet_transform_*` metrics per page via
/// [`instrumented_apply_all`].
pub struct TransformingSource {
    inner: Box<dyn Source>,
    transforms: Vec<CompiledTransform>,
    labels: Labels,
}

impl TransformingSource {
    /// Compile `transforms` and wrap `inner`. Returns
    /// [`FaucetError::Transform`] if any transform's compilation fails (e.g.
    /// invalid regex in `RenameKeys`).
    pub fn new(
        inner: Box<dyn Source>,
        transforms: Vec<RecordTransform>,
        labels: Labels,
    ) -> Result<Self, FaucetError> {
        let compiled = transforms
            .iter()
            .map(compile)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            inner,
            transforms: compiled,
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
        instrumented_apply_all(records, &self.transforms, &self.labels)
    }

    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let (records, bookmark) = self.inner.fetch_with_context_incremental(ctx).await?;
        let transformed = instrumented_apply_all(records, &self.transforms, &self.labels)?;
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
                let transformed = instrumented_apply_all(
                    page.records,
                    &self.transforms,
                    &self.labels,
                )?;
                yield StreamPage { records: transformed, bookmark: page.bookmark };
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::KeyCaseMode;
    use serde_json::json;

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
            vec![RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
            }],
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
            vec![RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
            }],
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

    use crate::pipeline::StreamPage;
    use futures::StreamExt;
    use std::pin::Pin;

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
            vec![RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
            }],
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
            ) -> Pin<Box<dyn futures_core::Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>>
            {
                Box::pin(async_stream::try_stream! {
                    yield StreamPage { records: Vec::new(), bookmark: Some(json!("v1")) };
                })
            }
        }
        let wrapped = TransformingSource::new(
            Box::new(EmptyWithBookmark),
            vec![RecordTransform::KeysCase {
                mode: KeyCaseMode::Snake,
            }],
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
}
