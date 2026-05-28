//! Wrap any [`Source`] with a fixed list of [`RecordTransform`]s applied to
//! every emitted record. The canonical way for library callers to attach
//! transforms; the CLI uses this same type internally.

use crate::error::FaucetError;
use crate::observability::{Labels, instrumented_apply_all};
use crate::traits::Source;
use crate::transform::{CompiledTransform, RecordTransform, compile};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::HashMap;

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
}
