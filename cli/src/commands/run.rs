//! `faucet run` — load a pipeline config, build the connectors, and execute.

use crate::cli::RunArgs;
use crate::config::PipelineConfig;
use crate::error::CliResult;
use crate::registry::{build_sink, build_source};
use crate::state::build_state_store;
use crate::transforms::compile_transforms;
use async_trait::async_trait;
use faucet_core::transform::{CompiledTransform, apply_all, compile as compile_transform};
use faucet_core::{FaucetError, Pipeline, Sink, Source, StateStore};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Adapter that applies a list of compiled transforms to every record before
/// the sink sees them.
struct TransformingSource {
    inner: Box<dyn Source>,
    transforms: Vec<CompiledTransform>,
}

#[async_trait]
impl Source for TransformingSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let records = self.inner.fetch_with_context(ctx).await?;
        Ok(records
            .into_iter()
            .map(|r| apply_all(r, &self.transforms))
            .collect())
    }

    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let (records, bookmark) = self.inner.fetch_with_context_incremental(ctx).await?;
        let transformed = records
            .into_iter()
            .map(|r| apply_all(r, &self.transforms))
            .collect();
        Ok((transformed, bookmark))
    }

    fn state_key(&self) -> Option<String> {
        self.inner.state_key()
    }

    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }
}

/// Sink wrapper that drops any records past a soft cap. Used by `--limit`.
struct LimitedSink {
    inner: Box<dyn Sink>,
    remaining: AtomicUsize,
}

#[async_trait]
impl Sink for LimitedSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let remaining = self.remaining.load(Ordering::Relaxed);
        if remaining == 0 {
            return Ok(0);
        }
        let take = remaining.min(records.len());
        let slice = &records[..take];
        let written = self.inner.write_batch(slice).await?;
        // Subtract however many actually landed, never going below zero.
        self.remaining
            .fetch_sub(written.min(remaining), Ordering::Relaxed);
        Ok(written)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }
}

/// Sink that swallows every batch — used by `--dry-run` to exercise the source
/// without touching the configured sink.
struct CountingSink {
    seen: AtomicUsize,
}

#[async_trait]
impl Sink for CountingSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.seen.fetch_add(records.len(), Ordering::Relaxed);
        Ok(records.len())
    }
}

/// Execute the `run` subcommand.
pub async fn run(args: RunArgs) -> CliResult<()> {
    let cfg = PipelineConfig::from_path(&args.config)?;
    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        args.config
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });

    let source = build_source(&cfg.source.kind, cfg.source.config.clone()).await?;
    let transforms = compile_transforms(&cfg.transforms)?;
    let source: Box<dyn Source> = if transforms.is_empty() {
        source
    } else {
        let compiled = transforms
            .iter()
            .map(compile_transform)
            .collect::<Result<Vec<_>, _>>()?;
        Box::new(TransformingSource {
            inner: source,
            transforms: compiled,
        })
    };

    let sink: Box<dyn Sink> = if args.dry_run {
        tracing::info!("dry-run mode — sink writes are suppressed");
        Box::new(CountingSink {
            seen: AtomicUsize::new(0),
        })
    } else {
        build_sink(&cfg.sink.kind, cfg.sink.config.clone()).await?
    };

    let sink: Box<dyn Sink> = match args.limit {
        Some(n) => Box::new(LimitedSink {
            inner: sink,
            remaining: AtomicUsize::new(n),
        }),
        None => sink,
    };

    let state = match (&cfg.state, &args.state_path) {
        (Some(spec), None) => Some(build_state_store(spec).await?),
        (None, Some(path)) => {
            // Default to file backend at the override path when the YAML has no
            // state: block but the user passed `--state-path` anyway.
            Some(state_from_override(path).await?)
        }
        (Some(spec), Some(path)) => {
            // Honour explicit override on top of the configured backend.
            if spec.kind == "file" {
                Some(state_from_override(path).await?)
            } else {
                tracing::warn!(
                    state = %spec.kind,
                    "--state-path is only meaningful for the 'file' backend; ignoring override"
                );
                Some(build_state_store(spec).await?)
            }
        }
        (None, None) => None,
    };

    let pipeline = Pipeline::new(source.as_ref(), sink.as_ref());
    let pipeline = match state {
        Some(store) => pipeline.with_state_store(Arc::clone(&store)),
        None => pipeline,
    };

    let result = pipeline.run().await?;

    tracing::info!(
        pipeline = %pipeline_name,
        records_written = result.records_written,
        has_bookmark = result.bookmark.is_some(),
        "pipeline completed"
    );
    println!(
        "{}: wrote {} record{}",
        pipeline_name,
        result.records_written,
        if result.records_written == 1 { "" } else { "s" }
    );
    Ok(())
}

async fn state_from_override(path: &std::path::Path) -> CliResult<Arc<dyn StateStore>> {
    Ok(Arc::new(faucet_core::FileStateStore::new(path)) as Arc<dyn StateStore>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn limited_sink_caps_writes() {
        struct CountingInner(std::sync::Mutex<Vec<Value>>);
        #[async_trait]
        impl Sink for CountingInner {
            async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
                self.0.lock().unwrap().extend(records.iter().cloned());
                Ok(records.len())
            }
        }
        let inner: Box<dyn Sink> = Box::new(CountingInner(std::sync::Mutex::new(Vec::new())));
        let sink = LimitedSink {
            inner,
            remaining: AtomicUsize::new(2),
        };
        let r1 = sink
            .write_batch(&[json!({"a": 1}), json!({"a": 2}), json!({"a": 3})])
            .await
            .unwrap();
        assert_eq!(r1, 2);
        let r2 = sink.write_batch(&[json!({"a": 4})]).await.unwrap();
        assert_eq!(r2, 0);
    }

    #[tokio::test]
    async fn counting_sink_swallows_all_records() {
        let sink = CountingSink {
            seen: AtomicUsize::new(0),
        };
        let n = sink
            .write_batch(&[json!({}), json!({}), json!({})])
            .await
            .unwrap();
        assert_eq!(n, 3);
        assert_eq!(sink.seen.load(Ordering::Relaxed), 3);
    }
}
