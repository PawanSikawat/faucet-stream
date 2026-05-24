//! Decorator wrapping a `&dyn StateStore` to emit spans + metrics around
//! every get / put / delete call.

use crate::error::FaucetError;
use crate::observability::decorator::error_kind;
use crate::observability::labels::Labels;
use crate::observability::timer::DurationGuard;
use crate::state::StateStore;
use async_trait::async_trait;
use metrics::{Label, SharedString, counter};
use serde_json::Value;
use std::sync::Arc;
use tracing::{Instrument, info_span};

/// Wraps an `Arc<dyn StateStore>` and emits faucet.state.* spans + metrics
/// around every operation.
pub struct InstrumentedStateStore {
    inner: Arc<dyn StateStore>,
    labels: Labels,
}

impl InstrumentedStateStore {
    pub fn new(inner: Arc<dyn StateStore>, labels: Labels) -> Self {
        Self { inner, labels }
    }

    fn base_labels(&self) -> Vec<Label> {
        vec![
            Label::new(
                "pipeline",
                SharedString::from(self.labels.pipeline.to_string()),
            ),
            Label::new("row", SharedString::from(self.labels.row.to_string())),
        ]
    }

    fn error_labels(&self, op: &'static str, kind: &'static str) -> Vec<Label> {
        let mut l = self.base_labels();
        l.push(Label::new("op", SharedString::const_str(op)));
        l.push(Label::new("kind", SharedString::const_str(kind)));
        l
    }
}

#[async_trait]
impl StateStore for InstrumentedStateStore {
    async fn get(&self, key: &str) -> Result<Option<Value>, FaucetError> {
        let span = info_span!("faucet.state.get",
            pipeline = %self.labels.pipeline,
            row = %self.labels.row,
            run_id = %self.labels.run_id,
            key = key,
        );
        let base = self.base_labels();
        let _timer = DurationGuard::new("faucet_state_get_duration_seconds", base.clone());
        let result = self.inner.get(key).instrument(span).await;
        match result {
            Ok(Some(v)) => {
                let mut l = base;
                l.push(Label::new("outcome", SharedString::const_str("hit")));
                counter!("faucet_state_get_total", l).increment(1);
                Ok(Some(v))
            }
            Ok(None) => {
                let mut l = base;
                l.push(Label::new("outcome", SharedString::const_str("miss")));
                counter!("faucet_state_get_total", l).increment(1);
                Ok(None)
            }
            Err(e) => {
                counter!(
                    "faucet_state_errors_total",
                    self.error_labels("get", error_kind(&e))
                )
                .increment(1);
                Err(e)
            }
        }
    }

    async fn put(&self, key: &str, value: &Value) -> Result<(), FaucetError> {
        let span = info_span!("faucet.state.put",
            pipeline = %self.labels.pipeline,
            row = %self.labels.row,
            run_id = %self.labels.run_id,
            key = key,
        );
        let base = self.base_labels();
        let _timer = DurationGuard::new("faucet_state_put_duration_seconds", base.clone());
        let result = self.inner.put(key, value).instrument(span).await;
        match result {
            Ok(()) => {
                counter!("faucet_state_put_total", base).increment(1);
                Ok(())
            }
            Err(e) => {
                counter!(
                    "faucet_state_errors_total",
                    self.error_labels("put", error_kind(&e))
                )
                .increment(1);
                Err(e)
            }
        }
    }

    async fn delete(&self, key: &str) -> Result<(), FaucetError> {
        let span = info_span!("faucet.state.delete",
            pipeline = %self.labels.pipeline,
            row = %self.labels.row,
            run_id = %self.labels.run_id,
            key = key,
        );
        let base = self.base_labels();
        let _timer = DurationGuard::new("faucet_state_delete_duration_seconds", base.clone());
        let result = self.inner.delete(key).instrument(span).await;
        match result {
            Ok(()) => {
                counter!("faucet_state_delete_total", base).increment(1);
                Ok(())
            }
            Err(e) => {
                counter!(
                    "faucet_state_errors_total",
                    self.error_labels("delete", error_kind(&e))
                )
                .increment(1);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::decorator::source_tests::{LOCK, labels, snapshotter};
    use crate::state::MemoryStateStore;
    use metrics_util::debugging::DebugValue;
    use serde_json::json;

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn get_records_hit_outcome() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let inner: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        inner.put("k", &json!("v")).await.unwrap();
        let wrapped = InstrumentedStateStore::new(Arc::clone(&inner), labels());
        let result = wrapped.get("k").await.unwrap();
        assert_eq!(result, Some(json!("v")));
        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(|(key, _u, _d, v)| {
            key.key().name() == "faucet_state_get_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == "hit")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(found, "expected faucet_state_get_total{{outcome=hit}}");
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn get_records_miss_outcome() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let inner: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
        let wrapped = InstrumentedStateStore::new(inner, labels());
        assert!(wrapped.get("absent").await.unwrap().is_none());
        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(|(key, _u, _d, v)| {
            key.key().name() == "faucet_state_get_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "outcome" && l.value() == "miss")
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(found, "expected faucet_state_get_total{{outcome=miss}}");
    }
}
