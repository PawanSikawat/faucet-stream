//! Wraps `transform::apply_all` with span + counter + histogram emission.

use crate::observability::labels::Labels;
use crate::observability::timer::DurationGuard;
use crate::transform::{CompiledTransform, apply_all};
use metrics::{Label, SharedString, counter};
use serde_json::Value;
use tracing::info_span;

/// Drop-in wrapper around `transform::apply_all` that emits the
/// `faucet.transform.apply` span and the documented metric surface.
///
/// `apply_all` is infallible (it returns the transformed `Value`); the
/// `faucet_transform_errors_total` counter from the spec exists as a
/// reserved name for future transform variants that may return `Result`.
/// Today this function only emits the records counter and duration
/// histogram.
pub fn instrumented_apply_all(
    record: Value,
    transforms: &[CompiledTransform],
    labels: &Labels,
) -> Value {
    let span = info_span!(
        "faucet.transform.apply",
        pipeline = %labels.pipeline,
        row = %labels.row,
        run_id = %labels.run_id,
        transform_count = transforms.len(),
    );
    let _enter = span.enter();
    let metric_labels = vec![
        Label::new("pipeline", SharedString::from(labels.pipeline.to_string())),
        Label::new("row", SharedString::from(labels.row.to_string())),
    ];
    let _timer = DurationGuard::new("faucet_transform_duration_seconds", metric_labels.clone());
    let out = apply_all(record, transforms);
    counter!("faucet_transform_records_total", metric_labels).increment(1);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observability::decorator::source_tests::{LOCK, snapshotter};
    use crate::transform::{RecordTransform, compile};
    use metrics_util::debugging::DebugValue;
    use serde_json::json;

    #[test]
    fn increments_records_counter() {
        let _g = LOCK.lock().unwrap();
        let snap = snapshotter();
        let labels = Labels::new("p", "r", "rid");
        let t =
            compile(&RecordTransform::KeysToSnakeCase).expect("KeysToSnakeCase transform compiles");
        instrumented_apply_all(json!({"FooBar": 1}), &[t], &labels);
        let snapshot = snap.snapshot();
        let found = snapshot.into_vec().into_iter().any(|(key, _u, _d, v)| {
            key.key().name() == "faucet_transform_records_total"
                && matches!(v, DebugValue::Counter(c) if c >= 1)
        });
        assert!(
            found,
            "expected faucet_transform_records_total counter to fire"
        );
    }
}
