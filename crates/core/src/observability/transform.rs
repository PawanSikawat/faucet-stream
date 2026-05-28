//! Wraps `transform::apply_all` with span + counter + histogram emission.

use crate::observability::labels::Labels;
use crate::observability::timer::DurationGuard;
use crate::transform::{CompiledTransform, apply_all};
use metrics::{Label, SharedString, counter};
use serde_json::Value;
use tracing::info_span;

/// Apply a sequence of compiled transforms to every record in `records`.
/// Emits one `faucet.transform.apply` span and one
/// `faucet_transform_records_total` counter increment per call (per page).
///
/// Returns [`FaucetError::Transform`](crate::FaucetError::Transform) if any
/// record's transform would silently lose data (a `flatten` / `keys_case`
/// key collision — #78/#28), incrementing `faucet_transform_errors_total`.
pub fn instrumented_apply_all(
    records: Vec<Value>,
    transforms: &[CompiledTransform],
    labels: &Labels,
) -> Result<Vec<Value>, crate::FaucetError> {
    let n = records.len();
    let span = info_span!(
        "faucet.transform.apply",
        pipeline = %labels.pipeline,
        row = %labels.row,
        run_id = %labels.run_id,
        records = n,
        transform_count = transforms.len(),
    );
    let _enter = span.enter();
    let metric_labels = vec![
        Label::new("pipeline", SharedString::from(labels.pipeline.to_string())),
        Label::new("row", SharedString::from(labels.row.to_string())),
    ];
    let _timer = DurationGuard::new("faucet_transform_duration_seconds", metric_labels.clone());
    let mut out: Vec<Value> = Vec::with_capacity(n);
    for r in records {
        match apply_all(r, transforms) {
            Ok(v) => out.push(v),
            Err(e) => {
                counter!("faucet_transform_errors_total", metric_labels.clone()).increment(1);
                return Err(e);
            }
        }
    }
    counter!("faucet_transform_records_total", metric_labels).increment(n as u64);
    Ok(out)
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
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let labels = Labels::new("p", "r", "rid");
        let t = compile(&RecordTransform::KeysCase {
            mode: crate::transform::KeyCaseMode::Snake,
        })
        .expect("KeysCase transform compiles");
        let result = instrumented_apply_all(
            vec![json!({"FooBar": 1}), json!({"BazQux": 2})],
            &[t],
            &labels,
        )
        .expect("keys_case transform succeeds");
        assert_eq!(result.len(), 2, "all records returned");
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
