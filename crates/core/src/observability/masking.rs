//! Observability wrapper around `masking::apply_masking`. Emits the
//! `faucet_masking_fields_total` metric from the returned outcome. The pure
//! logic lives in `crate::masking`.

use crate::masking::{CompiledMasking, MaskingOutcome, apply_masking};
use crate::observability::Labels;
use metrics::{Label, SharedString, counter};
use serde_json::Value;

/// Apply the masking pass and emit metrics. Infallible — masking rewrites
/// matching fields in place and never fails a run. Increments
/// `faucet_masking_fields_total{pipeline,row,rule,action,detector}` once per
/// masked field (`detector` is `""` for name-based matches).
pub fn instrumented_apply_masking(
    records: Vec<Value>,
    masking: &CompiledMasking,
    labels: &Labels,
) -> MaskingOutcome {
    let records_in = records.len();
    let span = tracing::info_span!(
        "faucet.masking.apply",
        pipeline = %labels.pipeline,
        row = %labels.row,
        run_id = %labels.run_id,
        rules = masking.rule_count(),
        records_in,
    );
    let _enter = span.enter();

    let outcome = apply_masking(records, masking);

    for hit in &outcome.hits {
        counter!(
            "faucet_masking_fields_total",
            vec![
                Label::new("pipeline", SharedString::from(labels.pipeline.to_string())),
                Label::new("row", SharedString::from(labels.row.to_string())),
                Label::new("rule", SharedString::from(hit.rule.clone())),
                Label::new("action", SharedString::const_str(hit.action)),
                Label::new(
                    "detector",
                    SharedString::const_str(hit.detector.unwrap_or("")),
                ),
            ]
        )
        .increment(1);
    }

    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::masking::MaskingSpec;
    use crate::observability::decorator::source_tests::{LOCK, snapshotter};
    use metrics_util::debugging::DebugValue;
    use serde_json::json;

    fn compiled(v: Value) -> CompiledMasking {
        let spec: MaskingSpec = serde_json::from_value(v).unwrap();
        CompiledMasking::compile(&spec).unwrap()
    }

    #[test]
    fn instrumented_returns_masked_records() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _snap = snapshotter();
        let out = instrumented_apply_masking(
            vec![json!({"email": "a@b.com"})],
            &compiled(json!({
                "rules": [{ "match": { "field_pattern": "^email$" },
                            "action": { "type": "redact" } }]
            })),
            &Labels::for_named("test"),
        );
        assert_eq!(out.records[0], json!({"email": "***"}));
        assert_eq!(out.hits.len(), 1);
    }

    #[test]
    fn emits_fields_total_with_rule_action_detector() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        instrumented_apply_masking(
            vec![json!({"contact": "a@b.com"})],
            &compiled(json!({
                "rules": [{ "name": "emails", "match": { "value_detector": "email" },
                            "action": { "type": "redact" } }]
            })),
            &Labels::for_named("test_masking_detector"),
        );
        let snapshot = snap.snapshot().into_vec();
        let found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_masking_fields_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "rule" && l.value() == "emails")
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "action" && l.value() == "redact")
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "detector" && l.value() == "email")
                && matches!(v, DebugValue::Counter(c) if *c >= 1)
        });
        assert!(
            found,
            "expected faucet_masking_fields_total{{rule=emails,action=redact,detector=email}}"
        );
    }
}
