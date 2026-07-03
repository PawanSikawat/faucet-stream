//! Observability wrapper around `contract::apply_contract`. Emits the
//! `faucet_contract_*` metrics from the returned outcome. The pure logic
//! lives in `crate::contract`.

use crate::contract::{CompiledContract, ContractOutcome, ContractViolation, apply_contract};
use crate::error::FaucetError;
use crate::observability::Labels;
use metrics::{Label, SharedString, counter};
use serde_json::Value;

/// Apply the contract pass and emit metrics. Returns the same outcome as
/// [`apply_contract`]; on a `fail`-policy breach, increments
/// `faucet_contract_aborts_total` before propagating the error.
pub fn instrumented_apply_contract(
    records: Vec<Value>,
    contract: &CompiledContract,
    labels: &Labels,
) -> Result<ContractOutcome, FaucetError> {
    let records_in = records.len();
    let span = tracing::info_span!(
        "faucet.contract.apply",
        pipeline = %labels.pipeline,
        row = %labels.row,
        run_id = %labels.run_id,
        version = %contract.version,
        records_in,
    );
    let _enter = span.enter();

    let base = || -> Vec<Label> {
        vec![
            Label::new("pipeline", SharedString::from(labels.pipeline.to_string())),
            Label::new("row", SharedString::from(labels.row.to_string())),
        ]
    };
    let violation_labels = |v: &ContractViolation, mode: &'static str| -> Vec<Label> {
        let mut l = base();
        l.push(Label::new(
            "field",
            SharedString::from(v.field.clone().unwrap_or_default()),
        ));
        l.push(Label::new("rule", SharedString::const_str(v.rule)));
        l.push(Label::new("mode", SharedString::const_str(mode)));
        l
    };

    let result = apply_contract(records, contract);

    match &result {
        Ok(outcome) => {
            for q in &outcome.quarantined {
                counter!(
                    "faucet_contract_violations_total",
                    violation_labels(&q.violation, "quarantine")
                )
                .increment(1);
            }
            for v in &outcome.warned {
                counter!(
                    "faucet_contract_violations_total",
                    violation_labels(v, "warn")
                )
                .increment(1);
            }
        }
        Err(FaucetError::ContractViolation { .. }) => {
            counter!("faucet_contract_aborts_total", base()).increment(1);
        }
        Err(_) => {}
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contract::ContractSpec;
    use crate::observability::decorator::source_tests::{LOCK, snapshotter};
    use metrics_util::debugging::DebugValue;
    use serde_json::json;

    fn compiled(on_breach: &str) -> CompiledContract {
        let spec: ContractSpec = serde_json::from_value(json!({
            "version": "1.0.0",
            "on_breach": on_breach,
            "fields": [{ "name": "id", "type": "integer" }]
        }))
        .unwrap();
        CompiledContract::compile(&spec).unwrap()
    }

    #[test]
    fn instrumented_returns_same_outcome() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let _snap = snapshotter();
        let out = instrumented_apply_contract(
            vec![json!({"id": 1}), json!({"id": "x"})],
            &compiled("quarantine"),
            &Labels::for_named("test"),
        )
        .unwrap();
        assert_eq!(out.survivors.len(), 1);
        assert_eq!(out.quarantined.len(), 1);
    }

    #[test]
    fn emits_violation_counter_with_rule_and_mode() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        instrumented_apply_contract(
            vec![json!({"id": "x"})],
            &compiled("warn"),
            &Labels::for_named("test_contract_warn"),
        )
        .unwrap();
        let snapshot = snap.snapshot().into_vec();
        let found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_contract_violations_total"
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "rule" && l.value() == "type")
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "mode" && l.value() == "warn")
                && key
                    .key()
                    .labels()
                    .any(|l| l.key() == "field" && l.value() == "id")
                && matches!(v, DebugValue::Counter(c) if *c >= 1)
        });
        assert!(
            found,
            "expected faucet_contract_violations_total{{rule=type,mode=warn,field=id}}"
        );
    }

    #[test]
    fn emits_aborts_total_on_fail_breach() {
        let _g = LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let snap = snapshotter();
        let err = instrumented_apply_contract(
            vec![json!({"id": "x"})],
            &compiled("fail"),
            &Labels::for_named("test_contract_abort"),
        )
        .unwrap_err();
        assert!(matches!(err, FaucetError::ContractViolation { .. }));
        let snapshot = snap.snapshot().into_vec();
        let found = snapshot.iter().any(|(key, _, _, v)| {
            key.key().name() == "faucet_contract_aborts_total"
                && matches!(v, DebugValue::Counter(c) if *c >= 1)
        });
        assert!(found, "expected faucet_contract_aborts_total >= 1");
    }
}
