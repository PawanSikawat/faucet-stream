//! Metric registration + emit helper for schema-drift handling (issue #194).
//! `faucet_schema_drift_total` counts top-level columns detected as drifted on
//! a page, labelled by the policy mode that handled the drift and the drift
//! kind. Emitted from the pipeline loop where the per-page drift pass runs.

use metrics::{counter, describe_counter};

/// Register HELP text for the schema-drift metric. Idempotent — safe to call
/// more than once. Invoked from `install_observability` so the description is
/// present in `/metrics` from t=0, even before the first drift event. No-op
/// when no recorder is installed.
pub fn describe() {
    describe_counter!(
        "faucet_schema_drift_total",
        "Top-level columns detected as drifted, by policy mode and drift kind."
    );
}

/// Emit `faucet_schema_drift_total{pipeline,row,connector,mode,kind}` for a
/// detected drift: `count` columns of `kind` handled under policy `mode`.
/// No-op when `count == 0`.
pub fn schema_drift(
    pipeline: &str,
    row: &str,
    connector: &str,
    mode: &str,
    kind: &str,
    count: u64,
) {
    if count == 0 {
        return;
    }
    counter!(
        "faucet_schema_drift_total",
        "pipeline" => pipeline.to_string(),
        "row" => row.to_string(),
        "connector" => connector.to_string(),
        "mode" => mode.to_string(),
        "kind" => kind.to_string(),
    )
    .increment(count);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn describe_is_callable_and_idempotent() {
        // No recorder installed in this test → describe is a no-op; the call
        // must not panic regardless, and repeat calls are safe.
        describe();
        describe();
    }

    #[test]
    fn emit_does_not_panic_without_recorder() {
        // Metric emits are no-ops when no recorder is installed; assert the
        // helper stays panic-free.
        schema_drift("p", "r", "postgres", "evolve", "added", 2);
        // count == 0 short-circuits without emitting; must also not panic.
        schema_drift("p", "r", "postgres", "ignore", "removed", 0);
    }
}
