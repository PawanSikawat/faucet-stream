//! Prometheus surface for `faucet backfill` (#282).
//!
//! - `faucet_backfill_units_total{pipeline,outcome}` — counter;
//!   `outcome` ∈ `ok` | `err` | `skipped`.
//! - `faucet_backfill_progress_ratio{pipeline}` — gauge; terminal-done units
//!   over planned units for the in-flight backfill (0.0–1.0).
//!
//! Emission follows the CLI-side convention (`faucet_schedule_*`,
//! `faucet_pipeline_sla_*`): plain `metrics` macros against whatever recorder
//! `install_observability` installed. Labels stay low-cardinality (`pipeline`
//! only — never unit ids).

use metrics::{counter, describe_counter, describe_gauge, gauge};
use std::sync::Once;

static DESCRIBE: Once = Once::new();

fn describe() {
    DESCRIBE.call_once(|| {
        describe_counter!(
            "faucet_backfill_units_total",
            "Backfill window units finished, by outcome (ok | err | skipped)"
        );
        describe_gauge!(
            "faucet_backfill_progress_ratio",
            "Completed fraction of the in-flight backfill's planned units (0.0-1.0)"
        );
    });
}

/// Count one finished (or resume-skipped) unit.
pub(crate) fn record_unit(pipeline: &str, outcome: &'static str) {
    describe();
    counter!(
        "faucet_backfill_units_total",
        "pipeline" => pipeline.to_owned(),
        "outcome" => outcome,
    )
    .increment(1);
}

/// Publish the completed fraction of the planned units.
pub(crate) fn set_progress(pipeline: &str, done: usize, planned: usize) {
    describe();
    let ratio = if planned == 0 {
        1.0
    } else {
        done as f64 / planned as f64
    };
    gauge!(
        "faucet_backfill_progress_ratio",
        "pipeline" => pipeline.to_owned(),
    )
    .set(ratio);
}
