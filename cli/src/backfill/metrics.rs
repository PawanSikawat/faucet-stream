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

/// Completed fraction of the planned units, clamped to `[0.0, 1.0]`.
///
/// Pure so the divide-by-zero guard and clamping are unit-testable without the
/// metrics recorder: an empty plan (`planned == 0`) is treated as fully done
/// (`1.0`), and `done > planned` (a resume double-count) never exceeds `1.0`.
fn progress_ratio(done: usize, planned: usize) -> f64 {
    if planned == 0 {
        1.0
    } else {
        (done as f64 / planned as f64).min(1.0)
    }
}

/// Publish the completed fraction of the planned units.
pub(crate) fn set_progress(pipeline: &str, done: usize, planned: usize) {
    describe();
    gauge!(
        "faucet_backfill_progress_ratio",
        "pipeline" => pipeline.to_owned(),
    )
    .set(progress_ratio(done, planned));
}

#[cfg(test)]
mod tests {
    use super::progress_ratio;

    #[test]
    fn empty_plan_is_fully_done() {
        // 0/0 must not be NaN — an empty plan is complete.
        assert_eq!(progress_ratio(0, 0), 1.0);
        assert_eq!(progress_ratio(5, 0), 1.0);
    }

    #[test]
    fn none_done_is_zero() {
        assert_eq!(progress_ratio(0, 4), 0.0);
    }

    #[test]
    fn all_done_is_one() {
        assert_eq!(progress_ratio(4, 4), 1.0);
    }

    #[test]
    fn partial_is_the_fraction() {
        assert_eq!(progress_ratio(1, 4), 0.25);
        assert_eq!(progress_ratio(3, 4), 0.75);
    }

    #[test]
    fn over_count_is_clamped_to_one() {
        assert_eq!(progress_ratio(9, 4), 1.0);
    }

    #[test]
    fn never_produces_nan() {
        for (d, p) in [(0, 0), (0, 1), (1, 1), (3, 7), (100, 1)] {
            assert!(!progress_ratio(d, p).is_nan(), "nan for {d}/{p}");
        }
    }
}
