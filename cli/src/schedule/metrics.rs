//! Scheduler metrics, emitted via the `metrics` facade. `pipeline` is the only
//! label (low cardinality); per-row outcomes are covered by the pipeline-run
//! metrics in `faucet-core`. No-ops when no recorder is installed.

use chrono::{DateTime, Utc};
use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};
use std::time::Duration;

/// Register HELP text for every scheduler metric. Idempotent — safe to call
/// more than once. Called from the scheduler loop at startup so the series'
/// descriptions are present in `/metrics` from t=0, even before the first tick.
pub fn describe() {
    describe_counter!(
        "faucet_schedule_runs_total",
        "Scheduled pipeline runs, by outcome (ok|err|skipped)."
    );
    describe_counter!(
        "faucet_schedule_overlaps_total",
        "Scheduler ticks that overlapped an in-flight run, by policy (skip|queue|forbid)."
    );
    describe_counter!(
        "faucet_schedule_reloads_total",
        "Hot config reloads (SIGHUP), by outcome (ok|error)."
    );
    describe_gauge!(
        "faucet_schedule_next_tick_unix_seconds",
        "Unix timestamp of the next scheduled tick."
    );
    describe_gauge!(
        "faucet_schedule_runs_in_flight",
        "Scheduled runs currently executing (0 or 1)."
    );
    describe_gauge!(
        "faucet_schedule_consecutive_failures",
        "Consecutive failed scheduled runs since the last success."
    );
    describe_gauge!(
        "faucet_schedule_heartbeat_unix_seconds",
        "Unix timestamp the scheduler loop last ran (alert if it stalls)."
    );
    describe_gauge!(
        "faucet_schedule_last_run_started_unix_seconds",
        "Unix timestamp the most recent run started."
    );
    describe_gauge!(
        "faucet_schedule_last_run_completed_unix_seconds",
        "Unix timestamp the most recent run completed."
    );
    describe_gauge!(
        "faucet_schedule_last_run_duration_seconds",
        "Wall-clock duration of the most recent run."
    );
    describe_histogram!(
        "faucet_schedule_run_lateness_seconds",
        "How late each run started relative to its scheduled tick."
    );
}

/// `outcome ∈ {"ok", "err", "skipped"}`.
pub fn run_outcome(pipeline: &str, outcome: &'static str) {
    counter!("faucet_schedule_runs_total", "pipeline" => pipeline.to_string(), "outcome" => outcome)
        .increment(1);
}

/// `policy ∈ {"skip", "queue", "forbid"}`.
/// Count a hot config reload attempt (`outcome` = `ok` | `error`).
pub fn reload(pipeline: &str, outcome: &'static str) {
    counter!("faucet_schedule_reloads_total", "pipeline" => pipeline.to_string(), "outcome" => outcome)
        .increment(1);
}

pub fn overlap(pipeline: &str, policy: &'static str) {
    counter!("faucet_schedule_overlaps_total", "pipeline" => pipeline.to_string(), "policy" => policy)
        .increment(1);
}

pub fn next_tick(pipeline: &str, when: DateTime<Utc>) {
    gauge!("faucet_schedule_next_tick_unix_seconds", "pipeline" => pipeline.to_string())
        .set(when.timestamp() as f64);
}

pub fn in_flight(pipeline: &str, n: u64) {
    gauge!("faucet_schedule_runs_in_flight", "pipeline" => pipeline.to_string()).set(n as f64);
}

pub fn consecutive_failures(pipeline: &str, n: u64) {
    gauge!("faucet_schedule_consecutive_failures", "pipeline" => pipeline.to_string())
        .set(n as f64);
}

pub fn heartbeat(pipeline: &str, now: DateTime<Utc>) {
    gauge!("faucet_schedule_heartbeat_unix_seconds", "pipeline" => pipeline.to_string())
        .set(now.timestamp() as f64);
}

pub fn last_run_started(pipeline: &str, when: DateTime<Utc>) {
    gauge!("faucet_schedule_last_run_started_unix_seconds", "pipeline" => pipeline.to_string())
        .set(when.timestamp() as f64);
}

pub fn last_run_completed(pipeline: &str, when: DateTime<Utc>) {
    gauge!("faucet_schedule_last_run_completed_unix_seconds", "pipeline" => pipeline.to_string())
        .set(when.timestamp() as f64);
}

pub fn last_run_duration(pipeline: &str, d: Duration) {
    gauge!("faucet_schedule_last_run_duration_seconds", "pipeline" => pipeline.to_string())
        .set(d.as_secs_f64());
}

/// `late` may be negative if a run started slightly early; clamp to 0.
pub fn lateness(pipeline: &str, late: chrono::Duration) {
    let secs = (late.num_milliseconds() as f64 / 1000.0).max(0.0);
    histogram!("faucet_schedule_run_lateness_seconds", "pipeline" => pipeline.to_string())
        .record(secs);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    /// Find the latest gauge value emitted for `name` with the given `pipeline`
    /// label in a `DebuggingRecorder` snapshot.
    fn gauge_value(
        snapshot: metrics_util::debugging::Snapshot,
        name: &str,
        pipeline: &str,
    ) -> Option<f64> {
        snapshot
            .into_vec()
            .into_iter()
            .find_map(|(key, _u, _d, v)| {
                let k = key.key();
                let labelled = k
                    .labels()
                    .any(|l| l.key() == "pipeline" && l.value() == pipeline);
                if k.name() == name && labelled {
                    match v {
                        DebugValue::Gauge(g) => Some(g.into_inner()),
                        _ => None,
                    }
                } else {
                    None
                }
            })
    }

    #[test]
    fn in_flight_sets_gauge() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        with_local_recorder(&recorder, || {
            in_flight("p", 1);
            in_flight("p", 0);
        });
        assert_eq!(
            gauge_value(snap.snapshot(), "faucet_schedule_runs_in_flight", "p"),
            Some(0.0),
            "in_flight(0) must leave the gauge at 0"
        );
    }

    #[test]
    fn consecutive_failures_sets_gauge() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        with_local_recorder(&recorder, || {
            consecutive_failures("p", 0);
        });
        assert_eq!(
            gauge_value(snap.snapshot(), "faucet_schedule_consecutive_failures", "p"),
            Some(0.0),
            "consecutive_failures(0) must register the series at 0"
        );
    }

    /// Mirrors what the scheduler does at startup (item 2): pre-emit both gauges
    /// so the series exist in `/metrics` before the first dispatch.
    #[test]
    fn startup_preemit_registers_both_gauges_at_zero() {
        let recorder = DebuggingRecorder::new();
        let snap = recorder.snapshotter();
        with_local_recorder(&recorder, || {
            describe();
            in_flight("p", 0);
            consecutive_failures("p", 0);
        });
        assert_eq!(
            gauge_value(snap.snapshot(), "faucet_schedule_runs_in_flight", "p"),
            Some(0.0),
            "runs_in_flight must exist at 0 from startup"
        );
        assert_eq!(
            gauge_value(snap.snapshot(), "faucet_schedule_consecutive_failures", "p"),
            Some(0.0),
            "consecutive_failures must exist at 0 from startup"
        );
    }
}
