//! Scheduler metrics, emitted via the `metrics` facade. `pipeline` is the only
//! label (low cardinality); per-row outcomes are covered by the pipeline-run
//! metrics in `faucet-core`. No-ops when no recorder is installed.

use chrono::{DateTime, Utc};
use metrics::{counter, gauge, histogram};
use std::time::Duration;

/// `outcome ∈ {"ok", "err", "skipped"}`.
pub fn run_outcome(pipeline: &str, outcome: &'static str) {
    counter!("faucet_schedule_runs_total", "pipeline" => pipeline.to_string(), "outcome" => outcome)
        .increment(1);
}

/// `policy ∈ {"skip", "queue", "forbid"}`.
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
