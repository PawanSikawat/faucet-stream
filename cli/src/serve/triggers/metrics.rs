//! `faucet_serve_trigger_*` metrics. Low-cardinality labels only (`trigger`
//! name, `type`). Mirrors `crate::schedule::metrics`.

use metrics::{counter, describe_counter, describe_gauge, gauge};

pub fn describe() {
    describe_counter!("faucet_serve_triggers_fired_total", "Events that warranted a fire, by trigger/type");
    describe_counter!("faucet_serve_trigger_runs_enqueued_total", "Fires that enqueued a run");
    describe_counter!("faucet_serve_trigger_runs_coalesced_total", "Fires deduped/coalesced (idempotency/debounce)");
    describe_counter!("faucet_serve_trigger_runs_dropped_total", "Fires dropped (e.g. queue_full)");
    describe_counter!("faucet_serve_trigger_errors_total", "Watcher poll/serve/fire errors");
    describe_gauge!("faucet_serve_triggers_active", "Number of spawned watchers");
    describe_gauge!("faucet_serve_trigger_healthy", "1 if the watcher is healthy, else 0");
    describe_gauge!("faucet_serve_trigger_last_fire_unix_seconds", "Unix time of the watcher's last fire");
}

pub fn active(n: usize) {
    gauge!("faucet_serve_triggers_active").set(n as f64);
}
pub fn fired(trigger: &str, kind: &'static str) {
    counter!("faucet_serve_triggers_fired_total", "trigger" => trigger.to_string(), "type" => kind).increment(1);
}
pub fn enqueued(trigger: &str) {
    counter!("faucet_serve_trigger_runs_enqueued_total", "trigger" => trigger.to_string()).increment(1);
}
pub fn coalesced(trigger: &str) {
    counter!("faucet_serve_trigger_runs_coalesced_total", "trigger" => trigger.to_string()).increment(1);
}
pub fn dropped(trigger: &str, reason: &'static str) {
    counter!("faucet_serve_trigger_runs_dropped_total", "trigger" => trigger.to_string(), "reason" => reason).increment(1);
}
pub fn error(trigger: &str, kind: &'static str) {
    counter!("faucet_serve_trigger_errors_total", "trigger" => trigger.to_string(), "type" => kind).increment(1);
}
pub fn healthy(trigger: &str, ok: bool) {
    gauge!("faucet_serve_trigger_healthy", "trigger" => trigger.to_string()).set(if ok { 1.0 } else { 0.0 });
}
pub fn last_fire(trigger: &str, unix_secs: i64) {
    gauge!("faucet_serve_trigger_last_fire_unix_seconds", "trigger" => trigger.to_string()).set(unix_secs as f64);
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::{DebuggingRecorder, Snapshotter};

    #[test]
    fn emits_fired_counter() {
        let recorder = DebuggingRecorder::new();
        let snap: Snapshotter = recorder.snapshotter();
        with_local_recorder(&recorder, || {
            fired("t", "webhook");
        });
        let metrics = snap.snapshot().into_vec();
        assert!(
            metrics.iter().any(|(k, _, _, _)| k.key().name() == "faucet_serve_triggers_fired_total"),
            "fired counter not emitted"
        );
    }
}
