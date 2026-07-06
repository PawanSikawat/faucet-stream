//! The runtime event a pipeline emits toward the notifier (#280).
//!
//! [`NotifyEvent`] is the pure, channel-agnostic description of something that
//! happened. Constructors fix the canonical severity per kind so callers at the
//! emit sites (executor, SLA pass, scheduler) don't have to. Rendering into a
//! Slack / PagerDuty / webhook body lives in [`crate::notify::render`].

use super::spec::{EventKind, Severity};
use serde_json::{Map, Value};

/// A single thing worth notifying about.
#[derive(Debug, Clone)]
pub struct NotifyEvent {
    pub kind: EventKind,
    pub severity: Severity,
    /// Pipeline name (metric-label identity).
    pub pipeline: String,
    /// Matrix row id (`""` for non-matrix runs).
    pub row: String,
    /// Short one-line summary.
    pub title: String,
    /// Human-readable detail.
    pub message: String,
    /// Structured context rendered into channel payloads (never contains
    /// secrets — the emit sites pass only safe scalars).
    pub details: Map<String, Value>,
}

impl NotifyEvent {
    fn base(
        kind: EventKind,
        severity: Severity,
        pipeline: impl Into<String>,
        row: impl Into<String>,
        title: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            kind,
            severity,
            pipeline: pipeline.into(),
            row: row.into(),
            title: title.into(),
            message: message.into(),
            details: Map::new(),
        }
    }

    fn with(mut self, key: &str, value: Value) -> Self {
        self.details.insert(key.to_string(), value);
        self
    }

    /// Correlation key for PagerDuty incident open/resolve pairing and for
    /// coalescing per (pipeline, row). A `run_success` resolves the incident a
    /// prior `run_failure` opened on the same key.
    pub fn incident_key(&self) -> String {
        format!("{}:{}", self.pipeline, self.row)
    }

    /// Per-rule coalesce key: the same kind repeating for the same (pipeline,
    /// row) coalesces within a rule's `dedupe_window_secs`.
    pub fn dedupe_key(&self) -> String {
        format!("{}:{}:{}", self.kind.as_str(), self.pipeline, self.row)
    }

    /// True when this event opens an incident (a failure-class event that a
    /// later success should resolve).
    pub fn opens_incident(&self) -> bool {
        matches!(
            self.kind,
            EventKind::RunFailure | EventKind::CircuitOpen | EventKind::ContractAbort
        )
    }

    /// True when this event closes any open incident for its `incident_key`.
    pub fn closes_incident(&self) -> bool {
        matches!(self.kind, EventKind::RunSuccess)
    }

    // ── Constructors (canonical severity per kind) ───────────────────────────

    pub fn run_failure(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        error_kind: &str,
        message: impl Into<String>,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::RunFailure,
            Severity::Error,
            p.clone(),
            row,
            format!("Pipeline `{p}` failed"),
            message,
        )
        .with("error_kind", Value::String(error_kind.to_string()))
    }

    pub fn run_success(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        rows_written: u64,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::RunSuccess,
            Severity::Info,
            p.clone(),
            row,
            format!("Pipeline `{p}` succeeded"),
            format!("Run completed, {rows_written} records written."),
        )
        .with("records_written", Value::from(rows_written))
    }

    pub fn sla_breach(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        sla_kind: &str,
        message: impl Into<String>,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::SlaBreach,
            Severity::Warning,
            p.clone(),
            row,
            format!("SLA breach ({sla_kind}) on `{p}`"),
            message,
        )
        .with("sla_kind", Value::String(sla_kind.to_string()))
    }

    pub fn circuit_open(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        failures: u32,
        cooldown_secs: u64,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::CircuitOpen,
            Severity::Critical,
            p.clone(),
            row,
            format!("Circuit breaker open on `{p}`"),
            format!("Tripped after {failures} consecutive failures; cooling down {cooldown_secs}s."),
        )
        .with("failures", Value::from(failures))
        .with("cooldown_secs", Value::from(cooldown_secs))
    }

    pub fn contract_abort(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::ContractAbort,
            Severity::Error,
            p.clone(),
            row,
            format!("Data contract breach aborted `{p}`"),
            message,
        )
    }

    pub fn dlq_threshold(
        pipeline: impl Into<String>,
        row: impl Into<String>,
        records_dlq: u64,
    ) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::DlqThreshold,
            Severity::Warning,
            p.clone(),
            row,
            format!("DLQ threshold reached on `{p}`"),
            format!("{records_dlq} records were routed to the dead-letter queue."),
        )
        .with("records_dlq", Value::from(records_dlq))
    }

    pub fn scheduler_stuck(pipeline: impl Into<String>, message: impl Into<String>) -> Self {
        let p = pipeline.into();
        Self::base(
            EventKind::SchedulerStuck,
            Severity::Critical,
            p.clone(),
            String::new(),
            format!("Scheduler stuck for `{p}`"),
            message,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_fix_severity_and_kind() {
        assert_eq!(
            NotifyEvent::run_failure("p", "", "sink", "boom").severity,
            Severity::Error
        );
        assert_eq!(
            NotifyEvent::circuit_open("p", "", 5, 30).severity,
            Severity::Critical
        );
        assert_eq!(
            NotifyEvent::run_success("p", "", 10).severity,
            Severity::Info
        );
        assert_eq!(
            NotifyEvent::sla_breach("p", "", "staleness", "old").severity,
            Severity::Warning
        );
        assert_eq!(
            NotifyEvent::scheduler_stuck("p", "no beat").kind,
            EventKind::SchedulerStuck
        );
    }

    #[test]
    fn incident_and_dedupe_keys() {
        let f = NotifyEvent::run_failure("p", "r1", "sink", "boom");
        assert_eq!(f.incident_key(), "p:r1");
        assert_eq!(f.dedupe_key(), "run_failure:p:r1");
        assert!(f.opens_incident());
        assert!(!f.closes_incident());

        let s = NotifyEvent::run_success("p", "r1", 3);
        assert_eq!(s.incident_key(), "p:r1"); // matches the failure it resolves
        assert!(s.closes_incident());
        assert!(!s.opens_incident());
    }

    #[test]
    fn details_carry_structured_context() {
        let e = NotifyEvent::dlq_threshold("p", "", 42);
        assert_eq!(e.details.get("records_dlq").unwrap(), &Value::from(42u64));
    }
}
