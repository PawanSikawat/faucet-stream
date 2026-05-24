//! Dead-letter queue (DLQ) wiring shared by the pipeline runner.
//!
//! The types defined here are config-shaped: they describe *what* the
//! pipeline should do with row-level failures, not *how* the routing is
//! executed. The execution lives in [`run_stream`](crate::run_stream).
//!
//! See `docs/superpowers/specs/2026-05-24-dlq-design.md`.

use crate::FaucetError;
use crate::traits::Sink;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Policy applied when a sink reports an outer failure (the whole batch
/// failed, no per-row info).
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnBatchError {
    /// Surface the underlying [`FaucetError`] and fail the pipeline (default).
    #[default]
    Propagate,
    /// Treat every row in the failed page as a DLQ candidate. Unsafe with
    /// best-effort APIs that haven't overridden
    /// [`Sink::write_batch_partial`] — already-committed rows would land in
    /// the DLQ as duplicates. Use with atomic sinks (single-statement
    /// INSERT, file writes) where the failure mode is "nothing landed".
    DlqAll,
}

/// Pipeline-level DLQ wiring.
#[derive(Clone)]
pub struct DlqConfig {
    /// Sink that receives DLQ envelopes.
    pub sink: Arc<dyn Sink>,
    /// What to do when the main sink fails wholesale.
    pub on_batch_error: OnBatchError,
    /// Per-page failure budget. `None` = unlimited.
    pub max_failures_per_page: Option<usize>,
    /// Cumulative failure budget across the run. `None` = unlimited.
    pub max_failures_total: Option<usize>,
    /// Always `true` in v1. Reserved for a future "headers-only" mode.
    pub include_original_payload: bool,
}

impl DlqConfig {
    /// Convenience constructor: `propagate` policy, no budgets, payload
    /// included.
    pub fn new(sink: Arc<dyn Sink>) -> Self {
        Self {
            sink,
            on_batch_error: OnBatchError::Propagate,
            max_failures_per_page: None,
            max_failures_total: None,
            include_original_payload: true,
        }
    }
}

impl fmt::Debug for DlqConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DlqConfig")
            .field("sink", &self.sink.connector_name())
            .field("on_batch_error", &self.on_batch_error)
            .field("max_failures_per_page", &self.max_failures_per_page)
            .field("max_failures_total", &self.max_failures_total)
            .field("include_original_payload", &self.include_original_payload)
            .finish()
    }
}

/// Counters returned alongside [`PipelineResult`](crate::PipelineResult)
/// when a DLQ is wired.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DlqStats {
    /// Total rows routed to the DLQ across the run.
    pub records_dlq: usize,
    /// Pages that produced at least one DLQ record.
    pub pages_with_failures: usize,
}

/// Reason a page produced DLQ traffic. Used as a metric label and span
/// attribute; closed-set enum so cardinality stays bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlqReason {
    /// At least one per-row outcome was `Err`, surfaced by an overriding
    /// [`Sink::write_batch_partial`].
    Partial,
    /// The whole batch failed and the configured policy was
    /// [`OnBatchError::DlqAll`].
    DlqAll,
}

impl DlqReason {
    /// Returns the stable Prometheus label value for this reason.
    /// Closed-set values: `"partial"` or `"dlq_all"`.
    pub fn as_str(self) -> &'static str {
        match self {
            DlqReason::Partial => "partial",
            DlqReason::DlqAll => "dlq_all",
        }
    }
}

/// Build a single DLQ envelope.
///
/// The schema is fixed; see the design spec for the rationale. `payload`
/// is included verbatim — no truncation, no transformation.
pub fn build_envelope(
    payload: &Value,
    error: &FaucetError,
    sink_name: &str,
    pipeline_name: &str,
    row: &str,
    record_index: usize,
) -> Value {
    let kind = crate::observability::decorator::error_kind(error);
    let message = error.to_string();
    // `as_millis()` returns u128. Convert via TryFrom so we saturate at
    // i64::MAX instead of silently wrapping to a negative number. The
    // saturation ceiling (year ~292,000,000) is impossible in practice,
    // so this only ever fires on a corrupt clock. `unwrap_or(0)` covers
    // the (also impossible on modern systems) clock-before-epoch case.
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    json!({
        "error": { "kind": kind, "message": message },
        "payload": payload,
        "ts_ms": ts_ms,
        "sink": sink_name,
        "pipeline": pipeline_name,
        "row": row,
        "record_index": record_index,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_has_all_required_fields() {
        let payload = json!({"user_id": 7, "name": "Alice"});
        let err = FaucetError::Sink("row rejected: bad timestamp".into());
        let env = build_envelope(&payload, &err, "bigquery", "users_etl", "us", 3);

        assert_eq!(env["error"]["kind"], "Sink");
        assert!(
            env["error"]["message"]
                .as_str()
                .unwrap()
                .contains("row rejected")
        );
        assert_eq!(env["payload"], payload);
        assert!(env["ts_ms"].as_i64().unwrap() > 0);
        assert_eq!(env["sink"], "bigquery");
        assert_eq!(env["pipeline"], "users_etl");
        assert_eq!(env["row"], "us");
        assert_eq!(env["record_index"], 3);
    }

    #[test]
    fn envelope_preserves_payload_byte_for_byte() {
        let payload = json!({
            "nested": { "a": [1, 2, 3], "b": null, "c": true },
            "unicode": "café — résumé"
        });
        let env = build_envelope(&payload, &FaucetError::Sink("x".into()), "s", "p", "", 0);
        assert_eq!(env["payload"], payload);
    }

    #[test]
    fn envelope_empty_row_serializes_as_empty_string() {
        let env = build_envelope(&json!({}), &FaucetError::Sink("x".into()), "s", "", "", 0);
        assert_eq!(env["row"], "");
        assert_eq!(env["pipeline"], "");
    }

    #[test]
    fn on_batch_error_defaults_to_propagate() {
        assert_eq!(OnBatchError::default(), OnBatchError::Propagate);
    }

    #[test]
    fn on_batch_error_serializes_snake_case() {
        let prop = serde_json::to_string(&OnBatchError::Propagate).unwrap();
        let all = serde_json::to_string(&OnBatchError::DlqAll).unwrap();
        assert_eq!(prop, "\"propagate\"");
        assert_eq!(all, "\"dlq_all\"");
    }

    #[test]
    fn on_batch_error_deserializes_snake_case() {
        let prop: OnBatchError = serde_json::from_str("\"propagate\"").unwrap();
        let all: OnBatchError = serde_json::from_str("\"dlq_all\"").unwrap();
        assert_eq!(prop, OnBatchError::Propagate);
        assert_eq!(all, OnBatchError::DlqAll);
    }

    #[test]
    fn dlq_reason_strings() {
        assert_eq!(DlqReason::Partial.as_str(), "partial");
        assert_eq!(DlqReason::DlqAll.as_str(), "dlq_all");
    }
}
