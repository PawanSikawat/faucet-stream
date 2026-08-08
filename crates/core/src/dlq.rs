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
    ///
    /// This budget is **shared across both sink-side row failures and
    /// quality-check quarantines**: a record routed to the DLQ by a
    /// `quarantine` quality check counts against it just as a sink-side
    /// row failure does.
    pub max_failures_per_page: Option<usize>,
    /// Cumulative failure budget across the run. `None` = unlimited.
    ///
    /// This budget is **shared across both sink-side row failures and
    /// quality-check quarantines**: records quarantined by the quality pass
    /// accumulate in this counter alongside sink-side failures.
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
    /// A record was quarantined (or batch-quarantined) by a data-quality check.
    Quality,
    /// A record was routed to the DLQ by an `on_drift`/`on_incompatible`
    /// quarantine policy.
    SchemaDrift,
    /// A record was routed to the DLQ by a data-contract `on_breach:
    /// quarantine` policy.
    Contract,
}

impl DlqReason {
    /// Returns the stable Prometheus label value for this reason.
    /// Closed-set values: `"partial"`, `"dlq_all"`, or `"quality"`.
    pub fn as_str(self) -> &'static str {
        match self {
            DlqReason::Partial => "partial",
            DlqReason::DlqAll => "dlq_all",
            DlqReason::Quality => "quality",
            DlqReason::SchemaDrift => "schema_drift",
            DlqReason::Contract => "contract",
        }
    }

    /// Every closed-set reason value, for validating a user-supplied
    /// `--reason` filter against the exact serde strings.
    pub const ALL: [DlqReason; 5] = [
        DlqReason::Partial,
        DlqReason::DlqAll,
        DlqReason::Quality,
        DlqReason::SchemaDrift,
        DlqReason::Contract,
    ];

    /// Parse a reason from its stable serde string (the inverse of
    /// [`as_str`](Self::as_str)). Returns `None` for an unknown value.
    pub fn from_serde_str(s: &str) -> Option<DlqReason> {
        DlqReason::ALL.into_iter().find(|r| r.as_str() == s)
    }
}

/// Build a single DLQ envelope.
///
/// The schema is fixed; see the design spec for the rationale. `payload`
/// is included verbatim — no truncation, no transformation. `reason`
/// records *which stage* quarantined the row (as the closed-set
/// [`DlqReason`] serde value) so tools like `faucet dlq inspect` /
/// `faucet dlq replay` can group and filter without re-deriving it from
/// the free-form error message. It is written as a top-level `reason`
/// field alongside the structured `error`.
pub fn build_envelope(
    payload: &Value,
    error: &FaucetError,
    reason: DlqReason,
    sink_name: &str,
    pipeline_name: &str,
    row: &str,
    record_index: usize,
) -> Value {
    let kind = crate::observability::decorator::error_kind(error);
    // The envelope is written to a file / object store, so it leaves the process:
    // scrub any resolved secret the error text picked up (a `reqwest` error
    // embeds the request URL, which may carry an API key in a query parameter).
    // No-op unless the host installed a redactor (#456 H5).
    let message = crate::redact::redact(&error.to_string());
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
        "reason": reason.as_str(),
        "payload": payload,
        "ts_ms": ts_ms,
        "sink": sink_name,
        "pipeline": pipeline_name,
        "row": row,
        "record_index": record_index,
    })
}

/// A DLQ envelope parsed back into its original payload plus the metadata
/// needed to inspect and replay it. Produced by [`unwrap_envelope`].
#[derive(Debug, Clone, PartialEq)]
pub struct UnwrappedEnvelope {
    /// The original record that was quarantined — replayed verbatim.
    pub payload: Value,
    /// The stage that quarantined the row (`build_envelope`'s `reason`
    /// field). `None` for envelopes written before the field existed.
    pub reason: Option<String>,
    /// The [`FaucetError`] variant name (`error.kind`), e.g. `"Sink"`,
    /// `"QualityFailure"`. `None` if the envelope omits it.
    pub error_kind: Option<String>,
    /// Human-readable failure message (`error.message`), if present.
    pub error_message: Option<String>,
    /// Position of the record within its original page.
    pub record_index: Option<u64>,
    /// Pipeline name that produced the envelope, if present.
    pub pipeline: Option<String>,
    /// Matrix row id that produced the envelope, if present.
    pub row: Option<String>,
    /// Sink name the record was destined for, if present.
    pub sink: Option<String>,
    /// Epoch-millis timestamp the envelope was written, if present.
    pub ts_ms: Option<i64>,
}

/// Error returned by [`unwrap_envelope`] when a value is not a usable DLQ
/// envelope. Only the *payload* is mandatory — every other field is
/// optional so envelopes written by older versions still replay.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EnvelopeError {
    /// The value was not a JSON object.
    #[error("DLQ envelope is not a JSON object")]
    NotObject,
    /// The mandatory `payload` field was absent — nothing to replay.
    #[error("DLQ envelope has no `payload` field")]
    MissingPayload,
}

/// Parse a DLQ envelope produced by [`build_envelope`] back into its
/// original payload plus metadata.
///
/// Only `payload` is required; all other fields are optional so envelopes
/// written before a field existed still round-trip (forward-compatible
/// read). Callers reading a DLQ location back (e.g. `faucet dlq inspect`)
/// should treat an [`EnvelopeError`] as "skip + count", never as fatal —
/// a DLQ file may legitimately contain arbitrary lines.
pub fn unwrap_envelope(value: &Value) -> Result<UnwrappedEnvelope, EnvelopeError> {
    let obj = value.as_object().ok_or(EnvelopeError::NotObject)?;
    let payload = obj.get("payload").ok_or(EnvelopeError::MissingPayload)?;
    let error = obj.get("error").and_then(|e| e.as_object());
    let str_field = |k: &str| obj.get(k).and_then(|v| v.as_str()).map(str::to_owned);
    Ok(UnwrappedEnvelope {
        payload: payload.clone(),
        reason: str_field("reason"),
        error_kind: error
            .and_then(|e| e.get("kind"))
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        error_message: error
            .and_then(|e| e.get("message"))
            .and_then(|v| v.as_str())
            .map(str::to_owned),
        record_index: obj.get("record_index").and_then(Value::as_u64),
        pipeline: str_field("pipeline"),
        row: str_field("row"),
        sink: str_field("sink"),
        ts_ms: obj.get("ts_ms").and_then(Value::as_i64),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_has_all_required_fields() {
        let payload = json!({"user_id": 7, "name": "Alice"});
        let err = FaucetError::Sink("row rejected: bad timestamp".into());
        let env = build_envelope(
            &payload,
            &err,
            DlqReason::Partial,
            "bigquery",
            "users_etl",
            "us",
            3,
        );

        assert_eq!(env["error"]["kind"], "Sink");
        assert_eq!(env["reason"], "partial");
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
        let env = build_envelope(
            &payload,
            &FaucetError::Sink("x".into()),
            DlqReason::Quality,
            "s",
            "p",
            "",
            0,
        );
        assert_eq!(env["payload"], payload);
    }

    #[test]
    fn envelope_empty_row_serializes_as_empty_string() {
        let env = build_envelope(
            &json!({}),
            &FaucetError::Sink("x".into()),
            DlqReason::DlqAll,
            "s",
            "",
            "",
            0,
        );
        assert_eq!(env["row"], "");
        assert_eq!(env["pipeline"], "");
    }

    #[test]
    fn dlq_reason_from_serde_str_round_trips() {
        for r in DlqReason::ALL {
            assert_eq!(DlqReason::from_serde_str(r.as_str()), Some(r));
        }
        assert_eq!(DlqReason::from_serde_str("nope"), None);
        assert_eq!(DlqReason::from_serde_str("sink_error"), None);
    }

    #[test]
    fn unwrap_envelope_round_trips_build_envelope() {
        let payload = json!({"id": 42, "name": "Zoe"});
        let err = FaucetError::QualityFailure {
            check: "not_null(email)".into(),
            message: "email is null".into(),
        };
        let env = build_envelope(&payload, &err, DlqReason::Quality, "pg", "etl", "eu", 5);
        let u = unwrap_envelope(&env).expect("valid envelope");
        assert_eq!(u.payload, payload);
        assert_eq!(u.reason.as_deref(), Some("quality"));
        assert_eq!(u.error_kind.as_deref(), Some("QualityFailure"));
        assert!(u.error_message.unwrap().contains("email is null"));
        assert_eq!(u.record_index, Some(5));
        assert_eq!(u.pipeline.as_deref(), Some("etl"));
        assert_eq!(u.row.as_deref(), Some("eu"));
        assert_eq!(u.sink.as_deref(), Some("pg"));
        assert!(u.ts_ms.unwrap() > 0);
    }

    #[test]
    fn unwrap_envelope_tolerates_legacy_envelope_without_reason() {
        // An envelope written before `reason`/`error` existed still yields its
        // payload; the missing metadata comes back as `None`, never a panic.
        let legacy = json!({ "payload": { "x": 1 } });
        let u = unwrap_envelope(&legacy).expect("payload present");
        assert_eq!(u.payload, json!({ "x": 1 }));
        assert_eq!(u.reason, None);
        assert_eq!(u.error_kind, None);
        assert_eq!(u.record_index, None);
    }

    #[test]
    fn unwrap_envelope_errors_on_non_object_and_missing_payload() {
        assert_eq!(
            unwrap_envelope(&json!("just a string")),
            Err(EnvelopeError::NotObject)
        );
        assert_eq!(
            unwrap_envelope(&json!([1, 2, 3])),
            Err(EnvelopeError::NotObject)
        );
        assert_eq!(
            unwrap_envelope(&json!({ "error": { "kind": "Sink" } })),
            Err(EnvelopeError::MissingPayload)
        );
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

    #[test]
    fn dlq_reason_quality_string() {
        assert_eq!(DlqReason::Quality.as_str(), "quality");
    }

    #[test]
    fn dlq_reason_schema_drift_string() {
        assert_eq!(DlqReason::SchemaDrift.as_str(), "schema_drift");
    }

    #[test]
    fn dlq_reason_contract_string() {
        assert_eq!(DlqReason::Contract.as_str(), "contract");
    }
}
