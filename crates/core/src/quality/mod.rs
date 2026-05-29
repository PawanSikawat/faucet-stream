//! Data-quality checks: declarative per-record and per-batch assertions that
//! quarantine violating records to the DLQ or abort the run. Pure evaluation;
//! the pipeline wires the DLQ routing in `run_stream`.
//!
//! See `docs/superpowers/specs/2026-05-29-quality-checks-design.md`.

pub mod batch;
pub mod compile;
pub mod config;
pub mod record;

pub use compile::CompiledQuality;
pub use config::{BatchCheck, CompareOp, JsonType, OnFailure, QualitySpec, RecordCheck};

use crate::error::FaucetError;
use crate::quality::batch::{evaluate_aggregate_check, evaluate_unique_check};
use crate::quality::compile::CompiledBatchKind;
use crate::quality::record::evaluate_record_check;
use serde_json::Value;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// A record removed from the page by a quality check, destined for the DLQ.
#[derive(Debug, Clone)]
pub struct QuarantinedRecord {
    pub record: Value,
    /// Check name (stable metric-label value), e.g. `"not_null"`.
    pub check: &'static str,
    /// Addressed field, if any (for the metric/envelope `field` label).
    pub field: Option<String>,
    /// Human-readable failure message (goes into the DLQ envelope error).
    pub message: String,
}

/// Per-check counters + elapsed time, keyed by check name. Emitted as metrics
/// by the observability wrapper.
///
/// Note: for per-record checks `pass`/`fail` count individual records; for
/// per-batch checks they count check *evaluations* (at most one per page).
/// Per-record quarantine volume for batch checks is exposed separately via
/// the `faucet_quality_records_quarantined_total` metric.
#[derive(Debug, Clone, Default)]
pub struct CheckTally {
    pub pass: u64,
    pub fail: u64,
    pub elapsed: Duration,
}

/// Result of applying the quality pass to one page.
#[derive(Debug, Clone, Default)]
pub struct QualityOutcome {
    /// Records that passed all checks and should be written to the sink.
    pub survivors: Vec<Value>,
    /// Records routed to the DLQ.
    pub quarantined: Vec<QuarantinedRecord>,
    /// Per-check pass/fail/elapsed tally.
    pub tally: HashMap<&'static str, CheckTally>,
}

impl QualityOutcome {
    fn bump(&mut self, name: &'static str, passed: bool, elapsed: Duration) {
        let e = self.tally.entry(name).or_default();
        if passed {
            e.pass += 1;
        } else {
            e.fail += 1;
        }
        e.elapsed += elapsed;
    }
}

/// Apply the full per-page quality pass. Pure: no metrics, no DLQ I/O.
///
/// Order: per-record checks (first-failure-wins) partition the page into
/// survivors + quarantined; per-batch checks then run over the survivors.
/// Returns `Err(FaucetError::QualityFailure)` on any `abort`.
pub fn apply_quality(
    records: Vec<Value>,
    q: &CompiledQuality,
) -> Result<QualityOutcome, FaucetError> {
    let mut out = QualityOutcome::default();

    // ── Per-record pass ───────────────────────────────────────────────
    'next_record: for rec in records {
        for check in &q.record {
            // Per-check timing feeds the per-page duration histogram (catches slow
            // checks like json_schema). The per-record Instant overhead is negligible
            // relative to a real check; cheap checks dominate it but the loop is cheap.
            let start = Instant::now();
            let result = evaluate_record_check(check, &rec);
            let elapsed = start.elapsed();
            match result {
                Ok(()) => out.bump(check.name, true, elapsed),
                Err(message) => {
                    out.bump(check.name, false, elapsed);
                    match check.on_failure {
                        OnFailure::Abort => {
                            return Err(FaucetError::QualityFailure {
                                check: check.name.to_string(),
                                message: field_msg(&check.field, &message),
                            });
                        }
                        OnFailure::Quarantine | OnFailure::QuarantineBatch => {
                            // (compile guarantees record checks are quarantine/abort)
                            out.quarantined.push(QuarantinedRecord {
                                record: rec,
                                check: check.name,
                                field: check.field.clone(),
                                message: field_msg(&check.field, &message),
                            });
                            continue 'next_record;
                        }
                    }
                }
            }
        }
        out.survivors.push(rec);
    }

    // ── Per-batch pass over survivors ─────────────────────────────────
    for check in &q.batch {
        let start = Instant::now();
        if matches!(check.kind, CompiledBatchKind::Unique { .. }) {
            let dups = evaluate_unique_check(check, &out.survivors);
            let elapsed = start.elapsed();
            let passed = dups.is_empty();
            out.bump(check.name, passed, elapsed);
            if passed {
                continue;
            }
            match check.on_failure {
                OnFailure::Abort => {
                    return Err(FaucetError::QualityFailure {
                        check: check.name.to_string(),
                        message: format!("{} duplicate row(s)", dups.len()),
                    });
                }
                _ => {
                    // quarantine the duplicate occurrences (keep first).
                    let dup_set: std::collections::HashSet<usize> = dups.into_iter().collect();
                    let mut survivors = Vec::with_capacity(out.survivors.len());
                    for (i, rec) in std::mem::take(&mut out.survivors).into_iter().enumerate() {
                        if dup_set.contains(&i) {
                            out.quarantined.push(QuarantinedRecord {
                                record: rec,
                                check: check.name,
                                field: check.field.clone(),
                                message: field_msg(&check.field, "duplicate key"),
                            });
                        } else {
                            survivors.push(rec);
                        }
                    }
                    out.survivors = survivors;
                }
            }
        } else {
            let result = evaluate_aggregate_check(check, &out.survivors);
            let elapsed = start.elapsed();
            match result {
                Ok(()) => out.bump(check.name, true, elapsed),
                Err(message) => {
                    out.bump(check.name, false, elapsed);
                    match check.on_failure {
                        OnFailure::Abort => {
                            return Err(FaucetError::QualityFailure {
                                check: check.name.to_string(),
                                message: field_msg(&check.field, &message),
                            });
                        }
                        // quarantine_batch: route all survivors to the DLQ.
                        _ => {
                            for rec in std::mem::take(&mut out.survivors) {
                                out.quarantined.push(QuarantinedRecord {
                                    record: rec,
                                    check: check.name,
                                    field: check.field.clone(),
                                    message: field_msg(&check.field, &message),
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(out)
}

fn field_msg(field: &Option<String>, message: &str) -> String {
    match field {
        Some(f) => format!("field '{f}': {message}"),
        None => message.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quality::config::{BatchCheck, CompareOp, OnFailure, RecordCheck};
    use serde_json::json;

    fn compiled(spec: QualitySpec) -> CompiledQuality {
        CompiledQuality::compile(&spec).unwrap()
    }

    #[test]
    fn per_record_quarantine_partitions_page() {
        let q = compiled(QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        });
        let page = vec![json!({"id": 1}), json!({"id": null}), json!({"id": 3})];
        let out = apply_quality(page, &q).unwrap();
        assert_eq!(out.survivors, vec![json!({"id": 1}), json!({"id": 3})]);
        assert_eq!(out.quarantined.len(), 1);
        assert_eq!(out.quarantined[0].check, "not_null");
        assert_eq!(out.quarantined[0].field.as_deref(), Some("id"));
    }

    #[test]
    fn per_record_abort_returns_err() {
        let q = compiled(QualitySpec {
            record: vec![RecordCheck::Compare {
                field: "age".into(),
                op: CompareOp::Gte,
                value: json!(0),
                on_failure: OnFailure::Abort,
            }],
            batch: vec![],
        });
        let page = vec![json!({"age": 5}), json!({"age": -1})];
        let err = apply_quality(page, &q).unwrap_err();
        assert!(matches!(err, crate::FaucetError::QualityFailure { .. }));
    }

    #[test]
    fn batch_eval_runs_over_survivors() {
        // not_null quarantines the null id; row_count(min:2) then sees only 1
        // survivor and aborts.
        let q = compiled(QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![BatchCheck::RowCount {
                min: Some(2),
                max: None,
                on_failure: OnFailure::Abort,
            }],
        });
        let page = vec![json!({"id": 1}), json!({"id": null})];
        let err = apply_quality(page, &q).unwrap_err();
        assert!(matches!(err, crate::FaucetError::QualityFailure { check, .. } if check == "row_count"));
    }

    #[test]
    fn unique_quarantine_routes_dupes_keeps_first() {
        let q = compiled(QualitySpec {
            record: vec![],
            batch: vec![BatchCheck::Unique {
                fields: vec!["id".into()],
                on_failure: OnFailure::Quarantine,
            }],
        });
        let page = vec![json!({"id": 1}), json!({"id": 1}), json!({"id": 2})];
        let out = apply_quality(page, &q).unwrap();
        assert_eq!(out.survivors, vec![json!({"id": 1}), json!({"id": 2})]);
        assert_eq!(out.quarantined.len(), 1);
        assert_eq!(out.quarantined[0].check, "unique");
    }

    #[test]
    fn quarantine_batch_routes_all_survivors() {
        let q = compiled(QualitySpec {
            record: vec![],
            batch: vec![BatchCheck::RowCount {
                min: Some(10),
                max: None,
                on_failure: OnFailure::QuarantineBatch,
            }],
        });
        let page = vec![json!({"id": 1}), json!({"id": 2})];
        let out = apply_quality(page, &q).unwrap();
        assert!(out.survivors.is_empty());
        assert_eq!(out.quarantined.len(), 2);
    }

    #[test]
    fn tally_counts_pass_and_fail() {
        let q = compiled(QualitySpec {
            record: vec![RecordCheck::NotNull {
                field: "id".into(),
                treat_missing_as_null: true,
                on_failure: OnFailure::Quarantine,
            }],
            batch: vec![],
        });
        let page = vec![json!({"id": 1}), json!({"id": null})];
        let out = apply_quality(page, &q).unwrap();
        let t = out.tally.get("not_null").unwrap();
        assert_eq!(t.pass, 1);
        assert_eq!(t.fail, 1);
    }
}
