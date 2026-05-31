//! Run-history storage. The trait is defined in full now (Phase 2/3 ships only
//! the in-memory backend in `memory.rs`); Phase 5 adds Postgres/SQLite as new
//! impls with no trait change. See spec §11 + §20.

pub mod memory;

use crate::executor::InvocationOutcome;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// Lifecycle state of a submitted run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl RunStatus {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }
}

/// Serializable mirror of one pipeline invocation's outcome.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InvocationRecord {
    pub row_id: String,
    pub parent_record_key: Option<String>,
    pub records_written: usize,
    pub error: Option<String>,
}

impl From<&InvocationOutcome> for InvocationRecord {
    fn from(o: &InvocationOutcome) -> Self {
        Self {
            row_id: o.row_id.clone(),
            parent_record_key: o.parent_record_key.clone(),
            records_written: o.records_written,
            error: o.error.clone(),
        }
    }
}

/// One run's full record — the GET / list element (spec §6).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub run_id: String,
    pub name: Option<String>,
    pub labels: BTreeMap<String, String>,
    pub status: RunStatus,
    pub submitted_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub elapsed_secs: Option<f64>,
    pub records_written: u64,
    pub invocations: Vec<InvocationRecord>,
    pub error: Option<String>,
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doctor_report: Option<serde_json::Value>,
}

impl RunRecord {
    /// A freshly-submitted run, before it acquires an execution slot.
    pub fn queued(
        run_id: String,
        name: Option<String>,
        labels: BTreeMap<String, String>,
        idempotency_key: Option<String>,
        submitted_at: DateTime<Utc>,
    ) -> Self {
        Self {
            run_id,
            name,
            labels,
            status: RunStatus::Queued,
            submitted_at,
            started_at: None,
            finished_at: None,
            elapsed_secs: None,
            records_written: 0,
            invocations: Vec::new(),
            error: None,
            idempotency_key,
            doctor_report: None,
        }
    }
}

/// Result of an atomic idempotency-key claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Claim {
    /// Key is new (or its prior claim expired) — caller owns it for `run_id`.
    Fresh,
    /// Key was already claimed with a matching payload — replay this run id.
    Replay(String),
    /// Key was claimed with a *different* payload — 409.
    Conflict,
}

/// Result of a delete attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeleteOutcome {
    Deleted,
    NotFound,
    StillRunning,
}

/// Filter + pagination for `list`. `limit`/`cursor` are resolved by the handler.
#[derive(Debug, Default, Clone)]
pub struct ListFilter {
    pub status: Option<RunStatus>,
    pub name: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: usize,
    pub cursor: Option<String>,
}

/// One page of `list` results, ordered `(submitted_at DESC, run_id DESC)`.
#[derive(Debug)]
pub struct ListPage {
    pub runs: Vec<RunRecord>,
    pub next_cursor: Option<String>,
}

/// Backend failure. The memory backend never returns one; the variant exists so
/// the async trait stays fallible for the Phase 5 SQL backends.
#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error("run-history backend error: {0}")]
    Backend(String),
}

#[async_trait]
pub trait RunHistory: Send + Sync {
    /// Atomically claim `key` for `run_id` (or report a replay/conflict). A prior
    /// claim older than `window` is treated as expired and re-claimable.
    async fn claim_idempotency(
        &self,
        key: &str,
        fingerprint: &str,
        run_id: &str,
        window: Duration,
    ) -> Result<Claim, HistoryError>;

    /// Insert or replace a run record.
    async fn upsert(&self, rec: &RunRecord) -> Result<(), HistoryError>;

    async fn get(&self, id: &str) -> Result<Option<RunRecord>, HistoryError>;

    async fn list(&self, filter: &ListFilter) -> Result<ListPage, HistoryError>;

    /// Delete a terminal run. Non-terminal → `StillRunning` (caller maps to 409).
    async fn delete(&self, id: &str) -> Result<DeleteOutcome, HistoryError>;

    /// Drop terminal records finished longer than `retain_for` ago. Returns the
    /// number removed.
    async fn purge_expired(&self, retain_for: Duration) -> Result<usize, HistoryError>;

    /// Mark any non-terminal record left over from a previous process as failed.
    /// The memory backend has nothing to recover (returns 0).
    async fn recover_orphans(&self) -> Result<usize, HistoryError>;

    /// True when the backend is in fallback mode (drives `/readyz`). Always false
    /// for memory.
    fn degraded(&self) -> bool;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_classification() {
        assert!(!RunStatus::Queued.is_terminal());
        assert!(!RunStatus::Running.is_terminal());
        assert!(RunStatus::Completed.is_terminal());
        assert!(RunStatus::Failed.is_terminal());
        assert!(RunStatus::Cancelled.is_terminal());
    }

    #[test]
    fn run_record_serializes_status_snake_case() {
        let rec = RunRecord::queued(
            "r1".into(),
            Some("n".into()),
            Default::default(),
            None,
            Utc::now(),
        );
        let v = serde_json::to_value(&rec).unwrap();
        assert_eq!(v["status"], "queued");
        assert_eq!(v["run_id"], "r1");
        // doctor_report is skipped when None.
        assert!(v.get("doctor_report").is_none());
    }
}
