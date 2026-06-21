//! Run-history storage. The trait is defined in full now; the in-memory backend
//! lives in `memory.rs`, and the feature-gated SQL backends (`postgres.rs` /
//! `sqlite.rs`, sharing `sql.rs`) wrap themselves in `fallback.rs` so an
//! unreachable backend degrades to in-memory rather than refusing to start.
//! See spec §11 + §20.

#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
pub mod fallback;
pub mod memory;
#[cfg(feature = "serve-history-postgres")]
pub mod postgres;
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
pub mod sql;
#[cfg(feature = "serve-history-sqlite")]
pub mod sqlite;

use crate::error::CliResult;
use crate::executor::InvocationOutcome;
use crate::serve::config::HistoryBackendSpec;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

/// Lifecycle state of a submitted run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Pending,
    Running,
    /// Mode B (#230): the run has been expanded into shards (rows in
    /// `faucet_serve_shards`). It does not execute as a whole — its shards do,
    /// each claimed/leased independently — and it is finalized to a terminal
    /// state once every shard is terminal. Non-terminal, owner-less, and not
    /// reclaimed by run-level orphan recovery (only its shards are).
    Sharded,
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
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Sharded => "sharded",
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
    /// Raw submitted config text — present only for cluster runs so any instance
    /// can re-resolve + re-run it. `None` for single-instance runs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_body: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_format: Option<crate::serve::load::ConfigFormat>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock: Option<String>,
    /// Failover re-run count (cluster mode). 0 on first submit.
    #[serde(default)]
    pub attempt: u32,
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
            config_body: None,
            config_format: None,
            timeout_secs: None,
            clock: None,
            attempt: 0,
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

/// Result of a failover reclaim pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReclaimReport {
    /// Orphans re-queued to `Pending` for another instance to re-run.
    pub requeued: usize,
    /// Orphans that hit the attempt cap and were marked `Failed` (poison).
    pub failed: usize,
}

/// Fields a serve instance heartbeats into the membership table. The
/// `instance_id` is the backend's own id (stamped server-side), so it is not
/// carried here.
#[derive(Debug, Clone)]
pub struct InstanceHeartbeat {
    pub started_at: DateTime<Utc>,
    pub listen: Option<String>,
    pub max_concurrent: u32,
    pub in_flight: u32,
}

/// One live cluster member (for `/readyz` + metrics).
#[derive(Debug, Clone, Serialize)]
pub struct InstanceRecord {
    pub instance_id: String,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat: DateTime<Utc>,
    pub listen: Option<String>,
    pub max_concurrent: u32,
    pub in_flight: u32,
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
    /// The backend is degraded and the operation can't be honored safely
    /// (e.g. an idempotency claim that would risk a duplicate run). Maps to a
    /// `503` so the caller can retry once the backend recovers (#146 M5).
    #[error("{0}")]
    Degraded(String),
}

/// One shard row to persist when a run is expanded into shards (Mode B, #230).
#[derive(Debug, Clone)]
pub struct ShardInsert {
    /// Stable shard id, unique within the run (the [`ShardSpec`](faucet_core::ShardSpec) id).
    pub shard_id: String,
    /// Opaque connector descriptor, persisted verbatim and handed to
    /// [`Source::apply_shard`](faucet_core::Source::apply_shard) on the worker.
    pub descriptor: serde_json::Value,
    /// Relative size estimate for skew-aware assignment, if the source provided one.
    pub size_estimate: Option<u64>,
}

/// A shard claimed for execution, carrying its parent run's record (whose
/// `config_body` the worker re-loads to build + shard the source).
#[derive(Debug, Clone)]
pub struct ClaimedShard {
    pub run_id: String,
    pub shard_id: String,
    pub descriptor: serde_json::Value,
    /// The parent run record (config body, name, etc.).
    pub run: RunRecord,
}

/// Aggregate shard status for a run, used by the coordinator to decide when the
/// parent run is finished.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ShardProgress {
    pub total: usize,
    pub completed: usize,
    pub failed: usize,
    pub running: usize,
    pub pending: usize,
}

impl ShardProgress {
    /// True when every shard has reached a terminal state (and at least one
    /// exists) — i.e. the parent run can be finalized.
    pub fn all_terminal(&self) -> bool {
        self.total > 0 && self.completed + self.failed == self.total
    }
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

    /// Mark non-terminal records whose owning instance's lease has expired as
    /// failed (instance-fenced orphan recovery — never touches a live peer's
    /// heartbeated runs, #146 H7). Returns the number recovered. The memory
    /// backend has nothing to recover (returns 0).
    async fn recover_orphans(&self) -> Result<usize, HistoryError>;

    /// Heartbeat: extend the lease of *this* instance's own non-terminal runs so
    /// a peer's [`recover_orphans`](Self::recover_orphans) won't reclaim them.
    /// Returns the number of leases renewed. The memory backend (single-process,
    /// unshared) is a no-op returning 0.
    async fn renew_leases(&self) -> Result<usize, HistoryError> {
        Ok(0)
    }

    /// Atomically claim up to `limit` oldest `Pending` runs for *this* instance,
    /// moving them `Pending` → `Running` with a fresh lease, and return the
    /// claimed records (with `config_body`) for the caller to execute. Exclusive:
    /// a run claimed by one caller is never returned to another. Default: none
    /// (memory is single-process and never writes `Pending`).
    async fn claim_pending(&self, limit: usize) -> Result<Vec<RunRecord>, HistoryError> {
        let _ = limit;
        Ok(Vec::new())
    }

    /// Cluster failover: expired-lease `Running` runs whose `attempt < max_attempts`
    /// go back to `Pending` (owner/lease cleared, `attempt++`); the rest are
    /// `Failed` (poison). Returns the counts. Default: nothing to reclaim.
    async fn reclaim_orphans(&self, max_attempts: u32) -> Result<ReclaimReport, HistoryError> {
        let _ = max_attempts;
        Ok(ReclaimReport::default())
    }

    /// Owner-fenced terminal write: persist `rec` only if this instance still owns
    /// the run. Returns `true` if the write landed, `false` if another instance
    /// reclaimed it (the caller should discard its result). Default: delegate to
    /// `upsert` (memory/single-process always owns its runs).
    async fn finalize_owned(&self, rec: &RunRecord) -> Result<bool, HistoryError> {
        self.upsert(rec).await.map(|_| true)
    }

    /// Cancel a still-`Pending` (unclaimed) run directly. Returns `true` if it was
    /// pending and is now `Cancelled`; `false` if it had already been claimed (the
    /// caller should fall back to [`request_cancel`](Self::request_cancel)).
    /// Default: `false`.
    async fn cancel_pending(&self, run_id: &str) -> Result<bool, HistoryError> {
        let _ = run_id;
        Ok(false)
    }

    /// Flag a `Running` run for cross-instance cancellation; its owning instance
    /// fires the local cancel on its next claim-loop tick. Default: no-op.
    async fn request_cancel(&self, run_id: &str) -> Result<(), HistoryError> {
        let _ = run_id;
        Ok(())
    }

    /// This instance's own `Running` runs that have a pending cancel request.
    /// Default: none.
    async fn pending_cancellations(&self) -> Result<Vec<String>, HistoryError> {
        Ok(Vec::new())
    }

    /// Membership heartbeat: upsert this instance's liveness row. Default: no-op.
    async fn heartbeat_instance(&self, beat: &InstanceHeartbeat) -> Result<(), HistoryError> {
        let _ = beat;
        Ok(())
    }

    /// Live cluster members (last heartbeat within `ttl`). Default: none.
    async fn live_instances(&self, ttl: Duration) -> Result<Vec<InstanceRecord>, HistoryError> {
        let _ = ttl;
        Ok(Vec::new())
    }

    // ── Source-shard coordination (Mode B, #230) ────────────────────────────
    //
    // All default to inert so the in-memory (single-process, unsharded) backend
    // and any non-cluster deployment are unaffected. Implemented by the SQL
    // backends, which share one `faucet_serve_shards` table.

    /// Idempotently insert the shard set for `run_id` (`INSERT … ON CONFLICT
    /// (run_id, shard_id) DO NOTHING`), so concurrent coordinators converge on
    /// the same set without a leader. Returns the number of rows newly inserted.
    /// Default: no-op.
    async fn insert_shards(
        &self,
        run_id: &str,
        shards: &[ShardInsert],
    ) -> Result<usize, HistoryError> {
        let _ = (run_id, shards);
        Ok(0)
    }

    /// Atomically claim up to `limit` `pending` shards for *this* instance
    /// (`pending` → `running` with a fresh lease), largest-estimated-size first
    /// for skew-aware balancing, returning each with its parent run record.
    /// Exclusive, like [`claim_pending`](Self::claim_pending). Default: none.
    async fn claim_shards(&self, limit: usize) -> Result<Vec<ClaimedShard>, HistoryError> {
        let _ = limit;
        Ok(Vec::new())
    }

    /// Heartbeat: extend the lease of this instance's own `running` shards so a
    /// peer's [`reclaim_shards`](Self::reclaim_shards) won't reassign them.
    /// Returns the number renewed. Default: no-op.
    async fn renew_shard_leases(&self) -> Result<usize, HistoryError> {
        Ok(0)
    }

    /// Rebalance: expired-lease `running` shards whose `attempt < max_attempts`
    /// go back to `pending` (owner cleared, `attempt++`) for another worker to
    /// claim; the rest are `failed` (poison). Returns the counts. Default: none.
    async fn reclaim_shards(&self, max_attempts: u32) -> Result<ReclaimReport, HistoryError> {
        let _ = max_attempts;
        Ok(ReclaimReport::default())
    }

    /// Owner-fenced terminal write for one shard (`running` → `completed`/`failed`),
    /// only if this instance still owns it. Returns `true` if the write landed.
    /// Default: `false`.
    async fn finalize_shard(
        &self,
        run_id: &str,
        shard_id: &str,
        success: bool,
    ) -> Result<bool, HistoryError> {
        let _ = (run_id, shard_id, success);
        Ok(false)
    }

    /// Aggregate shard status counts for a run (drives parent-run finalization).
    /// Default: empty.
    async fn shard_progress(&self, run_id: &str) -> Result<ShardProgress, HistoryError> {
        let _ = run_id;
        Ok(ShardProgress::default())
    }

    /// Distinct run_ids for which THIS instance owns a `running` shard whose
    /// parent run has a pending cancellation request (F10). The claim loop fires
    /// each returned run's local shard tokens via
    /// [`Registry::cancel_run_shards`](crate::serve::registry::Registry::cancel_run_shards).
    /// Default: none (single-process / memory owns no cross-instance shards).
    async fn pending_shard_cancellations(&self) -> Result<Vec<String>, HistoryError> {
        Ok(Vec::new())
    }

    /// Sweep `sharded` parent runs whose shards are ALL terminal and finalize
    /// each to `Completed` (no failures) or `Failed`, stamping `finished_at`
    /// (F11). Recovers a parent that no shard task finalized inline (e.g. the
    /// coordinator crashed after the last shard completed elsewhere). Returns the
    /// number finalized. Status-fenced, so a concurrent inline finalize is a
    /// benign no-op and the run-finished metric is never double-counted. Default:
    /// nothing to finalize.
    async fn finalize_completed_sharded_parents(&self) -> Result<usize, HistoryError> {
        Ok(0)
    }

    /// True when the backend is in fallback mode (drives `/readyz`). Always false
    /// for memory.
    fn degraded(&self) -> bool;
}

/// Build the configured run-history backend. `Memory` is always available; the
/// SQL backends require their respective `serve-history-*` build features (a
/// clear error otherwise). A SQL backend that fails to connect at startup
/// degrades to in-memory (via `FallbackHistory`) rather than aborting boot.
pub async fn connect(
    spec: &HistoryBackendSpec,
    idem_retention: Duration,
    lease_ttl: Duration,
    instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    match spec {
        HistoryBackendSpec::Memory => {
            Ok(Arc::new(memory::MemoryHistory::new(idem_retention)) as Arc<dyn RunHistory>)
        }
        HistoryBackendSpec::Postgres(url) => {
            connect_postgres(url, idem_retention, lease_ttl, instance_id).await
        }
        HistoryBackendSpec::Sqlite(url) => {
            connect_sqlite(url, idem_retention, lease_ttl, instance_id).await
        }
    }
}

#[cfg(feature = "serve-history-postgres")]
async fn connect_postgres(
    url: &str,
    idem: Duration,
    lease_ttl: Duration,
    instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    let result = connect_with_retry("postgres", || {
        postgres::PostgresHistory::connect(url, idem, lease_ttl, instance_id.to_string())
    })
    .await;
    Ok(into_history(result, idem, "postgres"))
}

#[cfg(not(feature = "serve-history-postgres"))]
async fn connect_postgres(
    _url: &str,
    _idem: Duration,
    _lease_ttl: Duration,
    _instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    Err(crate::error::CliError::Serve(
        "persistent Postgres run history requires building faucet with the \
         `serve-history-postgres` feature"
            .into(),
    ))
}

#[cfg(feature = "serve-history-sqlite")]
async fn connect_sqlite(
    url: &str,
    idem: Duration,
    lease_ttl: Duration,
    instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    let result = connect_with_retry("sqlite", || {
        sqlite::SqliteHistory::connect(url, idem, lease_ttl, instance_id.to_string())
    })
    .await;
    Ok(into_history(result, idem, "sqlite"))
}

#[cfg(not(feature = "serve-history-sqlite"))]
async fn connect_sqlite(
    _url: &str,
    _idem: Duration,
    _lease_ttl: Duration,
    _instance_id: &str,
) -> CliResult<Arc<dyn RunHistory>> {
    Err(crate::error::CliError::Serve(
        "persistent SQLite run history requires building faucet with the \
         `serve-history-sqlite` feature"
            .into(),
    ))
}

/// How many times `connect_with_retry` attempts a transient backend connect
/// before giving up and degrading. Eight attempts with capped exponential
/// backoff span a few seconds — long enough for two clustered instances to get
/// past the WAL/DDL startup race on a shared SQLite file, short enough not to
/// stall startup against a genuinely-down backend.
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
const CONNECT_ATTEMPTS: usize = 8;

/// Retry a *transient* backend-connect failure before falling back to degraded
/// mode. Two clustered instances opening the same SQLite file at startup briefly
/// race the WAL/DDL setup and surface `database is locked`; a freshly-booting
/// Postgres can refuse connections for a moment. Both are self-resolving — but
/// degrading permanently on the *first* blip strands a cluster instance on the
/// in-memory store, which cannot serve cluster submits and returns `503` for
/// every request (#235). A genuinely unreachable backend still degrades once the
/// attempt budget is spent, preserving the stay-alive fallback.
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
async fn connect_with_retry<H, F, Fut>(label: &str, mut make: F) -> Result<H, HistoryError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<H, HistoryError>>,
{
    let mut delay = Duration::from_millis(100);
    for attempt in 1..=CONNECT_ATTEMPTS {
        match make().await {
            Ok(backend) => return Ok(backend),
            Err(e) if attempt < CONNECT_ATTEMPTS && is_transient_connect_error(&e) => {
                tracing::warn!(
                    backend = label,
                    attempt,
                    error = %e,
                    "run-history backend connect failed transiently; retrying before degrading"
                );
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(1));
            }
            Err(e) => return Err(e),
        }
    }
    unreachable!("the final attempt returns Ok or Err rather than looping")
}

/// Whether a connect error is worth retrying: transient contention or a
/// still-booting backend, as opposed to a permanent misconfiguration (e.g. a
/// malformed URL) that no amount of retrying will fix.
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
fn is_transient_connect_error(e: &HistoryError) -> bool {
    let msg = e.to_string().to_ascii_lowercase();
    [
        "database is locked", // SQLite: two cluster instances race WAL/DDL at startup
        "busy",               // SQLITE_BUSY
        "connection refused", // backend still binding its listener
        "connection reset",
        "timed out",
        "timeout",
        "starting up",          // Postgres: "the database system is starting up"
        "too many connections", // transient connection saturation
    ]
    .iter()
    .any(|needle| msg.contains(needle))
}

/// Wrap a SQL backend in `FallbackHistory`: healthy on success; degraded-on-
/// in-memory (server stays up, `/readyz` reports 503) on a connect failure.
#[cfg(any(feature = "serve-history-postgres", feature = "serve-history-sqlite"))]
fn into_history<H: RunHistory + 'static>(
    result: Result<H, HistoryError>,
    idem: Duration,
    label: &'static str,
) -> Arc<dyn RunHistory> {
    match result {
        Ok(backend) => Arc::new(fallback::FallbackHistory::healthy(
            Box::new(backend),
            idem,
            label,
        )),
        Err(e) => {
            tracing::error!(
                backend = label, error = %e,
                "run-history backend unavailable at startup; starting DEGRADED on in-memory store"
            );
            Arc::new(fallback::FallbackHistory::degraded_at_startup(idem, label))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_classification() {
        assert!(!RunStatus::Queued.is_terminal());
        assert!(!RunStatus::Pending.is_terminal());
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

    #[test]
    fn pending_is_non_terminal_and_serializes_snake_case() {
        assert!(!RunStatus::Pending.is_terminal());
        assert_eq!(RunStatus::Pending.as_str(), "pending");
        let mut rec = RunRecord::queued("r".into(), None, Default::default(), None, Utc::now());
        rec.status = RunStatus::Pending;
        rec.attempt = 2;
        let v = serde_json::to_value(&rec).unwrap();
        assert_eq!(v["status"], "pending");
        assert_eq!(v["attempt"], 2);
        // Cluster config fields are skipped when absent.
        assert!(v.get("config_body").is_none());
    }

    #[test]
    fn shard_progress_all_terminal() {
        // No shards yet → not terminal (don't finalize an unexpanded run).
        assert!(!ShardProgress::default().all_terminal());
        // Some still running.
        let mut p = ShardProgress {
            total: 3,
            completed: 1,
            failed: 0,
            running: 1,
            pending: 1,
        };
        assert!(!p.all_terminal());
        // All terminal (mix of completed + failed sums to total).
        p = ShardProgress {
            total: 3,
            completed: 2,
            failed: 1,
            running: 0,
            pending: 0,
        };
        assert!(p.all_terminal());
    }

    #[tokio::test]
    async fn memory_backend_shard_methods_are_inert() {
        use crate::serve::history::memory::MemoryHistory;
        let h = MemoryHistory::new(Duration::from_secs(60));
        assert_eq!(h.insert_shards("r", &[]).await.unwrap(), 0);
        assert!(h.claim_shards(8).await.unwrap().is_empty());
        assert_eq!(h.renew_shard_leases().await.unwrap(), 0);
        assert!(!h.finalize_shard("r", "0", true).await.unwrap());
        assert_eq!(
            h.shard_progress("r").await.unwrap(),
            ShardProgress::default()
        );
    }

    #[tokio::test]
    async fn memory_backend_cluster_methods_are_inert() {
        use crate::serve::history::memory::MemoryHistory;
        let h = MemoryHistory::new(Duration::from_secs(60));
        assert!(h.claim_pending(8).await.unwrap().is_empty());
        assert_eq!(
            h.reclaim_orphans(3).await.unwrap(),
            ReclaimReport::default()
        );
        assert!(!h.cancel_pending("x").await.unwrap());
        h.request_cancel("x").await.unwrap();
        assert!(h.pending_cancellations().await.unwrap().is_empty());
        assert!(
            h.live_instances(Duration::from_secs(60))
                .await
                .unwrap()
                .is_empty()
        );

        // finalize_owned's default delegates to upsert (single-process always owns).
        let rec = RunRecord::queued("fo".into(), None, Default::default(), None, Utc::now());
        assert!(h.finalize_owned(&rec).await.unwrap());
        assert_eq!(h.get("fo").await.unwrap().unwrap().run_id, "fo");
    }
}

#[cfg(all(
    test,
    any(feature = "serve-history-postgres", feature = "serve-history-sqlite")
))]
mod connect_retry_tests {
    use super::*;
    use std::cell::Cell;

    #[test]
    fn classifies_transient_vs_permanent_connect_errors() {
        // SQLite concurrent-startup contention (#235) — retryable.
        assert!(is_transient_connect_error(&HistoryError::Backend(
            "SQLite connection failed: error returned from database: (code: 5) \
             database is locked"
                .into()
        )));
        // Booting Postgres — retryable.
        assert!(is_transient_connect_error(&HistoryError::Backend(
            "connection refused (os error 111)".into()
        )));
        // Permanent misconfiguration — not worth retrying.
        assert!(!is_transient_connect_error(&HistoryError::Backend(
            "invalid sqlite url 'sqlite::nonsense': ParseError".into()
        )));
    }

    #[tokio::test]
    async fn retries_a_transient_failure_then_succeeds() {
        let calls = Cell::new(0usize);
        let result: Result<u32, HistoryError> = connect_with_retry("test", || {
            let n = calls.get() + 1;
            calls.set(n);
            async move {
                if n < 3 {
                    Err(HistoryError::Backend("database is locked".into()))
                } else {
                    Ok(42u32)
                }
            }
        })
        .await;
        assert_eq!(result.unwrap(), 42);
        assert_eq!(
            calls.get(),
            3,
            "two transient failures retried, third succeeds"
        );
    }

    #[tokio::test]
    async fn does_not_retry_a_permanent_error() {
        let calls = Cell::new(0usize);
        let result: Result<u32, HistoryError> = connect_with_retry("test", || {
            calls.set(calls.get() + 1);
            async move { Err::<u32, _>(HistoryError::Backend("invalid sqlite url 'x'".into())) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(
            calls.get(),
            1,
            "a permanent error degrades immediately, no retry"
        );
    }
}
