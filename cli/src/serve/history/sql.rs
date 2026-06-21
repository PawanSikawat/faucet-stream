//! Shared SQL run-history machinery for the Postgres and SQLite backends
//! (Phase 5, #127). Both backends are identical except for the connection setup
//! and the placeholder dialect (`$n` vs `?`), so the schema, prepared-statement
//! text, pure helpers, and the entire `RunHistory` impl live here once and are
//! instantiated for each concrete `sqlx` pool via the `impl_sql_history!` macro.
//!
//! **Portability:** every column is `TEXT` (timestamps are stored as fixed-width
//! RFC3339 with nanosecond precision + `Z`, which sorts lexicographically in
//! chronological order, so keyset pagination and expiry comparisons work without
//! any database date type — and thus without the `sqlx` `chrono` feature). The
//! whole `RunRecord` is serialized into the `body` column (the source of truth on
//! read); the dedicated columns exist only for filtering, ordering, and expiry.
//!
//! **Idempotency** lives in a separate `faucet_serve_idem` table whose `key`
//! primary key is the required unique index (spec §10/§11). The claim is atomic
//! via `INSERT … ON CONFLICT DO NOTHING` plus an optimistic, expiry-guarded
//! takeover `UPDATE`, mirroring the memory backend's shard-locked semantics.

use super::{HistoryError, RunRecord, RunStatus};
use chrono::{DateTime, Utc};
use std::time::Duration;

/// DDL run at connect time. Valid verbatim on both Postgres and SQLite (only
/// `TEXT` columns, `IF NOT EXISTS`, and standard indexes).
pub const DDL: &[&str] = &[
    // `owner` is the id of the serve instance that owns the run; `lease_expires_at`
    // is the RFC3339 instant past which that ownership is presumed dead. Together
    // they fence orphan recovery: an instance only fails a non-terminal run whose
    // lease has expired, never another live instance's heartbeated runs (#146 H7).
    "CREATE TABLE IF NOT EXISTS faucet_serve_runs (\
        run_id TEXT PRIMARY KEY,\
        name TEXT,\
        status TEXT NOT NULL,\
        submitted_at TEXT NOT NULL,\
        finished_at TEXT,\
        idempotency_key TEXT,\
        owner TEXT,\
        lease_expires_at TEXT,\
        cancel_requested TEXT,\
        body TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS faucet_serve_runs_submitted_idx \
        ON faucet_serve_runs (submitted_at)",
    // Speeds the per-tick orphan scan / lease renewal, which filter on
    // (status, owner, lease_expires_at).
    "CREATE INDEX IF NOT EXISTS faucet_serve_runs_status_lease_idx \
        ON faucet_serve_runs (status, lease_expires_at)",
    // Speeds the cluster dispatcher's pending-run query (ordered by submitted_at).
    "CREATE INDEX IF NOT EXISTS faucet_serve_runs_pending_idx \
        ON faucet_serve_runs (status, submitted_at)",
    "CREATE TABLE IF NOT EXISTS faucet_serve_instances (\
        instance_id TEXT PRIMARY KEY,\
        started_at TEXT NOT NULL,\
        last_heartbeat TEXT NOT NULL,\
        listen TEXT,\
        max_concurrent TEXT,\
        in_flight TEXT)",
    "CREATE INDEX IF NOT EXISTS faucet_serve_instances_hb_idx \
        ON faucet_serve_instances (last_heartbeat)",
    "CREATE TABLE IF NOT EXISTS faucet_serve_idem (\
        key TEXT PRIMARY KEY,\
        run_id TEXT NOT NULL,\
        fingerprint TEXT NOT NULL,\
        claimed_at TEXT NOT NULL)",
    // Source shards for clustered Mode B (#230). One row per (run, shard);
    // `owner`/`lease_expires_at`/`attempt` reuse Mode A's lease-fencing semantics
    // at shard granularity. `size_estimate` (an integer stored as TEXT) drives
    // skew-aware, largest-first claiming. `descriptor` is the opaque connector
    // shard spec, replayed to the worker that claims the shard.
    "CREATE TABLE IF NOT EXISTS faucet_serve_shards (\
        run_id TEXT NOT NULL,\
        shard_id TEXT NOT NULL,\
        descriptor TEXT NOT NULL,\
        size_estimate TEXT,\
        status TEXT NOT NULL,\
        owner TEXT,\
        lease_expires_at TEXT,\
        attempt TEXT NOT NULL,\
        finished_at TEXT,\
        PRIMARY KEY (run_id, shard_id))",
    "CREATE INDEX IF NOT EXISTS faucet_serve_shards_claim_idx \
        ON faucet_serve_shards (status, lease_expires_at)",
];

/// SQL placeholder dialect.
#[derive(Clone, Copy, Debug)]
pub enum Dialect {
    Postgres,
    Sqlite,
}

/// Prepared-statement text for a backend, built once per dialect at connect time.
pub struct Stmts {
    /// (`cancel_requested` is intentionally NOT written by `upsert` — it is set
    /// only via `request_cancel` and cleared by `reclaim_requeue`; it defaults to
    /// NULL on insert.)
    pub upsert: String,
    pub select_body: String,
    pub select_status: String,
    pub select_submitted: String,
    pub delete: String,
    pub list: String,
    pub purge_runs: String,
    pub purge_idem: String,
    /// Select non-terminal runs whose owning instance's lease has expired (or
    /// is unset) — the orphans this instance may safely fail. Param: `now`.
    pub select_orphans: String,
    /// Extend the lease of this instance's own non-terminal runs (heartbeat).
    /// Params: `new_lease_expiry`, `instance_id`.
    pub renew_leases: String,
    pub insert_idem: String,
    pub select_idem: String,
    pub takeover_idem: String,
    /// Delete the idempotency claim(s) that point at a given run — used when a
    /// run is deleted so a replay of the key starts fresh rather than 404-ing
    /// on the missing record (#146 M8). Scoped by `run_id`, so a newer run that
    /// re-claimed the same key keeps its claim.
    pub delete_idem_by_run: String,
    /// Cluster dispatcher: fetch oldest pending runs up to a given limit.
    pub select_pending: String,
    /// Cluster dispatcher: atomically claim a pending run (set owner + running).
    pub claim_one: String,
    /// Cluster reclaimer: select expired running runs for requeue/fail evaluation.
    /// NOTE: `'queued'` is the single-instance status; cluster runs flow
    /// `pending → running`, so the failover reclaimer covers `'running'` only.
    pub reclaim_select: String,
    /// Cluster reclaimer: requeue an expired running run back to pending.
    pub reclaim_requeue: String,
    /// Cluster reclaimer: fail an expired running run that cannot be requeued.
    pub reclaim_fail: String,
    /// Finalize a run owned by this instance (terminal status update).
    pub finalize_owned: String,
    /// Cancel a pending run directly (transition pending → cancelled).
    pub cancel_pending: String,
    /// Request cancellation of an in-flight run owned by another instance.
    pub request_cancel: String,
    /// List run IDs owned by this instance that have a pending cancellation request.
    pub pending_cancellations: String,
    /// Upsert this instance's membership heartbeat into `faucet_serve_instances`.
    pub heartbeat_instance: String,
    /// List instances whose last heartbeat is at or after a given threshold.
    pub live_instances: String,
    /// Prune instances whose last heartbeat is before a given threshold.
    pub prune_instances: String,
    // ── Source shards (Mode B, #230) ─────────────────────────────────────────
    /// Idempotent shard insert (`ON CONFLICT (run_id, shard_id) DO NOTHING`).
    pub insert_shard: String,
    /// Select claimable pending shards joined to their run body, largest first.
    pub claim_shards_select: String,
    /// Atomically claim one pending shard for this instance.
    pub claim_shard_one: String,
    /// Heartbeat this instance's running shards.
    pub renew_shard_leases: String,
    /// Select expired-lease running shards for requeue/fail evaluation.
    pub reclaim_shards_select: String,
    /// Requeue an expired running shard back to pending (attempt++).
    pub reclaim_shard_requeue: String,
    /// Fail an expired running shard that exhausted its attempts (poison).
    pub reclaim_shard_fail: String,
    /// Owner-fenced terminal write for one shard.
    pub finalize_shard: String,
    /// Status counts for a run's shards.
    pub shard_progress: String,
    /// Distinct run_ids for which THIS instance owns a `running` shard whose
    /// parent run has a pending cancellation request (cross-instance shard
    /// cancel, F10). Param: `instance_id`.
    pub pending_shard_cancellations: String,
    /// Select run_ids of `sharded` parents (candidates to finalize once all
    /// their shards are terminal, F11).
    pub select_sharded_parents: String,
    /// Status-fenced terminal write for a `sharded` parent (F11). A benign
    /// double-finalize across instances is a no-op: the guard requires the
    /// parent to still be `sharded`. Does NOT re-arm owner/lease.
    pub finalize_sharded_parent: String,
    /// Delete a run's shard rows (paired with [`delete`](Self::delete) so a
    /// deleted run leaves no orphaned shard rows behind, F25). Param: `run_id`.
    pub delete_shards_by_run: String,
    /// Purge shard rows whose parent run no longer exists (run-record purged by
    /// retention, F25). No params — a set-difference against `faucet_serve_runs`.
    pub purge_orphan_shards: String,
}

impl Stmts {
    pub fn new(dialect: Dialect) -> Self {
        match dialect {
            Dialect::Postgres => Self::postgres(),
            Dialect::Sqlite => Self::sqlite(),
        }
    }

    fn postgres() -> Self {
        Self {
            upsert: "INSERT INTO faucet_serve_runs \
                (run_id,name,status,submitted_at,finished_at,idempotency_key,owner,lease_expires_at,body) \
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) \
                ON CONFLICT (run_id) DO UPDATE SET \
                name=excluded.name,status=excluded.status,submitted_at=excluded.submitted_at,\
                finished_at=excluded.finished_at,idempotency_key=excluded.idempotency_key,\
                owner=excluded.owner,lease_expires_at=excluded.lease_expires_at,\
                body=excluded.body"
                .into(),
            select_body: "SELECT body FROM faucet_serve_runs WHERE run_id=$1".into(),
            select_status: "SELECT status FROM faucet_serve_runs WHERE run_id=$1".into(),
            select_submitted: "SELECT submitted_at FROM faucet_serve_runs WHERE run_id=$1".into(),
            delete: "DELETE FROM faucet_serve_runs WHERE run_id=$1".into(),
            // Casts make the parameter types explicit so `$n IS NULL` cannot trip
            // Postgres' "could not determine data type of parameter" check.
            list: "SELECT body FROM faucet_serve_runs \
                WHERE ($1::text IS NULL OR status = $2::text) \
                AND ($3::text IS NULL OR name = $4::text) \
                AND ($5::text IS NULL OR submitted_at >= $6::text) \
                AND ($7::text IS NULL OR submitted_at <= $8::text) \
                AND ($9::text IS NULL OR (submitted_at < $10::text \
                    OR (submitted_at = $11::text AND run_id < $12::text))) \
                ORDER BY submitted_at DESC, run_id DESC LIMIT $13"
                .into(),
            purge_runs: "DELETE FROM faucet_serve_runs \
                WHERE status IN ('completed','failed','cancelled') \
                AND finished_at IS NOT NULL AND finished_at < $1"
                .into(),
            purge_idem: "DELETE FROM faucet_serve_idem WHERE claimed_at < $1".into(),
            select_orphans: "SELECT body FROM faucet_serve_runs \
                WHERE status IN ('queued','running') \
                AND (lease_expires_at IS NULL OR lease_expires_at < $1)"
                .into(),
            renew_leases: "UPDATE faucet_serve_runs SET lease_expires_at = $1 \
                WHERE owner = $2 AND status IN ('queued','running')"
                .into(),
            insert_idem: "INSERT INTO faucet_serve_idem (key,run_id,fingerprint,claimed_at) \
                VALUES ($1,$2,$3,$4) ON CONFLICT (key) DO NOTHING"
                .into(),
            select_idem: "SELECT run_id,fingerprint,claimed_at FROM faucet_serve_idem WHERE key=$1"
                .into(),
            takeover_idem: "UPDATE faucet_serve_idem \
                SET run_id=$1,fingerprint=$2,claimed_at=$3 WHERE key=$4 AND claimed_at=$5"
                .into(),
            delete_idem_by_run: "DELETE FROM faucet_serve_idem WHERE run_id=$1".into(),
            select_pending: "SELECT run_id, body FROM faucet_serve_runs \
                WHERE status = 'pending' ORDER BY submitted_at ASC LIMIT $1"
                .into(),
            claim_one: "UPDATE faucet_serve_runs \
                SET owner = $1, status = 'running', lease_expires_at = $2, body = $3 \
                WHERE run_id = $4 AND status = 'pending'"
                .into(),
            reclaim_select: "SELECT body FROM faucet_serve_runs \
                WHERE status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $1)"
                .into(),
            reclaim_requeue: "UPDATE faucet_serve_runs \
                SET status = 'pending', owner = NULL, lease_expires_at = NULL, \
                    cancel_requested = NULL, body = $1 \
                WHERE run_id = $2 AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $3)"
                .into(),
            reclaim_fail: "UPDATE faucet_serve_runs \
                SET status = 'failed', finished_at = $1, body = $2, owner = NULL \
                WHERE run_id = $3 AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $4)"
                .into(),
            finalize_owned: "UPDATE faucet_serve_runs \
                SET status = $1, finished_at = $2, lease_expires_at = $3, body = $4 \
                WHERE run_id = $5 AND owner = $6"
                .into(),
            cancel_pending: "UPDATE faucet_serve_runs \
                SET status = 'cancelled', finished_at = $1, body = $2 \
                WHERE run_id = $3 AND status = 'pending'"
                .into(),
            request_cancel: "UPDATE faucet_serve_runs \
                SET cancel_requested = $1 WHERE run_id = $2 AND status IN ('running','sharded')"
                .into(),
            pending_cancellations: "SELECT run_id FROM faucet_serve_runs \
                WHERE status = 'running' AND owner = $1 AND cancel_requested IS NOT NULL"
                .into(),
            heartbeat_instance: "INSERT INTO faucet_serve_instances \
                (instance_id, started_at, last_heartbeat, listen, max_concurrent, in_flight) \
                VALUES ($1,$2,$3,$4,$5,$6) \
                ON CONFLICT (instance_id) DO UPDATE SET \
                last_heartbeat = excluded.last_heartbeat, listen = excluded.listen, \
                max_concurrent = excluded.max_concurrent, in_flight = excluded.in_flight"
                .into(),
            live_instances: "SELECT instance_id, started_at, last_heartbeat, listen, \
                max_concurrent, in_flight FROM faucet_serve_instances \
                WHERE last_heartbeat >= $1"
                .into(),
            prune_instances: "DELETE FROM faucet_serve_instances WHERE last_heartbeat < $1".into(),
            insert_shard: "INSERT INTO faucet_serve_shards \
                (run_id, shard_id, descriptor, size_estimate, status, attempt) \
                VALUES ($1,$2,$3,$4,'pending','0') \
                ON CONFLICT (run_id, shard_id) DO NOTHING"
                .into(),
            claim_shards_select: "SELECT s.run_id, s.shard_id, s.descriptor, r.body \
                FROM faucet_serve_shards s JOIN faucet_serve_runs r ON r.run_id = s.run_id \
                WHERE s.status = 'pending' \
                ORDER BY CAST(COALESCE(s.size_estimate, '0') AS BIGINT) DESC, s.run_id, s.shard_id \
                LIMIT $1"
                .into(),
            claim_shard_one: "UPDATE faucet_serve_shards \
                SET owner = $1, status = 'running', lease_expires_at = $2 \
                WHERE run_id = $3 AND shard_id = $4 AND status = 'pending'"
                .into(),
            renew_shard_leases: "UPDATE faucet_serve_shards SET lease_expires_at = $1 \
                WHERE owner = $2 AND status = 'running'"
                .into(),
            reclaim_shards_select: "SELECT run_id, shard_id, attempt FROM faucet_serve_shards \
                WHERE status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $1)"
                .into(),
            reclaim_shard_requeue: "UPDATE faucet_serve_shards \
                SET status = 'pending', owner = NULL, lease_expires_at = NULL, attempt = $1 \
                WHERE run_id = $2 AND shard_id = $3 AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $4)"
                .into(),
            reclaim_shard_fail: "UPDATE faucet_serve_shards \
                SET status = 'failed', finished_at = $1, owner = NULL \
                WHERE run_id = $2 AND shard_id = $3 AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < $4)"
                .into(),
            finalize_shard: "UPDATE faucet_serve_shards \
                SET status = $1, finished_at = $2 \
                WHERE run_id = $3 AND shard_id = $4 AND owner = $5 AND status = 'running'"
                .into(),
            shard_progress: "SELECT status, COUNT(*) AS n FROM faucet_serve_shards \
                WHERE run_id = $1 GROUP BY status"
                .into(),
            pending_shard_cancellations: "SELECT DISTINCT s.run_id \
                FROM faucet_serve_shards s \
                JOIN faucet_serve_runs r ON r.run_id = s.run_id \
                WHERE s.owner = $1 AND s.status = 'running' \
                AND r.cancel_requested IS NOT NULL"
                .into(),
            select_sharded_parents: "SELECT run_id FROM faucet_serve_runs \
                WHERE status = 'sharded'"
                .into(),
            finalize_sharded_parent: "UPDATE faucet_serve_runs \
                SET status = $1, finished_at = $2, body = $3 \
                WHERE run_id = $4 AND status = 'sharded'"
                .into(),
            delete_shards_by_run: "DELETE FROM faucet_serve_shards WHERE run_id = $1".into(),
            purge_orphan_shards: "DELETE FROM faucet_serve_shards \
                WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)"
                .into(),
        }
    }

    fn sqlite() -> Self {
        Self {
            upsert: "INSERT INTO faucet_serve_runs \
                (run_id,name,status,submitted_at,finished_at,idempotency_key,owner,lease_expires_at,body) \
                VALUES (?,?,?,?,?,?,?,?,?) \
                ON CONFLICT (run_id) DO UPDATE SET \
                name=excluded.name,status=excluded.status,submitted_at=excluded.submitted_at,\
                finished_at=excluded.finished_at,idempotency_key=excluded.idempotency_key,\
                owner=excluded.owner,lease_expires_at=excluded.lease_expires_at,\
                body=excluded.body"
                .into(),
            select_body: "SELECT body FROM faucet_serve_runs WHERE run_id=?".into(),
            select_status: "SELECT status FROM faucet_serve_runs WHERE run_id=?".into(),
            select_submitted: "SELECT submitted_at FROM faucet_serve_runs WHERE run_id=?".into(),
            delete: "DELETE FROM faucet_serve_runs WHERE run_id=?".into(),
            list: "SELECT body FROM faucet_serve_runs \
                WHERE (? IS NULL OR status = ?) \
                AND (? IS NULL OR name = ?) \
                AND (? IS NULL OR submitted_at >= ?) \
                AND (? IS NULL OR submitted_at <= ?) \
                AND (? IS NULL OR (submitted_at < ? \
                    OR (submitted_at = ? AND run_id < ?))) \
                ORDER BY submitted_at DESC, run_id DESC LIMIT ?"
                .into(),
            purge_runs: "DELETE FROM faucet_serve_runs \
                WHERE status IN ('completed','failed','cancelled') \
                AND finished_at IS NOT NULL AND finished_at < ?"
                .into(),
            purge_idem: "DELETE FROM faucet_serve_idem WHERE claimed_at < ?".into(),
            select_orphans: "SELECT body FROM faucet_serve_runs \
                WHERE status IN ('queued','running') \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            renew_leases: "UPDATE faucet_serve_runs SET lease_expires_at = ? \
                WHERE owner = ? AND status IN ('queued','running')"
                .into(),
            insert_idem: "INSERT INTO faucet_serve_idem (key,run_id,fingerprint,claimed_at) \
                VALUES (?,?,?,?) ON CONFLICT (key) DO NOTHING"
                .into(),
            select_idem: "SELECT run_id,fingerprint,claimed_at FROM faucet_serve_idem WHERE key=?"
                .into(),
            takeover_idem: "UPDATE faucet_serve_idem \
                SET run_id=?,fingerprint=?,claimed_at=? WHERE key=? AND claimed_at=?"
                .into(),
            delete_idem_by_run: "DELETE FROM faucet_serve_idem WHERE run_id=?".into(),
            select_pending: "SELECT run_id, body FROM faucet_serve_runs \
                WHERE status = 'pending' ORDER BY submitted_at ASC LIMIT ?"
                .into(),
            claim_one: "UPDATE faucet_serve_runs \
                SET owner = ?, status = 'running', lease_expires_at = ?, body = ? \
                WHERE run_id = ? AND status = 'pending'"
                .into(),
            reclaim_select: "SELECT body FROM faucet_serve_runs \
                WHERE status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            reclaim_requeue: "UPDATE faucet_serve_runs \
                SET status = 'pending', owner = NULL, lease_expires_at = NULL, \
                    cancel_requested = NULL, body = ? \
                WHERE run_id = ? AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            reclaim_fail: "UPDATE faucet_serve_runs \
                SET status = 'failed', finished_at = ?, body = ?, owner = NULL \
                WHERE run_id = ? AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            finalize_owned: "UPDATE faucet_serve_runs \
                SET status = ?, finished_at = ?, lease_expires_at = ?, body = ? \
                WHERE run_id = ? AND owner = ?"
                .into(),
            cancel_pending: "UPDATE faucet_serve_runs \
                SET status = 'cancelled', finished_at = ?, body = ? \
                WHERE run_id = ? AND status = 'pending'"
                .into(),
            request_cancel: "UPDATE faucet_serve_runs \
                SET cancel_requested = ? WHERE run_id = ? AND status IN ('running','sharded')"
                .into(),
            pending_cancellations: "SELECT run_id FROM faucet_serve_runs \
                WHERE status = 'running' AND owner = ? AND cancel_requested IS NOT NULL"
                .into(),
            heartbeat_instance: "INSERT INTO faucet_serve_instances \
                (instance_id, started_at, last_heartbeat, listen, max_concurrent, in_flight) \
                VALUES (?,?,?,?,?,?) \
                ON CONFLICT (instance_id) DO UPDATE SET \
                last_heartbeat = excluded.last_heartbeat, listen = excluded.listen, \
                max_concurrent = excluded.max_concurrent, in_flight = excluded.in_flight"
                .into(),
            live_instances: "SELECT instance_id, started_at, last_heartbeat, listen, \
                max_concurrent, in_flight FROM faucet_serve_instances \
                WHERE last_heartbeat >= ?"
                .into(),
            prune_instances: "DELETE FROM faucet_serve_instances WHERE last_heartbeat < ?".into(),
            insert_shard: "INSERT INTO faucet_serve_shards \
                (run_id, shard_id, descriptor, size_estimate, status, attempt) \
                VALUES (?,?,?,?,'pending','0') \
                ON CONFLICT (run_id, shard_id) DO NOTHING"
                .into(),
            claim_shards_select: "SELECT s.run_id, s.shard_id, s.descriptor, r.body \
                FROM faucet_serve_shards s JOIN faucet_serve_runs r ON r.run_id = s.run_id \
                WHERE s.status = 'pending' \
                ORDER BY CAST(COALESCE(s.size_estimate, '0') AS INTEGER) DESC, s.run_id, s.shard_id \
                LIMIT ?"
                .into(),
            claim_shard_one: "UPDATE faucet_serve_shards \
                SET owner = ?, status = 'running', lease_expires_at = ? \
                WHERE run_id = ? AND shard_id = ? AND status = 'pending'"
                .into(),
            renew_shard_leases: "UPDATE faucet_serve_shards SET lease_expires_at = ? \
                WHERE owner = ? AND status = 'running'"
                .into(),
            reclaim_shards_select: "SELECT run_id, shard_id, attempt FROM faucet_serve_shards \
                WHERE status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            reclaim_shard_requeue: "UPDATE faucet_serve_shards \
                SET status = 'pending', owner = NULL, lease_expires_at = NULL, attempt = ? \
                WHERE run_id = ? AND shard_id = ? AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            reclaim_shard_fail: "UPDATE faucet_serve_shards \
                SET status = 'failed', finished_at = ?, owner = NULL \
                WHERE run_id = ? AND shard_id = ? AND status = 'running' \
                AND (lease_expires_at IS NULL OR lease_expires_at < ?)"
                .into(),
            finalize_shard: "UPDATE faucet_serve_shards \
                SET status = ?, finished_at = ? \
                WHERE run_id = ? AND shard_id = ? AND owner = ? AND status = 'running'"
                .into(),
            shard_progress: "SELECT status, COUNT(*) AS n FROM faucet_serve_shards \
                WHERE run_id = ? GROUP BY status"
                .into(),
            pending_shard_cancellations: "SELECT DISTINCT s.run_id \
                FROM faucet_serve_shards s \
                JOIN faucet_serve_runs r ON r.run_id = s.run_id \
                WHERE s.owner = ? AND s.status = 'running' \
                AND r.cancel_requested IS NOT NULL"
                .into(),
            select_sharded_parents: "SELECT run_id FROM faucet_serve_runs \
                WHERE status = 'sharded'"
                .into(),
            finalize_sharded_parent: "UPDATE faucet_serve_runs \
                SET status = ?, finished_at = ?, body = ? \
                WHERE run_id = ? AND status = 'sharded'"
                .into(),
            delete_shards_by_run: "DELETE FROM faucet_serve_shards WHERE run_id = ?".into(),
            purge_orphan_shards: "DELETE FROM faucet_serve_shards \
                WHERE run_id NOT IN (SELECT run_id FROM faucet_serve_runs)"
                .into(),
        }
    }
}

/// Bounded retry count for the atomic idempotency claim (handles a claim being
/// purged concurrently between the insert attempt and the read-back).
pub const CLAIM_ATTEMPTS: usize = 4;

/// Fixed-width RFC3339 (nanoseconds + `Z`) — lexicographically sortable.
pub fn fmt_ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

/// True when a claim timestamped `claimed_at` (RFC3339) is older than `window`.
/// An unparseable or future timestamp is treated as **not** expired (safe: it
/// won't be silently re-claimed).
pub fn is_expired(claimed_at: &str, now: DateTime<Utc>, window: Duration) -> bool {
    match DateTime::parse_from_rfc3339(claimed_at) {
        Ok(t) => now
            .signed_duration_since(t.with_timezone(&Utc))
            .to_std()
            .map(|age| age >= window)
            .unwrap_or(false),
        Err(_) => false,
    }
}

/// RFC3339 timestamp `window` before `now` (the purge / expiry threshold).
pub fn threshold(now: DateTime<Utc>, window: Duration) -> String {
    let delta =
        chrono::Duration::from_std(window).unwrap_or_else(|_| chrono::Duration::days(36_500));
    fmt_ts(now - delta)
}

pub fn encode_body(rec: &RunRecord) -> Result<String, HistoryError> {
    serde_json::to_string(rec).map_err(|e| HistoryError::Backend(format!("encode run record: {e}")))
}

pub fn decode_body(body: &str) -> Result<RunRecord, HistoryError> {
    serde_json::from_str(body).map_err(|e| HistoryError::Backend(format!("decode run record: {e}")))
}

pub fn parse_status(s: &str) -> RunStatus {
    match s {
        "queued" => RunStatus::Queued,
        "pending" => RunStatus::Pending,
        "running" => RunStatus::Running,
        "sharded" => RunStatus::Sharded,
        "completed" => RunStatus::Completed,
        "cancelled" => RunStatus::Cancelled,
        _ => RunStatus::Failed,
    }
}

/// Generate a concrete `RunHistory` implementation over a specific `sqlx` pool.
/// `$name` is the backend struct, `$pool` its `sqlx` pool type. The struct holds
/// the pool, the idempotency retention window, and the dialect's [`Stmts`].
macro_rules! impl_sql_history {
    ($name:ident, $pool:ty) => {
        /// SQL-backed [`RunHistory`](crate::serve::history::RunHistory). See
        /// [`crate::serve::history::sql`] for the shared schema + semantics.
        pub struct $name {
            pool: $pool,
            idem_retention: std::time::Duration,
            /// This serve instance's id, stamped as `owner` on every upsert.
            instance_id: String,
            /// How far ahead each upsert / heartbeat pushes a run's lease.
            lease_ttl: std::time::Duration,
            stmts: $crate::serve::history::sql::Stmts,
        }

        impl $name {
            /// Assemble from an already-connected pool (used by `connect`).
            pub fn from_parts(
                pool: $pool,
                idem_retention: std::time::Duration,
                lease_ttl: std::time::Duration,
                instance_id: String,
                stmts: $crate::serve::history::sql::Stmts,
            ) -> Self {
                Self {
                    pool,
                    idem_retention,
                    instance_id,
                    lease_ttl,
                    stmts,
                }
            }

            /// Borrow the underlying pool (tests close it to exercise fallback).
            pub fn pool(&self) -> &$pool {
                &self.pool
            }
        }

        #[async_trait::async_trait]
        impl $crate::serve::history::RunHistory for $name {
            async fn claim_idempotency(
                &self,
                key: &str,
                fingerprint: &str,
                run_id: &str,
                window: std::time::Duration,
            ) -> Result<$crate::serve::history::Claim, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::Claim;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;

                let now = chrono::Utc::now();
                let now_s = sql::fmt_ts(now);
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());

                for _ in 0..sql::CLAIM_ATTEMPTS {
                    // 1) Atomic first-claim: the winner inserts exactly one row.
                    let inserted = sqlx::query(&self.stmts.insert_idem)
                        .bind(key)
                        .bind(run_id)
                        .bind(fingerprint)
                        .bind(&now_s)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    if inserted == 1 {
                        return Ok(Claim::Fresh);
                    }
                    // 2) Conflict: inspect the existing claim.
                    let Some(row) = sqlx::query(&self.stmts.select_idem)
                        .bind(key)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(backend)?
                    else {
                        // Vanished between the insert and the read — retry.
                        continue;
                    };
                    let existing_run: String = row.try_get("run_id").map_err(backend)?;
                    let existing_fp: String = row.try_get("fingerprint").map_err(backend)?;
                    let claimed_at: String = row.try_get("claimed_at").map_err(backend)?;

                    if sql::is_expired(&claimed_at, now, window) {
                        // 3) Optimistic, expiry-guarded takeover: only the request
                        // that still sees `claimed_at` succeeds.
                        let took = sqlx::query(&self.stmts.takeover_idem)
                            .bind(run_id)
                            .bind(fingerprint)
                            .bind(&now_s)
                            .bind(key)
                            .bind(&claimed_at)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if took == 1 {
                            return Ok(Claim::Fresh);
                        }
                        continue; // lost the race; re-evaluate
                    }
                    return Ok(if existing_fp == fingerprint {
                        Claim::Replay(existing_run)
                    } else {
                        Claim::Conflict
                    });
                }
                // Pathological contention only. Conservative: a 409 is safer than
                // risking a duplicate run.
                tracing::warn!(
                    key,
                    "idempotency claim exhausted retries; reporting conflict"
                );
                Ok(Claim::Conflict)
            }

            async fn upsert(
                &self,
                rec: &$crate::serve::history::RunRecord,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let body = sql::encode_body(rec)?;
                let submitted = sql::fmt_ts(rec.submitted_at);
                let finished = rec.finished_at.map(sql::fmt_ts);
                // Stamp this instance as the owner and start/renew the lease.
                // The owner/lease are SQL-column-only (never in the record body),
                // so the heartbeat can extend a lease without a body read-modify-
                // write race (#146 H7).
                let lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);
                sqlx::query(&self.stmts.upsert)
                    .bind(&rec.run_id)
                    .bind(rec.name.as_deref())
                    .bind(rec.status.as_str())
                    .bind(&submitted)
                    .bind(finished.as_deref())
                    .bind(rec.idempotency_key.as_deref())
                    .bind(&self.instance_id)
                    .bind(&lease)
                    .bind(&body)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| HistoryError::Backend(e.to_string()))?;
                Ok(())
            }

            async fn get(
                &self,
                id: &str,
            ) -> Result<
                Option<$crate::serve::history::RunRecord>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let row = sqlx::query(&self.stmts.select_body)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| HistoryError::Backend(e.to_string()))?;
                match row {
                    None => Ok(None),
                    Some(r) => {
                        let body: String = r
                            .try_get("body")
                            .map_err(|e| HistoryError::Backend(e.to_string()))?;
                        Ok(Some(sql::decode_body(&body)?))
                    }
                }
            }

            async fn list(
                &self,
                filter: &$crate::serve::history::ListFilter,
            ) -> Result<$crate::serve::history::ListPage, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::ListPage;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());

                // Resolve the cursor's submitted_at for keyset pagination. An
                // unknown cursor is ignored (page starts from the top), matching
                // the memory backend.
                let cursor_ts: Option<String> = match &filter.cursor {
                    None => None,
                    Some(c) => sqlx::query(&self.stmts.select_submitted)
                        .bind(c)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(backend)?
                        .map(|r| r.try_get::<String, _>("submitted_at"))
                        .transpose()
                        .map_err(backend)?,
                };
                let cur_id = if cursor_ts.is_some() {
                    filter.cursor.as_deref()
                } else {
                    None
                };

                let status_s = filter.status.map(|s| s.as_str());
                let name_s = filter.name.as_deref();
                let since_s = filter.since.map(sql::fmt_ts);
                let until_s = filter.until.map(sql::fmt_ts);
                let limit = filter.limit.max(1);
                let fetch_n = limit as i64 + 1; // +1 to detect a next page

                let rows = sqlx::query(&self.stmts.list)
                    .bind(status_s)
                    .bind(status_s)
                    .bind(name_s)
                    .bind(name_s)
                    .bind(since_s.as_deref())
                    .bind(since_s.as_deref())
                    .bind(until_s.as_deref())
                    .bind(until_s.as_deref())
                    .bind(cursor_ts.as_deref())
                    .bind(cursor_ts.as_deref())
                    .bind(cursor_ts.as_deref())
                    .bind(cur_id)
                    .bind(fetch_n)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                let mut runs = Vec::with_capacity(rows.len());
                for r in &rows {
                    let body: String = r.try_get("body").map_err(backend)?;
                    runs.push(sql::decode_body(&body)?);
                }
                let next_cursor = if runs.len() > limit {
                    Some(runs[limit - 1].run_id.clone())
                } else {
                    None
                };
                runs.truncate(limit);
                Ok(ListPage { runs, next_cursor })
            }

            async fn delete(
                &self,
                id: &str,
            ) -> Result<$crate::serve::history::DeleteOutcome, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::DeleteOutcome;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let status: Option<String> = sqlx::query(&self.stmts.select_status)
                    .bind(id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(backend)?
                    .map(|r| r.try_get::<String, _>("status"))
                    .transpose()
                    .map_err(backend)?;
                match status {
                    None => Ok(DeleteOutcome::NotFound),
                    Some(s) if !sql::parse_status(&s).is_terminal() => {
                        Ok(DeleteOutcome::StillRunning)
                    }
                    Some(_) => {
                        sqlx::query(&self.stmts.delete)
                            .bind(id)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                        // Drop the run's idempotency claim too, so a replay of
                        // the key starts fresh instead of 404-ing on the deleted
                        // record until the claim self-expires (#146 M8). Scoped
                        // by run_id, so a newer run that re-claimed the same key
                        // keeps its claim.
                        sqlx::query(&self.stmts.delete_idem_by_run)
                            .bind(id)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                        // Drop the run's shard rows too (Mode B, #230), so a
                        // deleted run leaves no orphaned shard rows that would
                        // otherwise leak unboundedly (F25).
                        sqlx::query(&self.stmts.delete_shards_by_run)
                            .bind(id)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?;
                        Ok(DeleteOutcome::Deleted)
                    }
                }
            }

            async fn release_idempotency(
                &self,
                run_id: &str,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                sqlx::query(&self.stmts.delete_idem_by_run)
                    .bind(run_id)
                    .execute(&self.pool)
                    .await
                    .map_err(|e| HistoryError::Backend(e.to_string()))?;
                Ok(())
            }

            async fn purge_expired(
                &self,
                retain_for: std::time::Duration,
            ) -> Result<usize, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = chrono::Utc::now();
                let removed = sqlx::query(&self.stmts.purge_runs)
                    .bind(sql::threshold(now, retain_for))
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected() as usize;
                // Drop expired idempotency claims too (best-effort).
                let _ = sqlx::query(&self.stmts.purge_idem)
                    .bind(sql::threshold(now, self.idem_retention))
                    .execute(&self.pool)
                    .await;
                // Drop membership rows that have not heartbeated within the
                // run-retention window (far longer than the lease, so this never
                // prunes a live member — that's `live_instances(ttl)`'s job).
                let _ = sqlx::query(&self.stmts.prune_instances)
                    .bind(sql::threshold(now, retain_for))
                    .execute(&self.pool)
                    .await;
                // Reclaim shard rows whose parent run was just purged (F25):
                // `purge_runs` removed the expired terminal records above, so any
                // shard row no longer matching a run is orphaned. Best-effort.
                let _ = sqlx::query(&self.stmts.purge_orphan_shards)
                    .execute(&self.pool)
                    .await;
                Ok(removed)
            }

            async fn recover_orphans(&self) -> Result<usize, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = chrono::Utc::now();
                // Only non-terminal runs whose lease has expired (the owning
                // instance is presumed dead). A live instance heartbeats its
                // runs' leases into the future, so this never fails another
                // healthy instance's in-flight runs (#146 H7).
                let rows = sqlx::query(&self.stmts.select_orphans)
                    .bind(sql::fmt_ts(now))
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut count = 0usize;
                for r in &rows {
                    let body: String = r.try_get("body").map_err(backend)?;
                    let mut rec = sql::decode_body(&body)?;
                    rec.status = RunStatus::Failed;
                    rec.finished_at = Some(now);
                    rec.error = Some(
                        "owning serve instance's lease expired before the run finished".into(),
                    );
                    if rec.elapsed_secs.is_none()
                        && let Some(started) = rec.started_at
                    {
                        rec.elapsed_secs = (now - started).to_std().ok().map(|d| d.as_secs_f64());
                    }
                    self.upsert(&rec).await?;
                    count += 1;
                }
                Ok(count)
            }

            async fn renew_leases(&self) -> Result<usize, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let new_lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);
                let renewed = sqlx::query(&self.stmts.renew_leases)
                    .bind(&new_lease)
                    .bind(&self.instance_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected() as usize;
                Ok(renewed)
            }

            async fn claim_pending(
                &self,
                limit: usize,
            ) -> Result<Vec<$crate::serve::history::RunRecord>, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let now = chrono::Utc::now();
                let lease = sql::fmt_ts(now + self.lease_ttl);

                // 1. Candidate pending runs (oldest first), with their bodies.
                let rows = sqlx::query(&self.stmts.select_pending)
                    .bind(limit as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                // Per-row conditional claim (1 SELECT + N guarded UPDATEs). The
                // batch is bounded by the caller's free permits (small), and this
                // is portable across Postgres + SQLite — deliberately NOT a
                // Postgres-only `FOR UPDATE SKIP LOCKED`.
                let mut claimed = Vec::new();
                for row in &rows {
                    let run_id: String = row.try_get("run_id").map_err(backend)?;
                    let body: String = row.try_get("body").map_err(backend)?;
                    // Flip the record to Running and rewrite the body so the column
                    // and the (source-of-truth) body stay consistent — a GET right
                    // after the claim must not show a stale `pending`.
                    let mut r = sql::decode_body(&body)?;
                    r.status = RunStatus::Running;
                    let new_body = sql::encode_body(&r)?;
                    // 2. Conditional claim — only the first committer wins.
                    let won = sqlx::query(&self.stmts.claim_one)
                        .bind(&self.instance_id)
                        .bind(&lease)
                        .bind(&new_body)
                        .bind(&run_id)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    if won == 1 {
                        claimed.push(r);
                    }
                }
                Ok(claimed)
            }

            async fn reclaim_orphans(
                &self,
                max_attempts: u32,
            ) -> Result<$crate::serve::history::ReclaimReport, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::ReclaimReport;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = chrono::Utc::now();
                let now_s = sql::fmt_ts(now);

                let rows = sqlx::query(&self.stmts.reclaim_select)
                    .bind(&now_s)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                let mut report = ReclaimReport::default();
                for row in &rows {
                    let body: String = row.try_get("body").map_err(backend)?;
                    let mut rec = sql::decode_body(&body)?;
                    let next_attempt = rec.attempt + 1;
                    // Cap is on the attempts already made: a run that has been
                    // reclaimed fewer than `max_attempts` times gets another try;
                    // once it reaches the cap it is poisoned.
                    if rec.attempt < max_attempts {
                        // Re-queue for another instance to re-run.
                        rec.attempt = next_attempt;
                        rec.status = RunStatus::Pending;
                        let new_body = sql::encode_body(&rec)?;
                        let n = sqlx::query(&self.stmts.reclaim_requeue)
                            .bind(&new_body)
                            .bind(&rec.run_id)
                            .bind(&now_s)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if n == 1 {
                            report.requeued += 1;
                        }
                    } else {
                        // Poison: too many attempts.
                        rec.attempt = next_attempt;
                        rec.status = RunStatus::Failed;
                        rec.finished_at = Some(now);
                        rec.error = Some(format!(
                            "run reclaimed {next_attempt} times after its owning instance's \
                             lease expired; giving up (poison run)"
                        ));
                        if rec.elapsed_secs.is_none()
                            && let Some(started) = rec.started_at
                        {
                            rec.elapsed_secs =
                                (now - started).to_std().ok().map(|d| d.as_secs_f64());
                        }
                        let new_body = sql::encode_body(&rec)?;
                        let n = sqlx::query(&self.stmts.reclaim_fail)
                            .bind(&now_s)
                            .bind(&new_body)
                            .bind(&rec.run_id)
                            .bind(&now_s)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if n == 1 {
                            report.failed += 1;
                        }
                    }
                }
                Ok(report)
            }

            async fn finalize_owned(
                &self,
                rec: &$crate::serve::history::RunRecord,
            ) -> Result<bool, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                // Defensive: a terminal record must carry finished_at, or
                // purge_runs (which requires finished_at IS NOT NULL) can never
                // reclaim it. Stamp it if a caller left it unset.
                let mut rec = rec.clone();
                if rec.status.is_terminal() && rec.finished_at.is_none() {
                    rec.finished_at = Some(chrono::Utc::now());
                }
                let body = sql::encode_body(&rec)?;
                let finished = rec.finished_at.map(sql::fmt_ts);
                let lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);
                let n = sqlx::query(&self.stmts.finalize_owned)
                    .bind(rec.status.as_str())
                    .bind(finished.as_deref())
                    .bind(&lease)
                    .bind(&body)
                    .bind(&rec.run_id)
                    .bind(&self.instance_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected();
                Ok(n == 1)
            }

            async fn finalize_sharded_parent(
                &self,
                run_id: &str,
                status: $crate::serve::history::RunStatus,
                finished_at: chrono::DateTime<chrono::Utc>,
                error: Option<String>,
            ) -> Result<bool, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                // Read the parent body, apply the terminal status, and write back
                // conditional on it still being `sharded` — so a concurrent
                // double-finalize from two instances has exactly one winner and
                // neither re-stamps owner/lease on the terminal record (F45).
                let Some(row) = sqlx::query(&self.stmts.select_body)
                    .bind(run_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(backend)?
                else {
                    return Ok(false);
                };
                let body: String = row.try_get("body").map_err(backend)?;
                let mut rec = sql::decode_body(&body)?;
                if rec.status != RunStatus::Sharded {
                    return Ok(false);
                }
                rec.status = status;
                rec.finished_at = Some(finished_at);
                rec.error = error;
                let new_body = sql::encode_body(&rec)?;
                let n = sqlx::query(&self.stmts.finalize_sharded_parent)
                    .bind(status.as_str())
                    .bind(sql::fmt_ts(finished_at))
                    .bind(&new_body)
                    .bind(run_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected();
                Ok(n == 1)
            }

            async fn cancel_pending(
                &self,
                run_id: &str,
            ) -> Result<bool, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                // Read the pending run's body, flip it to Cancelled, and write back
                // conditional on it still being pending (loses the race to a claim).
                let Some(row) = sqlx::query(&self.stmts.select_body)
                    .bind(run_id)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(backend)?
                else {
                    return Ok(false);
                };
                let body: String = row.try_get("body").map_err(backend)?;
                let mut rec = sql::decode_body(&body)?;
                if rec.status != RunStatus::Pending {
                    return Ok(false);
                }
                let now = chrono::Utc::now();
                rec.status = RunStatus::Cancelled;
                rec.finished_at = Some(now);
                let new_body = sql::encode_body(&rec)?;
                let n = sqlx::query(&self.stmts.cancel_pending)
                    .bind(sql::fmt_ts(now))
                    .bind(&new_body)
                    .bind(run_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected();
                Ok(n == 1)
            }

            async fn request_cancel(
                &self,
                run_id: &str,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                sqlx::query(&self.stmts.request_cancel)
                    .bind(sql::fmt_ts(chrono::Utc::now()))
                    .bind(run_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?;
                Ok(())
            }

            async fn pending_cancellations(
                &self,
            ) -> Result<Vec<String>, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.pending_cancellations)
                    .bind(&self.instance_id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut ids = Vec::with_capacity(rows.len());
                for r in &rows {
                    ids.push(r.try_get::<String, _>("run_id").map_err(backend)?);
                }
                Ok(ids)
            }

            async fn heartbeat_instance(
                &self,
                beat: &$crate::serve::history::InstanceHeartbeat,
            ) -> Result<(), $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = sql::fmt_ts(chrono::Utc::now());
                sqlx::query(&self.stmts.heartbeat_instance)
                    .bind(&self.instance_id)
                    .bind(sql::fmt_ts(beat.started_at))
                    .bind(&now)
                    .bind(beat.listen.as_deref())
                    .bind(beat.max_concurrent.to_string())
                    .bind(beat.in_flight.to_string())
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?;
                Ok(())
            }

            async fn live_instances(
                &self,
                ttl: std::time::Duration,
            ) -> Result<Vec<$crate::serve::history::InstanceRecord>, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::InstanceRecord;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now = chrono::Utc::now();
                let rows = sqlx::query(&self.stmts.live_instances)
                    .bind(sql::threshold(now, ttl))
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let parse_dt = |s: &str| {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .map(|d| d.to_utc())
                        .unwrap_or(now)
                };
                let mut out = Vec::with_capacity(rows.len());
                for r in &rows {
                    let started: String = r.try_get("started_at").map_err(backend)?;
                    let hb: String = r.try_get("last_heartbeat").map_err(backend)?;
                    let mc: Option<String> = r.try_get("max_concurrent").map_err(backend)?;
                    let inf: Option<String> = r.try_get("in_flight").map_err(backend)?;
                    out.push(InstanceRecord {
                        instance_id: r.try_get("instance_id").map_err(backend)?,
                        started_at: parse_dt(&started),
                        last_heartbeat: parse_dt(&hb),
                        listen: r.try_get("listen").map_err(backend)?,
                        max_concurrent: mc.and_then(|s| s.parse().ok()).unwrap_or(0),
                        in_flight: inf.and_then(|s| s.parse().ok()).unwrap_or(0),
                    });
                }
                Ok(out)
            }

            // ── Source shards (Mode B, #230) ─────────────────────────────────

            async fn insert_shards(
                &self,
                run_id: &str,
                shards: &[$crate::serve::history::ShardInsert],
            ) -> Result<usize, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let mut inserted = 0usize;
                for s in shards {
                    let descriptor = serde_json::to_string(&s.descriptor).map_err(|e| {
                        HistoryError::Backend(format!("encode shard descriptor: {e}"))
                    })?;
                    let size = s.size_estimate.map(|n| n.to_string());
                    let n = sqlx::query(&self.stmts.insert_shard)
                        .bind(run_id)
                        .bind(&s.shard_id)
                        .bind(&descriptor)
                        .bind(size.as_deref())
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    inserted += n as usize;
                }
                Ok(inserted)
            }

            async fn claim_shards(
                &self,
                limit: usize,
            ) -> Result<
                Vec<$crate::serve::history::ClaimedShard>,
                $crate::serve::history::HistoryError,
            > {
                use sqlx::Row as _;
                use $crate::serve::history::ClaimedShard;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                if limit == 0 {
                    return Ok(Vec::new());
                }
                let lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);

                // 1. Candidate pending shards (largest estimated size first),
                //    joined to their parent run body.
                let rows = sqlx::query(&self.stmts.claim_shards_select)
                    .bind(limit as i64)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                // 2. Per-row conditional claim (portable; not FOR UPDATE SKIP LOCKED).
                let mut claimed = Vec::new();
                for row in &rows {
                    let run_id: String = row.try_get("run_id").map_err(backend)?;
                    let shard_id: String = row.try_get("shard_id").map_err(backend)?;
                    let descriptor_s: String = row.try_get("descriptor").map_err(backend)?;
                    let body: String = row.try_get("body").map_err(backend)?;
                    let won = sqlx::query(&self.stmts.claim_shard_one)
                        .bind(&self.instance_id)
                        .bind(&lease)
                        .bind(&run_id)
                        .bind(&shard_id)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    if won == 1 {
                        let descriptor: serde_json::Value = serde_json::from_str(&descriptor_s)
                            .map_err(|e| {
                                HistoryError::Backend(format!("decode shard descriptor: {e}"))
                            })?;
                        let run = sql::decode_body(&body)?;
                        claimed.push(ClaimedShard {
                            run_id,
                            shard_id,
                            descriptor,
                            run,
                        });
                    }
                }
                Ok(claimed)
            }

            async fn renew_shard_leases(
                &self,
            ) -> Result<usize, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let lease = sql::fmt_ts(chrono::Utc::now() + self.lease_ttl);
                let n = sqlx::query(&self.stmts.renew_shard_leases)
                    .bind(&lease)
                    .bind(&self.instance_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected() as usize;
                Ok(n)
            }

            async fn reclaim_shards(
                &self,
                max_attempts: u32,
            ) -> Result<$crate::serve::history::ReclaimReport, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::ReclaimReport;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let now_s = sql::fmt_ts(chrono::Utc::now());

                let rows = sqlx::query(&self.stmts.reclaim_shards_select)
                    .bind(&now_s)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                let mut report = ReclaimReport::default();
                for row in &rows {
                    let run_id: String = row.try_get("run_id").map_err(backend)?;
                    let shard_id: String = row.try_get("shard_id").map_err(backend)?;
                    let attempt_s: String = row.try_get("attempt").map_err(backend)?;
                    let attempt: u32 = attempt_s.parse().unwrap_or(0);
                    if attempt < max_attempts {
                        let next = (attempt + 1).to_string();
                        let n = sqlx::query(&self.stmts.reclaim_shard_requeue)
                            .bind(&next)
                            .bind(&run_id)
                            .bind(&shard_id)
                            .bind(&now_s)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if n == 1 {
                            report.requeued += 1;
                        }
                    } else {
                        let n = sqlx::query(&self.stmts.reclaim_shard_fail)
                            .bind(&now_s)
                            .bind(&run_id)
                            .bind(&shard_id)
                            .bind(&now_s)
                            .execute(&self.pool)
                            .await
                            .map_err(backend)?
                            .rows_affected();
                        if n == 1 {
                            report.failed += 1;
                        }
                    }
                }
                Ok(report)
            }

            async fn finalize_shard(
                &self,
                run_id: &str,
                shard_id: &str,
                success: bool,
            ) -> Result<bool, $crate::serve::history::HistoryError> {
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let status = if success { "completed" } else { "failed" };
                let now_s = sql::fmt_ts(chrono::Utc::now());
                let n = sqlx::query(&self.stmts.finalize_shard)
                    .bind(status)
                    .bind(&now_s)
                    .bind(run_id)
                    .bind(shard_id)
                    .bind(&self.instance_id)
                    .execute(&self.pool)
                    .await
                    .map_err(backend)?
                    .rows_affected();
                Ok(n == 1)
            }

            async fn shard_progress(
                &self,
                run_id: &str,
            ) -> Result<$crate::serve::history::ShardProgress, $crate::serve::history::HistoryError>
            {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::ShardProgress;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.shard_progress)
                    .bind(run_id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut p = ShardProgress::default();
                for row in &rows {
                    let status: String = row.try_get("status").map_err(backend)?;
                    let n: i64 = row.try_get("n").map_err(backend)?;
                    let n = n.max(0) as usize;
                    p.total += n;
                    match status.as_str() {
                        "completed" => p.completed += n,
                        "failed" => p.failed += n,
                        "running" => p.running += n,
                        _ => p.pending += n,
                    }
                }
                Ok(p)
            }

            async fn pending_shard_cancellations(
                &self,
            ) -> Result<Vec<String>, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.pending_shard_cancellations)
                    .bind(&self.instance_id)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let mut ids = Vec::with_capacity(rows.len());
                for r in &rows {
                    ids.push(r.try_get::<String, _>("run_id").map_err(backend)?);
                }
                Ok(ids)
            }

            async fn finalize_completed_sharded_parents(
                &self,
            ) -> Result<usize, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());

                // Candidate `sharded` parents — finalize each whose shards are all
                // terminal. The status-fenced UPDATE makes a concurrent finalize
                // (here or in `maybe_finalize_parent`) a benign no-op.
                let rows = sqlx::query(&self.stmts.select_sharded_parents)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;

                let mut finalized = 0usize;
                for row in &rows {
                    let run_id: String = row.try_get("run_id").map_err(backend)?;
                    let progress = self.shard_progress(&run_id).await?;
                    if !progress.all_terminal() {
                        continue;
                    }
                    let success = progress.failed == 0;
                    // Read-modify-write the body so the surfaced record stays
                    // consistent (status, finished_at, error) with the column.
                    let Some(body_row) = sqlx::query(&self.stmts.select_body)
                        .bind(&run_id)
                        .fetch_optional(&self.pool)
                        .await
                        .map_err(backend)?
                    else {
                        continue;
                    };
                    let body: String = body_row.try_get("body").map_err(backend)?;
                    let mut rec = sql::decode_body(&body)?;
                    // Skip if it raced to terminal already (column says sharded but
                    // the body was just updated). The fenced UPDATE is the real guard.
                    if rec.status != RunStatus::Sharded {
                        continue;
                    }
                    let now = chrono::Utc::now();
                    rec.status = if success {
                        RunStatus::Completed
                    } else {
                        RunStatus::Failed
                    };
                    rec.finished_at = Some(now);
                    if !success {
                        rec.error = Some(format!(
                            "{}/{} shard(s) failed",
                            progress.failed, progress.total
                        ));
                    }
                    let new_body = sql::encode_body(&rec)?;
                    let n = sqlx::query(&self.stmts.finalize_sharded_parent)
                        .bind(rec.status.as_str())
                        .bind(sql::fmt_ts(now))
                        .bind(&new_body)
                        .bind(&run_id)
                        .execute(&self.pool)
                        .await
                        .map_err(backend)?
                        .rows_affected();
                    if n == 1 {
                        finalized += 1;
                        $crate::serve::metrics::record_run_finished(
                            rec.status,
                            if success { "ok" } else { "error" },
                        );
                        tracing::info!(
                            run_id,
                            shards = progress.total,
                            failed = progress.failed,
                            "sharded run finalized by sweep (F11)"
                        );
                    }
                }
                Ok(finalized)
            }

            fn degraded(&self) -> bool {
                // A live SQL backend is never self-degraded; the FallbackHistory
                // wrapper owns degradation when the backend becomes unreachable.
                false
            }
        }
    };
}

pub(crate) use impl_sql_history;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postgres_shard_statements_are_built() {
        // SQLite tests only build the Sqlite statement set; exercise the
        // Postgres shard-statement construction too (Mode B, #230).
        let s = Stmts::new(Dialect::Postgres);
        assert!(s.insert_shard.contains("faucet_serve_shards"));
        assert!(s.insert_shard.contains("ON CONFLICT"));
        assert!(s.claim_shards_select.contains("JOIN faucet_serve_runs"));
        assert!(s.claim_shard_one.contains("'running'"));
        assert!(s.renew_shard_leases.contains("lease_expires_at"));
        assert!(s.reclaim_shards_select.contains("'running'"));
        assert!(s.reclaim_shard_requeue.contains("'pending'"));
        assert!(s.reclaim_shard_fail.contains("'failed'"));
        assert!(s.finalize_shard.contains("owner"));
        assert!(s.shard_progress.contains("GROUP BY"));
    }

    #[test]
    fn fmt_ts_is_fixed_width_and_sortable() {
        let a = fmt_ts(
            DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
                .unwrap()
                .to_utc(),
        );
        let b = fmt_ts(
            DateTime::parse_from_rfc3339("2026-01-01T00:00:01Z")
                .unwrap()
                .to_utc(),
        );
        assert!(a.ends_with('Z'));
        assert_eq!(a.len(), b.len(), "fixed width");
        assert!(a < b, "lexicographic order matches chronological order");
    }

    #[test]
    fn is_expired_respects_window() {
        let now = Utc::now();
        let old = fmt_ts(now - chrono::Duration::seconds(120));
        assert!(is_expired(&old, now, Duration::from_secs(60)));
        assert!(!is_expired(&old, now, Duration::from_secs(600)));
        // Unparseable → not expired (conservative).
        assert!(!is_expired("not-a-timestamp", now, Duration::ZERO));
    }

    #[test]
    fn parse_status_round_trips_known_and_defaults_failed() {
        for s in [
            RunStatus::Queued,
            RunStatus::Pending,
            RunStatus::Running,
            RunStatus::Completed,
            RunStatus::Failed,
            RunStatus::Cancelled,
        ] {
            assert_eq!(parse_status(s.as_str()), s);
        }
        assert_eq!(parse_status("garbage"), RunStatus::Failed);
    }

    #[test]
    fn body_round_trips() {
        let rec = RunRecord::queued(
            "r1".into(),
            Some("n".into()),
            Default::default(),
            Some("idem".into()),
            Utc::now(),
        );
        let encoded = encode_body(&rec).unwrap();
        let decoded = decode_body(&encoded).unwrap();
        assert_eq!(decoded.run_id, "r1");
        assert_eq!(decoded.idempotency_key.as_deref(), Some("idem"));
    }

    #[test]
    fn postgres_and_sqlite_statements_differ_only_in_placeholders() {
        let pg = Stmts::new(Dialect::Postgres);
        let lite = Stmts::new(Dialect::Sqlite);
        assert!(pg.upsert.contains("$1") && lite.upsert.contains('?'));
        assert!(pg.list.contains("$13") && lite.list.contains('?'));
        // Both target the same tables / conflict targets.
        assert!(pg.insert_idem.contains("ON CONFLICT (key) DO NOTHING"));
        assert!(lite.insert_idem.contains("ON CONFLICT (key) DO NOTHING"));
        assert!(pg.claim_one.contains("$3") && lite.claim_one.contains('?'));
        assert!(pg.heartbeat_instance.contains("faucet_serve_instances"));
        assert!(lite.heartbeat_instance.contains("faucet_serve_instances"));
    }
}
