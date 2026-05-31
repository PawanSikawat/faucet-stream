//! Shared SQL run-history machinery for the Postgres and SQLite backends
//! (Phase 5, #127). Both backends are identical except for the connection setup
//! and the placeholder dialect (`$n` vs `?`), so the schema, prepared-statement
//! text, pure helpers, and the entire `RunHistory` impl live here once and are
//! instantiated for each concrete `sqlx` pool via [`impl_sql_history!`].
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
    "CREATE TABLE IF NOT EXISTS faucet_serve_runs (\
        run_id TEXT PRIMARY KEY,\
        name TEXT,\
        status TEXT NOT NULL,\
        submitted_at TEXT NOT NULL,\
        finished_at TEXT,\
        idempotency_key TEXT,\
        body TEXT NOT NULL)",
    "CREATE INDEX IF NOT EXISTS faucet_serve_runs_submitted_idx \
        ON faucet_serve_runs (submitted_at)",
    "CREATE TABLE IF NOT EXISTS faucet_serve_idem (\
        key TEXT PRIMARY KEY,\
        run_id TEXT NOT NULL,\
        fingerprint TEXT NOT NULL,\
        claimed_at TEXT NOT NULL)",
];

/// SQL placeholder dialect.
#[derive(Clone, Copy, Debug)]
pub enum Dialect {
    Postgres,
    Sqlite,
}

/// Prepared-statement text for a backend, built once per dialect at connect time.
pub struct Stmts {
    pub upsert: String,
    pub select_body: String,
    pub select_status: String,
    pub select_submitted: String,
    pub delete: String,
    pub list: String,
    pub purge_runs: String,
    pub purge_idem: String,
    pub select_orphans: String,
    pub insert_idem: String,
    pub select_idem: String,
    pub takeover_idem: String,
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
                (run_id,name,status,submitted_at,finished_at,idempotency_key,body) \
                VALUES ($1,$2,$3,$4,$5,$6,$7) \
                ON CONFLICT (run_id) DO UPDATE SET \
                name=excluded.name,status=excluded.status,submitted_at=excluded.submitted_at,\
                finished_at=excluded.finished_at,idempotency_key=excluded.idempotency_key,\
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
                WHERE status IN ('queued','running')"
                .into(),
            insert_idem: "INSERT INTO faucet_serve_idem (key,run_id,fingerprint,claimed_at) \
                VALUES ($1,$2,$3,$4) ON CONFLICT (key) DO NOTHING"
                .into(),
            select_idem: "SELECT run_id,fingerprint,claimed_at FROM faucet_serve_idem WHERE key=$1"
                .into(),
            takeover_idem: "UPDATE faucet_serve_idem \
                SET run_id=$1,fingerprint=$2,claimed_at=$3 WHERE key=$4 AND claimed_at=$5"
                .into(),
        }
    }

    fn sqlite() -> Self {
        Self {
            upsert: "INSERT INTO faucet_serve_runs \
                (run_id,name,status,submitted_at,finished_at,idempotency_key,body) \
                VALUES (?,?,?,?,?,?,?) \
                ON CONFLICT (run_id) DO UPDATE SET \
                name=excluded.name,status=excluded.status,submitted_at=excluded.submitted_at,\
                finished_at=excluded.finished_at,idempotency_key=excluded.idempotency_key,\
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
                WHERE status IN ('queued','running')"
                .into(),
            insert_idem: "INSERT INTO faucet_serve_idem (key,run_id,fingerprint,claimed_at) \
                VALUES (?,?,?,?) ON CONFLICT (key) DO NOTHING"
                .into(),
            select_idem: "SELECT run_id,fingerprint,claimed_at FROM faucet_serve_idem WHERE key=?"
                .into(),
            takeover_idem: "UPDATE faucet_serve_idem \
                SET run_id=?,fingerprint=?,claimed_at=? WHERE key=? AND claimed_at=?"
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
        "running" => RunStatus::Running,
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
            stmts: $crate::serve::history::sql::Stmts,
        }

        impl $name {
            /// Assemble from an already-connected pool (used by `connect`).
            pub fn from_parts(
                pool: $pool,
                idem_retention: std::time::Duration,
                stmts: $crate::serve::history::sql::Stmts,
            ) -> Self {
                Self {
                    pool,
                    idem_retention,
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
                sqlx::query(&self.stmts.upsert)
                    .bind(&rec.run_id)
                    .bind(rec.name.as_deref())
                    .bind(rec.status.as_str())
                    .bind(&submitted)
                    .bind(finished.as_deref())
                    .bind(rec.idempotency_key.as_deref())
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
                        Ok(DeleteOutcome::Deleted)
                    }
                }
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
                Ok(removed)
            }

            async fn recover_orphans(&self) -> Result<usize, $crate::serve::history::HistoryError> {
                use sqlx::Row as _;
                use $crate::serve::history::HistoryError;
                use $crate::serve::history::RunStatus;
                use $crate::serve::history::sql;
                let backend = |e: sqlx::Error| HistoryError::Backend(e.to_string());
                let rows = sqlx::query(&self.stmts.select_orphans)
                    .fetch_all(&self.pool)
                    .await
                    .map_err(backend)?;
                let now = chrono::Utc::now();
                let mut count = 0usize;
                for r in &rows {
                    let body: String = r.try_get("body").map_err(backend)?;
                    let mut rec = sql::decode_body(&body)?;
                    rec.status = RunStatus::Failed;
                    rec.finished_at = Some(now);
                    rec.error = Some("server restarted before the run finished".into());
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
    }
}
