//! SQLite-backed run history (`serve-history-sqlite`, Phase 5 of #127).
//! Connection setup only — the schema, statements, and `RunHistory` impl are
//! shared with Postgres via [`impl_sql_history!`](super::sql).

use super::HistoryError;
use super::sql::{DDL, Dialect, Stmts, impl_sql_history};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use std::str::FromStr;
use std::time::Duration;

impl_sql_history!(SqliteHistory, sqlx::SqlitePool);

impl SqliteHistory {
    /// Connect (creating the database file if missing), create the schema if
    /// absent, and return the backend. WAL + a busy timeout let the connection
    /// pool tolerate concurrent run writes. `lease_ttl` and `instance_id` drive
    /// instance-fenced orphan recovery (#146 H7).
    pub async fn connect(
        url: &str,
        idem_retention: Duration,
        lease_ttl: Duration,
        instance_id: String,
    ) -> Result<Self, HistoryError> {
        let opts = SqliteConnectOptions::from_str(url)
            .map_err(|e| HistoryError::Backend(format!("invalid sqlite url '{url}': {e}")))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .map_err(|e| HistoryError::Backend(format!("SQLite connection failed: {e}")))?;
        for stmt in DDL {
            sqlx::query(stmt)
                .execute(&pool)
                .await
                .map_err(|e| HistoryError::Backend(format!("creating run-history schema: {e}")))?;
        }
        Ok(Self::from_parts(
            pool,
            idem_retention,
            lease_ttl,
            instance_id,
            Stmts::new(Dialect::Sqlite),
        ))
    }
}

#[cfg(test)]
mod shard_tests {
    use super::*;
    use crate::serve::history::{RunHistory, RunRecord, RunStatus, ShardInsert};
    use std::collections::BTreeMap;

    fn shard(id: &str, size: u64) -> ShardInsert {
        ShardInsert {
            shard_id: id.into(),
            descriptor: serde_json::json!({ "i": id }),
            size_estimate: Some(size),
        }
    }

    async fn backend(url: &str, instance: &str, ttl: Duration) -> SqliteHistory {
        SqliteHistory::connect(url, Duration::from_secs(300), ttl, instance.into())
            .await
            .expect("connect")
    }

    async fn seed_run(h: &SqliteHistory, run_id: &str) {
        let mut rec = RunRecord::queued(
            run_id.into(),
            None,
            BTreeMap::new(),
            None,
            chrono::Utc::now(),
        );
        rec.status = RunStatus::Pending;
        rec.config_body = Some("version: 1".into());
        h.upsert(&rec).await.expect("seed run");
    }

    fn url_in(dir: &std::path::Path) -> String {
        format!("sqlite://{}/h.db", dir.display())
    }

    #[tokio::test]
    async fn insert_shards_is_idempotent_and_progress_counts() {
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        seed_run(&h, "run1").await;
        let shards = [shard("0", 10), shard("1", 20), shard("2", 5)];

        assert_eq!(h.insert_shards("run1", &shards).await.unwrap(), 3);
        assert_eq!(
            h.insert_shards("run1", &shards).await.unwrap(),
            0,
            "re-insert is a no-op (ON CONFLICT DO NOTHING)"
        );

        let p = h.shard_progress("run1").await.unwrap();
        assert_eq!(p.total, 3);
        assert_eq!(p.pending, 3);
        assert!(!p.all_terminal());
    }

    #[tokio::test]
    async fn claim_shards_largest_first_marks_running_and_is_exclusive() {
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        seed_run(&h, "run1").await;
        h.insert_shards("run1", &[shard("0", 10), shard("1", 20), shard("2", 5)])
            .await
            .unwrap();

        let claimed = h.claim_shards(10).await.unwrap();
        assert_eq!(claimed.len(), 3);
        // Largest estimated size first.
        assert_eq!(claimed[0].shard_id, "1");
        assert_eq!(claimed[1].shard_id, "0");
        assert_eq!(claimed[2].shard_id, "2");
        // Parent run body is carried for the worker to rebuild the source.
        assert_eq!(claimed[0].run.config_body.as_deref(), Some("version: 1"));
        assert_eq!(claimed[0].descriptor, serde_json::json!({ "i": "1" }));

        let p = h.shard_progress("run1").await.unwrap();
        assert_eq!(p.running, 3);

        // Everything is claimed → a second claim returns nothing.
        assert!(h.claim_shards(10).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn finalize_shard_is_owner_fenced() {
        let dir = tempfile::tempdir().unwrap();
        let url = url_in(dir.path());
        let a = backend(&url, "inst-a", Duration::from_secs(60)).await;
        let b = backend(&url, "inst-b", Duration::from_secs(60)).await;
        seed_run(&a, "run1").await;
        a.insert_shards("run1", &[shard("0", 1)]).await.unwrap();

        // A claims the only shard.
        let claimed = a.claim_shards(10).await.unwrap();
        assert_eq!(claimed.len(), 1);

        // B does not own it → cannot finalize.
        assert!(
            !b.finalize_shard("run1", "0", true).await.unwrap(),
            "a non-owner must not finalize the shard"
        );
        // A owns it → finalize succeeds.
        assert!(a.finalize_shard("run1", "0", true).await.unwrap());

        let p = a.shard_progress("run1").await.unwrap();
        assert_eq!(p.completed, 1);
        assert!(p.all_terminal());
    }

    #[tokio::test]
    async fn reclaim_shards_requeues_expired_then_poisons() {
        let dir = tempfile::tempdir().unwrap();
        let url = url_in(dir.path());
        // lease_ttl = 0 → a claimed shard's lease is already in the past on the
        // next call, so it is reclaimable deterministically.
        let h = backend(&url, "inst-a", Duration::ZERO).await;
        seed_run(&h, "run1").await;
        h.insert_shards("run1", &[shard("0", 1)]).await.unwrap();
        h.claim_shards(10).await.unwrap();

        // First reclaim: attempt 0 < 2 → requeued back to pending.
        let r1 = h.reclaim_shards(2).await.unwrap();
        assert_eq!(r1.requeued, 1);
        assert_eq!(r1.failed, 0);
        assert_eq!(h.shard_progress("run1").await.unwrap().pending, 1);

        // Re-claim and reclaim until the attempt cap poisons it.
        h.claim_shards(10).await.unwrap();
        let r2 = h.reclaim_shards(2).await.unwrap();
        assert_eq!(r2.requeued, 1, "attempt 1 < 2 → still requeued");
        h.claim_shards(10).await.unwrap();
        let r3 = h.reclaim_shards(2).await.unwrap();
        assert_eq!(r3.failed, 1, "attempt 2 >= 2 → poisoned (failed)");
        assert_eq!(h.shard_progress("run1").await.unwrap().failed, 1);
    }

    #[tokio::test]
    async fn delete_run_removes_its_shard_rows() {
        // F25: deleting a terminal run must also drop its shard rows so they
        // don't leak unboundedly on the durable store.
        use crate::serve::history::DeleteOutcome;
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        seed_run(&h, "run1").await;
        h.insert_shards("run1", &[shard("0", 1), shard("1", 1)])
            .await
            .unwrap();
        // Make the run terminal so it is deletable.
        let mut rec = h.get("run1").await.unwrap().unwrap();
        rec.status = RunStatus::Completed;
        rec.finished_at = Some(chrono::Utc::now());
        h.upsert(&rec).await.unwrap();

        assert_eq!(h.shard_progress("run1").await.unwrap().total, 2);
        assert_eq!(h.delete("run1").await.unwrap(), DeleteOutcome::Deleted);
        assert_eq!(
            h.shard_progress("run1").await.unwrap().total,
            0,
            "shard rows must be removed when the run is deleted"
        );
    }

    #[tokio::test]
    async fn purge_expired_removes_orphaned_shard_rows() {
        // F25: purging expired terminal runs must reclaim their shard rows too.
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        seed_run(&h, "run1").await;
        h.insert_shards("run1", &[shard("0", 1)]).await.unwrap();
        let mut rec = h.get("run1").await.unwrap().unwrap();
        rec.status = RunStatus::Completed;
        rec.finished_at = Some(chrono::Utc::now());
        h.upsert(&rec).await.unwrap();

        // retain_for = 0 → the terminal run is immediately purgeable.
        let removed = h.purge_expired(Duration::ZERO).await.unwrap();
        assert_eq!(removed, 1, "the terminal run is purged");
        assert_eq!(
            h.shard_progress("run1").await.unwrap().total,
            0,
            "orphaned shard rows must be purged with their parent run"
        );
    }

    #[tokio::test]
    async fn audit_record_list_filter_and_purge() {
        use crate::serve::history::{AuditEntry, AuditFilter};
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        let now = chrono::Utc::now();
        let entry =
            |id: &str, principal: &str, action: &str, result: &str, secs_ago: i64| AuditEntry {
                id: id.into(),
                timestamp: now - chrono::Duration::seconds(secs_ago),
                principal: principal.into(),
                role: "admin".into(),
                action: action.into(),
                run_id: Some(format!("r-{id}")),
                config_fingerprint: Some("fp".into()),
                source_ip: Some("127.0.0.1".into()),
                result: result.into(),
            };
        h.record_audit(&entry("1", "alice", "run.submit", "ok", 3))
            .await
            .unwrap();
        h.record_audit(&entry("2", "bob", "run.submit", "denied", 2))
            .await
            .unwrap();
        h.record_audit(&entry("3", "alice", "run.cancel", "ok", 1))
            .await
            .unwrap();

        // Newest first.
        let all = h
            .list_audit(&AuditFilter {
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, "3");
        assert_eq!(all[0].run_id.as_deref(), Some("r-3"));
        assert_eq!(all[0].source_ip.as_deref(), Some("127.0.0.1"));

        // Filters.
        let alice = h
            .list_audit(&AuditFilter {
                principal: Some("alice".into()),
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(alice.len(), 2);
        let submits = h
            .list_audit(&AuditFilter {
                action: Some("run.submit".into()),
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(submits.len(), 2);

        // purge_expired(0) drops all audit rows.
        h.purge_expired(Duration::ZERO).await.unwrap();
        assert!(
            h.list_audit(&AuditFilter {
                limit: 50,
                ..Default::default()
            })
            .await
            .unwrap()
            .is_empty()
        );
    }

    #[tokio::test]
    async fn release_idempotency_drops_the_claim() {
        // F21: releasing a claim lets a replay of the key start fresh instead of
        // 404-ing for the whole retention window.
        use crate::serve::history::Claim;
        let dir = tempfile::tempdir().unwrap();
        let h = backend(&url_in(dir.path()), "a", Duration::from_secs(60)).await;
        let w = Duration::from_secs(3600);
        assert!(matches!(
            h.claim_idempotency("k", "fp1", "run1", w).await.unwrap(),
            Claim::Fresh
        ));
        // Without release, a different fingerprint on the same key is a Conflict.
        h.release_idempotency("run1").await.unwrap();
        // After release the key is free: a fresh claim (even a different
        // fingerprint / run) succeeds rather than replaying/conflicting.
        assert!(matches!(
            h.claim_idempotency("k", "fp2", "run2", w).await.unwrap(),
            Claim::Fresh
        ));
    }
}
