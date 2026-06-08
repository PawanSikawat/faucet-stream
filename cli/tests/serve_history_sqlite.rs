//! SQLite run-history backend integration tests (Phase 5, #127). SQLite is
//! embedded, so these exercise the *shared* SQL machinery (`history::sql`) used
//! by the Postgres backend too — against a real database file, no server needed.
//! Requires the `serve-history-sqlite` feature.
#![cfg(feature = "serve-history-sqlite")]

use chrono::{Duration as ChronoDuration, Utc};
use faucet_cli::serve::history::sqlite::SqliteHistory;
use faucet_cli::serve::history::{
    Claim, DeleteOutcome, ListFilter, RunHistory, RunRecord, RunStatus,
};
use std::collections::BTreeMap;
use std::time::Duration;

async fn store(dir: &tempfile::TempDir, file: &str) -> SqliteHistory {
    store_with(dir, file, Duration::from_secs(3600), "test-instance").await
}

/// Build a backend with an explicit lease TTL + instance id, for the
/// instance-fencing tests (#146 H7).
async fn store_with(
    dir: &tempfile::TempDir,
    file: &str,
    lease_ttl: Duration,
    instance: &str,
) -> SqliteHistory {
    let path = dir.path().join(file);
    SqliteHistory::connect(
        &format!("sqlite:{}", path.display()),
        Duration::from_secs(3600),
        lease_ttl,
        instance.to_string(),
    )
    .await
    .expect("connect sqlite history")
}

fn rec(id: &str, status: RunStatus, submitted: chrono::DateTime<Utc>) -> RunRecord {
    let mut r = RunRecord::queued(id.into(), None, BTreeMap::new(), None, submitted);
    r.status = status;
    if status.is_terminal() {
        r.finished_at = Some(submitted);
    }
    r
}

#[tokio::test]
async fn upsert_get_and_missing() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "a.db").await;
    let mut r = rec("run-1", RunStatus::Running, Utc::now());
    r.name = Some("nightly".into());
    r.records_written = 7;
    h.upsert(&r).await.unwrap();

    let got = h.get("run-1").await.unwrap().expect("present");
    assert_eq!(got.run_id, "run-1");
    assert_eq!(got.status, RunStatus::Running);
    assert_eq!(got.name.as_deref(), Some("nightly"));
    assert_eq!(got.records_written, 7);
    assert!(h.get("missing").await.unwrap().is_none());
}

#[tokio::test]
async fn idempotency_fresh_replay_conflict_at_sql_layer() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "idem.db").await;
    let w = Duration::from_secs(3600);
    assert_eq!(
        h.claim_idempotency("k", "fp1", "run1", w).await.unwrap(),
        Claim::Fresh
    );
    assert_eq!(
        h.claim_idempotency("k", "fp1", "run2", w).await.unwrap(),
        Claim::Replay("run1".into())
    );
    assert_eq!(
        h.claim_idempotency("k", "fp2", "run3", w).await.unwrap(),
        Claim::Conflict
    );
    // Expired prior claim (zero window) is re-claimable.
    assert_eq!(
        h.claim_idempotency("k2", "fpa", "r1", Duration::ZERO)
            .await
            .unwrap(),
        Claim::Fresh
    );
    assert_eq!(
        h.claim_idempotency("k2", "fpb", "r2", Duration::ZERO)
            .await
            .unwrap(),
        Claim::Fresh
    );
}

#[tokio::test]
async fn list_orders_desc_filters_and_paginates() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "list.db").await;
    let t0 = Utc::now();
    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        h.upsert(&rec(
            id,
            RunStatus::Completed,
            t0 + ChronoDuration::seconds(i as i64),
        ))
        .await
        .unwrap();
    }
    // Newest first, page size 2 → [c, b], cursor = b.
    let page = h
        .list(&ListFilter {
            limit: 2,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        page.runs
            .iter()
            .map(|r| r.run_id.clone())
            .collect::<Vec<_>>(),
        vec!["c", "b"]
    );
    assert_eq!(page.next_cursor.as_deref(), Some("b"));
    // Next page from the cursor → [a].
    let page2 = h
        .list(&ListFilter {
            limit: 2,
            cursor: Some("b".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(
        page2
            .runs
            .iter()
            .map(|r| r.run_id.clone())
            .collect::<Vec<_>>(),
        vec!["a"]
    );
    assert!(page2.next_cursor.is_none());

    // Status filter.
    h.upsert(&rec(
        "x",
        RunStatus::Failed,
        t0 + ChronoDuration::seconds(10),
    ))
    .await
    .unwrap();
    let failed = h
        .list(&ListFilter {
            status: Some(RunStatus::Failed),
            limit: 50,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(failed.runs.len(), 1);
    assert_eq!(failed.runs[0].run_id, "x");
}

#[tokio::test]
async fn delete_respects_terminal_state() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "del.db").await;
    h.upsert(&rec("run", RunStatus::Running, Utc::now()))
        .await
        .unwrap();
    assert_eq!(h.delete("run").await.unwrap(), DeleteOutcome::StillRunning);
    assert_eq!(h.delete("nope").await.unwrap(), DeleteOutcome::NotFound);
    h.upsert(&rec("run", RunStatus::Completed, Utc::now()))
        .await
        .unwrap();
    assert_eq!(h.delete("run").await.unwrap(), DeleteOutcome::Deleted);
    assert!(h.get("run").await.unwrap().is_none());
}

#[tokio::test]
async fn recover_orphans_marks_expired_lease_non_terminal_failed() {
    let dir = tempfile::tempdir().unwrap();
    {
        // A zero TTL makes the owner's lease expire immediately, so the orphan
        // is recoverable as soon as the owning "process" goes away.
        let h = store_with(&dir, "recover.db", Duration::ZERO, "inst-a").await;
        h.upsert(&rec("orphan", RunStatus::Running, Utc::now()))
            .await
            .unwrap();
        h.upsert(&rec("done", RunStatus::Completed, Utc::now()))
            .await
            .unwrap();
    } // drop the first "process"

    // A new instance reconnects (simulating a restart) and recovers.
    let h2 = store_with(&dir, "recover.db", Duration::from_secs(30), "inst-b").await;
    let recovered = h2.recover_orphans().await.unwrap();
    assert_eq!(
        recovered, 1,
        "only the non-terminal expired-lease run is recovered"
    );
    let orphan = h2.get("orphan").await.unwrap().unwrap();
    assert_eq!(orphan.status, RunStatus::Failed);
    assert!(orphan.error.as_deref().unwrap().contains("lease expired"));
    // The already-terminal run is untouched.
    assert_eq!(
        h2.get("done").await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    // Idempotent: a second pass finds nothing (the orphan is now terminal).
    assert_eq!(h2.recover_orphans().await.unwrap(), 0);
}

/// The H7 fix: a healthy peer's in-flight run carries a live (future) lease, so
/// a *different* instance's `recover_orphans` must NOT mark it failed.
#[tokio::test]
async fn recover_orphans_skips_live_lease_of_another_instance() {
    let dir = tempfile::tempdir().unwrap();
    // Instance A upserts a Running run with a long lease (it's alive).
    let a = store_with(&dir, "fence.db", Duration::from_secs(3600), "inst-a").await;
    a.upsert(&rec("a-run", RunStatus::Running, Utc::now()))
        .await
        .unwrap();

    // Instance B starts against the same DB and runs recovery. A's run has a
    // live lease, so it must be left alone.
    let b = store_with(&dir, "fence.db", Duration::from_secs(3600), "inst-b").await;
    assert_eq!(
        b.recover_orphans().await.unwrap(),
        0,
        "a live peer's run must not be recovered"
    );
    assert_eq!(
        b.get("a-run").await.unwrap().unwrap().status,
        RunStatus::Running,
        "the peer's run must still be Running"
    );
}

/// `renew_leases` is scoped to the calling instance's own non-terminal runs.
#[tokio::test]
async fn renew_leases_is_owner_and_status_scoped() {
    let dir = tempfile::tempdir().unwrap();
    let a = store_with(&dir, "renew.db", Duration::from_secs(3600), "inst-a").await;
    a.upsert(&rec("a-running", RunStatus::Running, Utc::now()))
        .await
        .unwrap();
    a.upsert(&rec("a-done", RunStatus::Completed, Utc::now()))
        .await
        .unwrap();

    // A renews only its own non-terminal run (the completed one is excluded).
    assert_eq!(a.renew_leases().await.unwrap(), 1);

    // A different instance owns nothing here, so it renews nothing.
    let b = store_with(&dir, "renew.db", Duration::from_secs(3600), "inst-b").await;
    assert_eq!(b.renew_leases().await.unwrap(), 0);
}

/// A heartbeat renews the lease, so a previously-recoverable run becomes
/// protected from a peer's recovery scan.
#[tokio::test]
async fn renew_leases_protects_a_run_from_recovery() {
    let dir = tempfile::tempdir().unwrap();
    // TTL 0 → the run's lease is born expired (recoverable).
    let a = store_with(&dir, "protect.db", Duration::ZERO, "inst-a").await;
    a.upsert(&rec("a-run", RunStatus::Running, Utc::now()))
        .await
        .unwrap();

    // A peer would recover it right now (expired lease)...
    let b = store_with(&dir, "protect.db", Duration::from_secs(3600), "inst-b").await;
    // ...but first A heartbeats with a fresh, long lease.
    let a_live = store_with(&dir, "protect.db", Duration::from_secs(3600), "inst-a").await;
    assert_eq!(a_live.renew_leases().await.unwrap(), 1);

    assert_eq!(
        b.recover_orphans().await.unwrap(),
        0,
        "the heartbeat extended the lease, so the run must no longer be an orphan"
    );
    assert_eq!(
        b.get("a-run").await.unwrap().unwrap().status,
        RunStatus::Running
    );
}

/// End-to-end: boot `faucet serve --history sqlite:…`, submit a run over HTTP,
/// and confirm it is persisted through the SQLite-backed history (GET + list).
/// Proves the `history::connect` → `FallbackHistory` → handler path is wired.
#[tokio::test(flavor = "multi_thread")]
async fn server_with_sqlite_history_persists_runs() {
    use faucet_cli::cli::ServeArgs;
    use faucet_cli::serve::ServeConfig;

    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("serve.db");
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let args = ServeArgs {
        listen: format!("127.0.0.1:{port}"),
        auth_token: None,
        no_auth: true,
        max_concurrent_runs: Some(2),
        max_queued_runs: Some(8),
        default_config: None,
        history: Some(format!("sqlite:{}", db.display())),
        cors_origin: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace_secs: 5,
        retain_terminal_runs_secs: 604_800,
        idempotency_retention_secs: 86_400,
        lease_ttl_secs: 30,
        probe_timeout_secs: 5,
        env_file: None,
        no_env_file: true,
        no_ui: false,
    };
    let mut config = ServeConfig::from_args(args).unwrap();
    config.log_level = "warn".into();
    tokio::spawn(async move {
        let _ = faucet_cli::serve::run_server(config).await;
    });

    let client = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{port}");
    // Wait for liveness.
    let mut up = false;
    for _ in 0..200 {
        if client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
        {
            up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(up, "server did not come up");
    // /readyz must be 200 — the SQLite backend connected (not degraded).
    assert_eq!(
        client
            .get(format!("{base}/readyz"))
            .send()
            .await
            .unwrap()
            .status(),
        200,
        "readyz must be 200 with a healthy sqlite backend"
    );

    // Submit a trivial run (connectors may be absent in this build → it ends
    // 'failed', but the record is still persisted by the history backend).
    let body = serde_json::json!({
        "config": "version: 1\npipeline:\n  source: { type: csv, config: { path: in.csv } }\n  sink: { type: jsonl, config: { path: out.jsonl } }\n"
    });
    let submit: serde_json::Value = client
        .post(format!("{base}/v1/runs"))
        .json(&body)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let run_id = submit["run_id"].as_str().unwrap().to_string();

    // Poll until terminal.
    let mut terminal = false;
    for _ in 0..400 {
        let rec: serde_json::Value = client
            .get(format!("{base}/v1/runs/{run_id}"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if matches!(
            rec["status"].as_str().unwrap_or(""),
            "completed" | "failed" | "cancelled"
        ) {
            terminal = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(terminal, "run never reached a terminal state");

    // The run is retrievable and appears in the list — i.e. it was persisted via
    // the SQLite backend, then read back through it.
    let listed: serde_json::Value = client
        .get(format!("{base}/v1/runs"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        listed["runs"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["run_id"] == run_id),
        "submitted run must be listed from the sqlite-backed history"
    );

    // The row really is in the database file.
    let h = store(&dir, "serve.db").await;
    assert!(
        h.get(&run_id).await.unwrap().is_some(),
        "run must be physically present in the sqlite file"
    );
}

#[tokio::test]
async fn purge_drops_expired_terminal_runs() {
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "purge.db").await;
    h.upsert(&rec(
        "old",
        RunStatus::Completed,
        Utc::now() - ChronoDuration::seconds(120),
    ))
    .await
    .unwrap();
    h.upsert(&rec("live", RunStatus::Running, Utc::now()))
        .await
        .unwrap();
    // retain_for = 0 → every terminal record is expired; running is kept.
    let removed = h.purge_expired(Duration::ZERO).await.unwrap();
    assert_eq!(removed, 1);
    assert!(h.get("old").await.unwrap().is_none());
    assert!(h.get("live").await.unwrap().is_some());
}

#[tokio::test]
async fn delete_also_removes_matching_idem_claim_at_sql_layer() {
    // M8 (#146): deleting a run drops its idempotency claim, so a replay of the
    // key starts a fresh run instead of 404-ing on the missing record until the
    // claim self-expires. Exercises the shared SQL `delete_idem_by_run`.
    let dir = tempfile::tempdir().unwrap();
    let h = store(&dir, "delete_idem.db").await;
    let w = Duration::from_secs(3600);
    assert_eq!(
        h.claim_idempotency("k", "fp", "r1", w).await.unwrap(),
        Claim::Fresh
    );
    let mut r = RunRecord::queued(
        "r1".into(),
        None,
        BTreeMap::new(),
        Some("k".into()),
        Utc::now(),
    );
    r.status = RunStatus::Completed;
    r.finished_at = Some(Utc::now());
    h.upsert(&r).await.unwrap();

    assert_eq!(h.delete("r1").await.unwrap(), DeleteOutcome::Deleted);
    // The claim is gone → a fresh run, not a replay of the deleted one.
    assert_eq!(
        h.claim_idempotency("k", "fp", "r2", w).await.unwrap(),
        Claim::Fresh
    );
}
