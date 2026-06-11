//! Integration tests for event-driven triggers (#196). Drive the serve runner
//! in-process against an in-memory history backend.
#![cfg(all(feature = "triggers", feature = "source-csv", feature = "sink-jsonl"))]

use std::sync::Arc;
use std::time::Duration;

use faucet_cli::serve::config::{AuthMode, HistoryBackendSpec, ServeConfig};
use faucet_cli::serve::history::memory::MemoryHistory;
use faucet_cli::serve::history::{ListFilter, RunHistory, RunStatus};
use faucet_cli::serve::logs::LogHub;
use faucet_cli::serve::state::ServerState;
use faucet_cli::serve::triggers::compiled::CompiledTriggers;
use faucet_cli::serve::triggers::health::TriggersHandle;
use tokio_util::sync::CancellationToken;

fn test_config() -> ServeConfig {
    ServeConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        auth: AuthMode::None,
        max_concurrent_runs: 4,
        max_queued_runs: 16,
        default_config_path: None,
        history: HistoryBackendSpec::Memory,
        cors_origins: vec![],
        body_limit_bytes: 1_048_576,
        shutdown_grace: Duration::from_secs(5),
        retain_terminal_runs: Duration::from_secs(60),
        idempotency_retention: Duration::from_secs(60),
        lease_ttl: Duration::from_secs(30),
        probe_timeout: Duration::from_secs(10),
        env_file: None,
        no_env_file: false,
        log_level: "warn".into(),
        ui_enabled: false,
        cluster: faucet_cli::serve::cluster::ClusterConfig::disabled(),
        triggers_path: None,
    }
}

/// A trivial pipeline (CSV from a temp file → jsonl to a temp file) embedded
/// inline so the webhook test needs no external services. `append: true` so
/// multiple concurrent runs that write to the same path don't conflict.
fn inline_pipeline(csv_path: &str, out_path: &str) -> serde_json::Value {
    serde_json::json!({
        "version": 1,
        "pipeline": {
            "source": { "type": "csv", "config": { "path": csv_path } },
            "sink": { "type": "jsonl", "config": { "path": out_path, "append": true } }
        }
    })
}

fn build_state(triggers: &CompiledTriggers) -> ServerState {
    let history = Arc::new(MemoryHistory::new(Duration::from_secs(60))) as Arc<dyn RunHistory>;
    let handle = TriggersHandle::from_compiled(&triggers.triggers);
    ServerState::new(
        &test_config(),
        None,
        CancellationToken::new(),
        history,
        LogHub::new(),
        None,
        handle,
    )
}

/// Poll `list` until at least `want` terminal runs are present, then return.
async fn wait_for_runs(state: &ServerState, want: usize) {
    for _ in 0..200 {
        let page = state
            .history()
            .list(&ListFilter { limit: 1_000, ..Default::default() })
            .await
            .unwrap();
        let terminal = page
            .runs
            .iter()
            .filter(|r| {
                matches!(
                    r.status,
                    RunStatus::Completed | RunStatus::Failed | RunStatus::Cancelled
                )
            })
            .count();
        if terminal >= want {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let page = state
        .history()
        .list(&ListFilter { limit: 1_000, ..Default::default() })
        .await
        .unwrap();
    let statuses: Vec<_> = page
        .runs
        .iter()
        .map(|r| format!("{}:{}", r.run_id, r.status.as_str()))
        .collect();
    panic!(
        "timed out waiting for {want} terminal run(s); have {} total: {statuses:?}",
        page.runs.len()
    );
}

#[tokio::test]
async fn webhook_trigger_enqueues_exactly_one_run() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "a,b\n1,2\n").unwrap();
    let out = dir.path().join("out.jsonl");

    let inline = inline_pipeline(csv.to_str().unwrap(), out.to_str().unwrap());
    let file: faucet_cli::serve::triggers::spec::TriggersFile = serde_yaml::from_str(&format!(
        "version: 1\ntriggers:\n  - name: hook\n    type: webhook\n    dedupe_header: Idempotency-Key\n    config: {}\n",
        serde_json::to_string(&inline).unwrap()
    ))
    .unwrap();
    let compiled = CompiledTriggers::compile(file).unwrap();
    let state = build_state(&compiled);

    // Fire via the enqueue path directly.
    let event = faucet_cli::serve::triggers::context::TriggerEvent::Webhook {
        method: "POST".into(),
        body: "{}".into(),
        headers: Default::default(),
        query: Default::default(),
        idem: "evt-1".into(),
    };
    let now = chrono::Utc::now().to_rfc3339();
    let outcome =
        faucet_cli::serve::triggers::enqueue::fire(&state, &compiled.triggers[0], event, &now)
            .await;
    assert!(
        matches!(
            outcome,
            faucet_cli::serve::triggers::enqueue::FireOutcome::Enqueued(_)
        ),
        "expected Enqueued, got {outcome:?}"
    );

    // Wait for the run to finish, then assert exactly one terminal run exists.
    wait_for_runs(&state, 1).await;
    let page = state
        .history()
        .list(&ListFilter { limit: 1_000, ..Default::default() })
        .await
        .unwrap();
    assert_eq!(page.runs.len(), 1, "expected exactly 1 run, got {}", page.runs.len());

    // A second fire with the SAME idempotency key must NOT create a second run.
    let event2 = faucet_cli::serve::triggers::context::TriggerEvent::Webhook {
        method: "POST".into(),
        body: "{}".into(),
        headers: Default::default(),
        query: Default::default(),
        idem: "evt-1".into(),
    };
    let _ =
        faucet_cli::serve::triggers::enqueue::fire(&state, &compiled.triggers[0], event2, &now)
            .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let page2 = state
        .history()
        .list(&ListFilter { limit: 1_000, ..Default::default() })
        .await
        .unwrap();
    assert_eq!(
        page2.runs.len(),
        1,
        "idempotency must dedupe the replay — got {} runs",
        page2.runs.len()
    );
}

#[cfg(feature = "triggers-object-store")]
#[tokio::test]
async fn object_arrival_enqueues_one_run_per_object_and_dedupes() {
    use faucet_cli::serve::triggers::object_arrival::{Cursor, ListedObject, ObjectArrivalWatcher};
    use faucet_cli::serve::triggers::watcher::Watcher;
    use object_store::memory::InMemory;
    use object_store::path::Path as OPath;
    use object_store::{ObjectStore, ObjectStoreExt};

    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("in.csv");
    std::fs::write(&csv, "a\n1\n").unwrap();
    let out = dir.path().join("out.jsonl");
    // Inline pipeline: the run just needs to complete; we don't use
    // ${trigger.object_key} here — we're testing enqueue count not data flow.
    let inline = inline_pipeline(csv.to_str().unwrap(), out.to_str().unwrap());

    let file: faucet_cli::serve::triggers::spec::TriggersFile = serde_yaml::from_str(&format!(
        "version: 1\ntriggers:\n  - name: drop\n    type: object_arrival\n    store: {{ type: s3, bucket: b }}\n    config: {}\n",
        serde_json::to_string(&inline).unwrap()
    ))
    .unwrap();
    let compiled = CompiledTriggers::compile(file).unwrap();
    let state = build_state(&compiled);

    // InMemory object store with one object.
    let store = InMemory::new();
    store
        .put(
            &OPath::from("incoming/a.json"),
            b"{}".to_vec().into(),
        )
        .await
        .unwrap();
    let store: Arc<dyn ObjectStore> = Arc::new(store);

    let mut watcher = ObjectArrivalWatcher::new(
        Arc::new(compiled.triggers[0].clone()),
        store.clone(),
        "b".into(),
        None,
        faucet_cli::serve::triggers::spec::ArrivalMode::PerObject,
        Duration::from_secs(30),
        faucet_cli::serve::triggers::spec::StartAt::Beginning,
        chrono::Utc::now(),
    );

    // One poll → exactly one run enqueued.
    let fired = watcher.poll(&state).await.unwrap();
    assert!(fired, "expected first poll to fire");
    wait_for_runs(&state, 1).await;
    let page = state
        .history()
        .list(&ListFilter { limit: 1_000, ..Default::default() })
        .await
        .unwrap();
    assert_eq!(page.runs.len(), 1, "expected 1 run after first poll");

    // Second poll with same object → cursor committed → no new run.
    let fired2 = watcher.poll(&state).await.unwrap();
    assert!(!fired2, "expected second poll to be idle (cursor committed)");
    tokio::time::sleep(Duration::from_millis(150)).await;
    let page2 = state
        .history()
        .list(&ListFilter { limit: 1_000, ..Default::default() })
        .await
        .unwrap();
    assert_eq!(
        page2.runs.len(),
        1,
        "cursor dedup must prevent a second run"
    );

    // Ensure these public types are reachable (exercises re-exports).
    let _ = (
        Cursor::starting_beginning(),
        ListedObject {
            key: "x".into(),
            last_modified: chrono::Utc::now(),
            size: 0,
            etag: None,
        },
    );
}

#[tokio::test]
async fn queue_depth_fires_once_on_rising_edge() {
    // Only run when the queue_depth module is compiled (any backend feature).
    #[cfg(any(feature = "triggers-redis", feature = "triggers-kafka"))]
    {
        use async_trait::async_trait;
        use faucet_cli::serve::triggers::queue_depth::{DepthProbe, QueueDepthWatcher};
        use faucet_cli::serve::triggers::watcher::Watcher;
        use std::sync::atomic::{AtomicU64, Ordering};

        struct FakeProbe {
            readings: Arc<Vec<u64>>,
            idx: Arc<AtomicU64>,
        }

        #[async_trait]
        impl DepthProbe for FakeProbe {
            async fn depth(&self) -> Result<u64, String> {
                let i = self.idx.fetch_add(1, Ordering::SeqCst) as usize;
                Ok(*self.readings.get(i).unwrap_or(&0))
            }
            fn queue_label(&self) -> String {
                "jobs".into()
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let csv = dir.path().join("in.csv");
        std::fs::write(&csv, "a\n1\n").unwrap();
        let out = dir.path().join("out.jsonl");
        let inline = inline_pipeline(csv.to_str().unwrap(), out.to_str().unwrap());
        let file: faucet_cli::serve::triggers::spec::TriggersFile = serde_yaml::from_str(&format!(
            "version: 1\ntriggers:\n  - name: drain\n    type: queue_depth\n    threshold: 5\n    queue: {{ type: redis, url: \"redis://x\", key: jobs }}\n    config: {}\n",
            serde_json::to_string(&inline).unwrap()
        ))
        .unwrap();
        let compiled = CompiledTriggers::compile(file).unwrap();
        let state = build_state(&compiled);

        let probe = FakeProbe {
            readings: Arc::new(vec![0, 9, 9, 0, 7]),
            idx: Arc::new(AtomicU64::new(0)),
        };
        let mut w = QueueDepthWatcher::new(
            Arc::new(compiled.triggers[0].clone()),
            Box::new(probe),
            5,
            Duration::from_secs(30),
        );

        assert!(!w.poll(&state).await.unwrap(), "depth 0: no fire");
        assert!(w.poll(&state).await.unwrap(), "depth 9: rising edge fires");
        assert!(!w.poll(&state).await.unwrap(), "depth 9 again: suppressed");
        assert!(!w.poll(&state).await.unwrap(), "depth 0: re-arm, no fire");
        assert!(w.poll(&state).await.unwrap(), "depth 7: second rising edge fires");

        wait_for_runs(&state, 2).await;
        let page = state
            .history()
            .list(&ListFilter { limit: 1_000, ..Default::default() })
            .await
            .unwrap();
        assert_eq!(
            page.runs.len(),
            2,
            "expected 2 runs (one per rising edge), got {}",
            page.runs.len()
        );
    }
}
