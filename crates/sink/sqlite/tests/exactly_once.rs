//! Acceptance test (#188): a crash between sink-write and bookmark-persist must
//! produce ZERO duplicate rows on resume for the idempotent SQLite sink.

use async_trait::async_trait;
use faucet_core::pipeline::{StreamPage, run_stream};
use faucet_core::state::{MemoryStateStore, StateStore};
use faucet_core::{DeliveryMode, FaucetError, RunStreamOptions, Source, Value};
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::json;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;

/// Deterministic CDC-like source: replays the same pages, each with a bookmark.
struct ReplaySource {
    pages: Vec<(Vec<Value>, Value)>,
}
#[async_trait]
impl Source for ReplaySource {
    async fn fetch_with_context(
        &self,
        _c: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Ok(self.pages.iter().flat_map(|(r, _)| r.clone()).collect())
    }
    fn supports_exactly_once(&self) -> bool {
        true
    }
}

fn pages(src: &ReplaySource) -> Vec<Result<StreamPage, FaucetError>> {
    src.pages
        .iter()
        .map(|(r, b)| {
            Ok(StreamPage {
                records: r.clone(),
                bookmark: Some(b.clone()),
            })
        })
        .collect()
}

async fn count_id(sink_url: &str, id: i64) -> i64 {
    let pool = sqlx::SqlitePool::connect(sink_url).await.unwrap();
    let row = sqlx::query("SELECT COUNT(*) AS n FROM events WHERE json_extract(data, '$.id') = ?")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    row.get::<i64, _>("n")
}

#[tokio::test]
async fn crash_between_write_and_bookmark_yields_no_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("eo.db");
    let url = format!("sqlite://{}", db.display());

    {
        let pool = sqlx::SqlitePool::connect(&format!("{url}?mode=rwc"))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE events (data TEXT)")
            .execute(&pool)
            .await
            .unwrap();
    }

    let cfg = SqliteSinkConfig {
        column_mapping: SqliteColumnMapping::Json {
            column: "data".into(),
        },
        ..SqliteSinkConfig::new(&url, "events")
    };
    let sink = SqliteSink::new(cfg).await.unwrap();

    let source = ReplaySource {
        pages: vec![
            (vec![json!({"id": 1})], json!("b1")),
            (vec![json!({"id": 2})], json!("b2")),
        ],
    };

    // Run 1: commit only page 1, then simulate a crash (state never persisted).
    struct DroppingStore;
    #[async_trait]
    impl StateStore for DroppingStore {
        async fn get(&self, _k: &str) -> Result<Option<Value>, FaucetError> {
            Ok(None)
        }
        async fn put(&self, _k: &str, _v: &Value) -> Result<(), FaucetError> {
            Ok(())
        }
        async fn delete(&self, _k: &str) -> Result<(), FaucetError> {
            Ok(())
        }
    }
    let opts1 = RunStreamOptions::new()
        .with_state(Arc::new(DroppingStore), "events::r1")
        .with_delivery(DeliveryMode::ExactlyOnce);
    let first_page: Vec<Result<StreamPage, FaucetError>> = vec![Ok(StreamPage {
        records: vec![json!({"id": 1})],
        bookmark: Some(json!("b1")),
    })];
    run_stream(futures::stream::iter(first_page), &sink, opts1)
        .await
        .unwrap();
    assert_eq!(count_id(&format!("{url}?mode=rwc"), 1).await, 1);

    // Run 2 (resume): fresh state, full replay. Page 1 must be skipped.
    let store: Arc<dyn StateStore> = Arc::new(MemoryStateStore::new());
    let opts2 = RunStreamOptions::new()
        .with_state(store, "events::r1")
        .with_delivery(DeliveryMode::ExactlyOnce);
    run_stream(futures::stream::iter(pages(&source)), &sink, opts2)
        .await
        .unwrap();

    assert_eq!(
        count_id(&format!("{url}?mode=rwc"), 1).await,
        1,
        "id=1 must NOT be duplicated"
    );
    assert_eq!(
        count_id(&format!("{url}?mode=rwc"), 2).await,
        1,
        "id=2 written once"
    );
}
