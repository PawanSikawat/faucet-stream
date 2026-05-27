//! Integration tests for the SQLite sink's `batch_size` re-chunking
//! behaviour.
//!
//! The sink is exercised end-to-end against a tempfile-backed SQLite
//! database (no Docker required). Tests assert on the final row count after
//! `write_batch` to verify that re-chunking, the `batch_size = 0` sentinel,
//! and undersized pages all round-trip the correct data.

use faucet_core::Sink;
use faucet_sink_sqlite::{SqliteColumnMapping, SqliteSink, SqliteSinkConfig};
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::sqlite::SqlitePoolOptions;
use tempfile::TempDir;

/// Spin up a fresh tempfile sqlite db with a `CREATE TABLE` statement and
/// return the path + url-style database URL.
async fn fresh_db(create_sql: &str) -> (TempDir, String) {
    let dir = TempDir::new().expect("tempdir");
    let path = dir.path().join("test.db");
    // SQLx requires an explicit `?mode=rwc` to create the file the first
    // time; once created, the file exists for the duration of the test.
    let url = format!("sqlite://{}?mode=rwc", path.display());
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");
    sqlx::query(create_sql)
        .execute(&pool)
        .await
        .expect("create table");
    pool.close().await;
    (dir, url)
}

async fn count_rows(url: &str, table: &str) -> i64 {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(url)
        .await
        .expect("connect");
    let row = sqlx::query(&format!("SELECT COUNT(*) AS n FROM {table}"))
        .fetch_one(&pool)
        .await
        .expect("count");
    let n: i64 = row.get("n");
    pool.close().await;
    n
}

#[tokio::test]
async fn json_mode_rechunks_large_page_into_multiple_inserts() {
    // 1500 records with batch_size=500 should produce 3 multi-row INSERT
    // transactions and end up with 1500 rows in the destination.
    let (_dir, url) = fresh_db("CREATE TABLE events (data TEXT NOT NULL)").await;

    let config = SqliteSinkConfig::new(&url, "events")
        .column_mapping(SqliteColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(500);
    let sink = SqliteSink::new(config).await.unwrap();

    let records: Vec<Value> = (0..1_500).map(|i| json!({"i": i})).collect();
    let n = sink.write_batch(&records).await.unwrap();

    assert_eq!(n, 1_500);
    assert_eq!(count_rows(&url, "events").await, 1_500);
}

#[tokio::test]
async fn json_mode_batch_size_zero_writes_single_transaction() {
    // batch_size=0 is the "no batching" sentinel: 2500 records get written
    // in a single transaction. All rows must land.
    let (_dir, url) = fresh_db("CREATE TABLE events (data TEXT NOT NULL)").await;

    let config = SqliteSinkConfig::new(&url, "events")
        .column_mapping(SqliteColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(0);
    let sink = SqliteSink::new(config).await.unwrap();

    let records: Vec<Value> = (0..2_500).map(|i| json!({"i": i})).collect();
    let n = sink.write_batch(&records).await.unwrap();

    assert_eq!(n, 2_500);
    assert_eq!(count_rows(&url, "events").await, 2_500);
}

#[tokio::test]
async fn json_mode_undersized_page_writes_in_single_chunk() {
    // 100 records with batch_size=500 must write in a single chunk and
    // succeed.
    let (_dir, url) = fresh_db("CREATE TABLE events (data TEXT NOT NULL)").await;

    let config = SqliteSinkConfig::new(&url, "events")
        .column_mapping(SqliteColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(500);
    let sink = SqliteSink::new(config).await.unwrap();

    let records: Vec<Value> = (0..100).map(|i| json!({"i": i})).collect();
    let n = sink.write_batch(&records).await.unwrap();

    assert_eq!(n, 100);
    assert_eq!(count_rows(&url, "events").await, 100);
}

#[tokio::test]
async fn json_mode_empty_input_is_noop() {
    let (_dir, url) = fresh_db("CREATE TABLE events (data TEXT NOT NULL)").await;

    let config = SqliteSinkConfig::new(&url, "events")
        .column_mapping(SqliteColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(500);
    let sink = SqliteSink::new(config).await.unwrap();

    let n = sink.write_batch(&[]).await.unwrap();
    assert_eq!(n, 0);
    assert_eq!(count_rows(&url, "events").await, 0);
}

#[tokio::test]
async fn json_mode_exact_multiple_writes_all_rows() {
    // 1000 records with batch_size=500 should produce exactly 2 chunks and
    // all 1000 rows must land.
    let (_dir, url) = fresh_db("CREATE TABLE events (data TEXT NOT NULL)").await;

    let config = SqliteSinkConfig::new(&url, "events")
        .column_mapping(SqliteColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(500);
    let sink = SqliteSink::new(config).await.unwrap();

    let records: Vec<Value> = (0..1_000).map(|i| json!({"i": i})).collect();
    let n = sink.write_batch(&records).await.unwrap();

    assert_eq!(n, 1_000);
    assert_eq!(count_rows(&url, "events").await, 1_000);
}

#[tokio::test]
async fn auto_map_mode_rechunks_large_page() {
    // 1500 records with batch_size=500 in AutoMap mode must produce 3
    // multi-row INSERT transactions and end up with all 1500 rows.
    let (_dir, url) =
        fresh_db("CREATE TABLE events (user_id TEXT NOT NULL, event TEXT NOT NULL)").await;

    let config = SqliteSinkConfig::new(&url, "events")
        .column_mapping(SqliteColumnMapping::AutoMap)
        .with_batch_size(500);
    let sink = SqliteSink::new(config).await.unwrap();

    let records: Vec<Value> = (0..1_500)
        .map(|i| json!({"user_id": format!("u{i}"), "event": "signup"}))
        .collect();
    let n = sink.write_batch(&records).await.unwrap();

    assert_eq!(n, 1_500);
    assert_eq!(count_rows(&url, "events").await, 1_500);
}

#[tokio::test]
async fn auto_map_binds_native_types_not_json_strings() {
    // Regression for #78/#4. AutoMap used to bind every value as
    // serde_json::to_string(v), so "Bob" was stored as the 5-char string
    // "Bob" (embedded quotes), `true` became the text "true", and a column
    // present in an earlier record but missing from a later one was bound as
    // the literal text "null" instead of SQL NULL.
    let (_dir, url) =
        fresh_db("CREATE TABLE people (name TEXT, active INTEGER, score REAL, note TEXT)").await;

    let config = SqliteSinkConfig::new(&url, "people").column_mapping(SqliteColumnMapping::AutoMap);
    let sink = SqliteSink::new(config).await.unwrap();

    // First record defines `note`; second omits it so AutoMap must bind a real
    // SQL NULL for the absent column (insert_columns is fixed from row 1).
    let records = vec![
        json!({"name": "Bob", "active": true, "score": 1.5, "note": "hi"}),
        json!({"name": "Sue", "active": false, "score": 2.5}),
    ];
    sink.write_batch(&records).await.unwrap();

    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .expect("connect");

    let bob = sqlx::query(
        "SELECT name, active, score, note, typeof(name) AS tn, typeof(active) AS ta \
         FROM people WHERE name = 'Bob'",
    )
    .fetch_one(&pool)
    .await
    .expect("bob row (name must be stored unquoted)");
    assert_eq!(bob.get::<String, _>("tn"), "text");
    assert_eq!(bob.get::<String, _>("name"), "Bob");
    assert_eq!(bob.get::<String, _>("ta"), "integer");
    assert_eq!(bob.get::<i64, _>("active"), 1);
    assert_eq!(bob.get::<f64, _>("score"), 1.5);
    assert_eq!(bob.get::<String, _>("note"), "hi");

    let sue =
        sqlx::query("SELECT active, note, typeof(note) AS tnote FROM people WHERE name = 'Sue'")
            .fetch_one(&pool)
            .await
            .expect("sue row");
    assert_eq!(sue.get::<i64, _>("active"), 0, "false must bind integer 0");
    assert_eq!(
        sue.get::<String, _>("tnote"),
        "null",
        "missing column must bind SQL NULL, not the text 'null'"
    );
    assert_eq!(sue.get::<Option<String>, _>("note"), None);

    pool.close().await;
}

#[tokio::test]
async fn auto_map_chunks_to_respect_sqlite_var_limit() {
    // Regression for #78/#21: SQLite caps bind variables at 32766. A wide
    // table at a large batch (100 cols × 1000 rows = 100_000 binds) in a
    // single INSERT would fail with "too many SQL variables"; the sink must
    // sub-chunk and still land every row.
    let cols: Vec<String> = (0..100).map(|i| format!("c{i}")).collect();
    let create = format!(
        "CREATE TABLE wide ({})",
        cols.iter()
            .map(|c| format!("{c} INTEGER"))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let (_dir, url) = fresh_db(&create).await;

    let config = SqliteSinkConfig::new(&url, "wide")
        .column_mapping(SqliteColumnMapping::AutoMap)
        .with_batch_size(0); // one slice → exercises the inner var-limit chunking
    let sink = SqliteSink::new(config).await.unwrap();

    let records: Vec<Value> = (0..1_000)
        .map(|r| {
            let mut m = serde_json::Map::new();
            for (i, c) in cols.iter().enumerate() {
                m.insert(c.clone(), json!(r * 100 + i as i64));
            }
            Value::Object(m)
        })
        .collect();

    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 1_000);
    assert_eq!(count_rows(&url, "wide").await, 1_000);
}

#[tokio::test]
async fn auto_map_mode_batch_size_zero_passes_page_through() {
    // batch_size=0 in AutoMap mode writes the entire slice as a single
    // transaction.
    let (_dir, url) =
        fresh_db("CREATE TABLE events (user_id TEXT NOT NULL, event TEXT NOT NULL)").await;

    let config = SqliteSinkConfig::new(&url, "events")
        .column_mapping(SqliteColumnMapping::AutoMap)
        .with_batch_size(0);
    let sink = SqliteSink::new(config).await.unwrap();

    let records: Vec<Value> = (0..2_500)
        .map(|i| json!({"user_id": format!("u{i}"), "event": "signup"}))
        .collect();
    let n = sink.write_batch(&records).await.unwrap();

    assert_eq!(n, 2_500);
    assert_eq!(count_rows(&url, "events").await, 2_500);
}

#[tokio::test]
async fn batch_size_atomicity_per_chunk_preserved() {
    // Each chunk is wrapped in its own BEGIN/COMMIT transaction. The
    // implementation guarantees `records.is_empty()` short-circuits before
    // any I/O; combined with the in-chunk multi-row INSERT this means every
    // returned row count corresponds to rows visible in the destination
    // (no half-committed batches).
    let (_dir, url) = fresh_db("CREATE TABLE events (data TEXT NOT NULL)").await;

    let config = SqliteSinkConfig::new(&url, "events")
        .column_mapping(SqliteColumnMapping::Json {
            column: "data".into(),
        })
        .with_batch_size(300);
    let sink = SqliteSink::new(config).await.unwrap();

    // Two separate writes — the second's transaction commit must not affect
    // the first's visibility.
    let first: Vec<Value> = (0..700).map(|i| json!({"i": i})).collect();
    let second: Vec<Value> = (0..400).map(|i| json!({"i": i + 1000})).collect();

    sink.write_batch(&first).await.unwrap();
    assert_eq!(count_rows(&url, "events").await, 700);

    sink.write_batch(&second).await.unwrap();
    assert_eq!(count_rows(&url, "events").await, 1_100);
}
