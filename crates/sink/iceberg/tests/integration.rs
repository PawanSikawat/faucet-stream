//! Integration tests for `IcebergSink` using a SQLite-backed SQL catalog and
//! a local-filesystem warehouse — no Docker, no network, fully in-process.
//!
//! # Requirements
//!
//! These tests require the `catalog-sql` Cargo feature to be enabled:
//!
//! ```sh
//! cargo test -p faucet-sink-iceberg --test integration --features catalog-sql
//! ```
//!
//! CI runs `cargo test --workspace --all-features`, which includes `catalog-sql`.
//!
//! # Approach
//!
//! - **Catalog:** `iceberg-catalog-sql` backed by a SQLite file in a `TempDir`.
//!   The SQL catalog auto-creates its schema tables (`iceberg_tables`,
//!   `iceberg_namespace_properties`) inside `SqlCatalog::new()` via
//!   `CREATE TABLE IF NOT EXISTS`, so no separate migration step is needed.
//! - **Warehouse:** local filesystem via `file://<tempdir>/warehouse`.
//!   `iceberg` 0.9.1's `LocalFsStorageFactory` (no extra feature flag required)
//!   handles `file://` paths natively.
//! - **Bind style:** SQLite requires `?` placeholders (`SqlBindStyle::QMark`).
//!   The sink's `build_sql` now infers this automatically from a `sqlite:` URI.
//!
//! # Sink defect surfaced by these tests
//!
//! Prior to this test, `build_sql` called `SqlCatalogBuilder::default().load(...)`
//! without calling `.with_storage_factory(...)`.  The `SqlCatalogBuilder` defaults
//! `storage_factory` to `None`, and `SqlCatalog::new()` returns an immediate error
//! when it is `None`.  The sink now sets `LocalFsStorageFactory` for local
//! warehouses.  A GitHub follow-up issue should track OpenDal-backed cloud
//! warehouses (S3/GCS) for the SQL catalog.

#![cfg(feature = "catalog-sql")]

use std::sync::Arc;

use faucet_core::Sink;
use faucet_sink_iceberg::{IcebergSink, IcebergSinkConfig};
use iceberg::io::LocalFsStorageFactory;
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use serde_json::json;
use tempfile::TempDir;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Returns an `IcebergSinkConfig` that writes to a SQLite catalog + local-FS
/// warehouse inside `dir`.
fn sink_config(dir: &TempDir, table: &str) -> IcebergSinkConfig {
    let db_path = dir.path().join("catalog.db");
    let warehouse_path = dir.path().join("warehouse");
    std::fs::create_dir_all(&warehouse_path).expect("create warehouse dir");

    // `?mode=rwc` enables SQLite's "read-write-create" mode so sqlx creates
    // the database file if it does not already exist (without this flag the
    // AnyPool connection fails with "unable to open database file").
    let sqlite_uri = format!("sqlite:{}?mode=rwc", db_path.display());
    let warehouse_uri = format!("file://{}", warehouse_path.display());

    serde_json::from_value(json!({
        "catalog": {
            "type": "sql",
            "uri": sqlite_uri,
            "warehouse": warehouse_uri
            // bind_style and storage_factory are inferred by build_sql from
            // the sqlite: URI prefix and the file:// warehouse prefix.
        },
        "namespace": ["db"],
        "table": table,
        "create_if_missing": true,
        // batch_size = 0 → the whole write_batch slice is one chunk.
        "batch_size": 0
    }))
    .expect("sink config parse")
}

/// Build a reader `SqlCatalog` pointing at the SAME SQLite database + warehouse
/// so we can inspect the committed table metadata after the sink runs.
async fn open_reader_catalog(dir: &TempDir) -> impl Catalog {
    let db_path = dir.path().join("catalog.db");
    let warehouse_path = dir.path().join("warehouse");

    // Reader also needs `mode=rwc` so sqlx can open the already-existing file
    // via the same AnyPool path that creates-if-missing.
    let sqlite_uri = format!("sqlite:{}?mode=rwc", db_path.display());
    let warehouse_uri = format!("file://{}", warehouse_path.display());

    let props = std::collections::HashMap::from([
        (SQL_CATALOG_PROP_URI.to_string(), sqlite_uri),
        (SQL_CATALOG_PROP_WAREHOUSE.to_string(), warehouse_uri),
        (
            SQL_CATALOG_PROP_BIND_STYLE.to_string(),
            SqlBindStyle::QMark.to_string(),
        ),
    ]);

    // Must use the SAME catalog name as the sink ("faucet-iceberg") because
    // the SQL catalog stores tables keyed by (catalog_name, namespace, table).
    SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(LocalFsStorageFactory))
        .load("faucet-iceberg", props)
        .await
        .expect("reader catalog open")
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Core path: write one batch → flush → assert exactly ONE snapshot.
/// Write a second batch → flush → assert TWO snapshots with a different
/// snapshot ID.  Then flush again with no writes → still TWO (no empty snapshot).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_and_flush_creates_iceberg_snapshots() {
    let dir = TempDir::new().expect("tempdir");

    // ── Write batch 1 ─────────────────────────────────────────────────────────

    let cfg = sink_config(&dir, "events");
    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");

    let records: Vec<serde_json::Value> = (0u64..100)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();

    let written = sink.write_batch(&records).await.expect("write_batch 1");
    assert_eq!(written, 100, "expected 100 rows written");

    sink.flush().await.expect("flush 1");

    // ── Assert snapshot 1 ─────────────────────────────────────────────────────

    let reader = open_reader_catalog(&dir).await;
    let ns = NamespaceIdent::from_strs(["db"]).expect("namespace ident");
    let tid = TableIdent::new(ns.clone(), "events".to_string());

    let table1 = reader
        .load_table(&tid)
        .await
        .expect("load_table after flush 1");
    let meta1 = table1.metadata();

    let snap_count_1 = meta1.snapshots().count();
    assert_eq!(
        snap_count_1, 1,
        "expected exactly 1 snapshot after first flush, got {snap_count_1}"
    );
    let snap1_id = meta1
        .current_snapshot_id()
        .expect("current_snapshot_id must be set after first flush");

    // ── Write batch 2 ─────────────────────────────────────────────────────────

    let records2: Vec<serde_json::Value> = (100u64..200)
        .map(|i| json!({ "id": i, "name": format!("n{i}") }))
        .collect();

    let written2 = sink.write_batch(&records2).await.expect("write_batch 2");
    assert_eq!(written2, 100, "expected 100 rows written in batch 2");

    sink.flush().await.expect("flush 2");

    // ── Assert snapshot 2 ─────────────────────────────────────────────────────

    let table2 = reader
        .load_table(&tid)
        .await
        .expect("load_table after flush 2");
    let meta2 = table2.metadata();

    let snap_count_2 = meta2.snapshots().count();
    assert_eq!(
        snap_count_2, 2,
        "expected exactly 2 snapshots after second flush, got {snap_count_2}"
    );

    let snap2_id = meta2
        .current_snapshot_id()
        .expect("current_snapshot_id must be set after second flush");

    assert_ne!(
        snap1_id, snap2_id,
        "second flush must produce a new snapshot id (got {snap1_id} twice)"
    );

    // ── Empty flush is a no-op ─────────────────────────────────────────────────

    sink.flush().await.expect("flush 3 (empty)");

    let table3 = reader
        .load_table(&tid)
        .await
        .expect("load_table after empty flush");
    assert_eq!(
        table3.metadata().snapshots().count(),
        2,
        "empty flush must NOT create a third snapshot"
    );
}

/// Empty `write_batch` followed by `flush` must not produce any snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_write_batch_no_snapshot() {
    let dir = TempDir::new().expect("tempdir");
    let cfg = sink_config(&dir, "empty_table");
    let sink = IcebergSink::new(cfg).await.expect("IcebergSink::new");

    let written = sink.write_batch(&[]).await.expect("empty write_batch");
    assert_eq!(written, 0);

    // Flush without any prior writes — table does not even exist yet.
    // flush() must succeed without panicking.
    sink.flush().await.expect("flush after empty write_batch");

    // The table was never created (no records → no schema inference →
    // no `create_table` call), so it should not exist in the catalog.
    let reader = open_reader_catalog(&dir).await;
    let ns = NamespaceIdent::from_strs(["db"]).expect("namespace ident");
    let tid = TableIdent::new(ns, "empty_table".to_string());
    let exists = reader.table_exists(&tid).await.expect("table_exists check");
    assert!(!exists, "table must not exist after empty write+flush");
}
