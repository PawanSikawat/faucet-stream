//! The compiled SQL transform: owns the DuckDB connection and runs each page.

use crate::compile::{Reloadable, build_connection, sql_escape, validate_query};
use crate::config::SqlTransformConfig;
use crate::shovel::{infer_schema, json_to_record_batch, record_batches_to_json, schema_eq};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use duckdb::Connection;
use duckdb::vtab::arrow::arrow_recordbatch_to_query_params;
use faucet_core::FaucetError;
use faucet_core::stage::TransformStage;
use serde_json::Value;
use std::sync::{Arc, Mutex};

struct State {
    conn: Connection,
    query: String,
    reloadables: Vec<Reloadable>,
    cached_schema: Option<SchemaRef>,
    pages_seen: u64,
    aggregates: Option<bool>,
    warned: bool,
}

/// A compiled SQL transform. One DuckDB connection, reused across the row's pages.
pub struct SqlTransform {
    state: Arc<Mutex<State>>,
}

impl std::fmt::Debug for SqlTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("SqlTransform");
        match self.state.lock() {
            Ok(st) => d.field("query", &st.query),
            Err(e) => d.field("query", &e.into_inner().query),
        };
        d.finish_non_exhaustive()
    }
}

impl SqlTransform {
    /// Build the connection, load reference relations, and validate the query.
    pub fn compile(cfg: &SqlTransformConfig) -> Result<Self, FaucetError> {
        let (conn, reloadables) = build_connection(cfg)?;
        validate_query(&conn, &cfg.query)?;
        Ok(Self {
            state: Arc::new(Mutex::new(State {
                conn,
                query: cfg.query.clone(),
                reloadables,
                cached_schema: None,
                pages_seen: 0,
                aggregates: None,
                warned: false,
            })),
        })
    }

    /// Consume into a page-level transform stage.
    pub fn into_page_stage(self) -> TransformStage {
        let state = self.state;
        TransformStage::PageFn(Arc::new(move |records: Vec<Value>| {
            let mut st = state.lock().unwrap_or_else(|e| e.into_inner());
            execute_page(&mut st, records)
        }))
    }
}

fn execute_page(st: &mut State, records: Vec<Value>) -> Result<Vec<Value>, FaucetError> {
    if records.is_empty() {
        return Ok(Vec::new());
    }
    reload_relations(st)?;

    // Schema cache: infer once per page, reuse the cached schema on a match,
    // otherwise adopt the freshly inferred one (first page or drift).
    let fresh = infer_schema(&records)?;
    let schema = match &st.cached_schema {
        Some(s) if schema_eq(s, &fresh) => s.clone(),
        _ => {
            st.cached_schema = Some(fresh.clone());
            fresh
        }
    };
    let batch = json_to_record_batch(&records, schema)?;
    // DuckDB's arrow vtab copies each arrow array into a `DataChunk` whose
    // capacity is `STANDARD_VECTOR_SIZE` (2048); handing it a single batch with
    // more rows than that trips `assert(array.len() <= out.capacity())` in
    // duckdb-rs and **aborts the process** (#372) — reachable whenever a page
    // larger than 2048 rows reaches the transform (`batch_size` > 2048 or
    // `batch_size: 0`). Register the batch in <=2048-row slices instead: CREATE
    // from the first slice, INSERT the rest into the same temp table. All slices
    // land in one `batch` relation, so query semantics (incl. GROUP BY / window
    // aggregation over the whole page) are unchanged; a <=2048-row page still
    // takes exactly one CREATE, identical to before.
    register_batch_chunked(st, batch)?;

    // First-page aggregation detection (now that `batch` exists).
    if st.aggregates.is_none() {
        st.aggregates = Some(plan_has_aggregate(&st.conn, &st.query));
    }
    st.pages_seen += 1;
    if st.pages_seen >= 2 && st.aggregates == Some(true) && !st.warned {
        st.warned = true;
        tracing::warn!(
            target: "faucet::transform::sql",
            "sql transform with aggregation received multiple pages; aggregation is \
             per-page — set batch_size: 0 for global aggregation"
        );
    }

    let out = {
        let mut stmt = st
            .conn
            .prepare(&st.query)
            .map_err(|e| FaucetError::Transform(format!("sql transform: prepare: {e}")))?;
        let batches: Vec<RecordBatch> = stmt
            .query_arrow([])
            .map_err(|e| FaucetError::Transform(format!("sql transform: execute: {e}")))?
            .collect();
        record_batches_to_json(&batches)?
    };
    Ok(out)
}

/// DuckDB's fixed vector size — the maximum rows a single arrow array may carry
/// into the arrow vtab without tripping the capacity assertion (#372).
const DUCKDB_VECTOR_SIZE: usize = 2048;

/// Register `batch` as the temp table `batch`, splitting it into <=2048-row
/// slices so an oversized page never aborts the process (#372). The first slice
/// CREATEs the table, the rest INSERT into it — all landing in one relation, so
/// the downstream query is unaffected. `RecordBatch::slice` is zero-copy.
fn register_batch_chunked(st: &mut State, batch: RecordBatch) -> Result<(), FaucetError> {
    let total = batch.num_rows();
    let mut offset = 0;
    let mut first = true;
    // `execute_page` already returned early on empty input, but guard anyway so
    // a zero-row batch still CREATEs an empty `batch` table (matching the prior
    // single-CREATE behaviour) rather than leaving a stale one.
    loop {
        let len = (total - offset).min(DUCKDB_VECTOR_SIZE);
        let slice = batch.slice(offset, len);
        let params = arrow_recordbatch_to_query_params(slice);
        let sql = if first {
            "CREATE OR REPLACE TEMP TABLE batch AS SELECT * FROM arrow(?, ?)"
        } else {
            "INSERT INTO batch SELECT * FROM arrow(?, ?)"
        };
        st.conn
            .execute(sql, params)
            .map_err(|e| FaucetError::Transform(format!("sql transform: register batch: {e}")))?;
        first = false;
        offset += len;
        if offset >= total {
            break;
        }
    }
    Ok(())
}

fn reload_relations(st: &mut State) -> Result<(), FaucetError> {
    for r in st.reloadables.iter_mut() {
        let cur = std::fs::metadata(&r.path).and_then(|m| m.modified()).ok();
        if cur != r.last_mtime {
            let stmt = if r.is_csv {
                format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}', header={});",
                    r.name,
                    sql_escape(&r.path),
                    r.has_header
                )
            } else {
                format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_json_auto('{}', format='newline_delimited');",
                    r.name,
                    sql_escape(&r.path)
                )
            };
            st.conn.execute_batch(&stmt).map_err(|e| {
                FaucetError::Transform(format!("sql transform: reload '{}': {e}", r.name))
            })?;
            r.last_mtime = cur;
        }
    }
    Ok(())
}

fn plan_has_aggregate(conn: &Connection, query: &str) -> bool {
    let explain = format!("EXPLAIN {query}");
    let mut found = false;
    if let Ok(mut stmt) = conn.prepare(&explain)
        && let Ok(rows) = stmt.query_map([], |row| row.get::<_, String>(1))
    {
        for r in rows.flatten() {
            let u = r.to_uppercase();
            if u.contains("AGGREGATE") || u.contains("WINDOW") {
                found = true;
                break;
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SqlTransformConfig;
    use faucet_core::stage::{apply_stages_to_page, compile_stage};
    use serde_json::json;

    fn run(query: &str, rows: Vec<Value>) -> Vec<Value> {
        let cfg = SqlTransformConfig {
            query: query.into(),
            relations: vec![],
            memory_limit: None,
            threads: Some(1),
        };
        let stage = compile_stage(&SqlTransform::compile(&cfg).unwrap().into_page_stage()).unwrap();
        apply_stages_to_page(rows, std::slice::from_ref(&stage)).unwrap()
    }

    // #372: a page larger than DuckDB's 2048-row vector size used to abort the
    // whole process inside the arrow vtab. After chunked registration it must
    // pass through cleanly.
    #[test]
    fn large_page_passthrough_does_not_abort() {
        let rows: Vec<Value> = (0..10_000).map(|i| json!({"id": i, "v": i * 2})).collect();
        let out = run("SELECT * FROM batch", rows);
        assert_eq!(out.len(), 10_000, "every row of a >2048-row page survives");
        // Spot-check a row past the first vector boundary.
        assert_eq!(out[5_000]["id"], json!(5_000));
    }

    // Chunked registration lands every slice in one `batch` relation, so a
    // GROUP BY still aggregates over the whole page, not per-2048-chunk.
    #[test]
    fn large_page_aggregate_is_global_over_the_whole_page() {
        let rows: Vec<Value> = (0..5_000).map(|i| json!({"k": i % 4, "v": 1})).collect();
        let out = run(
            "SELECT k, COUNT(*) AS n FROM batch GROUP BY k ORDER BY k",
            rows,
        );
        assert_eq!(out.len(), 4, "one group per key");
        let total: i64 = out.iter().map(|r| r["n"].as_i64().unwrap()).sum();
        assert_eq!(total, 5_000, "every row counted exactly once across chunks");
        for r in &out {
            assert_eq!(r["n"], json!(1_250), "5000 rows / 4 keys = 1250 each");
        }
    }

    // The <=2048 fast path (single CREATE) is unchanged.
    #[test]
    fn small_page_still_works() {
        let out = run(
            "SELECT id FROM batch WHERE id >= 1 ORDER BY id",
            vec![json!({"id": 0}), json!({"id": 1}), json!({"id": 2})],
        );
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["id"], json!(1));
    }
}
