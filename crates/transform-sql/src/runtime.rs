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
    let params = arrow_recordbatch_to_query_params(batch);
    st.conn
        .execute(
            "CREATE OR REPLACE TEMP TABLE batch AS SELECT * FROM arrow(?, ?)",
            params,
        )
        .map_err(|e| FaucetError::Transform(format!("sql transform: register batch: {e}")))?;

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
