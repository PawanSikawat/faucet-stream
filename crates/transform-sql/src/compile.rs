//! Connection setup, reference-relation loading, and query validation.

use crate::config::{RelationSource, RelationSpec, SqlTransformConfig};
use crate::shovel::values_to_record_batch;
use duckdb::Connection;
use duckdb::vtab::arrow::{ArrowVTab, arrow_recordbatch_to_query_params};
use faucet_core::FaucetError;

const RESERVED: &str = "batch";

fn cfg_err(msg: impl Into<String>) -> FaucetError {
    FaucetError::Config(format!("sql transform: {}", msg.into()))
}

/// A relation we may need to reload on mtime change.
pub(crate) struct Reloadable {
    pub name: String,
    pub path: String,
    pub has_header: bool,
    pub is_csv: bool,
    pub last_mtime: Option<std::time::SystemTime>,
}

/// Open an in-memory connection, apply pragmas, register the Arrow VTab, and
/// load every reference relation. Returns the connection + reload tracking.
pub(crate) fn build_connection(
    cfg: &SqlTransformConfig,
) -> Result<(Connection, Vec<Reloadable>), FaucetError> {
    let conn = Connection::open_in_memory().map_err(|e| cfg_err(format!("open duckdb: {e}")))?;
    if let Some(ref m) = cfg.memory_limit {
        conn.execute_batch(&format!("SET memory_limit='{}';", sql_escape(m)))
            .map_err(|e| cfg_err(format!("memory_limit: {e}")))?;
    }
    if let Some(t) = cfg.threads {
        conn.execute_batch(&format!("SET threads={t};"))
            .map_err(|e| cfg_err(format!("threads: {e}")))?;
    }
    conn.register_table_function::<ArrowVTab>("arrow")
        .map_err(|e| cfg_err(format!("register arrow vtab: {e}")))?;

    let mut reloadables = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for rel in &cfg.relations {
        validate_ident(&rel.name)?;
        if rel.name.eq_ignore_ascii_case(RESERVED) {
            return Err(cfg_err("relation name 'batch' is reserved for the page"));
        }
        if !seen.insert(rel.name.clone()) {
            return Err(cfg_err(format!("duplicate relation name '{}'", rel.name)));
        }
        load_relation(&conn, rel)?;
        if rel.reload_on_change
            && let Some(r) = reloadable_for(rel)?
        {
            reloadables.push(r);
        }
    }
    Ok((conn, reloadables))
}

fn validate_ident(name: &str) -> Result<(), FaucetError> {
    let ok = !name.is_empty()
        && name
            .chars()
            .next()
            .map(|c| c.is_ascii_alphabetic() || c == '_')
            .unwrap_or(false)
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(cfg_err(format!("invalid relation name '{name}'")))
    }
}

fn load_relation(conn: &Connection, rel: &RelationSpec) -> Result<(), FaucetError> {
    match &rel.source {
        RelationSource::Csv { path, has_header } => {
            check_exists(path)?;
            conn.execute_batch(&format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_csv_auto('{}', header={});",
                rel.name,
                sql_escape(path),
                has_header
            ))
            .map_err(|e| cfg_err(format!("load csv relation '{}': {e}", rel.name)))
        }
        RelationSource::Jsonl { path } => {
            check_exists(path)?;
            conn.execute_batch(&format!(
                "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_json_auto('{}', format='newline_delimited');",
                rel.name,
                sql_escape(path)
            ))
            .map_err(|e| cfg_err(format!("load jsonl relation '{}': {e}", rel.name)))
        }
        RelationSource::Values { columns, rows } => {
            if columns.is_empty() {
                return Err(cfg_err(format!(
                    "values relation '{}' has no columns",
                    rel.name
                )));
            }
            let batch = values_to_record_batch(columns, rows)?;
            let params = arrow_recordbatch_to_query_params(batch);
            conn.execute(
                &format!(
                    "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM arrow(?, ?)",
                    rel.name
                ),
                params,
            )
            .map_err(|e| cfg_err(format!("load values relation '{}': {e}", rel.name)))?;
            Ok(())
        }
    }
}

fn reloadable_for(rel: &RelationSpec) -> Result<Option<Reloadable>, FaucetError> {
    let (path, has_header, is_csv) = match &rel.source {
        RelationSource::Csv { path, has_header } => (path.clone(), *has_header, true),
        RelationSource::Jsonl { path } => (path.clone(), false, false),
        RelationSource::Values { .. } => return Ok(None),
    };
    let last_mtime = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
    Ok(Some(Reloadable {
        name: rel.name.clone(),
        path,
        has_header,
        is_csv,
        last_mtime,
    }))
}

fn check_exists(path: &str) -> Result<(), FaucetError> {
    if std::path::Path::new(path).exists() {
        Ok(())
    } else {
        Err(cfg_err(format!(
            "reference relation file does not exist: {path}"
        )))
    }
}

fn sql_escape(s: &str) -> String {
    s.replace('\'', "''")
}

/// Validate the query: parse + bind, tolerating the not-yet-existing `batch`.
pub(crate) fn validate_query(conn: &Connection, query: &str) -> Result<(), FaucetError> {
    match conn.prepare(query) {
        Ok(_) => Ok(()),
        Err(e) => {
            let msg = e.to_string();
            let lower = msg.to_lowercase();
            // Tolerate binder errors that are only about the missing `batch`.
            let about_batch = lower.contains("batch")
                && (lower.contains("does not exist")
                    || lower.contains("not found")
                    || lower.contains("referenced")
                    || lower.contains("no such table"));
            if about_batch {
                Ok(())
            } else {
                Err(FaucetError::Transform(format!(
                    "sql transform: invalid query: {msg}"
                )))
            }
        }
    }
}
