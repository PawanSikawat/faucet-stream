//! MySQL source implementation.

use crate::config::MysqlSourceConfig;
use async_trait::async_trait;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Column, MySqlPool, Row};
use std::pin::Pin;

/// A source that executes a SQL query against MySQL and returns rows as JSON.
pub struct MysqlSource {
    config: MysqlSourceConfig,
    pool: MySqlPool,
}

impl MysqlSource {
    /// Create a new MySQL source. Establishes a connection pool.
    pub async fn new(config: MysqlSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;

        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.connection_url)
            .await
            .map_err(|e| FaucetError::Config(format!("MySQL connection failed: {e}")))?;

        Ok(Self { config, pool })
    }
}

/// Convert a MySQL row column value to a `serde_json::Value`.
///
/// Attempts common types in order of likelihood. Falls back to `Value::Null`
/// for unsupported or null columns.
fn mysql_value_to_json(row: &sqlx::mysql::MySqlRow, col_name: &str) -> Value {
    // Try JSON first
    if let Ok(v) = row.try_get::<Value, _>(col_name) {
        return v;
    }

    // Try common scalar types
    if let Ok(v) = row.try_get::<String, _>(col_name) {
        return Value::String(v);
    }
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i32, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i16, _>(col_name) {
        return Value::Number(v.into());
    }
    // UNSIGNED integer columns (#264). sqlx-mysql treats UNSIGNED as a
    // distinct type from the signed decoders above, so without these arms
    // UNSIGNED columns fall through to the `bool` probe (TINYINT UNSIGNED ->
    // bool) or to the `Null` fall-through (larger UNSIGNED -> null), silently
    // corrupting every unsigned column including UNSIGNED primary keys.
    //
    // These are placed *after* the signed probes so a signed column always
    // matches a signed arm first, and *before* `bool`/`f64`/`f32` so a
    // `TINYINT UNSIGNED` decodes as a number rather than a bool (MySQL's
    // boolean is `TINYINT(1)`). `u64` fits `serde_json::Number` exactly, so
    // BIGINT UNSIGNED values above `i64::MAX` round-trip losslessly.
    if let Ok(v) = row.try_get::<u64, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<u32, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<u16, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<u8, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<f64, _>(col_name) {
        return serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<f32, _>(col_name) {
        return serde_json::Number::from_f64(v as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<bool, _>(col_name) {
        return Value::Bool(v);
    }

    // Richer types that would otherwise silently decode to Null (#78/#43).
    if let Ok(v) =
        row.try_get::<sqlx::types::chrono::DateTime<sqlx::types::chrono::Utc>, _>(col_name)
    {
        return Value::String(v.to_rfc3339());
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveDateTime, _>(col_name) {
        return Value::String(v.to_string());
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveDate, _>(col_name) {
        return Value::String(v.to_string());
    }
    if let Ok(v) = row.try_get::<sqlx::types::chrono::NaiveTime, _>(col_name) {
        return Value::String(v.to_string());
    }
    // DECIMAL → string, preserving exact precision.
    if let Ok(v) = row.try_get::<sqlx::types::BigDecimal, _>(col_name) {
        return Value::String(v.to_string());
    }
    // BLOB / BINARY → base64.
    if let Ok(v) = row.try_get::<Vec<u8>, _>(col_name) {
        use base64::Engine as _;
        return Value::String(base64::engine::general_purpose::STANDARD.encode(v));
    }

    Value::Null
}

/// Build the effective SQL query and ordered context-bind values for a given
/// parent context. Returns the literal query when there is no context.
fn resolve_query(
    config: &MysqlSourceConfig,
    context: &std::collections::HashMap<String, Value>,
) -> (String, Vec<Value>) {
    if context.is_empty() {
        (config.query.clone(), Vec::new())
    } else {
        faucet_core::util::substitute_context_bind_params(&config.query, context, 1, |_| {
            "?".to_string()
        })
    }
}

/// Apply context-derived bind values onto a sqlx query.
fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments>,
    bind_values: &'q [Value],
) -> sqlx::query::Query<'q, sqlx::MySql, sqlx::mysql::MySqlArguments> {
    for value in bind_values {
        query = match value {
            Value::String(s) => query.bind(s.clone()),
            Value::Number(n) if n.is_i64() => query.bind(n.as_i64().unwrap()),
            Value::Number(n) => query.bind(n.as_f64().unwrap_or(0.0)),
            Value::Bool(b) => query.bind(*b),
            Value::Null => query.bind(None::<String>),
            _ => query.bind(value.to_string()),
        };
    }
    query
}

/// Convert a single `MySqlRow` into a JSON object whose keys are the row's
/// column names.
fn row_to_json(row: &sqlx::mysql::MySqlRow) -> Value {
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = mysql_value_to_json(row, &name);
        map.insert(name, value);
    }
    Value::Object(map)
}

#[async_trait]
impl faucet_core::Source for MysqlSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (query_str, bind_values) = resolve_query(&self.config, context);
        let query = bind_params(sqlx::query(&query_str), &bind_values);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Config(format!("MySQL query failed: {e}")))?;

        let records: Vec<Value> = rows.iter().map(row_to_json).collect();
        tracing::info!(rows = records.len(), query = %self.config.query, "MySQL source fetch complete");
        Ok(records)
    }

    /// Stream rows from the underlying sqlx cursor without buffering the full
    /// result set. Each emitted [`StreamPage`] holds up to
    /// [`MysqlSourceConfig::batch_size`] rows.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire cursor into a single page. The
    /// mysql query source has no incremental-replication mode today, so
    /// every emitted page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let (query_str, bind_values) = resolve_query(&self.config, context);
            let query = bind_params(sqlx::query(&query_str), &bind_values);

            let mut rows = query.fetch(&self.pool);
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|e| FaucetError::Config(format!("MySQL query failed: {e}")))?
            {
                buffer.push(row_to_json(&row));
                if buffer.len() >= chunk {
                    let page = std::mem::replace(&mut buffer, Vec::with_capacity(initial_capacity));
                    total += page.len();
                    yield StreamPage { records: page, bookmark: None };
                }
            }
            if !buffer.is_empty() {
                total += buffer.len();
                yield StreamPage { records: buffer, bookmark: None };
            }

            tracing::info!(
                rows = total,
                batch_size,
                query = %self.config.query,
                "MySQL source stream complete",
            );
        })
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MysqlSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?query={}",
            faucet_core::redact_uri_credentials(&self.config.connection_url),
            self.config.query
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = MysqlSourceConfig::new("mysql://localhost/test", "SELECT 1");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match MysqlSource::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }

    // dataset_uri is a pure-config method; the source requires a live DB to
    // construct so we verify the credential-stripping logic directly.
    #[test]
    fn dataset_uri_strips_credentials() {
        let redacted = faucet_core::redact_uri_credentials("mysql://u:p@h:3306/db");
        let uri = format!("{}?query={}", redacted, "SELECT 1");
        assert_eq!(uri, "mysql://h:3306/db?query=SELECT 1");
    }
}
