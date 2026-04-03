//! MySQL source implementation.

use crate::config::MysqlSourceConfig;
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{Column, MySqlPool, Row};

/// A source that executes a SQL query against MySQL and returns rows as JSON.
pub struct MysqlSource {
    config: MysqlSourceConfig,
    pool: MySqlPool,
}

impl MysqlSource {
    /// Create a new MySQL source. Establishes a connection pool.
    pub async fn new(config: MysqlSourceConfig) -> Result<Self, FaucetError> {
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

    Value::Null
}

#[async_trait]
impl faucet_core::Source for MysqlSource {
    async fn fetch_with_context(
        &self,
        _context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let rows = sqlx::query(&self.config.query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Config(format!("MySQL query failed: {e}")))?;

        let mut records = Vec::with_capacity(rows.len());
        for row in &rows {
            let mut map = serde_json::Map::new();
            for col in row.columns() {
                let name = col.name().to_string();
                let value = mysql_value_to_json(row, &name);
                map.insert(name, value);
            }
            records.push(Value::Object(map));
        }

        tracing::info!(rows = records.len(), query = %self.config.query, "MySQL source fetch complete");
        Ok(records)
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MysqlSourceConfig))
            .expect("schema serialization")
    }
}
