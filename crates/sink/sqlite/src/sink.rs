//! SQLite sink implementation.

use crate::config::{SqliteColumnMapping, SqliteSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Row, SqlitePool};

/// A sink that writes JSON records to a SQLite table.
pub struct SqliteSink {
    config: SqliteSinkConfig,
    pool: SqlitePool,
}

impl SqliteSink {
    /// Create a new SQLite sink. Establishes a connection pool.
    pub async fn new(config: SqliteSinkConfig) -> Result<Self, FaucetError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite connection failed: {e}")))?;

        Ok(Self { config, pool })
    }

    /// Insert a batch of records using JSON column mode.
    /// Uses a single multi-row INSERT wrapped in a transaction.
    async fn insert_json(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Build multi-row INSERT: INSERT INTO t (col) VALUES (?), (?), ...
        let placeholders: Vec<&str> = records.iter().map(|_| "(?)").collect();
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            quote_ident(&self.config.table_name),
            quote_ident(column),
            placeholders.join(", ")
        );

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        let mut q = sqlx::query(&insert_sql);
        for record in records {
            let json_str = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
            q = q.bind(json_str);
        }

        q.execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;

        Ok(records.len())
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers column names from `pragma_table_info` and maps
    /// top-level JSON fields to columns. Uses a single multi-row INSERT
    /// wrapped in a transaction.
    async fn insert_auto_map(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names from the table using pragma_table_info.
        let columns: Vec<String> = sqlx::query(&format!(
            "PRAGMA table_info({})",
            quote_ident(&self.config.table_name)
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();

        if columns.is_empty() {
            return Err(FaucetError::Sink(format!(
                "table '{}' has no columns or does not exist",
                self.config.table_name
            )));
        }

        // Pre-validate all records and collect matched column values.
        let mut matched_rows: Vec<Vec<(&String, &Value)>> = Vec::with_capacity(records.len());
        let mut insert_columns: Option<Vec<String>> = None;

        for record in records {
            let obj = record
                .as_object()
                .ok_or_else(|| FaucetError::Sink("AutoMap requires JSON object records".into()))?;

            let matching: Vec<(&String, &Value)> = columns
                .iter()
                .filter_map(|col| obj.get(col).map(|v| (col, v)))
                .collect();

            if matching.is_empty() {
                tracing::warn!(
                    record_keys = ?obj.keys().collect::<Vec<_>>(),
                    table_columns = ?columns,
                    "record has no keys matching table columns, skipping"
                );
                continue;
            }

            if insert_columns.is_none() {
                insert_columns = Some(matching.iter().map(|(c, _)| (*c).clone()).collect());
            }

            matched_rows.push(matching);
        }

        let insert_columns = match insert_columns {
            Some(cols) => cols,
            None => return Ok(0),
        };

        if matched_rows.is_empty() {
            return Ok(0);
        }

        let num_cols = insert_columns.len();
        let num_rows = matched_rows.len();
        let col_names: Vec<String> = insert_columns.iter().map(|c| quote_ident(c)).collect();

        // Build multi-row VALUES clause: (?, ?), (?, ?), ...
        let row_placeholder = format!("({})", vec!["?"; num_cols].join(", "));
        let value_tuples: Vec<&str> = (0..num_rows).map(|_| row_placeholder.as_str()).collect();

        let query = format!(
            "INSERT INTO {} ({}) VALUES {}",
            quote_ident(&self.config.table_name),
            col_names.join(", "),
            value_tuples.join(", ")
        );

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction begin failed: {e}")))?;

        let mut q = sqlx::query(&query);
        for matched in &matched_rows {
            for col in &insert_columns {
                let val = matched.iter().find(|(c, _)| *c == col).map(|(_, v)| *v);
                // Bind native SQLite types so column affinity and typed reads
                // round-trip correctly. Binding every value as a JSON string
                // (the old behaviour) stored `"Bob"` with embedded quotes,
                // turned `true` into the text "true", and bound the literal
                // text "null" for absent columns instead of SQL NULL (#78/#4).
                q = match val {
                    None | Some(Value::Null) => q.bind(None::<String>),
                    Some(Value::Bool(b)) => q.bind(*b),
                    Some(Value::Number(n)) => {
                        if let Some(i) = n.as_i64() {
                            q.bind(i)
                        } else if let Some(f) = n.as_f64() {
                            q.bind(f)
                        } else {
                            // u64 above i64::MAX — preserve exact text.
                            q.bind(n.to_string())
                        }
                    }
                    Some(Value::String(s)) => q.bind(s.clone()),
                    // Arrays/objects have no scalar SQL representation — store
                    // their JSON text (suitable for TEXT / JSON columns).
                    Some(v) => q.bind(v.to_string()),
                };
            }
        }

        q.execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite insert failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("SQLite transaction commit failed: {e}")))?;

        Ok(num_rows)
    }
}

#[async_trait]
impl faucet_core::Sink for SqliteSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(SqliteSinkConfig))
            .expect("schema serialization")
    }

    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // `batch_size = 0` is the "no batching" sentinel: write the entire
        // upstream slice as a single multi-row INSERT inside one
        // `BEGIN`/`COMMIT` transaction, preserving `StreamPage` framing.
        // Otherwise re-chunk into `batch_size` slices so each transaction
        // stays near SQLite's sweet spot (~1000 rows per multi-row INSERT).
        let effective_chunk = if self.config.batch_size == 0 {
            records.len()
        } else {
            self.config.batch_size
        };

        let mut total = 0;
        for chunk in records.chunks(effective_chunk) {
            total += match &self.config.column_mapping {
                SqliteColumnMapping::Json { column } => self.insert_json(chunk, column).await?,
                SqliteColumnMapping::AutoMap => self.insert_auto_map(chunk).await?,
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            "SQLite write complete"
        );
        Ok(total)
    }
}
