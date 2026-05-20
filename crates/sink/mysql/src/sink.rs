//! MySQL sink implementation.

use crate::config::{MysqlColumnMapping, MysqlSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, Row};

/// A sink that writes JSON records to a MySQL table.
pub struct MysqlSink {
    config: MysqlSinkConfig,
    pool: MySqlPool,
}

/// Quote a MySQL identifier using backticks.
///
/// Wraps the name in backticks and escapes any embedded backticks by doubling
/// them, per MySQL convention.
fn quote_ident_mysql(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

impl MysqlSink {
    /// Create a new MySQL sink. Establishes a connection pool.
    pub async fn new(config: MysqlSinkConfig) -> Result<Self, FaucetError> {
        let pool = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.connection_url)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL connection failed: {e}")))?;

        Ok(Self { config, pool })
    }

    /// Insert a batch of records using JSON column mode.
    /// Uses a single multi-row INSERT for efficiency.
    async fn insert_json(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Build multi-row INSERT: INSERT INTO t (col) VALUES (?), (?), ...
        let placeholders: Vec<&str> = records.iter().map(|_| "(?)").collect();
        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            quote_ident_mysql(&self.config.table_name),
            quote_ident_mysql(column),
            placeholders.join(", ")
        );

        let mut q = sqlx::query(&insert_sql);
        for record in records {
            let json_str = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;
            q = q.bind(json_str);
        }

        q.execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL insert failed: {e}")))?;

        Ok(records.len())
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers column names from INFORMATION_SCHEMA and maps
    /// top-level JSON fields to columns. Uses a single multi-row INSERT.
    async fn insert_auto_map(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names from the table.
        let columns: Vec<String> = sqlx::query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION"
        )
        .bind(&self.config.table_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?
        .iter()
        .map(|row| row.get::<String, _>("COLUMN_NAME"))
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
        let col_names: Vec<String> = insert_columns
            .iter()
            .map(|c| quote_ident_mysql(c))
            .collect();

        // Build multi-row VALUES clause: (?, ?), (?, ?), ...
        let row_placeholder = format!("({})", vec!["?"; num_cols].join(", "));
        let value_tuples: Vec<&str> = (0..num_rows).map(|_| row_placeholder.as_str()).collect();

        let query = format!(
            "INSERT INTO {} ({}) VALUES {}",
            quote_ident_mysql(&self.config.table_name),
            col_names.join(", "),
            value_tuples.join(", ")
        );

        let mut q = sqlx::query(&query);
        for matched in &matched_rows {
            for col in &insert_columns {
                let val = matched.iter().find(|(c, _)| *c == col).map(|(_, v)| *v);
                let s = match val {
                    Some(v) => serde_json::to_string(v).map_err(|e| {
                        FaucetError::Sink(format!("failed to serialize column value: {e}"))
                    })?,
                    None => "null".to_string(),
                };
                q = q.bind(s);
            }
        }

        q.execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL insert failed: {e}")))?;

        Ok(num_rows)
    }
}

#[async_trait]
impl faucet_core::Sink for MysqlSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MysqlSinkConfig))
            .expect("schema serialization")
    }

    /// Write records to MySQL.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size` rows and
    /// each chunk is sent as a separate multi-row `INSERT`. When
    /// `config.batch_size == 0`, the entire slice is sent in a single
    /// multi-row `INSERT` — useful when upstream `StreamPage`s are already
    /// sized for MySQL's `max_allowed_packet` limit.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            // Sentinel: pass the entire upstream page through in a single
            // multi-row INSERT. Subject to MySQL's max_allowed_packet
            // (default 64MB).
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut total = 0;
        for chunk in chunks {
            total += match &self.config.column_mapping {
                MysqlColumnMapping::Json { column } => self.insert_json(chunk, column).await?,
                MysqlColumnMapping::AutoMap => self.insert_auto_map(chunk).await?,
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            "MySQL write complete"
        );
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_ident_mysql_simple() {
        assert_eq!(quote_ident_mysql("my_table"), "`my_table`");
    }

    #[test]
    fn quote_ident_mysql_with_backtick() {
        assert_eq!(quote_ident_mysql("has`tick"), "`has``tick`");
    }

    #[test]
    fn quote_ident_mysql_empty() {
        assert_eq!(quote_ident_mysql(""), "``");
    }

    #[test]
    fn quote_ident_mysql_special_chars() {
        assert_eq!(quote_ident_mysql("table; DROP"), "`table; DROP`");
    }
}
