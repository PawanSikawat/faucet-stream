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
            .max_connections(5)
            .connect(&config.connection_url)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL connection failed: {e}")))?;

        Ok(Self { config, pool })
    }

    /// Insert a batch of records using JSON column mode.
    async fn insert_json(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES (?)",
            quote_ident_mysql(&self.config.table_name),
            quote_ident_mysql(column)
        );

        let mut total = 0;
        for record in records {
            let json_str = serde_json::to_string(record)
                .map_err(|e| FaucetError::Sink(format!("failed to serialize record: {e}")))?;

            sqlx::query(&insert_sql)
                .bind(&json_str)
                .execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("MySQL insert failed: {e}")))?;

            total += 1;
        }

        Ok(total)
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers column names from INFORMATION_SCHEMA and maps
    /// top-level JSON fields to columns.
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

        let mut total = 0;
        for record in records {
            let obj = record
                .as_object()
                .ok_or_else(|| FaucetError::Sink("AutoMap requires JSON object records".into()))?;

            // Only insert keys that match existing columns.
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

            let col_names: Vec<String> =
                matching.iter().map(|(c, _)| quote_ident_mysql(c)).collect();
            let placeholders: Vec<&str> = matching.iter().map(|_| "?").collect();

            let query = format!(
                "INSERT INTO {} ({}) VALUES ({})",
                quote_ident_mysql(&self.config.table_name),
                col_names.join(", "),
                placeholders.join(", ")
            );

            let mut q = sqlx::query(&query);
            for (_, val) in &matching {
                // Bind as JSON string for MySQL compatibility.
                let s = serde_json::to_string(val).map_err(|e| {
                    FaucetError::Sink(format!("failed to serialize column value: {e}"))
                })?;
                q = q.bind(s);
            }

            q.execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("MySQL insert failed: {e}")))?;

            total += 1;
        }

        Ok(total)
    }
}

#[async_trait]
impl faucet_core::Sink for MysqlSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let mut total = 0;
        for chunk in records.chunks(self.config.batch_size) {
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
