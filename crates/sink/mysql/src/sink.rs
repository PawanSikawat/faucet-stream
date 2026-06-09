//! MySQL sink implementation.

use crate::config::{MysqlColumnMapping, MysqlSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use serde_json::Value;
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlConnection, MySqlPool, Row};

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
    ///
    /// Executes on the provided connection (a bare pool connection for
    /// `write_batch`, or a `&mut *tx` transaction for `write_batch_idempotent`).
    /// Uses a single multi-row INSERT for efficiency.
    async fn insert_json(
        &self,
        conn: &mut MySqlConnection,
        records: &[Value],
        column: &str,
    ) -> Result<usize, FaucetError> {
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

        q.execute(&mut *conn)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL insert failed: {e}")))?;

        Ok(records.len())
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers column names from INFORMATION_SCHEMA and maps top-level JSON
    /// fields to columns. Executes on the provided connection (a bare pool
    /// connection for `write_batch`, or a `&mut *tx` transaction for
    /// `write_batch_idempotent`). Uses a single multi-row INSERT.
    async fn insert_auto_map(
        &self,
        conn: &mut MySqlConnection,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names from the table.
        let columns: Vec<String> = sqlx::query(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ? AND TABLE_SCHEMA = DATABASE() ORDER BY ORDINAL_POSITION"
        )
        .bind(&self.config.table_name)
        .fetch_all(&mut *conn)
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

        // Pre-validate all records and collect matched column values. The
        // INSERT column set is the UNION of table columns present in ANY record
        // (in declared table order), not just the first record's keys —
        // otherwise a field present only in a later record of the batch would be
        // silently dropped (audit #146 H1). A row missing a unioned column binds
        // SQL NULL.
        let mut matched_rows: Vec<Vec<(&String, &Value)>> = Vec::with_capacity(records.len());
        let mut used: std::collections::HashSet<&str> = std::collections::HashSet::new();

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

            for (c, _) in &matching {
                used.insert(c.as_str());
            }
            matched_rows.push(matching);
        }

        if matched_rows.is_empty() {
            return Ok(0);
        }

        // Table columns (in declared order) that appear in at least one record.
        let insert_columns: Vec<String> = columns
            .iter()
            .filter(|c| used.contains(c.as_str()))
            .cloned()
            .collect();

        let num_cols = insert_columns.len();
        let num_rows = matched_rows.len();
        let col_names: Vec<String> = insert_columns
            .iter()
            .map(|c| quote_ident_mysql(c))
            .collect();

        // MySQL caps prepared-statement placeholders at 65535. A multi-row
        // INSERT binds `rows × num_cols`, so a wide table at a large batch_size
        // overflows and fails at runtime; split into sub-INSERTs of at most
        // floor(MAX / num_cols) rows (audit #146 H14 — postgres/sqlite/mssql
        // already sub-chunk this way).
        const MAX_MYSQL_PARAMS: usize = 65535;
        let max_rows_per_insert = (MAX_MYSQL_PARAMS / num_cols).max(1);

        for sub in matched_rows.chunks(max_rows_per_insert) {
            // Build multi-row VALUES clause: (?, ?), (?, ?), ...
            let row_placeholder = format!("({})", vec!["?"; num_cols].join(", "));
            let value_tuples: Vec<&str> =
                (0..sub.len()).map(|_| row_placeholder.as_str()).collect();

            let query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident_mysql(&self.config.table_name),
                col_names.join(", "),
                value_tuples.join(", ")
            );

            let mut q = sqlx::query(&query);
            for matched in sub {
                for col in &insert_columns {
                    let val = matched.iter().find(|(c, _)| *c == col).map(|(_, v)| *v);
                    // Bind native MySQL types. Binding every value as a JSON string
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

            q.execute(&mut *conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("MySQL insert failed: {e}")))?;
        }

        Ok(num_rows)
    }

    /// Create the commit-token watermark table if it does not yet exist.
    ///
    /// The table holds one row per pipeline scope (state key). MySQL requires a
    /// fixed-length column as the primary key, so `scope` is `VARCHAR(255)` and
    /// `token` is `VARCHAR(32)` (tokens are 20-char ulids, 32 chars is ample).
    async fn ensure_commit_table(&self) -> Result<(), FaucetError> {
        let sql = format!(
            "CREATE TABLE IF NOT EXISTS {t} ({s} VARCHAR(255) PRIMARY KEY, {k} VARCHAR(32) NOT NULL, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
            t = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&sql)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL commit-table create failed: {e}")))?;
        Ok(())
    }
}

#[async_trait]
impl faucet_core::Sink for MysqlSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(MysqlSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?table={}",
            faucet_core::redact_uri_credentials(&self.config.connection_url),
            self.config.table_name
        )
    }

    /// Preflight connectivity probe (`faucet doctor`).
    ///
    /// Acquires a connection from the existing pool and runs `SELECT 1`. This
    /// is non-mutating and idempotent — it validates that the database is
    /// reachable and the credentials are accepted without writing anything.
    async fn check(
        &self,
        ctx: &faucet_core::check::CheckContext,
    ) -> Result<faucet_core::check::CheckReport, FaucetError> {
        use faucet_core::check::{CheckReport, Probe};

        let started = std::time::Instant::now();
        let probe =
            match tokio::time::timeout(ctx.timeout, sqlx::query("SELECT 1").execute(&self.pool))
                .await
            {
                Ok(Ok(_)) => Probe::pass("auth", started.elapsed()),
                Ok(Err(e)) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    e.to_string(),
                    "check connection_url / credentials / that the database is reachable",
                ),
                Err(_) => Probe::fail_hint(
                    "auth",
                    started.elapsed(),
                    "timed out",
                    "check connection_url / credentials / that the database is reachable",
                ),
            };
        Ok(CheckReport::single(probe))
    }

    /// Write records to MySQL.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size` rows and
    /// each chunk is sent as a separate multi-row `INSERT`. When
    /// `config.batch_size == 0`, the entire slice is sent in a single
    /// multi-row `INSERT` — useful when upstream `StreamPage`s are already
    /// sized for MySQL's `max_allowed_packet` limit.
    ///
    /// Acquires a single connection from the pool and sends every chunk
    /// through it under autocommit (no explicit transaction), preserving the
    /// pre-refactor observable behaviour.
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut conn = self
            .pool
            .acquire()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL pool acquire failed: {e}")))?;

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
                MysqlColumnMapping::Json { column } => {
                    self.insert_json(&mut conn, chunk, column).await?
                }
                MysqlColumnMapping::AutoMap => self.insert_auto_map(&mut conn, chunk).await?,
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            "MySQL write complete"
        );
        Ok(total)
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        self.ensure_commit_table().await?;
        let sql = format!(
            "SELECT {k} FROM {t} WHERE {s} = ?",
            t = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            k = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
            s = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
        );
        let row = sqlx::query(&sql)
            .bind(scope)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL token read failed: {e}")))?;
        Ok(row.map(|r| r.get::<String, _>(0)))
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        self.ensure_commit_table().await?;
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL transaction begin failed: {e}")))?;

        let written = match &self.config.column_mapping {
            MysqlColumnMapping::Json { column } => {
                self.insert_json(&mut tx, records, column).await?
            }
            MysqlColumnMapping::AutoMap => self.insert_auto_map(&mut tx, records).await?,
        };

        let upsert = format!(
            "INSERT INTO {t} ({s}, {k}) VALUES (?, ?) ON DUPLICATE KEY UPDATE {k} = VALUES({k})",
            t = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_TABLE),
            s = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL),
            k = quote_ident_mysql(faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL),
        );
        sqlx::query(&upsert)
            .bind(scope)
            .bind(token)
            .execute(&mut *tx)
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL token upsert failed: {e}")))?;

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL transaction commit failed: {e}")))?;

        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // dataset_uri test is skipped: MysqlSink::new() requires a live pool
    // (connects to MySQL in new()), and no offline constructor exists.

    #[test]
    fn commit_token_table_is_the_shared_constant() {
        assert_eq!(faucet_core::idempotency::COMMIT_TOKEN_TABLE, "_faucet_commit_token");
    }

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
