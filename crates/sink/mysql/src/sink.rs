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

/// Build the `ON DUPLICATE KEY UPDATE …` tail for an upsert INSERT.
///
/// MySQL's `ON DUPLICATE KEY UPDATE` does not name a conflict target — it
/// relies on the table's existing PRIMARY or UNIQUE key. Non-key columns are
/// set from `VALUES(col)`. If every column is a key column there is nothing to
/// update, so a self-assignment no-op on the first key column is emitted to
/// keep the statement syntactically valid.
fn on_duplicate_clause(key: &[String], all_cols: &[String]) -> String {
    let updates: Vec<String> = all_cols
        .iter()
        .filter(|c| !key.iter().any(|k| k == *c))
        .map(|c| {
            let q = quote_ident_mysql(c);
            format!("{q} = VALUES({q})")
        })
        .collect();
    if updates.is_empty() {
        let q = quote_ident_mysql(&key[0]);
        format!("ON DUPLICATE KEY UPDATE {q} = {q}")
    } else {
        format!("ON DUPLICATE KEY UPDATE {}", updates.join(", "))
    }
}

impl MysqlSink {
    /// Create a new MySQL sink. Establishes a connection pool.
    pub async fn new(config: MysqlSinkConfig) -> Result<Self, FaucetError> {
        config.write.validate()?;
        if !matches!(config.write.write_mode, faucet_core::WriteMode::Append)
            && !matches!(config.column_mapping, MysqlColumnMapping::AutoMap)
        {
            return Err(FaucetError::Config(
                "mysql sink: write_mode upsert/delete requires column_mapping: auto_map \
                 (key columns must be real columns, not inside a JSON blob)"
                    .into(),
            ));
        }

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

    /// Core auto-map insert logic, optionally appending an `ON DUPLICATE KEY
    /// UPDATE …` clause when `conflict_key` is `Some`.
    ///
    /// Discovers column names from `INFORMATION_SCHEMA.COLUMNS` and maps
    /// top-level JSON fields to columns. Executes on the provided connection
    /// (a bare pool connection for `write_batch`, or `&mut *tx` for
    /// transactional paths). Uses sub-chunked multi-row INSERTs.
    ///
    /// When `conflict_key` is `Some(key)`, each sub-chunk's INSERT is given an
    /// `ON DUPLICATE KEY UPDATE …` tail so it upserts by the existing PRIMARY
    /// or UNIQUE key (last-write-wins within the batch is handled by the
    /// planner's dedup, so a single sub-chunk never double-hits the same
    /// conflict target).
    async fn insert_auto_map_with_conflict(
        &self,
        conn: &mut MySqlConnection,
        records: &[Value],
        conflict_key: Option<&[String]>,
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
            let base_query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                quote_ident_mysql(&self.config.table_name),
                col_names.join(", "),
                value_tuples.join(", ")
            );
            let query = match conflict_key {
                Some(key) => format!(
                    "{base_query} {}",
                    on_duplicate_clause(key, &insert_columns)
                ),
                None => base_query,
            };

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

    /// Auto-map insert with plain append semantics (no `ON DUPLICATE KEY`
    /// clause). Thin wrapper over
    /// [`insert_auto_map_with_conflict`](Self::insert_auto_map_with_conflict)
    /// so the append path and `write_batch_idempotent` keep their original
    /// signature.
    async fn insert_auto_map(
        &self,
        conn: &mut MySqlConnection,
        records: &[Value],
    ) -> Result<usize, FaucetError> {
        self.insert_auto_map_with_conflict(conn, records, None)
            .await
    }

    /// Delete rows whose key columns match any of `deletes`, using
    /// `DELETE FROM t WHERE (k1, …) IN ((?, …), …)`, chunked at MySQL's
    /// 65535-placeholder limit. Runs inside the caller's transaction.
    async fn delete_by_keys(
        &self,
        conn: &mut MySqlConnection,
        deletes: &[faucet_core::KeyTuple],
    ) -> Result<usize, FaucetError> {
        if deletes.is_empty() {
            return Ok(0);
        }
        let key = &self.config.write.key;
        let table_ref = quote_ident_mysql(&self.config.table_name);
        let col_list = key
            .iter()
            .map(|k| quote_ident_mysql(k))
            .collect::<Vec<_>>()
            .join(", ");

        const MAX_MYSQL_PARAMS: usize = 65535;
        let per = (MAX_MYSQL_PARAMS / key.len().max(1)).max(1);
        let mut total = 0usize;

        for chunk in deletes.chunks(per) {
            let tuples: Vec<String> = chunk
                .iter()
                .map(|_| format!("({})", vec!["?"; key.len()].join(", ")))
                .collect();
            let sql = format!(
                "DELETE FROM {table_ref} WHERE ({col_list}) IN ({})",
                tuples.join(", ")
            );
            let mut q = sqlx::query(&sql);
            for kt in chunk {
                for (_, v) in &kt.0 {
                    // Bind native MySQL types — same logic as in the INSERT path.
                    q = match v {
                        Value::Null => q.bind(None::<String>),
                        Value::Bool(b) => q.bind(*b),
                        Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                q.bind(i)
                            } else if let Some(f) = n.as_f64() {
                                q.bind(f)
                            } else {
                                q.bind(n.to_string())
                            }
                        }
                        Value::String(s) => q.bind(s.clone()),
                        other => q.bind(other.to_string()),
                    };
                }
            }
            let res = q
                .execute(&mut *conn)
                .await
                .map_err(|e| FaucetError::Sink(format!("MySQL delete failed: {e}")))?;
            total += res.rows_affected() as usize;
        }
        Ok(total)
    }

    /// Apply a planned upsert/delete batch inside one `BEGIN`/`COMMIT`
    /// transaction. Upserts and deletes are wrapped together so they commit
    /// atomically.
    async fn apply_plan(&self, plan: &faucet_core::WritePlan) -> Result<usize, FaucetError> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL transaction begin failed: {e}")))?;

        let mut affected = 0usize;
        if !plan.upserts.is_empty() {
            affected += self
                .insert_auto_map_with_conflict(
                    &mut tx,
                    &plan.upserts,
                    Some(&self.config.write.key),
                )
                .await?;
        }
        if !plan.deletes.is_empty() {
            affected += self.delete_by_keys(&mut tx, &plan.deletes).await?;
        }

        tx.commit()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL transaction commit failed: {e}")))?;
        Ok(affected)
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

    fn supported_write_modes(&self) -> &'static [faucet_core::WriteMode] {
        &[
            faucet_core::WriteMode::Append,
            faucet_core::WriteMode::Upsert,
            faucet_core::WriteMode::Delete,
        ]
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

        // Non-append modes: plan the writes and apply atomically.
        if !matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "mysql {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            return self.apply_plan(&plan).await;
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

    /// Write a batch and report per-row outcomes.
    ///
    /// In append mode this delegates to [`write_batch`](Self::write_batch) and
    /// maps a single success onto an all-`Ok(())` vector (the trait default).
    /// In upsert/delete mode the good rows are applied (upserts + deletes), and
    /// only the rows whose key could not be extracted (missing / null key) are
    /// reported as `Err` so the pipeline routes them to the DLQ per-row instead
    /// of sending the whole page.
    async fn write_batch_partial(
        &self,
        records: &[Value],
    ) -> Result<Vec<faucet_core::RowOutcome>, FaucetError> {
        if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            self.write_batch(records).await?;
            return Ok(records.iter().map(|_| Ok(())).collect());
        }

        let plan = faucet_core::plan_writes(records, &self.config.write);
        self.apply_plan(&plan).await?;

        let mut outcomes: Vec<faucet_core::RowOutcome> = records.iter().map(|_| Ok(())).collect();
        for (idx, msg) in &plan.failed {
            outcomes[*idx] = Err(FaucetError::Sink(format!(
                "mysql {}: {msg}",
                self.config.write.write_mode.as_str()
            )));
        }
        Ok(outcomes)
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

        // For upsert/delete modes, plan the page before opening the transaction
        // so a key-extraction failure aborts without leaving an open tx.
        let plan = if matches!(self.config.write.write_mode, faucet_core::WriteMode::Append) {
            None
        } else {
            let plan = faucet_core::plan_writes(records, &self.config.write);
            if let Some((idx, msg)) = plan.failed.first() {
                return Err(FaucetError::Sink(format!(
                    "mysql {}: row {idx}: {msg}",
                    self.config.write.write_mode.as_str()
                )));
            }
            Some(plan)
        };

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| FaucetError::Sink(format!("MySQL transaction begin failed: {e}")))?;

        // Data write and the commit-token upsert share ONE transaction so the
        // page is committed atomically with its watermark. For upsert/delete the
        // planned upserts/deletes commit together with the watermark in this same
        // tx (no nested tx — the helpers run on this transaction's connection).
        let written = match &plan {
            Some(plan) => {
                let mut affected = 0usize;
                if !plan.upserts.is_empty() {
                    affected += self
                        .insert_auto_map_with_conflict(
                            &mut tx,
                            &plan.upserts,
                            Some(&self.config.write.key),
                        )
                        .await?;
                }
                if !plan.deletes.is_empty() {
                    affected += self.delete_by_keys(&mut tx, &plan.deletes).await?;
                }
                affected
            }
            None => match &self.config.column_mapping {
                MysqlColumnMapping::Json { column } => {
                    self.insert_json(&mut tx, records, column).await?
                }
                MysqlColumnMapping::AutoMap => self.insert_auto_map(&mut tx, records).await?,
            },
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
        assert_eq!(
            faucet_core::idempotency::COMMIT_TOKEN_TABLE,
            "_faucet_commit_token"
        );
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

    #[test]
    fn mysql_on_duplicate_clause() {
        let clause = on_duplicate_clause(
            &["id".to_string()],
            &["id".to_string(), "name".to_string()],
        );
        assert_eq!(clause, "ON DUPLICATE KEY UPDATE `name` = VALUES(`name`)");
    }

    #[test]
    fn mysql_on_duplicate_all_keys_self_assign() {
        let clause = on_duplicate_clause(&["id".to_string()], &["id".to_string()]);
        assert_eq!(clause, "ON DUPLICATE KEY UPDATE `id` = `id`");
    }

    #[test]
    fn mysql_on_duplicate_composite_key_partial_update() {
        let clause = on_duplicate_clause(
            &["a".to_string(), "b".to_string()],
            &["a".to_string(), "b".to_string(), "v".to_string()],
        );
        assert_eq!(clause, "ON DUPLICATE KEY UPDATE `v` = VALUES(`v`)");
    }
}
