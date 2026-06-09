//! The MSSQL [`Sink`] implementation — connection pool, transaction-wrapped
//! multi-row `INSERT` with 2100-parameter auto-splitting, and row-isolation
//! partial-failure handling for DLQ routing.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use faucet_core::check::{CheckContext, CheckReport, Probe};
use faucet_core::{FaucetError, RowOutcome, Sink};
use serde_json::Value;
use tiberius::ToSql;

use faucet_common_mssql::{MssqlPool, MssqlPooledConnection, build_pool, quote_ident_mssql};

use crate::config::{MssqlColumnMapping, MssqlSinkConfig};
use crate::encode::{
    BoundParam, auto_row_params, build_insert_sql, max_rows_per_insert, resolve_insert_columns,
};

/// Microsoft SQL Server sink.
pub struct MssqlSink {
    config: MssqlSinkConfig,
    pool: MssqlPool,
    table_quoted: String,
    /// Cached writable (non-IDENTITY) columns for `auto_columns` mode.
    columns_cache: Mutex<Option<Vec<String>>>,
}

impl MssqlSink {
    /// Connect, validate, build the pool, and (in `json_column` + `create_table`
    /// mode) create the table if it doesn't exist.
    pub async fn new(config: MssqlSinkConfig) -> Result<Self, FaucetError> {
        config.validate()?;
        let table_quoted = quote_table(&config.table)?;
        let pool = build_pool(&config.connection, config.max_connections).await?;

        let sink = Self {
            config,
            pool,
            table_quoted,
            columns_cache: Mutex::new(None),
        };
        sink.maybe_create_table().await?;
        Ok(sink)
    }

    fn timeout(&self) -> Option<Duration> {
        match self.config.statement_timeout_secs {
            0 => None,
            secs => Some(Duration::from_secs(secs)),
        }
    }

    async fn maybe_create_table(&self) -> Result<(), FaucetError> {
        if !self.config.create_table {
            return Ok(());
        }
        let MssqlColumnMapping::JsonColumn { column } = &self.config.column_mapping else {
            return Ok(()); // validated: create_table only with json_column
        };
        let col = quote_ident_mssql(column)?;
        let sql = format!(
            "IF OBJECT_ID(N'{}', N'U') IS NULL \
             CREATE TABLE {} (id BIGINT IDENTITY(1,1) PRIMARY KEY, {} NVARCHAR(MAX))",
            self.config.table.replace('\'', "''"),
            self.table_quoted,
            col
        );
        let mut conn = self.checkout().await?;
        conn.simple_query(sql.as_str())
            .await
            .map_err(|e| FaucetError::Sink(format!("MSSQL create_table failed: {e}")))?
            .into_results()
            .await
            .map_err(|e| FaucetError::Sink(format!("MSSQL create_table failed: {e}")))?;
        Ok(())
    }

    async fn checkout(&self) -> Result<MssqlPooledConnection<'_>, FaucetError> {
        self.pool
            .get()
            .await
            .map_err(|e| FaucetError::Sink(format!("MSSQL pool checkout failed: {e}")))
    }

    /// Writable (non-IDENTITY) table columns, discovered once and cached.
    async fn insertable_columns(&self) -> Result<Vec<String>, FaucetError> {
        if let Some(cols) = self.columns_cache.lock().expect("columns mutex").clone() {
            return Ok(cols);
        }
        let cols = self.discover_columns().await?;
        *self.columns_cache.lock().expect("columns mutex") = Some(cols.clone());
        Ok(cols)
    }

    async fn discover_columns(&self) -> Result<Vec<String>, FaucetError> {
        let mut conn = self.checkout().await?;
        let table: &str = &self.config.table;
        let rows = conn
            .query(
                "SELECT c.name AS name FROM sys.columns c \
                 WHERE c.object_id = OBJECT_ID(@P1) AND c.is_identity = 0 \
                 ORDER BY c.column_id",
                &[&table],
            )
            .await
            .map_err(|e| FaucetError::Sink(format!("MSSQL column discovery failed: {e}")))?
            .into_first_result()
            .await
            .map_err(|e| FaucetError::Sink(format!("MSSQL column discovery failed: {e}")))?;

        let mut cols = Vec::with_capacity(rows.len());
        for row in &rows {
            if let Some(name) = row.get::<&str, _>("name") {
                cols.push(name.to_string());
            }
        }
        if cols.is_empty() {
            return Err(FaucetError::Sink(format!(
                "MSSQL table '{}' has no writable columns or does not exist",
                self.config.table
            )));
        }
        Ok(cols)
    }

    /// Resolve the column list + per-row owned params for one chunk.
    /// Returns `None` when there is nothing to insert (e.g. auto_columns with no
    /// matching keys).
    async fn prepare_chunk(
        &self,
        chunk: &[Value],
    ) -> Result<Option<(Vec<String>, Vec<Vec<BoundParam>>)>, FaucetError> {
        match &self.config.column_mapping {
            MssqlColumnMapping::JsonColumn { column } => {
                let cols = vec![column.clone()];
                let rows: Vec<Vec<BoundParam>> = chunk
                    .iter()
                    .map(|r| {
                        serde_json::to_string(r)
                            .map(|s| vec![BoundParam::Str(s)])
                            .map_err(|e| {
                                FaucetError::Sink(format!(
                                    "MSSQL json_column: failed to serialize record to JSON: {e}"
                                ))
                            })
                    })
                    .collect::<Result<_, _>>()?;
                Ok(Some((cols, rows)))
            }
            MssqlColumnMapping::AutoColumns { on_unknown_field } => {
                let insertable = self.insertable_columns().await?;
                let cols = resolve_insert_columns(&insertable, chunk, *on_unknown_field)?;
                if cols.is_empty() {
                    return Ok(None);
                }
                let rows: Vec<Vec<BoundParam>> =
                    chunk.iter().map(|r| auto_row_params(r, &cols)).collect();
                Ok(Some((cols, rows)))
            }
        }
    }

    /// Insert rows within an **already-open** transaction — caller owns
    /// `BEGIN TRAN`/`COMMIT TRAN`/`ROLLBACK TRAN`.  Splits into ≤2100-param
    /// sub-INSERTs but does NOT issue any transaction-control statements.
    ///
    /// Used by `write_batch_idempotent` so the data INSERTs and the commit-token
    /// MERGE share one externally-managed transaction.
    ///
    /// Returns `Err((error, timed_out))`. When `timed_out` is `true` the `exec`
    /// future was dropped mid-TDS, leaving the connection desynced — the caller
    /// must NOT issue ROLLBACK on it (mirrors `insert_chunk`).
    async fn insert_rows_no_txn(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        cols: &[String],
        rows: &[Vec<BoundParam>],
    ) -> Result<usize, (FaucetError, bool)> {
        if rows.is_empty() {
            return Ok(0);
        }
        let cols_quoted: Vec<String> = cols
            .iter()
            .map(|c| quote_ident_mssql(c))
            .collect::<Result<_, _>>()
            .map_err(|e| (e, false))?;
        let per_insert = max_rows_per_insert(cols_quoted.len());
        for sub in rows.chunks(per_insert) {
            let sql = build_insert_sql(&self.table_quoted, &cols_quoted, sub.len());
            let owned: Vec<&BoundParam> = sub.iter().flatten().collect();
            let refs: Vec<&dyn ToSql> = owned.iter().map(|p| p.as_tosql()).collect();
            let exec = async {
                conn.execute(sql.as_str(), &refs)
                    .await
                    .map(|_| ())
                    .map_err(|e| FaucetError::Sink(format!("MSSQL insert failed: {e}")))
            };
            // On timeout the `exec` future is dropped mid-TDS, desyncing the
            // connection — the caller must NOT issue ROLLBACK on it (mirrors
            // `insert_chunk`).
            let (result, timed_out) = match self.timeout() {
                Some(t) => match tokio::time::timeout(t, exec).await {
                    Ok(inner) => (inner, false),
                    Err(_) => (Err(FaucetError::Sink("MSSQL insert timed out".into())), true),
                },
                None => (exec.await, false),
            };
            if let Err(e) = result {
                return Err((e, timed_out));
            }
        }
        Ok(rows.len())
    }

    /// Ensure the per-sink commit-token watermark table exists.
    /// Uses `IF OBJECT_ID … IS NULL CREATE TABLE` so it is idempotent.
    async fn ensure_commit_table(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
    ) -> Result<(), FaucetError> {
        // NVARCHAR(450) is the maximum SQL Server index key byte budget (900 B /
        // 2 bytes-per-char = 450 chars). The table / column names are the fixed
        // constants — no user-controlled input in this string.
        let sql = format!(
            "IF OBJECT_ID(N'{tbl}', N'U') IS NULL \
             CREATE TABLE [{tbl}] ([scope] NVARCHAR(450) PRIMARY KEY, \
             [token] NVARCHAR(32) NOT NULL, \
             [updated_at] DATETIME2 DEFAULT SYSUTCDATETIME())",
            tbl = faucet_core::idempotency::COMMIT_TOKEN_TABLE,
        );
        control(conn, &sql).await
    }

    /// Insert one chunk, splitting into ≤2100-parameter sub-INSERTs wrapped in a
    /// single transaction (when `transaction_per_batch`). Returns rows inserted.
    async fn insert_chunk(
        &self,
        conn: &mut MssqlPooledConnection<'_>,
        cols: &[String],
        rows: &[Vec<BoundParam>],
    ) -> Result<usize, FaucetError> {
        if rows.is_empty() {
            return Ok(0);
        }
        let cols_quoted: Vec<String> = cols
            .iter()
            .map(|c| quote_ident_mssql(c))
            .collect::<Result<_, _>>()?;
        let per_insert = max_rows_per_insert(cols_quoted.len());

        // Wrap the chunk in a transaction when configured, OR whenever it spans
        // more than one ≤2100-param sub-INSERT. Under autocommit a multi-sub
        // chunk commits each sub-INSERT independently, so a later failure leaves
        // earlier sub-INSERTs committed — and both the batch-level transient
        // retry (`write_batch`) and the per-row isolation (`write_batch_partial`)
        // re-run the whole chunk, duplicating those committed rows (audit #146
        // H6). Forcing a transaction makes the chunk atomic so re-running is safe.
        let txn = self.config.transaction_per_batch || rows.len() > per_insert;
        if txn {
            control(conn, "BEGIN TRAN").await?;
        }

        for sub in rows.chunks(per_insert) {
            let sql = build_insert_sql(&self.table_quoted, &cols_quoted, sub.len());
            let owned: Vec<&BoundParam> = sub.iter().flatten().collect();
            let refs: Vec<&dyn ToSql> = owned.iter().map(|p| p.as_tosql()).collect();

            let exec = async {
                conn.execute(sql.as_str(), &refs)
                    .await
                    .map(|_| ())
                    .map_err(|e| FaucetError::Sink(format!("MSSQL insert failed: {e}")))
            };
            // Track whether the failure was a *timeout* specifically. On timeout
            // the `exec` future is dropped mid-TDS, leaving an unread response on
            // the wire — the connection is desynced and must NOT be reused (the
            // pool helper's contract is "drop it"). Issuing ROLLBACK on it would
            // run on a corrupt stream. A *normal* error leaves the connection in
            // sync, so ROLLBACK is safe and releases the transaction promptly.
            let (result, timed_out) = match self.timeout() {
                Some(t) => match tokio::time::timeout(t, exec).await {
                    Ok(inner) => (inner, false),
                    Err(_) => (
                        Err(FaucetError::Sink("MSSQL insert timed out".into())),
                        true,
                    ),
                },
                None => (exec.await, false),
            };
            if let Err(e) = result {
                if txn && !timed_out {
                    let _ = control(conn, "ROLLBACK TRAN").await;
                }
                return Err(e);
            }
        }

        if txn {
            control(conn, "COMMIT TRAN").await?;
        }
        Ok(rows.len())
    }
}

/// Run a transaction-control statement and drain its (empty) result.
async fn control(conn: &mut MssqlPooledConnection<'_>, stmt: &str) -> Result<(), FaucetError> {
    conn.simple_query(stmt)
        .await
        .map_err(|e| FaucetError::Sink(format!("MSSQL {stmt} failed: {e}")))?
        .into_results()
        .await
        .map_err(|e| FaucetError::Sink(format!("MSSQL {stmt} failed: {e}")))?;
    Ok(())
}

/// Quote a (possibly schema-qualified) table name: `dbo.events` → `[dbo].[events]`.
fn quote_table(table: &str) -> Result<String, FaucetError> {
    let parts: Vec<String> = table
        .split('.')
        .map(quote_ident_mssql)
        .collect::<Result<_, _>>()?;
    Ok(parts.join("."))
}

/// Heuristic for transient errors that warrant a batch-level retry / outer-Err
/// propagation rather than per-row DLQ isolation.
fn is_transient_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    m.contains("deadlock")
        || m.contains("timed out")
        || m.contains("timeout")
        || m.contains("connection")
        || m.contains("transport")
        || m.contains("link failure")
        || m.contains("1205")
}

const TRANSIENT_RETRIES: usize = 3;

#[async_trait]
impl Sink for MssqlSink {
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }
        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut total = 0usize;
        for chunk in chunks {
            let Some((cols, rows)) = self.prepare_chunk(chunk).await? else {
                continue;
            };
            // Bounded retry on transient (deadlock / lock-timeout) errors.
            let mut attempt = 0;
            loop {
                let mut conn = self.checkout().await?;
                match self.insert_chunk(&mut conn, &cols, &rows).await {
                    Ok(n) => {
                        total += n;
                        break;
                    }
                    Err(e) if is_transient_error(&e.to_string()) && attempt < TRANSIENT_RETRIES => {
                        attempt += 1;
                        let backoff = Duration::from_millis(50 * (1 << attempt));
                        tracing::warn!(attempt, error = %e, "MSSQL transient error; retrying batch");
                        tokio::time::sleep(backoff).await;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        tracing::info!(table = %self.config.table, rows = total, "MSSQL write complete");
        Ok(total)
    }

    async fn write_batch_partial(&self, records: &[Value]) -> Result<Vec<RowOutcome>, FaucetError> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut outcomes: Vec<RowOutcome> = Vec::with_capacity(records.len());
        for chunk in chunks {
            let Some((cols, rows)) = self.prepare_chunk(chunk).await? else {
                // Nothing to insert for this chunk (no matching columns): the
                // rows were effectively dropped per on_unknown_field; report Ok.
                outcomes.extend(chunk.iter().map(|_| Ok(())));
                continue;
            };

            let mut conn = self.checkout().await?;
            match self.insert_chunk(&mut conn, &cols, &rows).await {
                Ok(_) => outcomes.extend(chunk.iter().map(|_| Ok(()))),
                Err(e) if is_transient_error(&e.to_string()) => {
                    // Infra/transient — not row-specific. Propagate so the
                    // pipeline's on_batch_error policy decides.
                    return Err(e);
                }
                Err(_) if !self.config.isolate_row_failures => {
                    // One bad row fails the whole batch (caller's choice).
                    return Err(FaucetError::Sink(
                        "MSSQL batch insert failed and isolate_row_failures is disabled".into(),
                    ));
                }
                Err(_) => {
                    // Row-isolate: retry each row alone to find the offender.
                    for (i, row) in rows.iter().enumerate() {
                        let single = std::slice::from_ref(row);
                        let single_cols = cols.clone();
                        match self.insert_chunk(&mut conn, &single_cols, single).await {
                            Ok(_) => outcomes.push(Ok(())),
                            Err(e) if is_transient_error(&e.to_string()) => return Err(e),
                            Err(e) => {
                                tracing::warn!(row = i, error = %e, "MSSQL row rejected; routing to DLQ");
                                outcomes.push(Err(e));
                            }
                        }
                    }
                }
            }
        }
        Ok(outcomes)
    }

    async fn flush(&self) -> Result<(), FaucetError> {
        Ok(())
    }

    fn supports_idempotent_writes(&self) -> bool {
        true
    }

    async fn last_committed_token(&self, scope: &str) -> Result<Option<String>, FaucetError> {
        let mut conn = self.checkout().await?;
        self.ensure_commit_table(&mut conn).await?;
        let scope_owned = scope.to_string();
        let rows = conn
            .query(
                &format!(
                    "SELECT [token] FROM [{}] WHERE [scope] = @P1",
                    faucet_core::idempotency::COMMIT_TOKEN_TABLE
                ),
                &[&scope_owned],
            )
            .await
            .map_err(|e| FaucetError::Sink(format!("MSSQL token read failed: {e}")))?
            .into_first_result()
            .await
            .map_err(|e| FaucetError::Sink(format!("MSSQL token read failed: {e}")))?;
        Ok(rows
            .first()
            .and_then(|r| r.get::<&str, _>("token"))
            .map(str::to_string))
    }

    async fn write_batch_idempotent(
        &self,
        records: &[Value],
        scope: &str,
        token: &str,
    ) -> Result<usize, FaucetError> {
        let mut conn = self.checkout().await?;
        self.ensure_commit_table(&mut conn).await?;
        control(&mut conn, "BEGIN TRAN").await?;

        let written = match self.prepare_chunk(records).await {
            Ok(Some((cols, rows))) => match self.insert_rows_no_txn(&mut conn, &cols, &rows).await {
                Ok(n) => n,
                Err((e, timed_out)) => {
                    // Desynced connection on timeout — ROLLBACK would run on a
                    // corrupt stream (mirrors insert_chunk). Drop the conn instead.
                    if !timed_out {
                        let _ = control(&mut conn, "ROLLBACK TRAN").await;
                    }
                    return Err(e);
                }
            },
            Ok(None) => 0,
            Err(e) => {
                let _ = control(&mut conn, "ROLLBACK TRAN").await;
                return Err(e);
            }
        };

        // UPSERT the commit token atomically with the data rows.
        let merge = format!(
            "MERGE [{tbl}] AS t \
             USING (SELECT @P1 AS [scope], @P2 AS [token]) AS s \
             ON t.[scope] = s.[scope] \
             WHEN MATCHED THEN UPDATE SET t.[token] = s.[token], t.[updated_at] = SYSUTCDATETIME() \
             WHEN NOT MATCHED THEN INSERT ([scope], [token]) VALUES (s.[scope], s.[token]);",
            tbl = faucet_core::idempotency::COMMIT_TOKEN_TABLE,
        );
        let (scope_owned, token_owned) = (scope.to_string(), token.to_string());
        let refs: Vec<&dyn ToSql> = vec![&scope_owned, &token_owned];
        if let Err(e) = conn.execute(merge.as_str(), &refs).await {
            let _ = control(&mut conn, "ROLLBACK TRAN").await;
            return Err(FaucetError::Sink(format!("MSSQL token merge failed: {e}")));
        }

        control(&mut conn, "COMMIT TRAN").await?;
        Ok(written)
    }

    fn config_schema(&self) -> Value {
        serde_json::to_value(faucet_core::schema_for!(MssqlSinkConfig))
            .expect("schema serialization")
    }

    fn connector_name(&self) -> &'static str {
        "mssql"
    }

    fn dataset_uri(&self) -> String {
        let conn = self
            .config
            .connection
            .connection_url
            .as_deref()
            .or(self.config.connection.connection_string.as_deref())
            .unwrap_or("");
        format!(
            "{}?table={}",
            faucet_core::redact_uri_credentials(conn),
            self.config.table
        )
    }

    async fn check(&self, ctx: &CheckContext) -> Result<CheckReport, FaucetError> {
        let started = std::time::Instant::now();
        let probe = match tokio::time::timeout(ctx.timeout, self.pool.get()).await {
            Ok(Ok(_conn)) => Probe::pass("connect", started.elapsed()),
            Ok(Err(e)) => Probe::fail_hint(
                "connect",
                started.elapsed(),
                e.to_string(),
                "check connection_url / credentials / TLS / that the server is reachable",
            ),
            Err(_) => Probe::fail_hint(
                "connect",
                started.elapsed(),
                "timed out",
                "check connection_url / credentials / TLS / that the server is reachable",
            ),
        };
        Ok(CheckReport::single(probe))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // dataset_uri test is skipped: MssqlSink::new() requires a live pool
    // (connects to SQL Server in new()), and no offline constructor exists.

    #[test]
    fn quote_table_handles_schema_qualified() {
        assert_eq!(quote_table("dbo.events").unwrap(), "[dbo].[events]");
        assert_eq!(quote_table("events").unwrap(), "[events]");
        assert_eq!(
            quote_table("my.sales.events").unwrap(),
            "[my].[sales].[events]"
        );
    }

    #[test]
    fn idempotency_constant_names() {
        // The commit table and column constants used in ensure_commit_table,
        // last_committed_token, and write_batch_idempotent must match the
        // canonical values from faucet_core::idempotency.
        assert_eq!(
            faucet_core::idempotency::COMMIT_TOKEN_TABLE,
            "_faucet_commit_token",
            "COMMIT_TOKEN_TABLE name changed — update DDL and queries"
        );
        assert_eq!(
            faucet_core::idempotency::COMMIT_TOKEN_SCOPE_COL,
            "scope",
            "COMMIT_TOKEN_SCOPE_COL name changed — update DDL and queries"
        );
        assert_eq!(
            faucet_core::idempotency::COMMIT_TOKEN_TOKEN_COL,
            "token",
            "COMMIT_TOKEN_TOKEN_COL name changed — update DDL and queries"
        );
    }

    #[test]
    fn transient_classifier() {
        assert!(is_transient_error(
            "Transaction (Process ID 55) was deadlocked"
        ));
        assert!(is_transient_error(
            "Lock request time out period exceeded (1205)"
        ));
        assert!(is_transient_error("connection reset by peer"));
        assert!(!is_transient_error("Violation of PRIMARY KEY constraint"));
        assert!(!is_transient_error(
            "Conversion failed when converting date"
        ));
    }
}
