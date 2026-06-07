//! PostgreSQL sink implementation.

use crate::config::{PostgresColumnMapping, PostgresSinkConfig};
use async_trait::async_trait;
use faucet_core::FaucetError;
use faucet_core::util::quote_ident;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

/// Render a JSON value as the text to bind for a PostgreSQL column whose
/// underlying type is `udt` (`information_schema.columns.udt_name`), or `None`
/// for SQL `NULL`.
///
/// The accompanying placeholder is emitted as `$N::<udt>`, so PostgreSQL runs
/// the destination column type's input function over this text. That makes
/// `string → timestamptz/uuid/date`, `number → int4/numeric/float8`,
/// `bool → bool`, and `json → jsonb` all work — instead of binding every value
/// as `serde_json::Value` (which sqlx encodes as `jsonb`, so an insert into any
/// non-`jsonb` column fails at runtime with *"column is of type … but
/// expression is of type jsonb"*; this was the C1 bug in audit #146).
///
/// For `json`/`jsonb` columns the value is bound as its JSON text (so a string
/// keeps its quotes and objects/arrays round-trip); the `::jsonb` cast then
/// parses it. For every other type the scalar's plain text form is bound and
/// the column's input function parses it via the cast.
fn pg_bind_text(value: Option<&Value>, udt: &str) -> Option<String> {
    match value {
        None | Some(Value::Null) => None,
        Some(v) => {
            if udt.eq_ignore_ascii_case("json") || udt.eq_ignore_ascii_case("jsonb") {
                Some(v.to_string())
            } else {
                match v {
                    Value::Bool(b) => Some(b.to_string()),
                    Value::Number(n) => Some(n.to_string()),
                    Value::String(s) => Some(s.clone()),
                    // Arrays/objects have no scalar text form for a non-JSON
                    // column; bind their JSON text so the `::<type>` cast fails
                    // loudly rather than silently coercing.
                    other => Some(other.to_string()),
                }
            }
        }
    }
}

/// Build the SQL relation reference for the configured table, optionally
/// schema-qualified.
///
/// Both the AutoMap column-discovery probe and the `INSERT` statements use this
/// single helper, so column discovery is always scoped to the *exact* relation
/// the `INSERT` targets (#146 M13). With no schema the bare quoted table name
/// resolves against the connection's `search_path`; with a schema it becomes
/// `"schema"."table"`, pinning both discovery and insert to that namespace —
/// otherwise a table of the same name in another schema pollutes the
/// AutoMap column set (duplicate / wrong columns).
fn qualified_table_ref(schema: Option<&str>, table: &str) -> String {
    match schema {
        Some(s) => format!("{}.{}", quote_ident(s), quote_ident(table)),
        None => quote_ident(table),
    }
}

/// A sink that writes JSON records to a PostgreSQL table.
pub struct PostgresSink {
    config: PostgresSinkConfig,
    pool: PgPool,
}

impl PostgresSink {
    /// Create a new PostgreSQL sink. Establishes a connection pool.
    pub async fn new(config: PostgresSinkConfig) -> Result<Self, FaucetError> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.connection_url)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL connection failed: {e}")))?;

        Ok(Self { config, pool })
    }

    /// Insert a batch of records using JSONB column mode.
    async fn insert_jsonb(&self, records: &[Value], column: &str) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Use a single INSERT with unnest for efficiency.
        let json_values: Vec<serde_json::Value> = records.to_vec();
        let query = format!(
            "INSERT INTO {} ({}) SELECT * FROM unnest($1::jsonb[])",
            qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name),
            quote_ident(column)
        );

        sqlx::query(&query)
            .bind(json_values)
            .execute(&self.pool)
            .await
            .map_err(|e| FaucetError::Sink(format!("PostgreSQL insert failed: {e}")))?;

        Ok(records.len())
    }

    /// Insert a batch of records using auto-mapped columns.
    ///
    /// Discovers each column's name *and* underlying type (`udt_name`) from the
    /// table schema and maps top-level JSON fields to columns. Each placeholder
    /// is emitted as `$N::<udt>` and the value is bound as text (see
    /// [`pg_bind_text`]), so the destination column's input function parses it —
    /// numbers, booleans, timestamps, uuids, and JSON all land in their native
    /// column types. (Previously every value was bound as `serde_json::Value`,
    /// which sqlx encodes as `jsonb`, so an insert into any non-`jsonb` column
    /// failed at runtime — audit #146 C1.) Uses a single multi-row INSERT
    /// (sub-chunked at the 65535-parameter cap) for efficiency.
    async fn insert_auto_map(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        // Get column names AND their underlying types for the *exact* relation
        // the INSERT will target. Scoping by `to_regclass(<qualified ref>)`
        // resolves the relation the same way the INSERT does — by the configured
        // schema if set, otherwise by the connection's `search_path` — so a
        // table of the same name in another schema can no longer pollute the
        // column set with duplicate/wrong columns (#146 M13). The previous query
        // filtered `information_schema.columns` by `table_name` alone (no schema
        // predicate), merging every same-named table across all schemas.
        //
        // `pg_type.typname` is the concrete type (`int4`, `timestamptz`,
        // `numeric`, `jsonb`, `uuid`, `text`, …) — identical to the old
        // `information_schema.columns.udt_name` — used as the per-placeholder
        // cast target below. `::text` casts the `name`-typed catalog columns so
        // sqlx decodes them as `String`.
        let table_ref = qualified_table_ref(self.config.schema.as_deref(), &self.config.table_name);
        let columns: Vec<(String, String)> = sqlx::query(
            "SELECT a.attname::text AS column_name, t.typname::text AS udt_name \
             FROM pg_catalog.pg_attribute a \
             JOIN pg_catalog.pg_type t ON t.oid = a.atttypid \
             WHERE a.attrelid = to_regclass($1)::oid \
               AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
        )
        .bind(&table_ref)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| FaucetError::Sink(format!("failed to query table columns: {e}")))?
        .iter()
        .map(|row| {
            (
                row.get::<String, _>("column_name"),
                row.get::<String, _>("udt_name"),
            )
        })
        .collect();

        if columns.is_empty() {
            return Err(FaucetError::Sink(format!(
                "table {table_ref} has no columns or does not exist"
            )));
        }

        // Pre-validate all records and collect matched (column, udt, value)
        // triples per record. The INSERT column set is the UNION of table
        // columns present in ANY record (in declared table order), not just the
        // first record's keys — otherwise a field present only in a later record
        // of the batch would be silently dropped (audit #146 H1). A row missing
        // a unioned column binds SQL NULL.
        let mut matched_rows: Vec<Vec<(&String, &String, &Value)>> =
            Vec::with_capacity(records.len());
        let mut used: std::collections::HashSet<&str> = std::collections::HashSet::new();

        for record in records {
            let obj = record
                .as_object()
                .ok_or_else(|| FaucetError::Sink("AutoMap requires JSON object records".into()))?;

            let matching: Vec<(&String, &String, &Value)> = columns
                .iter()
                .filter_map(|(col, udt)| obj.get(col).map(|v| (col, udt, v)))
                .collect();

            if matching.is_empty() {
                tracing::warn!(
                    record_keys = ?obj.keys().collect::<Vec<_>>(),
                    table_columns = ?columns,
                    "record has no keys matching table columns, skipping"
                );
                continue;
            }

            for (c, _, _) in &matching {
                used.insert(c.as_str());
            }
            matched_rows.push(matching);
        }

        if matched_rows.is_empty() {
            return Ok(0);
        }

        // Table columns (in declared order, with their udt) present in at least
        // one record.
        let insert_columns: Vec<(String, String)> = columns
            .iter()
            .filter(|(c, _)| used.contains(c.as_str()))
            .cloned()
            .collect();

        let num_cols = insert_columns.len();
        let num_rows = matched_rows.len();
        let col_names: Vec<String> = insert_columns.iter().map(|(c, _)| quote_ident(c)).collect();

        // PostgreSQL caps bind parameters per statement at 65535. A multi-row
        // INSERT binds `rows × num_cols` parameters, so a wide table at a large
        // batch_size can exceed it and fail at runtime (#78/#21). Split into
        // sub-INSERTs of at most floor(MAX_PARAMS / num_cols) rows.
        const MAX_PG_PARAMS: usize = 65535;
        let max_rows_per_insert = (MAX_PG_PARAMS / num_cols).max(1);

        for sub in matched_rows.chunks(max_rows_per_insert) {
            // Build multi-row VALUES clause with per-column casts so the column
            // type's input function parses the bound text:
            //   ($1::int4, $2::timestamptz), ($3::int4, $4::timestamptz), ...
            let mut value_tuples: Vec<String> = Vec::with_capacity(sub.len());
            for row_idx in 0..sub.len() {
                let start = row_idx * num_cols + 1;
                let placeholders: Vec<String> = (0..num_cols)
                    .map(|c| format!("${}::{}", start + c, insert_columns[c].1))
                    .collect();
                value_tuples.push(format!("({})", placeholders.join(", ")));
            }

            let query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                table_ref,
                col_names.join(", "),
                value_tuples.join(", ")
            );

            let mut q = sqlx::query(&query);
            for matched in sub {
                // Bind values in the fixed column order, as text matching each
                // column's type. A record missing a column that appeared in the
                // first record binds SQL NULL.
                for (col, udt) in &insert_columns {
                    let val = matched
                        .iter()
                        .find(|(c, _, _)| *c == col)
                        .map(|(_, _, v)| *v);
                    q = q.bind(pg_bind_text(val, udt));
                }
            }

            q.execute(&self.pool)
                .await
                .map_err(|e| FaucetError::Sink(format!("PostgreSQL insert failed: {e}")))?;
        }

        Ok(num_rows)
    }
}

#[async_trait]
impl faucet_core::Sink for PostgresSink {
    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(PostgresSinkConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        let table = match &self.config.schema {
            Some(s) => format!("{}.{}", s, self.config.table_name),
            None => self.config.table_name.clone(),
        };
        format!(
            "{}?table={}",
            faucet_core::redact_uri_credentials(&self.config.connection_url),
            table
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

    /// Write records to PostgreSQL.
    ///
    /// When `config.batch_size > 0` and the input slice is larger than
    /// `batch_size`, the slice is split into chunks of `batch_size` rows and
    /// each chunk is sent as a separate multi-row `INSERT`. When
    /// `config.batch_size == 0`, the entire slice is sent in a single
    /// `INSERT` — useful when upstream `StreamPage`s are already sized for
    /// Postgres' per-statement bind-parameter limit (~65 535 / num_columns
    /// in AutoMap mode).
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        if records.is_empty() {
            return Ok(0);
        }

        let chunks: Vec<&[Value]> = if self.config.batch_size == 0 {
            // Sentinel: pass the entire upstream page through in a single
            // INSERT statement. Subject to Postgres' 65 535 bind-parameter
            // limit in AutoMap mode; JSONB mode binds a single array.
            vec![records]
        } else {
            records.chunks(self.config.batch_size).collect()
        };

        let mut total = 0;
        for chunk in chunks {
            total += match &self.config.column_mapping {
                PostgresColumnMapping::Jsonb { column } => self.insert_jsonb(chunk, column).await?,
                PostgresColumnMapping::AutoMap => self.insert_auto_map(chunk).await?,
            };
        }

        tracing::info!(
            table = %self.config.table_name,
            rows = total,
            "PostgreSQL write complete"
        );
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::{pg_bind_text, qualified_table_ref};
    use serde_json::json;

    // dataset_uri test is skipped: PostgresSink::new() requires a live pool
    // (connects to PostgreSQL in new()), and no offline constructor exists.
    // The URI format is covered by unit tests in faucet-core's redact tests.

    #[test]
    fn qualified_table_ref_unqualified_is_bare_quoted_table() {
        // No schema → bare quoted table, resolved against the search_path.
        assert_eq!(qualified_table_ref(None, "events"), "\"events\"");
    }

    #[test]
    fn qualified_table_ref_with_schema_is_schema_dot_table() {
        // With a schema → "schema"."table", so discovery and INSERT both
        // target the same explicit relation (#146 M13).
        assert_eq!(
            qualified_table_ref(Some("analytics"), "events"),
            "\"analytics\".\"events\""
        );
    }

    #[test]
    fn qualified_table_ref_escapes_embedded_quotes() {
        // SQL-injection safety: embedded double-quotes are doubled.
        assert_eq!(
            qualified_table_ref(Some("we\"ird"), "ta\"ble"),
            "\"we\"\"ird\".\"ta\"\"ble\""
        );
    }

    #[test]
    fn null_and_absent_bind_sql_null() {
        assert_eq!(pg_bind_text(None, "text"), None);
        assert_eq!(pg_bind_text(Some(&json!(null)), "int4"), None);
        assert_eq!(pg_bind_text(Some(&json!(null)), "jsonb"), None);
    }

    #[test]
    fn scalars_bind_plain_text_for_typed_columns() {
        // The `$N::<udt>` cast parses these via the column's input function.
        assert_eq!(
            pg_bind_text(Some(&json!(42)), "int4").as_deref(),
            Some("42")
        );
        assert_eq!(
            pg_bind_text(Some(&json!(1.5)), "numeric").as_deref(),
            Some("1.5")
        );
        assert_eq!(
            pg_bind_text(Some(&json!(true)), "bool").as_deref(),
            Some("true")
        );
        assert_eq!(
            pg_bind_text(Some(&json!("2025-01-01T00:00:00Z")), "timestamptz").as_deref(),
            Some("2025-01-01T00:00:00Z")
        );
        // A plain string into TEXT keeps NO JSON quotes (the bug bound `"Bob"`).
        assert_eq!(
            pg_bind_text(Some(&json!("Bob")), "text").as_deref(),
            Some("Bob")
        );
        // Large u64 beyond i64 keeps exact text (no f64 precision loss).
        assert_eq!(
            pg_bind_text(Some(&json!(18446744073709551615u64)), "numeric").as_deref(),
            Some("18446744073709551615")
        );
    }

    #[test]
    fn json_columns_get_json_text_with_quotes_preserved() {
        // For jsonb/json columns the value is bound as JSON text so the
        // `::jsonb` cast parses it: a string keeps its quotes, objects/arrays
        // round-trip.
        assert_eq!(
            pg_bind_text(Some(&json!("Bob")), "jsonb").as_deref(),
            Some("\"Bob\"")
        );
        assert_eq!(
            pg_bind_text(Some(&json!({"a": 1})), "jsonb").as_deref(),
            Some("{\"a\":1}")
        );
        assert_eq!(
            pg_bind_text(Some(&json!([1, 2])), "json").as_deref(),
            Some("[1,2]")
        );
        assert_eq!(pg_bind_text(Some(&json!(5)), "jsonb").as_deref(), Some("5"));
        // udt match is case-insensitive.
        assert_eq!(
            pg_bind_text(Some(&json!("x")), "JSONB").as_deref(),
            Some("\"x\"")
        );
    }

    #[test]
    fn objects_into_non_json_columns_emit_json_text_so_the_cast_fails_loudly() {
        // No scalar text form for an object targeting e.g. an int column; the
        // `::int4` cast will reject this rather than silently coercing.
        assert_eq!(
            pg_bind_text(Some(&json!({"a": 1})), "int4").as_deref(),
            Some("{\"a\":1}")
        );
    }
}
