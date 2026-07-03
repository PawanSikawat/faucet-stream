//! SQLite source implementation.

use crate::config::SqliteSourceConfig;
use async_trait::async_trait;
use faucet_core::shard::{
    PkShardBounds, ShardSpec, parse_pk_shard, pk_bounds_query, pk_shards_from_bounds,
};
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Column, Row, SqlitePool};
use std::pin::Pin;
use std::sync::Mutex;

/// A source that executes a SQL query against SQLite and returns rows as JSON.
pub struct SqliteSource {
    config: SqliteSourceConfig,
    pool: SqlitePool,
    /// Shard applied by the cluster coordinator (Mode B), if any. `None` (or the
    /// whole-dataset shard) means the full query is streamed. Stored behind a
    /// `Mutex` so `apply_shard(&self, …)` can record it before streaming.
    applied_shard: Mutex<Option<PkShardBounds>>,
}

/// Quote a SQLite identifier with backticks.
///
/// Deliberately NOT ANSI double quotes: SQLite's double-quoted-string
/// misfeature silently reinterprets a double-quoted identifier that does not
/// resolve to a column as a **string literal**, so a typo'd shard key would
/// make `MIN("typo")` return the literal string (→ bounds of 0) instead of
/// erroring. Backtick-quoted identifiers are always identifiers — an unknown
/// column surfaces as a proper "no such column" error. Embedded backticks are
/// doubled, preventing identifier injection.
fn quote_ident_sqlite(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

impl SqliteSource {
    /// Create a new SQLite source. Establishes a connection pool.
    pub async fn new(config: SqliteSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;

        let pool = SqlitePoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await
            .map_err(|e| FaucetError::Config(format!("SQLite connection failed: {e}")))?;

        Ok(Self {
            config,
            pool,
            applied_shard: Mutex::new(None),
        })
    }

    /// Apply the currently-set shard (if any) to a resolved query string.
    fn shard_wrap(&self, query: String) -> String {
        match &*self.applied_shard.lock().expect("shard mutex poisoned") {
            Some(bounds) => bounds.wrap(&query, quote_ident_sqlite),
            None => query,
        }
    }
}

/// Convert a SQLite row column value to a `serde_json::Value`.
///
/// SQLite has dynamic typing — values are stored as INTEGER, REAL, TEXT,
/// BLOB, or NULL. We try each type in order of specificity.
fn sqlite_value_to_json(row: &sqlx::sqlite::SqliteRow, col_name: &str) -> Value {
    // Try JSON first (TEXT that parses as JSON)
    if let Ok(v) = row.try_get::<Value, _>(col_name) {
        return v;
    }

    if let Ok(v) = row.try_get::<String, _>(col_name) {
        return Value::String(v);
    }
    if let Ok(v) = row.try_get::<i64, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<i32, _>(col_name) {
        return Value::Number(v.into());
    }
    if let Ok(v) = row.try_get::<f64, _>(col_name) {
        return serde_json::Number::from_f64(v)
            .map(Value::Number)
            .unwrap_or(Value::Null);
    }
    if let Ok(v) = row.try_get::<bool, _>(col_name) {
        return Value::Bool(v);
    }
    // BLOB → base64 so binary survives the JSON round-trip instead of decoding
    // to Null (#78/#43). SQLite has no native datetime/uuid/decimal types —
    // those are stored as TEXT/INTEGER/REAL and handled by the arms above.
    if let Ok(v) = row.try_get::<Vec<u8>, _>(col_name) {
        use base64::Engine as _;
        return Value::String(base64::engine::general_purpose::STANDARD.encode(v));
    }

    Value::Null
}

/// Build the effective SQL query and ordered context-bind values for a given
/// parent context. Returns the literal query when there is no context.
///
/// SQLite uses positional `?` placeholders (not the `$N` form used by
/// PostgreSQL), so the bind-marker formatter ignores the index.
fn resolve_query(
    config: &SqliteSourceConfig,
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

/// How a numeric bind value should be bound onto a sqlx query.
///
/// Classifying *before* binding keeps the integer/float decision in one pure,
/// unit-testable place and — critically — binds any integer in
/// `[i64::MIN, i64::MAX]` as an exact `i64` rather than an `f64`. Binding an
/// integer above `2^53` as `f64` silently rounds it (audit F38), so a large
/// 64-bit id threaded into `WHERE id = ?` would compare against the *wrong*
/// value and return wrong rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumberBind {
    /// Exact `i64` — covers every integer in `[i64::MIN, i64::MAX]`.
    I64,
    /// Value above `i64::MAX`; bind the `u64` reinterpreted as `i64` (SQLite
    /// stores INTEGER as a signed 8-byte value and has no unsigned type).
    U64,
    /// Genuine floating-point value — bind as `f64`.
    F64,
}

/// Classify a JSON number into the bind category to use.
///
/// `is_i64()` losslessly covers `[i64::MIN, i64::MAX]` (including the
/// `(2^53, i64::MAX]` range that `f64` would round); `is_u64()` covers values
/// above `i64::MAX`; everything else is a real float.
fn classify_number(n: &serde_json::Number) -> NumberBind {
    if n.is_i64() {
        NumberBind::I64
    } else if n.is_u64() {
        NumberBind::U64
    } else {
        NumberBind::F64
    }
}

/// Apply context-derived bind values onto a sqlx query.
fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    bind_values: &'q [Value],
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    for value in bind_values {
        query = match value {
            Value::String(s) => query.bind(s.clone()),
            Value::Number(n) => match classify_number(n) {
                // `unwrap()` is sound: the classifier proves the predicate.
                NumberBind::I64 => query.bind(n.as_i64().unwrap()),
                // SQLite has no unsigned integer type; reinterpret the bits so
                // the value round-trips through its signed 8-byte INTEGER
                // without the precision loss an `f64` cast would introduce.
                NumberBind::U64 => query.bind(n.as_u64().unwrap() as i64),
                NumberBind::F64 => query.bind(n.as_f64().unwrap_or(0.0)),
            },
            Value::Bool(b) => query.bind(*b),
            Value::Null => query.bind(None::<String>),
            _ => query.bind(value.to_string()),
        };
    }
    query
}

/// Convert a single `SqliteRow` into a JSON object whose keys are the row's
/// column names.
fn row_to_json(row: &sqlx::sqlite::SqliteRow) -> Value {
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = sqlite_value_to_json(row, &name);
        map.insert(name, value);
    }
    Value::Object(map)
}

#[async_trait]
impl faucet_core::Source for SqliteSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (query_str, bind_values) = resolve_query(&self.config, context);
        let query_str = self.shard_wrap(query_str);
        let query = bind_params(sqlx::query(&query_str), &bind_values);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Config(format!("SQLite query failed: {e}")))?;

        let records: Vec<Value> = rows.iter().map(row_to_json).collect();
        tracing::info!(
            rows = records.len(),
            query = %self.config.query,
            "SQLite source fetch complete"
        );
        Ok(records)
    }

    /// Stream rows from the underlying sqlx cursor without buffering the full
    /// result set. Each emitted [`StreamPage`] holds up to
    /// [`SqliteSourceConfig::batch_size`] rows.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire cursor into a single page. SQLite
    /// is an in-process engine with no server-side cursor concept, so this
    /// streams rows page-by-page off the local file rather than across a
    /// network wire. The sqlite query source has no incremental-replication
    /// mode today, so every emitted page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let (query_str, bind_values) = resolve_query(&self.config, context);
            let query_str = self.shard_wrap(query_str);
            let query = bind_params(sqlx::query(&query_str), &bind_values);

            let mut rows = query.fetch(&self.pool);
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|e| FaucetError::Config(format!("SQLite query failed: {e}")))?
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
                "SQLite source stream complete",
            );
        })
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(SqliteSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        let path = self
            .config
            .database_url
            .trim_start_matches("sqlite://")
            .trim_start_matches("sqlite:");
        format!("sqlite://{}?query={}", path, self.config.query)
    }

    /// Shardable when a [`ShardConfig`](crate::config::ShardConfig) is set.
    fn is_shardable(&self) -> bool {
        self.config.shard.is_some()
    }

    /// Enumerate contiguous primary-key range shards by computing the `key`
    /// column's `MIN`/`MAX` over the (unsharded) base query and splitting that
    /// range into ~`target` slices. Returns a single whole-dataset shard when no
    /// `shard` config is set or the result set is empty.
    async fn enumerate_shards(&self, target: usize) -> Result<Vec<ShardSpec>, FaucetError> {
        let Some(shard_cfg) = &self.config.shard else {
            return Ok(vec![ShardSpec::whole()]);
        };

        let bounds_sql = pk_bounds_query(
            &self.config.query,
            &quote_ident_sqlite(&shard_cfg.key),
            "INTEGER",
        );
        let row = sqlx::query(&bounds_sql)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "sqlite: failed to compute shard bounds for key {:?} \
                     (it must be an integer-typed column): {e}",
                    shard_cfg.key
                ))
            })?;

        let lo: Option<i64> = row
            .try_get("lo")
            .map_err(|e| FaucetError::Source(format!("sqlite: shard bounds decode failed: {e}")))?;
        let hi: Option<i64> = row
            .try_get("hi")
            .map_err(|e| FaucetError::Source(format!("sqlite: shard bounds decode failed: {e}")))?;
        Ok(pk_shards_from_bounds(&shard_cfg.key, lo, hi, target))
    }

    /// Narrow this source to a single PK-range shard. The whole-dataset shard
    /// clears any applied range (streams the full query).
    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        *self.applied_shard.lock().expect("shard mutex poisoned") =
            parse_pk_shard(shard, "sqlite")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use faucet_core::Source;

    #[tokio::test]
    async fn fetch_from_memory_db() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1 AS val, 'hello' AS msg");
        let source = SqliteSource::new(config).await.unwrap();
        let records = source.fetch_all().await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["val"], 1);
        assert_eq!(records[0]["msg"], "hello");
    }

    #[tokio::test]
    async fn fetch_from_table() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1");
        let source = SqliteSource::new(config).await.unwrap();

        // Create a table and insert data.
        sqlx::query("CREATE TABLE test_items (id INTEGER PRIMARY KEY, name TEXT, score REAL)")
            .execute(&source.pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO test_items (id, name, score) VALUES (1, 'Alice', 95.5), (2, 'Bob', 87.0)",
        )
        .execute(&source.pool)
        .await
        .unwrap();

        // Reuse the same pool by creating a new source pointing to same in-memory db.
        // For in-memory DBs, each connection gets its own DB, so we query through the existing pool.
        let rows = sqlx::query("SELECT * FROM test_items ORDER BY id")
            .fetch_all(&source.pool)
            .await
            .unwrap();

        assert_eq!(rows.len(), 2);
        let row0 = &rows[0];
        assert_eq!(row0.try_get::<i64, _>("id").unwrap(), 1);
        assert_eq!(row0.try_get::<String, _>("name").unwrap(), "Alice");
    }

    #[tokio::test]
    async fn blob_column_decodes_to_base64() {
        // Regression for #78/#43: a BLOB column must become base64, not Null.
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1");
        let source = SqliteSource::new(config).await.unwrap();
        sqlx::query("CREATE TABLE b (id INTEGER, data BLOB)")
            .execute(&source.pool)
            .await
            .unwrap();
        // X'00FF' = bytes [0x00, 0xFF] — non-UTF8 so it can't be read as text.
        sqlx::query("INSERT INTO b (id, data) VALUES (1, X'00FF')")
            .execute(&source.pool)
            .await
            .unwrap();
        let rows = sqlx::query("SELECT data FROM b")
            .fetch_all(&source.pool)
            .await
            .unwrap();
        let v = sqlite_value_to_json(&rows[0], "data");
        assert_eq!(v, Value::String("AP8=".to_string()), "BLOB must be base64");
    }

    #[tokio::test]
    async fn empty_result() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1 AS x WHERE 1 = 0");
        let source = SqliteSource::new(config).await.unwrap();
        let records = source.fetch_all().await.unwrap();
        assert!(records.is_empty());
    }

    #[tokio::test]
    async fn invalid_query_returns_error() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "INVALID SQL");
        let source = SqliteSource::new(config).await.unwrap();
        let result = source.fetch_all().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn fetch_with_context_substitutes_query_placeholders() {
        let config =
            SqliteSourceConfig::new("sqlite::memory:", "SELECT {val} AS result, {name} AS name");
        let source = SqliteSource::new(config).await.unwrap();

        let mut context = std::collections::HashMap::new();
        context.insert("val".to_string(), serde_json::json!(42));
        context.insert("name".to_string(), serde_json::json!("hello"));

        let records = source.fetch_with_context(&context).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["result"], 42);
        assert_eq!(records[0]["name"], "hello");
    }

    #[tokio::test]
    async fn fetch_with_context_prevents_sql_injection() {
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT {val} AS result");
        let source = SqliteSource::new(config).await.unwrap();

        let mut context = std::collections::HashMap::new();
        context.insert(
            "val".to_string(),
            serde_json::json!("1; DROP TABLE test; --"),
        );

        // Value is bound as a parameter, not interpolated — no injection possible
        let records = source.fetch_with_context(&context).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["result"], "1; DROP TABLE test; --");
    }

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 1");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match SqliteSource::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }

    // dataset_uri is a pure-config method — test the logic without needing a
    // live file path by exercising the trim logic directly.
    #[test]
    fn dataset_uri_strips_sqlite_scheme_logic() {
        // Verify the trim_start_matches chain that dataset_uri() uses.
        let url1 = "sqlite:///var/db/app.db";
        let path1 = url1
            .trim_start_matches("sqlite://")
            .trim_start_matches("sqlite:");
        assert_eq!(
            format!("sqlite://{}?query=SELECT 1", path1),
            "sqlite:///var/db/app.db?query=SELECT 1"
        );

        let url2 = "sqlite:/tmp/data.db";
        let path2 = url2
            .trim_start_matches("sqlite://")
            .trim_start_matches("sqlite:");
        assert_eq!(
            format!("sqlite://{}?query=SELECT 1", path2),
            "sqlite:///tmp/data.db?query=SELECT 1"
        );
    }

    // ── F38: numeric bind classification (precision-safe) ───────────────────

    fn num(v: serde_json::Value) -> serde_json::Number {
        match v {
            serde_json::Value::Number(n) => n,
            _ => panic!("not a number"),
        }
    }

    #[test]
    fn classify_small_int_is_i64() {
        assert_eq!(
            classify_number(&num(serde_json::json!(42))),
            NumberBind::I64
        );
        assert_eq!(
            classify_number(&num(serde_json::json!(-7))),
            NumberBind::I64
        );
        assert_eq!(classify_number(&num(serde_json::json!(0))), NumberBind::I64);
    }

    #[test]
    fn classify_above_2_pow_53_stays_i64_not_f64() {
        // 2^53 + 1 must NOT be bound as f64 (which would round it). It is a
        // valid i64, so it must classify as I64.
        let v = 9_007_199_254_740_993i64; // 2^53 + 1
        assert_eq!(classify_number(&num(serde_json::json!(v))), NumberBind::I64);
    }

    #[test]
    fn classify_i64_boundaries_are_i64() {
        assert_eq!(
            classify_number(&num(serde_json::json!(i64::MAX))),
            NumberBind::I64
        );
        assert_eq!(
            classify_number(&num(serde_json::json!(i64::MIN))),
            NumberBind::I64
        );
    }

    #[test]
    fn classify_above_i64_max_is_u64() {
        let v: u64 = i64::MAX as u64 + 1;
        assert_eq!(classify_number(&num(serde_json::json!(v))), NumberBind::U64);
        assert_eq!(
            classify_number(&num(serde_json::json!(u64::MAX))),
            NumberBind::U64
        );
    }

    #[test]
    fn classify_float_is_f64() {
        assert_eq!(
            classify_number(&num(serde_json::json!(3.5))),
            NumberBind::F64
        );
    }

    /// End-to-end proof through the real bind path: a 64-bit id above 2^53
    /// bound as a context param must match the stored row exactly (an f64 bind
    /// would round it and the WHERE clause would miss).
    #[tokio::test]
    async fn large_int_param_binds_without_precision_loss() {
        let big = 9_007_199_254_740_993i64; // 2^53 + 1
        let config =
            SqliteSourceConfig::new("sqlite::memory:", "SELECT {id} AS id, 'hit' AS marker");
        let source = SqliteSource::new(config).await.unwrap();

        let mut context = std::collections::HashMap::new();
        context.insert("id".to_string(), serde_json::json!(big));

        let records = source.fetch_with_context(&context).await.unwrap();
        assert_eq!(records.len(), 1);
        // The bound value must come back exactly — not rounded to 2^53.
        assert_eq!(records[0]["id"].as_i64().unwrap(), big);
    }

    #[tokio::test]
    async fn dataset_uri_memory_db() {
        // :memory: is a valid SQLite URL that can be opened without a real file.
        let config = SqliteSourceConfig::new("sqlite::memory:", "SELECT 42 AS n");
        let source = SqliteSource::new(config).await.unwrap();
        // ":memory:" has no sqlite:// prefix to strip; it passes through as-is.
        let uri = source.dataset_uri();
        assert!(uri.contains("SELECT 42 AS n"), "got: {uri}");
        assert!(uri.starts_with("sqlite://"), "got: {uri}");
    }

    // ── PK-range sharding (Mode B, #262) ─────────────────────────────────────

    /// Build a single-connection in-memory source so every query sees the same
    /// database (each pooled connection normally gets its own `:memory:` DB).
    async fn sharded_memory_source(query: &str, key: &str) -> SqliteSource {
        let mut config = SqliteSourceConfig::new("sqlite::memory:", query).with_max_connections(1);
        config.shard = Some(crate::config::ShardConfig { key: key.into() });
        SqliteSource::new(config).await.unwrap()
    }

    /// The core Mode B correctness guarantee, end-to-end on a real database:
    /// enumerating into N shards and reading each shard yields every row —
    /// including a NULL-key row invisible to MIN/MAX (F37) — exactly once.
    #[tokio::test]
    async fn shards_partition_rows_disjointly_and_completely() {
        let source = sharded_memory_source("SELECT k, label FROM items", "k").await;
        sqlx::query("CREATE TABLE items (k INTEGER, label TEXT)")
            .execute(&source.pool)
            .await
            .unwrap();
        for i in 1..=100i64 {
            sqlx::query("INSERT INTO items (k, label) VALUES (?, ?)")
                .bind(i)
                .bind(format!("row-{i}"))
                .execute(&source.pool)
                .await
                .unwrap();
        }
        // A NULL-key row: MIN/MAX can't see it, but exactly one shard must.
        sqlx::query("INSERT INTO items (k, label) VALUES (NULL, 'null-row')")
            .execute(&source.pool)
            .await
            .unwrap();

        assert!(source.is_shardable());
        let shards = source.enumerate_shards(4).await.expect("enumerate");
        assert!(
            (2..=4).contains(&shards.len()),
            "expected 2..=4 shards, got {}",
            shards.len()
        );

        let mut labels: Vec<String> = Vec::new();
        for shard in &shards {
            source.apply_shard(shard).await.expect("apply_shard");
            for rec in source.fetch_all().await.expect("fetch shard") {
                labels.push(rec["label"].as_str().unwrap().to_string());
            }
        }

        labels.sort();
        let mut expected: Vec<String> = (1..=100i64).map(|i| format!("row-{i}")).collect();
        expected.push("null-row".to_string());
        expected.sort();
        assert_eq!(
            labels, expected,
            "shards must union to all rows exactly once (no dup, no loss)"
        );
    }

    /// Applying the whole-dataset shard clears the range — full query again.
    #[tokio::test]
    async fn whole_shard_restores_full_query() {
        let source = sharded_memory_source("SELECT k FROM items", "k").await;
        sqlx::query("CREATE TABLE items (k INTEGER)")
            .execute(&source.pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO items (k) VALUES (1), (2), (3)")
            .execute(&source.pool)
            .await
            .unwrap();

        let shards = source.enumerate_shards(2).await.unwrap();
        source.apply_shard(&shards[0]).await.unwrap();
        let narrowed = source.fetch_all().await.unwrap().len();
        assert!(narrowed < 3, "a real shard narrows the result set");

        source
            .apply_shard(&faucet_core::ShardSpec::whole())
            .await
            .unwrap();
        assert_eq!(source.fetch_all().await.unwrap().len(), 3);
    }

    /// Enumeration over an empty result set degrades to one whole shard, and a
    /// config without `shard:` is not shardable.
    #[tokio::test]
    async fn empty_result_and_unsharded_config_yield_whole_shard() {
        let source = sharded_memory_source("SELECT k FROM items", "k").await;
        sqlx::query("CREATE TABLE items (k INTEGER)")
            .execute(&source.pool)
            .await
            .unwrap();
        let shards = source.enumerate_shards(4).await.unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_whole());

        let plain = SqliteSource::new(SqliteSourceConfig::new("sqlite::memory:", "SELECT 1"))
            .await
            .unwrap();
        assert!(!plain.is_shardable());
        let shards = plain.enumerate_shards(4).await.unwrap();
        assert_eq!(shards.len(), 1);
        assert!(shards[0].is_whole());
    }

    /// Error paths a coordinator must handle: a bad shard key errors at
    /// enumeration; a malformed descriptor is rejected by apply_shard.
    #[tokio::test]
    async fn shard_error_paths() {
        let source = sharded_memory_source("SELECT k FROM items", "no_such_column").await;
        sqlx::query("CREATE TABLE items (k INTEGER)")
            .execute(&source.pool)
            .await
            .unwrap();
        assert!(source.enumerate_shards(4).await.is_err());

        let bad = faucet_core::ShardSpec::new("0", serde_json::json!({ "key": "k" }));
        assert!(source.apply_shard(&bad).await.is_err());
    }
}
