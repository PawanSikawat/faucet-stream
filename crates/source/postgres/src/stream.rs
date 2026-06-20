//! PostgreSQL source implementation.

use crate::config::PostgresSourceConfig;
use async_trait::async_trait;
use faucet_core::shard::ShardSpec;
use faucet_core::util::quote_ident;
use faucet_core::{FaucetError, Stream, StreamPage};
use futures::TryStreamExt;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Column, PgPool, Row};
use std::pin::Pin;
use std::sync::Mutex;

/// A source that executes a SQL query against PostgreSQL and returns rows as JSON.
pub struct PostgresSource {
    config: PostgresSourceConfig,
    pool: PgPool,
    /// Shard applied by the cluster coordinator (Mode B), if any. `None` (or the
    /// whole-dataset shard) means the full query is streamed. Stored behind a
    /// `Mutex` so `apply_shard(&self, …)` can record it before streaming.
    applied_shard: Mutex<Option<ShardBounds>>,
}

/// Parsed integer range bounds for an applied PK-range shard.
#[derive(Clone, Debug)]
struct ShardBounds {
    key: String,
    lo: i64,
    hi: i64,
    /// `hi` is inclusive only for the last shard (so the max row is covered);
    /// every other shard is half-open `[lo, hi)`.
    hi_inclusive: bool,
}

impl ShardBounds {
    /// Parse from a [`ShardSpec`] descriptor produced by `enumerate_shards`.
    fn from_spec(spec: &ShardSpec) -> Option<Self> {
        let d = &spec.descriptor;
        Some(Self {
            key: d.get("key")?.as_str()?.to_string(),
            lo: d.get("lo")?.as_i64()?,
            hi: d.get("hi")?.as_i64()?,
            hi_inclusive: d
                .get("hi_inclusive")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }

    /// Wrap `inner` so only rows whose `key` falls in this shard's range are
    /// returned. The key is quoted (injection-safe); the bounds are inlined as
    /// integer literals (safe — they are `i64`s produced by enumeration).
    fn wrap(&self, inner: &str) -> String {
        let op = if self.hi_inclusive { "<=" } else { "<" };
        let key = quote_ident(&self.key);
        format!(
            "SELECT * FROM ({inner}) AS _faucet_shard \
             WHERE {key} >= {lo} AND {key} {op} {hi}",
            lo = self.lo,
            hi = self.hi
        )
    }
}

impl PostgresSource {
    /// Create a new PostgreSQL source. Establishes a connection pool.
    pub async fn new(config: PostgresSourceConfig) -> Result<Self, FaucetError> {
        faucet_core::validate_batch_size(config.batch_size)?;

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.connection_url)
            .await
            .map_err(|e| FaucetError::Config(format!("PostgreSQL connection failed: {e}")))?;

        Ok(Self {
            config,
            pool,
            applied_shard: Mutex::new(None),
        })
    }

    /// Apply the currently-set shard (if any) to a resolved query string.
    fn shard_wrap(&self, query: String) -> String {
        match &*self.applied_shard.lock().expect("shard mutex poisoned") {
            Some(bounds) => bounds.wrap(&query),
            None => query,
        }
    }
}

/// Split an inclusive integer range `[min, max]` into up to `target` contiguous
/// shards, each described by `{key, lo, hi, hi_inclusive}`. All but the last are
/// half-open `[lo, hi)`; the last is `[lo, max]` (inclusive) so `max` is covered.
///
/// Pure function (no I/O) so it is unit-testable without a database.
fn plan_pk_shards(key: &str, min: i64, max: i64, target: usize) -> Vec<ShardSpec> {
    let target = target.max(1);
    // Range width as u128 to avoid i64 overflow on full-range PKs.
    let width = (max as i128 - min as i128 + 1).max(1) as u128;
    let n = (target as u128).min(width) as usize; // never more shards than values
    let step = width.div_ceil(n as u128); // ceil so shards cover the whole range

    let mut shards = Vec::with_capacity(n);
    let mut lo = min as i128;
    for i in 0..n {
        let mut hi = lo + step as i128;
        let is_last = i == n - 1;
        if is_last || hi > max as i128 {
            hi = max as i128; // clamp; last shard is inclusive of max
        }
        let descriptor = serde_json::json!({
            "key": key,
            "lo": lo as i64,
            "hi": hi as i64,
            "hi_inclusive": is_last,
        });
        let size = (hi - lo).max(0) as u64 + if is_last { 1 } else { 0 };
        shards.push(ShardSpec::new(i.to_string(), descriptor).with_size(size));
        if is_last {
            break;
        }
        lo = hi;
    }
    shards
}

/// Convert a raw sqlx column value to a `serde_json::Value`.
///
/// Uses `try_get_raw` to inspect the type info and convert accordingly.
/// Falls back to `Value::Null` for unsupported or null columns.
fn pg_value_to_json(row: &sqlx::postgres::PgRow, col_name: &str) -> Value {
    // Try JSON/JSONB first — this is the most flexible
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

    // Richer types that would otherwise silently decode to Null (#78/#43).
    // Timestamps → RFC3339 / ISO-8601 strings.
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
    // UUID → canonical hyphenated string.
    if let Ok(v) = row.try_get::<sqlx::types::Uuid, _>(col_name) {
        return Value::String(v.to_string());
    }
    // NUMERIC / DECIMAL → string, preserving exact precision.
    if let Ok(v) = row.try_get::<sqlx::types::BigDecimal, _>(col_name) {
        return Value::String(v.to_string());
    }
    // BYTEA → base64 (so binary survives the JSON round-trip).
    if let Ok(v) = row.try_get::<Vec<u8>, _>(col_name) {
        use base64::Engine as _;
        return Value::String(base64::engine::general_purpose::STANDARD.encode(v));
    }

    Value::Null
}

/// Build the effective SQL query and ordered context-bind values for a given
/// parent context. Returns the literal query when there is no context.
fn resolve_query(
    config: &PostgresSourceConfig,
    context: &std::collections::HashMap<String, Value>,
) -> (String, Vec<Value>) {
    if context.is_empty() {
        (config.query.clone(), Vec::new())
    } else {
        faucet_core::util::substitute_context_bind_params(
            &config.query,
            context,
            config.params.len() + 1,
            |i| format!("${i}"),
        )
    }
}

/// Apply configured params followed by context-derived bind values onto a
/// sqlx query.
fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    config_params: &'q [Value],
    bind_values: &'q [Value],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    // Bind the static config params and the per-context values as native
    // scalar types, in positional order ($1, $2, …). Binding a raw
    // `serde_json::Value` encodes it as `jsonb` (sqlx), which breaks comparisons
    // against typed columns — e.g. `WHERE id = $1` against an integer column
    // fails with "operator does not exist: integer = jsonb". config_params
    // previously bound the raw Value and hit exactly this (audit #146 H12).
    for value in config_params.iter().chain(bind_values) {
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

/// Convert a single `PgRow` into a JSON object whose keys are the row's
/// column names.
fn row_to_json(row: &sqlx::postgres::PgRow) -> Value {
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = pg_value_to_json(row, &name);
        map.insert(name, value);
    }
    Value::Object(map)
}

#[async_trait]
impl faucet_core::Source for PostgresSource {
    async fn fetch_with_context(
        &self,
        context: &std::collections::HashMap<String, serde_json::Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let (query_str, bind_values) = resolve_query(&self.config, context);
        let query_str = self.shard_wrap(query_str);
        let query = bind_params(sqlx::query(&query_str), &self.config.params, &bind_values);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| FaucetError::Config(format!("PostgreSQL query failed: {e}")))?;

        let records: Vec<Value> = rows.iter().map(row_to_json).collect();
        tracing::info!(rows = records.len(), query = %self.config.query, "PostgreSQL source fetch complete");
        Ok(records)
    }

    /// Stream rows from the underlying sqlx cursor without buffering the full
    /// result set. Each emitted [`StreamPage`] holds up to
    /// [`PostgresSourceConfig::batch_size`] rows.
    ///
    /// The trait-level `batch_size` argument is ignored in favour of the
    /// config field — the config is the user-facing knob the README
    /// documents, and routing the pipeline-supplied hint through it would
    /// silently override an explicit config value.
    ///
    /// `batch_size = 0` drains the entire cursor into a single page. The
    /// postgres query source has no incremental-replication mode today, so
    /// every emitted page carries `bookmark: None`.
    fn stream_pages<'a>(
        &'a self,
        context: &'a std::collections::HashMap<String, Value>,
        _batch_size: usize,
    ) -> Pin<Box<dyn Stream<Item = Result<StreamPage, FaucetError>> + Send + 'a>> {
        let batch_size = self.config.batch_size;

        Box::pin(async_stream::try_stream! {
            let (query_str, bind_values) = resolve_query(&self.config, context);
            let query_str = self.shard_wrap(query_str);
            let query = bind_params(
                sqlx::query(&query_str),
                &self.config.params,
                &bind_values,
            );

            let mut rows = query.fetch(&self.pool);
            let chunk = if batch_size == 0 { usize::MAX } else { batch_size };
            let initial_capacity = if batch_size == 0 { 1024 } else { batch_size };
            let mut buffer: Vec<Value> = Vec::with_capacity(initial_capacity);
            let mut total = 0usize;

            while let Some(row) = rows
                .try_next()
                .await
                .map_err(|e| FaucetError::Config(format!("PostgreSQL query failed: {e}")))?
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
                "PostgreSQL source stream complete",
            );
        })
    }

    fn config_schema(&self) -> serde_json::Value {
        serde_json::to_value(faucet_core::schema_for!(PostgresSourceConfig))
            .expect("schema serialization")
    }

    fn dataset_uri(&self) -> String {
        format!(
            "{}?query={}",
            faucet_core::redact_uri_credentials(&self.config.connection_url),
            self.config.query
        )
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

        let key = quote_ident(&shard_cfg.key);
        let bounds_sql = format!(
            "SELECT MIN({key})::int8 AS lo, MAX({key})::int8 AS hi \
             FROM ({inner}) AS _faucet_bounds",
            inner = self.config.query
        );
        let row = bind_params(sqlx::query(&bounds_sql), &self.config.params, &[])
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                FaucetError::Source(format!(
                    "postgres: failed to compute shard bounds for key {:?} \
                     (it must be an integer-typed column): {e}",
                    shard_cfg.key
                ))
            })?;

        let lo: Option<i64> = row.try_get("lo").map_err(|e| {
            FaucetError::Source(format!("postgres: shard bounds decode failed: {e}"))
        })?;
        let hi: Option<i64> = row.try_get("hi").map_err(|e| {
            FaucetError::Source(format!("postgres: shard bounds decode failed: {e}"))
        })?;

        match (lo, hi) {
            (Some(lo), Some(hi)) => Ok(plan_pk_shards(&shard_cfg.key, lo, hi, target)),
            // Empty result set → nothing to shard; one (empty) whole shard.
            _ => Ok(vec![ShardSpec::whole()]),
        }
    }

    /// Narrow this source to a single PK-range shard. The whole-dataset shard
    /// clears any applied range (streams the full query).
    async fn apply_shard(&self, shard: &ShardSpec) -> Result<(), FaucetError> {
        let bounds = if shard.is_whole() {
            None
        } else {
            Some(ShardBounds::from_spec(shard).ok_or_else(|| {
                FaucetError::Source(format!(
                    "postgres: invalid shard descriptor: {}",
                    shard.descriptor
                ))
            })?)
        };
        *self.applied_shard.lock().expect("shard mutex poisoned") = bounds;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn new_rejects_out_of_range_batch_size() {
        let mut config = PostgresSourceConfig::new("postgres://localhost/test", "SELECT 1");
        config.batch_size = faucet_core::MAX_BATCH_SIZE + 1;
        match PostgresSource::new(config).await {
            Err(faucet_core::FaucetError::Config(m)) => {
                assert!(m.contains("batch_size"), "got: {m}")
            }
            _ => panic!("expected a batch_size Config error"),
        }
    }

    // ── PK-range sharding (pure logic) ──────────────────────────────────────

    #[test]
    fn plan_pk_shards_covers_full_range_without_gaps_or_overlap() {
        let shards = plan_pk_shards("id", 0, 99, 4);
        assert_eq!(shards.len(), 4);
        // Contiguous half-open ranges, last inclusive of max.
        let mut expected_lo = 0i64;
        for (i, s) in shards.iter().enumerate() {
            let d = &s.descriptor;
            assert_eq!(d["key"], "id");
            assert_eq!(d["lo"].as_i64().unwrap(), expected_lo);
            let hi = d["hi"].as_i64().unwrap();
            let last = i == shards.len() - 1;
            assert_eq!(d["hi_inclusive"].as_bool().unwrap(), last);
            if last {
                assert_eq!(hi, 99, "last shard's hi is the inclusive max");
            }
            expected_lo = hi; // next shard starts where this half-open one ended
        }
    }

    #[test]
    fn plan_pk_shards_never_more_shards_than_values() {
        // Range [5, 7] has 3 values; asking for 10 shards yields at most 3.
        let shards = plan_pk_shards("pk", 5, 7, 10);
        assert!(shards.len() <= 3, "got {} shards", shards.len());
        assert_eq!(shards[0].descriptor["lo"].as_i64().unwrap(), 5);
        assert_eq!(shards.last().unwrap().descriptor["hi"].as_i64().unwrap(), 7);
        assert!(
            shards.last().unwrap().descriptor["hi_inclusive"]
                .as_bool()
                .unwrap()
        );
    }

    #[test]
    fn plan_pk_shards_single_value_one_shard() {
        let shards = plan_pk_shards("id", 42, 42, 8);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].descriptor["lo"].as_i64().unwrap(), 42);
        assert_eq!(shards[0].descriptor["hi"].as_i64().unwrap(), 42);
        assert!(shards[0].descriptor["hi_inclusive"].as_bool().unwrap());
    }

    #[test]
    fn plan_pk_shards_target_zero_treated_as_one() {
        let shards = plan_pk_shards("id", 0, 9, 0);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].descriptor["hi"].as_i64().unwrap(), 9);
    }

    #[test]
    fn shard_bounds_wrap_builds_half_open_predicate() {
        let spec = ShardSpec::new(
            "1",
            serde_json::json!({"key": "id", "lo": 100, "hi": 200, "hi_inclusive": false}),
        );
        let b = ShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t");
        assert!(sql.contains("(SELECT * FROM t) AS _faucet_shard"));
        assert!(sql.contains(r#""id" >= 100"#), "got: {sql}");
        assert!(
            sql.contains(r#""id" < 200"#),
            "half-open upper bound: {sql}"
        );
    }

    #[test]
    fn shard_bounds_wrap_last_shard_is_inclusive() {
        let spec = ShardSpec::new(
            "2",
            serde_json::json!({"key": "id", "lo": 200, "hi": 300, "hi_inclusive": true}),
        );
        let b = ShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t");
        assert!(
            sql.contains(r#""id" <= 300"#),
            "inclusive upper bound: {sql}"
        );
    }

    #[test]
    fn shard_bounds_quotes_key_against_injection() {
        let spec = ShardSpec::new(
            "0",
            serde_json::json!({"key": "weird\"; DROP", "lo": 0, "hi": 1, "hi_inclusive": false}),
        );
        let b = ShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT 1");
        // The doubled quote escaping proves the identifier was quoted, not raw.
        assert!(
            sql.contains(r#""weird""; DROP""#),
            "key must be quoted: {sql}"
        );
    }

    #[test]
    fn shard_bounds_from_spec_rejects_malformed_descriptor() {
        let spec = ShardSpec::new("0", serde_json::json!({"key": "id"})); // no lo/hi
        assert!(ShardBounds::from_spec(&spec).is_none());
        assert!(ShardBounds::from_spec(&ShardSpec::whole()).is_none());
    }

    // dataset_uri is a pure-config method; the source requires a live DB to
    // construct so we test it via a config-derived assertion instead.
    #[test]
    fn dataset_uri_strips_credentials() {
        // We cannot construct PostgresSource offline, so we verify the
        // credential-stripping logic used by dataset_uri() directly.
        let redacted = faucet_core::redact_uri_credentials("postgres://u:p@h:5432/db");
        let uri = format!("{}?query={}", redacted, "SELECT 1");
        assert_eq!(uri, "postgres://h:5432/db?query=SELECT 1");
    }
}
