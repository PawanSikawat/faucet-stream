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
    /// When `true` this shard has **no lower bound** — it is the *first* shard,
    /// so it owns every key below `hi` including any below the enumerated `MIN`.
    /// Rows backfilled or inserted with smaller ids during the run are read by
    /// this shard instead of being silently dropped outside `[MIN, MAX]` (F54).
    lo_unbounded: bool,
    /// When `true` this shard has **no upper bound** — it is the *last* shard,
    /// so it owns every key at/above `lo` including any above the enumerated
    /// `MAX`. Rows appended above the captured `MAX` between coordination and
    /// shard execution are read by this shard instead of being lost (F55).
    hi_unbounded: bool,
    /// When `true` this shard *additionally* matches rows whose `key` is NULL.
    ///
    /// SQL aggregates (`MIN`/`MAX`) ignore NULLs, so a nullable shard key never
    /// produces a `[lo, hi]` range covering NULL-key rows — without this flag
    /// every sharded run would silently drop them (audit F37). Exactly one
    /// shard (the last) carries this flag, so NULL-key rows are read by
    /// precisely one shard: no loss, no duplication.
    include_null: bool,
}

impl ShardBounds {
    /// Parse from a [`ShardSpec`] descriptor produced by `enumerate_shards`.
    fn from_spec(spec: &ShardSpec) -> Option<Self> {
        let d = &spec.descriptor;
        Some(Self {
            key: d.get("key")?.as_str()?.to_string(),
            lo: d.get("lo")?.as_i64()?,
            hi: d.get("hi")?.as_i64()?,
            lo_unbounded: d
                .get("lo_unbounded")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            hi_unbounded: d
                .get("hi_unbounded")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            include_null: d
                .get("include_null")
                .and_then(Value::as_bool)
                .unwrap_or(false),
        })
    }

    /// Wrap `inner` so only rows whose `key` falls in this shard's range are
    /// returned. The key is quoted (injection-safe); the bounds are inlined as
    /// integer literals (safe — they are `i64`s produced by enumeration).
    ///
    /// The boundary shards are **open-ended** (`lo_unbounded` / `hi_unbounded`)
    /// so the union of all shards tiles `(-∞, +∞)`, matching unsharded
    /// semantics — no row is dropped for sorting outside the `[MIN, MAX]`
    /// captured at enumeration time (F54/F55). The single shard with
    /// `include_null` also matches `key IS NULL` so NULL-key rows (invisible to
    /// the `MIN`/`MAX` enumeration) are still read.
    fn wrap(&self, inner: &str) -> String {
        let key = quote_ident(&self.key);
        let mut parts: Vec<String> = Vec::with_capacity(2);
        if !self.lo_unbounded {
            parts.push(format!("{key} >= {lo}", lo = self.lo));
        }
        if !self.hi_unbounded {
            parts.push(format!("{key} < {hi}", hi = self.hi));
        }
        let range = parts.join(" AND ");
        let predicate = if self.include_null {
            if range.is_empty() {
                // A single fully-unbounded shard owns the whole dataset,
                // NULL-key rows included.
                "TRUE".to_string()
            } else {
                // Parenthesize so the OR binds correctly inside the WHERE clause.
                format!("(({range}) OR {key} IS NULL)")
            }
        } else if range.is_empty() {
            "TRUE".to_string()
        } else {
            range
        };
        format!("SELECT * FROM ({inner}) AS _faucet_shard WHERE {predicate}")
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

/// Split the integer range observed as `[min, max]` into up to `target`
/// contiguous shards, each described by
/// `{key, lo, hi, lo_unbounded, hi_unbounded, include_null}`. Interior cut
/// points are half-open `[lo, hi)`, but the **boundary shards are open-ended**:
/// the first shard has no lower bound and the last shard has no upper bound, so
/// the union of all shards tiles `(-∞, +∞)`.
///
/// Open-ended boundaries match unsharded semantics: `min`/`max` are captured
/// once at enumeration time, but rows can be inserted below `min` or above `max`
/// (or backfilled) before the workers actually stream their shards. Clamping the
/// boundary shards to the captured `[min, max]` would silently drop those rows
/// (audit F54/F55); leaving them open captures everything.
///
/// The last shard also carries `include_null: true` so that NULL-key rows —
/// invisible to the `MIN`/`MAX` enumeration that produced `[min, max]` — are
/// read by exactly one shard (no loss, no duplication; audit F37). Picking the
/// *last* shard means a single-shard plan still covers NULLs.
///
/// Coverage scheme (proven by `predicate_coverage_*` tests):
/// - non-NULL keys: the first shard owns `key < cut₁`, interior shards own
///   `[cutᵢ, cutᵢ₊₁)`, and the last shard owns `key >= cutₙ` — every value
///   (including below `min` and above `max`) falls in exactly one shard;
/// - NULL keys: matched only by the last shard's `OR key IS NULL` clause —
///   exactly one shard.
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
        let is_first = i == 0;
        let is_last = i == n - 1;
        if is_last || hi > max as i128 {
            hi = max as i128; // last interior cut closes at max (the last shard
            // is unbounded above, so `hi` is unused there)
        }
        let descriptor = serde_json::json!({
            "key": key,
            "lo": lo as i64,
            "hi": hi as i64,
            // Boundary shards are open-ended so the union tiles (-∞, +∞).
            "lo_unbounded": is_first,
            "hi_unbounded": is_last,
            // Exactly one shard (the last) owns the NULL-key rows.
            "include_null": is_last,
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

/// How a numeric bind value should be bound onto a sqlx query.
///
/// Classifying *before* binding keeps the integer/float decision in one pure,
/// unit-testable place and — critically — binds any integer in
/// `[i64::MIN, i64::MAX]` as an exact `i64` rather than an `f64`. Binding an
/// integer above `2^53` as `f64` silently rounds it (audit F38), so a large
/// 64-bit id threaded into `WHERE id = $1` would compare against the *wrong*
/// value and return wrong rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NumberBind {
    /// Exact `i64` — covers every integer in `[i64::MIN, i64::MAX]`.
    I64,
    /// Value above `i64::MAX`; bind the `u64` reinterpreted as `i64` (two's
    /// complement) so the bytes round-trip into an `int8`/`bigint` column.
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
            Value::Number(n) => match classify_number(n) {
                // `unwrap()` is sound: the classifier proves the predicate.
                NumberBind::I64 => query.bind(n.as_i64().unwrap()),
                // `u64::MAX` has no `i64` representation; reinterpret the bits
                // so the value round-trips into an `int8`/`bigint` column
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
        // The key precision bug: 2^53 + 1 must NOT be bound as f64 (which would
        // round it). It is a valid i64, so it must classify as I64.
        let v = 9_007_199_254_740_993i64; // 2^53 + 1
        assert_eq!(classify_number(&num(serde_json::json!(v))), NumberBind::I64);
    }

    #[test]
    fn classify_i64_max_is_i64() {
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
        // i64::MAX + 1 has no i64 representation but fits u64.
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
        assert_eq!(
            classify_number(&num(serde_json::json!(-0.5))),
            NumberBind::F64
        );
    }

    // ── PK-range sharding (pure logic) ──────────────────────────────────────

    #[test]
    fn plan_pk_shards_covers_full_range_without_gaps_or_overlap() {
        let shards = plan_pk_shards("id", 0, 99, 4);
        assert_eq!(shards.len(), 4);
        // Contiguous half-open interior cuts; boundary shards are open-ended.
        let mut expected_lo = 0i64;
        for (i, s) in shards.iter().enumerate() {
            let d = &s.descriptor;
            assert_eq!(d["key"], "id");
            assert_eq!(d["lo"].as_i64().unwrap(), expected_lo);
            let hi = d["hi"].as_i64().unwrap();
            let first = i == 0;
            let last = i == shards.len() - 1;
            assert_eq!(d["lo_unbounded"].as_bool().unwrap(), first);
            assert_eq!(d["hi_unbounded"].as_bool().unwrap(), last);
            expected_lo = hi; // next shard starts where this half-open one ended
        }
    }

    #[test]
    fn plan_pk_shards_never_more_shards_than_values() {
        // Range [5, 7] has 3 values; asking for 10 shards yields at most 3.
        let shards = plan_pk_shards("pk", 5, 7, 10);
        assert!(shards.len() <= 3, "got {} shards", shards.len());
        assert!(
            shards[0].descriptor["lo_unbounded"].as_bool().unwrap(),
            "first shard is unbounded below"
        );
        assert!(
            shards.last().unwrap().descriptor["hi_unbounded"]
                .as_bool()
                .unwrap(),
            "last shard is unbounded above"
        );
    }

    #[test]
    fn plan_pk_shards_single_value_one_shard() {
        let shards = plan_pk_shards("id", 42, 42, 8);
        assert_eq!(shards.len(), 1);
        // A lone shard is open-ended on both sides → the whole dataset.
        assert!(shards[0].descriptor["lo_unbounded"].as_bool().unwrap());
        assert!(shards[0].descriptor["hi_unbounded"].as_bool().unwrap());
    }

    #[test]
    fn plan_pk_shards_target_zero_treated_as_one() {
        let shards = plan_pk_shards("id", 0, 9, 0);
        assert_eq!(shards.len(), 1);
        assert_eq!(shards[0].descriptor["hi"].as_i64().unwrap(), 9);
    }

    #[test]
    fn shard_bounds_wrap_builds_half_open_predicate() {
        // An interior shard (bounded both sides) is half-open `[lo, hi)`.
        let spec = ShardSpec::new(
            "1",
            serde_json::json!({"key": "id", "lo": 100, "hi": 200, "lo_unbounded": false, "hi_unbounded": false}),
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
    fn shard_bounds_wrap_first_shard_has_no_lower_bound() {
        // F54: the first shard omits the `>= lo` floor so keys below the
        // enumerated MIN are still read.
        let spec = ShardSpec::new(
            "0",
            serde_json::json!({"key": "id", "lo": 0, "hi": 100, "lo_unbounded": true, "hi_unbounded": false}),
        );
        let b = ShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t");
        assert!(sql.contains(r#""id" < 100"#), "upper bound present: {sql}");
        assert!(!sql.contains(">="), "first shard has no lower floor: {sql}");
    }

    #[test]
    fn shard_bounds_wrap_last_shard_has_no_upper_bound() {
        // F55: the last shard omits the upper bound so keys above the
        // enumerated MAX are still read.
        let spec = ShardSpec::new(
            "2",
            serde_json::json!({"key": "id", "lo": 200, "hi": 300, "lo_unbounded": false, "hi_unbounded": true}),
        );
        let b = ShardBounds::from_spec(&spec).unwrap();
        let sql = b.wrap("SELECT * FROM t");
        assert!(sql.contains(r#""id" >= 200"#), "lower bound present: {sql}");
        assert!(
            !sql.contains(" < ") && !sql.contains("<="),
            "last shard has no upper bound: {sql}"
        );
    }

    #[test]
    fn shard_bounds_quotes_key_against_injection() {
        let spec = ShardSpec::new(
            "0",
            serde_json::json!({"key": "weird\"; DROP", "lo": 0, "hi": 1, "lo_unbounded": false, "hi_unbounded": false}),
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

    // ── F37: NULL-key shard coverage ────────────────────────────────────────

    #[test]
    fn exactly_one_shard_includes_null() {
        let shards = plan_pk_shards("id", 0, 99, 5);
        let null_owners: Vec<usize> = shards
            .iter()
            .enumerate()
            .filter(|(_, s)| s.descriptor["include_null"].as_bool().unwrap_or(false))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            null_owners,
            vec![shards.len() - 1],
            "exactly the last shard owns NULL keys"
        );
    }

    #[test]
    fn single_shard_plan_still_owns_null() {
        // A single value yields one shard; it must still cover NULL keys.
        let shards = plan_pk_shards("id", 7, 7, 4);
        assert_eq!(shards.len(), 1);
        assert!(shards[0].descriptor["include_null"].as_bool().unwrap());
    }

    #[test]
    fn last_shard_wrap_emits_is_null_clause() {
        let shards = plan_pk_shards("id", 0, 99, 3);
        let last = ShardBounds::from_spec(shards.last().unwrap()).unwrap();
        let sql = last.wrap("SELECT * FROM t");
        assert!(
            sql.contains(r#""id" IS NULL"#),
            "last shard must match NULL keys: {sql}"
        );
        assert!(sql.contains(" OR "), "NULL clause OR'd with range: {sql}");
    }

    #[test]
    fn non_last_shard_wrap_omits_is_null_clause() {
        let shards = plan_pk_shards("id", 0, 99, 3);
        // First shard is not the last → no NULL clause.
        let first = ShardBounds::from_spec(&shards[0]).unwrap();
        let sql = first.wrap("SELECT * FROM t");
        assert!(
            !sql.contains("IS NULL"),
            "non-last shard must not match NULL keys: {sql}"
        );
    }

    /// Property check on the generated predicates: OR-ing every shard's WHERE
    /// predicate must cover (a) every non-NULL key — including values *outside*
    /// the enumerated `[min, max]` (F54/F55) — exactly once and (b) NULL keys
    /// exactly once.
    #[test]
    fn predicate_coverage_complete_and_non_overlapping() {
        let (min, max, target) = (0i64, 19i64, 4usize);
        let bounds: Vec<ShardBounds> = plan_pk_shards("k", min, max, target)
            .iter()
            .map(|s| ShardBounds::from_spec(s).unwrap())
            .collect();

        // The boundary shards model SQL membership: open below for the first
        // shard, open above for the last.
        let matches_key = |b: &ShardBounds, key: i64| -> bool {
            let lower = b.lo_unbounded || key >= b.lo;
            let upper = b.hi_unbounded || key < b.hi;
            lower && upper
        };

        // (a) Every non-NULL key — well below min, in range, and well above max
        // — matches exactly one shard. Keys outside [min, max] model rows
        // inserted/backfilled during the coordinate→execute window.
        for key in (min - 50)..=(max + 50) {
            let matches = bounds.iter().filter(|b| matches_key(b, key)).count();
            assert_eq!(matches, 1, "key {key} matched {matches} shards (want 1)");
        }

        // (b) NULL keys match exactly one shard (the one with include_null).
        let null_matches = bounds.iter().filter(|b| b.include_null).count();
        assert_eq!(null_matches, 1, "NULL keys must match exactly one shard");
    }

    #[test]
    fn single_shard_wrap_selects_whole_dataset_including_null() {
        // A lone open-ended shard must select every row, NULL keys included.
        let shards = plan_pk_shards("id", 7, 7, 1);
        assert_eq!(shards.len(), 1);
        let b = ShardBounds::from_spec(&shards[0]).unwrap();
        let sql = b.wrap("SELECT * FROM t");
        assert!(sql.contains("WHERE TRUE"), "whole-dataset predicate: {sql}");
        assert!(!sql.contains(">="), "no bounds on a lone shard: {sql}");
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
