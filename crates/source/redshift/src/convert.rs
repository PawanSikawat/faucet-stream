//! Row decoding and parameter binding for the Redshift source.
//!
//! Redshift is PostgreSQL wire-compatible, so this mirrors the native Postgres
//! source: values decode through `sqlx`'s Postgres row API, and JSON bind values
//! are classified before binding so large integers keep full precision.

use serde_json::Value;
use sqlx::{Column, Row};

/// Convert a raw Redshift/Postgres column value to a `serde_json::Value`.
///
/// Tries progressively broader decodings and falls back to `Value::Null` for
/// unsupported or SQL-NULL columns.
pub(crate) fn pg_value_to_json(row: &sqlx::postgres::PgRow, col_name: &str) -> Value {
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
    if let Ok(v) = row.try_get::<sqlx::types::Uuid, _>(col_name) {
        return Value::String(v.to_string());
    }
    // NUMERIC / DECIMAL → string, preserving exact precision.
    if let Ok(v) = row.try_get::<sqlx::types::BigDecimal, _>(col_name) {
        return Value::String(v.to_string());
    }
    // Binary (VARBYTE / bytea) → base64 so it survives the JSON round-trip.
    if let Ok(v) = row.try_get::<Vec<u8>, _>(col_name) {
        use base64::Engine as _;
        return Value::String(base64::engine::general_purpose::STANDARD.encode(v));
    }
    Value::Null
}

/// Convert a single row into a JSON object keyed by column name.
pub(crate) fn row_to_json(row: &sqlx::postgres::PgRow) -> Value {
    let mut map = serde_json::Map::new();
    for col in row.columns() {
        let name = col.name().to_string();
        let value = pg_value_to_json(row, &name);
        map.insert(name, value);
    }
    Value::Object(map)
}

/// How a numeric bind value should be bound onto a `sqlx` query. Classifying
/// before binding keeps any integer in `[i64::MIN, i64::MAX]` exact (binding
/// large integers as `f64` silently rounds them).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumberBind {
    /// Exact `i64`.
    I64,
    /// Above `i64::MAX`; bind the `u64` reinterpreted as `i64` (two's complement).
    U64,
    /// Genuine floating-point value.
    F64,
}

/// Classify a JSON number into the bind category to use.
pub(crate) fn classify_number(n: &serde_json::Number) -> NumberBind {
    if n.is_i64() {
        NumberBind::I64
    } else if n.is_u64() {
        NumberBind::U64
    } else {
        NumberBind::F64
    }
}

/// Bind a slice of JSON values onto a `sqlx` query as native scalar types, in
/// positional order (`$1, $2, …`). Binding a raw `serde_json::Value` would
/// encode as `jsonb` and break comparisons against typed columns, so scalars
/// are bound as their native types.
pub(crate) fn bind_params<'q>(
    mut query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    binds: &'q [Value],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    for value in binds {
        query = match value {
            Value::String(s) => query.bind(s.clone()),
            Value::Number(n) => match classify_number(n) {
                NumberBind::I64 => query.bind(n.as_i64().unwrap()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn num(v: serde_json::Value) -> serde_json::Number {
        match v {
            serde_json::Value::Number(n) => n,
            _ => panic!("not a number"),
        }
    }

    #[test]
    fn classify_small_int_is_i64() {
        assert_eq!(classify_number(&num(json!(42))), NumberBind::I64);
        assert_eq!(classify_number(&num(json!(-7))), NumberBind::I64);
    }

    #[test]
    fn classify_above_2_pow_53_stays_i64() {
        let v = 9_007_199_254_740_993i64; // 2^53 + 1
        assert_eq!(classify_number(&num(json!(v))), NumberBind::I64);
    }

    #[test]
    fn classify_above_i64_max_is_u64() {
        let v: u64 = i64::MAX as u64 + 1;
        assert_eq!(classify_number(&num(json!(v))), NumberBind::U64);
    }

    #[test]
    fn classify_float_is_f64() {
        assert_eq!(classify_number(&num(json!(3.5))), NumberBind::F64);
    }
}
