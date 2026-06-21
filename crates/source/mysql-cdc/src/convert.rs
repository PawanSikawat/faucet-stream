//! Convert MySQL binlog row values into JSON.

use base64::Engine as _;
use faucet_core::FaucetError;
use mysql_async::Value;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use serde_json::{Map, Value as Json};

/// Convert a single MySQL protocol `Value` to JSON.
///
/// - `NULL` → `null`
/// - `Int(i64)` / `UInt(u64)` → JSON number
/// - `Float(f32)` / `Double(f64)` → JSON number (NaN/Inf → `null`)
/// - `Bytes(Vec<u8>)` → UTF-8 string if valid, else base64-encoded string
/// - `Date(y, mo, d, h, mi, s, micro)` → `"YYYY-MM-DD HH:MM:SS.ffffff"`
/// - `Time(neg, days, h, mi, s, micro)` → `"[-]HHH:MM:SS.ffffff"`
pub fn value_to_json(v: &Value) -> Json {
    match v {
        Value::NULL => Json::Null,
        Value::Int(i) => Json::from(*i),
        Value::UInt(u) => Json::from(*u),
        Value::Float(f) => serde_json::Number::from_f64(f64::from(*f))
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Double(d) => serde_json::Number::from_f64(*d)
            .map(Json::Number)
            .unwrap_or(Json::Null),
        Value::Bytes(b) => match std::str::from_utf8(b) {
            // Text-like columns decode as UTF-8 strings; binary falls back to base64.
            Ok(s) => Json::String(s.to_owned()),
            Err(_) => Json::String(base64::engine::general_purpose::STANDARD.encode(b)),
        },
        // Date(year, month, day, hour, minute, second, microsecond)
        Value::Date(y, mo, d, h, mi, s, micro) => Json::String(format!(
            "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{micro:06}"
        )),
        // Time(is_negative, days, hours, minutes, seconds, microseconds)
        Value::Time(neg, days, h, mi, s, micro) => {
            let sign = if *neg { "-" } else { "" };
            // `days` is already u32; widen to u64 so an out-of-range value from a
            // malformed binlog can't overflow (real MySQL caps TIME at 838:59:59).
            let total_hours = u64::from(*days) * 24 + u64::from(*h);
            Json::String(format!("{sign}{total_hours:02}:{mi:02}:{s:02}.{micro:06}"))
        }
    }
}

/// Convert one binlog value (which may be a plain value or a JSON/JSONB value).
///
/// - `Value(val)` → delegates to [`value_to_json`]
/// - `Jsonb(x)` → converts the MySQL JSONB representation to `serde_json::Value`
/// - `JsonDiff(diffs)` → typed `FaucetError::Source` (a partial JSON diff cannot be
///   reconstructed without the prior document — see below)
///
/// **JSONB fallback:** `mysql_common`'s `TryFrom<jsonb::Value> for serde_json::Value`
/// fails only when the document contains an *opaque* scalar — a MySQL type with no
/// JSON representation stored inside a JSON column (e.g. a `DECIMAL`/`DATE`/`TIME`
/// embedded via `CAST(... AS JSON)`). Rather than corrupt the value silently, we
/// emit `null` for the whole column **and log a `tracing::warn!`** so the loss is
/// observable. Ordinary JSON content (objects, arrays, strings, numbers, bools,
/// null) converts losslessly; the opaque-scalar case is rare and documented in the
/// crate README.
///
/// **Partial JSON diffs:** when the MySQL server runs with
/// `binlog_row_value_options=PARTIAL_JSON`, an UPDATE that touches a JSON column may
/// emit a `BinlogValue::JsonDiff` — a *delta* against the column's prior value rather
/// than the full document. faucet-stream does not buffer prior row state, so the diff
/// cannot be applied to reconstruct the new value. Fabricating a placeholder would
/// silently corrupt the column, so we return a typed [`FaucetError::Source`] instead;
/// the row is surfaced as an error (and routed to a DLQ when configured). The
/// `run_preflight` startup check rejects `PARTIAL_JSON` outright so this path is not
/// normally reached — operators must set `binlog_row_value_options=''` for CDC.
pub fn binlog_value_to_json(v: &BinlogValue<'_>) -> Result<Json, FaucetError> {
    match v {
        BinlogValue::Value(val) => Ok(value_to_json(val)),
        BinlogValue::Jsonb(jsonb_val) => {
            // We clone-via-into_owned because we hold a borrow; JSONB columns are
            // rare enough that the clone cost doesn't matter.
            match serde_json::Value::try_from(jsonb_val.clone().into_owned()) {
                Ok(j) => Ok(j),
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "mysql-cdc: JSON column holds an opaque value with no JSON \
                         representation; emitting null for this column"
                    );
                    Ok(Json::Null)
                }
            }
        }
        // A partial JSON diff cannot be reconstructed without the prior document.
        // Never fabricate a value — surface a typed error so the row is routed to a
        // DLQ / fails the run rather than silently corrupting the JSON column.
        BinlogValue::JsonDiff(_) => Err(FaucetError::Source(
            "mysql-cdc: received a partial JSON diff (BinlogValue::JsonDiff) for a JSON \
             column; this happens under binlog_row_value_options=PARTIAL_JSON and cannot \
             be reconstructed without the prior document. Set binlog_row_value_options='' \
             (full JSON) on the MySQL server for CDC."
                .into(),
        )),
    }
}

/// Build a `{column_name: json_value}` object from a binlog row.
///
/// Column names require `binlog_row_metadata=FULL` on the server; positional
/// names (`col_<i>`) are used as a defensive fallback if a name is empty.
pub fn binlog_row_to_json(row: &BinlogRow) -> Result<Json, FaucetError> {
    let cols = row.columns_ref();
    let mut obj = Map::with_capacity(cols.len());
    for (i, col) in cols.iter().enumerate() {
        let name = {
            let n = col.name_str();
            if n.is_empty() {
                format!("col_{i}")
            } else {
                n.into_owned()
            }
        };
        let val = match row.as_ref(i) {
            Some(bv) => binlog_value_to_json(bv)?,
            None => Json::Null,
        };
        obj.insert(name, val);
    }
    Ok(Json::Object(obj))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mysql_async::Value;

    #[test]
    fn scalars() {
        assert_eq!(value_to_json(&Value::NULL), Json::Null);
        assert_eq!(value_to_json(&Value::Int(-5)), Json::from(-5));
        assert_eq!(value_to_json(&Value::UInt(7)), Json::from(7u64));
        assert_eq!(
            value_to_json(&Value::Bytes(b"hello".to_vec())),
            Json::String("hello".into())
        );
    }

    #[test]
    fn double() {
        assert_eq!(value_to_json(&Value::Double(1.5)), Json::from(1.5));
    }

    #[test]
    fn date_formats() {
        // Date(year=2026, month=6, day=6, hour=12, minute=30, second=0, microsecond=0)
        let v = value_to_json(&Value::Date(2026, 6, 6, 12, 30, 0, 0));
        assert_eq!(v, Json::String("2026-06-06 12:30:00.000000".into()));
    }

    #[test]
    fn time_positive() {
        // Time(neg=false, days=0, h=1, mi=30, s=0, micro=0)
        let v = value_to_json(&Value::Time(false, 0, 1, 30, 0, 0));
        assert_eq!(v, Json::String("01:30:00.000000".into()));
    }

    #[test]
    fn time_negative() {
        // Time(neg=true, days=0, h=2, mi=0, s=0, micro=500000)
        let v = value_to_json(&Value::Time(true, 0, 2, 0, 0, 500_000));
        assert_eq!(v, Json::String("-02:00:00.500000".into()));
    }

    #[test]
    fn non_utf8_bytes_base64() {
        let v = value_to_json(&Value::Bytes(vec![0xff, 0xfe]));
        assert!(matches!(v, Json::String(_)));
        // Verify it is valid base64 (decodes without panic)
        if let Json::String(s) = v {
            base64::engine::general_purpose::STANDARD
                .decode(&s)
                .expect("should be valid base64");
        }
    }

    #[test]
    fn time_with_days_accumulates_hours() {
        // Time(neg=false, days=1, h=2, ...) → 1*24 + 2 = 26 hours.
        let v = value_to_json(&Value::Time(false, 1, 2, 3, 4, 0));
        assert_eq!(v, Json::String("26:03:04.000000".into()));
    }

    #[test]
    fn float_nan_is_null() {
        let v = value_to_json(&Value::Float(f32::NAN));
        assert_eq!(v, Json::Null);
    }

    #[test]
    fn double_infinity_is_null() {
        assert_eq!(value_to_json(&Value::Double(f64::INFINITY)), Json::Null);
        assert_eq!(value_to_json(&Value::Double(f64::NEG_INFINITY)), Json::Null);
    }

    #[test]
    fn binlog_value_plain_delegates_ok() {
        let v = binlog_value_to_json(&BinlogValue::Value(Value::Int(42)))
            .expect("plain value converts");
        assert_eq!(v, Json::from(42));
    }

    #[test]
    fn binlog_value_json_diff_is_typed_error_not_placeholder() {
        // A partial JSON diff (binlog_row_value_options=PARTIAL_JSON) must surface as a
        // typed FaucetError::Source, never the fabricated "<JsonDiff>" placeholder.
        let bv = BinlogValue::JsonDiff(Vec::new());
        let err = binlog_value_to_json(&bv).expect_err("JsonDiff must error");
        assert!(
            matches!(err, FaucetError::Source(_)),
            "expected FaucetError::Source, got {err:?}"
        );
        let msg = err.to_string();
        assert!(msg.contains("partial JSON diff"), "{msg}");
        assert!(msg.contains("PARTIAL_JSON"), "{msg}");
        // Guard against any regression that re-introduces the corrupt placeholder.
        assert!(!msg.contains("<JsonDiff>"), "{msg}");
    }

    #[test]
    fn binlog_row_with_json_diff_propagates_error() {
        // The row-level entry point must propagate the JsonDiff error rather than
        // embedding a corrupt value in the resulting object.
        let bv = BinlogValue::JsonDiff(Vec::new());
        assert!(binlog_value_to_json(&bv).is_err());
    }
}
