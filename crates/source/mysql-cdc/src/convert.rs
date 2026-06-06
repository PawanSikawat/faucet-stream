//! Convert MySQL binlog row values into JSON.

use base64::Engine as _;
use faucet_core::FaucetError;
use mysql_async::binlog::row::BinlogRow;
use mysql_async::binlog::value::BinlogValue;
use mysql_async::Value;
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
            Err(_) => Json::String(
                base64::engine::general_purpose::STANDARD.encode(b),
            ),
        },
        // Date(year, month, day, hour, minute, second, microsecond)
        Value::Date(y, mo, d, h, mi, s, micro) => Json::String(format!(
            "{y:04}-{mo:02}-{d:02} {h:02}:{mi:02}:{s:02}.{micro:06}"
        )),
        // Time(is_negative, days, hours, minutes, seconds, microseconds)
        Value::Time(neg, days, h, mi, s, micro) => {
            let sign = if *neg { "-" } else { "" };
            let total_hours = u32::from(*days) * 24 + u32::from(*h);
            Json::String(format!(
                "{sign}{total_hours:02}:{mi:02}:{s:02}.{micro:06}"
            ))
        }
    }
}

/// Convert one binlog value (which may be a plain value or a JSON/JSONB value).
///
/// - `Value(val)` → delegates to [`value_to_json`]
/// - `Jsonb(x)` → converts the MySQL JSONB representation to `serde_json::Value`
///   (falls back to `null` if conversion fails, e.g. opaque types)
/// - `JsonDiff(diffs)` → `"<JsonDiff>"` placeholder (refined in the stream loop)
pub fn binlog_value_to_json(v: &BinlogValue<'_>) -> Json {
    match v {
        BinlogValue::Value(val) => value_to_json(val),
        BinlogValue::Jsonb(jsonb_val) => {
            // mysql_common provides TryFrom<jsonb::Value<'_>> for serde_json::Value.
            // We clone-via-into_owned because we hold a borrow; the clone is
            // unavoidable here but JSONB columns are rare enough it doesn't matter.
            match serde_json::Value::try_from(jsonb_val.clone().into_owned()) {
                Ok(j) => j,
                Err(_) => Json::Null,
            }
        }
        BinlogValue::JsonDiff(_) => Json::String("<JsonDiff>".into()),
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
            Some(bv) => binlog_value_to_json(bv),
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
    fn float_nan_is_null() {
        let v = value_to_json(&Value::Float(f32::NAN));
        assert_eq!(v, Json::Null);
    }
}
