//! Type-aware conversion from Snowflake SQL REST API rows to JSON objects.
//!
//! The Snowflake SQL REST API returns every cell as a string, regardless of
//! its column type, and ships a schema (`resultSetMetaData.rowType`) alongside
//! the rows. This module pairs each cell with its column metadata and
//! produces a `serde_json::Value::Object` per row.
//!
//! Type coverage (Snowflake's `type` field is lowercase in the JSON v2 format):
//!
//! | Snowflake type          | JSON output                              |
//! |-------------------------|------------------------------------------|
//! | `fixed`                 | `Number` (i64 if it fits, else f64)      |
//! | `real`                  | `Number` (f64)                           |
//! | `boolean`               | `Bool`                                   |
//! | `text` / `binary`       | `String`                                 |
//! | `date` / `time` / `timestamp_*` | `String` (ISO/Unix-seconds as-is) |
//! | `variant` / `object` / `array` | parsed JSON value (falls back to `String`) |
//! | anything else           | `String` (raw cell)                      |
//!
//! `null` cells map to `Value::Null` regardless of declared type.

use serde::Deserialize;
use serde_json::{Map, Value};

/// One entry in `resultSetMetaData.rowType`. Only the fields we actually use
/// are deserialised; everything else is ignored.
#[derive(Debug, Clone, Deserialize)]
pub struct ColumnMeta {
    /// Column name as Snowflake reports it. Typically uppercase for
    /// unquoted identifiers; we keep the casing untouched.
    pub name: String,
    /// Low-level Snowflake type — lowercased identifiers like `"fixed"`,
    /// `"text"`, `"boolean"`, `"variant"`, `"timestamp_ntz"`. See the
    /// Snowflake [JSON v2 result format
    /// docs](https://docs.snowflake.com/en/developer-guide/sql-api/handling-responses).
    #[serde(rename = "type")]
    pub ty: String,
}

/// Build a JSON object out of one Snowflake row (an array of string cells).
///
/// `row` is expected to have the same length as `columns`. If the row is
/// shorter, missing trailing cells are treated as `null`; if it is longer,
/// the extras are silently ignored.
pub fn row_to_json(row: &[Value], columns: &[ColumnMeta]) -> Value {
    let mut obj = Map::with_capacity(columns.len());
    for (i, col) in columns.iter().enumerate() {
        let cell = row.get(i);
        obj.insert(col.name.clone(), cell_to_json(cell, &col.ty));
    }
    Value::Object(obj)
}

/// Convert a single Snowflake cell to a typed `serde_json::Value`.
fn cell_to_json(cell: Option<&Value>, ty: &str) -> Value {
    let Some(cell) = cell else {
        return Value::Null;
    };
    // SQL REST API ships `null` as a JSON null in the array.
    if cell.is_null() {
        return Value::Null;
    }
    // Pre-typed values (already a JSON number / bool / object) are passed
    // through verbatim. This is defensive — Snowflake's documented JSON v2
    // format always wraps cells as strings, but tolerating already-typed
    // values keeps the converter robust against future format tweaks.
    let s = match cell {
        Value::String(s) => s.as_str(),
        other => return other.clone(),
    };

    match ty.to_ascii_lowercase().as_str() {
        "fixed" => parse_number(s),
        "real" => parse_real(s),
        "boolean" => parse_bool(s),
        "variant" | "object" | "array" => {
            serde_json::from_str(s).unwrap_or_else(|_| Value::String(s.to_owned()))
        }
        _ => Value::String(s.to_owned()),
    }
}

/// Parse an integer-valued column (`FIXED`/`NUMBER` with scale 0 ships an
/// integer literal here; non-zero-scale columns ship a decimal string and
/// fall back to `f64`).
fn parse_number(s: &str) -> Value {
    if let Ok(i) = s.parse::<i64>() {
        return Value::Number(i.into());
    }
    parse_real(s)
}

/// Parse a floating-point column. Non-finite values (`Infinity`, `NaN`) are
/// not representable in JSON and fall back to a string for round-trip safety.
fn parse_real(s: &str) -> Value {
    match s.parse::<f64>() {
        Ok(f) => serde_json::Number::from_f64(f)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(s.to_owned())),
        Err(_) => Value::String(s.to_owned()),
    }
}

/// Parse a boolean column. Snowflake ships booleans as `"true"`/`"false"`
/// (lowercase) in the JSON v2 format, but we accept the canonical Snowflake
/// SQL forms (`"TRUE"`/`"FALSE"`, `"1"`/`"0"`) too.
fn parse_bool(s: &str) -> Value {
    match s {
        "true" | "TRUE" | "1" => Value::Bool(true),
        "false" | "FALSE" | "0" => Value::Bool(false),
        other => Value::String(other.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn col(name: &str, ty: &str) -> ColumnMeta {
        ColumnMeta {
            name: name.into(),
            ty: ty.into(),
        }
    }

    #[test]
    fn fixed_parses_as_integer() {
        let row = [json!("42")];
        let cols = [col("ID", "fixed")];
        assert_eq!(row_to_json(&row, &cols), json!({"ID": 42}));
    }

    #[test]
    fn fixed_decimal_falls_back_to_float() {
        let row = [json!("2.5")];
        let cols = [col("RATIO", "fixed")];
        assert_eq!(row_to_json(&row, &cols), json!({"RATIO": 2.5}));
    }

    #[test]
    fn real_parses_as_float() {
        let row = [json!("0.5")];
        let cols = [col("X", "real")];
        assert_eq!(row_to_json(&row, &cols), json!({"X": 0.5}));
    }

    #[test]
    fn boolean_parses_lowercase() {
        let row = [json!("true"), json!("false")];
        let cols = [col("A", "boolean"), col("B", "boolean")];
        assert_eq!(row_to_json(&row, &cols), json!({"A": true, "B": false}));
    }

    #[test]
    fn variant_parses_inner_json() {
        let row = [json!(r#"{"k":1}"#)];
        let cols = [col("DATA", "variant")];
        assert_eq!(row_to_json(&row, &cols), json!({"DATA": {"k": 1}}));
    }

    #[test]
    fn variant_falls_back_to_string_on_invalid_json() {
        let row = [json!("not-json")];
        let cols = [col("DATA", "variant")];
        assert_eq!(row_to_json(&row, &cols), json!({"DATA": "not-json"}));
    }

    #[test]
    fn text_passes_through() {
        let row = [json!("hello")];
        let cols = [col("NAME", "text")];
        assert_eq!(row_to_json(&row, &cols), json!({"NAME": "hello"}));
    }

    #[test]
    fn null_cell_maps_to_null() {
        let row = [json!(null)];
        let cols = [col("X", "fixed")];
        assert_eq!(row_to_json(&row, &cols), json!({"X": null}));
    }

    #[test]
    fn missing_trailing_cell_maps_to_null() {
        let row: [Value; 0] = [];
        let cols = [col("X", "text")];
        assert_eq!(row_to_json(&row, &cols), json!({"X": null}));
    }

    #[test]
    fn timestamp_passes_through_as_string() {
        let row = [json!("1700000000.000000000")];
        let cols = [col("TS", "timestamp_ntz")];
        assert_eq!(
            row_to_json(&row, &cols),
            json!({"TS": "1700000000.000000000"})
        );
    }

    #[test]
    fn unknown_type_passes_through_as_string() {
        let row = [json!("0xDEADBEEF")];
        let cols = [col("BLOB", "binary")];
        assert_eq!(row_to_json(&row, &cols), json!({"BLOB": "0xDEADBEEF"}));
    }

    #[test]
    fn non_finite_real_falls_back_to_string() {
        let row = [json!("NaN")];
        let cols = [col("X", "real")];
        // NaN is not representable in JSON; round-trip-safe fallback.
        assert_eq!(row_to_json(&row, &cols), json!({"X": "NaN"}));
    }
}
