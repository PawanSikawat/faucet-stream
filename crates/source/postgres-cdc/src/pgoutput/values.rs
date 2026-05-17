//! Postgres type OID -> JSON Value mapping for text-mode tuple cells.
//!
//! Reference: <https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat>
//! Only the OIDs that ship with every Postgres install are special-cased;
//! anything else falls back to a JSON string.

use base64::Engine;
use faucet_core::FaucetError;
use serde_json::Value;

// Selected pg_type built-in OIDs.
const OID_BOOL: u32 = 16;
const OID_BYTEA: u32 = 17;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_INT8: u32 = 20;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_NUMERIC: u32 = 1700;
const OID_JSON: u32 = 114;
const OID_JSONB: u32 = 3802;
// date/time/timestamp/timestamptz fall through to string — Postgres'
// canonical text form is already ISO-8601-ish and downstream consumers
// don't agree on a single binary encoding.

/// Decode a text-encoded value with the given column type OID into JSON.
///
/// Unknown OIDs and decode failures both fall back to wrapping the raw text
/// in a JSON string — this is the safest default for a generic CDC
/// connector. We never panic on bad data; the only way this returns `Err` is
/// if a structurally promised invariant is violated (e.g. `OID_BYTEA` text
/// that doesn't start with `\x`).
pub fn text_to_json(type_oid: u32, text: &str) -> Result<Value, FaucetError> {
    Ok(match type_oid {
        OID_BOOL => match text {
            "t" => Value::Bool(true),
            "f" => Value::Bool(false),
            other => {
                return Err(FaucetError::Source(format!(
                    "pgoutput: bool column has non-t/f text {other:?}"
                )));
            }
        },
        OID_INT2 | OID_INT4 | OID_INT8 => {
            let n: i64 = text.parse().map_err(|e| {
                FaucetError::Source(format!(
                    "pgoutput: int (oid={type_oid}) parse {text:?}: {e}"
                ))
            })?;
            Value::from(n)
        }
        OID_FLOAT4 | OID_FLOAT8 => match text {
            "NaN" | "Infinity" | "-Infinity" => Value::Null,
            other => {
                let n: f64 = other.parse().map_err(|e| {
                    FaucetError::Source(format!("pgoutput: float parse {text:?}: {e}"))
                })?;
                serde_json::Number::from_f64(n)
                    .map(Value::Number)
                    .unwrap_or(Value::Null)
            }
        },
        OID_NUMERIC => Value::String(text.into()),
        OID_BYTEA => {
            let stripped = text.strip_prefix("\\x").ok_or_else(|| {
                FaucetError::Source(format!(
                    "pgoutput: bytea text {text:?} missing '\\x' prefix"
                ))
            })?;
            let bytes = hex_decode(stripped)?;
            Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
        }
        OID_JSON | OID_JSONB => serde_json::from_str(text).map_err(|e| {
            FaucetError::Source(format!("pgoutput: json/jsonb parse {text:?}: {e}"))
        })?,
        _ => Value::String(text.into()),
    })
}

fn hex_decode(s: &str) -> Result<Vec<u8>, FaucetError> {
    if !s.len().is_multiple_of(2) {
        return Err(FaucetError::Source(format!(
            "pgoutput: bytea hex has odd length: {s:?}"
        )));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    for i in (0..s.len()).step_by(2) {
        out.push(
            u8::from_str_radix(&s[i..i + 2], 16)
                .map_err(|e| FaucetError::Source(format!("pgoutput: bytea hex {s:?}: {e}")))?,
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn bool_t_and_f() {
        assert_eq!(text_to_json(OID_BOOL, "t").unwrap(), json!(true));
        assert_eq!(text_to_json(OID_BOOL, "f").unwrap(), json!(false));
        assert!(text_to_json(OID_BOOL, "yes").is_err());
    }

    #[test]
    fn integer_types() {
        assert_eq!(text_to_json(OID_INT2, "32000").unwrap(), json!(32000));
        assert_eq!(text_to_json(OID_INT4, "-1").unwrap(), json!(-1));
        assert_eq!(
            text_to_json(OID_INT8, "9223372036854775807").unwrap(),
            json!(9223372036854775807_i64)
        );
        assert!(text_to_json(OID_INT4, "abc").is_err());
    }

    #[test]
    fn floats() {
        assert_eq!(text_to_json(OID_FLOAT8, "3.5").unwrap(), json!(3.5));
        assert_eq!(text_to_json(OID_FLOAT8, "NaN").unwrap(), Value::Null);
        assert_eq!(text_to_json(OID_FLOAT4, "Infinity").unwrap(), Value::Null);
    }

    #[test]
    fn numeric_kept_as_string() {
        assert_eq!(
            text_to_json(OID_NUMERIC, "12345.67890").unwrap(),
            json!("12345.67890")
        );
    }

    #[test]
    fn bytea_base64() {
        // \xDEADBEEF -> base64 "3q2+7w=="
        assert_eq!(
            text_to_json(OID_BYTEA, "\\xDEADBEEF").unwrap(),
            json!("3q2+7w==")
        );
        assert!(text_to_json(OID_BYTEA, "deadbeef").is_err()); // missing \x
        assert!(text_to_json(OID_BYTEA, "\\xZZ").is_err()); // not hex
    }

    #[test]
    fn json_columns_parsed() {
        assert_eq!(
            text_to_json(OID_JSON, r#"{"a":1}"#).unwrap(),
            json!({"a": 1})
        );
        assert_eq!(
            text_to_json(OID_JSONB, r#"[1,2,3]"#).unwrap(),
            json!([1, 2, 3])
        );
    }

    #[test]
    fn unknown_oid_falls_back_to_string() {
        assert_eq!(
            text_to_json(99999, "2026-05-17 12:34:56+00").unwrap(),
            json!("2026-05-17 12:34:56+00")
        );
    }
}
