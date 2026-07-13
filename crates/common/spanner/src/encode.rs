//! `serde_json::Value` → Spanner mutation value encoding.
//!
//! The sink writes arbitrary JSON records, so values are encoded against the
//! *destination column type* (read once from `INFORMATION_SCHEMA`). Mutations
//! only carry `prost_types::value::Kind`s — Spanner infers the type from the
//! table schema — so the object-safe [`ToKind`] wrapper [`EncodedKind`] is
//! all the mutation builders need.

use crate::types::SpannerType;
use gcloud_googleapis::spanner::v1::{Type, TypeCode};
use gcloud_spanner::statement::{ToKind, single_type};
use prost_types::value::Kind;
use serde_json::Value;

/// A pre-encoded mutation value. `get_type` reports STRING but is never
/// consulted on the mutation path (mutations infer types server-side from
/// the table schema); this wrapper must not be used for statement params.
pub struct EncodedKind(pub Kind);

impl ToKind for EncodedKind {
    fn to_kind(&self) -> Kind {
        self.0.clone()
    }
    fn get_type() -> Type
    where
        Self: Sized,
    {
        single_type(TypeCode::String)
    }
}

/// Encode one JSON value for a destination column of type `ty`.
///
/// Returns a human-readable error naming the mismatch; callers wrap it with
/// the column name and route the row to the DLQ or fail the batch.
pub fn encode_to_kind(value: &Value, ty: &SpannerType) -> Result<Kind, String> {
    if value.is_null() {
        return Ok(Kind::NullValue(0));
    }
    match ty {
        SpannerType::Bool => match value {
            Value::Bool(b) => Ok(Kind::BoolValue(*b)),
            other => Err(mismatch("BOOL", other)),
        },
        // INT64 travels string-encoded (f64 would corrupt > 2^53).
        SpannerType::Int64 => match value {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(Kind::StringValue(i.to_string()))
                } else if let Some(u) = n.as_u64() {
                    // > i64::MAX cannot be stored in INT64.
                    i64::try_from(u)
                        .map(|i| Kind::StringValue(i.to_string()))
                        .map_err(|_| format!("integer {u} overflows INT64"))
                } else {
                    Err(mismatch("INT64", value))
                }
            }
            // Allow pre-stringified integers (common after transforms).
            Value::String(s) if s.parse::<i64>().is_ok() => Ok(Kind::StringValue(s.clone())),
            other => Err(mismatch("INT64", other)),
        },
        SpannerType::Float32 | SpannerType::Float64 => match value {
            Value::Number(n) => n
                .as_f64()
                .map(Kind::NumberValue)
                .ok_or_else(|| mismatch("FLOAT64", value)),
            // NaN / Infinity travel as strings.
            Value::String(s) if matches!(s.as_str(), "NaN" | "Infinity" | "-Infinity") => {
                Ok(Kind::StringValue(s.clone()))
            }
            other => Err(mismatch("FLOAT64", other)),
        },
        SpannerType::String => match value {
            Value::String(s) => Ok(Kind::StringValue(s.clone())),
            // Scalars coerce to their canonical text; containers serialize as
            // JSON text so a bad mapping surfaces visibly, not silently.
            Value::Number(n) => Ok(Kind::StringValue(n.to_string())),
            Value::Bool(b) => Ok(Kind::StringValue(b.to_string())),
            other => Ok(Kind::StringValue(other.to_string())),
        },
        // Timestamps/dates must already be RFC 3339 text; bytes must already
        // be base64 (the same form the source decodes them to).
        SpannerType::Timestamp | SpannerType::Date | SpannerType::Bytes => match value {
            Value::String(s) => Ok(Kind::StringValue(s.clone())),
            other => Err(mismatch(type_name(ty), other)),
        },
        SpannerType::Numeric => match value {
            Value::String(s) => Ok(Kind::StringValue(s.clone())),
            Value::Number(n) => Ok(Kind::StringValue(n.to_string())),
            other => Err(mismatch("NUMERIC", other)),
        },
        // JSON columns take the value's RFC 7159 serialization verbatim.
        SpannerType::Json => Ok(Kind::StringValue(value.to_string())),
        SpannerType::Array(inner) => match value {
            Value::Array(items) => {
                let values: Result<Vec<prost_types::Value>, String> = items
                    .iter()
                    .map(|item| {
                        encode_to_kind(item, inner)
                            .map(|kind| prost_types::Value { kind: Some(kind) })
                    })
                    .collect();
                Ok(Kind::ListValue(prost_types::ListValue { values: values? }))
            }
            other => Err(mismatch("ARRAY", other)),
        },
        SpannerType::Other => Err(
            "unsupported destination column type (STRUCT/PROTO columns are not writable)".into(),
        ),
    }
}

fn type_name(ty: &SpannerType) -> &'static str {
    match ty {
        SpannerType::Timestamp => "TIMESTAMP",
        SpannerType::Date => "DATE",
        SpannerType::Bytes => "BYTES",
        _ => "value",
    }
}

fn mismatch(expected: &str, got: &Value) -> String {
    let got_ty = match got {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    };
    format!("expected {expected}-compatible value, got {got_ty}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn null_encodes_as_null_for_every_type() {
        for ty in [
            SpannerType::Bool,
            SpannerType::Int64,
            SpannerType::Json,
            SpannerType::Array(Box::new(SpannerType::String)),
        ] {
            assert!(matches!(
                encode_to_kind(&Value::Null, &ty).unwrap(),
                Kind::NullValue(_)
            ));
        }
    }

    #[test]
    fn int64_encodes_as_string_losslessly() {
        let big = 9_007_199_254_740_993_i64;
        assert_eq!(
            encode_to_kind(&json!(big), &SpannerType::Int64).unwrap(),
            Kind::StringValue(big.to_string())
        );
        assert_eq!(
            encode_to_kind(&json!("42"), &SpannerType::Int64).unwrap(),
            Kind::StringValue("42".into())
        );
        assert!(encode_to_kind(&json!(1.5), &SpannerType::Int64).is_err());
        assert!(encode_to_kind(&json!("abc"), &SpannerType::Int64).is_err());
        assert!(
            encode_to_kind(&json!(u64::MAX), &SpannerType::Int64)
                .unwrap_err()
                .contains("overflows INT64")
        );
    }

    #[test]
    fn floats_bools_and_special_values() {
        assert_eq!(
            encode_to_kind(&json!(2.5), &SpannerType::Float64).unwrap(),
            Kind::NumberValue(2.5)
        );
        assert_eq!(
            encode_to_kind(&json!("NaN"), &SpannerType::Float64).unwrap(),
            Kind::StringValue("NaN".into())
        );
        assert!(encode_to_kind(&json!("2.5"), &SpannerType::Float64).is_err());
        assert_eq!(
            encode_to_kind(&json!(true), &SpannerType::Bool).unwrap(),
            Kind::BoolValue(true)
        );
        assert!(encode_to_kind(&json!(1), &SpannerType::Bool).is_err());
    }

    #[test]
    fn strings_coerce_scalars_and_serialize_containers() {
        assert_eq!(
            encode_to_kind(&json!("x"), &SpannerType::String).unwrap(),
            Kind::StringValue("x".into())
        );
        assert_eq!(
            encode_to_kind(&json!(7), &SpannerType::String).unwrap(),
            Kind::StringValue("7".into())
        );
        assert_eq!(
            encode_to_kind(&json!({"a": 1}), &SpannerType::String).unwrap(),
            Kind::StringValue("{\"a\":1}".into())
        );
    }

    #[test]
    fn timestamp_date_bytes_require_strings() {
        assert_eq!(
            encode_to_kind(&json!("2026-01-01T00:00:00Z"), &SpannerType::Timestamp).unwrap(),
            Kind::StringValue("2026-01-01T00:00:00Z".into())
        );
        assert!(encode_to_kind(&json!(5), &SpannerType::Timestamp).is_err());
        assert!(encode_to_kind(&json!(5), &SpannerType::Date).is_err());
        assert!(encode_to_kind(&json!(5), &SpannerType::Bytes).is_err());
    }

    #[test]
    fn numeric_accepts_strings_and_numbers() {
        assert_eq!(
            encode_to_kind(&json!("123.456789012345"), &SpannerType::Numeric).unwrap(),
            Kind::StringValue("123.456789012345".into())
        );
        assert_eq!(
            encode_to_kind(&json!(12), &SpannerType::Numeric).unwrap(),
            Kind::StringValue("12".into())
        );
        assert!(encode_to_kind(&json!(true), &SpannerType::Numeric).is_err());
    }

    #[test]
    fn json_columns_take_any_value() {
        assert_eq!(
            encode_to_kind(&json!({"a": [1]}), &SpannerType::Json).unwrap(),
            Kind::StringValue("{\"a\":[1]}".into())
        );
        // A JSON string value serializes as a quoted JSON string.
        assert_eq!(
            encode_to_kind(&json!("s"), &SpannerType::Json).unwrap(),
            Kind::StringValue("\"s\"".into())
        );
    }

    #[test]
    fn arrays_recurse_and_reject_non_arrays() {
        let ty = SpannerType::Array(Box::new(SpannerType::Int64));
        let Kind::ListValue(list) = encode_to_kind(&json!([1, null, 3]), &ty).unwrap() else {
            panic!("expected list");
        };
        assert_eq!(list.values.len(), 3);
        assert!(encode_to_kind(&json!(1), &ty).is_err());
        // Element mismatch surfaces.
        assert!(encode_to_kind(&json!(["x"]), &ty).is_err());
    }

    #[test]
    fn unsupported_destination_types_error() {
        assert!(encode_to_kind(&json!(1), &SpannerType::Other).is_err());
    }

    #[test]
    fn encoded_kind_is_a_usable_tokind() {
        let ek = EncodedKind(Kind::StringValue("v".into()));
        assert_eq!(ek.to_kind(), Kind::StringValue("v".into()));
        let dyn_ref: &dyn ToKind = &ek;
        assert_eq!(dyn_ref.to_kind(), Kind::StringValue("v".into()));
    }
}
