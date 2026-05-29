//! Per-record check evaluation. Pure functions over `&Value`. A check returns
//! `Ok(())` on pass or `Err(message)` on fail; the message is surfaced in the
//! DLQ envelope or the abort error.

use crate::quality::compile::{CompiledRecordCheck, CompiledRecordKind};
use crate::quality::config::{CompareOp, JsonType};
use serde_json::Value;

/// Evaluate one compiled per-record check against a record. `Ok(())` = pass;
/// `Err(message)` = fail (message is human-readable, used in DLQ/abort text).
pub fn evaluate_record_check(c: &CompiledRecordCheck, rec: &Value) -> Result<(), String> {
    match &c.kind {
        CompiledRecordKind::NotNull {
            path,
            treat_missing_as_null,
        } => match path.resolve(rec).ok().flatten() {
            Some(Value::Null) => Err("value was null".into()),
            Some(_) => Ok(()),
            None => {
                if *treat_missing_as_null {
                    Err("field was missing".into())
                } else {
                    Ok(())
                }
            }
        },
        CompiledRecordKind::NotEmpty { path } => match path.resolve(rec).ok().flatten() {
            Some(Value::String(s)) if !s.trim().is_empty() => Ok(()),
            Some(Value::String(_)) => Err("string was empty/whitespace".into()),
            Some(Value::Null) => Err("value was null".into()),
            Some(_) => Err("value was not a string".into()),
            None => Err("field was missing".into()),
        },
        CompiledRecordKind::RegexMatch { path, re } => match path.resolve(rec).ok().flatten() {
            Some(Value::String(s)) if re.is_match(s) => Ok(()),
            Some(Value::String(_)) => Err("value did not match pattern".into()),
            Some(_) => Err("value was not a string".into()),
            None => Err("field was missing".into()),
        },
        CompiledRecordKind::ValueInSet { path, values } => match path.resolve(rec).ok().flatten() {
            Some(v) if values.contains(v) => Ok(()),
            Some(_) => Err("value not in allowed set".into()),
            None => Err("field was missing".into()),
        },
        CompiledRecordKind::NotInSet { path, values } => match path.resolve(rec).ok().flatten() {
            Some(v) if values.contains(v) => Err("value is in the forbidden set".into()),
            // present-and-not-in-set OR missing -> pass
            _ => Ok(()),
        },
        CompiledRecordKind::Compare { path, op, value } => {
            let resolved = path.resolve(rec).ok().flatten();
            let Some(actual) = resolved else {
                return Err("field was missing".into());
            };
            evaluate_compare(*op, actual, value)
        }
        CompiledRecordKind::TypeIs { path, expected } => match path.resolve(rec).ok().flatten() {
            Some(v) if json_type_matches(v, *expected) => Ok(()),
            Some(_) => Err(format!("value was not of type {expected}")),
            None => Err("field was missing".into()),
        },
        CompiledRecordKind::StringLength { path, min, max } => {
            match path.resolve(rec).ok().flatten() {
                Some(Value::String(s)) => {
                    let len = s.chars().count();
                    if let Some(lo) = min
                        && len < *lo
                    {
                        return Err(format!("string length {len} < min {lo}"));
                    }
                    if let Some(hi) = max
                        && len > *hi
                    {
                        return Err(format!("string length {len} > max {hi}"));
                    }
                    Ok(())
                }
                Some(_) => Err("value was not a string".into()),
                None => Err("field was missing".into()),
            }
        }
        #[cfg(feature = "quality-jsonschema")]
        CompiledRecordKind::JsonSchema { validator } => {
            if validator.is_valid(rec) {
                Ok(())
            } else {
                let msg = validator
                    .iter_errors(rec)
                    .next()
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "record did not validate against schema".into());
                Err(msg)
            }
        }
    }
}

/// Evaluate a `compare` check. Note: ordering ops (`gt`/`gte`/`lt`/`lte`)
/// convert both operands via `as_f64()`, which loses precision for integer
/// magnitudes above 2^53; `eq`/`ne` use exact structural JSON equality.
fn evaluate_compare(op: CompareOp, actual: &Value, expected: &Value) -> Result<(), String> {
    match op {
        CompareOp::Eq => {
            if actual == expected {
                Ok(())
            } else {
                Err("values were not equal".into())
            }
        }
        CompareOp::Ne => {
            if actual != expected {
                Ok(())
            } else {
                Err("values were equal".into())
            }
        }
        CompareOp::Gt | CompareOp::Gte | CompareOp::Lt | CompareOp::Lte => {
            let (Some(a), Some(b)) = (actual.as_f64(), expected.as_f64()) else {
                return Err("value was not numeric".into());
            };
            let ok = match op {
                CompareOp::Gt => a > b,
                CompareOp::Gte => a >= b,
                CompareOp::Lt => a < b,
                CompareOp::Lte => a <= b,
                _ => unreachable!(),
            };
            if ok {
                Ok(())
            } else {
                Err(format!("comparison {a} {op} {b} failed"))
            }
        }
    }
}

fn json_type_matches(v: &Value, expected: JsonType) -> bool {
    matches!(
        (v, expected),
        (Value::Bool(_), JsonType::Boolean)
            | (Value::Number(_), JsonType::Number)
            | (Value::String(_), JsonType::String)
            | (Value::Array(_), JsonType::Array)
            | (Value::Object(_), JsonType::Object)
            | (Value::Null, JsonType::Null)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::quality::compile::CompiledQuality;
    use crate::quality::config::{CompareOp, JsonType, OnFailure, QualitySpec, RecordCheck};
    use serde_json::json;

    fn one(check: RecordCheck) -> crate::quality::compile::CompiledRecordCheck {
        let spec = QualitySpec {
            record: vec![check],
            batch: vec![],
        };
        CompiledQuality::compile(&spec)
            .unwrap()
            .record
            .pop()
            .unwrap()
    }

    #[test]
    fn not_null_passes_present_fails_missing_and_null() {
        let c = one(RecordCheck::NotNull {
            field: "id".into(),
            treat_missing_as_null: true,
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"id": 1})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"id": null})).is_err());
        assert!(evaluate_record_check(&c, &json!({})).is_err());
    }

    #[test]
    fn not_null_treat_missing_false_only_explicit_null_fails() {
        let c = one(RecordCheck::NotNull {
            field: "id".into(),
            treat_missing_as_null: false,
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({})).is_ok()); // missing -> pass
        assert!(evaluate_record_check(&c, &json!({"id": null})).is_err());
    }

    #[test]
    fn not_empty() {
        let c = one(RecordCheck::NotEmpty {
            field: "name".into(),
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"name": "x"})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"name": "  "})).is_err());
        assert!(evaluate_record_check(&c, &json!({"name": ""})).is_err());
        assert!(evaluate_record_check(&c, &json!({"name": 5})).is_err()); // non-string
        assert!(evaluate_record_check(&c, &json!({})).is_err()); // missing
    }

    #[test]
    fn regex_match() {
        let c = one(RecordCheck::RegexMatch {
            field: "email".into(),
            pattern: r"^[^@]+@[^@]+\.[^@]+$".into(),
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"email": "a@b.com"})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"email": "nope"})).is_err());
        assert!(evaluate_record_check(&c, &json!({"email": 1})).is_err());
        assert!(evaluate_record_check(&c, &json!({})).is_err());
    }

    #[test]
    fn value_in_set_and_not_in_set_missing_handling() {
        let in_set = one(RecordCheck::ValueInSet {
            field: "status".into(),
            values: vec![json!("active"), json!("closed")],
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&in_set, &json!({"status": "active"})).is_ok());
        assert!(evaluate_record_check(&in_set, &json!({"status": "x"})).is_err());
        assert!(evaluate_record_check(&in_set, &json!({})).is_err()); // missing -> fail

        let not_in = one(RecordCheck::NotInSet {
            field: "status".into(),
            values: vec![json!("banned")],
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&not_in, &json!({"status": "active"})).is_ok());
        assert!(evaluate_record_check(&not_in, &json!({"status": "banned"})).is_err());
        assert!(evaluate_record_check(&not_in, &json!({})).is_ok()); // missing -> pass
    }

    #[test]
    fn compare_ordering_and_equality() {
        let gte = one(RecordCheck::Compare {
            field: "age".into(),
            op: CompareOp::Gte,
            value: json!(0),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&gte, &json!({"age": 0})).is_ok());
        assert!(evaluate_record_check(&gte, &json!({"age": -1})).is_err());
        assert!(evaluate_record_check(&gte, &json!({"age": "x"})).is_err()); // non-numeric
        assert!(evaluate_record_check(&gte, &json!({})).is_err()); // missing

        let eq = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Eq,
            value: json!(5),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&eq, &json!({"v": 5})).is_ok());
        assert!(evaluate_record_check(&eq, &json!({"v": "5"})).is_err()); // no coercion

        let ne = one(RecordCheck::Compare {
            field: "v".into(),
            op: CompareOp::Ne,
            value: json!(5),
            on_failure: OnFailure::Abort,
        });
        assert!(evaluate_record_check(&ne, &json!({"v": 6})).is_ok()); // 6 != 5 -> pass
        assert!(evaluate_record_check(&ne, &json!({"v": 5})).is_err()); // 5 == 5 -> fail
    }

    #[test]
    fn not_empty_null_reports_null() {
        let c = one(RecordCheck::NotEmpty {
            field: "name".into(),
            on_failure: OnFailure::Quarantine,
        });
        let err = evaluate_record_check(&c, &json!({"name": null})).unwrap_err();
        assert!(err.contains("null"));
    }

    #[test]
    fn type_is() {
        let b = one(RecordCheck::TypeIs {
            field: "active".into(),
            expected: JsonType::Boolean,
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&b, &json!({"active": true})).is_ok());
        assert!(evaluate_record_check(&b, &json!({"active": 1})).is_err());
        assert!(evaluate_record_check(&b, &json!({})).is_err());

        let n = one(RecordCheck::TypeIs {
            field: "x".into(),
            expected: JsonType::Null,
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&n, &json!({"x": null})).is_ok());
        assert!(evaluate_record_check(&n, &json!({})).is_err()); // missing != null
    }

    #[test]
    fn string_length() {
        let c = one(RecordCheck::StringLength {
            field: "name".into(),
            min: Some(1),
            max: Some(3),
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"name": "ab"})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"name": ""})).is_err());
        assert!(evaluate_record_check(&c, &json!({"name": "abcd"})).is_err());
        assert!(evaluate_record_check(&c, &json!({"name": "é"})).is_ok()); // 1 char
        assert!(evaluate_record_check(&c, &json!({"name": 5})).is_err());
    }

    #[cfg(feature = "quality-jsonschema")]
    #[test]
    fn json_schema() {
        let c = one(RecordCheck::JsonSchema {
            schema: json!({"type": "object", "required": ["id"], "properties": {"id": {"type": "integer"}}}),
            on_failure: OnFailure::Quarantine,
        });
        assert!(evaluate_record_check(&c, &json!({"id": 1})).is_ok());
        assert!(evaluate_record_check(&c, &json!({"id": "x"})).is_err());
        assert!(evaluate_record_check(&c, &json!({})).is_err());
    }
}
