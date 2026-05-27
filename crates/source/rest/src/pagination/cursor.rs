//! Cursor/token-based pagination.

use faucet_core::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::Value;
use std::collections::HashMap;

pub fn apply_params(
    params: &mut HashMap<String, String>,
    param_name: &str,
    token: &Option<String>,
) {
    if let Some(t) = token {
        params.insert(param_name.to_string(), t.clone());
    }
}

pub fn advance(
    body: &Value,
    next_token_path: &str,
    next_token: &mut Option<String>,
) -> Result<bool, FaucetError> {
    let results = body
        .query(next_token_path)
        .map_err(|e| FaucetError::JsonPath(format!("{e}")))?;
    match results.first() {
        // Null or empty string → last page.
        Some(Value::Null) => {
            *next_token = None;
            Ok(false)
        }
        Some(Value::String(s)) => {
            if s.is_empty() {
                *next_token = None;
                Ok(false)
            } else {
                *next_token = Some(s.clone());
                Ok(true)
            }
        }
        // Numbers and booleans are scalar cursors — send their text form.
        Some(v @ (Value::Number(_) | Value::Bool(_))) => {
            *next_token = Some(v.to_string());
            Ok(true)
        }
        // An array/object cursor would be serialized to JSON and sent as a
        // query param, which is never what the API expects — almost always a
        // misconfigured `next_token_path`. Fail loudly instead (#78 LOW).
        Some(v @ (Value::Array(_) | Value::Object(_))) => Err(FaucetError::JsonPath(format!(
            "cursor path '{next_token_path}' resolved to a non-scalar value ({}); a pagination \
             cursor must be a string, number, or boolean",
            if v.is_array() { "array" } else { "object" }
        ))),
        None => {
            *next_token = None;
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn string_cursor_advances() {
        let body = json!({"next": "abc"});
        let mut tok = None;
        assert!(advance(&body, "$.next", &mut tok).unwrap());
        assert_eq!(tok, Some("abc".into()));
    }

    #[test]
    fn numeric_cursor_advances_as_string() {
        let body = json!({"next": 42});
        let mut tok = None;
        assert!(advance(&body, "$.next", &mut tok).unwrap());
        assert_eq!(tok, Some("42".into()));
    }

    #[test]
    fn null_and_empty_stop() {
        let mut tok = Some("stale".into());
        assert!(!advance(&json!({"next": null}), "$.next", &mut tok).unwrap());
        assert!(tok.is_none());
        let mut tok2 = Some("stale".into());
        assert!(!advance(&json!({"next": ""}), "$.next", &mut tok2).unwrap());
        assert!(tok2.is_none());
    }

    #[test]
    fn non_scalar_cursor_errors() {
        let mut tok = None;
        assert!(advance(&json!({"next": [1, 2]}), "$.next", &mut tok).is_err());
        assert!(advance(&json!({"next": {"a": 1}}), "$.next", &mut tok).is_err());
    }
}
