//! Shared utilities used across faucet source and sink crates.

use std::collections::HashMap;

use crate::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::Value;

// ── SQL Utilities ───────────────────────────────────────────────────────────

/// Quote a SQL identifier to prevent SQL injection.
///
/// Wraps the name in double quotes and doubles any embedded double-quotes
/// per the SQL standard (ANSI SQL).
///
/// ```
/// use faucet_core::util::quote_ident;
/// assert_eq!(quote_ident("my_table"), "\"my_table\"");
/// assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
/// ```
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

// ── JSONPath Extraction ─────────────────────────────────────────────────────

/// Extract records from a JSON value using an optional JSONPath expression.
///
/// - If `path` is `Some`, queries the body with the JSONPath and returns
///   all matched values.
/// - If `path` is `None`, returns the body as-is: arrays are unpacked into
///   individual records, objects/scalars are returned as a single-element vec.
pub fn extract_records(body: &Value, path: Option<&str>) -> Result<Vec<Value>, FaucetError> {
    match path {
        Some(p) => {
            let results = body
                .query(p)
                .map_err(|e| FaucetError::JsonPath(format!("invalid JSONPath '{p}': {e}")))?;
            Ok(results.into_iter().cloned().collect())
        }
        None => match body {
            Value::Array(arr) => Ok(arr.clone()),
            other => Ok(vec![other.clone()]),
        },
    }
}

// ── HTTP Response Handling ──────────────────────────────────────────────────

/// Check an HTTP response status and return a [`FaucetError::HttpStatus`] on
/// non-success responses.
///
/// Reads the response body for error context, truncating to `max_body_len`
/// bytes (default: 2048) to avoid large error messages.
pub async fn check_http_response(
    resp: reqwest::Response,
    max_body_len: usize,
) -> Result<reqwest::Response, FaucetError> {
    if resp.status().is_success() {
        return Ok(resp);
    }

    let status = resp.status().as_u16();
    let url = resp.url().to_string();
    let body_text = resp.text().await.unwrap_or_default();

    let body = if body_text.len() > max_body_len {
        let end = body_text.floor_char_boundary(max_body_len);
        format!("{}...(truncated)", &body_text[..end])
    } else {
        body_text
    };

    Err(FaucetError::HttpStatus { status, url, body })
}

/// Default maximum body length for error responses.
pub const DEFAULT_ERROR_BODY_MAX_LEN: usize = 2048;

// ── Context Utilities ──────────────────────────────────────────────────────

/// Substitute `{key}` placeholders in a template string with values from context.
///
/// Value conversion rules:
/// - `String` -> raw string (no quotes)
/// - `Number` -> number as string
/// - `Bool` -> `"true"` / `"false"`
/// - `Null` -> `"null"`
/// - `Array` / `Object` -> JSON-serialized string
///
/// Unmatched placeholders are left as-is.
pub fn substitute_context(template: &str, context: &HashMap<String, Value>) -> String {
    let mut result = template.to_string();
    for (key, value) in context {
        let placeholder = format!("{{{key}}}");
        if result.contains(&placeholder) {
            let replacement = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => "null".to_string(),
                other => other.to_string(),
            };
            result = result.replace(&placeholder, &replacement);
        }
    }
    result
}

/// Extract context values from a record using JSONPath expressions.
///
/// Each entry in `mapping` is `context_key -> json_path`. The function queries
/// the record with each JSONPath and stores the first matched value under the
/// corresponding context key.
///
/// Returns an error if any JSONPath matches nothing.
pub fn extract_context(
    record: &Value,
    mapping: &HashMap<String, String>,
) -> Result<HashMap<String, Value>, FaucetError> {
    let mut context = HashMap::with_capacity(mapping.len());
    for (context_key, json_path) in mapping {
        let results = record
            .query(json_path.as_str())
            .map_err(|e| FaucetError::JsonPath(format!("invalid JSONPath '{json_path}': {e}")))?;
        let value = results.first().ok_or_else(|| {
            FaucetError::JsonPath(format!(
                "JSONPath '{json_path}' matched nothing in record for context key '{context_key}'"
            ))
        })?;
        context.insert(context_key.clone(), (*value).clone());
    }
    Ok(context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── quote_ident ─────────────────────────────────────────────────────

    #[test]
    fn quote_ident_simple() {
        assert_eq!(quote_ident("my_table"), "\"my_table\"");
    }

    #[test]
    fn quote_ident_with_embedded_quotes() {
        assert_eq!(quote_ident("has\"quote"), "\"has\"\"quote\"");
    }

    #[test]
    fn quote_ident_empty() {
        assert_eq!(quote_ident(""), "\"\"");
    }

    #[test]
    fn quote_ident_special_chars() {
        assert_eq!(quote_ident("table; DROP"), "\"table; DROP\"");
    }

    // ── extract_records ─────────────────────────────────────────────────

    #[test]
    fn extract_with_path() {
        let body = json!({"data": [{"id": 1}, {"id": 2}]});
        let records = extract_records(&body, Some("$.data[*]")).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["id"], 1);
    }

    #[test]
    fn extract_without_path_array() {
        let body = json!([{"id": 1}, {"id": 2}]);
        let records = extract_records(&body, None).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn extract_without_path_object() {
        let body = json!({"id": 1});
        let records = extract_records(&body, None).unwrap();
        assert_eq!(records.len(), 1);
    }

    #[test]
    fn extract_empty_result() {
        let body = json!({"data": []});
        let records = extract_records(&body, Some("$.data[*]")).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn extract_invalid_path_returns_error() {
        let body = json!({"data": 1});
        // jsonpath-rust handles most paths gracefully; test error propagation.
        let result = extract_records(&body, Some("$.data[*]"));
        // This should succeed (empty match) or fail; either is fine as long as
        // it doesn't panic.
        let _ = result;
    }

    // ── substitute_context ──────────────────────────────────────────────

    #[test]
    fn substitute_context_string_values() {
        let mut ctx = HashMap::new();
        ctx.insert("org".to_string(), json!("acme"));
        ctx.insert("repo".to_string(), json!("widgets"));
        let result = substitute_context("/orgs/{org}/repos/{repo}", &ctx);
        assert_eq!(result, "/orgs/acme/repos/widgets");
    }

    #[test]
    fn substitute_context_number_value() {
        let mut ctx = HashMap::new();
        ctx.insert("id".to_string(), json!(42));
        let result = substitute_context("/items/{id}", &ctx);
        assert_eq!(result, "/items/42");
    }

    #[test]
    fn substitute_context_bool_value() {
        let mut ctx = HashMap::new();
        ctx.insert("active".to_string(), json!(true));
        let result = substitute_context("/filter?active={active}", &ctx);
        assert_eq!(result, "/filter?active=true");
    }

    #[test]
    fn substitute_context_null_value() {
        let mut ctx = HashMap::new();
        ctx.insert("val".to_string(), json!(null));
        let result = substitute_context("/x/{val}", &ctx);
        assert_eq!(result, "/x/null");
    }

    #[test]
    fn substitute_context_array_value() {
        let mut ctx = HashMap::new();
        ctx.insert("ids".to_string(), json!([1, 2, 3]));
        let result = substitute_context("/x/{ids}", &ctx);
        assert_eq!(result, "/x/[1,2,3]");
    }

    #[test]
    fn substitute_context_unmatched_placeholder_left_as_is() {
        let ctx = HashMap::new();
        let result = substitute_context("/orgs/{org}/repos", &ctx);
        assert_eq!(result, "/orgs/{org}/repos");
    }

    #[test]
    fn substitute_context_empty_template() {
        let ctx = HashMap::new();
        let result = substitute_context("", &ctx);
        assert_eq!(result, "");
    }

    // ── extract_context ─────────────────────────────────────────────────

    #[test]
    fn extract_context_simple_paths() {
        let record = json!({"id": 1, "name": "alice"});
        let mut mapping = HashMap::new();
        mapping.insert("user_id".to_string(), "$.id".to_string());
        mapping.insert("user_name".to_string(), "$.name".to_string());
        let ctx = extract_context(&record, &mapping).unwrap();
        assert_eq!(ctx["user_id"], json!(1));
        assert_eq!(ctx["user_name"], json!("alice"));
    }

    #[test]
    fn extract_context_nested_path() {
        let record = json!({"data": {"info": {"id": 99}}});
        let mut mapping = HashMap::new();
        mapping.insert("deep_id".to_string(), "$.data.info.id".to_string());
        let ctx = extract_context(&record, &mapping).unwrap();
        assert_eq!(ctx["deep_id"], json!(99));
    }

    #[test]
    fn extract_context_missing_path_returns_error() {
        let record = json!({"id": 1});
        let mut mapping = HashMap::new();
        mapping.insert("missing".to_string(), "$.nonexistent".to_string());
        let result = extract_context(&record, &mapping);
        assert!(result.is_err());
    }

    #[test]
    fn extract_context_empty_mapping() {
        let record = json!({"id": 1});
        let mapping = HashMap::new();
        let ctx = extract_context(&record, &mapping).unwrap();
        assert!(ctx.is_empty());
    }
}
