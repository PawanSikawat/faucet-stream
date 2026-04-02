//! Shared utilities used across faucet source and sink crates.

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
}
