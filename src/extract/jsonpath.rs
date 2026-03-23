//! JSONPath-based record extraction.

use crate::error::FaucetError;
use jsonpath_rust::JsonPath;
use serde_json::Value;

/// Extract records from a JSON response body using a JSONPath expression.
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_nested_records() {
        let body = json!({
            "data": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ],
            "meta": {"total": 2}
        });
        let records = extract_records(&body, Some("$.data[*]")).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["name"], "Alice");
    }

    #[test]
    fn test_extract_no_path_array() {
        let body = json!([{"id": 1}, {"id": 2}]);
        let records = extract_records(&body, None).unwrap();
        assert_eq!(records.len(), 2);
    }

    #[test]
    fn test_extract_no_path_object() {
        let body = json!({"id": 1, "name": "Alice"});
        let records = extract_records(&body, None).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["name"], "Alice");
    }

    #[test]
    fn test_extract_empty_result() {
        let body = json!({"data": []});
        let records = extract_records(&body, Some("$.data[*]")).unwrap();
        assert!(records.is_empty());
    }
}
