//! JSONPath-based record extraction.
//!
//! This module re-exports [`faucet_core::util::extract_records`] for backwards
//! compatibility. New code should use `faucet_core::util::extract_records`
//! directly.

pub use faucet_core::util::extract_records;

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
