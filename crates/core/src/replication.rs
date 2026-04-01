//! Incremental replication support.

use serde_json::Value;
use std::cmp::Ordering;

/// Determines how records are replicated from the source.
#[derive(Debug, Clone, Default, PartialEq)]
pub enum ReplicationMethod {
    /// All records are fetched on every run (default).
    #[default]
    FullTable,
    /// Only records where the `replication_key` field is strictly greater than
    /// the stored bookmark (`start_replication_value`) are kept.
    Incremental,
}

/// Filter `records` to only those where `record[key] > start`.
///
/// Records that are missing the key are excluded.
/// String values are compared lexicographically (ISO-8601 dates compare correctly this way).
/// Numeric values are compared as `f64`.
pub fn filter_incremental(records: Vec<Value>, key: &str, start: &Value) -> Vec<Value> {
    records
        .into_iter()
        .filter(|r| r.get(key).is_some_and(|v| json_gt(v, start)))
        .collect()
}

/// Return the maximum value of `record[key]` across all records, if any.
pub fn max_replication_value<'a>(records: &'a [Value], key: &str) -> Option<&'a Value> {
    records
        .iter()
        .filter_map(|r| r.get(key))
        .max_by(|a, b| json_compare(a, b))
}

pub(crate) fn json_compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            let af = an.as_f64().unwrap_or(f64::NEG_INFINITY);
            let bf = bn.as_f64().unwrap_or(f64::NEG_INFINITY);
            af.partial_cmp(&bf).unwrap_or(Ordering::Equal)
        }
        (Value::String(as_), Value::String(bs)) => as_.cmp(bs),
        _ => Ordering::Equal,
    }
}

fn json_gt(a: &Value, b: &Value) -> bool {
    json_compare(a, b) == Ordering::Greater
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_filter_incremental_strings() {
        let records = vec![
            json!({"id": 1, "updated_at": "2024-01-01"}),
            json!({"id": 2, "updated_at": "2024-06-01"}),
            json!({"id": 3, "updated_at": "2024-12-01"}),
        ];
        let start = json!("2024-06-01");
        let filtered = filter_incremental(records, "updated_at", &start);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["id"], 3);
    }

    #[test]
    fn test_filter_incremental_numbers() {
        let records = vec![
            json!({"id": 1, "seq": 100}),
            json!({"id": 2, "seq": 200}),
            json!({"id": 3, "seq": 300}),
        ];
        let start = json!(150);
        let filtered = filter_incremental(records, "seq", &start);
        assert_eq!(filtered.len(), 2);
        assert_eq!(filtered[0]["id"], 2);
        assert_eq!(filtered[1]["id"], 3);
    }

    #[test]
    fn test_filter_incremental_missing_key_excluded() {
        let records = vec![
            json!({"id": 1}),
            json!({"id": 2, "updated_at": "2024-12-01"}),
        ];
        let start = json!("2024-01-01");
        let filtered = filter_incremental(records, "updated_at", &start);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["id"], 2);
    }

    #[test]
    fn test_filter_incremental_equal_excluded() {
        let records = vec![
            json!({"id": 1, "updated_at": "2024-06-01"}),
            json!({"id": 2, "updated_at": "2024-06-02"}),
        ];
        let start = json!("2024-06-01");
        let filtered = filter_incremental(records, "updated_at", &start);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0]["id"], 2);
    }

    #[test]
    fn test_max_replication_value_strings() {
        let records = vec![
            json!({"updated_at": "2024-01-01"}),
            json!({"updated_at": "2024-12-01"}),
            json!({"updated_at": "2024-06-01"}),
        ];
        let max = max_replication_value(&records, "updated_at").unwrap();
        assert_eq!(max, &json!("2024-12-01"));
    }

    #[test]
    fn test_max_replication_value_numbers() {
        let records = vec![json!({"seq": 5}), json!({"seq": 10}), json!({"seq": 3})];
        let max = max_replication_value(&records, "seq").unwrap();
        assert_eq!(max, &json!(10));
    }

    #[test]
    fn test_max_replication_value_empty() {
        let records: Vec<Value> = vec![];
        assert!(max_replication_value(&records, "updated_at").is_none());
    }
}
