//! Incremental replication support.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;

/// Determines how records are replicated from the source.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type")]
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
/// Records missing the key are excluded. Strings compare lexicographically
/// (ISO-8601 dates compare correctly this way); integers compare exactly
/// (no `f64` precision loss); floats compare as `f64`.
///
/// If a record's key value is a *different JSON type* than `start` (e.g. a
/// numeric key against a string bookmark), the comparison is not meaningful;
/// rather than silently dropping the record — which is data loss (#78/#27) —
/// it is **kept** and a warning is logged.
pub fn filter_incremental(records: Vec<Value>, key: &str, start: &Value) -> Vec<Value> {
    records
        .into_iter()
        .filter(|r| match r.get(key) {
            None => false,
            Some(v) if type_rank(v) != type_rank(start) => {
                tracing::warn!(
                    key,
                    "incremental replication: record key type does not match the bookmark \
                     type; keeping the record to avoid silently dropping data"
                );
                true
            }
            Some(v) => json_gt(v, start),
        })
        .collect()
}

/// Return the maximum value of `record[key]` across all records, if any.
pub fn max_replication_value<'a>(records: &'a [Value], key: &str) -> Option<&'a Value> {
    records
        .iter()
        .filter_map(|r| r.get(key))
        .max_by(|a, b| json_compare(a, b))
}

/// Return the larger of two replication values using the same ordering as
/// [`max_replication_value`] (string lexicographic, numeric for numbers,
/// falling back to `a` on type mismatch).
pub fn max_value(a: Value, b: Value) -> Value {
    match json_compare(&a, &b) {
        Ordering::Less => b,
        _ => a,
    }
}

/// Type-rank for a total ordering across JSON value kinds, so comparisons of
/// differing types are deterministic instead of collapsing to `Equal`.
fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

/// Exact integer view of a JSON number (`i64` or `u64`), widened to `i128` so
/// both halves of the range compare without `f64` precision loss. `None` for
/// non-integral (floating) numbers.
fn number_as_i128(n: &serde_json::Number) -> Option<i128> {
    n.as_i64()
        .map(i128::from)
        .or_else(|| n.as_u64().map(i128::from))
}

/// Total ordering over JSON values used for replication bookmarks.
///
/// - Numbers: compared exactly as `i128` when both are integral (so cursors
///   above 2^53 don't lose precision); otherwise as `f64`, with NaN ordered
///   last.
/// - Same-type scalars/containers: natural ordering (strings lexicographic,
///   bools `false < true`, arrays element-wise, objects by serialized form).
/// - Different types: ordered by [`type_rank`] so the result is always total.
pub(crate) fn json_compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            match (number_as_i128(an), number_as_i128(bn)) {
                (Some(ai), Some(bi)) => ai.cmp(&bi),
                _ => {
                    let af = an.as_f64().unwrap_or(f64::NAN);
                    let bf = bn.as_f64().unwrap_or(f64::NAN);
                    af.partial_cmp(&bf).unwrap_or_else(|| {
                        // At least one NaN — order NaN last, deterministically.
                        match (af.is_nan(), bf.is_nan()) {
                            (false, true) => Ordering::Less,
                            (true, false) => Ordering::Greater,
                            _ => Ordering::Equal,
                        }
                    })
                }
            }
        }
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Array(x), Value::Array(y)) => {
            for (xi, yi) in x.iter().zip(y.iter()) {
                let c = json_compare(xi, yi);
                if c != Ordering::Equal {
                    return c;
                }
            }
            x.len().cmp(&y.len())
        }
        // Objects have no natural order; use the serialized form for a stable
        // total order (objects as replication keys are pathological).
        (Value::Object(_), Value::Object(_)) => a.to_string().cmp(&b.to_string()),
        // Different JSON types — order by type rank so comparison is total.
        _ => type_rank(a).cmp(&type_rank(b)),
    }
}

/// Total-order "greater than" over JSON values, using the same comparison
/// [`filter_incremental`] applies to replication keys (numbers numerically,
/// strings lexicographically — so RFC3339 timestamps order correctly). Public
/// so callers bounding a replay window (e.g. `faucet backfill --to-bookmark`)
/// compare exactly like the incremental filter does.
pub fn json_gt(a: &Value, b: &Value) -> bool {
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

    #[test]
    fn test_max_value_picks_larger_string() {
        assert_eq!(
            max_value(json!("2024-01-01"), json!("2024-06-01")),
            json!("2024-06-01")
        );
    }

    #[test]
    fn test_max_value_picks_larger_number() {
        assert_eq!(max_value(json!(5), json!(10)), json!(10));
    }

    #[test]
    fn test_max_value_returns_a_on_type_mismatch() {
        // String outranks Number in the total type-rank ordering, so the
        // larger (a) is returned.
        assert_eq!(max_value(json!("string"), json!(5)), json!("string"));
    }

    #[test]
    fn filter_incremental_keeps_large_integer_beyond_f64_precision() {
        // Regression for #78/#27: integer cursors above 2^53 lose precision
        // when compared as f64, so a genuinely-greater value compared Equal
        // and was silently dropped.
        let two_pow_53 = 9_007_199_254_740_992_i64; // 2^53
        let records = vec![
            json!({"id": 1, "seq": two_pow_53 + 1}),
            json!({"id": 2, "seq": two_pow_53 + 2}),
        ];
        let start = json!(two_pow_53);
        let filtered = filter_incremental(records, "seq", &start);
        assert_eq!(
            filtered.len(),
            2,
            "both values are strictly greater than 2^53"
        );
    }

    #[test]
    fn json_compare_distinguishes_large_integers() {
        let a = json!(9_007_199_254_740_993_i64); // 2^53 + 1
        let b = json!(9_007_199_254_740_992_i64); // 2^53
        assert_eq!(json_compare(&a, &b), Ordering::Greater);
    }

    #[test]
    fn filter_incremental_keeps_records_on_type_mismatch() {
        // Regression for #78/#27: a bookmark/key type mismatch must not be
        // silently treated as "not greater" and the record dropped — that is
        // data loss. Keep the record instead.
        let records = vec![json!({"id": 1, "seq": 20_240_701})];
        let start = json!("2024-06-01"); // string bookmark vs numeric key
        let filtered = filter_incremental(records, "seq", &start);
        assert_eq!(filtered.len(), 1, "type mismatch must not silently drop");
    }
}
