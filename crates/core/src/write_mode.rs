//! Unified write-mode types + planner shared by every upsert-capable sink.

use crate::error::FaucetError;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// Write semantics for a sink. Serialized snake_case. Default `Append`.
#[derive(
    Debug, Clone, Copy, Default, Serialize, Deserialize, schemars::JsonSchema, PartialEq, Eq,
)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Insert every record (today's behaviour).
    #[default]
    Append,
    /// Insert-or-update by `key`; optionally route delete-marked rows to deletes.
    Upsert,
    /// Delete by `key` for every record.
    Delete,
}

impl WriteMode {
    /// Lowercase wire name, for error messages.
    pub fn as_str(&self) -> &'static str {
        match self {
            WriteMode::Append => "append",
            WriteMode::Upsert => "upsert",
            WriteMode::Delete => "delete",
        }
    }
}

/// Identifies a record as a delete (vs. an upsert) by a marker field's value.
/// e.g. `{ field: "__op", values: ["d", "delete"] }`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema, PartialEq, Eq)]
pub struct DeleteMarker {
    /// Field name whose value flags a delete.
    pub field: String,
    /// Values of `field` that mean "this row is a delete".
    pub values: Vec<String>,
}

/// Shared write-mode config, embedded in each upsert-capable sink config via
/// `#[serde(flatten)]` so `write_mode` / `key` / `delete_marker` appear at the
/// sink-config top level.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WriteSpec {
    /// Append (default), upsert, or delete.
    #[serde(default)]
    pub write_mode: WriteMode,
    /// Key columns. Required and non-empty for upsert/delete; ignored for append.
    #[serde(default)]
    pub key: Vec<String>,
    /// Optional. Upsert only: rows whose `field` matches one of `values` are
    /// deletes; all others are upserts. The marker field is stripped from
    /// upsert rows before writing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delete_marker: Option<DeleteMarker>,
}

impl WriteSpec {
    /// Validate internal consistency at config-load time.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if matches!(self.write_mode, WriteMode::Upsert | WriteMode::Delete) && self.key.is_empty() {
            return Err(FaucetError::Config(format!(
                "write_mode: {} requires a non-empty `key`",
                self.write_mode.as_str()
            )));
        }
        Ok(())
    }
}

/// Ordered key column → value pairs, in `key` declaration order.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyTuple(pub Vec<(String, Value)>);

/// The partition of a page by write mode. Infallible to build — per-row
/// failures (missing/null key) land in `failed` with their original page index
/// so the caller can route them to a DLQ or abort.
#[derive(Debug, Default)]
pub struct WritePlan {
    /// Rows to insert-or-update, deduped (last-write-wins), marker stripped.
    pub upserts: Vec<Value>,
    /// Key tuples to delete, deduped.
    pub deletes: Vec<KeyTuple>,
    /// `(page_index, message)` for rows whose key could not be extracted.
    pub failed: Vec<(usize, String)>,
}

#[derive(Clone)]
enum Action {
    Upsert(Value),
    Delete(KeyTuple),
}

/// Partition `page` into upserts + deletes per `spec`. The single place all six
/// sinks share. `WriteMode::Append` should never reach here (callers route
/// append separately); if it does, every row is treated as an upsert.
pub fn plan_writes(page: &[Value], spec: &WriteSpec) -> WritePlan {
    debug_assert!(
        spec.write_mode != WriteMode::Append,
        "plan_writes called with WriteMode::Append — callers must route append separately"
    );
    let mut plan = WritePlan::default();
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut order: Vec<Action> = Vec::new();

    for (i, rec) in page.iter().enumerate() {
        let key_tuple = match extract_key(rec, &spec.key) {
            Ok(k) => k,
            Err(msg) => {
                plan.failed.push((i, msg));
                continue;
            }
        };
        let canon = canonical(&key_tuple);

        let is_delete = match spec.write_mode {
            WriteMode::Delete => true,
            WriteMode::Upsert => is_delete_marked(rec, spec.delete_marker.as_ref()),
            WriteMode::Append => false,
        };

        let action = if is_delete {
            Action::Delete(key_tuple)
        } else {
            Action::Upsert(strip_marker(rec.clone(), spec.delete_marker.as_ref()))
        };

        match index.get(&canon) {
            Some(&slot) => order[slot] = action,
            None => {
                index.insert(canon, order.len());
                order.push(action);
            }
        }
    }

    for action in order {
        match action {
            Action::Upsert(v) => plan.upserts.push(v),
            Action::Delete(k) => plan.deletes.push(k),
        }
    }
    plan
}

/// Pull the key columns out of a record in `key` order. Missing key or null
/// key value is an error.
fn extract_key(rec: &Value, key: &[String]) -> Result<KeyTuple, String> {
    let obj = rec
        .as_object()
        .ok_or_else(|| "record is not a JSON object".to_string())?;
    let mut out = Vec::with_capacity(key.len());
    for col in key {
        match obj.get(col) {
            None => return Err(format!("missing key column '{col}'")),
            Some(Value::Null) => return Err(format!("null value for key column '{col}'")),
            Some(v) => out.push((col.clone(), v.clone())),
        }
    }
    Ok(KeyTuple(out))
}

fn is_delete_marked(rec: &Value, marker: Option<&DeleteMarker>) -> bool {
    let Some(dm) = marker else { return false };
    let Some(v) = rec.get(&dm.field) else {
        return false;
    };
    let Some(s) = v.as_str() else { return false };
    dm.values.iter().any(|m| m == s)
}

fn strip_marker(mut rec: Value, marker: Option<&DeleteMarker>) -> Value {
    if let (Some(dm), Value::Object(map)) = (marker, &mut rec) {
        map.remove(&dm.field);
    }
    rec
}

/// Stable canonical string for a key tuple, for dedup.
fn canonical(k: &KeyTuple) -> String {
    let arr: Vec<&Value> = k.0.iter().map(|(_, v)| v).collect();
    serde_json::to_string(&arr).expect("a Vec<&serde_json::Value> always serializes")
}

/// Join a key tuple's values into a single document id (Elasticsearch `_id`,
/// composite keys). Each value rendered as its plain string / JSON form.
///
/// Assumes each key column has a consistent JSON type across records (the
/// normal case for SQL and CDC sources); it does not disambiguate, e.g., the
/// integer `7` from the string `"7"` in the same column.
pub fn key_to_doc_id(k: &KeyTuple, separator: &str) -> String {
    k.0.iter()
        .map(|(_, v)| match v {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .collect::<Vec<_>>()
        .join(separator)
}

/// Build a Mongo/ES filter document `{ col: value, … }` from a key tuple.
pub fn key_to_filter(k: &KeyTuple) -> Map<String, Value> {
    k.0.iter().map(|(c, v)| (c.clone(), v.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn upsert_spec(keys: &[&str]) -> WriteSpec {
        WriteSpec {
            write_mode: WriteMode::Upsert,
            key: keys.iter().map(|s| s.to_string()).collect(),
            delete_marker: None,
        }
    }

    #[test]
    fn upsert_extracts_key_and_keeps_row() {
        let plan = plan_writes(&[json!({"id": 1, "name": "a"})], &upsert_spec(&["id"]));
        assert_eq!(plan.upserts, vec![json!({"id": 1, "name": "a"})]);
        assert!(plan.deletes.is_empty());
        assert!(plan.failed.is_empty());
    }

    #[test]
    fn missing_key_goes_to_failed_with_original_index() {
        let plan = plan_writes(
            &[json!({"id": 1}), json!({"name": "no-key"})],
            &upsert_spec(&["id"]),
        );
        assert_eq!(plan.upserts.len(), 1);
        assert_eq!(plan.failed.len(), 1);
        assert_eq!(plan.failed[0].0, 1, "failed row keeps its page index");
    }

    #[test]
    fn null_key_value_is_a_failure() {
        let plan = plan_writes(&[json!({"id": null})], &upsert_spec(&["id"]));
        assert!(plan.upserts.is_empty());
        assert_eq!(plan.failed.len(), 1);
    }

    #[test]
    fn delete_marker_routes_to_deletes_and_strips_marker() {
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: Some(DeleteMarker {
                field: "__op".into(),
                values: vec!["d".into()],
            }),
        };
        let plan = plan_writes(
            &[
                json!({"id": 1, "name": "a", "__op": "u"}),
                json!({"id": 2, "__op": "d"}),
            ],
            &spec,
        );
        assert_eq!(plan.upserts, vec![json!({"id": 1, "name": "a"})]);
        assert_eq!(plan.deletes.len(), 1);
        assert_eq!(plan.deletes[0].0, vec![("id".to_string(), json!(2))]);
    }

    #[test]
    fn last_write_wins_dedup_keeps_final_upsert() {
        let plan = plan_writes(
            &[json!({"id": 1, "v": "old"}), json!({"id": 1, "v": "new"})],
            &upsert_spec(&["id"]),
        );
        assert_eq!(plan.upserts, vec![json!({"id": 1, "v": "new"})]);
    }

    #[test]
    fn last_write_wins_delete_after_upsert_is_a_delete() {
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: Some(DeleteMarker {
                field: "__op".into(),
                values: vec!["d".into()],
            }),
        };
        let plan = plan_writes(
            &[json!({"id": 1, "__op": "u"}), json!({"id": 1, "__op": "d"})],
            &spec,
        );
        assert!(plan.upserts.is_empty());
        assert_eq!(plan.deletes.len(), 1);
    }

    #[test]
    fn delete_mode_routes_every_row_to_deletes() {
        let spec = WriteSpec {
            write_mode: WriteMode::Delete,
            key: vec!["id".into()],
            delete_marker: None,
        };
        let plan = plan_writes(&[json!({"id": 1}), json!({"id": 2})], &spec);
        assert!(plan.upserts.is_empty());
        assert_eq!(plan.deletes.len(), 2);
    }

    #[test]
    fn composite_key_tuple_is_ordered() {
        let plan = plan_writes(
            &[json!({"a": 1, "b": 2, "v": 9})],
            &upsert_spec(&["a", "b"]),
        );
        assert_eq!(plan.upserts.len(), 1);
        let plan2 = plan_writes(
            &[
                json!({"a": 1, "b": 2, "v": "x"}),
                json!({"a": 1, "b": 3, "v": "y"}),
            ],
            &upsert_spec(&["a", "b"]),
        );
        assert_eq!(plan2.upserts.len(), 2, "(1,2) and (1,3) are distinct keys");
    }

    #[test]
    fn validate_rejects_upsert_without_key() {
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec![],
            delete_marker: None,
        };
        assert!(spec.validate().is_err());
    }

    #[test]
    fn validate_allows_append_without_key() {
        assert!(WriteSpec::default().validate().is_ok());
    }

    #[test]
    fn last_write_wins_upsert_after_delete_is_an_upsert() {
        // Inverse of the delete-after-upsert case: [delete, upsert] → upsert wins.
        let spec = WriteSpec {
            write_mode: WriteMode::Upsert,
            key: vec!["id".into()],
            delete_marker: Some(DeleteMarker {
                field: "__op".into(),
                values: vec!["d".into()],
            }),
        };
        let plan = plan_writes(
            &[
                json!({"id": 1, "__op": "d"}),
                json!({"id": 1, "v": 9, "__op": "u"}),
            ],
            &spec,
        );
        assert!(plan.deletes.is_empty());
        assert_eq!(plan.upserts, vec![json!({"id": 1, "v": 9})]);
    }

    #[test]
    fn empty_page_produces_empty_plan() {
        let plan = plan_writes(&[], &upsert_spec(&["id"]));
        assert!(plan.upserts.is_empty());
        assert!(plan.deletes.is_empty());
        assert!(plan.failed.is_empty());
    }

    #[test]
    fn delete_mode_dedups_repeated_key() {
        // Same key deleted twice in one page collapses to a single delete.
        let spec = WriteSpec {
            write_mode: WriteMode::Delete,
            key: vec!["id".into()],
            delete_marker: None,
        };
        let plan = plan_writes(&[json!({"id": 1}), json!({"id": 1})], &spec);
        assert_eq!(plan.deletes.len(), 1);
    }
}
