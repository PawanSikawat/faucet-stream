//! JSON Schema inference from record samples.
//!
//! Given a slice of JSON values (records from a REST API), produces a JSON Schema
//! that is valid for all of them.  The algorithm:
//!
//! * Each field type is inferred independently per record then **merged** across records.
//! * A field absent from some records gets `"null"` added to its type.
//! * `"integer"` widens to `"number"` when the same field is an integer in some records
//!   and a float in others.
//! * Nested objects are recursively inferred and merged.

use serde_json::{Map, Value, json};
use std::collections::HashSet;

/// Infer a JSON Schema `object` descriptor from a slice of record values.
///
/// Non-object top-level values are ignored.  Returns an empty-properties
/// object schema when `records` is empty or contains no objects.
pub fn infer_schema(records: &[Value]) -> Value {
    let objects: Vec<&Map<String, Value>> = records.iter().filter_map(|r| r.as_object()).collect();

    if objects.is_empty() {
        return json!({"type": "object", "properties": {}});
    }

    // Collect all field names across all records.
    let all_keys: HashSet<&String> = objects.iter().flat_map(|o| o.keys()).collect();

    let mut properties = Map::new();

    for key in all_keys {
        let values: Vec<&Value> = objects.iter().filter_map(|o| o.get(key)).collect();
        let records_with_key = values.len();

        let mut field_schema = values
            .into_iter()
            .map(infer_value_schema)
            .reduce(merge_schemas)
            .unwrap_or_else(|| json!({}));

        // Fields absent from some records are implicitly nullable.
        if records_with_key < objects.len() {
            add_null_type(&mut field_schema);
        }

        properties.insert(key.clone(), field_schema);
    }

    json!({
        "type": "object",
        "properties": Value::Object(properties)
    })
}

// ── Internal helpers ──────────────────────────────────────────────────────────

fn infer_value_schema(v: &Value) -> Value {
    match v {
        Value::Null => json!({"type": "null"}),
        Value::Bool(_) => json!({"type": "boolean"}),
        Value::Number(n) => {
            if n.is_i64() || n.is_u64() {
                json!({"type": "integer"})
            } else {
                json!({"type": "number"})
            }
        }
        Value::String(_) => json!({"type": "string"}),
        Value::Array(arr) => {
            let items = if arr.is_empty() {
                json!({})
            } else {
                arr.iter()
                    .map(infer_value_schema)
                    .reduce(merge_schemas)
                    .unwrap_or_else(|| json!({}))
            };
            json!({"type": "array", "items": items})
        }
        Value::Object(map) => {
            let props: Map<String, Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), infer_value_schema(v)))
                .collect();
            json!({"type": "object", "properties": Value::Object(props)})
        }
    }
}

/// Merge two schemas into one that is valid for both.
fn merge_schemas(a: Value, b: Value) -> Value {
    let mut types = collect_types(&a)
        .union(&collect_types(&b))
        .cloned()
        .collect::<Vec<_>>();

    // Numeric widening: integer + number → number.
    if types.contains(&"integer".to_string()) && types.contains(&"number".to_string()) {
        types.retain(|t| t != "integer");
    }
    types.sort();
    types.dedup();

    // Merge object properties when both schemas are (or include) objects.
    if types.contains(&"object".to_string()) {
        let props = merge_properties(extract_properties(&a), extract_properties(&b));
        return json!({
            "type": make_type_value(types),
            "properties": Value::Object(props)
        });
    }

    // Merge array item schemas.
    if types == ["array"] {
        let items_a = a.get("items").cloned().unwrap_or_else(|| json!({}));
        let items_b = b.get("items").cloned().unwrap_or_else(|| json!({}));
        return json!({
            "type": "array",
            "items": merge_schemas(items_a, items_b)
        });
    }

    json!({"type": make_type_value(types)})
}

fn merge_properties(a: Map<String, Value>, b: Map<String, Value>) -> Map<String, Value> {
    let keys_a: HashSet<String> = a.keys().cloned().collect();
    let keys_b: HashSet<String> = b.keys().cloned().collect();
    let mut result = Map::new();

    // Keys in both: merge.
    for key in keys_a.intersection(&keys_b) {
        result.insert(key.clone(), merge_schemas(a[key].clone(), b[key].clone()));
    }
    // Keys only in A: field can be absent → nullable.
    for key in keys_a.difference(&keys_b) {
        let mut s = a[key].clone();
        add_null_type(&mut s);
        result.insert(key.clone(), s);
    }
    // Keys only in B: field can be absent → nullable.
    for key in keys_b.difference(&keys_a) {
        let mut s = b[key].clone();
        add_null_type(&mut s);
        result.insert(key.clone(), s);
    }

    result
}

fn collect_types(schema: &Value) -> HashSet<String> {
    match schema.get("type") {
        Some(Value::String(t)) => std::iter::once(t.clone()).collect(),
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        _ => HashSet::new(),
    }
}

fn extract_properties(schema: &Value) -> Map<String, Value> {
    schema
        .get("properties")
        .and_then(|p| p.as_object())
        .cloned()
        .unwrap_or_default()
}

/// Add `"null"` to the type of `schema` if not already present.
fn add_null_type(schema: &mut Value) {
    let mut types = collect_types(schema);
    if types.contains("null") {
        return;
    }
    types.insert("null".to_string());
    let new_type = make_type_value(types.into_iter().collect());
    if let Some(t) = schema.get_mut("type") {
        *t = new_type;
    }
}

fn make_type_value(mut types: Vec<String>) -> Value {
    types.sort();
    types.dedup();
    if types.len() == 1 {
        Value::String(types.remove(0))
    } else {
        Value::Array(types.into_iter().map(Value::String).collect())
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_infer_schema_basic_types() {
        let records = vec![json!({"id": 1, "name": "Alice", "score": 9.5, "active": true})];
        let schema = infer_schema(&records);
        let props = &schema["properties"];
        assert_eq!(props["id"]["type"], "integer");
        assert_eq!(props["name"]["type"], "string");
        assert_eq!(props["score"]["type"], "number");
        assert_eq!(props["active"]["type"], "boolean");
    }

    #[test]
    fn test_infer_schema_nullable_absent_field() {
        let records = vec![json!({"id": 1, "email": "a@example.com"}), json!({"id": 2})];
        let schema = infer_schema(&records);
        let props = &schema["properties"];
        assert_eq!(props["id"]["type"], "integer");
        // email is absent in second record → nullable
        let email_type = &props["email"]["type"];
        assert!(
            email_type == &json!(["null", "string"]) || email_type == &json!(["string", "null"]),
            "expected nullable string, got {email_type}"
        );
    }

    #[test]
    fn test_infer_schema_explicit_null_value() {
        let records = vec![json!({"tag": "foo"}), json!({"tag": null})];
        let schema = infer_schema(&records);
        let tag_type = &schema["properties"]["tag"]["type"];
        assert!(
            tag_type == &json!(["null", "string"]) || tag_type == &json!(["string", "null"]),
            "expected nullable string, got {tag_type}"
        );
    }

    #[test]
    fn test_infer_schema_integer_widens_to_number() {
        let records = vec![json!({"val": 42}), json!({"val": 3.15})];
        let schema = infer_schema(&records);
        assert_eq!(schema["properties"]["val"]["type"], "number");
    }

    #[test]
    fn test_infer_schema_array_field() {
        let records = vec![json!({"tags": ["rust", "api"]})];
        let schema = infer_schema(&records);
        assert_eq!(schema["properties"]["tags"]["type"], "array");
        assert_eq!(schema["properties"]["tags"]["items"]["type"], "string");
    }

    #[test]
    fn test_infer_schema_nested_object() {
        let records = vec![
            json!({"address": {"city": "NYC", "zip": "10001"}}),
            json!({"address": {"city": "LA"}}),
        ];
        let schema = infer_schema(&records);
        let addr = &schema["properties"]["address"];
        assert_eq!(addr["type"], "object");
        assert_eq!(addr["properties"]["city"]["type"], "string");
        // zip absent from second record → nullable
        let zip_type = &addr["properties"]["zip"]["type"];
        assert!(
            zip_type == &json!(["null", "string"]) || zip_type == &json!(["string", "null"]),
            "expected nullable string, got {zip_type}"
        );
    }

    #[test]
    fn test_infer_schema_empty_records() {
        let schema = infer_schema(&[]);
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["properties"], json!({}));
    }

    #[test]
    fn test_infer_schema_skips_non_objects() {
        // Top-level arrays and primitives are ignored.
        let records = vec![json!("string"), json!(42), json!({"id": 1})];
        let schema = infer_schema(&records);
        assert_eq!(schema["properties"]["id"]["type"], "integer");
    }

    #[test]
    fn test_add_null_type_idempotent() {
        let mut s = json!({"type": ["null", "string"]});
        add_null_type(&mut s);
        // Should not duplicate "null".
        assert_eq!(s["type"], json!(["null", "string"]));
    }

    #[test]
    fn test_merge_schemas_object_merges_properties() {
        let a = json!({"type": "object", "properties": {"x": {"type": "integer"}}});
        let b = json!({"type": "object", "properties": {"y": {"type": "string"}}});
        let merged = merge_schemas(a, b);
        assert_eq!(merged["type"], "object");
        // x is absent from b → nullable in merged
        let x_type = &merged["properties"]["x"]["type"];
        assert!(
            x_type == &json!(["integer", "null"]) || x_type == &json!(["null", "integer"]),
            "got {x_type}"
        );
        // y is absent from a → nullable in merged
        let y_type = &merged["properties"]["y"]["type"];
        assert!(
            y_type == &json!(["null", "string"]) || y_type == &json!(["string", "null"]),
            "got {y_type}"
        );
    }

    #[test]
    fn test_merge_schemas_array_items_merged() {
        let a = json!({"type": "array", "items": {"type": "integer"}});
        let b = json!({"type": "array", "items": {"type": "string"}});
        let merged = merge_schemas(a, b);
        assert_eq!(merged["type"], "array");
        let items_type = &merged["items"]["type"];
        assert!(
            items_type == &json!(["integer", "string"])
                || items_type == &json!(["string", "integer"]),
            "got {items_type}"
        );
    }
}
