//! Machine-readable contract exports: JSON Schema and an OpenLineage
//! `SchemaDatasetFacet`. Pure functions over a [`ContractSpec`] — no I/O.
//! Used by `faucet contract --export …`; available to library callers too.

use crate::contract::config::{ContractFieldType, ContractSpec, FieldContract};
use serde_json::{Map, Value, json};

/// Pinned OpenLineage `SchemaDatasetFacet` schema URL used by
/// [`to_openlineage_facet`].
pub const OPENLINEAGE_SCHEMA_FACET_URL: &str =
    "https://openlineage.io/spec/facets/1-1-1/SchemaDatasetFacet.json";

/// Render the contract as a standalone JSON Schema (draft 2020-12) document.
///
/// Mapping: each field becomes a property; `required: true` fields land in
/// the top-level `required` array; `nullable` widens the property `type` to
/// `[…, "null"]` (and appends `null` to `enum` when both are set);
/// `allow_extra_fields: false` maps to `additionalProperties: false`. The
/// contract version travels as `x-faucet-contract-version`.
pub fn to_json_schema(spec: &ContractSpec) -> Value {
    let mut properties = Map::new();
    let mut required: Vec<Value> = Vec::new();
    for f in &spec.fields {
        if f.required {
            required.push(Value::String(f.name.clone()));
        }
        properties.insert(f.name.clone(), field_schema(f));
    }
    let mut root = Map::new();
    root.insert(
        "$schema".into(),
        json!("https://json-schema.org/draft/2020-12/schema"),
    );
    root.insert(
        "title".into(),
        json!(format!("faucet data contract v{}", spec.version)),
    );
    if let Some(d) = &spec.description {
        root.insert("description".into(), json!(d));
    }
    root.insert("x-faucet-contract-version".into(), json!(spec.version));
    if let Some(o) = &spec.owner {
        root.insert("x-faucet-contract-owner".into(), json!(o));
    }
    root.insert("type".into(), json!("object"));
    root.insert("properties".into(), Value::Object(properties));
    root.insert("required".into(), Value::Array(required));
    root.insert(
        "additionalProperties".into(),
        json!(spec.allow_extra_fields),
    );
    Value::Object(root)
}

fn json_schema_type(ty: ContractFieldType) -> &'static str {
    match ty {
        ContractFieldType::String => "string",
        ContractFieldType::Integer => "integer",
        ContractFieldType::Number => "number",
        ContractFieldType::Boolean => "boolean",
        ContractFieldType::Object => "object",
        ContractFieldType::Array => "array",
    }
}

fn field_schema(f: &FieldContract) -> Value {
    let mut prop = Map::new();
    let base = json_schema_type(f.field_type);
    if f.nullable {
        prop.insert("type".into(), json!([base, "null"]));
    } else {
        prop.insert("type".into(), json!(base));
    }
    if let Some(values) = &f.allowed_values {
        let mut e = values.clone();
        if f.nullable {
            e.push(Value::Null);
        }
        prop.insert("enum".into(), Value::Array(e));
    }
    if let Some(p) = &f.pattern {
        prop.insert("pattern".into(), json!(p));
    }
    if let Some(min) = f.min {
        prop.insert("minimum".into(), json!(min));
    }
    if let Some(max) = f.max {
        prop.insert("maximum".into(), json!(max));
    }
    if let Some(min) = f.min_length {
        prop.insert("minLength".into(), json!(min));
    }
    if let Some(max) = f.max_length {
        prop.insert("maxLength".into(), json!(max));
    }
    if let Some(d) = &f.description {
        prop.insert("description".into(), json!(d));
    }
    Value::Object(prop)
}

/// Render the contract as an OpenLineage `SchemaDatasetFacet` — the same
/// facet shape `faucet-lineage` attaches to dataset events, so downstream
/// OpenLineage consumers can ingest the contract as a schema promise.
///
/// `producer` identifies the emitting tool (e.g. the faucet repo URL + version).
pub fn to_openlineage_facet(spec: &ContractSpec, producer: &str) -> Value {
    let fields: Vec<Value> = spec
        .fields
        .iter()
        .map(|f| {
            let mut entry = Map::new();
            entry.insert("name".into(), json!(f.name));
            entry.insert("type".into(), json!(json_schema_type(f.field_type)));
            if let Some(d) = &f.description {
                entry.insert("description".into(), json!(d));
            }
            Value::Object(entry)
        })
        .collect();
    json!({
        "_producer": producer,
        "_schemaURL": OPENLINEAGE_SCHEMA_FACET_URL,
        "x-faucet-contract-version": spec.version,
        "fields": fields,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> ContractSpec {
        serde_json::from_value(json!({
            "version": "2.1.0",
            "description": "orders",
            "owner": "data-platform",
            "allow_extra_fields": false,
            "fields": [
                { "name": "id", "type": "integer", "min": 1.0, "max": 100.0 },
                { "name": "status", "type": "string", "enum": ["open", "closed"],
                  "nullable": true, "description": "lifecycle" },
                { "name": "email", "type": "string", "pattern": "^.+@.+$",
                  "required": false, "min_length": 3, "max_length": 254 }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn json_schema_maps_types_required_and_constraints() {
        let s = to_json_schema(&spec());
        assert_eq!(s["x-faucet-contract-version"], "2.1.0");
        assert_eq!(s["x-faucet-contract-owner"], "data-platform");
        assert_eq!(s["description"], "orders");
        assert_eq!(s["type"], "object");
        assert_eq!(s["additionalProperties"], false);
        assert_eq!(s["required"], json!(["id", "status"]));
        assert_eq!(s["properties"]["id"]["type"], "integer");
        assert_eq!(s["properties"]["id"]["minimum"], 1.0);
        assert_eq!(s["properties"]["id"]["maximum"], 100.0);
        // nullable widens type and appends null to enum
        assert_eq!(s["properties"]["status"]["type"], json!(["string", "null"]));
        assert_eq!(
            s["properties"]["status"]["enum"],
            json!(["open", "closed", null])
        );
        assert_eq!(s["properties"]["status"]["description"], "lifecycle");
        assert_eq!(s["properties"]["email"]["pattern"], "^.+@.+$");
        assert_eq!(s["properties"]["email"]["minLength"], 3);
        assert_eq!(s["properties"]["email"]["maxLength"], 254);
    }

    #[test]
    fn json_schema_allows_extra_fields_by_default() {
        let spec: ContractSpec = serde_json::from_value(json!({
            "version": "1", "fields": [{ "name": "id", "type": "string" }]
        }))
        .unwrap();
        let s = to_json_schema(&spec);
        assert_eq!(s["additionalProperties"], true);
        assert!(s.get("x-faucet-contract-owner").is_none());
    }

    #[test]
    fn openlineage_facet_shape() {
        let f = to_openlineage_facet(&spec(), "https://example.com/faucet/v1");
        assert_eq!(f["_producer"], "https://example.com/faucet/v1");
        assert_eq!(f["_schemaURL"], OPENLINEAGE_SCHEMA_FACET_URL);
        assert_eq!(f["x-faucet-contract-version"], "2.1.0");
        let fields = f["fields"].as_array().unwrap();
        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0]["name"], "id");
        assert_eq!(fields[0]["type"], "integer");
        assert!(fields[0].get("description").is_none());
        assert_eq!(fields[1]["description"], "lifecycle");
    }
}
