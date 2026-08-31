//! Salesforce object discovery → dataset descriptors (#647).
//!
//! Pure, network-free: given the JSON of Salesforce's global describe
//! (`GET /sobjects`) and each object's describe (`GET /sobjects/<obj>/describe`),
//! build one [`DatasetDescriptor`] per queryable object carrying a
//! **field-complete** `SELECT … FROM <Object>` (compound/non-queryable field
//! types excluded), a typed schema, and the source/sink config patches a matrix
//! row deep-merges. The HTTP fetching lives in `stream.rs`; everything here is
//! deterministic and unit-tested.

use faucet_core::discover::{DatasetDescriptor, columns_to_schema, nullable_type};
use serde_json::{Value, json};

/// Salesforce field `type`s that are **not** valid in a plain SOQL `SELECT`:
/// compound parents (their child components are selectable individually) and
/// blob fields. Everything else — scalars, `id`/`reference`, formulas,
/// picklists, `encryptedstring` — is selectable.
const UNSELECTABLE_TYPES: &[&str] = &["address", "location", "base64"];

/// Object-name suffixes that are almost never useful to bulk-sync; excluded by
/// default (the CLI `--include` filter can still name them explicitly).
const DEFAULT_SKIP_SUFFIXES: &[&str] = &["ChangeEvent", "Feed", "Share", "History", "Tag"];

/// Map a Salesforce field `type` to a JSON-Schema type fragment matching the
/// shape [`infer_schema`](faucet_core::schema::infer_schema) produces.
pub fn sf_type_to_json(sf_type: &str) -> Value {
    let ty = match sf_type {
        "boolean" => "boolean",
        "int" => "integer",
        "double" | "currency" | "percent" => "number",
        // string, id, reference, date, datetime, time, textarea, phone, url,
        // email, picklist, multipicklist, combobox, anyType, encryptedstring,
        // … all serialize as JSON strings.
        _ => "string",
    };
    json!({ "type": ty })
}

/// Whether a field of this Salesforce `type` can appear in a SOQL `SELECT`.
pub fn is_selectable_type(sf_type: &str) -> bool {
    !UNSELECTABLE_TYPES.contains(&sf_type)
}

/// Object name → snake_case sink table id (`OpportunityLineItem` →
/// `opportunity_line_item`, `Churn__c` → `churn_c`).
pub fn snake(name: &str) -> String {
    let base = name.strip_suffix("__c").map(|b| format!("{b}_c")).unwrap_or_else(|| name.to_string());
    let mut out = String::with_capacity(base.len() + 4);
    for (i, ch) in base.chars().enumerate() {
        if ch.is_ascii_uppercase() && i > 0 && !out.ends_with('_') {
            out.push('_');
        }
        out.push(ch.to_ascii_lowercase());
    }
    out
}

/// Should an object from the global describe be considered for discovery?
/// Keeps `queryable` objects, drops the default noise suffixes. (`--include` /
/// `--exclude` globbing is applied later by the CLI over the descriptor names.)
fn keep_object(obj: &Value) -> Option<String> {
    let name = obj.get("name")?.as_str()?;
    if obj.get("queryable").and_then(Value::as_bool) != Some(true) {
        return None;
    }
    if DEFAULT_SKIP_SUFFIXES.iter().any(|s| name.ends_with(s)) {
        return None;
    }
    Some(name.to_string())
}

/// Names of queryable objects from a global-describe response
/// (`{ "sobjects": [ { "name", "queryable", … } ] }`), sorted for determinism.
pub fn queryable_objects(global: &Value) -> Vec<String> {
    let mut names: Vec<String> = global
        .get("sobjects")
        .and_then(Value::as_array)
        .map(|a| a.iter().filter_map(keep_object).collect())
        .unwrap_or_default();
    names.sort();
    names
}

/// Build a descriptor for one object from its describe response
/// (`{ "name", "fields": [ { "name", "type", "nillable" } ] }`). Returns `None`
/// when the object exposes no selectable fields (nothing to query).
pub fn descriptor_for_object(
    object: &str,
    describe: &Value,
    operation: &str,
    route_by_table_id: bool,
) -> Option<DatasetDescriptor> {
    let fields = describe.get("fields").and_then(Value::as_array)?;
    let mut names: Vec<&str> = Vec::with_capacity(fields.len());
    let mut cols: Vec<(String, Value)> = Vec::with_capacity(fields.len());
    for f in fields {
        let fname = match f.get("name").and_then(Value::as_str) {
            Some(n) => n,
            None => continue,
        };
        let ftype = f.get("type").and_then(Value::as_str).unwrap_or("string");
        if !is_selectable_type(ftype) {
            continue;
        }
        names.push(fname);
        let frag = sf_type_to_json(ftype);
        // Salesforce `nillable` marks nullability; Id/system fields are non-null.
        let frag = if f.get("nillable").and_then(Value::as_bool) == Some(false) {
            frag
        } else {
            nullable_type(frag)
        };
        cols.push((fname.to_string(), frag));
    }
    if names.is_empty() {
        return None;
    }
    let soql = format!("SELECT {} FROM {}", names.join(", "), object);
    let config_patch = json!({
        "async_job": { "submit": { "json": { "operation": operation, "query": soql } } }
    });
    let mut d = DatasetDescriptor::new(object, "sobject", config_patch)
        .with_schema(columns_to_schema(cols));
    if route_by_table_id {
        d = d.with_sink_patch(json!({ "table_id": snake(object) }));
    }
    Some(d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn types_map_to_json() {
        assert_eq!(sf_type_to_json("boolean"), json!({"type": "boolean"}));
        assert_eq!(sf_type_to_json("int"), json!({"type": "integer"}));
        assert_eq!(sf_type_to_json("double"), json!({"type": "number"}));
        assert_eq!(sf_type_to_json("currency"), json!({"type": "number"}));
        assert_eq!(sf_type_to_json("string"), json!({"type": "string"}));
        assert_eq!(sf_type_to_json("id"), json!({"type": "string"}));
        assert_eq!(sf_type_to_json("datetime"), json!({"type": "string"}));
        assert_eq!(sf_type_to_json("picklist"), json!({"type": "string"}));
    }

    #[test]
    fn compound_and_blob_types_excluded() {
        assert!(!is_selectable_type("address"));
        assert!(!is_selectable_type("location"));
        assert!(!is_selectable_type("base64"));
        assert!(is_selectable_type("string"));
        assert!(is_selectable_type("reference"));
        assert!(is_selectable_type("encryptedstring"));
    }

    #[test]
    fn snake_cases_objects() {
        assert_eq!(snake("Account"), "account");
        assert_eq!(snake("OpportunityLineItem"), "opportunity_line_item");
        assert_eq!(snake("Churn__c"), "churn_c");
        assert_eq!(snake("Associated_Account__c"), "associated_account_c");
    }

    #[test]
    fn queryable_objects_filters_and_sorts() {
        let global = json!({ "sobjects": [
            { "name": "Opportunity", "queryable": true },
            { "name": "Account", "queryable": true },
            { "name": "AccountFeed", "queryable": true },      // noise suffix
            { "name": "SecretThing", "queryable": false },      // not queryable
            { "name": "AccountHistory", "queryable": true },    // noise suffix
        ]});
        assert_eq!(queryable_objects(&global), vec!["Account", "Opportunity"]);
    }

    #[test]
    fn descriptor_builds_soql_schema_and_patches() {
        let describe = json!({ "name": "Account", "fields": [
            { "name": "Id", "type": "id", "nillable": false },
            { "name": "Name", "type": "string", "nillable": true },
            { "name": "AnnualRevenue", "type": "currency", "nillable": true },
            { "name": "IsDeleted", "type": "boolean", "nillable": false },
            { "name": "BillingAddress", "type": "address", "nillable": true }, // dropped
        ]});
        let d = descriptor_for_object("Account", &describe, "queryAll", true).unwrap();
        assert_eq!(d.name, "Account");
        assert_eq!(d.kind, "sobject");
        // BillingAddress (compound) is excluded from the SELECT.
        let q = d.config_patch["async_job"]["submit"]["json"]["query"].as_str().unwrap();
        assert_eq!(q, "SELECT Id, Name, AnnualRevenue, IsDeleted FROM Account");
        assert_eq!(
            d.config_patch["async_job"]["submit"]["json"]["operation"],
            "queryAll"
        );
        // schema: types + nullability
        let s = d.schema.as_ref().unwrap();
        assert_eq!(s["properties"]["Id"]["type"], "string"); // non-null → bare
        assert_eq!(s["properties"]["Name"]["type"][1], "null"); // nillable → [T,null]
        assert_eq!(s["properties"]["AnnualRevenue"]["type"][0], "number");
        assert_eq!(s["properties"]["IsDeleted"]["type"], "boolean");
        assert!(s["properties"].get("BillingAddress").is_none());
        // sink routing
        assert_eq!(d.sink_patch, Some(json!({ "table_id": "account" })));
    }

    #[test]
    fn descriptor_without_sink_routing() {
        let describe = json!({ "fields": [ { "name": "Id", "type": "id" } ] });
        let d = descriptor_for_object("Account", &describe, "query", false).unwrap();
        assert_eq!(d.sink_patch, None);
        assert_eq!(
            d.config_patch["async_job"]["submit"]["json"]["operation"],
            "query"
        );
    }

    #[test]
    fn object_with_no_selectable_fields_is_none() {
        let describe = json!({ "fields": [ { "name": "Loc", "type": "location" } ] });
        assert!(descriptor_for_object("Weird", &describe, "queryAll", true).is_none());
        // missing fields array → None
        assert!(descriptor_for_object("X", &json!({}), "queryAll", true).is_none());
    }
}
