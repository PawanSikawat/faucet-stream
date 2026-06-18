//! Schema-drift detection + policy types (issue #194).
//!
//! Drift is the divergence between an incoming page's inferred top-level shape
//! (via [`crate::schema::infer_schema`]) and the sink's live destination schema
//! (via [`crate::Sink::current_schema`]). The pure [`diff_schema`] classifies
//! each top-level column into one bucket; [`SchemaDriftPolicy`] decides what the
//! pipeline does with the result. Nested objects are treated as a single column.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// One column's drift, expressed in JSON-Schema type-fragment terms
/// (e.g. `{"type":"integer"}` or `{"type":["string","null"]}`).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnChange {
    /// Top-level column name.
    pub name: String,
    /// Destination type fragment; `None` for an addition (not in destination).
    pub from: Option<Value>,
    /// Inferred type fragment from the incoming page.
    pub to: Value,
}

/// Result of diffing a page's inferred shape against the destination schema.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaDiff {
    /// In the page, not in the destination.
    pub additions: Vec<ColumnChange>,
    /// Existing column whose type widened losslessly (e.g. integer→number,
    /// or gained nullability).
    pub widenings: Vec<ColumnChange>,
    /// Existing column whose type changed in a way that cannot be auto-applied
    /// (narrowing / incompatible type swap).
    pub incompatible: Vec<ColumnChange>,
    /// In the destination and NOT NULL, absent from the page — would fail an
    /// insert unless relaxed to nullable.
    pub droppable_required: Vec<String>,
}

impl SchemaDiff {
    /// `true` when no drift of any kind was detected.
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty()
            && self.widenings.is_empty()
            && self.incompatible.is_empty()
            && self.droppable_required.is_empty()
    }

    /// Column names that drifted, for error messages / metrics.
    pub fn changed_columns(&self) -> Vec<String> {
        self.additions
            .iter()
            .chain(&self.widenings)
            .chain(&self.incompatible)
            .map(|c| c.name.clone())
            .chain(self.droppable_required.iter().cloned())
            .collect()
    }
}

/// The applyable subset of a [`SchemaDiff`] handed to [`crate::Sink::evolve_schema`].
/// Never carries `incompatible` columns — those are routed by the policy.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SchemaEvolution {
    pub additions: Vec<ColumnChange>,
    pub widenings: Vec<ColumnChange>,
    /// Columns to relax from NOT NULL to nullable.
    pub relax_nullability: Vec<String>,
}

impl SchemaEvolution {
    pub fn is_empty(&self) -> bool {
        self.additions.is_empty() && self.widenings.is_empty() && self.relax_nullability.is_empty()
    }
}

/// The set of JSON-Schema primitive type names a fragment carries (excluding `null`),
/// plus whether `null` is present.
fn type_set(fragment: &Value) -> (Vec<String>, bool) {
    let mut names = Vec::new();
    let mut nullable = false;
    match fragment.get("type") {
        Some(Value::String(t)) => {
            if t == "null" {
                nullable = true
            } else {
                names.push(t.clone())
            }
        }
        Some(Value::Array(arr)) => {
            for v in arr {
                if let Some(t) = v.as_str() {
                    if t == "null" {
                        nullable = true
                    } else {
                        names.push(t.to_string())
                    }
                }
            }
        }
        _ => {}
    }
    names.sort();
    (names, nullable)
}

/// Is moving from `from` to `to` a lossless widening?
/// Top-level rules (issue #194): integer→number, and gaining nullability.
fn is_widening(from: &Value, to: &Value) -> bool {
    let (fnames, fnull) = type_set(from);
    let (tnames, tnull) = type_set(to);
    // Same non-null base types, but `to` adds nullability → widening.
    if fnames == tnames && !fnull && tnull {
        return true;
    }
    // integer → number (with or without matching nullability).
    if fnames == vec!["integer".to_string()] && tnames == vec!["number".to_string()] && fnull == tnull
    {
        return true;
    }
    false
}

/// Are the two fragments the same type (ignoring property/items detail —
/// top-level only)?
fn same_base_type(a: &Value, b: &Value) -> bool {
    let (an, anull) = type_set(a);
    let (bn, bnull) = type_set(b);
    an == bn && anull == bnull
}

/// Diff a page's inferred shape against the destination schema (top-level columns).
///
/// `destination` and `page` are both `infer_schema`-shaped object schemas
/// (`{"type":"object","properties":{...}}`). `allow_widening` gates whether a
/// lossless widening lands in `widenings` (true) or `incompatible` (false).
pub fn diff_schema(destination: &Value, page: &Value, allow_widening: bool) -> SchemaDiff {
    let empty = serde_json::Map::new();
    let dest_props = destination
        .get("properties")
        .and_then(|p| p.as_object())
        .unwrap_or(&empty);
    let page_props = page
        .get("properties")
        .and_then(|p| p.as_object())
        .unwrap_or(&empty);

    let mut diff = SchemaDiff::default();

    for (name, page_ty) in page_props {
        match dest_props.get(name) {
            None => diff.additions.push(ColumnChange {
                name: name.clone(),
                from: None,
                to: page_ty.clone(),
            }),
            Some(dest_ty) => {
                if same_base_type(dest_ty, page_ty) {
                    continue; // no change
                }
                let change = ColumnChange {
                    name: name.clone(),
                    from: Some(dest_ty.clone()),
                    to: page_ty.clone(),
                };
                if allow_widening && is_widening(dest_ty, page_ty) {
                    diff.widenings.push(change);
                } else {
                    diff.incompatible.push(change);
                }
            }
        }
    }

    // Destination columns absent from the page: drift only if NOT NULL.
    for (name, dest_ty) in dest_props {
        if !page_props.contains_key(name) {
            let (_, nullable) = type_set(dest_ty);
            if !nullable {
                diff.droppable_required.push(name.clone());
            }
        }
    }
    diff.additions.sort_by(|a, b| a.name.cmp(&b.name));
    diff.widenings.sort_by(|a, b| a.name.cmp(&b.name));
    diff.incompatible.sort_by(|a, b| a.name.cmp(&b.name));
    diff.droppable_required.sort();
    diff
}

/// What to do when drift is detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnDrift {
    /// Detect + emit a metric and a one-shot log; write the page unchanged.
    #[default]
    Warn,
    /// Apply additive/widening DDL to the destination, then write.
    Evolve,
    /// Drop unknown (non-destination) fields from every record; write the rest.
    Ignore,
    /// Route the records that exhibit the drift to the DLQ; write the rest.
    Quarantine,
    /// Raise `FaucetError::SchemaDrift` and abort.
    Fail,
}

/// `evolve`-only: what to do with a narrowing/incompatible change that can't be
/// auto-applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum OnIncompatible {
    /// Abort the run (default).
    #[default]
    Fail,
    /// Route the offending records to the DLQ.
    Quarantine,
}

fn default_true() -> bool {
    true
}

/// User-facing `schema:` config block (pipeline level).
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct SchemaDriftSpec {
    /// Policy applied when drift is detected.
    #[serde(default)]
    pub on_drift: OnDrift,
    /// Whether a lossless widening counts as evolvable (vs incompatible).
    /// Only consulted by `evolve`. Default: true.
    #[serde(default = "default_true")]
    pub allow_type_widening: bool,
    /// `evolve` only: action for an incompatible residue. Default: fail.
    #[serde(default)]
    pub on_incompatible: OnIncompatible,
}

/// Compiled, ready-to-run drift policy. Cheap to clone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaDriftPolicy {
    pub on_drift: OnDrift,
    pub allow_widening: bool,
    pub on_incompatible: OnIncompatible,
}

impl SchemaDriftPolicy {
    /// Compile a spec into a runnable policy. Infallible — there is nothing to
    /// validate that serde hasn't already (the DLQ requirement is enforced by
    /// the pipeline at run start and by the CLI at config-load).
    pub fn compile(spec: &SchemaDriftSpec) -> Self {
        Self {
            on_drift: spec.on_drift,
            allow_widening: spec.allow_type_widening,
            on_incompatible: spec.on_incompatible,
        }
    }

    /// `true` when this policy can route records to a DLQ (so one must exist).
    pub fn requires_dlq(&self) -> bool {
        self.on_drift == OnDrift::Quarantine
            || (self.on_drift == OnDrift::Evolve && self.on_incompatible == OnIncompatible::Quarantine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn schema(props: Value) -> Value {
        json!({ "type": "object", "properties": props })
    }

    #[test]
    fn no_drift_when_shapes_match() {
        let dest = schema(json!({ "id": {"type": "integer"}, "name": {"type": "string"} }));
        let page = schema(json!({ "id": {"type": "integer"}, "name": {"type": "string"} }));
        let d = diff_schema(&dest, &page, true);
        assert!(d.is_empty(), "got {d:?}");
    }

    #[test]
    fn detects_addition() {
        let dest = schema(json!({ "id": {"type": "integer"} }));
        let page = schema(json!({ "id": {"type": "integer"}, "email": {"type": "string"} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.additions.len(), 1);
        assert_eq!(d.additions[0].name, "email");
        assert!(d.additions[0].from.is_none());
        assert_eq!(d.additions[0].to, json!({"type": "string"}));
        assert!(d.widenings.is_empty() && d.incompatible.is_empty());
    }

    #[test]
    fn integer_to_number_is_widening_when_allowed() {
        let dest = schema(json!({ "score": {"type": "integer"} }));
        let page = schema(json!({ "score": {"type": "number"} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.widenings.len(), 1, "got {d:?}");
        assert_eq!(d.widenings[0].name, "score");
        assert!(d.incompatible.is_empty());
    }

    #[test]
    fn integer_to_number_is_incompatible_when_widening_disallowed() {
        let dest = schema(json!({ "score": {"type": "integer"} }));
        let page = schema(json!({ "score": {"type": "number"} }));
        let d = diff_schema(&dest, &page, false);
        assert_eq!(d.incompatible.len(), 1, "got {d:?}");
        assert!(d.widenings.is_empty());
    }

    #[test]
    fn gaining_nullability_is_widening() {
        let dest = schema(json!({ "name": {"type": "string"} }));
        let page = schema(json!({ "name": {"type": ["string", "null"]} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.widenings.len(), 1, "got {d:?}");
    }

    #[test]
    fn string_to_integer_is_incompatible() {
        let dest = schema(json!({ "id": {"type": "string"} }));
        let page = schema(json!({ "id": {"type": "integer"} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.incompatible.len(), 1, "got {d:?}");
        assert!(d.widenings.is_empty());
    }

    #[test]
    fn required_destination_column_absent_from_page_is_droppable_required() {
        // Destination has a non-nullable `created_at` the page never provides.
        let dest = schema(json!({
            "id": {"type": "integer"},
            "created_at": {"type": "string"}
        }));
        let page = schema(json!({ "id": {"type": "integer"} }));
        let d = diff_schema(&dest, &page, true);
        assert_eq!(d.droppable_required, vec!["created_at".to_string()], "got {d:?}");
    }

    #[test]
    fn nullable_destination_column_absent_from_page_is_not_drift() {
        // A column the destination already allows to be null is fine to omit.
        let dest = schema(json!({
            "id": {"type": "integer"},
            "note": {"type": ["string", "null"]}
        }));
        let page = schema(json!({ "id": {"type": "integer"} }));
        let d = diff_schema(&dest, &page, true);
        assert!(d.is_empty(), "got {d:?}");
    }

    #[test]
    fn nested_object_treated_as_single_column() {
        // A change *inside* a nested object is invisible — top-level only.
        let dest = schema(json!({ "meta": {"type": "object", "properties": {"a": {"type": "integer"}}} }));
        let page = schema(json!({ "meta": {"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "string"}}} }));
        let d = diff_schema(&dest, &page, true);
        assert!(d.is_empty(), "nested changes must not surface as drift; got {d:?}");
    }

    #[test]
    fn spec_defaults() {
        let spec: SchemaDriftSpec = serde_json::from_str("{}").unwrap();
        assert_eq!(spec.on_drift, OnDrift::Warn);
        assert!(spec.allow_type_widening);
        assert_eq!(spec.on_incompatible, OnIncompatible::Fail);
    }

    #[test]
    fn on_drift_serializes_snake_case() {
        assert_eq!(serde_json::to_string(&OnDrift::Evolve).unwrap(), "\"evolve\"");
        assert_eq!(serde_json::to_string(&OnDrift::Quarantine).unwrap(), "\"quarantine\"");
    }

    #[test]
    fn policy_compile_carries_flags() {
        let spec: SchemaDriftSpec =
            serde_json::from_str(r#"{"on_drift":"evolve","allow_type_widening":false}"#).unwrap();
        let policy = SchemaDriftPolicy::compile(&spec);
        assert_eq!(policy.on_drift, OnDrift::Evolve);
        assert!(!policy.allow_widening);
        assert_eq!(policy.on_incompatible, OnIncompatible::Fail);
    }

    #[test]
    fn policy_requires_dlq_only_for_quarantine_paths() {
        let q: SchemaDriftSpec = serde_json::from_str(r#"{"on_drift":"quarantine"}"#).unwrap();
        assert!(SchemaDriftPolicy::compile(&q).requires_dlq());
        let evo_q: SchemaDriftSpec =
            serde_json::from_str(r#"{"on_drift":"evolve","on_incompatible":"quarantine"}"#).unwrap();
        assert!(SchemaDriftPolicy::compile(&evo_q).requires_dlq());
        let warn: SchemaDriftSpec = serde_json::from_str(r#"{"on_drift":"warn"}"#).unwrap();
        assert!(!SchemaDriftPolicy::compile(&warn).requires_dlq());
    }
}
