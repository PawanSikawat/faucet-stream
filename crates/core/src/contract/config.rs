//! Config-shaped types for the data-contract layer (issue #204). Pure
//! declarations — validation and compilation live in `compile.rs`, evaluation
//! in the module root.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

fn default_true() -> bool {
    true
}

/// What to do when a record breaches the contract. Contract-level (not
/// per-field): a contract is a single versioned promise, so one policy
/// governs every rule in it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnBreach {
    /// Surface [`FaucetError::ContractViolation`](crate::FaucetError::ContractViolation)
    /// on the first breach and fail the run (default — a contract is a
    /// promise, so silent divergence is the worst outcome).
    #[default]
    Fail,
    /// Route breaching records to the DLQ; write the rest. Requires a DLQ.
    Quarantine,
    /// Log + count breaches, but write every record unchanged.
    Warn,
}

impl std::fmt::Display for OnBreach {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            OnBreach::Fail => "fail",
            OnBreach::Quarantine => "quarantine",
            OnBreach::Warn => "warn",
        })
    }
}

/// Declared type of a contract field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ContractFieldType {
    /// JSON string.
    String,
    /// JSON number with no fractional part (fits `i64`/`u64` as parsed).
    Integer,
    /// Any JSON number (integer or float).
    Number,
    /// JSON boolean.
    Boolean,
    /// JSON object. Nested shapes are treated as one opaque column — a
    /// contract describes the top-level output shape, matching the
    /// schema-drift convention.
    Object,
    /// JSON array.
    Array,
}

impl std::fmt::Display for ContractFieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            ContractFieldType::String => "string",
            ContractFieldType::Integer => "integer",
            ContractFieldType::Number => "number",
            ContractFieldType::Boolean => "boolean",
            ContractFieldType::Object => "object",
            ContractFieldType::Array => "array",
        })
    }
}

/// The `contract:` config block: a declarative, versioned schema + semantic
/// contract for a pipeline's output, enforced per page after transforms and
/// quality checks and before the sink write.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ContractSpec {
    /// Contract version, e.g. `"1.0.0"`. Required and non-empty. Carried
    /// into breach errors, DLQ envelopes, and every export format so
    /// producers and consumers can pin the promise they agreed on.
    /// Recommendation: bump the major version on breaking changes (removing
    /// a field, narrowing a type) and the minor version on additive ones.
    pub version: String,

    /// Human-readable description of the dataset this contract covers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,

    /// Owning team or person (documentation metadata; carried into exports).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,

    /// Breach policy: `fail` (default) aborts the run, `quarantine` routes
    /// breaching records to the DLQ (a `dlq:` block is required), `warn`
    /// logs + counts but writes everything.
    #[serde(default)]
    pub on_breach: OnBreach,

    /// When `false`, a top-level key not declared in `fields` is a breach
    /// (`extra_field`). Default `true` — additive fields are allowed.
    #[serde(default = "default_true")]
    pub allow_extra_fields: bool,

    /// The promised top-level fields. Must be non-empty; names must be unique.
    pub fields: Vec<FieldContract>,
}

/// One promised top-level field: its type plus optional semantic constraints.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FieldContract {
    /// Top-level field name (contracts describe the output's top-level shape;
    /// nested objects are typed as `object`).
    pub name: String,

    /// Declared type. `integer` means a JSON number with no fractional part;
    /// `number` accepts any JSON number.
    #[serde(rename = "type")]
    pub field_type: ContractFieldType,

    /// When `true` (default) the field must be present in every record.
    /// A non-required field that is absent skips all other checks.
    #[serde(default = "default_true")]
    pub required: bool,

    /// When `true`, an explicit JSON `null` is allowed (and skips the
    /// type/constraint checks). Default `false`.
    #[serde(default)]
    pub nullable: bool,

    /// Allowed values (exact JSON equality). Must be non-empty when present,
    /// and every value must match the declared `type`. Use `nullable` for
    /// null — `null` is not allowed inside `enum`.
    #[serde(rename = "enum", default, skip_serializing_if = "Option::is_none")]
    pub allowed_values: Option<Vec<Value>>,

    /// Regex the value must match. `string` fields only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,

    /// Minimum numeric value (inclusive). `integer`/`number` fields only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min: Option<f64>,

    /// Maximum numeric value (inclusive). `integer`/`number` fields only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<f64>,

    /// Minimum string length in characters (inclusive). `string` fields only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_length: Option<usize>,

    /// Maximum string length in characters (inclusive). `string` fields only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_length: Option<usize>,

    /// Column description (documentation metadata; carried into exports).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn on_breach_defaults_to_fail() {
        assert_eq!(OnBreach::default(), OnBreach::Fail);
    }

    #[test]
    fn on_breach_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&OnBreach::Quarantine).unwrap(),
            "\"quarantine\""
        );
        let w: OnBreach = serde_json::from_str("\"warn\"").unwrap();
        assert_eq!(w, OnBreach::Warn);
    }

    #[test]
    fn field_type_round_trips() {
        let t: ContractFieldType = serde_json::from_str("\"integer\"").unwrap();
        assert_eq!(t, ContractFieldType::Integer);
        assert_eq!(t.to_string(), "integer");
    }

    #[test]
    fn parses_full_contract_block() {
        let spec: ContractSpec = serde_json::from_value(json!({
            "version": "1.2.0",
            "description": "orders output",
            "owner": "data-platform",
            "on_breach": "quarantine",
            "allow_extra_fields": false,
            "fields": [
                { "name": "id", "type": "integer", "min": 1 },
                { "name": "status", "type": "string",
                  "enum": ["open", "closed"], "description": "lifecycle state" },
                { "name": "email", "type": "string", "pattern": "^.+@.+$",
                  "required": false, "nullable": true,
                  "min_length": 3, "max_length": 254 }
            ]
        }))
        .unwrap();
        assert_eq!(spec.version, "1.2.0");
        assert_eq!(spec.on_breach, OnBreach::Quarantine);
        assert!(!spec.allow_extra_fields);
        assert_eq!(spec.fields.len(), 3);
        assert!(spec.fields[0].required, "required defaults to true");
        assert!(!spec.fields[0].nullable, "nullable defaults to false");
        assert!(!spec.fields[2].required);
        assert!(spec.fields[2].nullable);
        assert_eq!(
            spec.fields[1].allowed_values.as_ref().unwrap(),
            &vec![json!("open"), json!("closed")]
        );
    }

    #[test]
    fn rejects_unknown_keys() {
        let err = serde_json::from_value::<ContractSpec>(json!({
            "version": "1", "fields": [], "nope": true
        }));
        assert!(err.is_err());
    }

    #[test]
    fn defaults_when_optional_fields_absent() {
        let spec: ContractSpec = serde_json::from_value(json!({
            "version": "1.0.0",
            "fields": [{ "name": "id", "type": "string" }]
        }))
        .unwrap();
        assert_eq!(spec.on_breach, OnBreach::Fail);
        assert!(spec.allow_extra_fields);
        assert!(spec.description.is_none());
        assert!(spec.owner.is_none());
    }
}
