//! Compile YAML/JSON transform declarations into `TransformStage` values.
//!
//! The built-in transforms are exposed via config — custom closure
//! transforms require Rust code and are reserved for the library API.

use crate::config::TransformSpec;
use crate::error::{CliError, CliResult};
#[cfg(feature = "transforms")]
use faucet_core::{CastOnError, CastType, JsonSchema, KeyCaseMode, ValueCaseMode, schema_for};
use faucet_core::{RecordTransform, TransformStage};
#[cfg(feature = "transforms")]
use serde::Deserialize;
use serde_json::Value;
#[cfg(feature = "transforms")]
use std::collections::HashMap;

/// Inline-config schema for the `flatten` transform.
#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct FlattenConfig {
    /// Separator joining nested keys (default: `"__"`).
    #[serde(default = "default_separator")]
    separator: String,
}

#[cfg(feature = "transforms")]
fn default_separator() -> String {
    "__".to_owned()
}

/// Inline-config schema for the `rename_keys` transform.
#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct RenameKeysConfig {
    /// Rust regex matched against every key.
    pattern: String,
    /// Replacement string. May reference capture groups (`$1`, `${name}`).
    replacement: String,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct FieldsConfig {
    /// Top-level field names to act on.
    fields: Vec<String>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct SetConfig {
    /// Map of field name → constant value to set on every record.
    values: serde_json::Map<String, Value>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct RenameFieldConfig {
    /// Map of old field name → new field name.
    fields: HashMap<String, String>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct CastConfig {
    /// Map of field name → target type.
    fields: HashMap<String, CastType>,
    /// What to do when a value cannot be cast. Default: `error`.
    #[serde(default)]
    on_error: CastOnError,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct RedactConfig {
    /// Top-level field names to overwrite with `mask`.
    fields: Vec<String>,
    /// Replacement value. Default: the string `"***"`.
    #[serde(default = "default_mask")]
    mask: Value,
}

#[cfg(feature = "transforms")]
fn default_mask() -> Value {
    Value::String("***".to_owned())
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct ValueCaseConfig {
    /// String-valued fields to re-case.
    fields: Vec<String>,
    /// Casing convention to apply to each listed field.
    mode: ValueCaseMode,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct SpellSymbolsConfig {
    /// Extra symbol → word overrides layered on top of the built-in map.
    #[serde(default)]
    extra: HashMap<String, String>,
    /// String inserted between expanded words. Default: a single space.
    #[serde(default = "default_spell_separator")]
    separator: String,
}

#[cfg(feature = "transforms")]
fn default_spell_separator() -> String {
    " ".to_owned()
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize, JsonSchema)]
struct KeysCaseConfig {
    /// Output convention for every key in the record.
    mode: KeyCaseMode,
}

#[cfg(feature = "transform-filter")]
#[derive(Debug, Deserialize, JsonSchema)]
struct FilterConfig {
    /// JSONPath subset: bare key, dot path, or bracketed string key.
    path: String,
    /// One of `eq`, `ne`, `exists`, `in`, `not_in`.
    op: faucet_core::FilterOp,
    /// Required for `eq`/`ne`/`in`/`not_in`. For `in`/`not_in`, must be an array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<Value>,
}

#[cfg(feature = "transform-explode")]
#[derive(Debug, Deserialize, JsonSchema)]
struct ExplodeConfig {
    /// JSONPath subset: bare key, dot path, or bracketed string key.
    path: String,
    /// Prefix prepended to object-element fields. Defaults to the last
    /// segment of `path`. Empty string = pure LATERAL FLATTEN (no prefix).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prefix: Option<String>,
    /// Separator between prefix and element field key. Default `"_"`.
    #[serde(default = "default_explode_separator_cli")]
    separator: String,
    /// `passthrough` (default), `drop`, or `error` when path doesn't yield a
    /// non-empty array.
    #[serde(default)]
    on_missing: faucet_core::OnMissing,
}

#[cfg(feature = "transform-explode")]
fn default_explode_separator_cli() -> String {
    "_".to_owned()
}

/// One row in the transform registry — the single source of truth for every
/// built-in transform's kind, one-line description, JSON Schema, and
/// `TransformSpec → TransformStage` decoder. `compile_one`,
/// `transform_descriptions`, and `transform_schema` all read from this list
/// so adding a new transform means appending one entry (no parallel match
/// arms to keep in sync).
struct TransformDef {
    kind: &'static str,
    description: &'static str,
    schema_fn: fn() -> Value,
    compile_fn: fn(&str, Value) -> CliResult<TransformStage>,
}

/// Every transform compiled into this build, in display order.
///
/// Non-capturing closures coerce to the `fn` pointers held by `TransformDef`,
/// so each row stays a single self-contained record next to its sibling
/// entries.
fn registry() -> Vec<TransformDef> {
    #[cfg(feature = "transforms")]
    {
        vec![
            TransformDef {
                kind: "flatten",
                description: "Flatten nested objects into a single level (configurable separator).",
                schema_fn: || schema::<FlattenConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<FlattenConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Flatten {
                        separator: cfg.separator,
                    }))
                },
            },
            TransformDef {
                kind: "rename_keys",
                description: "Rewrite every key via a regex pattern + replacement.",
                schema_fn: || schema::<RenameKeysConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<RenameKeysConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::RenameKeys {
                        pattern: cfg.pattern,
                        replacement: cfg.replacement,
                    }))
                },
            },
            TransformDef {
                kind: "keys_case",
                description: "Re-case every key (snake / camel / pascal / kebab / screaming_snake).",
                schema_fn: || schema::<KeysCaseConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<KeysCaseConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::KeysCase {
                        mode: cfg.mode,
                    }))
                },
            },
            TransformDef {
                kind: "select",
                description: "Keep only the listed top-level fields; drop the rest.",
                schema_fn: || schema::<FieldsConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<FieldsConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Select {
                        fields: cfg.fields,
                    }))
                },
            },
            TransformDef {
                kind: "drop",
                description: "Remove the listed top-level fields.",
                schema_fn: || schema::<FieldsConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<FieldsConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Drop {
                        fields: cfg.fields,
                    }))
                },
            },
            TransformDef {
                kind: "set",
                description: "Set named fields to constant values on every record.",
                schema_fn: || schema::<SetConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<SetConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Set {
                        values: cfg.values,
                    }))
                },
            },
            TransformDef {
                kind: "rename_field",
                description: "Rename specific top-level fields by name.",
                schema_fn: || schema::<RenameFieldConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<RenameFieldConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::RenameField {
                        fields: cfg.fields,
                    }))
                },
            },
            TransformDef {
                kind: "cast",
                description: "Coerce named fields to int / float / bool / string / timestamp.",
                schema_fn: || schema::<CastConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<CastConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Cast {
                        fields: cfg.fields,
                        on_error: cfg.on_error,
                    }))
                },
            },
            TransformDef {
                kind: "redact",
                description: "Overwrite the listed fields with a mask value (default `***`).",
                schema_fn: || schema::<RedactConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<RedactConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::Redact {
                        fields: cfg.fields,
                        mask: cfg.mask,
                    }))
                },
            },
            TransformDef {
                kind: "value_case",
                description: "Lowercase, uppercase, or trim the value of named string fields.",
                schema_fn: || schema::<ValueCaseConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<ValueCaseConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::ValueCase {
                        fields: cfg.fields,
                        mode: cfg.mode,
                    }))
                },
            },
            TransformDef {
                kind: "spell_symbols",
                description: "Replace punctuation/symbols in string values with their spelled-out words.",
                schema_fn: || schema::<SpellSymbolsConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<SpellSymbolsConfig>(kind, config)?;
                    Ok(TransformStage::Map(RecordTransform::SpellSymbols {
                        extra: cfg.extra,
                        separator: cfg.separator,
                    }))
                },
            },
            #[cfg(feature = "transform-filter")]
            TransformDef {
                kind: "filter",
                description: "Keep records where a JSONPath predicate is true.",
                schema_fn: || schema::<FilterConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<FilterConfig>(kind, config)?;
                    // Re-use stage's compile-time validation so error messages match.
                    let stage = TransformStage::Filter(faucet_core::FilterSpec {
                        path: cfg.path,
                        op: cfg.op,
                        value: cfg.value,
                    });
                    faucet_core::compile_stage(&stage).map_err(|e| match e {
                        faucet_core::FaucetError::Transform(msg) => CliError::InvalidTransform {
                            name: kind.to_owned(),
                            message: msg,
                        },
                        other => CliError::InvalidTransform {
                            name: kind.to_owned(),
                            message: format!("{other}"),
                        },
                    })?;
                    Ok(stage)
                },
            },
            #[cfg(feature = "transform-explode")]
            TransformDef {
                kind: "explode",
                description: "Expand an array field into one record per element.",
                schema_fn: || schema::<ExplodeConfig>(),
                compile_fn: |kind, config| {
                    let cfg = decode::<ExplodeConfig>(kind, config)?;
                    let stage = TransformStage::Explode(faucet_core::ExplodeSpec {
                        path: cfg.path,
                        prefix: cfg.prefix,
                        separator: cfg.separator,
                        on_missing: cfg.on_missing,
                    });
                    faucet_core::compile_stage(&stage).map_err(|e| match e {
                        faucet_core::FaucetError::Transform(msg) => CliError::InvalidTransform {
                            name: kind.to_owned(),
                            message: msg,
                        },
                        other => CliError::InvalidTransform {
                            name: kind.to_owned(),
                            message: format!("{other}"),
                        },
                    })?;
                    Ok(stage)
                },
            },
        ]
    }
    #[cfg(not(feature = "transforms"))]
    {
        Vec::new()
    }
}

/// Compile a list of [`TransformSpec`]s into [`TransformStage`]s in the
/// declared order. Most built-ins compile to a [`TransformStage::Map`];
/// richer stages (e.g. `filter`, future fan-outs) compile to other
/// variants. Unknown or malformed entries surface as a `CliError`.
pub fn compile_transforms(specs: &[TransformSpec]) -> CliResult<Vec<TransformStage>> {
    let mut out = Vec::with_capacity(specs.len());
    for s in specs {
        out.push(compile_one(s)?);
    }
    Ok(out)
}

fn compile_one(spec: &TransformSpec) -> CliResult<TransformStage> {
    match registry().into_iter().find(|t| t.kind == spec.kind) {
        Some(def) => (def.compile_fn)(&spec.kind, spec.config.clone()),
        None => Err(unknown_transform(&spec.kind)),
    }
}

/// One-line summary of every transform compiled into this build. Used by
/// `faucet list`.
pub fn transform_descriptions() -> Vec<(&'static str, &'static str)> {
    registry()
        .into_iter()
        .map(|t| (t.kind, t.description))
        .collect()
}

/// Names of every transform compiled into this build.
pub fn available_transforms() -> Vec<&'static str> {
    registry().into_iter().map(|t| t.kind).collect()
}

// Keep in sync with faucet_core::{RecordCheck, BatchCheck} — one entry per check variant.
/// One-line descriptions of the available quality checks, for `faucet list`.
pub fn quality_descriptions() -> Vec<(&'static str, &'static str)> {
    vec![
        ("not_null", "field present and non-null"),
        ("not_empty", "string non-empty after trim"),
        ("regex_match", "string matches a regex"),
        ("value_in_set", "value is in an allowed set"),
        ("not_in_set", "value is not in a forbidden set"),
        ("compare", "numeric/scalar comparison (gt/gte/lt/lte/eq/ne)"),
        ("type_is", "value is of an expected JSON type"),
        ("string_length", "string length within [min,max]"),
        (
            "json_schema",
            "record validates against a JSON Schema (feature-gated)",
        ),
        ("row_count", "batch row count within [min,max]"),
        ("null_rate", "batch null rate of a field <= max"),
        ("unique", "composite key unique within the batch"),
        (
            "distinct_count",
            "distinct values of a field within [min,max]",
        ),
    ]
}

/// Return the JSON Schema for the named transform's config. Mirrors
/// `registry::source_schema` / `sink_schema` so `faucet schema transform <name>`
/// reads symmetrically with the connector variants.
pub fn transform_schema(name: &str) -> CliResult<Value> {
    registry()
        .into_iter()
        .find(|t| t.kind == name)
        .map(|t| (t.schema_fn)())
        .ok_or_else(|| unknown_transform(name))
}

fn unknown_transform(name: &str) -> CliError {
    let available = available_transforms();
    CliError::UnknownTransform {
        name: name.to_owned(),
        available: if available.is_empty() {
            "(none — rebuild faucet-cli with the `transforms` feature enabled)".to_owned()
        } else {
            available.join(", ")
        },
    }
}

#[cfg(feature = "transforms")]
fn schema<T: JsonSchema>() -> Value {
    serde_json::to_value(schema_for!(T)).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
}

#[cfg(feature = "transforms")]
fn decode<T: serde::de::DeserializeOwned>(name: &str, config: Value) -> CliResult<T> {
    serde_json::from_value(config).map_err(|e| CliError::InvalidTransform {
        name: name.to_owned(),
        message: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn empty_list_compiles_to_empty() {
        let out = compile_transforms(&[]).unwrap();
        assert!(out.is_empty());
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_keys_case_and_flatten() {
        let specs = vec![
            TransformSpec {
                kind: "keys_case".into(),
                config: json!({"mode": "snake"}),
            },
            TransformSpec {
                kind: "flatten".into(),
                config: json!({"separator": "."}),
            },
        ];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn keys_case_rejects_unknown_mode() {
        let specs = vec![TransformSpec {
            kind: "keys_case".into(),
            config: json!({"mode": "spongebob"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "keys_case"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn keys_case_requires_mode() {
        let specs = vec![TransformSpec {
            kind: "keys_case".into(),
            config: json!({}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "keys_case"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn snake_case_kind_is_no_longer_recognized() {
        // Removed in favour of `keys_case { mode: snake }`.
        let specs = vec![TransformSpec {
            kind: "snake_case".into(),
            config: json!({}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::UnknownTransform { name, .. } => assert_eq!(name, "snake_case"),
            other => panic!("expected UnknownTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn rename_keys_requires_pattern_and_replacement() {
        let specs = vec![TransformSpec {
            kind: "rename_keys".into(),
            config: json!({"pattern": "^_"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "rename_keys"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[test]
    fn unknown_transform_errors() {
        let specs = vec![TransformSpec {
            kind: "make_uppercase".into(),
            config: json!({}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::UnknownTransform { name, .. } => assert_eq!(name, "make_uppercase"),
            other => panic!("expected UnknownTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_select_and_drop() {
        let specs = vec![
            TransformSpec {
                kind: "select".into(),
                config: json!({"fields": ["id", "name"]}),
            },
            TransformSpec {
                kind: "drop".into(),
                config: json!({"fields": ["secret"]}),
            },
        ];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 2);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_set_with_object_values() {
        let specs = vec![TransformSpec {
            kind: "set".into(),
            config: json!({"values": {"_source": "api", "version": 1}}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_rename_field() {
        let specs = vec![TransformSpec {
            kind: "rename_field".into(),
            config: json!({"fields": {"old": "new"}}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn compiles_cast_with_default_on_error() {
        let specs = vec![TransformSpec {
            kind: "cast".into(),
            config: json!({"fields": {"age": "int", "price": "float"}}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn cast_rejects_unknown_target_type() {
        let specs = vec![TransformSpec {
            kind: "cast".into(),
            config: json!({"fields": {"x": "uuid"}}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "cast"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn cast_rejects_unknown_on_error_mode() {
        let specs = vec![TransformSpec {
            kind: "cast".into(),
            config: json!({"fields": {"x": "int"}, "on_error": "explode"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "cast"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn redact_uses_default_mask_when_omitted() {
        let specs = vec![TransformSpec {
            kind: "redact".into(),
            config: json!({"fields": ["ssn"]}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn value_case_requires_mode() {
        let specs = vec![TransformSpec {
            kind: "value_case".into(),
            config: json!({"fields": ["email"]}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "value_case"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn available_transforms_lists_every_kind() {
        let names = available_transforms();
        for expected in [
            "flatten",
            "rename_keys",
            "keys_case",
            "select",
            "drop",
            "set",
            "rename_field",
            "cast",
            "redact",
            "value_case",
            "spell_symbols",
            "filter",
            "explode",
        ] {
            assert!(names.contains(&expected), "missing {expected}");
        }
        assert!(
            !names.contains(&"snake_case"),
            "snake_case must be removed in favour of keys_case"
        );
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn transform_descriptions_covers_every_compiled_kind() {
        // descriptions and available_transforms must never drift — `faucet list`
        // and the `UnknownTransform` "Available:" line both read from this.
        let names = available_transforms();
        let desc_names: Vec<&'static str> = transform_descriptions()
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(names, desc_names);
        for (_, desc) in transform_descriptions() {
            assert!(!desc.is_empty(), "every transform needs a description");
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn transform_schema_returns_object_for_every_kind() {
        for name in available_transforms() {
            let schema = transform_schema(name).unwrap_or_else(|e| {
                panic!("schema lookup failed for {name}: {e}");
            });
            assert!(schema.is_object(), "schema for {name} must be an object");
        }
    }

    #[cfg(feature = "transforms")]
    #[test]
    fn transform_schema_select_and_drop_share_shape() {
        // Both accept `{ fields: Vec<String> }` — the schema is the same object,
        // just titled `FieldsConfig`.
        let select = transform_schema("select").unwrap();
        let drop = transform_schema("drop").unwrap();
        assert_eq!(select, drop);
    }

    #[test]
    fn transform_schema_unknown_errors_with_available_list() {
        let err = transform_schema("make_uppercase").unwrap_err();
        match err {
            CliError::UnknownTransform { name, available } => {
                assert_eq!(name, "make_uppercase");
                #[cfg(feature = "transforms")]
                assert!(available.contains("flatten"), "{available}");
                #[cfg(not(feature = "transforms"))]
                assert!(available.contains("rebuild"), "{available}");
            }
            other => panic!("expected UnknownTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn compiles_filter_eq() {
        let specs = vec![TransformSpec {
            kind: "filter".into(),
            config: json!({"path": "status", "op": "eq", "value": "active"}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], TransformStage::Filter(_)));
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_rejects_in_with_non_array_value() {
        let specs = vec![TransformSpec {
            kind: "filter".into(),
            config: json!({"path": "v", "op": "in", "value": "scalar"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, message } => {
                assert_eq!(name, "filter");
                assert!(message.contains("requires an array"), "{message}");
            }
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_rejects_exists_with_value() {
        let specs = vec![TransformSpec {
            kind: "filter".into(),
            config: json!({"path": "v", "op": "exists", "value": "x"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "filter"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_rejects_bad_path() {
        let specs = vec![TransformSpec {
            kind: "filter".into(),
            config: json!({"path": "$..items", "op": "exists"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "filter"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn compiles_explode_with_defaults() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({"path": "items"}),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
        assert!(matches!(out[0], TransformStage::Explode(_)));
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn compiles_explode_with_custom_prefix_and_on_missing() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({
                "path": "items",
                "prefix": "item",
                "separator": "_",
                "on_missing": "drop"
            }),
        }];
        let out = compile_transforms(&specs).unwrap();
        assert_eq!(out.len(), 1);
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_rejects_bad_path() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({"path": "$..items"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "explode"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_rejects_invalid_on_missing() {
        let specs = vec![TransformSpec {
            kind: "explode".into(),
            config: json!({"path": "items", "on_missing": "explode_harder"}),
        }];
        let err = compile_transforms(&specs).unwrap_err();
        match err {
            CliError::InvalidTransform { name, .. } => assert_eq!(name, "explode"),
            other => panic!("expected InvalidTransform, got {other:?}"),
        }
    }

    #[test]
    fn quality_descriptions_has_one_entry_per_check() {
        // 9 per-record checks (incl. json_schema) + 4 per-batch checks = 13.
        // If you add a RecordCheck/BatchCheck variant in faucet-core, add its
        // description here too.
        assert_eq!(quality_descriptions().len(), 13);
    }
}
