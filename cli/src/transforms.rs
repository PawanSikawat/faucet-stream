//! Compile YAML/JSON transform declarations into `RecordTransform` values.
//!
//! The built-in transforms are exposed via config — custom closure
//! transforms require Rust code and are reserved for the library API.

use crate::config::TransformSpec;
use crate::error::{CliError, CliResult};
use faucet_core::RecordTransform;
#[cfg(feature = "transforms")]
use faucet_core::{CastOnError, CastType, KeyCaseMode, ValueCaseMode};
use serde::Deserialize;
use serde_json::Value;
#[cfg(feature = "transforms")]
use std::collections::HashMap;

/// Inline-config schema for the `flatten` transform.
#[derive(Debug, Deserialize)]
struct FlattenConfig {
    #[serde(default = "default_separator")]
    separator: String,
}

fn default_separator() -> String {
    "__".to_owned()
}

/// Inline-config schema for the `rename_keys` transform.
#[derive(Debug, Deserialize)]
struct RenameKeysConfig {
    pattern: String,
    replacement: String,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize)]
struct FieldsConfig {
    fields: Vec<String>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize)]
struct SetConfig {
    values: serde_json::Map<String, Value>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize)]
struct RenameFieldConfig {
    fields: HashMap<String, String>,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize)]
struct CastConfig {
    fields: HashMap<String, CastType>,
    #[serde(default)]
    on_error: CastOnError,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize)]
struct RedactConfig {
    fields: Vec<String>,
    #[serde(default = "default_mask")]
    mask: Value,
}

#[cfg(feature = "transforms")]
fn default_mask() -> Value {
    Value::String("***".to_owned())
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize)]
struct ValueCaseConfig {
    fields: Vec<String>,
    mode: ValueCaseMode,
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize)]
struct SpellSymbolsConfig {
    #[serde(default)]
    extra: HashMap<String, String>,
    #[serde(default = "default_spell_separator")]
    separator: String,
}

#[cfg(feature = "transforms")]
fn default_spell_separator() -> String {
    " ".to_owned()
}

#[cfg(feature = "transforms")]
#[derive(Debug, Deserialize)]
struct KeysCaseConfig {
    mode: KeyCaseMode,
}

/// Compile a list of [`TransformSpec`]s into [`RecordTransform`]s in the
/// declared order. Unknown or malformed entries surface as a `CliError`.
pub fn compile_transforms(specs: &[TransformSpec]) -> CliResult<Vec<RecordTransform>> {
    let mut out = Vec::with_capacity(specs.len());
    for s in specs {
        out.push(compile_one(s)?);
    }
    Ok(out)
}

fn compile_one(spec: &TransformSpec) -> CliResult<RecordTransform> {
    match spec.kind.as_str() {
        #[cfg(feature = "transforms")]
        "flatten" => {
            let cfg = decode::<FlattenConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::Flatten {
                separator: cfg.separator,
            })
        }
        #[cfg(feature = "transforms")]
        "rename_keys" => {
            let cfg = decode::<RenameKeysConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::RenameKeys {
                pattern: cfg.pattern,
                replacement: cfg.replacement,
            })
        }
        #[cfg(feature = "transforms")]
        "keys_case" => {
            let cfg = decode::<KeysCaseConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::KeysCase { mode: cfg.mode })
        }
        #[cfg(feature = "transforms")]
        "select" => {
            let cfg = decode::<FieldsConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::Select { fields: cfg.fields })
        }
        #[cfg(feature = "transforms")]
        "drop" => {
            let cfg = decode::<FieldsConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::Drop { fields: cfg.fields })
        }
        #[cfg(feature = "transforms")]
        "set" => {
            let cfg = decode::<SetConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::Set { values: cfg.values })
        }
        #[cfg(feature = "transforms")]
        "rename_field" => {
            let cfg = decode::<RenameFieldConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::RenameField { fields: cfg.fields })
        }
        #[cfg(feature = "transforms")]
        "cast" => {
            let cfg = decode::<CastConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::Cast {
                fields: cfg.fields,
                on_error: cfg.on_error,
            })
        }
        #[cfg(feature = "transforms")]
        "redact" => {
            let cfg = decode::<RedactConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::Redact {
                fields: cfg.fields,
                mask: cfg.mask,
            })
        }
        #[cfg(feature = "transforms")]
        "value_case" => {
            let cfg = decode::<ValueCaseConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::ValueCase {
                fields: cfg.fields,
                mode: cfg.mode,
            })
        }
        #[cfg(feature = "transforms")]
        "spell_symbols" => {
            let cfg = decode::<SpellSymbolsConfig>(&spec.kind, spec.config.clone())?;
            Ok(RecordTransform::SpellSymbols {
                extra: cfg.extra,
                separator: cfg.separator,
            })
        }
        other => Err(CliError::UnknownTransform {
            name: other.to_owned(),
            available: available_transforms().join(", "),
        }),
    }
}

/// Names of every transform compiled into this build.
pub fn available_transforms() -> Vec<&'static str> {
    #[cfg(feature = "transforms")]
    {
        vec![
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
        ]
    }
    #[cfg(not(feature = "transforms"))]
    {
        Vec::new()
    }
}

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
        ] {
            assert!(names.contains(&expected), "missing {expected}");
        }
        assert!(
            !names.contains(&"snake_case"),
            "snake_case must be removed in favour of keys_case"
        );
    }
}
