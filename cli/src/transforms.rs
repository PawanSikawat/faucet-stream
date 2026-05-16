//! Compile YAML/JSON transform declarations into `RecordTransform` values.
//!
//! Only the built-in transforms (flatten, rename_keys, snake_case) are
//! exposed via config — custom closure transforms require Rust code and are
//! reserved for the library API.

use crate::config::TransformSpec;
use crate::error::{CliError, CliResult};
use faucet_core::RecordTransform;
use serde::Deserialize;
use serde_json::Value;

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
        "snake_case" => Ok(RecordTransform::KeysToSnakeCase),
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
        vec!["flatten", "rename_keys", "snake_case"]
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
    fn compiles_snake_case_and_flatten() {
        let specs = vec![
            TransformSpec {
                kind: "snake_case".into(),
                config: json!({}),
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
}
