//! Build a `PipelineConfig` from a snapshot of `FAUCET_*` environment variables.
//!
//! The public surface is intentionally split between pure functions (taking a
//! `HashMap<String, String>` env snapshot — fully testable) and a thin shell
//! `from_process_env()` that captures `std::env::vars()`.
//!
//! See `cli/README.md` and issue #42 for the user-facing variable schema.

use crate::config::{ConnectorSpec, StateStoreSpec, TransformSpec};
use crate::error::{CliError, CliResult};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};

/// Walk every env var starting with `prefix`, strip the prefix, lowercase the
/// remainder into a field name, apply the `_JSON` precedence rule, and assemble
/// a `Value::Object`. Returns the object verbatim — no shape validation; the
/// connector's own `Deserialize` impl is the gate.
///
/// The `_JSON` suffix is the escape hatch for nested / tagged-enum fields
/// (auth, pagination, etc.) that don't flatten cleanly into env-var names.
pub fn extract_scope(env: &HashMap<String, String>, prefix: &str) -> CliResult<Value> {
    let mut object: Map<String, Value> = Map::new();
    // Track which env var supplied each field so a conflict error can name both.
    let mut json_fields: HashMap<String, String> = HashMap::new();
    let mut scalar_fields: HashMap<String, String> = HashMap::new();

    for (key, value) in env {
        let Some(suffix) = key.strip_prefix(prefix) else {
            continue;
        };
        if suffix.is_empty() {
            // Bare prefix (e.g. exactly "FAUCET_SOURCE_REST_") — skip, no field.
            continue;
        }
        let lowercase = suffix.to_ascii_lowercase();
        if let Some(field) = lowercase.strip_suffix("_json") {
            if let Some(scalar_var) = scalar_fields.get(field) {
                return Err(CliError::EnvConflict {
                    field: field.to_owned(),
                    scalar_var: scalar_var.clone(),
                    json_var: key.clone(),
                });
            }
            let parsed: Value =
                serde_json::from_str(value).map_err(|e| CliError::InvalidEnvJson {
                    var: key.clone(),
                    message: e.to_string(),
                })?;
            object.insert(field.to_owned(), parsed);
            json_fields.insert(field.to_owned(), key.clone());
        } else {
            if let Some(json_var) = json_fields.get(&lowercase) {
                return Err(CliError::EnvConflict {
                    field: lowercase.clone(),
                    scalar_var: key.clone(),
                    json_var: json_var.clone(),
                });
            }
            object.insert(lowercase.clone(), coerce_scalar(value));
            scalar_fields.insert(lowercase, key.clone());
        }
    }
    Ok(Value::Object(object))
}

/// Try to parse `s` as a JSON value (numbers, bools, null, strings, objects,
/// arrays). Falls back to a plain `Value::String` if the parse fails. This
/// matches YAML's auto-typing so `30` becomes a number and `true` becomes a
/// bool — the connector's `Deserialize` impl gets exactly what it would have
/// gotten via YAML.
fn coerce_scalar(s: &str) -> Value {
    serde_json::from_str::<Value>(s).unwrap_or_else(|_| Value::String(s.to_owned()))
}

/// Construct the source [`ConnectorSpec`] from `FAUCET_SOURCE` + `FAUCET_SOURCE_<KIND>_*`.
pub fn build_source(env: &HashMap<String, String>) -> CliResult<ConnectorSpec> {
    let kind = env
        .get("FAUCET_SOURCE")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CliError::MissingEnvSelector {
            var: "FAUCET_SOURCE".to_owned(),
        })?
        .clone();
    let prefix = format!(
        "FAUCET_SOURCE_{}_",
        kind.to_ascii_uppercase().replace('-', "_")
    );
    let config = extract_scope(env, &prefix)?;
    Ok(ConnectorSpec { kind, config })
}

/// Construct the sink [`ConnectorSpec`] from `FAUCET_SINK` + `FAUCET_SINK_<KIND>_*`.
pub fn build_sink(env: &HashMap<String, String>) -> CliResult<ConnectorSpec> {
    let kind = env
        .get("FAUCET_SINK")
        .filter(|v| !v.is_empty())
        .ok_or_else(|| CliError::MissingEnvSelector {
            var: "FAUCET_SINK".to_owned(),
        })?
        .clone();
    let prefix = format!(
        "FAUCET_SINK_{}_",
        kind.to_ascii_uppercase().replace('-', "_")
    );
    let config = extract_scope(env, &prefix)?;
    Ok(ConnectorSpec { kind, config })
}

/// Construct an optional [`StateStoreSpec`] from `FAUCET_STATE` + `FAUCET_STATE_<KIND>_*`.
/// Returns `Ok(None)` when `FAUCET_STATE` is unset or empty (the common case).
pub fn build_state(env: &HashMap<String, String>) -> CliResult<Option<StateStoreSpec>> {
    let Some(kind) = env.get("FAUCET_STATE").filter(|v| !v.is_empty()).cloned() else {
        return Ok(None);
    };
    let prefix = format!(
        "FAUCET_STATE_{}_",
        kind.to_ascii_uppercase().replace('-', "_")
    );
    let config = extract_scope(env, &prefix)?;
    Ok(Some(StateStoreSpec { kind, config }))
}

/// Construct an ordered `Vec<TransformSpec>` from `FAUCET_TRANSFORM_<N>` selectors
/// and `FAUCET_TRANSFORM_<N>_<FIELD>` config fields. Indices must be contiguous
/// starting at 1; any gap is an error so a misnumbered var never silently drops
/// a transform.
pub fn build_transforms(env: &HashMap<String, String>) -> CliResult<Vec<TransformSpec>> {
    // Collect the kind selectors first — keys that are exactly
    // `FAUCET_TRANSFORM_<digits>` (no trailing field name).
    let mut kinds: BTreeMap<u32, String> = BTreeMap::new();
    for (key, value) in env {
        let Some(rest) = key.strip_prefix("FAUCET_TRANSFORM_") else {
            continue;
        };
        if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        let Ok(idx) = rest.parse::<u32>() else {
            continue;
        };
        kinds.insert(idx, value.clone());
    }
    if kinds.is_empty() {
        return Ok(Vec::new());
    }
    // Indices must be 1, 2, 3, …
    for (expected, actual) in (1u32..).zip(kinds.keys().copied()) {
        if expected != actual {
            return Err(CliError::TransformIndexGap { missing: expected });
        }
    }
    // Harvest per-transform config blocks.
    let mut out = Vec::with_capacity(kinds.len());
    for (idx, kind) in kinds {
        let prefix = format!("FAUCET_TRANSFORM_{idx}_");
        let config = extract_scope(env, &prefix)?;
        out.push(TransformSpec { kind, config });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn extract_scope_lowercases_field_names() {
        let e = env(&[("FAUCET_SOURCE_REST_BASE_URL", "https://x.example")]);
        let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
        assert_eq!(v, json!({"base_url": "https://x.example"}));
    }

    #[test]
    fn extract_scope_ignores_unrelated_keys() {
        let e = env(&[
            ("FAUCET_SOURCE_REST_BASE_URL", "https://x.example"),
            ("PATH", "/usr/bin"),
            ("FAUCET_SINK_JSONL_PATH", "./out.jsonl"),
        ]);
        let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
        assert_eq!(v, json!({"base_url": "https://x.example"}));
    }

    #[test]
    fn extract_scope_coerces_numbers_and_bools() {
        let e = env(&[
            ("FAUCET_SOURCE_REST_TIMEOUT_SECS", "30"),
            ("FAUCET_SOURCE_REST_FOLLOW_REDIRECTS", "true"),
            ("FAUCET_SOURCE_REST_BASE_URL", "https://x.example"),
        ]);
        let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
        assert_eq!(v["timeout_secs"], json!(30));
        assert_eq!(v["follow_redirects"], json!(true));
        assert_eq!(v["base_url"], json!("https://x.example"));
    }

    #[test]
    fn extract_scope_handles_json_suffix() {
        let e = env(&[(
            "FAUCET_SOURCE_REST_AUTH_JSON",
            r#"{"type":"ApiKey","header":"Authorization","value":"Bearer x"}"#,
        )]);
        let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
        assert_eq!(
            v["auth"],
            json!({"type": "ApiKey", "header": "Authorization", "value": "Bearer x"})
        );
    }

    #[test]
    fn extract_scope_rejects_invalid_json_suffix() {
        let e = env(&[("FAUCET_SOURCE_REST_AUTH_JSON", "not-json")]);
        let err = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap_err();
        match err {
            CliError::InvalidEnvJson { var, .. } => {
                assert_eq!(var, "FAUCET_SOURCE_REST_AUTH_JSON")
            }
            other => panic!("expected InvalidEnvJson, got {other:?}"),
        }
    }

    #[test]
    fn extract_scope_conflict_scalar_then_json() {
        let e = env(&[
            ("FAUCET_SOURCE_REST_AUTH", "bearer"),
            ("FAUCET_SOURCE_REST_AUTH_JSON", r#"{"type":"ApiKey"}"#),
        ]);
        let err = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap_err();
        match err {
            CliError::EnvConflict {
                field,
                scalar_var,
                json_var,
            } => {
                assert_eq!(field, "auth");
                assert_eq!(scalar_var, "FAUCET_SOURCE_REST_AUTH");
                assert_eq!(json_var, "FAUCET_SOURCE_REST_AUTH_JSON");
            }
            other => panic!("expected EnvConflict, got {other:?}"),
        }
    }

    #[test]
    fn extract_scope_conflict_detection_is_order_independent() {
        // HashMap iteration order is randomized per instance via the random
        // hasher state; loop 50 times to exercise both ordering branches
        // (scalar-arriving-first AND json-arriving-first) statistically.
        for _ in 0..50 {
            let e = env(&[
                ("FAUCET_SOURCE_REST_AUTH", "bearer"),
                ("FAUCET_SOURCE_REST_AUTH_JSON", r#"{"type":"ApiKey"}"#),
            ]);
            let err = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap_err();
            match err {
                CliError::EnvConflict {
                    field,
                    scalar_var,
                    json_var,
                } => {
                    assert_eq!(field, "auth");
                    assert_eq!(scalar_var, "FAUCET_SOURCE_REST_AUTH");
                    assert_eq!(json_var, "FAUCET_SOURCE_REST_AUTH_JSON");
                }
                other => panic!("expected EnvConflict, got {other:?}"),
            }
        }
    }

    #[test]
    fn extract_scope_skips_bare_prefix() {
        let e = env(&[("FAUCET_SOURCE_REST_", "ignored")]);
        let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
        assert_eq!(v, json!({}));
    }

    #[test]
    fn extract_scope_empty_when_no_matches() {
        let e = env(&[("PATH", "/usr/bin")]);
        let v = extract_scope(&e, "FAUCET_SOURCE_REST_").unwrap();
        assert_eq!(v, json!({}));
    }

    #[test]
    fn build_source_reads_selector_and_scope() {
        let e = env(&[
            ("FAUCET_SOURCE", "rest"),
            ("FAUCET_SOURCE_REST_BASE_URL", "https://x.example"),
            ("FAUCET_SOURCE_REST_TIMEOUT_SECS", "30"),
        ]);
        let spec = build_source(&e).unwrap();
        assert_eq!(spec.kind, "rest");
        assert_eq!(spec.config["base_url"], json!("https://x.example"));
        assert_eq!(spec.config["timeout_secs"], json!(30));
    }

    #[test]
    fn build_source_uses_kind_scope_so_other_kinds_dont_leak() {
        let e = env(&[
            ("FAUCET_SOURCE", "csv"),
            ("FAUCET_SOURCE_CSV_PATH", "./in.csv"),
            ("FAUCET_SOURCE_REST_BASE_URL", "https://other.example"),
        ]);
        let spec = build_source(&e).unwrap();
        assert_eq!(spec.kind, "csv");
        assert_eq!(spec.config, json!({"path": "./in.csv"}));
    }

    #[test]
    fn build_source_errors_when_selector_missing() {
        let e = env(&[("FAUCET_SOURCE_REST_BASE_URL", "https://x.example")]);
        let err = build_source(&e).unwrap_err();
        match err {
            CliError::MissingEnvSelector { var } => assert_eq!(var, "FAUCET_SOURCE"),
            other => panic!("expected MissingEnvSelector, got {other:?}"),
        }
    }

    #[test]
    fn build_source_errors_when_selector_empty() {
        let e = env(&[("FAUCET_SOURCE", "")]);
        let err = build_source(&e).unwrap_err();
        match err {
            CliError::MissingEnvSelector { var } => assert_eq!(var, "FAUCET_SOURCE"),
            other => panic!("expected MissingEnvSelector, got {other:?}"),
        }
    }

    #[test]
    fn build_sink_reads_selector_and_scope() {
        let e = env(&[
            ("FAUCET_SINK", "jsonl"),
            ("FAUCET_SINK_JSONL_PATH", "./out.jsonl"),
        ]);
        let spec = build_sink(&e).unwrap();
        assert_eq!(spec.kind, "jsonl");
        assert_eq!(spec.config, json!({"path": "./out.jsonl"}));
    }

    #[test]
    fn build_sink_errors_when_selector_missing() {
        let e = env(&[("FAUCET_SINK_JSONL_PATH", "./out.jsonl")]);
        let err = build_sink(&e).unwrap_err();
        match err {
            CliError::MissingEnvSelector { var } => assert_eq!(var, "FAUCET_SINK"),
            other => panic!("expected MissingEnvSelector, got {other:?}"),
        }
    }

    #[test]
    fn build_sink_errors_when_selector_empty() {
        let e = env(&[("FAUCET_SINK", "")]);
        let err = build_sink(&e).unwrap_err();
        match err {
            CliError::MissingEnvSelector { var } => assert_eq!(var, "FAUCET_SINK"),
            other => panic!("expected MissingEnvSelector, got {other:?}"),
        }
    }

    #[test]
    fn build_state_returns_none_when_unset() {
        let e = env(&[("FAUCET_SOURCE", "rest")]);
        let spec = build_state(&e).unwrap();
        assert!(spec.is_none());
    }

    #[test]
    fn build_state_returns_none_when_empty() {
        let e = env(&[("FAUCET_STATE", "")]);
        let spec = build_state(&e).unwrap();
        assert!(spec.is_none());
    }

    #[test]
    fn build_state_reads_file_backend() {
        let e = env(&[
            ("FAUCET_STATE", "file"),
            ("FAUCET_STATE_FILE_PATH", "./.faucet-state"),
        ]);
        let spec = build_state(&e).unwrap().unwrap();
        assert_eq!(spec.kind, "file");
        assert_eq!(spec.config, json!({"path": "./.faucet-state"}));
    }

    #[test]
    fn build_state_reads_memory_with_empty_scope() {
        let e = env(&[("FAUCET_STATE", "memory")]);
        let spec = build_state(&e).unwrap().unwrap();
        assert_eq!(spec.kind, "memory");
        assert_eq!(spec.config, json!({}));
    }

    #[test]
    fn build_transforms_empty_when_unset() {
        let e = env(&[("FAUCET_SOURCE", "rest")]);
        let t = build_transforms(&e).unwrap();
        assert!(t.is_empty());
    }

    #[test]
    fn build_transforms_single_kind_no_config() {
        let e = env(&[("FAUCET_TRANSFORM_1", "snake_case")]);
        let t = build_transforms(&e).unwrap();
        assert_eq!(t.len(), 1);
        assert_eq!(t[0].kind, "snake_case");
        assert_eq!(t[0].config, json!({}));
    }

    #[test]
    fn build_transforms_ordered_and_with_config() {
        let e = env(&[
            ("FAUCET_TRANSFORM_1", "snake_case"),
            ("FAUCET_TRANSFORM_2", "flatten"),
            ("FAUCET_TRANSFORM_2_SEPARATOR", "__"),
        ]);
        let t = build_transforms(&e).unwrap();
        assert_eq!(t.len(), 2);
        assert_eq!(t[0].kind, "snake_case");
        assert_eq!(t[1].kind, "flatten");
        assert_eq!(t[1].config, json!({"separator": "__"}));
    }

    #[test]
    fn build_transforms_handles_double_digit_indices() {
        let e = env(&[
            ("FAUCET_TRANSFORM_1", "snake_case"),
            ("FAUCET_TRANSFORM_2", "flatten"),
            ("FAUCET_TRANSFORM_3", "rename_keys"),
        ]);
        let t = build_transforms(&e).unwrap();
        assert_eq!(t.len(), 3);
        assert_eq!(t[2].kind, "rename_keys");
    }

    #[test]
    fn build_transforms_gap_errors() {
        let e = env(&[
            ("FAUCET_TRANSFORM_1", "snake_case"),
            ("FAUCET_TRANSFORM_3", "flatten"),
        ]);
        let err = build_transforms(&e).unwrap_err();
        match err {
            CliError::TransformIndexGap { missing } => assert_eq!(missing, 2),
            other => panic!("expected TransformIndexGap, got {other:?}"),
        }
    }

    #[test]
    fn build_transforms_must_start_at_one() {
        let e = env(&[("FAUCET_TRANSFORM_2", "snake_case")]);
        let err = build_transforms(&e).unwrap_err();
        match err {
            CliError::TransformIndexGap { missing } => assert_eq!(missing, 1),
            other => panic!("expected TransformIndexGap, got {other:?}"),
        }
    }

    #[test]
    fn build_transforms_ignores_field_vars_when_indexing_kinds() {
        // FAUCET_TRANSFORM_1_SEPARATOR should NOT be mistaken for a kind at index 1.
        let e = env(&[("FAUCET_TRANSFORM_1_SEPARATOR", "__")]);
        // No FAUCET_TRANSFORM_1 selector means no transforms at all (not a gap error,
        // because the indices set is empty).
        let t = build_transforms(&e).unwrap();
        assert!(t.is_empty());
    }
}
