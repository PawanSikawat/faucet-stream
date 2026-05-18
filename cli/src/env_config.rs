//! Build a `PipelineConfig` from a snapshot of `FAUCET_*` environment variables.
//!
//! The public surface is intentionally split between pure functions (taking a
//! `HashMap<String, String>` env snapshot — fully testable) and a thin shell
//! `from_process_env()` that captures `std::env::vars()`.
//!
//! See `cli/README.md` and issue #42 for the user-facing variable schema.

use crate::config::ConnectorSpec;
use crate::error::{CliError, CliResult};
use serde_json::{Map, Value};
use std::collections::HashMap;

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
}
