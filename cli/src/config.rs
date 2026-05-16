//! Parsed `pipeline.yaml` / `pipeline.json` schema.
//!
//! The wire format is intentionally loose: every connector keeps its own
//! config schema, and the CLI threads a `serde_json::Value` through to the
//! connector's `serde::Deserialize` impl. That keeps this struct stable as
//! new fields are added to individual connectors without needing CLI work.

use crate::error::{CliError, CliResult};
use crate::interpolate::interpolate;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};

/// Top-level pipeline definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    /// Config-format version. Currently always `1`.
    #[serde(default = "default_version")]
    pub version: u32,

    /// Optional human-readable name (used in logs and error messages).
    #[serde(default)]
    pub name: Option<String>,

    /// The source connector — fetches records.
    pub source: ConnectorSpec,

    /// Record transforms applied between source and sink, in declaration order.
    #[serde(default)]
    pub transforms: Vec<TransformSpec>,

    /// The sink connector — writes records.
    pub sink: ConnectorSpec,

    /// Optional state store for incremental-replication bookmarks.
    #[serde(default)]
    pub state: Option<StateStoreSpec>,
}

fn default_version() -> u32 {
    1
}

/// A `{ type, config }` block, the universal shape for both sources and sinks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorSpec {
    /// Connector type — matches the suffix of the underlying crate
    /// (e.g. `rest` for `faucet-source-rest`).
    #[serde(rename = "type")]
    pub kind: String,

    /// Connector-specific config object. Passed through verbatim to the
    /// connector's `serde::Deserialize` impl.
    #[serde(default = "empty_object")]
    pub config: Value,
}

fn empty_object() -> Value {
    Value::Object(Default::default())
}

/// A single transform declaration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformSpec {
    /// Built-in transform identifier: `flatten`, `rename_keys`, `snake_case`.
    #[serde(rename = "type")]
    pub kind: String,

    /// Transform-specific config object (e.g. `{ separator: "__" }` for flatten).
    #[serde(default = "empty_object")]
    pub config: Value,
}

/// State-store backend selector.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateStoreSpec {
    /// Store type: `file`, `memory`, `redis`, or `postgres`.
    #[serde(rename = "type")]
    pub kind: String,

    /// Store-specific config.
    #[serde(default = "empty_object")]
    pub config: Value,
}

impl PipelineConfig {
    /// Load a pipeline config from disk. The file extension determines the
    /// parser: `.yaml` / `.yml` → YAML, `.json` → JSON. Other extensions are
    /// rejected.
    pub fn from_path(path: impl AsRef<Path>) -> CliResult<Self> {
        let path = path.as_ref();
        let raw = std::fs::read_to_string(path).map_err(|source| CliError::ReadConfig {
            path: path.to_path_buf(),
            source,
        })?;
        let interpolated = interpolate(&raw)?;
        Self::from_text(&interpolated, path)
    }

    /// Parse an interpolated config string. `path` is only used for error
    /// messages and to pick the parser based on extension.
    pub fn from_text(text: &str, path: &Path) -> CliResult<Self> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase);
        let cfg: PipelineConfig = match ext.as_deref() {
            Some("yaml" | "yml") => {
                serde_yaml::from_str(text).map_err(|e| CliError::ParseConfig {
                    path: path.to_path_buf(),
                    message: e.to_string(),
                })?
            }
            Some("json") => serde_json::from_str(text).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: e.to_string(),
            })?,
            _ => {
                return Err(CliError::UnknownExtension {
                    path: path.to_path_buf(),
                });
            }
        };
        if cfg.version != 1 {
            return Err(CliError::ParseConfig {
                path: path.to_path_buf(),
                message: format!(
                    "unsupported pipeline version {}, only version 1 is recognised",
                    cfg.version
                ),
            });
        }
        Ok(cfg)
    }
}

/// Convenience: parse a config from text using a synthetic path so the right
/// parser is selected. Used by tests and the `validate --stdin` flow.
pub fn parse_with_extension(text: &str, ext: &str) -> CliResult<PipelineConfig> {
    let synthetic = PathBuf::from(format!("pipeline.{ext}"));
    PipelineConfig::from_text(text, &synthetic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_minimal_yaml() {
        let yaml = r#"
version: 1
source:
  type: rest
  config:
    base_url: https://api.example.com
sink:
  type: jsonl
  config:
    path: ./out.jsonl
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.source.kind, "rest");
        assert_eq!(cfg.sink.kind, "jsonl");
        assert_eq!(cfg.transforms.len(), 0);
        assert!(cfg.state.is_none());
    }

    #[test]
    fn parses_minimal_json() {
        let raw = r#"{
            "version": 1,
            "source": {"type": "rest", "config": {}},
            "sink": {"type": "jsonl", "config": {"path": "./out.jsonl"}}
        }"#;
        let cfg = parse_with_extension(raw, "json").unwrap();
        assert_eq!(cfg.source.kind, "rest");
    }

    #[test]
    fn rejects_unknown_extension() {
        let text = "version: 1\n";
        let err = PipelineConfig::from_text(text, Path::new("pipeline.toml")).unwrap_err();
        assert!(matches!(err, CliError::UnknownExtension { .. }));
    }

    #[test]
    fn rejects_future_version() {
        let yaml = r#"
version: 99
source: { type: rest, config: {} }
sink: { type: jsonl, config: { path: "./x" } }
"#;
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        match err {
            CliError::ParseConfig { message, .. } => assert!(message.contains("version 99")),
            other => panic!("expected ParseConfig, got {other:?}"),
        }
    }

    #[test]
    fn transforms_and_state_round_trip() {
        let yaml = r#"
version: 1
source:
  type: rest
  config: {}
transforms:
  - type: snake_case
  - type: flatten
    config: { separator: "__" }
sink:
  type: jsonl
  config: { path: "./out.jsonl" }
state:
  type: file
  config: { path: "./.faucet-state" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.transforms.len(), 2);
        assert_eq!(cfg.transforms[0].kind, "snake_case");
        assert_eq!(cfg.transforms[1].kind, "flatten");
        assert_eq!(cfg.transforms[1].config, json!({"separator": "__"}));
        let state = cfg.state.unwrap();
        assert_eq!(state.kind, "file");
    }

    #[test]
    fn from_path_interpolates_env_var() {
        unsafe { std::env::set_var("FAUCET_CFG_URL", "https://x.example") };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
source:
  type: rest
  config:
    base_url: ${env:FAUCET_CFG_URL}
sink:
  type: jsonl
  config:
    path: ./out.jsonl
"#,
        )
        .unwrap();
        let cfg = PipelineConfig::from_path(&path).unwrap();
        assert_eq!(cfg.source.config["base_url"], "https://x.example");
        unsafe { std::env::remove_var("FAUCET_CFG_URL") };
    }
}
