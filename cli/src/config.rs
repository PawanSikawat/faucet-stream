//! Parsed `pipeline.yaml` / `pipeline.json` schema (matrix-aware).
//!
//! Top-level shape:
//!
//! ```yaml
//! version: 1
//! name: optional-human-name
//! pipeline:               # required — full base config
//!   source: { type, config }
//!   sink:   { type, config }
//!   transforms: [...]
//!   state:  { type, config }
//! matrix:                 # optional — omitted or empty == one anonymous row
//!   - id: <string>
//!     parent: <id>
//!     parent_key: <jsonpath>   # default "id"
//!     source: { ... }     # partial override, deep-merged into pipeline.source
//!     sink:   { ... }
//!     transforms: [...]   # if Some, replaces pipeline.transforms wholesale
//!     state:  { ... }     # if Some, replaces pipeline.state wholesale
//! execution:              # optional
//!   max_concurrent: <usize>
//!   on_error: continue|stop
//! ```
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
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    /// Config-format version. Currently always `1`.
    #[serde(default = "default_version")]
    pub version: u32,

    /// Optional human-readable name (used in logs and error messages).
    #[serde(default)]
    pub name: Option<String>,

    /// Base pipeline — every matrix row is deep-merged into this.
    pub pipeline: PipelineSpec,

    /// Matrix of per-row overrides. Empty or omitted means "one anonymous row"
    /// (full pipeline runs once with no merge).
    #[serde(default)]
    pub matrix: Vec<MatrixRow>,

    /// Optional execution controls (concurrency, on-error policy).
    #[serde(default)]
    pub execution: Option<ExecutionSpec>,
}

/// The base pipeline definition that every matrix row is deep-merged into.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub source: ConnectorSpec,
    pub sink: ConnectorSpec,
    #[serde(default)]
    pub transforms: Vec<TransformSpec>,
    #[serde(default)]
    pub state: Option<StateStoreSpec>,
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

/// A partial connector override carried by a matrix row. Both `type` and
/// `config` are optional so rows can swap the kind, override only the inner
/// config, or both.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialConnector {
    /// Override the connector kind (otherwise inherits from `pipeline.*`).
    #[serde(rename = "type", default)]
    pub kind: Option<String>,
    /// Partial config object — deep-merged into the inherited base.
    #[serde(default)]
    pub config: Option<Value>,
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

/// One row of the `matrix:` block.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixRow {
    /// Row identifier. Required for parent/child references and runtime
    /// `${id.path}` interpolation. Anonymous rows get a synthetic `row-N` id.
    #[serde(default)]
    pub id: Option<String>,

    /// If set, this row runs once per record produced by the named parent row.
    #[serde(default)]
    pub parent: Option<String>,

    /// Dotted field path inside each parent record that uniquely identifies
    /// the record. Used as the state-key suffix. Default: `id`.
    #[serde(default = "default_parent_key")]
    pub parent_key: String,

    /// Partial override of `pipeline.source` (deep-merged).
    #[serde(default)]
    pub source: Option<PartialConnector>,

    /// Partial override of `pipeline.sink` (deep-merged).
    #[serde(default)]
    pub sink: Option<PartialConnector>,

    /// If `Some`, replaces `pipeline.transforms` wholesale (arrays don't merge).
    #[serde(default)]
    pub transforms: Option<Vec<TransformSpec>>,

    /// If `Some`, replaces `pipeline.state` wholesale.
    #[serde(default)]
    pub state: Option<StateStoreSpec>,
}

/// Execution-time controls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionSpec {
    /// Maximum concurrent pipeline invocations (root + per-parent-record
    /// child invocations all share this budget). Defaults to
    /// `num_cpus::get().min(4)` at runtime when `None`.
    #[serde(default)]
    pub max_concurrent: Option<usize>,

    /// What to do when a pipeline invocation fails.
    #[serde(default)]
    pub on_error: OnError,
}

/// Failure-handling policy across the matrix.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OnError {
    /// Skip the failed invocation's subtree but keep running siblings (default).
    #[default]
    Continue,
    /// Cancel every pending and in-flight invocation on first failure.
    Stop,
}

fn default_version() -> u32 {
    1
}
fn default_parent_key() -> String {
    "id".to_owned()
}
fn empty_object() -> Value {
    Value::Object(Default::default())
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

    /// Parse an already-interpolated config string. `path` is only used for
    /// error messages and to pick the parser by file extension.
    pub fn from_text(text: &str, path: &Path) -> CliResult<Self> {
        let ext = path
            .extension()
            .and_then(|e| e.to_str())
            .map(str::to_ascii_lowercase);
        let cfg: PipelineConfig = match ext.as_deref() {
            Some("yaml" | "yml") => {
                serde_yaml::from_str(text).map_err(|e| CliError::ParseConfig {
                    path: path.to_path_buf(),
                    message: friendly_parse_error(&e.to_string()),
                })?
            }
            Some("json") => serde_json::from_str(text).map_err(|e| CliError::ParseConfig {
                path: path.to_path_buf(),
                message: friendly_parse_error(&e.to_string()),
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

/// Translate the typical serde "missing field" message into a hint when the
/// caller appears to be using the pre-#54 top-level shape.
fn friendly_parse_error(raw: &str) -> String {
    let lower = raw.to_ascii_lowercase();
    if lower.contains("missing field `pipeline`") {
        return format!(
            "{raw}\n\nhint: top-level `source:` / `sink:` is no longer supported. Wrap them in a `pipeline:` block — see `faucet init` for the new shape."
        );
    }
    raw.to_owned()
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
    fn parses_minimal_pipeline_yaml() {
        let yaml = r#"
version: 1
pipeline:
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
        assert_eq!(cfg.pipeline.source.kind, "rest");
        assert_eq!(cfg.pipeline.sink.kind, "jsonl");
        assert!(cfg.matrix.is_empty());
        assert!(cfg.execution.is_none());
        assert!(cfg.pipeline.transforms.is_empty());
        assert!(cfg.pipeline.state.is_none());
    }

    #[test]
    fn parses_minimal_json() {
        let raw = r#"{
            "version": 1,
            "pipeline": {
                "source": {"type": "rest", "config": {}},
                "sink":   {"type": "jsonl", "config": {"path": "./out.jsonl"}}
            }
        }"#;
        let cfg = parse_with_extension(raw, "json").unwrap();
        assert_eq!(cfg.pipeline.source.kind, "rest");
    }

    #[test]
    fn parses_matrix_rows_with_partial_overrides() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://api.example.com } }
  sink:   { type: jsonl, config: { path: ./out.jsonl } }
matrix:
  - id: users
    source: { config: { path: /v1/users } }
    sink:   { config: { path: ./users.jsonl } }
  - id: posts
    parent: users
    parent_key: user_id
    source: { config: { path: "/v1/users/${users.id}/posts" } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.matrix.len(), 2);
        assert_eq!(cfg.matrix[0].id.as_deref(), Some("users"));
        assert!(cfg.matrix[0].parent.is_none());
        let users_src = cfg.matrix[0].source.as_ref().unwrap();
        assert_eq!(users_src.config.as_ref().unwrap()["path"], "/v1/users");

        assert_eq!(cfg.matrix[1].parent.as_deref(), Some("users"));
        assert_eq!(cfg.matrix[1].parent_key, "user_id");
    }

    #[test]
    fn parent_key_defaults_to_id() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
matrix:
  - { id: users }
  - { id: posts, parent: users }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.matrix[1].parent_key, "id");
    }

    #[test]
    fn parses_execution_block() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
execution:
  max_concurrent: 8
  on_error: stop
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let exec = cfg.execution.unwrap();
        assert_eq!(exec.max_concurrent, Some(8));
        assert_eq!(exec.on_error, OnError::Stop);
    }

    #[test]
    fn on_error_defaults_to_continue() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
execution: { max_concurrent: 2 }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert_eq!(cfg.execution.unwrap().on_error, OnError::Continue);
    }

    #[test]
    fn rejects_old_top_level_source_sink_with_hint() {
        // Pre-#54 shape: `source:` and `sink:` at the top level.
        let yaml = r#"
version: 1
source: { type: rest, config: {} }
sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let err = parse_with_extension(yaml, "yaml").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("pipeline"),
            "expected a hint about wrapping in `pipeline:`, got: {msg}"
        );
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
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./x } }
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
pipeline:
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
        assert_eq!(cfg.pipeline.transforms.len(), 2);
        assert_eq!(cfg.pipeline.transforms[0].kind, "snake_case");
        assert_eq!(cfg.pipeline.transforms[1].kind, "flatten");
        assert_eq!(
            cfg.pipeline.transforms[1].config,
            json!({"separator": "__"})
        );
        let state = cfg.pipeline.state.unwrap();
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
pipeline:
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
        assert_eq!(cfg.pipeline.source.config["base_url"], "https://x.example");
        unsafe { std::env::remove_var("FAUCET_CFG_URL") };
    }

    #[test]
    fn from_path_leaves_id_path_tokens_unresolved_at_load_time() {
        // `${users.id}` must survive load-time interpolation so the matrix
        // expander / record-time resolver can handle it later.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("pipeline.yaml");
        std::fs::write(
            &path,
            r#"
version: 1
pipeline:
  source: { type: rest, config: { path: "/v1/users/${users.id}/posts" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#,
        )
        .unwrap();
        let cfg = PipelineConfig::from_path(&path).unwrap();
        assert_eq!(
            cfg.pipeline.source.config["path"],
            "/v1/users/${users.id}/posts"
        );
    }
}
