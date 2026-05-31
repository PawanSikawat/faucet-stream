//! Turn a submitted config body into expanded nodes, applying the workspace
//! `--default-config` base. Mirrors `PipelineConfig::from_path_async` but merges
//! a base `Value` and uses `from_value`. All `${env}`/`${file}`/`${secret}` and
//! `${vault:…}`-style directives resolve against the *server's* environment and
//! credentials (the documented privilege surface — spec §13).

use crate::config::PipelineConfig;
use crate::expand::{ExpandedNode, expand};
use crate::serve::error::ServeError;
use serde_json::Value;

/// Wire format of a submitted config body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConfigFormat {
    #[default]
    Yaml,
    Json,
}

/// A loaded submission: the merged/resolved config and its expanded nodes.
#[derive(Debug)]
pub struct LoadedSubmission {
    pub cfg: PipelineConfig,
    pub nodes: Vec<ExpandedNode>,
}

/// Load + merge + expand a submitted config body.
pub async fn load_submission(
    body: &str,
    format: ConfigFormat,
    default_base: Option<&Value>,
) -> Result<LoadedSubmission, ServeError> {
    // 1. ${env}/${file}/${secret} interpolation against the server's env/fs.
    let interpolated =
        crate::interpolate::interpolate(body).map_err(|e| ServeError::BadConfig(e.to_string()))?;

    // 2. Parse to a Value per the declared format.
    let submitted: Value = match format {
        ConfigFormat::Yaml => serde_yaml::from_str(&interpolated)
            .map_err(|e| ServeError::BadConfig(format!("invalid YAML: {e}")))?,
        ConfigFormat::Json => serde_json::from_str(&interpolated)
            .map_err(|e| ServeError::BadConfig(format!("invalid JSON: {e}")))?,
    };

    // 3. Merge onto the workspace default (submitted wins; see merge.rs semantics).
    let merged = match default_base {
        Some(base) => {
            let mut m = base.clone();
            crate::merge::merge_value(&mut m, submitted);
            m
        }
        None => submitted,
    };

    // 4. Version gate + structural-ref resolution.
    let mut cfg = PipelineConfig::from_value(merged).map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;

    // serve runs once per submission; a schedule: block is a category error.
    #[cfg(feature = "schedule")]
    if cfg.schedule.is_some() {
        return Err(ServeError::BadConfig(
            "submitted config contains a `schedule:` block — serve runs once per \
             submission; use `faucet schedule` for cron scheduling"
                .into(),
        ));
    }

    // 5. Secret-manager directives (${vault:…} etc.) with the server's creds.
    crate::secrets::resolve_secrets(&mut cfg)
        .await
        .map_err(|e| ServeError::BadConfig(e.to_string()))?;

    // 6. Expand the matrix.
    let nodes = expand(&cfg).map_err(|e| ServeError::Unprocessable {
        message: e.to_string(),
        details: None,
    })?;

    Ok(LoadedSubmission { cfg, nodes })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn base() -> Value {
        json!({
            "version": 1,
            "pipeline": {
                "source": { "type": "csv", "config": { "path": "DEFAULT.csv" } },
                "sink": { "type": "jsonl", "config": { "path": "out.jsonl" } }
            }
        })
    }

    #[tokio::test]
    async fn submitted_overrides_default() {
        let body = r#"{ "pipeline": { "source": { "config": { "path": "OVERRIDE.csv" } } } }"#;
        let loaded = load_submission(body, ConfigFormat::Json, Some(&base()))
            .await
            .unwrap();
        // The override wins; the default sink survives the merge.
        let node = &loaded.nodes[0];
        assert_eq!(node.source.config["path"], "OVERRIDE.csv");
        assert_eq!(node.sink.config["path"], "out.jsonl");
    }

    #[tokio::test]
    async fn missing_version_without_base_is_unprocessable() {
        // version defaults to 1 via serde, so this exercises the expand/validation
        // failure path (pipeline with no source/sink). Accept either layer's error.
        let body = r#"{ "pipeline": {} }"#;
        let err = load_submission(body, ConfigFormat::Json, None)
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            ServeError::Unprocessable { .. } | ServeError::BadConfig(_)
        ));
    }

    #[cfg(feature = "schedule")]
    #[tokio::test]
    async fn schedule_block_is_rejected() {
        let body = r#"
version: 1
pipeline:
  source: { type: csv, config: { path: x.csv } }
  sink: { type: jsonl, config: { path: out.jsonl } }
schedule:
  cron: "0 * * * *"
  timezone: UTC
"#;
        let err = load_submission(body, ConfigFormat::Yaml, None)
            .await
            .unwrap_err();
        match err {
            ServeError::BadConfig(m) => assert!(m.contains("schedule:")),
            other => panic!("expected BadConfig, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn invalid_yaml_is_bad_config() {
        let err = load_submission("{[bad", ConfigFormat::Yaml, None)
            .await
            .unwrap_err();
        assert!(matches!(err, ServeError::BadConfig(_)));
    }
}
