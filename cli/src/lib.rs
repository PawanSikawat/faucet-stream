#![cfg_attr(docsrs, feature(doc_cfg))]

//! # faucet-cli
//!
//! A config-driven runner for [`faucet-stream`](https://docs.rs/faucet-stream)
//! pipelines.  Define a source, optional transforms, a sink, and (optionally)
//! a state store in a YAML or JSON file, then run it with the `faucet` binary —
//! no Rust code required.
//!
//! The library half of this crate exposes the same building blocks the binary
//! uses (config parsing, env interpolation, the connector registry) so that
//! integrations and tests can reuse them.

pub mod auth_catalog;
pub mod cli;
pub mod commands;
pub mod compose;
pub mod config;
pub mod env_config;
pub mod env_loader;
pub mod error;
pub mod executor;
pub mod expand;
pub mod init_template;
pub mod interpolate;
#[cfg(feature = "lineage")]
pub mod lineage_glue;
pub mod merge;
pub mod obs;
pub mod pipeline_test;
pub mod registry;
pub mod replication;
#[cfg(feature = "schedule")]
pub mod schedule;
pub mod secrets;
#[cfg(feature = "serve")]
pub mod serve;
pub mod state;
pub mod transforms;

pub use error::{CliError, CliResult};

/// Convenience entry point for integration tests and custom hosts: parse a
/// YAML config string, expand the matrix, and run all rows.
///
/// This skips the `install_observability` call that [`commands::run::run`]
/// performs — callers can wire their own `metrics` recorder / tracing
/// subscriber before calling this function (or not at all).
pub async fn run_from_yaml_str(yaml: &str) -> CliResult<executor::RunSummary> {
    // Parse first, then resolve ${env}/${file}/${secret} INTO the parsed tree
    // (post-parse) so a resolved value can never alter the document's structure
    // (F43) — mirroring the binary's `from_path` path.
    let mut value: serde_json::Value =
        serde_yaml::from_str(yaml).map_err(|e| CliError::ParseConfig {
            path: std::path::PathBuf::from("<yaml-string>"),
            message: e.to_string(),
        })?;
    interpolate::interpolate_value(&mut value)?;
    let interpolated = serde_yaml::to_string(&value).map_err(|e| CliError::ParseConfig {
        path: std::path::PathBuf::from("<yaml-string>"),
        message: e.to_string(),
    })?;
    let mut cfg: config::PipelineConfig =
        serde_yaml::from_str(&interpolated).map_err(|e| CliError::ParseConfig {
            path: std::path::PathBuf::from("<yaml-string>"),
            message: e.to_string(),
        })?;
    if cfg.version != 1 {
        return Err(CliError::ParseConfig {
            path: std::path::PathBuf::from("<yaml-string>"),
            message: format!(
                "unsupported pipeline version {}, only version 1 is recognised",
                cfg.version
            ),
        });
    }
    crate::secrets::resolve_secrets(&mut cfg).await?;
    let pipeline_name = cfg.name.clone().unwrap_or_else(|| "unnamed".to_string());
    let auth = auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    let resilience = match &cfg.resilience {
        Some(spec) => Some(spec.to_policy()?),
        None => None,
    };
    let nodes = expand::expand(&cfg)?;
    executor::run_expanded(
        nodes,
        executor::ExecuteOptions {
            pipeline_name,
            execution: cfg.execution.clone(),
            dry_run: false,
            limit: None,
            state_path_override: None,
            shard: None,
            auth,
            clock: chrono::Utc::now().fixed_offset(),
            cancel: None,
            resilience,
            #[cfg(feature = "lineage")]
            lineage: None,
            #[cfg(feature = "lineage")]
            lineage_cfg: None,
        },
    )
    .await
}
