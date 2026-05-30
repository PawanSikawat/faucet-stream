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
pub mod config;
pub mod env_config;
pub mod env_loader;
pub mod error;
pub mod executor;
pub mod expand;
pub mod init_template;
pub mod interpolate;
pub mod merge;
pub mod registry;
pub mod secrets;
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
    // Parse through the same interpolate → from_text path that the binary uses,
    // but accept a bare string instead of a file path.
    let interpolated = interpolate::interpolate(yaml)?;
    let cfg: config::PipelineConfig =
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
    let pipeline_name = cfg.name.clone().unwrap_or_else(|| "unnamed".to_string());
    let auth = auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
    let nodes = expand::expand(&cfg)?;
    executor::run_expanded(
        nodes,
        executor::ExecuteOptions {
            pipeline_name,
            execution: cfg.execution.clone(),
            dry_run: false,
            limit: None,
            state_path_override: None,
            auth,
        },
    )
    .await
}
