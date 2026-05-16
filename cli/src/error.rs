//! CLI-level error type. Wraps every failure mode the binary surfaces so
//! `main()` can render a single, user-readable line per failure.

use std::path::PathBuf;
use thiserror::Error;

/// Convenience alias used by every CLI module.
pub type CliResult<T> = Result<T, CliError>;

/// Top-level error variants for the `faucet` binary.
#[derive(Debug, Error)]
pub enum CliError {
    /// Failed to read a config file from disk.
    #[error("failed to read config file '{path}': {source}")]
    ReadConfig {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The config file extension is neither `.yaml`/`.yml` nor `.json`.
    #[error(
        "unsupported config extension for '{path}' — use .yaml, .yml, or .json (mixed JSON/YAML in a single file is not allowed)"
    )]
    UnknownExtension { path: PathBuf },

    /// Failed to parse the raw config text after interpolation.
    #[error("failed to parse config '{path}': {message}")]
    ParseConfig { path: PathBuf, message: String },

    /// An `${env:VAR}` reference could not be resolved.
    #[error("missing environment variable '{var}' referenced in config at '{location}'")]
    MissingEnvVar { var: String, location: String },

    /// A `${file:PATH}` reference could not be read.
    #[error("failed to read interpolated file '{}' referenced in config: {source}", path.display())]
    ReadInterpolatedFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// An interpolation directive used an unknown prefix.
    #[error(
        "unsupported interpolation prefix '{prefix}' in '{full}' — expected env, file, or secret"
    )]
    UnknownInterpolationPrefix { prefix: String, full: String },

    /// The named connector is unknown (or its feature flag is disabled in this build).
    #[error("unknown {kind} '{name}'. Available: {available}")]
    UnknownConnector {
        kind: &'static str,
        name: String,
        available: String,
    },

    /// The state-store type referenced in the config is unknown or not compiled in.
    #[error("unknown state store '{name}'. Available: {available}")]
    UnknownStateStore { name: String, available: String },

    /// A transform type referenced in the config is not recognised.
    #[error("unknown transform '{name}'. Available: {available}")]
    UnknownTransform { name: String, available: String },

    /// The transform config block could not be deserialized into the expected shape.
    #[error("invalid transform '{name}': {message}")]
    InvalidTransform { name: String, message: String },

    /// A connector config object failed to deserialize.
    #[error("invalid config for {kind} '{name}': {message}")]
    InvalidConnectorConfig {
        kind: &'static str,
        name: String,
        message: String,
    },

    /// A scaffold target already exists.
    #[error("refusing to overwrite existing file '{path}' — pass --force to overwrite")]
    ScaffoldExists { path: PathBuf },

    /// Pass-through for failures bubbling up from `faucet-core` or a connector.
    #[error(transparent)]
    Faucet(#[from] faucet_core::FaucetError),

    /// Pass-through I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
