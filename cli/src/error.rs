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

    /// The CLI was invoked with `--from-env` but the required selector env var
    /// (`FAUCET_SOURCE` or `FAUCET_SINK`) is unset.
    #[error(
        "missing required environment variable '{var}' — set it before invoking `faucet run --from-env`"
    )]
    MissingEnvSelector { var: String },

    /// `--env-file` was supplied without `--from-env`.
    #[error(
        "the argument '--env-file' cannot be used without '--from-env'\n\nUsage: faucet run --from-env --env-file <ENV_FILE>"
    )]
    EnvFileRequiresFromEnv,

    /// Both a scalar env var and its `_JSON` counterpart were set for the same field.
    #[error(
        "conflicting environment variables for field '{field}': both '{scalar_var}' and '{json_var}' are set — pick one"
    )]
    EnvConflict {
        field: String,
        scalar_var: String,
        json_var: String,
    },

    /// A `*_JSON` env var did not parse as JSON.
    #[error("environment variable '{var}' is not valid JSON: {message}")]
    InvalidEnvJson { var: String, message: String },

    /// `FAUCET_TRANSFORM_<N>` indices are not contiguous starting at 1.
    #[error(
        "transform env vars must be contiguous starting at FAUCET_TRANSFORM_1; index {missing} is missing"
    )]
    TransformIndexGap { missing: u32 },

    /// Pass-through for failures bubbling up from `faucet-core` or a connector.
    #[error(transparent)]
    Faucet(#[from] faucet_core::FaucetError),

    /// Pass-through I/O error.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_env_selector_renders() {
        let e = CliError::MissingEnvSelector {
            var: "FAUCET_SOURCE".to_owned(),
        };
        let msg = e.to_string();
        assert!(msg.contains("FAUCET_SOURCE"));
        assert!(msg.contains("--from-env"));
    }

    #[test]
    fn env_conflict_names_both_vars() {
        let e = CliError::EnvConflict {
            field: "auth".to_owned(),
            scalar_var: "FAUCET_SOURCE_REST_AUTH".to_owned(),
            json_var: "FAUCET_SOURCE_REST_AUTH_JSON".to_owned(),
        };
        let msg = e.to_string();
        assert!(msg.contains("FAUCET_SOURCE_REST_AUTH"));
        assert!(msg.contains("FAUCET_SOURCE_REST_AUTH_JSON"));
    }

    #[test]
    fn invalid_env_json_names_var_and_parse_error() {
        let e = CliError::InvalidEnvJson {
            var: "FAUCET_SOURCE_REST_AUTH_JSON".to_owned(),
            message: "expected value at line 1 column 1".to_owned(),
        };
        let msg = e.to_string();
        assert!(msg.contains("FAUCET_SOURCE_REST_AUTH_JSON"));
        assert!(msg.contains("expected value"));
    }

    #[test]
    fn transform_index_gap_reports_missing_index() {
        let e = CliError::TransformIndexGap { missing: 2 };
        let msg = e.to_string();
        assert!(msg.contains('2'));
        assert!(msg.to_ascii_lowercase().contains("transform"));
    }
}
