//! Argument parser shared by `main.rs` and the integration tests.

use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// `faucet` — config-driven runner for faucet-stream pipelines.
#[derive(Debug, Parser)]
#[command(name = "faucet", version, about, long_about = None)]
pub struct Cli {
    /// Override the global log level (also honors `FAUCET_LOG`).
    #[arg(long, global = true, env = "FAUCET_LOG", default_value = "info")]
    pub log_level: String,

    #[command(subcommand)]
    pub command: Command,
}

/// Top-level subcommands.
#[derive(Debug, Subcommand)]
pub enum Command {
    /// Execute a pipeline config end-to-end.
    Run(RunArgs),
    /// Parse + validate a pipeline config without running it.
    Validate(ValidateArgs),
    /// Print the JSON Schema for a specific connector.
    Schema(SchemaArgs),
    /// List every compiled-in source and sink with a one-line description.
    List,
    /// Run only the source side and print records to stdout (uses the stdout sink).
    Preview(PreviewArgs),
    /// Scaffold a starter `pipeline.yaml` to disk.
    Init(InitArgs),
}

/// `faucet run` arguments.
#[derive(Debug, Parser)]
#[command(group(
    clap::ArgGroup::new("source-of-truth")
        .required(true)
        .args(["config", "from_env"]),
))]
pub struct RunArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config.
    /// Mutually exclusive with `--from-env`.
    pub config: Option<PathBuf>,
    /// Build the pipeline entirely from `FAUCET_*` environment variables —
    /// no YAML required. See `cli/README.md` for the variable schema.
    #[arg(long)]
    pub from_env: bool,
    /// Path to a `.env` file to load before reading `FAUCET_*` variables.
    /// Only honoured together with `--from-env`. Existing process-env values win.
    #[arg(long, requires = "from_env")]
    pub env_file: Option<PathBuf>,
    /// Stop after fetching from the source — write nothing to the sink.
    #[arg(long)]
    pub dry_run: bool,
    /// Stop after writing this many records to the sink. Default: unlimited.
    #[arg(long)]
    pub limit: Option<usize>,
    /// Override the state-store directory (file backend only).
    #[arg(long)]
    pub state_path: Option<PathBuf>,
}

/// `faucet validate` arguments.
#[derive(Debug, Parser)]
pub struct ValidateArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config.
    pub config: PathBuf,
}

/// `faucet schema` arguments.
#[derive(Debug, Parser)]
pub struct SchemaArgs {
    /// `source` or `sink`.
    #[arg(value_parser = ["source", "sink"])]
    pub kind: String,
    /// Connector name (e.g. `rest`, `jsonl`, `bigquery`).
    pub name: String,
}

/// `faucet preview` arguments.
#[derive(Debug, Parser)]
pub struct PreviewArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config.
    pub config: PathBuf,
    /// Stop after this many records. Default: 10.
    #[arg(long, default_value_t = 10)]
    pub limit: usize,
}

/// `faucet init` arguments.
#[derive(Debug, Parser)]
pub struct InitArgs {
    /// Name of the pipeline (used in the generated file's `name:` field).
    pub name: String,
    /// Output file path. Defaults to `pipeline.yaml`.
    #[arg(long, default_value = "pipeline.yaml")]
    pub output: PathBuf,
    /// Overwrite the output file if it already exists.
    #[arg(long)]
    pub force: bool,
}
