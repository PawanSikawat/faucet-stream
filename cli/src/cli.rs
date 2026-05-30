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
    /// List every compiled-in source, sink, and transform with a one-line description.
    List,
    /// Run only the source side and print records to stdout (uses the stdout sink).
    Preview(PreviewArgs),
    /// Scaffold a starter `pipeline.yaml` to disk.
    Init(InitArgs),
}

/// `faucet run` arguments.
#[derive(Debug, Parser)]
pub struct RunArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config.
    /// If omitted (and `--from-env` is not set), auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in the current directory.
    /// Mutually exclusive with `--from-env`.
    #[arg(conflicts_with = "from_env")]
    pub config: Option<PathBuf>,
    /// Build the pipeline entirely from `FAUCET_*` environment variables —
    /// no YAML required. See `cli/README.md` for the variable schema.
    #[arg(long)]
    pub from_env: bool,
    /// Path to a `.env` file to load before reading variables. Works in both
    /// YAML mode (for `${env:VAR}` interpolation) and `--from-env` mode.
    /// When omitted, `.env` in the current directory is auto-loaded if present.
    /// Existing process-env values always win over file-supplied ones.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from the current directory.
    #[arg(long)]
    pub no_env_file: bool,
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
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config. If omitted,
    /// auto-discover `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Validate grammar and structure only — skip fetching from secrets
    /// managers (no network / credentials needed).
    #[arg(long)]
    pub no_secrets: bool,
}

/// `faucet schema` arguments.
#[derive(Debug, Parser)]
pub struct SchemaArgs {
    #[command(subcommand)]
    pub target: SchemaTarget,
}

/// Schema subcommand target — which connector or system component to describe.
#[derive(Debug, Subcommand)]
pub enum SchemaTarget {
    /// JSON Schema for a source connector config.
    Source {
        /// Connector name (e.g. `rest`, `graphql`, `postgres`).
        name: String,
    },
    /// JSON Schema for a sink connector config.
    Sink {
        /// Connector name (e.g. `jsonl`, `bigquery`, `postgres`).
        name: String,
    },
    /// JSON Schema for a transform's inline config.
    Transform {
        /// Transform name (e.g. `flatten`, `keys_case`, `cast`).
        /// Run `faucet list` to see what is compiled in.
        name: String,
    },
    /// JSON Schema for the DLQ (Dead Letter Queue) specification.
    Dlq,
    /// JSON Schema for the `quality:` block.
    #[cfg(feature = "quality")]
    Quality,
    /// Grammar reference for secrets-manager interpolation directives.
    Secrets,
}

/// `faucet preview` arguments.
#[derive(Debug, Parser)]
pub struct PreviewArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config. If omitted,
    /// auto-discover `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Stop after this many records. Default: 10.
    #[arg(long, default_value_t = 10)]
    pub limit: usize,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
}

/// `faucet init` arguments.
#[derive(Debug, Parser)]
pub struct InitArgs {
    /// Name written into the generated file's `name:` field. Defaults to
    /// `my-pipeline` when omitted.
    pub name: Option<String>,
    /// Source connector kind to scaffold (e.g. `rest`, `postgres`, `s3`).
    /// Defaults to `rest`. Run `faucet list` to see what is compiled in.
    #[arg(long)]
    pub source: Option<String>,
    /// Sink connector kind to scaffold (e.g. `jsonl`, `bigquery`).
    /// Defaults to `jsonl`. Run `faucet list` to see what is compiled in.
    #[arg(long)]
    pub sink: Option<String>,
    /// Output file path. Defaults to `pipeline.yaml`.
    #[arg(long, short = 'o', default_value = "pipeline.yaml")]
    pub output: PathBuf,
    /// Overwrite the output file if it already exists.
    #[arg(long)]
    pub force: bool,
    /// Prompt for the source and sink kinds interactively instead of using
    /// `--source` / `--sink`. Requires the `cli-interactive` build feature
    /// and a TTY on stdin; falls back to the arg-driven path otherwise.
    #[arg(long)]
    pub interactive: bool,
    /// Name of the template under which to register the scaffolded source
    /// and sink. The generated config uses `pipeline.sources.<TEMPLATE>` and
    /// `pipeline.sinks.<TEMPLATE>`. Defaults to `default` so a matrix row
    /// without a `ref:` field still resolves through the new schema.
    #[arg(long, default_value = "default")]
    pub template: String,
}
