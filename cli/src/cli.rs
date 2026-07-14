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
    /// Replay a bounded historical window of a pipeline: chunk --from/--to
    /// into window units, run them with bounded parallelism, and record
    /// durable, resumable progress. Exits non-zero if any unit fails.
    Backfill(BackfillArgs),
    /// Bulk-snapshot a database table, then stream CDC from a position captured
    /// before the snapshot (a true mirror with `write_mode: upsert`).
    /// Long-running when `replication.continuous` is true (Ctrl-C / SIGTERM to stop).
    Replicate(ReplicateArgs),
    /// Connect to a config's source, enumerate the datasets behind it
    /// (tables / collections / indices / prefixes), and emit a ready-to-run
    /// config with one matrix row per dataset.
    Discover(DiscoverArgs),
    /// Parse + validate a pipeline config without running it.
    Validate(ValidateArgs),
    /// Print the JSON Schema for a specific connector.
    Schema(SchemaArgs),
    /// List every compiled-in source, sink, and transform with a one-line
    /// description (`--available` lists the whole connector registry instead).
    List(ListArgs),
    /// Search the connector registry index for connectors by name / keyword.
    Search(SearchArgs),
    /// Show how to install or enable a connector from the registry index
    /// (prints the recipe; never executes anything).
    Install(InstallArgs),
    /// Run only the source side and print records to stdout (uses the stdout sink).
    Preview(PreviewArgs),
    /// Read-only preview of what a config would do: resolved pipeline, inferred
    /// output schema, sink schema delta, lineage, and target sinks — zero writes.
    Plan(PlanArgs),
    /// Watch a config and re-run a sample offline on every save, printing a
    /// live diff of the output. Requires the `cli-dev` build feature.
    #[cfg(feature = "cli-dev")]
    Dev(DevArgs),
    /// Scaffold a starter `pipeline.yaml` to disk.
    Init(InitArgs),
    /// Scaffold a new artifact — currently a third-party connector crate.
    New(NewArgs),
    /// Probe every connector in a config (auth / network / permissions) and
    /// print a green/red checklist. Exits non-zero if any probe fails.
    Doctor(DoctorArgs),
    /// Run fixture-based offline pipeline tests from one or more spec files.
    /// No real source or sink is touched. Exits non-zero if any case fails.
    Test(TestArgs),
    /// Inspect, replay, or discard dead-letter-queue envelopes written by a
    /// pipeline's `dlq:` sink.
    Dlq(DlqArgs),
    /// Validate a config's `contract:` block and print a summary, or export
    /// it in a machine-readable format (`--export`).
    #[cfg(feature = "contract")]
    Contract(ContractArgs),
    /// Validate a config's `masking:` block and print which rules apply to
    /// each destination sink.
    #[cfg(feature = "masking")]
    Masking(MaskingArgs),
    /// Run a pipeline on a cron schedule (long-running; Ctrl-C / SIGTERM to stop).
    #[cfg(feature = "schedule")]
    Schedule(ScheduleArgs),
    /// Run a long-running HTTP control plane (submit / poll / cancel pipeline runs).
    #[cfg(feature = "serve")]
    Serve(ServeArgs),
    /// Send a synthetic notification through a config's `notifications:` rules
    /// to validate channel setup end-to-end (no pipeline runs).
    #[cfg(feature = "notify")]
    Notify(NotifyArgs),
    /// Browse the Data Movement Catalog accumulated by a config's `catalog:`
    /// store — datasets, schema timelines, volume/freshness, lineage.
    #[cfg(feature = "catalog")]
    Catalog(CatalogArgs),
}

/// `faucet catalog` arguments.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogArgs {
    #[command(subcommand)]
    pub command: CatalogCommand,
}

/// `faucet catalog` subcommands.
#[cfg(feature = "catalog")]
#[derive(Debug, Subcommand)]
pub enum CatalogCommand {
    /// List every catalogued dataset (newest activity first).
    Datasets(CatalogDatasetsArgs),
    /// Show one dataset's detail: schema timeline, volume points, edges.
    Show(CatalogShowArgs),
    /// Print the dataset lineage graph (optionally rooted at a dataset).
    Lineage(CatalogLineageArgs),
}

/// Shared config-loading flags for the `faucet catalog` subcommands.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogConfigArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `catalog:` block naming the store. If omitted, auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    #[arg(long)]
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    /// Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Emit machine-readable JSON instead of the human summary.
    #[arg(long)]
    pub json: bool,
}

/// `faucet catalog datasets` arguments.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogDatasetsArgs {
    #[command(flatten)]
    pub common: CatalogConfigArgs,
    /// Only datasets of this connector kind (e.g. `postgres`, `csv`).
    #[arg(long)]
    pub kind: Option<String>,
    /// Case-insensitive substring match on the dataset URI.
    #[arg(long)]
    pub q: Option<String>,
    /// Max datasets to list.
    #[arg(long, default_value_t = 100)]
    pub limit: usize,
}

/// `faucet catalog show <id>` arguments.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogShowArgs {
    /// Dataset id (from `faucet catalog datasets`), or a unique prefix of one.
    pub id: String,
    #[command(flatten)]
    pub common: CatalogConfigArgs,
}

/// `faucet catalog lineage` arguments.
#[cfg(feature = "catalog")]
#[derive(Debug, Parser)]
pub struct CatalogLineageArgs {
    #[command(flatten)]
    pub common: CatalogConfigArgs,
    /// Dataset id to root the graph at (whole graph when omitted).
    #[arg(long)]
    pub root: Option<String>,
    /// BFS hop bound around --root.
    #[arg(long, default_value_t = 5)]
    pub depth: u32,
}

/// `faucet notify test` arguments.
#[cfg(feature = "notify")]
#[derive(Debug, Parser)]
pub struct NotifyArgs {
    #[command(subcommand)]
    pub command: NotifyCommand,
}

/// `faucet notify` subcommands.
#[cfg(feature = "notify")]
#[derive(Debug, Subcommand)]
pub enum NotifyCommand {
    /// Fire one synthetic event at every matching rule in the config.
    Test(NotifyTestArgs),
}

/// `faucet notify test <config>` arguments.
#[cfg(feature = "notify")]
#[derive(Debug, Parser)]
pub struct NotifyTestArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `notifications:` block. If omitted, auto-discover in cwd.
    pub config: Option<PathBuf>,
    /// Which event to synthesize (defaults to `run_failure`).
    #[arg(long, default_value = "run_failure")]
    pub event: String,
    /// Path to a `.env` file for `${env:VAR}` interpolation.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Disable `.env` auto-discovery.
    #[arg(long)]
    pub no_env_file: bool,
}

/// `faucet test` arguments.
#[derive(Debug, Parser)]
pub struct TestArgs {
    /// One or more test-spec files (`.yaml`, `.yml`, or `.json`), e.g.
    /// `faucet test tests/*.yaml`.
    #[arg(required = true)]
    pub specs: Vec<PathBuf>,
    /// Run only cases whose name contains this substring.
    #[arg(long)]
    pub filter: Option<String>,
    /// Emit a machine-readable JSON report instead of the human checklist.
    #[arg(long)]
    pub json: bool,
    /// Default `${now.*}` clock for cases without their own `clock:` field
    /// (RFC3339 like `2026-01-31T00:00:00Z`, or a date `2026-01-31`).
    /// Defaults to process start (UTC).
    #[arg(long)]
    pub clock: Option<String>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation in
    /// referenced pipeline configs. Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from each referenced config's `profiles:` block.
    /// Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Resolve `${vault:…}` / `${aws-sm:…}` / … secret directives in
    /// referenced configs (requires network + credentials). By default tests
    /// load configs offline and leave secret directives unresolved — safe
    /// because the real source/sink configs holding them are never used.
    #[arg(long)]
    pub resolve_secrets: bool,
}

/// `faucet dlq` arguments.
#[derive(Debug, Parser)]
pub struct DlqArgs {
    #[command(subcommand)]
    pub command: DlqCommand,
}

/// `faucet dlq` subcommands.
#[derive(Debug, Subcommand)]
pub enum DlqCommand {
    /// Read a DLQ location back and print a per-reason / per-error-kind
    /// breakdown plus a sample of quarantined records.
    Inspect(DlqInspectArgs),
    /// Re-feed quarantined records through a pipeline config (transforms →
    /// quality → contract → sink). Rows that fail again land in a *fresh* DLQ.
    Replay(DlqReplayArgs),
    /// Remove processed envelopes from a DLQ location (archive by default,
    /// or `--delete`), filtered by reason and/or age.
    Discard(DlqDiscardArgs),
}

/// `faucet dlq inspect <location>` arguments.
#[derive(Debug, Parser)]
pub struct DlqInspectArgs {
    /// DLQ location: a `.jsonl` file, a directory of `*.jsonl` files, or a glob.
    pub location: String,
    /// Only include envelopes with this DLQ reason (`partial` / `dlq_all` /
    /// `quality` / `schema_drift` / `contract`).
    #[arg(long)]
    pub reason: Option<String>,
    /// Number of sample records to show. Default: 5.
    #[arg(long, default_value_t = 5)]
    pub limit: usize,
    /// Key for a DLQ sealed at rest by the jsonl sink's `encryption` block.
    /// Repeat the flag to also try older (rotated) keys. Requires a build
    /// with the `encryption` feature.
    #[arg(long = "encryption-key")]
    pub encryption_key: Vec<String>,
    /// Emit a machine-readable JSON summary instead of the human report.
    #[arg(long)]
    pub json: bool,
}

/// `faucet dlq replay <config> --from <location>` arguments.
#[derive(Debug, Parser)]
pub struct DlqReplayArgs {
    /// Path to the pipeline config whose sink / transforms / quality / contract
    /// the replayed records flow through. If omitted, auto-discover in cwd.
    pub config: Option<PathBuf>,
    /// DLQ location to replay from: a `.jsonl` file, a directory, or a glob.
    #[arg(long)]
    pub from: String,
    /// Only replay envelopes with this DLQ reason.
    #[arg(long)]
    pub reason: Option<String>,
    /// Where replayed rows that fail *again* are quarantined. Defaults to a
    /// `replay-failed.jsonl` sibling of the source (never the source itself).
    #[arg(long)]
    pub failed_dlq: Option<String>,
    /// Which root row of the config to replay through. Defaults to the first root.
    #[arg(long)]
    pub row: Option<String>,
    /// Report what would be replayed without writing to the sink.
    #[arg(long)]
    pub dry_run: bool,
    /// Key for a DLQ sealed at rest by the jsonl sink's `encryption` block.
    /// Repeat the flag to also try older (rotated) keys. Requires a build
    /// with the `encryption` feature.
    #[arg(long = "encryption-key")]
    pub encryption_key: Vec<String>,
    /// (Replay picks up the config's own dlq `encryption` block automatically
    /// when no key is passed.)
    /// Emit a machine-readable JSON result instead of the human summary.
    #[arg(long)]
    pub json: bool,
    /// Path to a `.env` file for `${env:VAR}` interpolation in the config.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet dlq discard <location>` arguments.
#[derive(Debug, Parser)]
pub struct DlqDiscardArgs {
    /// DLQ location: a `.jsonl` file, a directory of `*.jsonl` files, or a glob.
    pub location: String,
    /// Only discard envelopes with this DLQ reason.
    #[arg(long)]
    pub reason: Option<String>,
    /// Only discard envelopes older than this: an RFC3339 timestamp
    /// (`2026-06-01T00:00:00Z`) or a relative age (`7d`, `24h`, `30m`).
    #[arg(long)]
    pub before: Option<String>,
    /// Permanently delete matching envelopes instead of archiving them to a
    /// `<file>.archived.jsonl` sibling.
    #[arg(long)]
    pub delete: bool,
    /// Key for a DLQ sealed at rest by the jsonl sink's `encryption` block.
    /// Repeat the flag to also try older (rotated) keys. Requires a build
    /// with the `encryption` feature.
    #[arg(long = "encryption-key")]
    pub encryption_key: Vec<String>,
    /// Emit a machine-readable JSON result instead of the human summary.
    #[arg(long)]
    pub json: bool,
}

/// `faucet doctor` arguments.
#[derive(Debug, Parser)]
pub struct DoctorArgs {
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
    /// Per-probe timeout in seconds.
    #[arg(long, default_value_t = 10)]
    pub timeout_secs: u64,
    /// Emit machine-readable JSON instead of the human checklist.
    #[arg(long)]
    pub json: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet contract` arguments.
#[cfg(feature = "contract")]
#[derive(Debug, Parser)]
pub struct ContractArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `pipeline.contract:` block. If omitted, auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Export the contract in a machine-readable format instead of the
    /// human summary: the canonical contract JSON, a standalone JSON Schema,
    /// or an OpenLineage schema facet.
    #[arg(long, value_enum)]
    pub export: Option<ContractExportFormat>,
}

/// Arguments for `faucet masking`.
#[cfg(feature = "masking")]
#[derive(Debug, Parser)]
pub struct MaskingArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `pipeline.masking:` block. If omitted, auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// Export format for `faucet contract --export`.
#[cfg(feature = "contract")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ContractExportFormat {
    /// The canonical contract document as JSON.
    Contract,
    /// A standalone JSON Schema (draft 2020-12) for the promised records.
    JsonSchema,
    /// An OpenLineage `SchemaDatasetFacet` JSON document.
    Openlineage,
}

/// `faucet schedule` arguments.
#[cfg(feature = "schedule")]
#[derive(Debug, Parser)]
pub struct ScheduleArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a `schedule:`
    /// block. If omitted, auto-discover `faucet.yaml` / `.yml` / `.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Run exactly one pipeline run immediately, then exit (ignores cron timing).
    /// Useful for platform-driven invocation (k8s CronJob / systemd OnCalendar).
    #[arg(long)]
    pub once: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet serve` arguments.
#[cfg(feature = "serve")]
#[derive(Debug, Clone, Parser)]
pub struct ServeArgs {
    /// Bind address. Defaults to loopback; set 0.0.0.0:PORT to expose externally.
    #[arg(long, env = "FAUCET_SERVE_LISTEN", default_value = "127.0.0.1:8080")]
    pub listen: String,
    /// Bearer token required on /v1/* requests. Prefer the env var (avoids `ps` leakage).
    #[arg(long, env = "FAUCET_SERVE_AUTH_TOKEN", conflicts_with = "no_auth")]
    pub auth_token: Option<String>,
    /// Explicitly disable authentication. Required if no token is set, so an
    /// unauthenticated server is never accidental.
    #[arg(long)]
    pub no_auth: bool,
    /// Path to an RBAC auth config (YAML/JSON) defining principals — each a
    /// `{ name, token, role }` where role is `viewer` / `operator` / `admin`.
    /// Enables role-based access control + an audit log. Mutually exclusive with
    /// `--auth-token` / `--no-auth`.
    #[arg(long, conflicts_with_all = ["auth_token", "no_auth"])]
    pub auth_config: Option<std::path::PathBuf>,
    /// Max pipeline runs executing at once. Default: min(16, cpu count).
    #[arg(long)]
    pub max_concurrent_runs: Option<usize>,
    /// Max queued (not-yet-running) runs before POST /v1/runs returns 429.
    /// Default: 8 × max-concurrent-runs.
    #[arg(long)]
    pub max_queued_runs: Option<usize>,
    /// Workspace-default config merged under every submitted run.
    #[arg(long)]
    pub default_config: Option<std::path::PathBuf>,
    /// Run-history backend URL: omitted = in-memory; postgres://… ; sqlite:… .
    #[arg(long)]
    pub history: Option<String>,
    /// CORS allow-list origin (repeatable). Omitted = CORS disabled.
    #[arg(long)]
    pub cors_origin: Vec<String>,
    /// Max POST /v1/runs body size in bytes (413 on exceed).
    #[arg(long, default_value_t = 1_048_576)]
    pub body_limit_bytes: usize,
    /// SIGTERM/SIGINT drain window in seconds.
    #[arg(long, default_value_t = 60)]
    pub shutdown_grace_secs: u64,
    /// Retain terminal run records this long (seconds).
    #[arg(long, default_value_t = 604_800)]
    pub retain_terminal_runs_secs: u64,
    /// Idempotency-key replay window (seconds).
    #[arg(long, default_value_t = 86_400)]
    pub idempotency_retention_secs: u64,
    /// Run-ownership lease TTL in seconds (multi-instance orphan fencing). A run
    /// is owned by the instance executing it and its lease is heartbeated at
    /// ~⅓ of this interval; only a run whose lease has expired (owner presumed
    /// dead) is recovered as failed. Make this comfortably larger than expected
    /// GC/IO stalls so a healthy-but-slow instance is never falsely reclaimed.
    /// Only relevant with a persistent (postgres/sqlite) history backend.
    #[arg(long, default_value_t = 30)]
    pub lease_ttl_secs: u64,
    /// Per-probe timeout for `doctor_first` preflight (seconds).
    #[arg(long, default_value_t = 10)]
    pub probe_timeout_secs: u64,
    /// Path to a `.env` file loaded for the server's own startup interpolation.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<std::path::PathBuf>,
    /// Skip auto-loading `.env` from cwd at startup.
    #[arg(long)]
    pub no_env_file: bool,
    /// Disable serving the embedded web console (only meaningful in a build that
    /// includes the `serve-ui` feature; the API is unaffected).
    #[arg(long)]
    pub no_ui: bool,
    /// Enable clustered execution: run a claim loop that pulls Pending runs from
    /// the shared history DB so N instances pull-balance and fail over. Requires
    /// a postgres/sqlite --history backend.
    #[arg(long)]
    pub cluster: bool,
    /// Claim-loop poll interval (seconds) in cluster mode. Also the
    /// cross-instance cancel-propagation lag. Must be > 0.
    #[arg(long, default_value_t = 2)]
    pub cluster_poll_secs: u64,
    /// Max failover re-runs of an orphaned run before it is marked Failed
    /// (poison). Must be > 0.
    #[arg(long, default_value_t = 3)]
    pub cluster_max_attempts: u32,
    /// Path to a triggers file (YAML/JSON) defining event-driven pipeline
    /// triggers (object-arrival / webhook / queue-depth). Requires a build with
    /// the `triggers` feature. See `faucet schema triggers`.
    #[arg(long)]
    pub triggers: Option<std::path::PathBuf>,
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
    /// Override the `${now.*}` interpolation clock (RFC3339 like
    /// `2026-01-31T00:00:00Z`, or a date `2026-01-31`). Default: process start (UTC).
    /// Use for backfills.
    #[arg(long)]
    pub clock: Option<String>,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    /// Not applicable in `--from-env` mode (no config file to compose).
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Show a live full-screen terminal UI (per-invocation throughput, errors,
    /// DLQ counts, bookmark age) while the pipeline runs. Requires a binary
    /// built with the `cli-tui` feature and a real terminal on stdout —
    /// on a non-TTY (CI, pipes) the run proceeds normally with a notice.
    /// Press `q` to cancel cooperatively (in-flight work flushes at the next
    /// page boundary).
    #[arg(long)]
    pub tui: bool,
}

/// `faucet backfill` arguments.
#[derive(Debug, Parser)]
pub struct BackfillArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config. If omitted,
    /// auto-discover `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Window start (inclusive): RFC3339 (`2026-06-01T00:00:00Z`) or a date
    /// (`2026-06-01`, midnight in --timezone). Requires --to.
    #[arg(long, requires = "to", conflicts_with = "from_bookmark")]
    pub from: Option<String>,
    /// Window end (exclusive): RFC3339 or a date.
    #[arg(long, requires = "from", conflicts_with = "from_bookmark")]
    pub to: Option<String>,
    /// Chunk the range into windows of this duration (`45s`, `30m`, `6h`,
    /// `1d`, `1w`) so each chunk is an independent, resumable unit. Defaults
    /// to the config's `backfill.window`; omitted = one unit for the whole
    /// range.
    #[arg(long)]
    pub window: Option<String>,
    /// Replay from this explicit bookmark value instead of a wall-clock
    /// range (seeded into the backfill's scoped state key; the source's own
    /// incremental logic reads forward from it). JSON or a bare string.
    #[arg(long)]
    pub from_bookmark: Option<String>,
    /// Upper bookmark bound: records whose --bookmark-field orders after
    /// this value are dropped before the sink.
    #[arg(long, requires_all = ["from_bookmark", "bookmark_field"])]
    pub to_bookmark: Option<String>,
    /// Record field the --to-bookmark bound applies to.
    #[arg(long)]
    pub bookmark_field: Option<String>,
    /// Max concurrently-running window units. Defaults to the config's
    /// `backfill.concurrency`, else 1 (sequential).
    #[arg(long)]
    pub concurrency: Option<usize>,
    /// IANA timezone for date boundaries and `${now.*}` rendering. Defaults
    /// to the config's `backfill.timezone`, else UTC.
    #[arg(long)]
    pub timezone: Option<String>,
    /// Root row of the config to backfill. Defaults to the only root.
    #[arg(long)]
    pub row: Option<String>,
    /// Redirect writes to this named sink template under `pipeline.sinks`
    /// (backfill into a staging table first).
    #[arg(long)]
    pub into: Option<String>,
    /// Print the planned units without running anything.
    #[arg(long)]
    pub dry_run: bool,
    /// Continue a previously-interrupted backfill of the same range: skip
    /// units already done, re-run failed and pending ones.
    #[arg(long, conflicts_with = "restart")]
    pub resume: bool,
    /// Discard a previous progress marker for this range and start over.
    #[arg(long)]
    pub restart: bool,
    /// Emit a machine-readable JSON report instead of the human summary.
    #[arg(long)]
    pub json: bool,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    /// Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet replicate` arguments.
#[derive(Debug, Parser)]
pub struct ReplicateArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config with a
    /// `replication:` block. If omitted, auto-discover
    /// `faucet.yaml` / `.yml` / `.json` in cwd.
    pub config: Option<PathBuf>,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet discover` arguments.
#[derive(Debug, Parser)]
pub struct DiscoverArgs {
    /// Path to a `.yaml`, `.yml`, or `.json` pipeline config whose source
    /// points at the system to introspect. If omitted, auto-discover
    /// `faucet.yaml` / `faucet.yml` / `faucet.json` in cwd.
    pub config: Option<PathBuf>,
    /// Which source template to introspect (an entry under `pipeline.sources`).
    /// Defaults to `default` (the legacy singular `pipeline.source`).
    #[arg(long)]
    pub source: Option<String>,
    /// Only include datasets whose name matches this `*`-wildcard pattern
    /// (repeatable; no patterns = include everything).
    #[arg(long)]
    pub include: Vec<String>,
    /// Exclude datasets whose name matches this `*`-wildcard pattern
    /// (repeatable; applied after --include).
    #[arg(long)]
    pub exclude: Vec<String>,
    /// Write the generated config to this file instead of stdout.
    #[arg(long, short = 'o')]
    pub output: Option<PathBuf>,
    /// Overwrite the --output file if it already exists.
    #[arg(long)]
    pub force: bool,
    /// Emit the discovered datasets as machine-readable JSON instead of a
    /// generated config.
    #[arg(long)]
    pub json: bool,
    /// Path to a `.env` file to load for `${env:VAR}` interpolation.
    /// Defaults to `.env` in cwd if present.
    #[arg(long, conflicts_with = "no_env_file")]
    pub env_file: Option<PathBuf>,
    /// Skip auto-loading `.env` from cwd.
    #[arg(long)]
    pub no_env_file: bool,
    /// Select a named overlay from the config's `profiles:` block.
    /// Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
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
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
    /// Print the fully-composed config (after extends/!include/profile, before
    /// `${...}` interpolation) and exit. For debugging composition precedence.
    /// `--no-secrets` is redundant here (no interpolation or secret fetch occurs).
    #[arg(long)]
    pub show_composed: bool,
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
    /// Composed JSON Schema for the **entire** `faucet.yaml` / `faucet.json`
    /// config document (top-level grammar + per-connector `type` discrimination).
    /// Point an editor at it with a `# yaml-language-server: $schema=…` header.
    Config,
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
    /// JSON Schema for the `replication:` (snapshot→CDC) block.
    Replication,
    /// JSON Schema for the `backfill:` (window replay defaults) block.
    Backfill,
    /// JSON Schema for the top-level `execution:` block.
    Execution,
    /// JSON Schema for the top-level `resilience:` block.
    Resilience,
    /// JSON Schema for the top-level `sla:` (freshness/volume SLA) block.
    Sla,
    /// JSON Schema for the `quality:` block.
    #[cfg(feature = "quality")]
    Quality,
    /// JSON Schema for the `contract:` block.
    #[cfg(feature = "contract")]
    Contract,
    /// JSON Schema for the `masking:` (PII masking) block.
    #[cfg(feature = "masking")]
    Masking,
    /// JSON Schema for the `faucet test` spec file.
    Test,
    /// Grammar reference for secrets-manager interpolation directives.
    Secrets,
    /// JSON Schema for the `schedule:` block.
    #[cfg(feature = "schedule")]
    Schedule,
    /// JSON Schema for the `lineage:` (OpenLineage) block.
    #[cfg(feature = "lineage")]
    Lineage,
    /// JSON Schema for the `--triggers` file (event-driven pipeline triggers).
    #[cfg(feature = "triggers")]
    Triggers,
    /// JSON Schema for the `notifications:` (incident-routing) block.
    #[cfg(feature = "notify")]
    Notifications,
    /// JSON Schema for the `catalog:` (Data Movement Catalog store) block.
    #[cfg(feature = "catalog")]
    Catalog,
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
    /// Select a named overlay from the config's `profiles:` block and deep-merge
    /// it over the composed base. Overrides the `FAUCET_PROFILE` env var.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
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
    /// (singer only) Run `<executable> --discover` to fetch the tap's catalog,
    /// write it next to the output, and scaffold the config with the discovered
    /// streams listed. Requires `--source singer` and `--executable`.
    #[arg(long)]
    pub discover: bool,
    /// (singer only) The Singer tap executable to discover with (used by
    /// `--discover`), e.g. `tap-github` or `/opt/taps/tap-csv`.
    #[arg(long)]
    pub executable: Option<String>,
    /// (singer only) The target stream to emit. When given with `--discover`,
    /// the written catalog marks this stream — and any inferable parent
    /// streams (e.g. a parent-keyed tap's parent) — `selected`, and the
    /// scaffolded config's `stream:` is set to it. Most DB / SDK taps sync
    /// nothing unless a stream is selected in the catalog.
    #[arg(long)]
    pub stream: Option<String>,
}

/// `faucet plan` arguments.
#[derive(Debug, Parser)]
pub struct PlanArgs {
    /// Path to a `.yaml`/`.yml`/`.json` config (auto-discovered if omitted).
    pub config: Option<PathBuf>,
    /// Which row to plan (default: the first root row).
    #[arg(long)]
    pub row: Option<String>,
    /// Offline sample of input records (`.jsonl` or a `.json` array) to preview
    /// the output schema, volume, and sink delta through — no source is touched.
    #[arg(long)]
    pub sample: Option<PathBuf>,
    /// Pull a capped, read-only sample from the real source instead of a
    /// fixture (bounded by `--limit`; no bookmark is advanced).
    #[arg(long)]
    pub live: bool,
    /// Cap for `--live` sampling.
    #[arg(long, default_value_t = 10)]
    pub limit: usize,
    /// Emit the plan as JSON.
    #[arg(long)]
    pub json: bool,
    /// Resolve secrets-manager directives (needs network/credentials). Off by
    /// default so `plan` works offline like `faucet test`.
    #[arg(long)]
    pub resolve_secrets: bool,
    /// Select a `profiles:` overlay.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet dev` arguments.
#[derive(Debug, Parser)]
pub struct DevArgs {
    /// Path to the `.yaml`/`.yml`/`.json` config to watch.
    pub config: PathBuf,
    /// Which row to run (default: the first root row).
    #[arg(long)]
    pub row: Option<String>,
    /// Offline sample of input records (`.jsonl` or `.json` array). Required
    /// for the offline loop.
    #[arg(long)]
    pub sample: Option<PathBuf>,
    /// (reserved) pull a capped read-only sample from the real source.
    #[arg(long)]
    pub live: bool,
    /// Cap for `--live` sampling.
    #[arg(long, default_value_t = 10)]
    pub limit: usize,
    /// Run once and exit instead of watching (also the non-TTY fallback).
    #[arg(long)]
    pub once: bool,
    /// Debounce window between re-runs, in milliseconds.
    #[arg(long, default_value_t = 300)]
    pub debounce_ms: u64,
    /// Select a `profiles:` overlay.
    #[arg(long, env = "FAUCET_PROFILE")]
    pub profile: Option<String>,
}

/// `faucet list` arguments.
#[derive(Debug, Parser)]
pub struct ListArgs {
    /// List every connector in the registry index (not just the compiled-in
    /// ones), marking which are already in this binary.
    #[arg(long)]
    pub available: bool,
    /// Read a custom registry index instead of the built-in one.
    #[arg(long)]
    pub index: Option<PathBuf>,
}

/// `faucet search` arguments.
#[derive(Debug, Parser)]
pub struct SearchArgs {
    /// Term to match against connector name / description / keywords / crate.
    pub term: String,
    /// Read a custom registry index instead of the built-in one.
    #[arg(long)]
    pub index: Option<PathBuf>,
    /// Emit matches as JSON.
    #[arg(long)]
    pub json: bool,
}

/// `faucet install` arguments.
#[derive(Debug, Parser)]
pub struct InstallArgs {
    /// Connector system name (e.g. `kafka`).
    pub name: String,
    /// Disambiguate when a name exists as both a source and a sink.
    #[arg(long)]
    pub kind: Option<String>,
    /// Read a custom registry index instead of the built-in one.
    #[arg(long)]
    pub index: Option<PathBuf>,
}

/// `faucet new` arguments.
#[derive(Debug, Parser)]
pub struct NewArgs {
    #[command(subcommand)]
    pub target: NewTarget,
}

/// What `faucet new` scaffolds.
#[derive(Debug, Subcommand)]
pub enum NewTarget {
    /// Scaffold a ready-to-build `faucet-source-<name>` / `faucet-sink-<name>`
    /// connector crate following every repo convention.
    Connector(NewConnectorArgs),
}

/// `faucet new connector` arguments.
#[derive(Debug, Parser)]
pub struct NewConnectorArgs {
    /// Connector system name (lowercase, e.g. `acme` or `acme-widgets`). Becomes
    /// the crate name `faucet-<kind>-<name>` and the YAML `type:` value.
    pub name: String,
    /// Whether to scaffold a `source` or a `sink`.
    #[arg(long)]
    pub kind: String,
    /// Also scaffold a `faucet-common-<name>` crate for config shared between a
    /// source/sink pair.
    #[arg(long)]
    pub common: bool,
    /// Directory to write the new crate(s) into. Defaults to the current dir.
    #[arg(long, short = 'o', default_value = ".")]
    pub output: PathBuf,
    /// Overwrite any existing files.
    #[arg(long)]
    pub force: bool,
}
