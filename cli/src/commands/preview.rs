//! `faucet preview` — run only the source side of the first root row and emit
//! the first N records to stdout as JSON Lines.
//!
//! Child rows can't be previewed in isolation in v1: they need parent records
//! to resolve `${parent.path}` tokens. Preview the parent first, then point
//! the child at a `${file:...}` fixture if you need to drive it standalone.

use crate::cli::PreviewArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::expand::{NodeRole, expand};
use crate::registry::build_source;
use crate::transforms::compile_transforms;
use faucet_core::transform::{apply_all, compile as compile_transform};

#[cfg(feature = "sink-stdout")]
use faucet_core::{Pipeline, Sink};

/// Execute the `preview` subcommand.
#[cfg(feature = "sink-stdout")]
pub async fn run(args: PreviewArgs) -> CliResult<()> {
    use faucet_sink_stdout::{StdoutFormat, StdoutSink, StdoutSinkConfig};
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path(&path)?;
    let nodes = expand(&cfg)?;
    let first_root = nodes
        .iter()
        .find(|n| matches!(n.role, NodeRole::Root))
        .ok_or_else(|| CliError::ParseConfig {
            path: std::path::PathBuf::from("(preview)"),
            message: "no root rows in matrix to preview".to_owned(),
        })?;
    tracing::info!(row = %first_root.id, "previewing first root row");

    let source = build_source(&first_root.source.kind, first_root.source.config.clone()).await?;
    let transforms = compile_transforms(&first_root.transforms)?;
    let records = source.fetch_all().await?;
    let records: Vec<_> = if transforms.is_empty() {
        records
    } else {
        let compiled = transforms
            .iter()
            .map(compile_transform)
            .collect::<Result<Vec<_>, _>>()?;
        records
            .into_iter()
            .map(|r| apply_all(r, &compiled))
            .collect()
    };

    let limited: Vec<_> = records.into_iter().take(args.limit).collect();
    let sink = StdoutSink::new(
        StdoutSinkConfig::new()
            .format(StdoutFormat::JsonLines)
            .flush_per_record(true),
    );
    sink.write_batch(&limited).await?;
    sink.flush().await?;

    let _ = std::marker::PhantomData::<Pipeline<'_, dyn faucet_core::Source, dyn Sink>>;
    Ok(())
}

#[cfg(not(feature = "sink-stdout"))]
pub async fn run(_args: PreviewArgs) -> CliResult<()> {
    Err(CliError::UnknownConnector {
        kind: "sink",
        name: "stdout".into(),
        available: "(preview requires faucet-cli to be built with the 'sink-stdout' feature)"
            .into(),
    })
}
