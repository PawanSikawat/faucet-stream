//! `faucet run` — load a pipeline config, expand the matrix, execute every
//! invocation under bounded concurrency.

use crate::cli::RunArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::executor::{ExecuteOptions, run_expanded};
use crate::expand::expand;

/// Execute the `run` subcommand.
pub async fn run(args: RunArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let resolved_config_path: Option<std::path::PathBuf> = if args.from_env {
        None
    } else {
        Some(match args.config.as_ref() {
            Some(p) => p.clone(),
            None => {
                crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?
            }
        })
    };

    let cfg = if args.from_env {
        crate::env_config::from_process_env()?
    } else {
        PipelineConfig::from_path(
            resolved_config_path
                .as_ref()
                .expect("YAML mode always resolves a path above"),
        )?
    };
    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        resolved_config_path
            .as_ref()
            .and_then(|p| p.file_stem())
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });

    let nodes = expand(&cfg)?;
    let summary = run_expanded(
        nodes,
        ExecuteOptions {
            pipeline_name: pipeline_name.clone(),
            execution: cfg.execution.clone(),
            dry_run: args.dry_run,
            limit: args.limit,
            state_path_override: args.state_path.clone(),
        },
    )
    .await?;

    let total_written: usize = summary.invocations.iter().map(|i| i.records_written).sum();
    let success = summary
        .invocations
        .iter()
        .filter(|i| i.error.is_none())
        .count();
    let failed = summary.failure_count();

    tracing::info!(
        pipeline = %pipeline_name,
        invocations = summary.invocations.len(),
        succeeded = success,
        failed,
        records_written = total_written,
        "pipeline completed"
    );
    println!(
        "{}: {} invocation{}, {} ok, {} failed, wrote {} record{}",
        pipeline_name,
        summary.invocations.len(),
        if summary.invocations.len() == 1 {
            ""
        } else {
            "s"
        },
        success,
        failed,
        total_written,
        if total_written == 1 { "" } else { "s" }
    );

    if summary.had_failures() {
        return Err(CliError::PipelineHadFailures { count: failed });
    }
    Ok(())
}
