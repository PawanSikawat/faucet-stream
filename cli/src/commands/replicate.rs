//! `faucet replicate` — load a config with a `replication:` block, validate it,
//! and run the two-phase snapshot→CDC orchestration.

use crate::cli::ReplicateArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::replication::compiled::CompiledReplication;
use crate::replication::{ReplicationOptions, run_replication};

/// Execute the `replicate` subcommand.
pub async fn run(args: ReplicateArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;
    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };

    let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
    let spec = cfg.replication.as_ref().ok_or_else(|| {
        CliError::Config(
            "no `replication:` block in config — use `faucet run` for a one-shot run, or add a \
             `replication:` block (see `faucet schema replication`)"
                .into(),
        )
    })?;
    let compiled = CompiledReplication::compile(spec, &cfg)?;

    crate::obs::install(&cfg)?;

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("pipeline")
            .to_owned()
    });
    let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;

    run_replication(
        &cfg,
        &compiled,
        ReplicationOptions {
            pipeline_name,
            execution: cfg.execution.clone(),
            auth,
            clock: chrono::Utc::now().fixed_offset(),
        },
    )
    .await?;

    println!("replication finished");
    Ok(())
}
