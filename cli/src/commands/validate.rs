//! `faucet validate` — parse + expand a pipeline config without running.
//!
//! Surfaces every per-row error with the row id, so a config with multiple
//! issues reports them together instead of failing at the first one.

use crate::cli::ValidateArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::expand::{NodeRole, expand};
use crate::registry::{sink_schema, source_schema};
use crate::state::available_state_kinds;
use crate::transforms::available_transforms;

/// Execute the `validate` subcommand.
pub async fn run(args: ValidateArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = if args.no_secrets {
        // Grammar / structure only — never touch the network.
        PipelineConfig::from_path_tolerating_secrets(&path)?
    } else {
        // Real preflight: report each secret reference, then resolve.
        let refs = crate::secrets::scan_path_refs(&path)?;
        let cfg = PipelineConfig::from_path_async(&path).await?;
        for (scheme, reference) in &refs {
            println!("secret: {scheme}:{reference} → resolved");
        }
        cfg
    };
    let nodes = expand(&cfg)?;

    for node in &nodes {
        // Verifying the schema lookup also catches unknown connector kinds.
        source_schema(&node.source.kind)?;
        sink_schema(&node.sink.kind)?;
        for t in &node.transforms {
            if !available_transforms().contains(&t.kind.as_str()) {
                return Err(CliError::UnknownTransform {
                    name: format!("{} (row '{}')", t.kind, node.id),
                    available: available_transforms().join(", "),
                });
            }
        }
        if let Some(state) = &node.state
            && !available_state_kinds().contains(&state.kind.as_str())
        {
            return Err(CliError::UnknownStateStore {
                name: format!("{} (row '{}')", state.kind, node.id),
                available: available_state_kinds().join(", "),
            });
        }
    }

    let roots = nodes
        .iter()
        .filter(|n| matches!(n.role, NodeRole::Root))
        .count();
    let children = nodes.len() - roots;
    println!(
        "ok: '{}' rows={} (roots={}, children={}) execution={}",
        cfg.name.as_deref().unwrap_or("(unnamed)"),
        nodes.len(),
        roots,
        children,
        cfg.execution
            .as_ref()
            .map(|e| format!(
                "max_concurrent={:?} on_error={:?}",
                e.max_concurrent.unwrap_or(0),
                e.on_error
            ))
            .unwrap_or_else(|| "(defaults)".to_owned()),
    );
    for node in &nodes {
        let role = match &node.role {
            NodeRole::Root => "root".to_owned(),
            NodeRole::Child {
                parent_id,
                parent_key,
            } => {
                format!("child of '{parent_id}' (parent_key={parent_key})")
            }
        };
        println!(
            "  - {} [{}] source={} sink={}",
            node.id, role, node.source.kind, node.sink.kind
        );
    }
    Ok(())
}
