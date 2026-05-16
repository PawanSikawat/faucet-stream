//! `faucet validate` — parse + validate a pipeline config without running.

use crate::cli::ValidateArgs;
use crate::config::PipelineConfig;
use crate::error::CliResult;
use crate::registry::{sink_schema, source_schema};
use crate::state::available_state_kinds;
use crate::transforms::available_transforms;

/// Execute the `validate` subcommand.
pub async fn run(args: ValidateArgs) -> CliResult<()> {
    let cfg = PipelineConfig::from_path(&args.config)?;
    // Verifying the schema lookup also catches unknown connector kinds.
    source_schema(&cfg.source.kind)?;
    sink_schema(&cfg.sink.kind)?;

    for t in &cfg.transforms {
        if !available_transforms().contains(&t.kind.as_str()) {
            return Err(crate::error::CliError::UnknownTransform {
                name: t.kind.clone(),
                available: available_transforms().join(", "),
            });
        }
    }
    if let Some(state) = &cfg.state
        && !available_state_kinds().contains(&state.kind.as_str())
    {
        return Err(crate::error::CliError::UnknownStateStore {
            name: state.kind.clone(),
            available: available_state_kinds().join(", "),
        });
    }

    println!(
        "ok: '{}' source={} sink={} transforms={} state={}",
        cfg.name.as_deref().unwrap_or("(unnamed)"),
        cfg.source.kind,
        cfg.sink.kind,
        cfg.transforms.len(),
        cfg.state
            .as_ref()
            .map(|s| s.kind.as_str())
            .unwrap_or("(none)"),
    );
    Ok(())
}
