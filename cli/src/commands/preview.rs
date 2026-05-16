//! `faucet preview` — run only the source side and emit the first N records
//! to stdout as JSON Lines.
//!
//! Requires the `sink-stdout` feature to be enabled.

use crate::cli::PreviewArgs;
use crate::config::PipelineConfig;
use crate::error::CliResult;
use crate::registry::build_source;
use crate::transforms::compile_transforms;
use faucet_core::transform::{apply_all, compile as compile_transform};

#[cfg(feature = "sink-stdout")]
use faucet_core::{Pipeline, Sink};

/// Execute the `preview` subcommand.
#[cfg(feature = "sink-stdout")]
pub async fn run(args: PreviewArgs) -> CliResult<()> {
    use faucet_sink_stdout::{StdoutFormat, StdoutSink, StdoutSinkConfig};
    let cfg = PipelineConfig::from_path(&args.config)?;
    let source = build_source(&cfg.source.kind, cfg.source.config.clone()).await?;

    let transforms = compile_transforms(&cfg.transforms)?;
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

    // Suppress unused-import warning on the path that builds Pipeline.
    let _ = std::marker::PhantomData::<Pipeline<'_, dyn faucet_core::Source, dyn Sink>>;
    Ok(())
}

#[cfg(not(feature = "sink-stdout"))]
pub async fn run(_args: PreviewArgs) -> CliResult<()> {
    Err(crate::error::CliError::UnknownConnector {
        kind: "sink",
        name: "stdout".into(),
        available: "(preview requires faucet-cli to be built with the 'sink-stdout' feature)"
            .into(),
    })
}
