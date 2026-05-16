//! `faucet schema` — print the JSON Schema for a connector's config.

use crate::cli::SchemaArgs;
use crate::error::CliResult;
use crate::registry::{sink_schema, source_schema};

/// Execute the `schema` subcommand.
pub async fn run(args: SchemaArgs) -> CliResult<()> {
    let schema = match args.kind.as_str() {
        "source" => source_schema(&args.name)?,
        "sink" => sink_schema(&args.name)?,
        // clap restricts this via value_parser, but keep the safety net.
        other => {
            return Err(crate::error::CliError::ParseConfig {
                path: std::path::PathBuf::from(other),
                message: format!("kind must be 'source' or 'sink', got {other}"),
            });
        }
    };
    let body = serde_json::to_string_pretty(&schema).unwrap_or_else(|_| schema.to_string());
    println!("{body}");
    Ok(())
}
