//! `faucet schema` — print the JSON Schema for a connector's config.

use crate::cli::{SchemaArgs, SchemaTarget};
use crate::error::CliResult;
use crate::registry::{sink_schema, source_schema};
use crate::transforms::transform_schema;

/// Execute the `schema` subcommand.
pub async fn run(args: SchemaArgs) -> CliResult<()> {
    let schema = match args.target {
        SchemaTarget::Source { name } => source_schema(&name)?,
        SchemaTarget::Sink { name } => sink_schema(&name)?,
        SchemaTarget::Transform { name } => transform_schema(&name)?,
        SchemaTarget::Dlq => {
            let dlq_schema = faucet_core::schema_for!(crate::config::DlqSpec);
            serde_json::to_value(dlq_schema)
                .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
    };
    let body = serde_json::to_string_pretty(&schema).unwrap_or_else(|_| schema.to_string());
    println!("{body}");
    Ok(())
}
