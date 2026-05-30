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
        #[cfg(feature = "quality")]
        SchemaTarget::Quality => {
            let quality_schema = faucet_core::schema_for!(faucet_core::QualitySpec);
            serde_json::to_value(quality_schema)
                .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        #[cfg(feature = "schedule")]
        SchemaTarget::Schedule => {
            let s = faucet_core::schema_for!(crate::schedule::spec::ScheduleSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Secrets => serde_json::json!({
            "title": "Secrets-manager interpolation grammar",
            "schemes": {
                "vault":    { "syntax": "${vault:<path>[#field]}", "auth": ["VAULT_ADDR", "VAULT_TOKEN", "VAULT_NAMESPACE (optional)"] },
                "aws-sm":   { "syntax": "${aws-sm:<name-or-ARN>[#field]}", "auth": ["aws-config default credential chain"] },
                "gcp-sm":   { "syntax": "${gcp-sm:projects/<p>/secrets/<s>/versions/<v>}", "auth": ["Application Default Credentials"] },
                "azure-kv": { "syntax": "${azure-kv:<vault>/<secret>[/<version>]}", "auth": ["AZURE_* env / managed identity / az login"] }
            },
            "notes": [
                "#field parses the secret as JSON and extracts one key (vault, aws-sm).",
                "Resolved at config load; fetched concurrently and de-duplicated; never persisted.",
                "Build with --features secrets (or per-backend secrets-vault / secrets-aws-sm / ...)."
            ]
        }),
    };
    let body = serde_json::to_string_pretty(&schema).unwrap_or_else(|_| schema.to_string());
    println!("{body}");
    Ok(())
}
