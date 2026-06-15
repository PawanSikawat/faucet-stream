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
        SchemaTarget::Replication => {
            let s = faucet_core::schema_for!(crate::replication::spec::ReplicationSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
        }
        SchemaTarget::Execution => {
            let s = faucet_core::schema_for!(crate::config::ExecutionSpec);
            serde_json::to_value(s).unwrap_or_else(|_| serde_json::json!({"type": "object"}))
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
        #[cfg(feature = "lineage")]
        SchemaTarget::Lineage => lineage_schema(),
        #[cfg(feature = "triggers")]
        SchemaTarget::Triggers => {
            let s = faucet_core::schema_for!(crate::serve::triggers::spec::TriggersFile);
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

/// JSON Schema for the `lineage:` config block (`faucet schema lineage`).
#[cfg(feature = "lineage")]
pub fn lineage_schema() -> serde_json::Value {
    serde_json::to_value(faucet_lineage::schemars_schema())
        .unwrap_or_else(|_| serde_json::json!({"type": "object"}))
}

#[cfg(test)]
mod tests {
    use crate::cli::{SchemaArgs, SchemaTarget};

    #[cfg(feature = "lineage")]
    #[test]
    fn schema_lineage_returns_object_schema() {
        let v = super::lineage_schema();
        assert_eq!(v["type"], "object");
        assert!(v["properties"].get("transport").is_some());
        assert!(v["properties"].get("namespace").is_some());
    }

    #[tokio::test]
    async fn schema_replication_target_ok() {
        // Covers the `SchemaTarget::Replication` arm — it serializes the
        // ReplicationSpec JSON Schema to stdout and returns Ok.
        let r = super::run(SchemaArgs {
            target: SchemaTarget::Replication,
        })
        .await;
        assert!(r.is_ok(), "{r:?}");
    }

    #[tokio::test]
    async fn schema_execution_target_ok() {
        let r = super::run(SchemaArgs {
            target: SchemaTarget::Execution,
        })
        .await;
        assert!(r.is_ok(), "{r:?}");
    }

    #[test]
    fn execution_schema_includes_adaptive_batch_size() {
        let schema = faucet_core::schema_for!(crate::config::ExecutionSpec);
        let value = serde_json::to_value(schema).expect("execution schema serializes");
        assert!(value["properties"].get("adaptive_batch_size").is_some());
    }
}
