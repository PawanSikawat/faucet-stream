//! `faucet contract` — validate a config's `contract:` block and print a
//! human summary, or export it in a machine-readable format (`--export
//! contract | json-schema | openlineage`). Offline-safe: secrets are never
//! fetched (a contract holds no credentials).

use crate::cli::{ContractArgs, ContractExportFormat};
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use faucet_core::contract::{CompiledContract, ContractSpec, to_json_schema, to_openlineage_facet};

/// Producer identifier embedded in the OpenLineage export.
const PRODUCER: &str = concat!(
    "https://github.com/faucet-hq/faucet-stream/tree/v",
    env!("CARGO_PKG_VERSION")
);

/// Execute the `contract` subcommand.
pub async fn run(args: ContractArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path_tolerating_secrets(&path, args.profile.as_deref())?;
    let spec = cfg.pipeline.contract.as_ref().ok_or_else(|| {
        CliError::Config(
            "no `pipeline.contract:` block in this config — add one, or run \
             `faucet schema contract` to see the block's JSON Schema"
                .to_string(),
        )
    })?;
    // Compile first so a malformed contract fails before anything is printed.
    let compiled =
        CompiledContract::compile(spec).map_err(|e| CliError::Config(format!("contract: {e}")))?;

    match args.export {
        None => print!("{}", render_summary(spec, &compiled)),
        Some(format) => {
            let doc = export(spec, format);
            let body = serde_json::to_string_pretty(&doc).unwrap_or_else(|_| doc.to_string());
            println!("{body}");
        }
    }
    Ok(())
}

/// Render the exported document for the requested format. Pure.
pub fn export(spec: &ContractSpec, format: ContractExportFormat) -> serde_json::Value {
    match format {
        ContractExportFormat::Contract => {
            serde_json::to_value(spec).unwrap_or_else(|_| serde_json::json!({}))
        }
        ContractExportFormat::JsonSchema => to_json_schema(spec),
        ContractExportFormat::Openlineage => to_openlineage_facet(spec, PRODUCER),
    }
}

/// Render the human summary. Pure — returned as a string for testability.
fn render_summary(spec: &ContractSpec, compiled: &CompiledContract) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "contract v{} — valid ({} field{})",
        spec.version,
        spec.fields.len(),
        if spec.fields.len() == 1 { "" } else { "s" }
    );
    if let Some(d) = &spec.description {
        let _ = writeln!(out, "  description: {d}");
    }
    if let Some(o) = &spec.owner {
        let _ = writeln!(out, "  owner: {o}");
    }
    let _ = writeln!(out, "  on_breach: {}", spec.on_breach);
    let _ = writeln!(out, "  allow_extra_fields: {}", spec.allow_extra_fields);
    let _ = writeln!(out, "  fields:");
    for f in &spec.fields {
        let mut flags: Vec<String> = Vec::new();
        if !f.required {
            flags.push("optional".into());
        }
        if f.nullable {
            flags.push("nullable".into());
        }
        if let Some(values) = &f.allowed_values {
            flags.push(format!("enum[{}]", values.len()));
        }
        if f.pattern.is_some() {
            flags.push("pattern".into());
        }
        if f.min.is_some() || f.max.is_some() {
            flags.push("range".into());
        }
        if f.min_length.is_some() || f.max_length.is_some() {
            flags.push("length".into());
        }
        let suffix = if flags.is_empty() {
            String::new()
        } else {
            format!(" ({})", flags.join(", "))
        };
        let _ = writeln!(out, "    - {}: {}{}", f.name, f.field_type, suffix);
    }
    if compiled.requires_dlq() {
        let _ = writeln!(
            out,
            "  note: on_breach=quarantine requires a `dlq:` block at run time"
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> ContractSpec {
        serde_json::from_value(json!({
            "version": "1.2.0",
            "owner": "data-platform",
            "on_breach": "quarantine",
            "allow_extra_fields": false,
            "fields": [
                { "name": "id", "type": "integer", "min": 0 },
                { "name": "status", "type": "string", "enum": ["a", "b"],
                  "required": false, "nullable": true }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn summary_lists_fields_policy_and_dlq_note() {
        let s = spec();
        let compiled = CompiledContract::compile(&s).unwrap();
        let out = render_summary(&s, &compiled);
        assert!(out.contains("contract v1.2.0 — valid (2 fields)"), "{out}");
        assert!(out.contains("owner: data-platform"), "{out}");
        assert!(out.contains("on_breach: quarantine"), "{out}");
        assert!(out.contains("allow_extra_fields: false"), "{out}");
        assert!(out.contains("- id: integer (range)"), "{out}");
        assert!(
            out.contains("- status: string (optional, nullable, enum[2])"),
            "{out}"
        );
        assert!(out.contains("requires a `dlq:` block"), "{out}");
    }

    #[test]
    fn summary_omits_dlq_note_for_fail() {
        let s: ContractSpec = serde_json::from_value(json!({
            "version": "1",
            "fields": [{ "name": "id", "type": "string" }]
        }))
        .unwrap();
        let compiled = CompiledContract::compile(&s).unwrap();
        let out = render_summary(&s, &compiled);
        assert!(out.contains("(1 field)"), "{out}");
        assert!(!out.contains("dlq"), "{out}");
    }

    #[test]
    fn export_contract_round_trips_the_spec() {
        let doc = export(&spec(), ContractExportFormat::Contract);
        let back: ContractSpec = serde_json::from_value(doc).unwrap();
        assert_eq!(back.version, "1.2.0");
        assert_eq!(back.fields.len(), 2);
    }

    #[test]
    fn export_json_schema_is_a_schema_document() {
        let doc = export(&spec(), ContractExportFormat::JsonSchema);
        assert_eq!(doc["x-faucet-contract-version"], "1.2.0");
        assert_eq!(doc["type"], "object");
        assert_eq!(doc["additionalProperties"], false);
        assert!(doc["properties"]["id"].is_object());
    }

    #[test]
    fn export_openlineage_is_a_schema_facet() {
        let doc = export(&spec(), ContractExportFormat::Openlineage);
        assert!(doc["_producer"].as_str().unwrap().contains("faucet-stream"));
        assert_eq!(doc["fields"].as_array().unwrap().len(), 2);
    }
}
