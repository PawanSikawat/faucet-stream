//! `faucet init` — scaffold a starter `pipeline.yaml` from each connector's
//! JSON Schema. Defaults to a `rest` → `jsonl` pipeline so `faucet init` with
//! no flags continues to produce the same shape it did before this command
//! grew schema-driven scaffolding.

use std::collections::HashMap;

use crate::cli::InitArgs;
use crate::error::{CliError, CliResult};
#[cfg(feature = "cli-interactive")]
use crate::init_template::discover_tagged_enum_fields;
use crate::init_template::schema_to_yaml_template_with_choices;
use crate::registry;

const DEFAULT_SOURCE: &str = "rest";
const DEFAULT_SINK: &str = "jsonl";
const DEFAULT_NAME: &str = "my-pipeline";
const CONFIG_INDENT: usize = 6;

/// Execute the `init` subcommand.
pub async fn run(args: InitArgs) -> CliResult<()> {
    if args.output.exists() && !args.force {
        return Err(CliError::ScaffoldExists {
            path: args.output.clone(),
        });
    }

    let (source_kind, sink_kind) = resolve_kinds(&args)?;
    let name = args.name.as_deref().unwrap_or(DEFAULT_NAME);

    let source_schema = registry::source_schema(&source_kind)?;
    let sink_schema = registry::sink_schema(&sink_kind)?;
    let (source_choices, sink_choices) = if args.interactive {
        interactive_variant_choices(&source_schema, &sink_schema)?
    } else {
        (HashMap::new(), HashMap::new())
    };

    let body = render_pipeline(
        name,
        &source_kind,
        &source_schema,
        &source_choices,
        &sink_kind,
        &sink_schema,
        &sink_choices,
    );
    std::fs::write(&args.output, body)?;
    println!("wrote {}", args.output.display());
    Ok(())
}

fn resolve_kinds(args: &InitArgs) -> CliResult<(String, String)> {
    let source = args.source.clone();
    let sink = args.sink.clone();

    let (source, sink) = if args.interactive {
        interactive_prompt(source, sink)?
    } else {
        (
            source.unwrap_or_else(|| DEFAULT_SOURCE.to_string()),
            sink.unwrap_or_else(|| DEFAULT_SINK.to_string()),
        )
    };

    if !registry::source_exists(&source) {
        return Err(unknown_kind_err("source", &source));
    }
    if !registry::sink_exists(&sink) {
        return Err(unknown_kind_err("sink", &sink));
    }
    Ok((source, sink))
}

#[cfg(feature = "cli-interactive")]
fn interactive_prompt(source: Option<String>, sink: Option<String>) -> CliResult<(String, String)> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return fallback_kinds(source, sink, "stdin is not a TTY");
    }
    let sources = registry::source_kinds();
    let sinks = registry::sink_kinds();
    let s = if let Some(s) = source {
        s
    } else {
        prompt_select("source", &sources)?
    };
    let k = if let Some(k) = sink {
        k
    } else {
        prompt_select("sink", &sinks)?
    };
    Ok((s, k))
}

#[cfg(not(feature = "cli-interactive"))]
fn interactive_prompt(source: Option<String>, sink: Option<String>) -> CliResult<(String, String)> {
    fallback_kinds(
        source,
        sink,
        "the `cli-interactive` build feature is not enabled",
    )
}

fn fallback_kinds(
    source: Option<String>,
    sink: Option<String>,
    reason: &str,
) -> CliResult<(String, String)> {
    match (source, sink) {
        (Some(s), Some(k)) => Ok((s, k)),
        (s, k) => {
            tracing::warn!(
                "ignoring --interactive: {reason}; falling back to --source/--sink (or defaults)"
            );
            Ok((
                s.unwrap_or_else(|| DEFAULT_SOURCE.to_string()),
                k.unwrap_or_else(|| DEFAULT_SINK.to_string()),
            ))
        }
    }
}

#[cfg(feature = "cli-interactive")]
fn prompt_select(kind: &str, options: &[&'static str]) -> CliResult<String> {
    let choice = inquire::Select::new(&format!("Pick a {kind} connector"), options.to_vec())
        .prompt()
        .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;
    Ok(choice.to_string())
}

#[cfg(feature = "cli-interactive")]
fn interactive_variant_choices(
    source_schema: &serde_json::Value,
    sink_schema: &serde_json::Value,
) -> CliResult<(HashMap<String, String>, HashMap<String, String>)> {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return Ok((HashMap::new(), HashMap::new()));
    }
    let source = prompt_variants_for("source", source_schema)?;
    let sink = prompt_variants_for("sink", sink_schema)?;
    Ok((source, sink))
}

#[cfg(not(feature = "cli-interactive"))]
fn interactive_variant_choices(
    _source_schema: &serde_json::Value,
    _sink_schema: &serde_json::Value,
) -> CliResult<(HashMap<String, String>, HashMap<String, String>)> {
    tracing::warn!("ignoring --interactive: the `cli-interactive` build feature is not enabled");
    Ok((HashMap::new(), HashMap::new()))
}

#[cfg(feature = "cli-interactive")]
fn prompt_variants_for(
    side: &str,
    schema: &serde_json::Value,
) -> CliResult<HashMap<String, String>> {
    let fields = discover_tagged_enum_fields(schema);
    let mut choices = HashMap::new();
    for field in fields {
        let label = format!("Pick a variant for {side}.{}", field.path);
        let opts: Vec<String> = field.variants.clone();
        let chosen = inquire::Select::new(&label, opts)
            .prompt()
            .map_err(|e| CliError::Io(std::io::Error::other(e.to_string())))?;
        choices.insert(field.path, chosen);
    }
    Ok(choices)
}

fn unknown_kind_err(kind: &'static str, name: &str) -> CliError {
    let available = if kind == "source" {
        registry::source_kinds()
    } else {
        registry::sink_kinds()
    };
    CliError::UnknownConnector {
        kind,
        name: name.to_owned(),
        available: if available.is_empty() {
            "(none — rebuild faucet-cli with the relevant feature enabled)".to_owned()
        } else {
            available.join(", ")
        },
    }
}

fn render_pipeline(
    name: &str,
    source_kind: &str,
    source_schema: &serde_json::Value,
    source_choices: &HashMap<String, String>,
    sink_kind: &str,
    sink_schema: &serde_json::Value,
    sink_choices: &HashMap<String, String>,
) -> String {
    let source_yaml =
        schema_to_yaml_template_with_choices(source_schema, CONFIG_INDENT, source_choices);
    let sink_yaml = schema_to_yaml_template_with_choices(sink_schema, CONFIG_INDENT, sink_choices);

    let mut body = String::new();
    body.push_str("version: 1\n");
    body.push_str(&format!("name: {name}\n\n"));
    body.push_str("# The base pipeline. Every matrix row (below) is deep-merged into this.\n");
    body.push_str("# Even with no matrix block, this section runs once on its own.\n");
    body.push_str("pipeline:\n");
    body.push_str("  source:\n");
    body.push_str(&format!("    type: {source_kind}\n"));
    body.push_str("    config:\n");
    body.push_str(&source_yaml);
    body.push('\n');
    body.push_str("  # transforms:\n");
    body.push_str("  #   - type: snake_case\n\n");
    body.push_str("  sink:\n");
    body.push_str(&format!("    type: {sink_kind}\n"));
    body.push_str("    config:\n");
    body.push_str(&sink_yaml);
    body.push('\n');
    body.push_str("  # Optional state store (required by CDC sources and resumable runs).\n");
    body.push_str("  # state:\n");
    body.push_str("  #   type: file\n");
    body.push_str("  #   config: { path: ./.faucet-state }\n\n");
    body.push_str("  # Optional Dead Letter Queue.\n");
    body.push_str("  # dlq:\n");
    body.push_str("  #   sink:\n");
    body.push_str("  #     type: jsonl\n");
    body.push_str("  #     config: { path: ./dlq.jsonl }\n");
    body.push_str("  #   on_batch_error: propagate   # or dlq_all\n\n");
    body.push_str("# Optional matrix block — each row is deep-merged into `pipeline:` above.\n");
    body.push_str("# matrix:\n");
    body.push_str("#   - id: users\n");
    body.push_str("#     source: { config: { path: /v1/users } }\n");
    body
}
