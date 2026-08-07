//! MCP tool definitions + in-process dispatch (issue #420).
//!
//! Every tool is a thin shape-adapter over an existing faucet capability —
//! `registry` (list / schema), `init_template` (scaffold), `expand` /
//! `topology` (validate), `preview`, and `run_from_yaml_str` (the one gated
//! mutating tool). No pipeline logic is re-implemented here.

use super::McpContext;
use crate::config::PipelineConfig;
use crate::mcp::protocol::{ToolDef, tool_error, tool_text};
use serde_json::{Value, json};

/// Hard cap on `preview` rows regardless of the requested limit — an MCP
/// client must never trigger a full extract of a large source.
const PREVIEW_MAX: usize = 100;

/// Build the advertised tool list for `tools/list`, honoring the mutation gate.
pub fn tool_defs(ctx: &McpContext) -> Vec<ToolDef> {
    let mut defs = vec![
        ToolDef {
            name: "list_connectors",
            description: "List all compiled-in sources, sinks, transforms, and state stores, each with a one-line description and (for connectors) a conformance tier.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["source", "sink", "transform", "state", "all"], "description": "Filter to one category (default: all)." }
                }
            }),
        },
        ToolDef {
            name: "get_connector_schema",
            description: "Return the JSON Schema for a connector or transform's config block.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "kind": { "type": "string", "enum": ["source", "sink", "transform"] },
                    "name": { "type": "string", "description": "Connector/transform name, e.g. 'rest' or 'keys_case'." }
                },
                "required": ["kind", "name"]
            }),
        },
        ToolDef {
            name: "scaffold_config",
            description: "Generate a commented YAML pipeline skeleton for a source→sink pair (read-only: returns text, writes nothing).",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "source": { "type": "string", "description": "Source connector kind." },
                    "sink": { "type": "string", "description": "Sink connector kind." },
                    "name": { "type": "string", "description": "Optional pipeline name." }
                },
                "required": ["source", "sink"]
            }),
        },
        ToolDef {
            name: "validate_config",
            description: "Fully validate a pipeline YAML/JSON config (structure, templates, matrix/topology graph). Returns a per-node report or the validation error.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "The pipeline config document (YAML or JSON)." }
                },
                "required": ["config"]
            }),
        },
        ToolDef {
            name: "preview",
            description: "Fetch a bounded sample of records from a config's first source (source side only; downstream sinks are not run). Capped at 100 rows.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string", "description": "The pipeline config document (YAML or JSON)." },
                    "limit": { "type": "integer", "description": "Max rows to return (1–100, default 10)." }
                },
                "required": ["config"]
            }),
        },
    ];
    if ctx.allow_mutations {
        defs.push(ToolDef {
            name: "run_pipeline",
            description: "Run a pipeline from an inline config. MUTATING — gated behind --allow-mutations. Pass dry_run:true to validate+preview only.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "config": { "type": "string" },
                    "dry_run": { "type": "boolean", "description": "If true, validate + preview only; do not write to any sink." }
                },
                "required": ["config"]
            }),
        });
    }
    #[cfg(feature = "templates")]
    if ctx.templates.is_some() {
        defs.push(ToolDef {
            name: "list_templates",
            description: "List registered pipeline templates (latest version of each) with the typed params each one takes.",
            input_schema: json!({ "type": "object", "properties": {} }),
        });
        defs.push(ToolDef {
            name: "get_template",
            description: "Show one registered pipeline template: its declared params, stored config body, and available versions.",
            input_schema: json!({
                "type": "object",
                "properties": {
                    "id": { "type": "string", "description": "Template id." },
                    "version": { "description": "Version: a number, or a named channel (\"latest\" — the default — \"prod\", \"pre-prod\", \"staging\", \"canary\", \"stable\", \"test\", \"dev\", \"previous\").", "oneOf": [{ "type": "integer" }, { "type": "string" }] }
                },
                "required": ["id"]
            }),
        });
        if ctx.allow_mutations {
            defs.push(ToolDef {
                name: "register_template",
                description: "Register a config (declaring typed `params:`) as a new pipeline-template version. MUTATING — gated behind --allow-mutations.",
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "config": { "type": "string", "description": "The pipeline config document (YAML or JSON), stored verbatim." },
                        "id": { "type": "string", "description": "Template id. Derived from the config's `name:` when omitted." },
                        "description": { "type": "string" },
                        "tags": { "type": "array", "items": { "type": "string" }, "description": "Named channels to point at the new version (dev/test/staging/pre-prod/canary/stable/prod/previous). The version number always auto-increments; `latest` is derived and rejected." }
                    },
                    "required": ["config"]
                }),
            });
            defs.push(ToolDef {
                name: "run_template",
                description: "Run a registered pipeline template with the given params. MUTATING — gated behind --allow-mutations. Pass dry_run:true to materialize + validate only.",
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "id": { "type": "string" },
                        "version": { "description": "Version: a number, or a named channel (\"latest\" — the default — \"prod\", \"pre-prod\", \"staging\", \"canary\", \"stable\", \"test\", \"dev\", \"previous\").", "oneOf": [{ "type": "integer" }, { "type": "string" }] },
                        "params": { "type": "object", "description": "Values for the template's declared params." },
                        "env": { "type": "object", "description": "Per-run overrides for ${env:VAR} resolution." },
                        "dry_run": { "type": "boolean", "description": "If true, materialize + validate only; do not write to any sink." }
                    },
                    "required": ["id"]
                }),
            });
        }
    }
    defs
}

/// Dispatch a `tools/call`. Returns the MCP `tools/call` result envelope
/// (`content` + `isError`). A tool-level failure is `tool_error(..)`, not a
/// JSON-RPC protocol error.
pub async fn call_tool(ctx: &McpContext, name: &str, args: &Value) -> Value {
    let result: Result<String, String> = match name {
        "list_connectors" => list_connectors(args),
        "get_connector_schema" => get_connector_schema(args),
        "scaffold_config" => scaffold_config(args),
        "validate_config" => validate_config(ctx, args).await,
        "preview" => preview(ctx, args).await,
        "run_pipeline" => {
            if !ctx.allow_mutations {
                Err("run_pipeline is disabled; start the MCP server with --allow-mutations to enable mutating tools".to_string())
            } else {
                run_pipeline(ctx, args).await
            }
        }
        #[cfg(feature = "templates")]
        "list_templates" => list_templates(ctx).await,
        #[cfg(feature = "templates")]
        "get_template" => get_template(ctx, args).await,
        #[cfg(feature = "templates")]
        "register_template" => {
            if !ctx.allow_mutations {
                Err(MUTATION_GATE.to_string())
            } else {
                register_template(ctx, args).await
            }
        }
        #[cfg(feature = "templates")]
        "run_template" => {
            if !ctx.allow_mutations {
                Err(MUTATION_GATE.to_string())
            } else {
                run_template(ctx, args).await
            }
        }
        other => Err(format!("unknown tool '{other}'")),
    };
    match result {
        Ok(text) => tool_text(text),
        // Redact any resolved secret material that reached an error string.
        Err(msg) => tool_error(crate::secrets::registry::redact(&msg)),
    }
}

fn str_arg<'a>(args: &'a Value, key: &str) -> Result<&'a str, String> {
    args.get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("missing required string argument '{key}'"))
}

fn tier_of(kind: &str, is_source: bool) -> &'static str {
    crate::conformance::tier_for(kind, is_source).as_str()
}

fn list_connectors(args: &Value) -> Result<String, String> {
    let filter = args.get("kind").and_then(Value::as_str).unwrap_or("all");
    let want = |c: &str| filter == "all" || filter == c;

    let mut out = json!({});
    let obj = out.as_object_mut().unwrap();
    if want("source") {
        let sources: Vec<Value> = crate::registry::source_descriptions()
            .into_iter()
            .map(|(name, desc)| json!({ "name": name, "description": desc, "tier": tier_of(name, true) }))
            .collect();
        obj.insert("sources".into(), json!(sources));
    }
    if want("sink") {
        let sinks: Vec<Value> = crate::registry::sink_descriptions()
            .into_iter()
            .map(|(name, desc)| json!({ "name": name, "description": desc, "tier": tier_of(name, false) }))
            .collect();
        obj.insert("sinks".into(), json!(sinks));
    }
    if want("transform") {
        let transforms: Vec<Value> = crate::transforms::transform_descriptions()
            .into_iter()
            .map(|(name, desc)| json!({ "name": name, "description": desc }))
            .collect();
        obj.insert("transforms".into(), json!(transforms));
    }
    if want("state") {
        obj.insert(
            "state_stores".into(),
            json!(crate::state::available_state_kinds()),
        );
    }
    Ok(pretty(&out))
}

fn get_connector_schema(args: &Value) -> Result<String, String> {
    let kind = str_arg(args, "kind")?;
    let name = str_arg(args, "name")?;
    let schema = match kind {
        "source" => crate::registry::source_schema(name),
        "sink" => crate::registry::sink_schema(name),
        "transform" => crate::transforms::transform_schema(name),
        other => return Err(format!("kind must be source|sink|transform, got '{other}'")),
    }
    .map_err(|e| e.to_string())?;
    Ok(pretty(&schema))
}

fn scaffold_config(args: &Value) -> Result<String, String> {
    let source = str_arg(args, "source")?;
    let sink = str_arg(args, "sink")?;
    let name = args
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("pipeline");

    let src_schema = crate::registry::source_schema(source).map_err(|e| e.to_string())?;
    let sink_schema = crate::registry::sink_schema(sink).map_err(|e| e.to_string())?;
    let src_yaml = crate::init_template::schema_to_yaml_template(&src_schema, 6);
    let sink_yaml = crate::init_template::schema_to_yaml_template(&sink_schema, 6);

    Ok(format!(
        "version: 1\nname: {name}\npipeline:\n  source:\n    type: {source}\n    config:\n{src_yaml}\n  sink:\n    type: {sink}\n    config:\n{sink_yaml}"
    ))
}

/// Parse an inline config document, mirroring the file-load pipeline: resolve
/// `${env:}` / `${file:}` / `${secret:}` per scalar, bind `${param.*}`, then take
/// the typed path. YAML is a JSON superset, so one parser handles both wire
/// formats.
///
/// `mode` decides what an unsupplied `required` param means: `Placeholder` for
/// the read-only introspection tools (a parameterized config still validates),
/// `Strict` where the config is about to actually run.
fn parse_config_with(text: &str, mode: crate::params::BindMode) -> Result<PipelineConfig, String> {
    let mut doc: Value = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    crate::interpolate::interpolate_value(&mut doc).map_err(|e| e.to_string())?;
    crate::params::bind_document(&mut doc, &Default::default(), mode).map_err(|e| e.to_string())?;
    PipelineConfig::from_value(doc).map_err(|e| e.to_string())
}

/// Read-only introspection: a config whose required params arrive later still
/// validates, against type-shaped placeholders.
fn parse_config(text: &str) -> Result<PipelineConfig, String> {
    parse_config_with(text, crate::params::BindMode::Placeholder)
}

async fn validate_config(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let text = str_arg(args, "config")?;
    let cfg = parse_config(text)?;

    if crate::topology::is_topology(&cfg) {
        let topo = crate::topology::build_topology(&cfg, &ctx.auth)
            .await
            .map_err(|e| e.to_string())?;
        return Ok(pretty(&json!({
            "valid": true,
            "mode": "topology",
            "nodes": topo.nodes().iter().map(|n| json!({"id": n.id, "kind": n.kind.kind_str()})).collect::<Vec<_>>(),
            "edges": topo.edges().len(),
        })));
    }

    let nodes = crate::expand::expand(&cfg).map_err(|e| e.to_string())?;
    let rows: Vec<Value> = nodes
        .iter()
        .map(|n| {
            json!({
                "id": n.id,
                "source": n.source.kind,
                "sink": n.sink.kind,
                "transforms": n.transforms.len(),
            })
        })
        .collect();
    Ok(pretty(&json!({
        "valid": true,
        "mode": "matrix",
        "name": cfg.name,
        "rows": rows,
    })))
}

async fn preview(ctx: &McpContext, args: &Value) -> Result<String, String> {
    use faucet_core::stage::{apply_stages, compile_stage};

    let text = str_arg(args, "config")?;
    let limit = args
        .get("limit")
        .and_then(Value::as_u64)
        .map(|n| (n as usize).clamp(1, PREVIEW_MAX))
        .unwrap_or(10);

    let cfg = parse_config(text)?;
    if crate::topology::is_topology(&cfg) {
        return crate::topology::preview_to_string(&cfg, &ctx.auth, limit)
            .await
            .map_err(|e| e.to_string());
    }

    let nodes = crate::expand::expand(&cfg).map_err(|e| e.to_string())?;
    let first_root = nodes
        .iter()
        .find(|n| matches!(n.role, crate::expand::NodeRole::Root))
        .ok_or_else(|| "no root row to preview".to_string())?;

    let source = crate::registry::build_source(
        &first_root.source.kind,
        first_root.source.config.clone(),
        &ctx.auth,
        None,
    )
    .await
    .map_err(|e| e.to_string())?;
    let stages =
        crate::transforms::compile_transforms(&first_root.transforms).map_err(|e| e.to_string())?;
    let records = source.fetch_all().await.map_err(|e| e.to_string())?;
    let records: Vec<Value> = if stages.is_empty() {
        records
    } else {
        let compiled = stages
            .iter()
            .map(compile_stage)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| e.to_string())?;
        let mut out = Vec::with_capacity(records.len());
        for r in records {
            out.extend(apply_stages(r, &compiled).map_err(|e| e.to_string())?);
        }
        out
    };
    let limited: Vec<Value> = records.into_iter().take(limit).collect();
    Ok(pretty(
        &json!({ "row": first_root.id, "count": limited.len(), "records": limited }),
    ))
}

async fn run_pipeline(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let text = str_arg(args, "config")?;
    let dry_run = args
        .get("dry_run")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    if dry_run {
        let mut report = validate_config(ctx, args).await?;
        report.push_str("\n\n-- preview --\n");
        report.push_str(
            &preview(ctx, args)
                .await
                .unwrap_or_else(|e| format!("preview skipped: {e}")),
        );
        return Ok(report);
    }

    let summary = crate::run_from_yaml_str(text)
        .await
        .map_err(|e| e.to_string())?;
    let failed = summary.failure_count();
    let total: usize = summary.invocations.iter().map(|i| i.records_written).sum();
    let doc = json!({
        "invocations": summary.invocations.len(),
        "ok": summary.invocations.len() - failed,
        "failed": failed,
        "records_written": total,
    });
    if failed > 0 {
        return Err(format!(
            "pipeline had {failed} failed invocation(s): {}",
            pretty(&doc)
        ));
    }
    Ok(pretty(&doc))
}

fn pretty(v: &Value) -> String {
    serde_json::to_string_pretty(v).unwrap_or_else(|_| v.to_string())
}

// ── Pipeline template tools (#444) ──────────────────────────────────────────

/// Shared refusal text for a mutating template tool on a read-only server.
#[cfg(feature = "templates")]
const MUTATION_GATE: &str =
    "this tool is disabled; start the MCP server with --allow-mutations to enable mutating tools";

/// The wired template registry, or a tool error explaining how to wire one.
#[cfg(feature = "templates")]
fn template_store(ctx: &McpContext) -> Result<&crate::templates::TemplateStore, String> {
    ctx.templates.as_ref().ok_or_else(|| {
        "no pipeline-template registry is configured — start `faucet mcp --template-store \
         <url>`, or use the /mcp route of a `faucet serve --mcp` whose --history backend holds \
         the registry"
            .to_string()
    })
}

#[cfg(feature = "templates")]
async fn list_templates(ctx: &McpContext) -> Result<String, String> {
    let store = template_store(ctx)?;
    let templates = store.template_list().await.map_err(|e| e.to_string())?;
    Ok(pretty(&json!({
        "count": templates.len(),
        "templates": templates,
    })))
}

/// Read the optional `version` argument: a number, or one of the closed set of
/// named channels. Absent = `latest`, so an agent that never mentions versions
/// always gets the newest registration.
#[cfg(feature = "templates")]
fn version_arg(args: &Value) -> Result<crate::serve::history::templates::VersionSelector, String> {
    use crate::serve::history::templates::VersionSelector;
    match args.get("version") {
        None | Some(Value::Null) => Ok(VersionSelector::default()),
        Some(v) => serde_json::from_value::<VersionSelector>(v.clone()).map_err(|e| e.to_string()),
    }
}

/// Resolve the `version` argument against the registry (a named channel needs a
/// lookup). `Ok(None)` = the newest version.
#[cfg(feature = "templates")]
async fn resolved_version_arg(
    store: &crate::templates::TemplateStore,
    id: &str,
    args: &Value,
) -> Result<Option<u32>, String> {
    crate::templates::resolve_version(store, id, version_arg(args)?)
        .await
        .map_err(|e| e.to_string())
}

#[cfg(feature = "templates")]
async fn get_template(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let store = template_store(ctx)?;
    let id = str_arg(args, "id")?;
    let version = resolved_version_arg(store, id, args).await?;
    let record = store
        .template_get(id, version)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no pipeline template '{id}'"))?;
    let versions = store
        .template_versions(id)
        .await
        .map_err(|e| e.to_string())?;
    let tags = store.template_tags(id).await.map_err(|e| e.to_string())?;
    let latest = versions.first().copied().unwrap_or(record.version);
    Ok(pretty(&json!({
        "template": record,
        "versions": versions,
        "latest_version": latest,
        "is_latest": record.version == latest,
        "tags": tags,
    })))
}

/// Read the optional `tags` array, validating each against the closed channel set.
#[cfg(feature = "templates")]
fn tags_arg(args: &Value) -> Result<Vec<crate::serve::history::templates::VersionChannel>, String> {
    use crate::serve::history::templates::VersionChannel;
    let Some(list) = args.get("tags").and_then(Value::as_array) else {
        return Ok(Vec::new());
    };
    list.iter()
        .map(|v| {
            v.as_str()
                .ok_or_else(|| "each `tags` entry must be a channel name".to_string())
                .and_then(|s| VersionChannel::parse(s).map_err(|e| e.to_string()))
        })
        .collect()
}

#[cfg(feature = "templates")]
async fn register_template(ctx: &McpContext, args: &Value) -> Result<String, String> {
    use crate::templates::RegisterRequest;
    let store = template_store(ctx)?;
    let config = str_arg(args, "config")?;
    let record = crate::templates::register(
        store,
        RegisterRequest {
            id: args.get("id").and_then(Value::as_str).map(str::to_string),
            body: config.to_string(),
            // MCP always hands over an inline document; YAML parses JSON too.
            format: crate::serve::load::ConfigFormat::Yaml,
            description: args
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_string),
            tags: tags_arg(args)?,
            created_by: Some("mcp".to_string()),
        },
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(pretty(&json!({
        "registered": record.summary(),
    })))
}

#[cfg(feature = "templates")]
async fn run_template(ctx: &McpContext, args: &Value) -> Result<String, String> {
    let store = template_store(ctx)?;
    let id = str_arg(args, "id")?;
    let version = resolved_version_arg(store, id, args).await?;
    let dry_run = args
        .get("dry_run")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let supplied: crate::params::SuppliedParams = args
        .get("params")
        .and_then(Value::as_object)
        .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    let env: std::collections::BTreeMap<String, String> = args
        .get("env")
        .and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let materialized = crate::templates::materialize(store, id, version, &supplied, &env)
        .await
        .map_err(|e| e.to_string())?;

    if dry_run {
        // Validate the materialized config without touching a sink, and never
        // echo the body — a secret param value would be in it.
        let cfg = parse_config_with(&materialized.body, crate::params::BindMode::Strict)?;
        let rows = crate::expand::expand(&cfg)
            .map_err(|e| e.to_string())?
            .len();
        return Ok(pretty(&json!({
            "template_id": materialized.template_id,
            "template_version": materialized.version,
            "params": materialized.params_redacted,
            "rows": rows,
            "dry_run": true,
        })));
    }

    // The materialized body is JSON, which `run_from_yaml_str` parses (YAML is a
    // JSON superset) and takes through the ordinary run path.
    let summary = crate::run_from_yaml_str(&materialized.body)
        .await
        .map_err(|e| e.to_string())?;
    let failed = summary.failure_count();
    let total: usize = summary.invocations.iter().map(|i| i.records_written).sum();
    let doc = json!({
        "template_id": materialized.template_id,
        "template_version": materialized.version,
        "params": materialized.params_redacted,
        "invocations": summary.invocations.len(),
        "ok": summary.invocations.len() - failed,
        "failed": failed,
        "records_written": total,
    });
    if failed > 0 {
        return Err(format!(
            "template run had {failed} failed invocation(s): {}",
            pretty(&doc)
        ));
    }
    Ok(pretty(&doc))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mcp::McpContext;

    fn ctx(allow: bool) -> McpContext {
        McpContext::new(
            crate::auth_catalog::build_auth_catalog(None).unwrap(),
            allow,
        )
    }

    #[test]
    fn tool_defs_gate_mutations() {
        let ro = tool_defs(&ctx(false));
        assert!(ro.iter().all(|t| t.name != "run_pipeline"));
        let rw = tool_defs(&ctx(true));
        assert!(rw.iter().any(|t| t.name == "run_pipeline"));
    }

    #[tokio::test]
    async fn list_connectors_includes_sources_and_tier() {
        let out = call_tool(&ctx(false), "list_connectors", &json!({})).await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"sources\""));
        assert!(text.contains("\"tier\""));
    }

    #[tokio::test]
    async fn list_connectors_filter_kind() {
        let out = call_tool(
            &ctx(false),
            "list_connectors",
            &json!({"kind": "transform"}),
        )
        .await;
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"transforms\""));
        assert!(!text.contains("\"sources\""));
    }

    #[tokio::test]
    async fn get_connector_schema_unknown_is_tool_error() {
        let out = call_tool(
            &ctx(false),
            "get_connector_schema",
            &json!({"kind":"source","name":"nope"}),
        )
        .await;
        assert_eq!(out["isError"], true);
    }

    #[tokio::test]
    async fn unknown_tool_errors() {
        let out = call_tool(&ctx(false), "does_not_exist", &json!({})).await;
        assert_eq!(out["isError"], true);
    }

    #[tokio::test]
    async fn run_pipeline_blocked_without_mutations() {
        let out = call_tool(&ctx(false), "run_pipeline", &json!({"config":"version: 1"})).await;
        assert_eq!(out["isError"], true);
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("--allow-mutations")
        );
    }

    // ── handler coverage: scaffold / validate / preview / run ────────────────

    fn csv_config(dir: &std::path::Path) -> String {
        let csv = dir.join("in.csv");
        std::fs::write(&csv, "id,name\n1,alice\n2,bob\n").unwrap();
        let out = dir.join("out.jsonl");
        format!(
            "version: 1\nname: t\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            csv.display(),
            out.display()
        )
    }

    fn topology_config(dir: &std::path::Path) -> String {
        let csv = dir.join("in.csv");
        std::fs::write(&csv, "id,name\n1,alice\n").unwrap();
        let out = dir.join("out.jsonl");
        format!(
            "version: 1\nname: t\npipeline:\n  sources:\n    s: {{ type: csv, config: {{ path: {} }} }}\n  sinks:\n    o: {{ type: jsonl, config: {{ path: {} }} }}\n  nodes:\n    src: {{ kind: source, ref: s }}\n    w: {{ kind: sink, ref: o }}\n  edges:\n    - {{ from: src, to: w }}\n",
            csv.display(),
            out.display()
        )
    }

    #[tokio::test]
    async fn scaffold_config_emits_yaml() {
        let out = call_tool(
            &ctx(false),
            "scaffold_config",
            &json!({"source":"csv","sink":"jsonl","name":"demo"}),
        )
        .await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("name: demo"));
        assert!(text.contains("type: csv"));
        assert!(text.contains("type: jsonl"));
    }

    #[tokio::test]
    async fn scaffold_config_missing_arg_errors() {
        let out = call_tool(&ctx(false), "scaffold_config", &json!({"source":"csv"})).await;
        assert_eq!(out["isError"], true);
        assert!(out["content"][0]["text"].as_str().unwrap().contains("sink"));
    }

    #[tokio::test]
    async fn validate_config_matrix_ok() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(false),
            "validate_config",
            &json!({ "config": csv_config(dir.path()) }),
        )
        .await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"mode\": \"matrix\""));
        assert!(text.contains("\"valid\": true"));
    }

    #[tokio::test]
    async fn validate_config_topology_ok() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(false),
            "validate_config",
            &json!({ "config": topology_config(dir.path()) }),
        )
        .await;
        assert_eq!(out["isError"], false);
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("\"mode\": \"topology\"")
        );
    }

    #[tokio::test]
    async fn validate_config_bad_yaml_errors() {
        let out = call_tool(
            &ctx(false),
            "validate_config",
            &json!({ "config": "this: is: not: valid: yaml:" }),
        )
        .await;
        assert_eq!(out["isError"], true);
    }

    #[tokio::test]
    async fn preview_matrix_returns_records() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(false),
            "preview",
            &json!({ "config": csv_config(dir.path()), "limit": 1 }),
        )
        .await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("\"count\": 1"));
        assert!(text.contains("alice"));
    }

    #[tokio::test]
    async fn preview_topology_returns_sources() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(false),
            "preview",
            &json!({ "config": topology_config(dir.path()) }),
        )
        .await;
        assert_eq!(out["isError"], false);
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("\"sources\"")
        );
    }

    #[tokio::test]
    async fn run_pipeline_dry_run_validates_and_previews() {
        let dir = tempfile::tempdir().unwrap();
        let out = call_tool(
            &ctx(true),
            "run_pipeline",
            &json!({ "config": csv_config(dir.path()), "dry_run": true }),
        )
        .await;
        assert_eq!(out["isError"], false);
        let text = out["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("-- preview --"));
    }

    #[tokio::test]
    async fn run_pipeline_real_writes_sink() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = csv_config(dir.path());
        let out = call_tool(&ctx(true), "run_pipeline", &json!({ "config": cfg })).await;
        assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
        assert!(
            out["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains("\"records_written\": 2")
        );
        assert_eq!(
            std::fs::read_to_string(dir.path().join("out.jsonl"))
                .unwrap()
                .lines()
                .count(),
            2
        );
    }

    #[tokio::test]
    async fn get_connector_schema_transform_ok() {
        let out = call_tool(
            &ctx(false),
            "get_connector_schema",
            &json!({"kind":"transform","name":"keys_case"}),
        )
        .await;
        assert_eq!(out["isError"], false);
    }

    #[tokio::test]
    async fn get_connector_schema_bad_kind_errors() {
        let out = call_tool(
            &ctx(false),
            "get_connector_schema",
            &json!({"kind":"weird","name":"x"}),
        )
        .await;
        assert_eq!(out["isError"], true);
    }

    #[tokio::test]
    async fn validate_config_accepts_a_parameterized_config() {
        // Read-only introspection binds required params to placeholders, so a
        // template-shaped config still validates (#444).
        let dir = tempfile::tempdir().unwrap();
        let cfg = format!(
            "version: 1\nname: t\nparams:\n  tag: {{ required: true }}\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
            dir.path().join("in-${param.tag}.csv").display(),
            dir.path().join("out.jsonl").display()
        );
        let out = call_tool(&ctx(false), "validate_config", &json!({ "config": cfg })).await;
        assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
    }

    // ── Pipeline template tools (#444) ──────────────────────────────────────

    #[cfg(feature = "templates")]
    mod templates {
        use super::*;
        use std::sync::Arc;
        use std::time::Duration;

        fn tpl_ctx(allow: bool) -> McpContext {
            let store = Arc::new(crate::serve::history::memory::MemoryHistory::new(
                Duration::from_secs(60),
            )) as crate::templates::TemplateStore;
            McpContext::new(
                crate::auth_catalog::build_auth_catalog(None).unwrap(),
                allow,
            )
            .with_templates(store)
        }

        fn body(dir: &std::path::Path) -> String {
            let csv = dir.join("in.csv");
            std::fs::write(&csv, "id,name\n1,alice\n2,bob\n").unwrap();
            format!(
                "version: 1\nname: mcp-tpl\nparams:\n  tag: {{ required: true }}\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
                csv.display(),
                dir.join("out-${param.tag}.jsonl").display()
            )
        }

        #[tokio::test]
        async fn tools_are_hidden_without_a_store() {
            let names: Vec<&str> = tool_defs(&ctx(true)).iter().map(|t| t.name).collect();
            assert!(!names.contains(&"list_templates"), "{names:?}");
            assert!(!names.contains(&"register_template"), "{names:?}");
            // Calling one anyway is a clear tool error, not a panic.
            let out = call_tool(&ctx(true), "list_templates", &json!({})).await;
            assert_eq!(out["isError"], true);
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("--template-store")
            );
        }

        #[tokio::test]
        async fn read_tools_are_ungated_and_write_tools_are_gated() {
            let ro: Vec<&str> = tool_defs(&tpl_ctx(false)).iter().map(|t| t.name).collect();
            assert!(ro.contains(&"list_templates"), "{ro:?}");
            assert!(ro.contains(&"get_template"), "{ro:?}");
            assert!(!ro.contains(&"register_template"), "{ro:?}");
            assert!(!ro.contains(&"run_template"), "{ro:?}");

            let rw: Vec<&str> = tool_defs(&tpl_ctx(true)).iter().map(|t| t.name).collect();
            assert!(rw.contains(&"register_template"), "{rw:?}");
            assert!(rw.contains(&"run_template"), "{rw:?}");

            for tool in ["register_template", "run_template"] {
                let out = call_tool(&tpl_ctx(false), tool, &json!({"id":"x","config":"y"})).await;
                assert_eq!(out["isError"], true, "{tool} must be gated");
                assert!(
                    out["content"][0]["text"]
                        .as_str()
                        .unwrap()
                        .contains("--allow-mutations")
                );
            }
        }

        #[tokio::test]
        async fn register_list_get_and_run_round_trip() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);

            let out = call_tool(
                &ctx,
                "register_template",
                &json!({ "config": body(dir.path()) }),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("mcp-tpl"), "{text}");

            let out = call_tool(&ctx, "list_templates", &json!({})).await;
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"count\": 1"), "{text}");

            let out = call_tool(&ctx, "get_template", &json!({"id":"mcp-tpl"})).await;
            assert_eq!(out["isError"], false);
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"versions\""), "{text}");
            assert!(text.contains("${param.tag}"), "body is verbatim: {text}");

            let out = call_tool(&ctx, "get_template", &json!({"id":"nope"})).await;
            assert_eq!(out["isError"], true);

            // A missing required param is a tool error naming it.
            let out = call_tool(&ctx, "run_template", &json!({"id":"mcp-tpl"})).await;
            assert_eq!(out["isError"], true);
            assert!(out["content"][0]["text"].as_str().unwrap().contains("tag"));

            // dry_run materializes + validates without writing.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-tpl","params":{"tag":"dry"},"dry_run":true}),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"dry_run\": true"), "{text}");
            assert!(!dir.path().join("out-dry.jsonl").exists());

            // The real run writes through the ordinary pipeline path.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-tpl","params":{"tag":"real"}}),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"records_written\": 2"), "{text}");
            assert_eq!(
                std::fs::read_to_string(dir.path().join("out-real.jsonl"))
                    .unwrap()
                    .lines()
                    .count(),
                2
            );
        }

        #[tokio::test]
        async fn register_with_tags_and_run_by_channel() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);

            // Register v1 with `dev`, then v2 untagged, so `dev` and `latest`
            // point at different versions.
            let out = call_tool(
                &ctx,
                "register_template",
                &json!({ "config": body(dir.path()), "tags": ["dev"] }),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            call_tool(
                &ctx,
                "register_template",
                &json!({ "config": body(dir.path()) }),
            )
            .await;

            let out = call_tool(&ctx, "get_template", &json!({"id":"mcp-tpl"})).await;
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"latest_version\": 2"), "{text}");
            assert!(text.contains("\"dev\": 1"), "{text}");

            // Running by channel picks v1; by `latest` (omitted) picks v2.
            for (version, want) in [(json!("dev"), 1), (json!("latest"), 2), (json!(1), 1)] {
                let out = call_tool(
                    &ctx,
                    "run_template",
                    &json!({"id":"mcp-tpl","params":{"tag":"c"},"version":version,"dry_run":true}),
                )
                .await;
                assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
                let text = out["content"][0]["text"].as_str().unwrap();
                assert!(
                    text.contains(&format!("\"template_version\": {want}")),
                    "{version} should resolve to v{want}: {text}"
                );
            }

            // A channel outside the closed set is a tool error, on both paths.
            for args in [
                json!({ "config": body(dir.path()), "tags": ["prd"] }),
                json!({ "config": body(dir.path()), "tags": ["latest"] }),
            ] {
                let out = call_tool(&ctx, "register_template", &args).await;
                assert_eq!(out["isError"], true, "{args}");
            }
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-tpl","params":{"tag":"c"},"version":"nope"}),
            )
            .await;
            assert_eq!(out["isError"], true);
            // An unset channel is refused rather than falling back to latest.
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-tpl","params":{"tag":"c"},"version":"canary"}),
            )
            .await;
            assert_eq!(out["isError"], true);
            assert!(
                out["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("no `canary` version")
            );
        }

        #[tokio::test]
        async fn secret_params_are_redacted_in_the_response() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);
            let csv = dir.path().join("in.csv");
            std::fs::write(&csv, "id\n1\n").unwrap();
            let cfg = format!(
                "version: 1\nname: mcp-secret\nparams:\n  token: {{ required: true, secret: true }}\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}\n",
                csv.display(),
                dir.path().join("out.jsonl").display()
            );
            call_tool(&ctx, "register_template", &json!({ "config": cfg })).await;
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-secret","params":{"token":"top-secret-token-value"},"dry_run":true}),
            )
            .await;
            let text = out["content"][0]["text"].as_str().unwrap();
            assert!(text.contains("\"***\""), "{text}");
            assert!(!text.contains("top-secret-token-value"), "leaked: {text}");
        }

        #[tokio::test]
        async fn register_rejects_an_invalid_config() {
            let out = call_tool(
                &tpl_ctx(true),
                "register_template",
                &json!({ "config": "version: 1\nname: x\nbogus: 1\npipeline: {}\n" }),
            )
            .await;
            assert_eq!(out["isError"], true);
        }

        #[tokio::test]
        async fn env_overrides_flow_through_run_template() {
            let dir = tempfile::tempdir().unwrap();
            let ctx = tpl_ctx(true);
            let csv = dir.path().join("in.csv");
            std::fs::write(&csv, "id\n1\n").unwrap();
            let cfg = format!(
                "version: 1\nname: mcp-env\npipeline:\n  source:\n    type: csv\n    config:\n      path: {}\n  sink:\n    type: jsonl\n    config:\n      path: {}/out-${{env:MCP_TPL_SUFFIX}}.jsonl\n",
                csv.display(),
                dir.path().display()
            );
            call_tool(&ctx, "register_template", &json!({ "config": cfg })).await;
            let out = call_tool(
                &ctx,
                "run_template",
                &json!({"id":"mcp-env","env":{"MCP_TPL_SUFFIX":"eu"}}),
            )
            .await;
            assert_eq!(out["isError"], false, "{}", out["content"][0]["text"]);
            assert!(dir.path().join("out-eu.jsonl").exists());
        }
    }
}
