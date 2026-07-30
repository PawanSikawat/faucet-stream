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
use std::path::Path;

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

/// Parse an inline config document. Tries YAML then JSON via `from_text`.
fn parse_config(text: &str) -> Result<PipelineConfig, String> {
    // `from_text` picks the parser from the path extension; give it a `.yaml`
    // path (YAML is a JSON superset, so a JSON document also parses).
    PipelineConfig::from_text(text, Path::new("mcp-inline.yaml")).map_err(|e| e.to_string())
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
}
