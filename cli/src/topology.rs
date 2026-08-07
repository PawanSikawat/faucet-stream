//! Topology mode (issues #71 / #72): build and run a
//! [`faucet_core::Topology`] from a config's `pipeline.nodes` / `edges` block.
//!
//! When `pipeline.nodes` is non-empty the pipeline runs as an explicit node
//! graph rather than a matrix. This module resolves each node's connector
//! templates, compiles its transforms, wires the edges, and drives the core
//! topology executor — reusing [`crate::registry`] for connector construction
//! and [`crate::state`] / [`crate::executor::build_dlq_config`] for the
//! sink-side plumbing.

use crate::auth_catalog::AuthCatalog;
use crate::config::{ConnectorSpec, NodeSpec, PipelineConfig};
use crate::error::{CliError, CliResult};
use crate::executor::{InvocationOutcome, RunSummary};
use crate::merge::merge_value;
use crate::registry::{build_sink, build_source};
use crate::transforms::compile_transforms;
use faucet_core::stage::compile_stage;
use faucet_core::topology::{
    JoinConfig, JoinNode, NodeKind, Topology, TopologyOnError, TopologyOptions,
};
use serde_json::Value;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

/// Whether the config selects topology mode (a non-empty `pipeline.nodes`).
pub fn is_topology(cfg: &PipelineConfig) -> bool {
    !cfg.pipeline.nodes.is_empty()
}

/// Config-level graph validation: the checks that need only the `nodes:` /
/// `edges:` spec, no connectors. Run as a fail-fast prelude to
/// [`build_topology`] (so a wiring typo is reported before any client is
/// constructed) and standalone by the template registry, which validates a
/// config it must not build connectors for (#444).
///
/// Node **arity** (a tee's fan-out, a join's labelled inputs, …) is validated by
/// [`faucet_core::topology::Topology::validate`] once the graph is built —
/// deliberately not re-implemented here, so the two can never disagree.
pub fn validate_topology_spec(cfg: &PipelineConfig) -> CliResult<()> {
    if !cfg.matrix.is_empty() {
        return Err(CliError::MatrixAndNodesBothPresent);
    }
    let spec = &cfg.pipeline;
    let mut known: Vec<String> = spec.nodes.keys().cloned().collect();
    known.sort_unstable();
    for edge in &spec.edges {
        for endpoint in [&edge.from, &edge.to] {
            if !spec.nodes.contains_key(endpoint) {
                return Err(CliError::EdgeEndpointMissing {
                    name: endpoint.clone(),
                    known: known.clone(),
                });
            }
        }
    }
    Ok(())
}

/// Resolve one source/sink node's template `ref` + inline overrides into a
/// concrete `(kind, config)` pair.
fn resolve_connector(
    templates: &HashMap<String, ConnectorSpec>,
    legacy: &Option<ConnectorSpec>,
    template_ref: Option<&str>,
    kind_override: Option<&str>,
    config_override: Option<&Value>,
    node_id: &str,
    kind_label: &'static str,
) -> CliResult<(String, Value)> {
    let name = template_ref.unwrap_or("default");
    let base: ConnectorSpec = if name == "default" {
        templates
            .get("default")
            .cloned()
            .or_else(|| legacy.clone())
            .ok_or(CliError::MissingTemplate {
                kind: kind_label,
                row_id: node_id.to_string(),
            })?
    } else {
        templates
            .get(name)
            .cloned()
            .ok_or_else(|| CliError::UnknownTemplate {
                kind: kind_label,
                name: name.to_string(),
                row_id: node_id.to_string(),
                known: {
                    let mut k: Vec<String> = templates.keys().cloned().collect();
                    if legacy.is_some() {
                        k.push("default".to_string());
                    }
                    k.sort();
                    k
                },
            })?
    };
    let mut kind = base.kind;
    let mut config = base.config;
    if let Some(k) = kind_override {
        kind = k.to_string();
    }
    if let Some(c) = config_override {
        merge_value(&mut config, c.clone());
    }
    Ok((kind, config))
}

/// Build a [`faucet_core::Topology`] from the config's `pipeline.nodes` /
/// `edges` block.
pub async fn build_topology(cfg: &PipelineConfig, auth: &AuthCatalog) -> CliResult<Topology> {
    // Cheap graph checks first, so a wiring typo never costs a connector build.
    validate_topology_spec(cfg)?;

    let spec = &cfg.pipeline;
    let mut builder = Topology::builder();

    // Deterministic node order (sorted by id) so errors/logs are stable.
    let mut node_ids: Vec<&String> = spec.nodes.keys().collect();
    node_ids.sort();

    for id in &node_ids {
        let node = &spec.nodes[*id];
        let kind: NodeKind = match node {
            NodeSpec::Source {
                template,
                kind,
                config,
            } => {
                let (k, c) = resolve_connector(
                    &spec.sources,
                    &spec.source,
                    template.as_deref(),
                    kind.as_deref(),
                    config.as_ref(),
                    id,
                    "source",
                )?;
                NodeKind::Source(build_source(&k, c, auth, None).await?)
            }
            NodeSpec::Sink {
                template,
                kind,
                config,
            } => {
                let (k, c) = resolve_connector(
                    &spec.sinks,
                    &spec.sink,
                    template.as_deref(),
                    kind.as_deref(),
                    config.as_ref(),
                    id,
                    "sink",
                )?;
                NodeKind::Sink(build_sink(&k, c, auth).await?)
            }
            NodeSpec::Transform { transforms } => {
                let stages = compile_transforms(transforms)?;
                let compiled = stages
                    .iter()
                    .map(compile_stage)
                    .collect::<Result<Vec<_>, _>>()?;
                NodeKind::Transform(compiled)
            }
            NodeSpec::Tee {
                channel_capacity,
                fanout,
            } => NodeKind::Tee {
                capacity: *channel_capacity,
                fanout: *fanout,
            },
            NodeSpec::Merge => NodeKind::Merge,
            NodeSpec::Join(js) => NodeKind::Join(JoinNode {
                config: JoinConfig {
                    mode: js.mode,
                    build_key: js.build.key.clone(),
                    probe_key: js.probe.key.clone(),
                    projections: js.project.clone(),
                    on_missing: js.on_missing.clone(),
                    on_duplicate: js.on_duplicate,
                    on_collision: js.on_collision,
                    key_normalize: js.key_normalize,
                    max_build_records: js.max_build_records,
                },
                build_edge: js.build.edge.clone(),
                probe_edge: js.probe.edge.clone(),
            }),
        };
        builder = builder.node((*id).clone(), kind);
    }

    // Edge endpoints were already validated by `validate_topology_spec` above —
    // before any connector was constructed — so just wire them.
    for e in &spec.edges {
        builder = match &e.label {
            Some(label) => builder.labelled_edge(e.from.clone(), e.to.clone(), label.clone()),
            None => builder.edge(e.from.clone(), e.to.clone()),
        };
    }

    builder.build().map_err(|e| CliError::InvalidTopology {
        message: e.to_string(),
    })
}

/// Collect a bounded preview of each `source` node's records (source side
/// only; downstream nodes are not run). Returns `(node_id, records)` per
/// source node, in sorted node-id order.
pub async fn preview_records(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    limit: usize,
) -> CliResult<Vec<(String, Vec<Value>)>> {
    if !cfg.matrix.is_empty() {
        return Err(CliError::MatrixAndNodesBothPresent);
    }
    let spec = &cfg.pipeline;
    let mut ids: Vec<&String> = spec.nodes.keys().collect();
    ids.sort();

    let mut out = Vec::new();
    for id in ids {
        if let NodeSpec::Source {
            template,
            kind,
            config,
        } = &spec.nodes[id]
        {
            let (k, c) = resolve_connector(
                &spec.sources,
                &spec.source,
                template.as_deref(),
                kind.as_deref(),
                config.as_ref(),
                id,
                "source",
            )?;
            let source = build_source(&k, c, auth, None).await?;
            let records = source.fetch_all().await?;
            out.push((
                id.clone(),
                records.into_iter().take(limit).collect::<Vec<_>>(),
            ));
        }
    }
    if out.is_empty() {
        return Err(CliError::InvalidTopology {
            message: "no source nodes to preview".to_string(),
        });
    }
    Ok(out)
}

/// Preview topology mode: build each `source` node and print the first
/// `limit` records per source to stdout as JSON Lines.
pub async fn preview(cfg: &PipelineConfig, auth: &AuthCatalog, limit: usize) -> CliResult<()> {
    for (id, records) in preview_records(cfg, auth, limit).await? {
        tracing::info!(node = %id, "previewing source node");
        for rec in records {
            println!("{}", serde_json::to_string(&rec).unwrap_or_default());
        }
    }
    Ok(())
}

/// Preview topology sources into a JSON string (for the MCP `preview` tool).
pub async fn preview_to_string(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    limit: usize,
) -> CliResult<String> {
    let sources = preview_records(cfg, auth, limit).await?;
    let doc: Vec<Value> = sources
        .into_iter()
        .map(|(id, records)| {
            serde_json::json!({ "node": id, "count": records.len(), "records": records })
        })
        .collect();
    Ok(
        serde_json::to_string_pretty(&serde_json::json!({ "sources": doc }))
            .unwrap_or_else(|_| "[]".to_string()),
    )
}

/// Build and run the topology, returning a [`RunSummary`] shaped like a matrix
/// run (one invocation per sink node, plus one per node failure under
/// `on_error: continue`).
pub async fn run_topology(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    cancel: Option<CancellationToken>,
) -> CliResult<RunSummary> {
    let topo = build_topology(cfg, auth).await?;

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| "unnamed".to_string());
    let run_id = uuid::Uuid::now_v7().to_string();

    let on_error = match cfg.execution.as_ref().map(|e| e.on_error) {
        Some(crate::config::OnError::Stop) => TopologyOnError::Propagate,
        _ => TopologyOnError::Continue,
    };

    let mut opts = TopologyOptions::new(pipeline_name).with_on_error(on_error);
    opts.run_id = run_id;

    if let Some(state) = &cfg.pipeline.state {
        opts = opts.with_state_store(crate::state::build_state_store(state).await?);
    }
    if let Some(dlq) = &cfg.pipeline.dlq {
        opts = opts.with_dlq(crate::executor::build_dlq_config(dlq).await?);
    }
    if let Some(c) = cancel {
        opts = opts.with_cancel(c);
    }

    let result = topo.run(opts).await?;

    let mut invocations: Vec<InvocationOutcome> = result
        .per_sink
        .into_iter()
        .map(|(node_id, records)| InvocationOutcome {
            row_id: node_id,
            parent_record_key: None,
            records_written: records,
            error: None,
            metrics: None,
        })
        .collect();
    invocations.sort_by(|a, b| a.row_id.cmp(&b.row_id));

    for msg in result.errors {
        invocations.push(InvocationOutcome {
            row_id: "topology".to_string(),
            parent_record_key: None,
            records_written: 0,
            error: Some(msg),
            metrics: None,
        });
    }

    Ok(RunSummary { invocations })
}
