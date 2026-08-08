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
use chrono::{DateTime, FixedOffset};
use faucet_core::stage::compile_stage;
use faucet_core::topology::{
    JoinConfig, JoinNode, NodeKind, Topology, TopologyGovernance, TopologyOnError, TopologyOptions,
};
use serde_json::Value;
use std::collections::HashMap;
use tokio_util::sync::CancellationToken;

/// Whether the config selects topology mode (a non-empty `pipeline.nodes`).
pub fn is_topology(cfg: &PipelineConfig) -> bool {
    !cfg.pipeline.nodes.is_empty()
}

/// Per-run knobs for topology mode, mirroring the matrix path's
/// [`crate::executor::ExecuteOptions`] subset that applies to a node graph.
#[derive(Default, Clone)]
pub struct TopologyRunOptions {
    /// External cancellation (serve run-cancel / timeout / shutdown, TUI quit).
    pub cancel: Option<CancellationToken>,
    /// Preview: build no real sinks, count records instead, and never persist a
    /// bookmark (#456 C2).
    pub dry_run: bool,
    /// Preview: stop after this many records per sink, and never persist a
    /// bookmark (#456 C2).
    pub limit: Option<usize>,
    /// Clock backing `${now.*}` in node configs. `None` = process start.
    pub clock: Option<DateTime<FixedOffset>>,
}

impl TopologyRunOptions {
    /// The effective `${now.*}` clock.
    fn clock(&self) -> DateTime<FixedOffset> {
        self.clock
            .unwrap_or_else(|| chrono::Utc::now().fixed_offset())
    }

    /// Whether this is a non-writing preview, which must not persist bookmarks.
    fn is_preview(&self) -> bool {
        self.dry_run || self.limit.is_some()
    }
}

/// Config blocks that topology mode does not (yet) act on.
///
/// Returned as `(block, consequence)` pairs so both `faucet validate` and the run
/// path can say exactly what is inert and why it matters. Silence here was the
/// original defect: a config could declare a policy, validate as "valid", and run
/// with the policy doing nothing (#456 M2).
pub fn inert_blocks(cfg: &PipelineConfig) -> Vec<(&'static str, &'static str)> {
    let mut out = Vec::new();
    #[cfg(feature = "notify")]
    if !cfg.notifications.is_empty() {
        out.push((
            "notifications",
            "no alert is sent when the run fails or breaches an SLA",
        ));
    }
    #[cfg(feature = "lineage")]
    if cfg.lineage.is_some() {
        out.push(("lineage", "no OpenLineage events are emitted"));
    }
    #[cfg(feature = "catalog")]
    if cfg.catalog.is_some() {
        out.push((
            "catalog",
            "no datasets, schemas, or lineage edges are recorded",
        ));
    }
    if cfg.sla.is_some() {
        out.push((
            "sla",
            "freshness and volume are not evaluated after the run",
        ));
    }
    out
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
    // Exactly-once in a node graph (#458). Each sink node commits under its own
    // scope, so the requirements are the matrix ones applied per node — plus a
    // single-source restriction, because nothing records which source a given
    // sink's bookmark came from. Checked here, at config-load time, so an
    // unsupported combination never runs *as if* it were exactly-once (#456 H2).
    if cfg.delivery == faucet_core::DeliveryMode::ExactlyOnce {
        validate_exactly_once(cfg)?;
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

/// The connector kind a source/sink node resolves to, without building anything.
///
/// Mirrors [`resolve_connector`]'s kind precedence (inline `type` override, else
/// the referenced template, else the legacy singular block) so the gate below and
/// the builder can never disagree about what a node *is*.
fn resolved_node_kind(cfg: &PipelineConfig, node: &NodeSpec) -> Option<String> {
    let (template, kind, templates, legacy) = match node {
        NodeSpec::Source { template, kind, .. } => {
            (template, kind, &cfg.pipeline.sources, &cfg.pipeline.source)
        }
        NodeSpec::Sink { template, kind, .. } => {
            (template, kind, &cfg.pipeline.sinks, &cfg.pipeline.sink)
        }
        _ => return None,
    };
    if let Some(k) = kind {
        return Some(k.clone());
    }
    let name = template.as_deref().unwrap_or("default");
    templates
        .get(name)
        .or(if name == "default" {
            legacy.as_ref()
        } else {
            None
        })
        .map(|t| t.kind.clone())
}

/// The four atomic-watermark requirements, per node, plus the single-source rule.
///
/// Ordered so the message names the *limiting* side, and suggests the keyed-upsert
/// alternative when the sinks could do it — the same shape as the matrix gate in
/// `expand`, so an operator moving a pipeline between the two forms reads the same
/// diagnosis.
fn validate_exactly_once(cfg: &PipelineConfig) -> CliResult<()> {
    let nodes = &cfg.pipeline.nodes;
    let sources: Vec<(&String, String)> = nodes
        .iter()
        .filter(|(_, n)| matches!(n, NodeSpec::Source { .. }))
        .map(|(id, n)| (id, resolved_node_kind(cfg, n).unwrap_or_default()))
        .collect();
    let sinks: Vec<(&String, String)> = nodes
        .iter()
        .filter(|(_, n)| matches!(n, NodeSpec::Sink { .. }))
        .map(|(id, n)| (id, resolved_node_kind(cfg, n).unwrap_or_default()))
        .collect();

    // 1. One source. A sink's bookmark records the position of whichever source
    //    fed its pages, and the graph does not record which one — so with several
    //    sources there is no sound resume point to anchor the watermark against.
    if sources.len() != 1 {
        return Err(CliError::Config(format!(
            "`delivery: exactly_once` needs exactly one source node; this graph has {}. A \
             sink's commit watermark is only meaningful against a known source position, and \
             nothing records which source fed a given page. Split the graph into one pipeline \
             per source, or use `write_mode: upsert` + `key` on the sinks for keyed-upsert \
             effectively-once with any number of sources",
            sources.len()
        )));
    }
    // 2. The source must replay deterministically.
    let (src_id, src_kind) = &sources[0];
    if !crate::registry::source_supports_exactly_once(src_kind) {
        return Err(CliError::Config(format!(
            "node '{src_id}': `delivery: exactly_once` is not supported by source '{src_kind}' \
             (deterministic-replay sources only: {})",
            crate::registry::EXACTLY_ONCE_SOURCE_KINDS.join(", ")
        )));
    }
    // 3. Every sink must commit data + token atomically.
    for (id, kind) in &sinks {
        if !crate::registry::sink_supports_idempotent_writes(kind) {
            return Err(CliError::Config(format!(
                "node '{id}': `delivery: exactly_once` is not supported by sink '{kind}' \
                 (sinks that commit a watermark atomically: {}). Every sink node must qualify — \
                 each one keeps its own watermark",
                crate::registry::IDEMPOTENT_SINK_KINDS.join(", ")
            )));
        }
    }
    // 4. Durable state — the per-node sequence has to survive a restart.
    match cfg.pipeline.state.as_ref() {
        None => {
            return Err(CliError::Config(
                "`delivery: exactly_once` requires a durable `state:` block: each sink node \
                 persists its commit sequence there, and without it every restart would \
                 re-commit from zero"
                    .into(),
            ));
        }
        Some(state) if state.kind == "memory" => {
            return Err(CliError::Config(
                "`delivery: exactly_once` requires a durable `state:` block, and `memory` does \
                 not survive the process. Use `file`, `redis`, or `postgres`"
                    .into(),
            ));
        }
        Some(_) => {}
    }
    // 5. No DLQ — routing a row aside breaks the all-or-nothing page commit.
    if cfg.pipeline.dlq.is_some() {
        return Err(CliError::Config(
            "`delivery: exactly_once` is incompatible with a `dlq:` block in this version: a \
             page's rows and its commit token are written as one unit, so a partial page \
             cannot be split off to a dead-letter queue"
                .into(),
        ));
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
/// `edges` block, with default run options (no preview, process-start clock).
pub async fn build_topology(cfg: &PipelineConfig, auth: &AuthCatalog) -> CliResult<Topology> {
    build_topology_with(cfg, auth, &TopologyRunOptions::default()).await
}

/// Build a [`faucet_core::Topology`], honouring the run options.
///
/// Two things happen here that the matrix path does per invocation in
/// [`crate::executor`], and that topology mode used to skip entirely:
///
/// - **`${now.*}` is resolved** in every node's source/sink config, and a
///   leftover `${backfill.*}` token is rejected. Without this the literal token
///   string reached the connector, so a dated path became a directory named
///   `${now.date}` (#456 H4).
/// - **Preview modes wrap the sinks**: `--dry-run` swaps in a counting sink and
///   `--limit` truncates, so neither performs a real write (#456 C2).
pub async fn build_topology_with(
    cfg: &PipelineConfig,
    auth: &AuthCatalog,
    opts: &TopologyRunOptions,
) -> CliResult<Topology> {
    // Cheap graph checks first, so a wiring typo never costs a connector build.
    validate_topology_spec(cfg)?;

    let clock = opts.clock();
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
                let (k, mut c) = resolve_connector(
                    &spec.sources,
                    &spec.source,
                    template.as_deref(),
                    kind.as_deref(),
                    config.as_ref(),
                    id,
                    "source",
                )?;
                crate::executor::resolve_now_inplace(&mut c, clock)?;
                crate::executor::reject_unresolved_backfill_tokens(&c, "source")?;
                NodeKind::Source(build_source(&k, c, auth, None).await?)
            }
            NodeSpec::Sink {
                template,
                kind,
                config,
            } => {
                let (k, mut c) = resolve_connector(
                    &spec.sinks,
                    &spec.sink,
                    template.as_deref(),
                    kind.as_deref(),
                    config.as_ref(),
                    id,
                    "sink",
                )?;
                crate::executor::resolve_now_inplace(&mut c, clock)?;
                crate::executor::reject_unresolved_backfill_tokens(&c, "sink")?;
                // Preview modes must never reach the real destination.
                let sink: Box<dyn faucet_core::Sink> = if opts.dry_run {
                    Box::new(crate::executor::CountingSink::new())
                } else {
                    build_sink(&k, c, auth).await?
                };
                let sink = match opts.limit {
                    Some(n) => Box::new(crate::executor::LimitedSink::wrap(sink, n)) as Box<_>,
                    None => sink,
                };
                NodeKind::Sink(sink)
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

/// Compile the config's governance blocks for a node graph.
///
/// Mirrors the matrix path in [`crate::executor`] so a topology enforces the same
/// policies — before this existed, a config declaring `masking:` ran with no
/// masking at all and PII reached every destination in the clear (#456 C3).
///
/// Masking is destination-scoped, so it is compiled **per sink node** against
/// that node's identifiers (node id, template ref, connector kind) — any of which
/// an `applies_to` rule may name. A sink for which no rule applies gets no entry,
/// so the pass is skipped entirely for it.
fn build_governance(cfg: &PipelineConfig) -> CliResult<TopologyGovernance> {
    #[allow(unused_mut)]
    let mut g = TopologyGovernance::new();

    #[cfg(feature = "quality")]
    if let Some(spec) = &cfg.pipeline.quality {
        g.quality = Some(std::sync::Arc::new(
            faucet_core::CompiledQuality::compile(spec)
                .map_err(|e| CliError::Config(format!("quality: {e}")))?,
        ));
    }
    #[cfg(feature = "contract")]
    if let Some(spec) = &cfg.pipeline.contract {
        g.contract = Some(std::sync::Arc::new(
            faucet_core::CompiledContract::compile(spec)
                .map_err(|e| CliError::Config(format!("contract: {e}")))?,
        ));
    }
    if let Some(spec) = &cfg.pipeline.schema {
        g.schema_drift = Some(faucet_core::SchemaDriftPolicy::compile(spec));
    }
    if let Some(spec) = &cfg.resilience {
        g.resilience = Some(spec.to_policy()?);
    }
    // Delivery guarantee (#458). `validate_topology_spec` has already checked the
    // per-node requirements, so by here `exactly_once` is known to be supportable.
    g.delivery = cfg.delivery;

    #[cfg(feature = "masking")]
    if let Some(spec) = &cfg.pipeline.masking {
        for (node_id, node) in &cfg.pipeline.nodes {
            let NodeSpec::Sink { template, kind, .. } = node else {
                continue;
            };
            let template_ref = template.as_deref().unwrap_or("default");
            // The node's own kind override, else the template's declared kind.
            let resolved_kind = kind.clone().or_else(|| {
                cfg.pipeline
                    .sinks
                    .get(template_ref)
                    .or(cfg.pipeline.sink.as_ref())
                    .map(|t| t.kind.clone())
            });
            let mut ids: Vec<&str> = vec![node_id.as_str(), template_ref];
            if let Some(k) = resolved_kind.as_deref() {
                ids.push(k);
            }
            let compiled = faucet_core::CompiledMasking::compile_for_sink(spec, &ids)
                .map_err(|e| CliError::Config(format!("masking: {e}")))?;
            if !compiled.is_empty() {
                g.masking_by_sink
                    .insert(node_id.clone(), std::sync::Arc::new(compiled));
            }
        }
    }
    Ok(g)
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
    run: TopologyRunOptions,
) -> CliResult<RunSummary> {
    for (block, consequence) in inert_blocks(cfg) {
        tracing::warn!(
            block,
            "`{block}:` is not applied in topology mode (`pipeline.nodes`) — {consequence}"
        );
    }

    let topo = build_topology_with(cfg, auth, &run).await?;

    let pipeline_name = cfg.name.clone().unwrap_or_else(|| "unnamed".to_string());
    let run_id = uuid::Uuid::now_v7().to_string();

    let on_error = match cfg.execution.as_ref().map(|e| e.on_error) {
        Some(crate::config::OnError::Stop) => TopologyOnError::Propagate,
        _ => TopologyOnError::Continue,
    };

    let mut opts = TopologyOptions::new(pipeline_name.clone()).with_on_error(on_error);
    opts.run_id = run_id;

    if let Some(state) = &cfg.pipeline.state {
        let store = crate::state::build_state_store(state).await?;
        // A preview must not advance a durable bookmark: the counting/truncating
        // sinks return `Ok` without a real write, so a persisted bookmark would
        // make the next real run resume past records nobody wrote (#456 C2,
        // mirroring #321 H1 on the matrix path). Reads still pass through.
        let store = if run.is_preview() {
            std::sync::Arc::new(crate::executor::ReadOnlyStateStore { inner: store })
                as std::sync::Arc<dyn faucet_core::StateStore>
        } else {
            store
        };
        opts = opts.with_state_store(store);
    }
    if let Some(dlq) = &cfg.pipeline.dlq {
        opts = opts.with_dlq(crate::executor::build_dlq_config(dlq).await?);
    }
    if let Some(c) = run.cancel.clone() {
        opts = opts.with_cancel(c);
    }

    // `run_reported` rather than `run_with`: the post-run pass below emits one
    // notification and evaluates one SLA per **sink node**, which needs to know
    // which node failed (#459).
    let state_store = opts.state_store.clone();
    let cancelled = run.cancel.as_ref().is_some_and(|c| c.is_cancelled());
    let reported = topo.run_reported(opts, build_governance(cfg)?).await?;

    // Per-sink-node observability. A sink node is a topology's analogue of a
    // matrix invocation — it owns a state key, a bookmark, and a record count —
    // so the SLA and notification passes key off it, reusing the same standalone
    // functions the executor calls rather than a parallel implementation.
    if !run.is_preview() && !cancelled {
        post_run_observability(cfg, &pipeline_name, &reported, state_store.as_ref()).await;
    }

    let mut invocations: Vec<InvocationOutcome> = reported
        .result
        .per_sink
        .iter()
        .map(|(node_id, records)| InvocationOutcome {
            row_id: node_id.clone(),
            parent_record_key: None,
            records_written: *records,
            error: None,
            metrics: None,
        })
        .collect();
    invocations.sort_by(|a, b| a.row_id.cmp(&b.row_id));

    // Failures, attributed to the node that produced them instead of a flat
    // "topology" row (#459).
    for n in reported.nodes.iter().filter(|n| n.error.is_some()) {
        invocations.push(InvocationOutcome {
            row_id: n.node_id.clone(),
            parent_record_key: None,
            records_written: 0,
            error: n.error.clone(),
            metrics: None,
        });
    }

    Ok(RunSummary { invocations })
}

/// Freshness/volume SLAs and notifications, once per sink node.
///
/// Deliberately a thin adapter: `sla::evaluate_post_run` and the `NotifyEvent`
/// constructors are already standalone, so topology mode calls exactly what the
/// matrix executor calls. Neither can fail a run — an SLA violation is a signal
/// and a notification is best-effort — so this returns nothing.
async fn post_run_observability(
    cfg: &PipelineConfig,
    pipeline_name: &str,
    reported: &faucet_core::topology::TopologyRun,
    state_store: Option<&std::sync::Arc<dyn faucet_core::StateStore>>,
) {
    #[cfg(feature = "notify")]
    let notifier = match crate::notify::Notifier::from_specs(&cfg.notifications) {
        Ok(n) => n,
        Err(e) => {
            // A malformed block is a config error, but it must not fail a run that
            // has already written its data.
            tracing::error!(error = %e, "notifications config invalid; not notifying");
            None
        }
    };
    let now = chrono::Utc::now().timestamp();

    for node in reported.nodes.iter().filter(|n| n.kind == "sink") {
        let row = node.node_id.as_str();

        // ── SLA (#202) ───────────────────────────────────────────────────────
        let violations = match cfg.sla.as_ref() {
            Some(spec) => {
                let base_key = format!("{pipeline_name}::{row}");
                let outcome = match &node.error {
                    None => crate::sla::RunOutcome::Success {
                        rows: node.records as u64,
                    },
                    Some(_) => crate::sla::RunOutcome::Failure,
                };
                let v = crate::sla::evaluate_post_run(
                    spec,
                    state_store,
                    &base_key,
                    pipeline_name,
                    row,
                    outcome,
                    now,
                )
                .await;
                for violation in &v {
                    tracing::warn!(node = %row, kind = violation.kind(), "SLA violation: {violation}");
                }
                v
            }
            None => Vec::new(),
        };

        // ── Notifications (#280) ─────────────────────────────────────────────
        #[cfg(feature = "notify")]
        if let Some(notifier) = &notifier {
            use crate::notify::NotifyEvent;
            match &node.error {
                None => {
                    notifier
                        .emit(NotifyEvent::run_success(
                            pipeline_name,
                            row,
                            node.records as u64,
                        ))
                        .await;
                }
                Some(msg) => {
                    notifier
                        .emit(NotifyEvent::run_failure(pipeline_name, row, "sink", msg))
                        .await;
                }
            }
            for v in &violations {
                notifier
                    .emit(NotifyEvent::sla_breach(
                        pipeline_name,
                        row,
                        v.kind(),
                        v.to_string(),
                    ))
                    .await;
            }
        }
        #[cfg(not(feature = "notify"))]
        let _ = &violations;
    }
}
