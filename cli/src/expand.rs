//! Expand a parsed [`PipelineConfig`] into a flat list of [`ExpandedNode`]s
//! ready for the executor to run.
//!
//! Responsibilities:
//! - Assign synthetic ids to anonymous rows (`row-0`, `row-1`, …).
//! - Reject reserved row ids (`env`, `file`, `secret`, `matrix`, `pipeline`).
//! - Reject duplicate ids.
//! - Validate that every `parent:` references a known row id and that the
//!   parent chain has no cycles.
//! - Deep-merge each row's partial overrides into `pipeline.*`.
//! - Find every `${id.path}` token surviving from load-time interpolation and
//!   record where each one came from. Tokens that reference unknown ids
//!   produce a `CliError::UnknownInterpolationId` here, not at runtime.

use crate::config::{
    ConnectorSpec, MatrixRow, PartialConnector, PipelineConfig, PipelineSpec, StateStoreSpec,
    TransformSpec,
};
use crate::error::{CliError, CliResult};
use crate::interpolate::{Directive, iter_directives};
use crate::merge::merge_value;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};

/// Row ids that callers can never use because they collide with
/// load-time interpolation prefixes or future runtime scopes.
pub const RESERVED_IDS: &[&str] = &[
    "env",
    "file",
    "secret",
    "matrix",
    "pipeline",
    "now",
    "backfill",
    "param",
    "partition",
];

/// One fully-merged matrix row, ready for the executor.
#[derive(Debug, Clone)]
pub struct ExpandedNode {
    pub id: String,
    pub row_index: usize,
    pub role: NodeRole,
    pub source: ConnectorSpec,
    pub sink: ConnectorSpec,
    pub transforms: Vec<TransformSpec>,
    pub state: Option<StateStoreSpec>,
    /// Resolved DLQ spec for this row, or `None` if no DLQ applies.
    pub dlq: Option<crate::config::DlqSpec>,
    /// Pipeline-level quality spec, shared by every node. `quality:` has no
    /// matrix-row override in v1, so this is `cfg.pipeline.quality` verbatim.
    #[cfg(feature = "quality")]
    pub quality: Option<faucet_core::QualitySpec>,
    /// Pipeline-level data contract, shared by every node (`contract:` has no
    /// matrix-row override in v1) — `cfg.pipeline.contract` verbatim.
    #[cfg(feature = "contract")]
    pub contract: Option<faucet_core::ContractSpec>,
    /// Pipeline-level PII masking policy, shared by every node (`masking:` has
    /// no matrix-row override in v1) — `cfg.pipeline.masking` verbatim. The
    /// executor compiles it *scoped to this node's sink* ([`sink_ref`] +
    /// [`sink`].`kind`) so `applies_to` destination-scoping works (#206).
    ///
    /// [`sink_ref`]: ExpandedNode::sink_ref
    /// [`sink`]: ExpandedNode::sink
    #[cfg(feature = "masking")]
    pub masking: Option<faucet_core::MaskingSpec>,
    /// The sink template name this node resolved (`sink.ref`, or `"default"`
    /// for the legacy singular `pipeline.sink`). Used to scope masking
    /// `applies_to` rules per destination.
    pub sink_ref: String,
    /// Compiled schema-drift policy spec (pipeline-level; same for every node).
    pub schema: Option<faucet_core::SchemaDriftSpec>,
    /// Delivery guarantee for this row. Resolved from the row's override or
    /// falls back to the top-level `cfg.delivery`.
    pub delivery: faucet_core::DeliveryMode,
    /// The **derived** end-to-end guarantee this row's source × sink × config
    /// actually provides (issue #292) — computed for every row regardless of
    /// the requested `delivery:` mode, so `faucet validate` / `doctor` report
    /// it truthfully (e.g. a keyed-upsert row is effectively-once even when
    /// the user did not ask for `exactly_once`).
    pub delivery_guarantee: faucet_core::DeliveryGuarantee,
    /// Row ids this node waits for (deduplicated, declaration order). The
    /// executor starts the node only after every listed row's invocations
    /// finish successfully; a failed or skipped dependency skips this node.
    pub depends_on: Vec<String>,
    /// Resolved readiness status for this row's source (#371) — the source
    /// template's `status` overridden by the row's `source.status`, defaulting
    /// to [`SourceStatus::Active`] when neither is set. Drives the runtime
    /// run-set status gate; does not affect the state key.
    ///
    /// [`SourceStatus::Active`]: crate::config::SourceStatus::Active
    pub status: crate::config::SourceStatus,
    /// Effective classification tags (#376) = source-template `tags` ∪ row
    /// `tags` (union, deduped, sorted). Drives the runtime `--tag` narrowing;
    /// does not affect the state key.
    pub tags: Vec<String>,
    /// Every `${id.path}` placeholder that survived load-time interpolation.
    /// Populated by `collect_deferred`; the executor uses this to know
    /// which parent record to feed which row.
    pub deferred_refs: Vec<DeferredRef>,
    /// A pre-built source that replaces the registry-built one for this node.
    /// Set only by `faucet dlq replay` (#281), which injects a
    /// [`DlqReaderSource`](crate::dlq_replay::reader::DlqReaderSource) so the
    /// executor runs it through the normal pipeline path. `None` for every
    /// config-driven node (the executor builds the source from `source.kind`).
    pub source_override: Option<crate::dlq_replay::reader::SourceOverride>,
    /// Scoped-cleanup claim (#478): the source's `complete_for` scope, still
    /// carrying any `${parent.*}` / `${now.*}` tokens — the executor resolves
    /// them per invocation, like the connector configs. `Some` only when the
    /// destination sink also opted in with `cleanup: delete_missing`, so this
    /// being present already means a cleanup is intended.
    pub cleanup_scope: Option<std::collections::BTreeMap<String, serde_json::Value>>,
}

#[derive(Debug, Clone)]
pub enum NodeRole {
    /// Root node — runs once per pipeline invocation.
    Root,
    /// Child node — runs once per record produced by the parent row.
    Child {
        parent_id: String,
        parent_key: String,
    },
}

#[derive(Debug, Clone)]
pub struct DeferredRef {
    pub referenced_id: String,
    pub dotted_path: String,
    pub token: String,
}

/// In-memory lookup of source / sink templates, built once per `expand()` call.
/// Combines named entries from `pipeline.sources` / `pipeline.sinks` with the
/// legacy singular `pipeline.source` / `pipeline.sink` (registered as `default`).
struct Registry<'a> {
    sources: HashMap<&'a str, &'a ConnectorSpec>,
    sinks: HashMap<&'a str, &'a ConnectorSpec>,
}

impl<'a> Registry<'a> {
    fn build(spec: &'a PipelineSpec) -> CliResult<Self> {
        let mut sources: HashMap<&'a str, &'a ConnectorSpec> = HashMap::new();
        if let Some(default) = spec.source.as_ref() {
            sources.insert("default", default);
        }
        for (name, s) in spec.sources.iter() {
            if sources.contains_key(name.as_str()) {
                return Err(CliError::DuplicateTemplate {
                    kind: "source",
                    name: name.clone(),
                });
            }
            sources.insert(name.as_str(), s);
        }

        let mut sinks: HashMap<&'a str, &'a ConnectorSpec> = HashMap::new();
        if let Some(default) = spec.sink.as_ref() {
            if default.transforms.is_some() {
                return Err(CliError::TransformsOnSink {
                    name: "default".to_string(),
                });
            }
            if !default.inherit_transforms {
                return Err(CliError::InheritTransformsOnSink {
                    name: "default".to_string(),
                });
            }
            sinks.insert("default", default);
        }
        for (name, s) in spec.sinks.iter() {
            if sinks.contains_key(name.as_str()) {
                return Err(CliError::DuplicateTemplate {
                    kind: "sink",
                    name: name.clone(),
                });
            }
            if s.transforms.is_some() {
                return Err(CliError::TransformsOnSink { name: name.clone() });
            }
            if !s.inherit_transforms {
                return Err(CliError::InheritTransformsOnSink { name: name.clone() });
            }
            sinks.insert(name.as_str(), s);
        }
        Ok(Self { sources, sinks })
    }

    fn known(&self, kind: &'static str) -> Vec<String> {
        debug_assert!(
            matches!(kind, "source" | "sink"),
            "Registry::known called with kind = {:?}",
            kind
        );
        let map = if kind == "source" {
            &self.sources
        } else {
            &self.sinks
        };
        let mut out: Vec<String> = map.keys().map(|s| (*s).to_string()).collect();
        out.sort();
        out
    }

    fn resolve(
        &self,
        kind: &'static str,
        row_id: &str,
        overlay: Option<&PartialConnector>,
    ) -> CliResult<ConnectorSpec> {
        debug_assert!(
            matches!(kind, "source" | "sink"),
            "Registry::resolve called with kind = {:?}",
            kind
        );
        let map = if kind == "source" {
            &self.sources
        } else {
            &self.sinks
        };
        let ref_name = overlay
            .and_then(|p| p.r#ref.as_deref())
            .unwrap_or("default");
        let base = map.get(ref_name).ok_or_else(|| {
            if ref_name == "default" {
                CliError::MissingTemplate {
                    kind,
                    row_id: row_id.to_owned(),
                }
            } else {
                CliError::UnknownTemplate {
                    kind,
                    name: ref_name.to_owned(),
                    row_id: row_id.to_owned(),
                    known: self.known(kind),
                }
            }
        })?;
        let mut out = (*base).clone();
        if let Some(p) = overlay {
            if let Some(k) = &p.kind {
                out.kind = k.clone();
            }
            if let Some(c) = &p.config {
                merge_value(&mut out.config, c.clone());
            }
            // Readiness ladder is a scalar: a row `source.status` replaces the
            // template's (#371). `tags` are handled separately (union, not
            // replace) in `expand`, since `PartialConnector` carries no `tags`.
            if p.status.is_some() {
                out.status = p.status;
            }
        }
        Ok(out)
    }
}

/// Expand `cfg` into a topologically valid list of nodes. Roots come first,
/// then children in BFS order.
pub fn expand(cfg: &PipelineConfig) -> CliResult<Vec<ExpandedNode>> {
    // Fail-fast at config load: validate the execution-level adaptive
    // batch-size controller here (the shared `validate`/`run`/`preview`/
    // `doctor`/`schedule` gate) so `faucet validate` rejects a bad block
    // rather than only surfacing it mid-run in the executor.
    if let Some(ab) = cfg
        .execution
        .as_ref()
        .and_then(|e| e.adaptive_batch_size.as_ref())
    {
        // `validate()` returns `FaucetError::Config` whose message already names
        // the offending field; propagate it directly (CliError: From<FaucetError>).
        ab.validate()?;
    }

    // Implicit single-row case: empty matrix → run pipeline once with no merge.
    let synthetic_row;
    let rows: &[MatrixRow] = if cfg.matrix.is_empty() {
        synthetic_row = [MatrixRow {
            id: None,
            parent: None,
            depends_on: Vec::new(),
            parent_key: "id".into(),
            source: None,
            sink: None,
            transforms: None,
            inherit_transforms: true,
            state: None,
            dlq: None,
            delivery: None,
            tags: Vec::new(),
        }];
        &synthetic_row
    } else {
        &cfg.matrix
    };

    // 1) Assign / validate ids.
    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    let mut seen: HashSet<String> = HashSet::new();
    for (i, row) in rows.iter().enumerate() {
        let id = match &row.id {
            Some(s) => s.clone(),
            None => format!("row-{i}"),
        };
        if RESERVED_IDS.contains(&id.as_str()) {
            return Err(CliError::ReservedRowId { id });
        }
        if !seen.insert(id.clone()) {
            return Err(CliError::DuplicateRowId { id });
        }
        ids.push(id);
    }
    let id_set: HashSet<&str> = ids.iter().map(String::as_str).collect();

    // 2) Validate parents + detect cycles.
    let mut parents: HashMap<&str, &str> = HashMap::new();
    for (i, row) in rows.iter().enumerate() {
        let id = ids[i].as_str();
        if let Some(parent) = row.parent.as_deref() {
            if !id_set.contains(parent) {
                return Err(CliError::UnknownParent {
                    id: id.to_owned(),
                    parent: parent.to_owned(),
                });
            }
            if parent == id {
                return Err(CliError::ParentCycle {
                    ids: vec![id.to_owned()],
                });
            }
            parents.insert(id, parent);
        }
    }
    detect_cycle(&parents)?;

    // 2b) Validate `depends_on` edges (unknown id, self-dependency) and
    // dedup each row's list while preserving declaration order. Then check
    // the *combined* parent + depends_on graph for cycles — `detect_cycle`
    // above only walks single-parent chains, so a cycle routed through a
    // `depends_on` edge would otherwise deadlock the executor at run time.
    let mut deps_by_row: Vec<Vec<String>> = Vec::with_capacity(rows.len());
    for (i, row) in rows.iter().enumerate() {
        let id = ids[i].as_str();
        let mut deps: Vec<String> = Vec::with_capacity(row.depends_on.len());
        for dep in &row.depends_on {
            if !id_set.contains(dep.as_str()) {
                return Err(CliError::UnknownDependency {
                    id: id.to_owned(),
                    depends_on: dep.clone(),
                });
            }
            if dep == id {
                return Err(CliError::DependencyCycle {
                    ids: vec![id.to_owned()],
                });
            }
            if !deps.contains(dep) {
                deps.push(dep.clone());
            }
        }
        deps_by_row.push(deps);
    }
    detect_combined_cycle(&ids, &parents, &deps_by_row)?;

    // 3) Validate `${id.path}` references — each `id` must be a known row.
    // We scan the *raw* (pre-merge) row configs because interpolation lives in
    // strings that survive merging unchanged.
    for (i, row) in rows.iter().enumerate() {
        let id = ids[i].as_str();
        if let Some(p) = &row.source
            && let Some(c) = &p.config
        {
            check_refs(c, &id_set, id)?;
        }
        if let Some(p) = &row.sink
            && let Some(c) = &p.config
        {
            check_refs(c, &id_set, id)?;
        }
    }
    if let Some(s) = &cfg.pipeline.source {
        check_refs(&s.config, &id_set, "pipeline.source")?;
    }
    if let Some(s) = &cfg.pipeline.sink {
        check_refs(&s.config, &id_set, "pipeline.sink")?;
    }
    for (name, s) in &cfg.pipeline.sources {
        check_refs(&s.config, &id_set, &format!("pipeline.sources.{name}"))?;
    }
    for (name, s) in &cfg.pipeline.sinks {
        check_refs(&s.config, &id_set, &format!("pipeline.sinks.{name}"))?;
    }

    // 4) Build template registry — validates duplicate default conflicts.
    let registry = Registry::build(&cfg.pipeline)?;

    // 5) Build expanded nodes. Order: roots first (in declaration order),
    // then BFS over children — guarantees a parent appears before its children.
    let mut by_parent: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut roots: Vec<usize> = Vec::new();
    for (i, row) in rows.iter().enumerate() {
        match row.parent.as_deref() {
            None => roots.push(i),
            Some(p) => by_parent.entry(p).or_default().push(i),
        }
    }

    let mut order: Vec<usize> = Vec::with_capacity(rows.len());
    let mut queue: std::collections::VecDeque<usize> = roots.into_iter().collect();
    while let Some(idx) = queue.pop_front() {
        order.push(idx);
        if let Some(children) = by_parent.get(ids[idx].as_str()) {
            queue.extend(children.iter().copied());
        }
    }
    debug_assert_eq!(order.len(), rows.len());

    let mut out = Vec::with_capacity(rows.len());
    for &i in &order {
        let row = &rows[i];
        let row_id = ids[i].as_str();
        let merged_source = registry.resolve("source", row_id, row.source.as_ref())?;
        let merged_sink = registry.resolve("sink", row_id, row.sink.as_ref())?;
        // The sink template name this row resolved (or the legacy `default`),
        // used to scope masking `applies_to` per destination.
        let sink_ref = row
            .sink
            .as_ref()
            .and_then(|s| s.r#ref.clone())
            .unwrap_or_else(|| "default".to_string());
        let role = match &row.parent {
            None => NodeRole::Root,
            Some(p) => NodeRole::Child {
                parent_id: p.clone(),
                parent_key: row.parent_key.clone(),
            },
        };
        let mut deferred = Vec::new();
        collect_deferred(&merged_source.config, &mut deferred);
        collect_deferred(&merged_sink.config, &mut deferred);

        // Resolved readiness status (#371): `merged_source.status` already
        // carries the template→row `source.status` scalar merge; default to
        // `active` when neither declared it.
        let status = merged_source.status.unwrap_or_default();

        // Effective tags (#376) = source-template `tags` ∪ row `tags`. This is
        // the one deliberate exception to `merge.rs`'s array-replace rule:
        // tags union rather than replace. Validate + dedup + sort so the set is
        // canonical and order-insensitive.
        let tags = resolve_tags(&merged_source.tags, &row.tags, row_id)?;

        // Resolve transforms, state, and DLQ (row overrides win over base).
        // Three-layer additive resolution:
        //   T_pipeline ++ T_source ++ T_row
        // gated on each layer's `inherit_transforms` flag.
        let src_inherit = merged_source.inherit_transforms;
        let row_inherit = row.inherit_transforms;
        let mut transforms: Vec<TransformSpec> = Vec::new();
        if src_inherit && row_inherit {
            transforms.extend(cfg.pipeline.transforms.iter().cloned());
        }
        if row_inherit && let Some(src_ts) = merged_source.transforms.as_ref() {
            transforms.extend(src_ts.iter().cloned());
        }
        if let Some(row_ts) = row.transforms.as_ref() {
            transforms.extend(row_ts.iter().cloned());
        }
        let state = row.state.clone().or_else(|| cfg.pipeline.state.clone());
        // Row override wins; fall back to the top-level delivery mode.
        let delivery = row.delivery.unwrap_or(cfg.delivery);
        // Three-state match: Some(None) = disable, Some(Some(spec)) = replace,
        // None = inherit. The naive `.flatten().or_else()` would conflate
        // disable and absent, silently inheriting on explicit null.
        let dlq = match row.dlq.clone() {
            Some(None) => None,
            Some(Some(spec)) => Some(spec),
            None => cfg.pipeline.dlq.clone(),
        };

        if let Some(ref d) = dlq {
            if matches!(d.max_failures_per_page, Some(0)) {
                return Err(CliError::InvalidDlqBudget {
                    field: "max_failures_per_page",
                });
            }
            if matches!(d.max_failures_total, Some(0)) {
                return Err(CliError::InvalidDlqBudget {
                    field: "max_failures_total",
                });
            }
            if !crate::registry::sink_exists(&d.sink.kind) {
                return Err(CliError::UnknownDlqSinkKind {
                    kind: d.sink.kind.clone(),
                    context: format!("row `{row_id}`"),
                });
            }
        }

        // Runtime interpolation (`${row.path}` parent refs and `${now.*}`) is
        // resolved only in source/sink configs. A token in a transform / state
        // / dlq config would otherwise reach the connector as a literal
        // `${...}` string with no error (#146 M2) — reject it at expand time.
        for (ti, t) in transforms.iter().enumerate() {
            reject_runtime_tokens(
                &t.config,
                &format!("row `{row_id}` transform[{ti}] (`{}`)", t.kind),
            )?;
        }
        if let Some(ref st) = state {
            reject_runtime_tokens(&st.config, &format!("row `{row_id}` state config"))?;
        }
        if let Some(ref d) = dlq {
            reject_runtime_tokens(&d.sink.config, &format!("row `{row_id}` dlq sink config"))?;
        }

        // `quality:` is pipeline-level only in v1 (no matrix-row override), so
        // every node carries the same spec. Compile it once per node to (a)
        // surface invalid paths/regexes/bounds at expand time, and (b) fail
        // fast when a quarantine check has no DLQ to route to — the core guards
        // this at run start too, but catching it here makes `faucet validate`
        // a friendly, fast failure.
        #[cfg(feature = "quality")]
        let quality = cfg.pipeline.quality.clone();
        #[cfg(feature = "quality")]
        if let Some(ref spec) = quality {
            let compiled = faucet_core::CompiledQuality::compile(spec)
                .map_err(|e| CliError::Config(format!("quality (row `{row_id}`): {e}")))?;
            if compiled.requires_dlq() && dlq.is_none() {
                return Err(CliError::Config(format!(
                    "row `{row_id}`: a quality check uses `on_failure: quarantine` \
                     but no DLQ is configured — add a `dlq:` block (or change the \
                     check's `on_failure` to `abort`)"
                )));
            }
        }

        // `contract:` is pipeline-level only in v1 (like `quality:`). Compile
        // it once per node so a malformed contract (bad regex, duplicate
        // fields, misplaced constraints) surfaces at expand time, and fail
        // fast when `on_breach: quarantine` has no DLQ to route to.
        #[cfg(feature = "contract")]
        let contract = cfg.pipeline.contract.clone();
        #[cfg(feature = "contract")]
        if let Some(ref spec) = contract {
            let compiled = faucet_core::CompiledContract::compile(spec)
                .map_err(|e| CliError::Config(format!("contract (row `{row_id}`): {e}")))?;
            if compiled.requires_dlq() && dlq.is_none() {
                return Err(CliError::Config(format!(
                    "row `{row_id}`: the contract uses `on_breach: quarantine` \
                     but no DLQ is configured — add a `dlq:` block (or change \
                     `on_breach` to `fail` or `warn`)"
                )));
            }
        }

        // `masking:` is pipeline-level only in v1 (like `quality:`/`contract:`).
        // Compile it once per node so a malformed policy (empty rules, empty
        // match, bad regex) surfaces at expand time. No DLQ gate — masking
        // never quarantines; it rewrites matching fields in place.
        #[cfg(feature = "masking")]
        let masking = cfg.pipeline.masking.clone();
        #[cfg(feature = "masking")]
        if let Some(ref spec) = masking {
            faucet_core::CompiledMasking::compile(spec)
                .map_err(|e| CliError::Config(format!("masking (row `{row_id}`): {e}")))?;
        }

        // Resilience poison-pill cross-check: `poison.action: dlq` routes
        // persistently-failing rows to the DLQ, so a DLQ must be configured.
        // Caught here so `faucet validate` reports it before any run starts.
        if let Some(spec) = &cfg.resilience
            && matches!(
                spec.poison.as_ref().map(|p| p.action),
                Some(crate::config::PoisonActionSpec::Dlq)
            )
            && dlq.is_none()
        {
            return Err(CliError::Config(format!(
                "row '{row_id}': resilience.poison.action=dlq requires a dlq: block"
            )));
        }

        // SLA gate (load-time, #202): validate the spec once per row and
        // require a `state:` block when staleness / volume-anomaly checks need
        // persisted history. `min_rows_per_run` alone is stateless and passes
        // without one.
        if let Some(ref sla) = cfg.sla {
            sla.validate()
                .map_err(|e| CliError::Config(format!("sla: {e}")))?;
            if sla.needs_state() {
                match state.as_ref() {
                    None => {
                        return Err(CliError::Config(format!(
                            "row '{row_id}': sla.max_staleness_secs / sla.volume_anomaly \
                             need persisted run history — add a `state:` block \
                             (min_rows_per_run alone works without one)"
                        )));
                    }
                    Some(s) if s.kind == "memory" => {
                        tracing::warn!(
                            row = %row_id,
                            "sla: the `memory` state store resets on process exit — \
                             staleness/volume baselines only persist within a single \
                             `faucet schedule`/`serve` process; use `file`, `redis`, \
                             or `postgres` for one-shot runs"
                        );
                    }
                    Some(_) => {}
                }
            }
        }

        // write_mode × sink validation (load-time): reject an unsupported mode
        // for the sink kind, and upsert/delete without a key, before any run.
        // Runs for every row; append rows pass trivially.
        let requested_mode = merged_sink
            .config
            .get("write_mode")
            .and_then(|v| v.as_str())
            .unwrap_or("append");
        let mode = match requested_mode {
            "append" => faucet_core::WriteMode::Append,
            "upsert" => faucet_core::WriteMode::Upsert,
            "delete" => faucet_core::WriteMode::Delete,
            other => {
                return Err(CliError::Config(format!(
                    "row '{}': unknown write_mode '{}' (expected append, upsert, or delete)",
                    ids[i], other
                )));
            }
        };
        if !crate::registry::sink_supported_write_modes(&merged_sink.kind).contains(&mode) {
            return Err(CliError::Config(format!(
                "row '{}': write_mode '{}' is not supported by sink '{}' \
                 (upsert/delete sinks: {})",
                ids[i],
                requested_mode,
                merged_sink.kind,
                crate::registry::UPSERT_SINK_KINDS.join(", ")
            )));
        }
        if matches!(
            mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        ) {
            let key_present = merged_sink
                .config
                .get("key")
                .and_then(|v| v.as_array())
                .map(|a| !a.is_empty())
                .unwrap_or(false);
            if !key_present {
                return Err(CliError::Config(format!(
                    "row '{}': write_mode '{}' requires a non-empty `key`",
                    ids[i], requested_mode
                )));
            }
        }

        // Derived end-to-end delivery guarantee (issue #292): computed for
        // *every* row — regardless of the requested `delivery:` mode — so
        // `faucet validate` / `doctor` report the truth (a keyed-upsert row is
        // effectively-once even when the user did not request `exactly_once`).
        // `keyed_upsert_configured` relies on the write_mode gate above: after
        // it, an upsert/delete mode implies a non-empty `key`.
        let keyed_upsert_configured = matches!(
            mode,
            faucet_core::WriteMode::Upsert | faucet_core::WriteMode::Delete
        );
        let guarantee_inputs = faucet_core::GuaranteeInputs {
            replay: crate::registry::source_replay_guarantee(&merged_source.kind),
            sink_atomic: crate::registry::sink_supports_idempotent_writes(&merged_sink.kind),
            keyed_upsert_configured,
            durable_state: matches!(state.as_ref(), Some(s) if s.kind != "memory"),
            dlq: dlq.is_some(),
        };
        let delivery_guarantee = faucet_core::derive_delivery_guarantee(&guarantee_inputs);

        // Exactly-once delivery gate: `delivery: exactly_once` means "require
        // ≥ effectively-once". Enforced at config-load time so `faucet
        // validate` catches an unsupported topology before any run starts,
        // with the error naming the limiting side. A derived `AtLeastOnce`
        // implies keyed dedup is not configured (the keyed mechanism has no
        // other requirement), so the cascade below walks the atomic-watermark
        // requirements in order.
        if delivery == faucet_core::DeliveryMode::ExactlyOnce
            && delivery_guarantee == faucet_core::DeliveryGuarantee::AtLeastOnce
        {
            if !crate::registry::source_supports_exactly_once(&merged_source.kind) {
                let keyed_hint = if crate::registry::UPSERT_SINK_KINDS.contains(&&*merged_sink.kind)
                {
                    format!(
                        ", or configure `write_mode: upsert` + `key` on sink '{}' for \
                         keyed-upsert effectively-once with any source",
                        merged_sink.kind
                    )
                } else {
                    String::new()
                };
                return Err(CliError::Config(format!(
                    "row '{}': delivery: exactly_once is not supported by source '{}' \
                     (deterministic-replay sources only: {}{})",
                    ids[i],
                    merged_source.kind,
                    crate::registry::EXACTLY_ONCE_SOURCE_KINDS.join(", "),
                    keyed_hint
                )));
            }
            if !crate::registry::sink_supports_idempotent_writes(&merged_sink.kind) {
                let keyed_hint = if crate::registry::UPSERT_SINK_KINDS.contains(&&*merged_sink.kind)
                {
                    format!(
                        "; alternatively configure `write_mode: upsert` + `key` on '{}' for \
                         keyed-upsert effectively-once",
                        merged_sink.kind
                    )
                } else {
                    String::new()
                };
                return Err(CliError::Config(format!(
                    "row '{}': delivery: exactly_once is not supported by sink '{}' \
                     (idempotent sinks only: {}{})",
                    ids[i],
                    merged_sink.kind,
                    crate::registry::IDEMPOTENT_SINK_KINDS.join(", "),
                    keyed_hint
                )));
            }
            // Require a *durable* state store. The atomic-watermark mechanism
            // persists the monotonic page sequence alongside the bookmark
            // (`wrap_state(bookmark, seq)`) and resumes from it across
            // restarts; the in-process `memory` store loses that watermark on
            // exit, so a restart would re-run already-committed pages — exactly
            // the duplication exactly-once exists to prevent (F24). Mirror the
            // `faucet replicate` gate, which already rejects `memory`.
            match state.as_ref() {
                None => {
                    return Err(CliError::Config(format!(
                        "row '{}': delivery: exactly_once requires a state store",
                        ids[i]
                    )));
                }
                Some(s) if s.kind == "memory" => {
                    return Err(CliError::Config(format!(
                        "row '{}': delivery: exactly_once requires a durable state store, \
                         not `memory` — the cross-restart watermark/sequence guarantee \
                         depends on it (use `file`, `redis`, or `postgres`)",
                        ids[i]
                    )));
                }
                Some(_) => {}
            }
            if dlq.is_some() {
                return Err(CliError::Config(format!(
                    "row '{}': delivery: exactly_once is not compatible with a DLQ in this version",
                    ids[i]
                )));
            }
            // The cascade above covers every way the derivation can land on
            // at-least-once; reaching here would mean it diverged from the
            // checks.
            unreachable!("delivery-guarantee derivation and the exactly-once gate diverged");
        }

        // ── Scoped-cleanup gates (load-time, #478) ──────────────────────
        // Cleanup DELETES data, so every precondition is checked before a run
        // starts rather than discovered mid-flight.
        if merged_sink.complete_for.is_some() {
            return Err(CliError::Config(format!(
                "row '{}': `complete_for` belongs on the source, not the sink — only the \
                 source can claim a fetch returned every record for a scope",
                ids[i]
            )));
        }
        let cleanup_scope = match merged_source.complete_for.as_ref() {
            None => None,
            Some(claim) if claim.on_missing == crate::config::OnMissing::Ignore => {
                // A claim with no action is inert by design — it documents the
                // scope without authorising a delete.
                None
            }
            Some(claim) => {
                if claim.scope.is_empty() {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.scope` is empty — an empty scope matches \
                         every row in the destination",
                        ids[i]
                    )));
                }
                if !crate::registry::sink_supports_cleanup(&merged_sink.kind) {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.on_missing: delete` is not supported by sink \
                         '{}' (cleanup-capable sinks: {})",
                        ids[i],
                        merged_sink.kind,
                        crate::registry::CLEANUP_SINK_KINDS.join(", ")
                    )));
                }
                if !matches!(mode, faucet_core::WriteMode::Upsert) {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.on_missing: delete` requires \
                         `write_mode: upsert` (got '{}') — on an append sink there is no key \
                         to tell a written row from a stale one",
                        ids[i], requested_mode
                    )));
                }
                // Cleanup is a second, non-idempotent write outside the
                // commit-token transaction, so it cannot compose with the
                // atomic-watermark path.
                if matches!(delivery, faucet_core::DeliveryMode::ExactlyOnce) {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.on_missing: delete` is incompatible with \
                         `delivery: exactly_once` — the scoped delete happens outside the \
                         commit-token transaction, so it cannot be replayed idempotently",
                        ids[i]
                    )));
                }
                // A quarantined record never reaches the sink, so the cleanup
                // tracker never sees its key — and the delete would then remove
                // its destination row, losing data the source still has. Reject
                // rather than silently delete.
                let mut quarantines: Vec<&str> = Vec::new();
                #[cfg(feature = "quality")]
                if let Some(q) = cfg.pipeline.quality.as_ref()
                    && faucet_core::CompiledQuality::compile(q)
                        .map(|c| c.requires_dlq())
                        .unwrap_or(false)
                {
                    quarantines.push("quality");
                }
                #[cfg(feature = "contract")]
                if let Some(c) = cfg.pipeline.contract.as_ref()
                    && faucet_core::CompiledContract::compile(c)
                        .map(|c| c.requires_dlq())
                        .unwrap_or(false)
                {
                    quarantines.push("contract");
                }
                if let Some(sd) = cfg.pipeline.schema.as_ref()
                    && faucet_core::SchemaDriftPolicy::compile(sd).requires_dlq()
                {
                    quarantines.push("schema");
                }
                if !quarantines.is_empty() {
                    return Err(CliError::Config(format!(
                        "row '{}': `complete_for.on_missing: delete` is incompatible with a \
                         quarantining `{}` policy — a quarantined record never reaches the \
                         sink, so cleanup cannot tell it from a record deleted at the source \
                         and would delete its destination row",
                        ids[i],
                        quarantines.join("`/`")
                    )));
                }
                Some(claim.scope.clone())
            }
        };

        // Schema-drift policy gates (load-time):
        //  - `evolve` requires an evolution-capable sink.
        //  - `quarantine` (drift or incompatible) requires a DLQ, and is
        //    incompatible with exactly-once (which forbids a DLQ).
        if let Some(ref sd) = cfg.pipeline.schema {
            let policy = faucet_core::SchemaDriftPolicy::compile(sd);
            if policy.on_drift == faucet_core::OnDrift::Evolve
                && !crate::registry::sink_supports_schema_evolution(&merged_sink.kind)
            {
                return Err(CliError::Config(format!(
                    "row '{}': schema.on_drift: evolve is not supported by sink '{}' \
                     (evolvable sinks: postgres, mysql, mssql, sqlite, bigquery, elasticsearch)",
                    ids[i], merged_sink.kind
                )));
            }
            if policy.requires_dlq() && dlq.is_none() {
                return Err(CliError::Config(format!(
                    "row '{}': schema.on_drift/on_incompatible 'quarantine' requires a `dlq:` block",
                    ids[i]
                )));
            }
            if policy.requires_dlq() && delivery == faucet_core::DeliveryMode::ExactlyOnce {
                return Err(CliError::Config(format!(
                    "row '{}': schema quarantine is incompatible with delivery: exactly_once \
                     (exactly_once forbids a DLQ)",
                    ids[i]
                )));
            }
        }

        out.push(ExpandedNode {
            id: ids[i].clone(),
            row_index: i,
            role,
            source: merged_source,
            sink: merged_sink,
            transforms,
            state,
            dlq,
            delivery,
            delivery_guarantee,
            #[cfg(feature = "quality")]
            quality,
            #[cfg(feature = "contract")]
            contract,
            #[cfg(feature = "masking")]
            masking,
            sink_ref,
            schema: cfg.pipeline.schema.clone(),
            depends_on: deps_by_row[i].clone(),
            status,
            tags,
            deferred_refs: deferred,
            source_override: None,
            cleanup_scope,
        });
    }
    Ok(out)
}

fn detect_cycle(parents: &HashMap<&str, &str>) -> CliResult<()> {
    // Each node has at most one parent ⇒ cycle detection is "walk parents
    // until we hit `None` or revisit a node we've already seen this walk".
    for &start in parents.keys() {
        let mut visited: BTreeSet<&str> = BTreeSet::new();
        let mut cur = start;
        while let Some(&p) = parents.get(cur) {
            if !visited.insert(cur) {
                let chain: Vec<String> = visited.iter().map(|s| (*s).to_string()).collect();
                return Err(CliError::ParentCycle { ids: chain });
            }
            cur = p;
            if cur == start {
                let mut chain: Vec<String> = visited.iter().map(|s| (*s).to_string()).collect();
                chain.push(start.to_string());
                return Err(CliError::ParentCycle { ids: chain });
            }
        }
    }
    Ok(())
}

/// Kahn's algorithm over the combined `parent:` + `depends_on:` edge set.
/// Pure-parent cycles are already caught by [`detect_cycle`] (with its more
/// specific error), so any leftover here necessarily involves a `depends_on`
/// edge. Rows that cannot be topologically ordered are the cycle participants
/// (plus any rows downstream of them — still actionable, since the report
/// names every row that would never become ready).
fn detect_combined_cycle(
    ids: &[String],
    parents: &HashMap<&str, &str>,
    deps_by_row: &[Vec<String>],
) -> CliResult<()> {
    let index_of: HashMap<&str, usize> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| (id.as_str(), i))
        .collect();
    let mut in_degree = vec![0usize; ids.len()];
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); ids.len()];
    for (i, id) in ids.iter().enumerate() {
        let mut prereqs: Vec<usize> = Vec::new();
        if let Some(p) = parents.get(id.as_str()) {
            prereqs.push(index_of[p]);
        }
        prereqs.extend(deps_by_row[i].iter().map(|d| index_of[d.as_str()]));
        for p in prereqs {
            in_degree[i] += 1;
            dependents[p].push(i);
        }
    }
    let mut queue: std::collections::VecDeque<usize> =
        (0..ids.len()).filter(|&i| in_degree[i] == 0).collect();
    let mut processed = 0usize;
    while let Some(i) = queue.pop_front() {
        processed += 1;
        for &d in &dependents[i] {
            in_degree[d] -= 1;
            if in_degree[d] == 0 {
                queue.push_back(d);
            }
        }
    }
    if processed < ids.len() {
        let mut stuck: Vec<String> = (0..ids.len())
            .filter(|&i| in_degree[i] > 0)
            .map(|i| ids[i].clone())
            .collect();
        stuck.sort();
        return Err(CliError::DependencyCycle { ids: stuck });
    }
    Ok(())
}

/// Verify that every `${X.path}` token in `value` has `X` listed in `id_set`.
/// Load-time prefixes (`env`, `file`, `secret`) were already handled and are
/// ignored here.
fn check_refs(value: &Value, id_set: &HashSet<&str>, owner: &str) -> CliResult<()> {
    walk_strings(value, &mut |s| {
        for (token, dir) in iter_directives(s) {
            // Load-time / template directives (`${env:..}`, `${vars.X}`, …) are
            // resolved before expansion; only deferred `${id.path}` references
            // are validated here, against the known row ids.
            // `now` and `backfill` are reserved built-in deferred ids
            // resolved at run time (`backfill` by `faucet backfill`, #282).
            // `${param.*}` is bound *pre-parse* (`params::bind_document`), so a
            // token surviving to expansion means the config was built through a
            // path that skipped binding — e.g. a host calling
            // `PipelineConfig::from_text`/`from_value` directly. Name the cause
            // rather than reporting a generic unknown row id (#444).
            if let Directive::Deferred { id, .. } = dir
                && id == crate::params::PARAM_ID
            {
                return Err(CliError::Config(format!(
                    "interpolation token `{token}` (in {owner}) was never bound — a `${{param.*}}` \
                     reference is resolved when the run is triggered. Load the config through \
                     `PipelineConfig::from_path*` (or supply values with `--param`) so params are \
                     bound before expansion"
                )));
            }
            if let Directive::Deferred { id, .. } = dir
                && id != "now"
                && id != "backfill"
                && id != "partition"
                && !id_set.contains(id)
            {
                return Err(CliError::UnknownInterpolationId {
                    id: id.to_owned(),
                    token: format!("{token} (in {owner})"),
                });
            }
        }
        Ok(())
    })
}

/// Reject any runtime interpolation token (`${id.path}` parent-record refs and
/// `${now.*}`) found in `value`. These resolve **only** in source/sink configs;
/// elsewhere — transform / state / dlq bodies — they would silently reach the
/// connector as a literal `${...}` string (#146 M2). Load-time directives
/// (`${env:}`, `${vars.X}`, `${sources.X}`, …) are already resolved before
/// expansion, so any deferred token still present here is genuinely
/// unsupported in this location.
fn reject_runtime_tokens(value: &Value, location: &str) -> CliResult<()> {
    walk_strings(value, &mut |s| {
        for (token, dir) in iter_directives(s) {
            if let Directive::Deferred { .. } = dir {
                return Err(CliError::Config(format!(
                    "interpolation token `{token}` in {location} is not supported: \
                     `${{...}}` runtime tokens (parent-record references and `${{now.*}}`) \
                     resolve only in source/sink configs"
                )));
            }
        }
        Ok(())
    })
}

/// Compute a row's effective tag set = `template_tags` ∪ `row_tags` (#376).
/// Union — the deliberate exception to `merge.rs`'s array-replace rule.
/// Validates each tag (charset `^[a-z0-9][a-z0-9_-]*$`, non-empty), dedups, and
/// returns a sorted, canonical list so `--tag` matching is order-insensitive.
fn resolve_tags(
    template_tags: &[String],
    row_tags: &[String],
    row_id: &str,
) -> CliResult<Vec<String>> {
    let mut set: BTreeSet<String> = BTreeSet::new();
    for tag in template_tags.iter().chain(row_tags.iter()) {
        validate_tag(tag, row_id)?;
        set.insert(tag.clone());
    }
    Ok(set.into_iter().collect())
}

/// A tag must be lowercase kebab/snake: `^[a-z0-9][a-z0-9_-]*$`.
fn validate_tag(tag: &str, row_id: &str) -> CliResult<()> {
    let ok = {
        let mut chars = tag.chars();
        match chars.next() {
            Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit() => {
                chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-')
            }
            _ => false,
        }
    };
    if !ok {
        return Err(CliError::Config(format!(
            "row '{row_id}': invalid tag '{tag}' — tags must match ^[a-z0-9][a-z0-9_-]*$ \
             (lowercase letters, digits, `_`, `-`; first char alphanumeric)"
        )));
    }
    Ok(())
}

fn collect_deferred(value: &Value, out: &mut Vec<DeferredRef>) {
    let _ = walk_strings(value, &mut |s| {
        for (token, dir) in iter_directives(s) {
            if let Directive::Deferred { id, path } = dir {
                // `now` / `backfill` / `partition` are reserved built-ins
                // resolved at run time, not parent-record dependencies — skip
                // them so the executor doesn't treat them as deferred
                // parent-record refs.
                if id == "now" || id == "backfill" || id == "partition" {
                    continue;
                }
                out.push(DeferredRef {
                    referenced_id: id.to_owned(),
                    dotted_path: path.to_owned(),
                    token: token.to_owned(),
                });
            }
        }
        Ok(())
    });
}

fn walk_strings<F>(value: &Value, f: &mut F) -> CliResult<()>
where
    F: FnMut(&str) -> CliResult<()>,
{
    match value {
        Value::String(s) => f(s),
        Value::Array(a) => a.iter().try_for_each(|v| walk_strings(v, f)),
        Value::Object(m) => m.values().try_for_each(|v| walk_strings(v, f)),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{OnBatchErrorSpec, parse_with_extension};

    fn cfg(yaml: &str) -> PipelineConfig {
        parse_with_extension(yaml, "yaml").unwrap()
    }

    #[test]
    fn implicit_single_row_when_matrix_absent() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].id, "row-0");
        assert!(matches!(nodes[0].role, NodeRole::Root));
        assert_eq!(nodes[0].source.kind, "rest");
        assert_eq!(nodes[0].sink.kind, "jsonl");
    }

    #[test]
    fn rejects_runtime_token_in_dlq_config() {
        // M2 (#146): `${now.*}` / `${parent.path}` resolve only in source/sink
        // configs. In a dlq config they would silently pass through as a literal
        // `${...}` string — expand must reject them with a clear error.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  dlq:
    sink: { type: jsonl, config: { path: "dead-${now.date}.jsonl" } }
"#);
        let err = expand(&c).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("now.date") && m.contains("dlq")),
            "got: {err:?}"
        );
    }

    #[test]
    fn rejects_runtime_token_in_state_config() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  state:
    type: file
    config: { path: "state-${now.date}" }
"#);
        let err = expand(&c).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("state")),
            "got: {err:?}"
        );
    }

    #[test]
    fn rejects_runtime_token_in_transform_config() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  transforms:
    - type: set
      config: { field: ts, value: "${now.datetime}" }
"#);
        let err = expand(&c).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("transform")),
            "got: {err:?}"
        );
    }

    #[test]
    fn allows_runtime_token_in_source_and_sink_configs() {
        // The same tokens remain valid in source/sink configs (regression guard
        // that M2's rejection didn't over-reach).
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: "https://x?d=${now.date}" } }
  sink:   { type: jsonl, config: { path: "out-${now.date}.jsonl" } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn merges_row_overrides_into_pipeline_source() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x, headers: { a: 1 } } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
    source: { config: { path: /v1/users, headers: { b: 2 } } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].id, "users");
        assert_eq!(nodes[0].source.config["base_url"], "https://x");
        assert_eq!(nodes[0].source.config["path"], "/v1/users");
        assert_eq!(nodes[0].source.config["headers"]["a"], 1);
        assert_eq!(nodes[0].source.config["headers"]["b"], 2);
    }

    #[test]
    fn errors_on_unknown_parent() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: child
    parent: nobody
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::UnknownParent { .. }
        ));
    }

    #[test]
    fn errors_on_duplicate_ids() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: x }
  - { id: x }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::DuplicateRowId { .. }
        ));
    }

    #[test]
    fn errors_on_reserved_id() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: env }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::ReservedRowId { .. }
        ));
    }

    #[test]
    fn errors_on_self_parent_cycle() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, parent: a }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::ParentCycle { .. }
        ));
    }

    #[test]
    fn errors_on_two_node_cycle() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, parent: b }
  - { id: b, parent: a }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::ParentCycle { .. }
        ));
    }

    #[test]
    fn errors_on_unknown_dependency() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: facts, depends_on: [nobody] }
"#);
        match expand(&c).unwrap_err() {
            CliError::UnknownDependency { id, depends_on } => {
                assert_eq!(id, "facts");
                assert_eq!(depends_on, "nobody");
            }
            other => panic!("expected UnknownDependency, got {other:?}"),
        }
    }

    #[test]
    fn errors_on_self_dependency() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, depends_on: [a] }
"#);
        match expand(&c).unwrap_err() {
            CliError::DependencyCycle { ids } => assert_eq!(ids, vec!["a".to_string()]),
            other => panic!("expected DependencyCycle, got {other:?}"),
        }
    }

    #[test]
    fn errors_on_depends_on_cycle() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, depends_on: [b] }
  - { id: b, depends_on: [a] }
"#);
        match expand(&c).unwrap_err() {
            CliError::DependencyCycle { ids } => {
                assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected DependencyCycle, got {other:?}"),
        }
    }

    #[test]
    fn errors_on_mixed_parent_depends_on_cycle() {
        // `a` is a child of `b` (parent edge b -> a) while `b` waits for `a`
        // (dependency edge a -> b). Neither the parent-only walk nor a
        // depends_on-only check sees this — only the combined graph does.
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: a, parent: b }
  - { id: b, depends_on: [a] }
"#);
        match expand(&c).unwrap_err() {
            CliError::DependencyCycle { ids } => {
                assert_eq!(ids, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected DependencyCycle, got {other:?}"),
        }
    }

    #[test]
    fn depends_on_is_recorded_and_deduped() {
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: dims }
  - { id: staging }
  - { id: facts, depends_on: [dims, staging, dims] }
"#);
        let nodes = expand(&c).unwrap();
        let facts = nodes.iter().find(|n| n.id == "facts").unwrap();
        assert_eq!(
            facts.depends_on,
            vec!["dims".to_string(), "staging".to_string()]
        );
        assert!(matches!(facts.role, NodeRole::Root));
        let dims = nodes.iter().find(|n| n.id == "dims").unwrap();
        assert!(dims.depends_on.is_empty());
    }

    #[test]
    fn depends_on_may_target_a_child_row() {
        // Waiting on a per-record fan-out row is legal: the dependent starts
        // only after every one of the child's invocations completes.
        let c = cfg(r#"
version: 1
pipeline: { source: { type: rest, config: {} }, sink: { type: jsonl, config: { path: ./o } } }
matrix:
  - { id: users }
  - { id: posts, parent: users }
  - { id: rollup, depends_on: [posts] }
"#);
        let nodes = expand(&c).unwrap();
        let rollup = nodes.iter().find(|n| n.id == "rollup").unwrap();
        assert_eq!(rollup.depends_on, vec!["posts".to_string()]);
    }

    #[test]
    fn errors_on_unknown_interpolation_id() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "https://x/${nobody.id}" } }
  sink:   { type: jsonl, config: { path: ./o } }
"#);
        assert!(matches!(
            expand(&c).unwrap_err(),
            CliError::UnknownInterpolationId { .. }
        ));
    }

    #[test]
    fn dot_form_reserved_prefix_is_validated_as_deferred_id() {
        // Regression for #78/#39: `${env.foo}` has no colon, so it is a
        // deferred reference to id `env`, not a load-time `env:` directive.
        // The validator must reject it (as the runtime would), rather than
        // silently skipping it and letting `run` fail later.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "https://x/${env.foo}" } }
  sink:   { type: jsonl, config: { path: ./o } }
"#);
        match expand(&c).unwrap_err() {
            CliError::UnknownInterpolationId { id, .. } => assert_eq!(id, "env"),
            other => panic!("expected UnknownInterpolationId for `env`, got {other:?}"),
        }
    }

    #[test]
    fn accepts_id_path_when_referenced_row_exists() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
  - id: posts
    parent: users
    source: { config: { path: "/v1/users/${users.id}/posts" } }
"#);
        let nodes = expand(&c).unwrap();
        let posts = nodes.iter().find(|n| n.id == "posts").unwrap();
        assert_eq!(posts.deferred_refs.len(), 1);
        assert_eq!(posts.deferred_refs[0].referenced_id, "users");
        assert_eq!(posts.deferred_refs[0].dotted_path, "id");
    }

    #[test]
    fn nested_referenced_path_resolves() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
  - id: addrs
    parent: users
    source: { config: { path: "/users/${users.addr.city}/addr" } }
"#);
        let nodes = expand(&c).unwrap();
        let addrs = nodes.iter().find(|n| n.id == "addrs").unwrap();
        assert_eq!(addrs.deferred_refs[0].dotted_path, "addr.city");
    }

    #[test]
    fn roots_come_before_children_in_order() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: posts
    parent: users
  - id: users
"#);
        let nodes = expand(&c).unwrap();
        let users_idx = nodes.iter().position(|n| n.id == "users").unwrap();
        let posts_idx = nodes.iter().position(|n| n.id == "posts").unwrap();
        assert!(users_idx < posts_idx, "users must precede posts");
    }

    #[test]
    fn child_node_has_parent_role() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
  - id: posts
    parent: users
    parent_key: user_id
"#);
        let nodes = expand(&c).unwrap();
        let posts = nodes.iter().find(|n| n.id == "posts").unwrap();
        match &posts.role {
            NodeRole::Child {
                parent_id,
                parent_key,
            } => {
                assert_eq!(parent_id, "users");
                assert_eq!(parent_key, "user_id");
            }
            other => panic!("expected Child, got {other:?}"),
        }
    }

    #[test]
    fn expand_rejects_zero_per_page_budget() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
    max_failures_per_page: 0
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        assert!(matches!(
            err,
            CliError::InvalidDlqBudget {
                field: "max_failures_per_page"
            }
        ));
    }

    #[test]
    fn expand_rejects_zero_total_budget() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
    max_failures_total: 0
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        assert!(matches!(
            err,
            CliError::InvalidDlqBudget {
                field: "max_failures_total"
            }
        ));
    }

    #[test]
    fn expand_rejects_unknown_dlq_sink_kind() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: not_a_sink, config: {} }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        assert!(matches!(err, CliError::UnknownDlqSinkKind { .. }));
    }

    #[cfg(feature = "quality")]
    #[test]
    fn expand_rejects_quarantine_without_dlq() {
        // A quality check with `on_failure: quarantine` needs a DLQ to route to.
        // `expand` must reject the config so `faucet validate` fails fast.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  quality:
    record:
      - { type: not_null, field: id, on_failure: quarantine }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match err {
            CliError::Config(msg) => {
                assert!(msg.contains("quarantine"), "{msg}");
                assert!(msg.contains("DLQ") || msg.contains("dlq"), "{msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(feature = "quality")]
    #[test]
    fn expand_accepts_quarantine_with_dlq() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
  quality:
    record:
      - { type: not_null, field: id, on_failure: quarantine }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 1);
        let q = nodes[0]
            .quality
            .as_ref()
            .expect("quality threaded onto node");
        assert_eq!(q.record.len(), 1);
    }

    #[cfg(feature = "quality")]
    #[test]
    fn expand_accepts_abort_quality_without_dlq() {
        // `on_failure: abort` does not route to a DLQ, so no DLQ is required.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  quality:
    record:
      - { type: not_null, field: id, on_failure: abort }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert!(nodes[0].quality.is_some());
    }

    #[cfg(feature = "contract")]
    #[test]
    fn expand_rejects_contract_quarantine_without_dlq() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  contract:
    version: "1.0.0"
    on_breach: quarantine
    fields:
      - { name: id, type: integer }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match err {
            CliError::Config(msg) => {
                assert!(msg.contains("on_breach: quarantine"), "{msg}");
                assert!(msg.contains("dlq"), "{msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[cfg(feature = "contract")]
    #[test]
    fn expand_accepts_contract_quarantine_with_dlq() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./dlq.jsonl } }
  contract:
    version: "1.0.0"
    on_breach: quarantine
    fields:
      - { name: id, type: integer }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 1);
        let c = nodes[0]
            .contract
            .as_ref()
            .expect("contract threaded onto node");
        assert_eq!(c.version, "1.0.0");
        assert_eq!(c.fields.len(), 1);
    }

    #[cfg(feature = "contract")]
    #[test]
    fn expand_accepts_contract_fail_without_dlq() {
        // `on_breach: fail` (the default) does not route to a DLQ.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  contract:
    version: "1.0.0"
    fields:
      - { name: id, type: integer }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert!(nodes[0].contract.is_some());
    }

    #[cfg(feature = "contract")]
    #[test]
    fn expand_rejects_malformed_contract() {
        // A bad regex must surface at expand time (load-time), not mid-run.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  contract:
    version: "1.0.0"
    fields:
      - { name: email, type: string, pattern: "[invalid" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match err {
            CliError::Config(msg) => assert!(msg.contains("invalid pattern"), "{msg}"),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn legacy_singular_source_resolves_as_default_template() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].source.kind, "rest");
        assert_eq!(nodes[0].source.config["base_url"], "https://x");
    }

    #[test]
    fn row_with_ref_picks_named_template() {
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    users_api: { type: rest, config: { base_url: https://x } }
  sinks:
    archive:   { type: jsonl, config: { path: ./out } }
matrix:
  - id: load_users
    source:
      ref: users_api
      config: { path: /v1/users }
    sink:
      ref: archive
      config: { path: ./users.jsonl }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].source.kind, "rest");
        assert_eq!(nodes[0].source.config["base_url"], "https://x");
        assert_eq!(nodes[0].source.config["path"], "/v1/users");
        assert_eq!(nodes[0].sink.config["path"], "./users.jsonl");
    }

    #[test]
    fn row_without_ref_falls_back_to_default_template() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
    source: { config: { path: /v1/users } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].source.kind, "rest");
        assert_eq!(nodes[0].source.config["path"], "/v1/users");
    }

    #[test]
    fn unknown_template_ref_errors_with_known_list() {
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    a: { type: rest, config: {} }
    b: { type: rest, config: {} }
  sinks:
    s: { type: jsonl, config: { path: ./o } }
matrix:
  - id: x
    source: { ref: c }
    sink: { ref: s }
"#);
        let err = expand(&c).unwrap_err();
        match err {
            CliError::UnknownTemplate {
                kind,
                name,
                row_id,
                known,
            } => {
                assert_eq!(kind, "source");
                assert_eq!(name, "c");
                assert_eq!(row_id, "x");
                assert_eq!(known, vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected UnknownTemplate, got {other:?}"),
        }
    }

    #[test]
    fn missing_default_template_errors() {
        // No singular `source:` and no `sources.default` — a row without a ref
        // has nowhere to go.
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    users_api: { type: rest, config: {} }
  sink: { type: jsonl, config: { path: ./o } }
matrix:
  - id: x
    source: { config: { path: /v1 } }
"#);
        let err = expand(&c).unwrap_err();
        match err {
            CliError::MissingTemplate { kind, row_id } => {
                assert_eq!(kind, "source");
                assert_eq!(row_id, "x");
            }
            other => panic!("expected MissingTemplate, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_default_template_errors() {
        // Defining both legacy `source:` and `sources.default:` is a conflict.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sources:
    default: { type: rest, config: {} }
  sink: { type: jsonl, config: { path: ./o } }
"#);
        let err = expand(&c).unwrap_err();
        match err {
            CliError::DuplicateTemplate { kind, name } => {
                assert_eq!(kind, "source");
                assert_eq!(name, "default");
            }
            other => panic!("expected DuplicateTemplate, got {other:?}"),
        }
    }

    #[test]
    fn row_can_override_template_kind() {
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    out: { type: jsonl, config: { path: ./o } }
matrix:
  - id: x
    source: { ref: api, type: graphql, config: { query: "{users{id}}" } }
    sink: { ref: out }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes[0].source.kind, "graphql");
        assert_eq!(nodes[0].source.config["base_url"], "https://x");
        assert_eq!(nodes[0].source.config["query"], "{users{id}}");
    }

    #[test]
    fn expand_accepts_inherited_disabled_replaced_dlq_rows() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  dlq:
    sink: { type: jsonl, config: { path: ./base.jsonl } }
matrix:
  - id: a
  - id: b
    dlq: null
  - id: c
    dlq:
      sink: { type: jsonl, config: { path: ./c.jsonl } }
      on_batch_error: dlq_all
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 3);
        // Row a inherits.
        assert_eq!(nodes[0].dlq.as_ref().unwrap().sink.kind, "jsonl");
        assert_eq!(
            nodes[0]
                .dlq
                .as_ref()
                .unwrap()
                .sink
                .config
                .get("path")
                .unwrap(),
            "./base.jsonl"
        );
        // Row b is disabled.
        assert!(nodes[1].dlq.is_none());
        // Row c is replaced.
        assert_eq!(
            nodes[2].dlq.as_ref().unwrap().on_batch_error,
            OnBatchErrorSpec::DlqAll
        );
        assert_eq!(
            nodes[2]
                .dlq
                .as_ref()
                .unwrap()
                .sink
                .config
                .get("path")
                .unwrap(),
            "./c.jsonl"
        );
    }

    #[test]
    fn multiple_rows_pick_different_templates() {
        let c = cfg(r#"
version: 1
pipeline:
  sources:
    users_api:  { type: rest, config: { base_url: https://users.example } }
    orders_api: { type: rest, config: { base_url: https://orders.example } }
  sinks:
    archive: { type: jsonl, config: { path: ./out } }
matrix:
  - id: load_users
    source: { ref: users_api, config: { path: /v1/users } }
    sink:   { ref: archive,   config: { path: ./users.jsonl } }
  - id: load_orders
    source: { ref: orders_api, config: { path: /v1/orders } }
    sink:   { ref: archive,    config: { path: ./orders.jsonl } }
"#);
        let nodes = expand(&c).unwrap();
        assert_eq!(nodes.len(), 2);
        let users = nodes.iter().find(|n| n.id == "load_users").unwrap();
        let orders = nodes.iter().find(|n| n.id == "load_orders").unwrap();
        assert_eq!(users.source.config["base_url"], "https://users.example");
        assert_eq!(users.source.config["path"], "/v1/users");
        assert_eq!(orders.source.config["base_url"], "https://orders.example");
        assert_eq!(orders.source.config["path"], "/v1/orders");
        // Both rows share the same sink template but pick different output paths.
        assert_eq!(users.sink.config["path"], "./users.jsonl");
        assert_eq!(orders.sink.config["path"], "./orders.jsonl");
    }

    #[test]
    fn sink_template_with_transforms_errors_at_expand() {
        let yaml = r#"
version: 1
pipeline:
  source:
    type: rest
    config: {}
  sinks:
    bad:
      type: jsonl
      config: { destination: /tmp/x.jsonl }
      transforms:
        - { type: flatten, config: { separator: "_" } }
matrix:
  - id: row
    sink: { ref: bad }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let err = crate::expand::expand(&cfg).expect_err("expected TransformsOnSink");
        match err {
            crate::error::CliError::TransformsOnSink { name } => assert_eq!(name, "bad"),
            other => panic!("expected TransformsOnSink, got {other:?}"),
        }
    }

    #[test]
    fn sink_template_with_inherit_transforms_false_errors_at_expand() {
        let yaml = r#"
version: 1
pipeline:
  source:
    type: rest
    config: {}
  sinks:
    bad:
      type: jsonl
      config: { destination: /tmp/x.jsonl }
      inherit_transforms: false
matrix:
  - id: row
    sink: { ref: bad }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let err = crate::expand::expand(&cfg).expect_err("expected InheritTransformsOnSink");
        match err {
            crate::error::CliError::InheritTransformsOnSink { name } => assert_eq!(name, "bad"),
            other => panic!("expected InheritTransformsOnSink, got {other:?}"),
        }
    }

    fn kinds(transforms: &[crate::config::TransformSpec]) -> Vec<String> {
        transforms.iter().map(|t| t.kind.clone()).collect()
    }

    #[test]
    fn three_layer_concat_default_inherit() {
        let yaml = r#"
version: 1
pipeline:
  transforms:
    - { type: flatten, config: { separator: "_" } }
  sources:
    s:
      type: rest
      config: {}
      transforms:
        - { type: keys_case, config: { mode: snake } }
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
    source: { ref: s }
    transforms:
      - { type: select, config: { fields: [id] } }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(
            kinds(&nodes[0].transforms),
            vec!["flatten", "keys_case", "select"]
        );
    }

    #[test]
    fn source_inherit_false_drops_pipeline_layer() {
        let yaml = r#"
version: 1
pipeline:
  transforms:
    - { type: flatten, config: { separator: "_" } }
  sources:
    s:
      type: rest
      config: {}
      inherit_transforms: false
      transforms:
        - { type: keys_case, config: { mode: snake } }
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
    source: { ref: s }
    transforms:
      - { type: select, config: { fields: [id] } }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(kinds(&nodes[0].transforms), vec!["keys_case", "select"]);
    }

    #[test]
    fn row_inherit_false_drops_pipeline_and_source_layers() {
        let yaml = r#"
version: 1
pipeline:
  transforms:
    - { type: flatten, config: { separator: "_" } }
  sources:
    s:
      type: rest
      config: {}
      transforms:
        - { type: keys_case, config: { mode: snake } }
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
    source: { ref: s }
    inherit_transforms: false
    transforms:
      - { type: select, config: { fields: [id] } }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(kinds(&nodes[0].transforms), vec!["select"]);
    }

    #[test]
    fn both_inherit_false_yields_row_only() {
        let yaml = r#"
version: 1
pipeline:
  transforms:
    - { type: flatten, config: { separator: "_" } }
  sources:
    s:
      type: rest
      config: {}
      inherit_transforms: false
      transforms:
        - { type: keys_case, config: { mode: snake } }
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
    source: { ref: s }
    inherit_transforms: false
    transforms:
      - { type: select, config: { fields: [id] } }
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert_eq!(kinds(&nodes[0].transforms), vec!["select"]);
    }

    #[test]
    fn all_layers_omitted_yields_empty_transforms() {
        let yaml = r#"
version: 1
pipeline:
  source:
    type: rest
    config: {}
  sink:
    type: jsonl
    config: { destination: /tmp/x.jsonl }
matrix:
  - id: row
"#;
        let cfg = crate::config::PipelineConfig::from_text(yaml, std::path::Path::new("test.yaml"))
            .unwrap();
        let nodes = crate::expand::expand(&cfg).unwrap();
        assert!(nodes[0].transforms.is_empty());
    }

    #[test]
    fn now_is_a_valid_builtin_ref_not_an_unknown_id() {
        // A root pipeline referencing ${now.date} must pass expand validation.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: "out-${now.date}.jsonl" } }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        // expand must NOT raise UnknownInterpolationId for `now`.
        assert!(expand(&cfg).is_ok());
    }

    #[test]
    fn now_is_a_reserved_row_id() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
matrix:
  - id: now
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        match expand(&cfg).unwrap_err() {
            CliError::ReservedRowId { id } => assert_eq!(id, "now"),
            other => panic!("expected ReservedRowId, got {other:?}"),
        }
    }

    #[test]
    fn expand_rejects_invalid_adaptive_batch_size_at_load() {
        // Fail-fast: an invalid execution.adaptive_batch_size block must be
        // rejected by `expand` (the gate `faucet validate` uses), not only at
        // run time in the executor.
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
execution:
  adaptive_batch_size:
    enabled: true
    min: 5000
    max: 100
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        assert!(
            err.to_string().contains("adaptive_batch_size.min"),
            "expected adaptive validation error, got: {err}"
        );
    }

    #[test]
    fn expand_accepts_valid_adaptive_batch_size() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
execution:
  adaptive_batch_size:
    enabled: true
    min: 100
    max: 5000
    target_latency_ms: 500
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        assert!(expand(&cfg).is_ok());
    }

    // --- exactly-once delivery gate tests ---

    #[test]
    fn exactly_once_rejects_non_cdc_source() {
        // rest→stdout with exactly_once must fail: rest is not replay-capable.
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: stdout, config: {} }
  state:
    type: memory
    config: {}
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("rest"),
                    "expected source kind in error, got: {msg}"
                );
                assert!(
                    msg.contains("exactly_once") || msg.contains("not supported"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn exactly_once_rejects_non_idempotent_sink() {
        // postgres-cdc→stdout: source is OK but stdout is not idempotent.
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: stdout, config: {} }
  state:
    type: memory
    config: {}
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("stdout"),
                    "expected sink kind in error, got: {msg}"
                );
                assert!(
                    msg.contains("exactly_once") || msg.contains("not supported"),
                    "got: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn exactly_once_accepted_with_cdc_source_idempotent_sink_and_state() {
        // postgres-cdc → sqlite + a *durable* state store → must expand
        // successfully. (Must not be `memory`: exactly-once needs cross-restart
        // durability — see `exactly_once_rejects_memory_state`.)
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: sqlite, config: {} }
  state:
    type: file
    config: { path: "/tmp/faucet-eo-state.json" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].delivery, faucet_core::DeliveryMode::ExactlyOnce);
        assert_eq!(
            nodes[0].delivery_guarantee,
            faucet_core::DeliveryGuarantee::EffectivelyOnce(
                faucet_core::EffectivelyOnceMechanism::AtomicWatermark
            )
        );
    }

    #[test]
    fn exactly_once_accepted_via_keyed_upsert_with_any_source() {
        // rest → postgres with `write_mode: upsert` + `key`: accepted under
        // exactly_once via the keyed-upsert mechanism (#292) — no CDC source,
        // no state store required.
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:
    type: postgres
    config:
      connection_url: "postgres://localhost/db"
      table_name: t
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(
            nodes[0].delivery_guarantee,
            faucet_core::DeliveryGuarantee::EffectivelyOnce(
                faucet_core::EffectivelyOnceMechanism::KeyedUpsert
            )
        );
    }

    #[test]
    fn exactly_once_kafka_source_accepted_with_atomic_sink() {
        // kafka → sqlite + durable state: the kafka source's offset bookmarks
        // qualify it for the atomic-watermark mechanism (#291).
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source:
    type: kafka
    config: { brokers: "localhost:9092", topics: [t], group_id: g, max_messages: 10 }
  sink:   { type: sqlite, config: {} }
  state:
    type: file
    config: { path: "/tmp/faucet-eo-kafka-state.json" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(
            nodes[0].delivery_guarantee,
            faucet_core::DeliveryGuarantee::EffectivelyOnce(
                faucet_core::EffectivelyOnceMechanism::AtomicWatermark
            )
        );
    }

    #[test]
    fn exactly_once_source_error_hints_keyed_upsert_for_capable_sink() {
        // rest → postgres (no write_mode): the source error should point at
        // the keyed-upsert alternative since postgres is upsert-capable.
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:
    type: postgres
    config:
      connection_url: "postgres://localhost/db"
      table_name: t
      column_mapping: auto_map
  state:
    type: file
    config: { path: "/tmp/faucet-eo-hint-state.json" }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => assert!(
                msg.contains("write_mode: upsert"),
                "expected keyed-upsert hint, got: {msg}"
            ),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn derived_guarantee_is_at_least_once_by_default() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: stdout, config: {} }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        assert_eq!(
            nodes[0].delivery_guarantee,
            faucet_core::DeliveryGuarantee::AtLeastOnce
        );
    }

    #[test]
    fn exactly_once_rejects_memory_state() {
        // A non-durable `memory` store defeats the cross-restart watermark
        // guarantee, so it must be rejected at config-load (F24).
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: sqlite, config: {} }
  state:
    type: memory
    config: {}
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => assert!(
                msg.contains("durable") && msg.contains("memory"),
                "expected durable/memory mention, got: {msg}"
            ),
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn exactly_once_rejects_missing_state_store() {
        // Valid CDC pair but no state block → must fail with "requires a state store".
        let yaml = r#"
version: 1
delivery: exactly_once
pipeline:
  source: { type: postgres-cdc, config: {} }
  sink:   { type: sqlite, config: {} }
"#;
        let cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = expand(&cfg).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("state store") || msg.contains("state"),
                    "expected state-store mention in error, got: {msg}"
                );
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn rejects_upsert_on_unsupported_sink() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: jsonl, config: { path: "out.jsonl", write_mode: upsert, key: [id] } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("write_mode") && msg.contains("upsert") && msg.contains("jsonl"),
            "{msg}"
        );
    }

    #[test]
    fn rejects_upsert_without_key() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: postgres, config: { connection_url: "postgres://x", table_name: t, column_mapping: auto_map, write_mode: upsert } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("key"), "{msg}");
    }

    #[test]
    fn accepts_upsert_on_postgres_with_key() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: postgres, config: { connection_url: "postgres://x", table_name: t, column_mapping: auto_map, write_mode: upsert, key: [id] } }
"#);
        assert!(expand(&c).is_ok());
    }

    #[test]
    fn bigquery_upsert_passes_write_mode_gate() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: bigquery, config: { project_id: p, dataset_id: d, table_id: t, auth: { type: application_default }, write_mode: upsert, key: [id] } }
"#);
        assert!(expand(&c).is_ok());
    }

    #[test]
    fn accepts_append_by_default_on_any_sink() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: jsonl, config: { path: "out.jsonl" } }
"#);
        assert!(expand(&c).is_ok());
    }

    #[test]
    fn rejects_delete_without_key() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: mongodb, config: { connection_url: "mongodb://x", database: d, collection: c, write_mode: delete } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("delete") && msg.contains("key"), "{msg}");
    }

    #[test]
    fn rejects_unknown_write_mode() {
        let c = cfg(r#"
version: 1
name: t
pipeline:
  source: { type: rest, config: { url: "http://x" } }
  sink:   { type: postgres, config: { connection_url: "postgres://x", table_name: t, column_mapping: auto_map, write_mode: replace } }
"#);
        let err = expand(&c).unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("unknown write_mode") && msg.contains("replace"),
            "{msg}"
        );
    }

    #[test]
    fn rejects_poison_dlq_action_without_dlq() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
resilience:
  poison: { max_row_attempts: 3, action: dlq }
"#);
        let err = expand(&c).unwrap_err();
        assert!(
            matches!(&err, CliError::Config(m) if m.contains("poison.action=dlq") && m.contains("dlq:")),
            "got: {err:?}"
        );
    }

    #[test]
    fn accepts_poison_dlq_action_with_dlq() {
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
  dlq:
    sink: { type: jsonl, config: { path: ./dead.jsonl } }
resilience:
  poison: { max_row_attempts: 3, action: dlq }
"#);
        let nodes = expand(&c).expect("poison.action=dlq with a dlq: block should validate");
        assert_eq!(nodes.len(), 1);
    }

    #[test]
    fn accepts_poison_drop_action_without_dlq() {
        // action=drop discards rows in place, so no DLQ is required.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
resilience:
  poison: { max_row_attempts: 3, action: drop }
"#);
        let nodes = expand(&c).expect("poison.action=drop needs no dlq");
        assert_eq!(nodes.len(), 1);
    }

    // --- schema-drift composition gate tests ---

    #[test]
    fn evolve_on_non_evolvable_sink_rejected() {
        // jsonl is not evolution-capable; on_drift: evolve must fail.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
  schema:
    on_drift: evolve
"#);
        let err = expand(&c).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("evolve"),
                    "expected evolve mention, got: {msg}"
                );
                assert!(msg.contains("jsonl"), "expected sink kind, got: {msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn quarantine_drift_without_dlq_rejected() {
        // on_drift: quarantine requires a dlq: block.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: postgres, config: {} }
  schema:
    on_drift: quarantine
"#);
        let err = expand(&c).unwrap_err();
        match &err {
            CliError::Config(msg) => {
                assert!(
                    msg.contains("quarantine"),
                    "expected quarantine mention, got: {msg}"
                );
                assert!(msg.contains("dlq") || msg.contains("DLQ"), "got: {msg}");
            }
            other => panic!("expected Config error, got {other:?}"),
        }
    }

    #[test]
    fn evolve_on_postgres_passes() {
        // postgres is evolution-capable; on_drift: evolve must expand.
        let c = cfg(r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: postgres, config: {} }
  schema:
    on_drift: evolve
"#);
        assert!(expand(&c).is_ok());
    }
}
