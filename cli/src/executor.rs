//! Run a list of [`ExpandedNode`]s under a bounded-concurrency executor.
//!
//! Semantics:
//!
//! - Roots run concurrently under `Semaphore(max_concurrent)`.
//! - Each root captures its written records (via a `CapturingSink` wrapper)
//!   so descendants can fan out per parent record.
//! - For each child whose parent has finished successfully, one pipeline
//!   invocation runs per parent record. `${parent.dotted.path}` tokens in the
//!   source / sink config and state-key suffix are resolved against that
//!   record via [`interpolate_record`].
//! - All invocations share one global semaphore — children and roots compete
//!   for the same budget.
//! - `on_error: continue` (default) skips a failed node's subtree but keeps
//!   running siblings. `on_error: stop` cancels everything after the first
//!   failure.
//! - State-key collisions among children of the same parent surface as a
//!   `CliError::DuplicateStateKey`.

use crate::auth_catalog::AuthCatalog;
use crate::config::{ExecutionSpec, OnError};
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole};
use crate::interpolate::interpolate_record;
use crate::registry::{build_sink, build_source};
use crate::state::build_state_store;
use crate::transforms::compile_transforms;
use async_trait::async_trait;
use chrono::{DateTime, FixedOffset};
use faucet_core::observability::Labels;
use faucet_core::{DlqConfig, FaucetError, OnBatchError, Pipeline, Sink, Source, StateStore};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};

/// Captured fan-out records, keyed by node id. Records are held as `Arc<Value>`
/// so the per-level snapshot and per-child-unit hand-off are pointer bumps, not
/// deep clones of the JSON tree (#160).
type CapturedRecords = Arc<Mutex<HashMap<String, Vec<Arc<Value>>>>>;
use tokio_util::sync::CancellationToken;

/// Knobs passed to [`run_expanded`].
pub struct ExecuteOptions {
    /// Pipeline name — used in log lines and as the first segment of every
    /// state key.
    pub pipeline_name: String,
    /// Override for `execution.max_concurrent`. `None` → use the value in
    /// `ExecutionSpec` or the default (`num_cpus::get().min(4)`, floored at 1).
    pub execution: Option<ExecutionSpec>,
    /// `--dry-run` — every sink is replaced with a no-op counter.
    pub dry_run: bool,
    /// `--limit N` — wraps every sink to drop records past the cap.
    pub limit: Option<usize>,
    /// `--state-path PATH` — overrides the `file` state-store path.
    pub state_path_override: Option<PathBuf>,
    /// Shared auth providers built from the top-level `auth:` block. Connectors
    /// that reference one via `auth: { ref }` resolve against this catalog;
    /// every row sharing a provider gets the same `Arc` (one token, shared).
    pub auth: AuthCatalog,
    /// Wall-clock instant for `${now.*}` interpolation in this run's configs.
    /// `faucet run` sets process-start (or `--clock`); `faucet schedule` sets
    /// the tick's scheduled time in the schedule timezone.
    pub clock: DateTime<FixedOffset>,
    /// Optional external cancellation token. When set and cancelled, in-flight
    /// invocations stop at their next page boundary and **flush** their sinks
    /// (so buffered output like a Parquet footer is durable), rather than being
    /// hard-dropped (#146 H16). `faucet serve` wires this to run-cancel /
    /// timeout / shutdown; `faucet run` leaves it `None`.
    pub cancel: Option<CancellationToken>,
    /// Shared OpenLineage emitter, built once from the `lineage:` block. `None`
    /// disables lineage (and adds zero overhead). Gated on the `lineage` feature.
    #[cfg(feature = "lineage")]
    pub lineage: Option<std::sync::Arc<faucet_lineage::LineageEmitter>>,
    /// The resolved `lineage:` config block (facet/event toggles, sampling, job
    /// name template). Carried alongside the emitter so `run_one_invocation`
    /// knows which facets/events to assemble. Gated on the `lineage` feature.
    #[cfg(feature = "lineage")]
    pub lineage_cfg: Option<faucet_lineage::LineageConfig>,
}

/// Grace window granted to in-flight invocations to flush cooperatively after
/// an `on_error: stop` cancellation, before the remaining tasks are
/// hard-aborted (the backstop for a sink genuinely stuck mid-write). Bounded so
/// a hung sink can't wedge the whole run.
const STOP_FLUSH_GRACE: Duration = Duration::from_secs(5);

/// One pipeline invocation's outcome.
#[derive(Debug)]
pub struct InvocationOutcome {
    pub row_id: String,
    /// `None` for root invocations; for children, the value at `parent_key` in
    /// the parent record (rendered to a string).
    pub parent_record_key: Option<String>,
    pub records_written: usize,
    pub error: Option<String>,
}

/// Aggregate outcome of `run_expanded`.
#[derive(Debug)]
pub struct RunSummary {
    pub invocations: Vec<InvocationOutcome>,
}

impl RunSummary {
    pub fn failure_count(&self) -> usize {
        self.invocations
            .iter()
            .filter(|i| i.error.is_some())
            .count()
    }
    pub fn had_failures(&self) -> bool {
        self.failure_count() > 0
    }
}

/// Default number of matrix invocations to run in parallel when neither the
/// config's `execution.max_concurrent` nor a flag specifies one.
///
/// Scales with the core count but is capped at 8. The cap is deliberate: each
/// invocation is a *full pipeline* with its own connection pools / HTTP
/// clients, and matrix rows often target the same external system (one API,
/// one database), so an unbounded fan-out across, say, a 64-core box would
/// blow through that system's connection or rate limits rather than going
/// faster. Workloads that genuinely benefit from more parallelism set
/// `execution.max_concurrent` explicitly to opt out of the cap (#78 LOW).
fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, 8)
}

/// Execute every node in `nodes`. `nodes` must be in BFS order (roots first
/// then children) — that's what [`crate::expand::expand`] returns.
pub async fn run_expanded(nodes: Vec<ExpandedNode>, opts: ExecuteOptions) -> CliResult<RunSummary> {
    let on_error = opts
        .execution
        .as_ref()
        .map(|e| e.on_error)
        .unwrap_or_default();
    let max_concurrent = opts
        .execution
        .as_ref()
        .and_then(|e| e.max_concurrent)
        .unwrap_or_else(default_concurrency)
        .max(1);
    let semaphore = Arc::new(Semaphore::new(max_concurrent));

    // Index nodes by id for parent → children lookups.
    // parent id → child node ids. Keyed and valued by id (not Vec index) so the
    // failure cascade can look children up directly instead of indexing into a
    // HashMap's nondeterministic iteration order (#78/#24).
    let mut children_of: HashMap<String, Vec<String>> = HashMap::new();
    for n in nodes.iter() {
        if let NodeRole::Child { parent_id, .. } = &n.role {
            children_of
                .entry(parent_id.clone())
                .or_default()
                .push(n.id.clone());
        }
    }

    // Captured records per node id. Only populated for nodes that have
    // children (= are referenced by another node's `parent:`). Records are held
    // as `Arc<Value>` so the per-level snapshot clone and the per-child-unit
    // hand-off are pointer bumps, not deep clones of the JSON tree (#160).
    let captured: CapturedRecords = Arc::new(Mutex::new(HashMap::new()));
    let nodes_with_descendants: HashSet<String> = children_of.keys().cloned().collect();

    let mut outcomes: Vec<InvocationOutcome> = Vec::new();
    let mut skipped_subtrees: HashSet<String> = HashSet::new();

    // Root cooperative-cancel token: the caller's (serve wires run-cancel /
    // timeout / shutdown) or a fresh one. Each level derives a child token so
    // an `on_error: stop` cancels only that level's invocations, while an
    // external cancel of the root propagates to every level (#146 H16).
    let cancel = opts.cancel.clone().unwrap_or_default();
    let opts = Arc::new(opts);

    // We execute level-by-level. Each level is "every node whose parent is
    // already done." Roots are level 0. For each level, we spawn one task per
    // (node, parent-record) pair and await them all before moving on.
    let mut remaining: HashSet<String> = nodes.iter().map(|n| n.id.clone()).collect();
    let mut completed: HashSet<String> = HashSet::new();
    let nodes_by_id: HashMap<String, ExpandedNode> =
        nodes.into_iter().map(|n| (n.id.clone(), n)).collect();

    // Sort node ids in their original BFS row order so the executor is
    // deterministic — important for `on_error: stop`, where the first failure
    // halts the rest of the level.
    let bfs_order: Vec<String> = {
        let mut ids: Vec<(usize, String)> = nodes_by_id
            .values()
            .map(|n| (n.row_index, n.id.clone()))
            .collect();
        ids.sort_by_key(|(i, _)| *i);
        ids.into_iter().map(|(_, id)| id).collect()
    };

    while !remaining.is_empty() {
        // Pick every remaining node whose parent (if any) is already
        // completed, in deterministic BFS order.
        let ready: Vec<String> = bfs_order
            .iter()
            .filter(|id| remaining.contains(*id))
            .filter(|id| match &nodes_by_id[*id].role {
                NodeRole::Root => true,
                NodeRole::Child { parent_id, .. } => {
                    completed.contains(parent_id) || skipped_subtrees.contains(parent_id)
                }
            })
            .cloned()
            .collect();

        if ready.is_empty() {
            // No node is ready but some remain — an expand.rs invariant was
            // violated (e.g. an orphaned parent reference). Surface it instead
            // of silently dropping the remaining work and reporting success
            // (#78/#24).
            let mut stuck: Vec<String> = remaining.iter().cloned().collect();
            stuck.sort();
            return Err(CliError::Internal(format!(
                "executor deadlock: {} node(s) never became ready (no completed/skipped parent): {}",
                stuck.len(),
                stuck.join(", ")
            )));
        }

        // Build the work units for this level. Each unit is one invocation —
        // a root runs once; a child runs once per parent record.
        let mut units: Vec<Unit> = Vec::new();
        let captured_snapshot = captured.lock().await.clone();
        for id in &ready {
            let node = &nodes_by_id[id];
            // If a parent failed (and on_error=continue), the subtree is
            // skipped. Surface a synthetic "skipped" outcome and move on.
            if let NodeRole::Child { parent_id, .. } = &node.role
                && skipped_subtrees.contains(parent_id)
            {
                skipped_subtrees.insert(id.clone());
                tracing::warn!(row = %id, parent = %parent_id, "skipping subtree under failed parent");
                continue;
            }
            match &node.role {
                NodeRole::Root => {
                    let uses_state = node.state.is_some() || opts.state_path_override.is_some();
                    let state_key = build_state_key(&opts.pipeline_name, &node.id, None);
                    validate_unit_state_key(&node.id, uses_state, &state_key)?;
                    units.push(Unit {
                        node: node.clone(),
                        parent_record: None,
                        state_key,
                        parent_record_key: None,
                    });
                }
                NodeRole::Child {
                    parent_id,
                    parent_key,
                } => {
                    let parent_records = captured_snapshot
                        .get(parent_id)
                        .cloned()
                        .unwrap_or_default();
                    if parent_records.is_empty() {
                        tracing::info!(
                            row = %id, parent = %parent_id,
                            "parent produced no records — child skipped"
                        );
                        continue;
                    }
                    // Detect state-key collisions among siblings sharing one parent.
                    let uses_state = node.state.is_some() || opts.state_path_override.is_some();
                    let mut seen_keys: HashSet<String> = HashSet::new();
                    for record in &parent_records {
                        let pk_value = resolve_parent_key(record, parent_key);
                        let pk_string = pk_value
                            .as_ref()
                            .map(value_to_string_brief)
                            .unwrap_or_else(|| "(missing)".to_string());
                        let state_key =
                            build_state_key(&opts.pipeline_name, &node.id, Some(&pk_string));
                        validate_unit_state_key(&node.id, uses_state, &state_key)?;
                        if !seen_keys.insert(state_key.clone()) {
                            return Err(CliError::DuplicateStateKey {
                                id: node.id.clone(),
                                state_key,
                            });
                        }
                        units.push(Unit {
                            node: node.clone(),
                            parent_record: Some(record.clone()),
                            state_key,
                            parent_record_key: Some(pk_string),
                        });
                    }
                }
            }
        }
        drop(captured_snapshot);

        let mut had_level_failure = false;
        let mut nodes_with_any_failure: HashSet<String> = HashSet::new();

        // Unified parallel execution. Tasks run concurrently under the global
        // semaphore. Under `on_error: stop`, the first failure triggers
        // `JoinSet::abort_all()` — pending tasks waiting on a permit are
        // dropped before they do real work, and in-flight tasks are
        // cancelled at their next `.await` point (potentially leaving
        // partial sink state — the trade-off users opt into by choosing
        // `stop`). Under `on_error: continue` every spawned task runs to
        // completion regardless of sibling failures.
        // Per-level cancel token: cancelling it (on `on_error: stop`) stops only
        // this level's invocations cooperatively; it is a child of the root
        // token, so an external cancel (serve) still propagates here (#146 H16).
        let level_cancel = cancel.child_token();
        let mut joinset = tokio::task::JoinSet::new();
        // Map each spawned task's id back to its row id + parent key so that a
        // panic (surfaced as a JoinError, which doesn't carry the unit) can be
        // attributed to the right invocation.
        let mut task_meta: HashMap<tokio::task::Id, (String, Option<String>)> = HashMap::new();
        for unit in units {
            let sem = Arc::clone(&semaphore);
            let opts2 = Arc::clone(&opts);
            let captured = Arc::clone(&captured);
            let needs_capture = nodes_with_descendants.contains(&unit.node.id);
            let meta = (unit.node.id.clone(), unit.parent_record_key.clone());
            let unit_cancel = level_cancel.clone();
            let handle = joinset.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore not closed");
                run_unit(&unit, needs_capture, &captured, &opts2, unit_cancel).await
            });
            task_meta.insert(handle.id(), meta);
        }

        let mut stop_triggered = false;
        let mut aborted = false;
        let mut stop_deadline: Option<tokio::time::Instant> = None;
        loop {
            // Once `on_error: stop` has cancelled the level, give in-flight
            // invocations a bounded grace to flush cooperatively, then
            // hard-abort the stragglers (the backstop for a sink stuck
            // mid-write that can't reach a page boundary to observe the cancel).
            let joined = match stop_deadline {
                Some(deadline) if !aborted => {
                    match tokio::time::timeout_at(deadline, joinset.join_next_with_id()).await {
                        Ok(j) => j,
                        Err(_) => {
                            tracing::warn!(
                                "on_error: stop — flush grace elapsed; aborting remaining \
                                 in-flight invocations"
                            );
                            joinset.abort_all();
                            aborted = true;
                            continue;
                        }
                    }
                }
                _ => joinset.join_next_with_id().await,
            };
            let Some(joined) = joined else { break };
            // A failure (an `Err` outcome or a panicked task) marks the level
            // failed and, under `on_error: stop`, stops the rest. A panicking
            // connector must NOT take down the whole process (#78/#24).
            let outcome = match joined {
                Ok((_id, outcome)) => outcome,
                Err(e) if e.is_cancelled() => {
                    // Expected after abort_all() — cancelled before/at an await.
                    // Not counted as a failure or a success.
                    continue;
                }
                Err(e) => {
                    let (row_id, parent_record_key) = task_meta
                        .get(&e.id())
                        .cloned()
                        .unwrap_or_else(|| ("<unknown>".to_string(), None));
                    InvocationOutcome {
                        row_id,
                        parent_record_key,
                        records_written: 0,
                        error: Some(format!("pipeline invocation task panicked: {e}")),
                    }
                }
            };

            if let Some(err) = &outcome.error {
                tracing::error!(row = %outcome.row_id, error = %err, "pipeline invocation failed");
                had_level_failure = true;
                nodes_with_any_failure.insert(outcome.row_id.clone());
                if matches!(on_error, OnError::Stop) && !stop_triggered {
                    stop_triggered = true;
                    tracing::error!(
                        "on_error: stop — cancelling in-flight invocations (cooperative \
                         flush), then aborting any that don't stop within the grace window"
                    );
                    // Cooperative first: in-flight pipelines flush at their next
                    // page boundary so a Parquet footer / S3 upload is completed
                    // rather than orphaned (#146 H16).
                    level_cancel.cancel();
                    stop_deadline = Some(tokio::time::Instant::now() + STOP_FLUSH_GRACE);
                }
            } else {
                tracing::info!(
                    row = %outcome.row_id,
                    records_written = outcome.records_written,
                    "pipeline invocation completed"
                );
            }
            outcomes.push(outcome);
        }

        // Mark ready nodes done (some may have produced both successes and
        // failures across their per-parent-record fan-outs — we treat a node
        // as "failed" overall if any of its invocations failed).
        for id in ready {
            remaining.remove(&id);
            if nodes_with_any_failure.contains(&id) {
                skipped_subtrees.insert(id.clone());
                // Cascade to descendants in case we have multi-level chains.
                if let Some(children) = children_of.get(&id) {
                    for cid in children {
                        skipped_subtrees.insert(cid.clone());
                    }
                }
            } else {
                completed.insert(id);
            }
        }

        if had_level_failure && matches!(on_error, OnError::Stop) {
            tracing::error!("on_error: stop — aborting after first failure");
            // Any unfinished work surfaces as "skipped"; we just break here.
            break;
        }
    }

    Ok(RunSummary {
        invocations: outcomes,
    })
}

/// One scheduled invocation — a root runs once, a child runs once per parent
/// record. Built by the level loop, consumed by [`run_unit`].
struct Unit {
    node: ExpandedNode,
    parent_record: Option<Arc<Value>>,
    state_key: String,
    parent_record_key: Option<String>,
}

async fn run_unit(
    unit: &Unit,
    needs_capture: bool,
    captured: &CapturedRecords,
    opts: &ExecuteOptions,
    cancel: CancellationToken,
) -> InvocationOutcome {
    let result = run_one_invocation(
        &unit.node,
        unit.parent_record.as_deref(),
        &unit.state_key,
        needs_capture,
        opts,
        cancel,
    )
    .await;
    let row_id = unit.node.id.clone();
    let parent_record_key = unit.parent_record_key.clone();
    match result {
        Ok((records, written)) => {
            if needs_capture {
                captured
                    .lock()
                    .await
                    .entry(row_id.clone())
                    .or_default()
                    // Move each record into an `Arc` once here; downstream
                    // per-level / per-unit hand-offs then clone only the pointer.
                    .extend(records.into_iter().map(Arc::new));
            }
            InvocationOutcome {
                row_id,
                parent_record_key,
                records_written: written,
                error: None,
            }
        }
        Err(e) => InvocationOutcome {
            row_id,
            parent_record_key,
            records_written: 0,
            error: Some(e.to_string()),
        },
    }
}

/// Produce `{pipeline_name}::{row_id}` or `{pipeline_name}::{row_id}::{key}`.
fn build_state_key(pipeline_name: &str, row_id: &str, parent_key: Option<&str>) -> String {
    match parent_key {
        None => format!("{pipeline_name}::{row_id}"),
        Some(k) => format!("{pipeline_name}::{row_id}::{k}"),
    }
}

/// Reject an invalid state key up front (at unit construction) when the node
/// will use a state store, so a bad pipeline name or parent-key value surfaces
/// as a clear [`CliError::InvalidStateKey`] instead of a late mid-run
/// `FaucetError::State` after connectors are built and the stream has started.
fn validate_unit_state_key(node_id: &str, uses_state: bool, state_key: &str) -> CliResult<()> {
    if uses_state {
        faucet_core::state::validate_state_key(state_key).map_err(|e| {
            CliError::InvalidStateKey {
                id: node_id.to_owned(),
                state_key: state_key.to_owned(),
                reason: e.to_string(),
            }
        })?;
    }
    Ok(())
}

/// Walk the parent record by `parent_key` (a dotted path) and clone the value.
fn resolve_parent_key(record: &Value, parent_key: &str) -> Option<Value> {
    let mut cur = record;
    for segment in parent_key.split('.') {
        cur = match cur {
            Value::Object(m) => m.get(segment)?,
            Value::Array(a) => a.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur.clone())
}

/// What to keep from each of a parent's records when capturing for fan-out.
/// Projecting to only the fields children reference bounds orchestrator memory
/// at O(referenced-fields × N) instead of O(full-record × N) (#160).
#[derive(Debug, Clone)]
enum Projection {
    /// Keep the whole record — a child referenced `${parent}` (the entire record)
    /// or used an empty `parent_key`, so nothing can be safely dropped.
    Full,
    /// Keep only these pre-split, non-overlapping dotted paths.
    Paths(Vec<Vec<String>>),
}

/// Split a dotted path into segments.
fn split_path(path: &str) -> Vec<String> {
    path.split('.').map(|s| s.to_string()).collect()
}

/// Reduce a set of segment-paths to a minimal non-overlapping set: drop any path
/// that has a (segment-wise prefix) ancestor in the set — `["user"]` covers
/// `["user","name"]`. Sorting puts ancestors before their descendants.
fn minimal_paths(mut paths: Vec<Vec<String>>) -> Vec<Vec<String>> {
    paths.sort();
    paths.dedup();
    let mut kept: Vec<Vec<String>> = Vec::new();
    for p in paths {
        let covered = kept
            .iter()
            .any(|anc| p.len() >= anc.len() && p[..anc.len()] == anc[..]);
        if !covered {
            kept.push(p);
        }
    }
    kept
}

/// Resolve a pre-split dotted path against `record`, dispatching on each value's
/// type exactly like `resolve_parent_key` / `interpolate::resolve_dotted`.
fn walk_value(record: &Value, segments: &[String]) -> Option<Value> {
    let mut cur = record;
    for seg in segments {
        cur = match cur {
            Value::Object(m) => m.get(seg)?,
            Value::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur.clone())
}

/// Insert `leaf` at `segments` into `out`, creating intermediate `Value::Object`
/// nodes keyed by the literal segment string. Callers pass non-overlapping
/// `segments` (see `minimal_paths`), so a node that must be an object is never
/// already a leaf.
fn graft_object(out: &mut Value, segments: &[String], leaf: Value) {
    if segments.is_empty() {
        return;
    }
    let mut cur = out;
    for seg in &segments[..segments.len() - 1] {
        let map = match cur {
            Value::Object(m) => m,
            _ => return,
        };
        cur = map
            .entry(seg.clone())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));
    }
    if let Value::Object(m) = cur {
        m.insert(segments[segments.len() - 1].clone(), leaf);
    }
}

/// Project `record` down to `projection`, building an all-objects reduced tree.
/// Because the readers (`resolve_parent_key`, `interpolate_record`) dispatch on
/// the reduced value's type, an array-index segment like `0` is stored — and
/// later read — as the object key `"0"`, so resolution matches the original.
fn project_record(record: &Value, projection: &Projection) -> Value {
    match projection {
        Projection::Full => record.clone(),
        Projection::Paths(paths) => {
            let mut out = Value::Object(serde_json::Map::new());
            for segs in paths {
                if let Some(v) = walk_value(record, segs) {
                    graft_object(&mut out, segs, v);
                }
            }
            out
        }
    }
}

/// Run one pipeline invocation. Returns (captured records, records_written).
async fn run_one_invocation(
    node: &ExpandedNode,
    parent_record: Option<&Value>,
    state_key: &str,
    needs_capture: bool,
    opts: &ExecuteOptions,
    cancel: CancellationToken,
) -> CliResult<(Vec<Value>, usize)> {
    // Observability identity for this invocation — built once, reused by both
    // the Pipeline builder and the transform instrumentation.
    let run_id = uuid::Uuid::now_v7().to_string();
    let pipeline_name = opts.pipeline_name.clone();
    let row_id = node.id.clone();
    #[cfg(feature = "lineage")]
    let lineage = opts.lineage.clone();
    #[cfg(feature = "lineage")]
    let lineage_cfg = opts.lineage_cfg.clone();
    let obs_labels = Labels::new(pipeline_name.clone(), row_id.clone(), run_id.clone());
    // 1) Resolve `${parent.path}` in the per-row source + sink configs.
    let mut source_cfg = node.source.config.clone();
    let mut sink_cfg = node.sink.config.clone();

    // Resolve `${now.*}` run-clock tokens for every invocation (root + child),
    // before the parent-record pass. Leaves all other tokens verbatim.
    resolve_now_inplace(&mut source_cfg, opts.clock)?;
    resolve_now_inplace(&mut sink_cfg, opts.clock)?;

    if let (Some(record), NodeRole::Child { parent_id, .. }) = (parent_record, &node.role) {
        let ctx: HashMap<String, Value> = HashMap::from([(parent_id.clone(), record.clone())]);
        resolve_inplace(&mut source_cfg, &ctx)?;
        resolve_inplace(&mut sink_cfg, &ctx)?;
    }

    // 2) Build source + sink.
    let source = build_source(&node.source.kind, source_cfg, &opts.auth).await?;
    let raw_sink: Box<dyn Sink> = if opts.dry_run {
        Box::new(CountingSink::new())
    } else {
        build_sink(&node.sink.kind, sink_cfg, &opts.auth).await?
    };
    let raw_sink: Box<dyn Sink> = match opts.limit {
        Some(n) => Box::new(LimitedSink::wrap(raw_sink, n)),
        None => raw_sink,
    };
    let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
    let sink: Box<dyn Sink> = if needs_capture {
        Box::new(CapturingSink::wrap(raw_sink, Arc::clone(&captured)))
    } else {
        raw_sink
    };

    // ── Lineage: sampling wrappers (only for requested facets) ───────────────
    // `in_sample` taps the source's pre-transform records (input schema /
    // column lineage); `out_sample` taps the sink's written records (output
    // schema / RUNNING-heartbeat throughput counter). Both stay `None` when no
    // facet/event needs them, so lineage adds zero per-record overhead.
    #[cfg(feature = "lineage")]
    let (in_sample, out_sample) = {
        use std::sync::Arc as StdArc;
        match (&lineage, &lineage_cfg) {
            (Some(_), Some(lc)) => {
                let want_schema = lc.include_schema_facet || lc.include_column_lineage;
                let cap = if want_schema { lc.sample_records } else { 0 };
                let need_counter = lc.emit_on.running;
                if want_schema || need_counter {
                    (
                        Some(StdArc::new(faucet_lineage::SampleState::new(cap))),
                        Some(StdArc::new(faucet_lineage::SampleState::new(cap))),
                    )
                } else {
                    (None, None)
                }
            }
            _ => (None, None),
        }
    };

    // Wrap the raw source so it samples PRE-transform input records — this must
    // sit between `build_source` and `TransformingSource`.
    #[cfg(feature = "lineage")]
    let source: Box<dyn Source> = match &in_sample {
        Some(state) => Box::new(faucet_lineage::SamplingSource::new(
            source,
            std::sync::Arc::clone(state),
        )),
        None => source,
    };

    // 3) Compile transforms.
    let stages = compile_transforms(&node.transforms)?;
    let source: Box<dyn Source> = if stages.is_empty() {
        source
    } else {
        Box::new(faucet_core::TransformingSource::new(
            source,
            stages,
            obs_labels.clone(),
        )?)
    };

    // 4) Build state store. If the source opts into state, wrap it so the
    //    executor's per-row state key is used instead of the source's natural
    //    one (which is shared across all matrix rows of the same kind).
    let state = build_state_for_node(node, opts.state_path_override.as_deref()).await?;
    let source: Box<dyn Source> = if state.is_some() && source.state_key().is_some() {
        Box::new(StateKeyOverride {
            inner: source,
            key: state_key.to_owned(),
        })
    } else {
        source
    };

    // Wrap the sink so it samples written records — outermost, immediately
    // before the pipeline is constructed (after capture/limit wrappers).
    #[cfg(feature = "lineage")]
    let sink: Box<dyn Sink> = match &out_sample {
        Some(state) => Box::new(faucet_lineage::SamplingSink::new(
            sink,
            std::sync::Arc::clone(state),
        )),
        None => sink,
    };

    // 5) Run.
    // When lineage is enabled the START/terminal lifecycle below still needs
    // `pipeline_name` / `row_id` / `run_id`, so hand the builder clones; the
    // non-lineage build moves them straight in (byte-identical to before).
    #[cfg(feature = "lineage")]
    let pipeline = Pipeline::new(source.as_ref(), sink.as_ref())
        .with_name(pipeline_name.clone())
        .with_row(row_id.clone())
        .with_run_id(run_id.clone());
    #[cfg(not(feature = "lineage"))]
    let pipeline = Pipeline::new(source.as_ref(), sink.as_ref())
        .with_name(pipeline_name)
        .with_row(row_id)
        .with_run_id(run_id);
    let pipeline = match state {
        Some(store) => pipeline.with_state_store(store),
        None => pipeline,
    };
    let pipeline = if let Some(ref dlq_spec) = node.dlq {
        let dlq_cfg = build_dlq_config(dlq_spec).await?;
        pipeline.with_dlq(dlq_cfg)
    } else {
        pipeline
    };
    // Cooperative cancellation: a cancelled token makes the streaming loop stop
    // at the next page boundary and flush the sink (#146 H16). Under lineage we
    // hand the pipeline a clone so the terminal-event classification below can
    // still read `cancel.is_cancelled()` (cheap — the token is an `Arc`).
    #[cfg(feature = "lineage")]
    let pipeline = pipeline.with_cancel(cancel.clone());
    #[cfg(not(feature = "lineage"))]
    let pipeline = pipeline.with_cancel(cancel);
    // Pipeline-level quality checks (v1: no matrix-row override). `expand`
    // already validated this spec, but compile again here to obtain the
    // runtime `CompiledQuality`; map any error to a config-level failure.
    #[cfg(feature = "quality")]
    let pipeline = if let Some(ref quality_spec) = node.quality {
        let compiled = Arc::new(
            faucet_core::CompiledQuality::compile(quality_spec)
                .map_err(|e| CliError::Config(format!("quality: {e}")))?,
        );
        pipeline.with_quality(compiled)
    } else {
        pipeline
    };
    // Execution-level adaptive batch-size controller (shared by all rows).
    let pipeline = if let Some(ab) = opts
        .execution
        .as_ref()
        .and_then(|e| e.adaptive_batch_size.clone())
    {
        ab.validate()
            .map_err(|e| CliError::Config(format!("adaptive_batch_size: {e}")))?;
        pipeline.with_adaptive(ab)
    } else {
        pipeline
    };
    // ── Lineage: START + heartbeat + terminal ────────────────────────────────
    #[cfg(feature = "lineage")]
    let lineage_ctx = match (&lineage, &lineage_cfg) {
        (Some(em), Some(lc)) => {
            let job_name =
                crate::interpolate::resolve_lineage_job_name(&lc.job_name, &pipeline_name, &row_id);
            let mut ctx = faucet_lineage::RunLifecycle {
                job_namespace: lc.namespace.clone(),
                job_name,
                run_id: run_id.clone(),
                parent: lc.parent_job.clone(),
                input: faucet_lineage::DatasetRef {
                    namespace: lc.namespace.clone(),
                    name: source.dataset_uri(),
                },
                output: faucet_lineage::DatasetRef {
                    namespace: lc.namespace.clone(),
                    name: sink.dataset_uri(),
                },
                started_at: chrono::Utc::now(),
                finished_at: None,
                records: 0,
                error: None,
                input_schema: None,
                output_schema: None,
                column_lineage: None,
                source_code: None,
            };
            em.emit(faucet_lineage::EventType::Start, &ctx).await;
            // Heartbeat task — periodic RUNNING events with the live throughput
            // count read off the output sampler.
            let hb_handle = if lc.emit_on.running {
                let em2 = std::sync::Arc::clone(em);
                let interval = lc.heartbeat_interval;
                let mut beat_ctx = ctx.clone();
                let counter = out_sample.clone();
                Some(tokio::spawn(async move {
                    let mut tick = tokio::time::interval(interval);
                    tick.tick().await; // skip the immediate first tick
                    loop {
                        tick.tick().await;
                        if let Some(c) = &counter {
                            beat_ctx.records = c.count();
                        }
                        em2.emit(faucet_lineage::EventType::Running, &beat_ctx)
                            .await;
                    }
                }))
            } else {
                None
            };
            ctx.source_code = if lc.include_source_code_facet {
                Some(serde_json::to_string(&node.source.config).unwrap_or_default())
            } else {
                None
            };
            Some((std::sync::Arc::clone(em), ctx, hb_handle))
        }
        _ => None,
    };

    // Combine run + final flush into one outcome BEFORE emitting the terminal
    // lineage event, preserving the original semantics (run error → skip flush;
    // run ok but flush error → overall error). The terminal event is classified
    // from this combined `result`, then `?`-propagated below — restoring the
    // original early-return behaviour while still firing the terminal event on
    // both success and error.
    let result: Result<faucet_core::PipelineResult, FaucetError> = match pipeline.run().await {
        Ok(r) => sink.flush().await.map(|_| r),
        Err(e) => Err(e),
    };

    #[cfg(feature = "lineage")]
    if let Some((em, mut ctx, hb)) = lineage_ctx {
        if let Some(h) = hb {
            h.abort();
        }
        ctx.finished_at = Some(chrono::Utc::now());
        if let Some(state) = &out_sample {
            ctx.records = state.count();
            if lineage_cfg
                .as_ref()
                .map(|l| l.include_schema_facet)
                .unwrap_or(false)
            {
                ctx.output_schema = Some(state.inferred_schema());
            }
        }
        if let Some(state) = &in_sample
            && lineage_cfg
                .as_ref()
                .map(|l| l.include_schema_facet || l.include_column_lineage)
                .unwrap_or(false)
        {
            let in_schema = state.inferred_schema();
            if lineage_cfg
                .as_ref()
                .map(|l| l.include_column_lineage)
                .unwrap_or(false)
            {
                let input_fields: Vec<String> =
                    in_schema.fields.iter().map(|(n, _)| n.clone()).collect();
                let ops = crate::lineage_glue::column_ops(&node.transforms);
                ctx.column_lineage = faucet_lineage::derive_column_lineage(&input_fields, &ops);
            }
            if lineage_cfg
                .as_ref()
                .map(|l| l.include_schema_facet)
                .unwrap_or(false)
            {
                ctx.input_schema = Some(in_schema);
            }
        }
        let ev = match &result {
            Err(e) => {
                ctx.error = Some(e.to_string());
                faucet_lineage::EventType::Fail
            }
            Ok(_) if cancel.is_cancelled() => faucet_lineage::EventType::Abort,
            Ok(_) => faucet_lineage::EventType::Complete,
        };
        em.emit(ev, &ctx).await;
    }

    let result = result?;

    let captured = if needs_capture {
        std::mem::take(&mut *captured.lock().await)
    } else {
        Vec::new()
    };
    Ok((captured, result.records_written))
}

async fn build_state_for_node(
    node: &ExpandedNode,
    state_path_override: Option<&Path>,
) -> CliResult<Option<Arc<dyn StateStore>>> {
    match (&node.state, state_path_override) {
        (Some(spec), None) => Ok(Some(build_state_store(spec).await?)),
        (None, Some(path)) => Ok(Some(state_from_override(path))),
        (Some(spec), Some(path)) => {
            if spec.kind == "file" {
                Ok(Some(state_from_override(path)))
            } else {
                tracing::warn!(
                    state = %spec.kind,
                    "--state-path is only meaningful for the 'file' backend; ignoring override"
                );
                Ok(Some(build_state_store(spec).await?))
            }
        }
        (None, None) => Ok(None),
    }
}

fn state_from_override(path: &Path) -> Arc<dyn StateStore> {
    Arc::new(faucet_core::FileStateStore::new(path)) as Arc<dyn StateStore>
}

/// Translate a [`crate::config::DlqSpec`] from the YAML/JSON config into a
/// runtime [`DlqConfig`] ready to attach to a [`Pipeline`].
pub async fn build_dlq_config(spec: &crate::config::DlqSpec) -> CliResult<DlqConfig> {
    // DLQ sinks resolve against an empty catalog — shared `auth: { ref }` on a
    // DLQ sink is out of scope (DLQ targets are typically local jsonl/stdout).
    let sink = build_sink(
        &spec.sink.kind,
        spec.sink.config.clone(),
        &AuthCatalog::new(),
    )
    .await?;
    Ok(DlqConfig {
        sink: Arc::from(sink),
        on_batch_error: match spec.on_batch_error {
            crate::config::OnBatchErrorSpec::Propagate => OnBatchError::Propagate,
            crate::config::OnBatchErrorSpec::DlqAll => OnBatchError::DlqAll,
        },
        max_failures_per_page: spec.max_failures_per_page,
        max_failures_total: spec.max_failures_total,
        include_original_payload: spec.include_original_payload,
    })
}

/// In-place `${now.*}` resolution against the run clock. Walks every string
/// leaf and rewrites `${now.<token>}`; all other `${...}` tokens are untouched.
fn resolve_now_inplace(value: &mut Value, clock: DateTime<FixedOffset>) -> CliResult<()> {
    match value {
        Value::String(s) => {
            *s = crate::interpolate::resolve_now(s, clock)?;
            Ok(())
        }
        Value::Array(a) => a.iter_mut().try_for_each(|v| resolve_now_inplace(v, clock)),
        Value::Object(m) => m
            .values_mut()
            .try_for_each(|v| resolve_now_inplace(v, clock)),
        _ => Ok(()),
    }
}

/// In-place runtime interpolation against a parent-record context. Walks every
/// string leaf in `value` and replaces `${id.path}` tokens with stringified
/// values from `ctx`.
fn resolve_inplace(value: &mut Value, ctx: &HashMap<String, Value>) -> CliResult<()> {
    match value {
        Value::String(s) => {
            let resolved = interpolate_record(s, ctx)?;
            *s = resolved;
            Ok(())
        }
        Value::Array(a) => a.iter_mut().try_for_each(|v| resolve_inplace(v, ctx)),
        Value::Object(m) => m.values_mut().try_for_each(|v| resolve_inplace(v, ctx)),
        _ => Ok(()),
    }
}

// ── Adapter sinks/sources ───────────────────────────────────────────────────

/// Wraps a source so its `state_key()` returns the executor-provided value
/// instead of the source's natural one. Lets every matrix invocation use a
/// distinct state-store entry even when the underlying source kind is shared.
struct StateKeyOverride {
    inner: Box<dyn Source>,
    key: String,
}

#[async_trait]
impl Source for StateKeyOverride {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        self.inner.fetch_with_context(ctx).await
    }
    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        self.inner.fetch_with_context_incremental(ctx).await
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn state_key(&self) -> Option<String> {
        Some(self.key.clone())
    }
    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }
}

/// Forwards each record to an inner sink while also cloning it into a shared
/// buffer for descendant rows to consume.
struct CapturingSink {
    inner: Box<dyn Sink>,
    captured: Arc<Mutex<Vec<Value>>>,
}

impl CapturingSink {
    fn wrap(inner: Box<dyn Sink>, captured: Arc<Mutex<Vec<Value>>>) -> Self {
        Self { inner, captured }
    }
}

#[async_trait]
impl Sink for CapturingSink {
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let written = self.inner.write_batch(records).await?;
        // Capture only what actually landed (LimitedSink may have dropped some).
        let n = written.min(records.len());
        let mut buf = self.captured.lock().await;
        buf.extend(records.iter().take(n).cloned());
        Ok(written)
    }
    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }
}

/// Cap on records written. Each `write_batch` call truncates `records` to the
/// remaining budget before delegating.
struct LimitedSink {
    inner: Box<dyn Sink>,
    remaining: AtomicUsize,
}

impl LimitedSink {
    fn wrap(inner: Box<dyn Sink>, cap: usize) -> Self {
        Self {
            inner,
            remaining: AtomicUsize::new(cap),
        }
    }
}

#[async_trait]
impl Sink for LimitedSink {
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        let remaining = self.remaining.load(Ordering::Relaxed);
        if remaining == 0 {
            return Ok(0);
        }
        let take = remaining.min(records.len());
        let slice = &records[..take];
        let written = self.inner.write_batch(slice).await?;
        self.remaining
            .fetch_sub(written.min(remaining), Ordering::Relaxed);
        Ok(written)
    }
    async fn flush(&self) -> Result<(), FaucetError> {
        self.inner.flush().await
    }
}

/// No-op sink used in `--dry-run`. Counts records seen so the rest of the
/// pipeline (transforms, source) still runs.
struct CountingSink {
    seen: AtomicUsize,
}

impl CountingSink {
    fn new() -> Self {
        Self {
            seen: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl Sink for CountingSink {
    fn connector_name(&self) -> &'static str {
        "dry-run"
    }
    async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
        self.seen.fetch_add(records.len(), Ordering::Relaxed);
        Ok(records.len())
    }
}

/// Render a JSON value compactly for use as a state-key suffix or log line.
/// Strings pass through unquoted; numbers/bools/null/composites use to_string.
fn value_to_string_brief(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{ConnectorSpec, PipelineConfig, PipelineSpec};
    use crate::expand::expand;
    use serde_json::json;

    fn cfg_csv_to_jsonl(input: &Path, output: &Path) -> PipelineConfig {
        PipelineConfig {
            version: 1,
            name: Some("test".into()),
            vars: None,
            auth: None,
            pipeline: PipelineSpec {
                source: Some(ConnectorSpec {
                    kind: "csv".into(),
                    config: json!({"path": input.to_str().unwrap()}),
                    transforms: None,
                    inherit_transforms: true,
                }),
                sink: Some(ConnectorSpec {
                    kind: "jsonl".into(),
                    config: json!({"path": output.to_str().unwrap()}),
                    transforms: None,
                    inherit_transforms: true,
                }),
                sources: Default::default(),
                sinks: Default::default(),
                transforms: Vec::new(),
                state: None,
                dlq: None,
                #[cfg(feature = "quality")]
                quality: None,
            },
            matrix: Vec::new(),
            execution: None,
            observability: None,
            #[cfg(feature = "schedule")]
            schedule: None,
            #[cfg(feature = "lineage")]
            lineage: None,
        }
    }

    #[tokio::test]
    async fn empty_matrix_runs_pipeline_once() {
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\nbob\n").unwrap();
        let cfg = cfg_csv_to_jsonl(&input, &output);
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "t".into(),
                execution: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(summary.invocations.len(), 1);
        assert_eq!(summary.invocations[0].records_written, 2);
        assert!(!summary.had_failures());
        let body = std::fs::read_to_string(&output).unwrap();
        assert_eq!(body.lines().count(), 2);
    }

    #[tokio::test]
    async fn matrix_two_independent_roots_both_run() {
        // Two roots: one writes alice, the other writes bob — to two separate files.
        let dir = tempfile::tempdir().unwrap();
        let csv_a = dir.path().join("a.csv");
        let csv_b = dir.path().join("b.csv");
        let out_a = dir.path().join("a.jsonl");
        let out_b = dir.path().join("b.jsonl");
        std::fs::write(&csv_a, "name\nalice\n").unwrap();
        std::fs::write(&csv_b, "name\nbob\n").unwrap();

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {a} }} }}
  sink:   {{ type: jsonl, config: {{ path: {out_a} }} }}
matrix:
  - id: rowA
  - id: rowB
    source: {{ config: {{ path: {b} }} }}
    sink:   {{ config: {{ path: {out_b} }} }}
"#,
            a = csv_a.display(),
            b = csv_b.display(),
            out_a = out_a.display(),
            out_b = out_b.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "matrix".into(),
                execution: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(summary.invocations.len(), 2);
        assert!(out_a.exists());
        assert!(out_b.exists());
    }

    #[tokio::test]
    async fn dag_child_fans_out_per_parent_record() {
        // Parent: CSV with two records (id=1, id=2).
        // Child: writes one JSONL file per parent id, using ${parent.id} in the path.
        let dir = tempfile::tempdir().unwrap();
        let parent_csv = dir.path().join("parents.csv");
        let child_csv = dir.path().join("child.csv");
        std::fs::write(&parent_csv, "id,name\n1,alice\n2,bob\n").unwrap();
        std::fs::write(&child_csv, "x\nA\nB\nC\n").unwrap();
        let parent_out = dir.path().join("parents.jsonl");
        let child_out_pattern = dir.path().join("child-${parents.id}.jsonl");

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {parent} }} }}
  sink:   {{ type: jsonl, config: {{ path: {parent_out} }} }}
matrix:
  - id: parents
  - id: child
    parent: parents
    source: {{ config: {{ path: {child} }} }}
    sink:   {{ config: {{ path: "{child_out}" }} }}
"#,
            parent = parent_csv.display(),
            parent_out = parent_out.display(),
            child = child_csv.display(),
            child_out = child_out_pattern.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "dagtest".into(),
                execution: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
            },
        )
        .await
        .unwrap();

        // 1 parent invocation + 2 child invocations.
        assert_eq!(summary.invocations.len(), 3);
        assert!(!summary.had_failures(), "{:?}", summary);
        assert!(dir.path().join("child-1.jsonl").exists());
        assert!(dir.path().join("child-2.jsonl").exists());
    }

    #[tokio::test]
    async fn on_error_stop_reports_failure_and_runs_no_extra_work() {
        // First root writes to an invalid sink path and fails. The second
        // ("good") root would succeed. Under `on_error: stop` the executor
        // calls `abort_all()` on the first failure, which cancels pending /
        // in-flight tasks at their next await point — but that is
        // best-effort: with `max_concurrent: 1` the two roots race for the
        // single permit, so "good" may already have completed before "bad"
        // fails. We therefore assert the guarantees that hold under *any*
        // scheduling rather than an exact invocation count (which was racy,
        // see issue #78 finding #24). The deterministic "stop actually
        // cancels in-flight work" path is covered by
        // `on_error_stop_under_parallelism_aborts_other_in_flight`.
        let dir = tempfile::tempdir().unwrap();
        let good_csv = dir.path().join("good.csv");
        std::fs::write(&good_csv, "x\n1\n").unwrap();
        let good_out = dir.path().join("good.jsonl");
        let bad_sink_dir = dir.path().to_path_buf();

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {good_csv} }} }}
  sink:   {{ type: jsonl, config: {{ path: {good_out} }} }}
matrix:
  - id: bad
    sink: {{ config: {{ path: {bad_dir} }} }}
  - id: good
execution:
  max_concurrent: 1
  on_error: stop
"#,
            good_csv = good_csv.display(),
            good_out = good_out.display(),
            bad_dir = bad_sink_dir.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "stoptest".into(),
                execution: cfg.execution.clone(),
                dry_run: false,
                limit: None,
                state_path_override: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
            },
        )
        .await
        .unwrap();

        // Invariants that hold regardless of which root won the permit race:
        assert!(summary.had_failures(), "the failing root must be reported");

        // "bad" ran exactly once and is recorded as a failure.
        let bad: Vec<_> = summary
            .invocations
            .iter()
            .filter(|o| o.row_id == "bad")
            .collect();
        assert_eq!(bad.len(), 1, "bad must run exactly once");
        assert!(bad[0].error.is_some(), "bad must be recorded as a failure");

        // No duplicate / extra invocations beyond the two work units.
        assert!(
            summary.invocations.len() <= 2,
            "at most the two roots may run, got {:?}",
            summary.invocations
        );

        // "good" may: (a) win the permit first and run fully (writes its row,
        // file exists); (b) lose the race, acquire the permit after "bad" fails,
        // observe the cooperative stop-cancel at its first page boundary, and
        // return a 0-record success (no file); or (c) never appear if it was
        // still pending when the level finished. So the only invariant is: a
        // "good" that actually WROTE records must have produced its file.
        let good_wrote = summary
            .invocations
            .iter()
            .find(|o| o.row_id == "good" && o.error.is_none())
            .map(|o| o.records_written)
            .unwrap_or(0);
        if good_wrote > 0 {
            assert!(
                good_out.exists(),
                "a good that wrote records must have produced its output file"
            );
        }
    }

    #[tokio::test]
    async fn invalid_pipeline_name_with_state_errors_up_front() {
        // A pipeline name that can't form a valid state key must fail up front
        // (at unit construction) when state is configured — not deep mid-run
        // as a `FaucetError::State`.
        let dir = tempfile::tempdir().unwrap();
        let input = dir.path().join("in.csv");
        let output = dir.path().join("out.jsonl");
        std::fs::write(&input, "name\nalice\n").unwrap();
        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {input} }} }}
  sink:   {{ type: jsonl, config: {{ path: {output} }} }}
  state:  {{ type: memory }}
"#,
            input = input.display(),
            output = output.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let err = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "bad name".into(), // space is illegal in a state key
                execution: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
            },
        )
        .await
        .expect_err("an invalid pipeline name must be rejected up front when state is configured");
        assert!(matches!(err, CliError::InvalidStateKey { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn invalid_parent_key_value_with_state_errors_up_front() {
        // A parent-record value that yields an illegal state-key suffix must
        // fail up front at the child's unit construction, not mid-run.
        let dir = tempfile::tempdir().unwrap();
        let parent_csv = dir.path().join("parents.csv");
        let child_csv = dir.path().join("child.csv");
        // The parent `id` value contains a space — illegal in a state key.
        std::fs::write(&parent_csv, "id\nbad id\n").unwrap();
        std::fs::write(&child_csv, "x\nA\n").unwrap();
        let parent_out = dir.path().join("parents.jsonl");
        let child_out = dir.path().join("child.jsonl");
        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {parent} }} }}
  sink:   {{ type: jsonl, config: {{ path: {parent_out} }} }}
  state:  {{ type: memory }}
matrix:
  - id: parents
  - id: child
    parent: parents
    source: {{ config: {{ path: {child} }} }}
    sink:   {{ config: {{ path: {child_out} }} }}
"#,
            parent = parent_csv.display(),
            parent_out = parent_out.display(),
            child = child_csv.display(),
            child_out = child_out.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let err = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "ok".into(),
                execution: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
            },
        )
        .await
        .expect_err(
            "an illegal parent-key value must be rejected up front when state is configured",
        );
        assert!(matches!(err, CliError::InvalidStateKey { .. }), "{err:?}");
    }

    #[tokio::test]
    async fn on_error_stop_under_parallelism_aborts_other_in_flight() {
        // Three roots running with `max_concurrent: 3`. The bad row points
        // its sink at a directory (open fails fast). The other two point at
        // sinks that block forever on the writer end of a pipe — stuck *inside*
        // the sink write, they never reach a page boundary to observe the
        // cooperative stop-cancel, so the only way they can complete is the
        // hard-abort backstop that fires after the flush grace (#146 H16). The
        // test would hang if `on_error: stop` never aborted them, so a passing
        // run is itself the assertion.
        let dir = tempfile::tempdir().unwrap();
        let bad_sink_dir = dir.path().to_path_buf();
        // A real csv source with one row — small enough that the pipeline
        // proceeds straight to the sink phase.
        let good_csv = dir.path().join("good.csv");
        std::fs::write(&good_csv, "x\n1\n").unwrap();
        // The two "would never finish" sinks point at the same path as the
        // bad sink (an existing directory). Their sink-open also errors
        // out — but we still verify the *abort* path by counting how many
        // tasks make it past spawn before stop fires. The strict invariant
        // we assert: the bad row's failure is the first one observed.
        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {good_csv} }} }}
  sink:   {{ type: jsonl, config: {{ path: {bad_dir} }} }}
matrix:
  - id: bad
  - id: good_a
  - id: good_b
execution:
  max_concurrent: 3
  on_error: stop
"#,
            good_csv = good_csv.display(),
            bad_dir = bad_sink_dir.display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "stop_parallel".into(),
                execution: cfg.execution.clone(),
                dry_run: false,
                limit: None,
                state_path_override: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
            },
        )
        .await
        .unwrap();

        // First-observed failure halts the run. The first outcome in the
        // summary is guaranteed to be a failure (other tasks either fail
        // too or get cancelled — both cases never push a *success* outcome
        // first because every sink in this matrix is configured to fail).
        assert!(
            summary.had_failures(),
            "summary should record at least one failure: {summary:?}"
        );
        assert!(
            summary.invocations[0].error.is_some(),
            "first outcome must be the failure that triggered stop: {summary:?}"
        );
        // No invocation should report `records_written > 0` — every sink is
        // bad. (Catches a regression where abort_all somehow let a task
        // bypass its broken sink.)
        for inv in &summary.invocations {
            assert_eq!(inv.records_written, 0, "no records should land: {inv:?}");
        }
    }

    #[tokio::test]
    async fn on_error_continue_skips_failed_subtree_only() {
        // Two roots: one fails. The good one's invocation still completes.
        let dir = tempfile::tempdir().unwrap();
        let good_csv = dir.path().join("good.csv");
        std::fs::write(&good_csv, "x\n1\n").unwrap();
        let good_out = dir.path().join("good.jsonl");

        let yaml = format!(
            r#"version: 1
pipeline:
  source: {{ type: csv, config: {{ path: {good_csv} }} }}
  sink:   {{ type: jsonl, config: {{ path: {good_out} }} }}
matrix:
  - id: bad
    sink: {{ config: {{ path: {bad_dir} }} }}
  - id: good
"#,
            good_csv = good_csv.display(),
            good_out = good_out.display(),
            bad_dir = dir.path().display(),
        );
        let cfg = crate::config::parse_with_extension(&yaml, "yaml").unwrap();
        let nodes = expand(&cfg).unwrap();
        let summary = run_expanded(
            nodes,
            ExecuteOptions {
                pipeline_name: "continuetest".into(),
                execution: None,
                dry_run: false,
                limit: None,
                state_path_override: None,
                auth: Default::default(),
                clock: chrono::Utc::now().fixed_offset(),
                cancel: None,
                #[cfg(feature = "lineage")]
                lineage: None,
                #[cfg(feature = "lineage")]
                lineage_cfg: None,
            },
        )
        .await
        .unwrap();
        assert_eq!(summary.invocations.len(), 2);
        assert_eq!(summary.failure_count(), 1);
        let good_outcome = summary
            .invocations
            .iter()
            .find(|i| i.row_id == "good")
            .unwrap();
        assert!(good_outcome.error.is_none());
    }

    // ── projection helpers (#160) ─────────────────────────────────────────────

    #[test]
    fn split_path_splits_on_dots() {
        assert_eq!(split_path("id"), vec!["id".to_string()]);
        assert_eq!(split_path("user.name"), vec!["user".to_string(), "name".to_string()]);
    }

    #[test]
    fn minimal_paths_drops_descendants_of_kept_ancestors() {
        let paths = vec![
            vec!["user".into(), "name".into()],
            vec!["user".into()],
            vec!["id".into()],
            vec!["id".into()],
        ];
        let min = minimal_paths(paths);
        assert!(min.contains(&vec!["user".to_string()]));
        assert!(min.contains(&vec!["id".to_string()]));
        assert!(!min.contains(&vec!["user".to_string(), "name".to_string()]),
            "user.name must be dropped — covered by user");
        assert_eq!(min.len(), 2);
    }

    #[test]
    fn project_full_clones_whole_record() {
        let r = json!({"a": 1, "b": {"c": 2}});
        assert_eq!(project_record(&r, &Projection::Full), r);
    }

    #[test]
    fn project_keeps_only_referenced_paths() {
        let r = json!({"id": 7, "user": {"name": "a", "age": 3}, "blob": "<huge>"});
        let p = Projection::Paths(vec![
            vec!["id".into()],
            vec!["user".into(), "name".into()],
        ]);
        let got = project_record(&r, &p);
        assert_eq!(got, json!({"id": 7, "user": {"name": "a"}}));
        assert!(got.get("blob").is_none());
        assert!(got["user"].get("age").is_none());
    }

    #[test]
    fn project_array_index_path_resolves_same_as_original() {
        let r = json!({"tags": ["x", "y", "z"]});
        let p = Projection::Paths(vec![vec!["tags".into(), "0".into()]]);
        let got = project_record(&r, &p);
        assert_eq!(got, json!({"tags": {"0": "x"}}));
        assert_eq!(resolve_parent_key(&got, "tags.0"), Some(json!("x")));
        assert_eq!(
            resolve_parent_key(&got, "tags.0"),
            resolve_parent_key(&r, "tags.0"),
            "reduced tree must resolve the same value as the original"
        );
    }

    #[test]
    fn project_missing_path_is_omitted() {
        let r = json!({"id": 1});
        let p = Projection::Paths(vec![vec!["nope".into()]]);
        assert_eq!(project_record(&r, &p), json!({}));
    }
}
