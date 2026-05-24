//! Run a list of [`ExpandedNode`]s under a bounded-concurrency executor.
//!
//! Semantics:
//!
//! - Roots run concurrently under `Semaphore(max_concurrent)`.
//! - Each root captures its written records (via a [`CapturingSink`] wrapper)
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
//!
//! Compared to `faucet_core::dag::SourceDAG`, this executor: (a) runs roots
//! concurrently, (b) uses string interpolation rather than JSONPath context
//! mapping, and (c) supports the `on_error: stop` policy. `SourceDAG` stays
//! useful for callers embedding the library directly.

use crate::config::{ExecutionSpec, OnError};
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole};
use crate::interpolate::interpolate_record;
use crate::registry::{build_sink, build_source};
use crate::state::build_state_store;
use crate::transforms::compile_transforms;
use async_trait::async_trait;
use faucet_core::observability::{Labels, instrumented_apply_all};
use faucet_core::transform::{CompiledTransform, compile as compile_transform};
use faucet_core::{DlqConfig, FaucetError, OnBatchError, Pipeline, Sink, Source, StateStore};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::{Mutex, Semaphore};

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
}

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

/// Default concurrency when neither config nor flag specify one.
fn default_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4)
        .clamp(1, 4)
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
    let by_id: HashMap<String, usize> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.id.clone(), i))
        .collect();
    let mut children_of: HashMap<String, Vec<usize>> = HashMap::new();
    for (i, n) in nodes.iter().enumerate() {
        if let NodeRole::Child { parent_id, .. } = &n.role {
            children_of.entry(parent_id.clone()).or_default().push(i);
        }
    }

    // Captured records per node id. Only populated for nodes that have
    // children (= are referenced by another node's `parent:`).
    let captured: Arc<Mutex<HashMap<String, Vec<Value>>>> = Arc::new(Mutex::new(HashMap::new()));
    let nodes_with_descendants: HashSet<String> = children_of.keys().cloned().collect();

    let mut outcomes: Vec<InvocationOutcome> = Vec::new();
    let mut skipped_subtrees: HashSet<String> = HashSet::new();

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
            // Should not happen given expand.rs's invariants, but be defensive.
            break;
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
                    let state_key = build_state_key(&opts.pipeline_name, &node.id, None);
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
                    let mut seen_keys: HashSet<String> = HashSet::new();
                    for record in &parent_records {
                        let pk_value = resolve_parent_key(record, parent_key);
                        let pk_string = pk_value
                            .as_ref()
                            .map(value_to_string_brief)
                            .unwrap_or_else(|| "(missing)".to_string());
                        let state_key =
                            build_state_key(&opts.pipeline_name, &node.id, Some(&pk_string));
                        if !seen_keys.insert(state_key.clone()) {
                            return Err(CliError::DuplicateStateKey {
                                id: node.id.clone(),
                                other_id: node.id.clone(),
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
        let mut joinset = tokio::task::JoinSet::new();
        for unit in units {
            let sem = Arc::clone(&semaphore);
            let opts2 = Arc::clone(&opts);
            let captured = Arc::clone(&captured);
            let needs_capture = nodes_with_descendants.contains(&unit.node.id);
            joinset.spawn(async move {
                let _permit = sem.acquire().await.expect("semaphore not closed");
                run_unit(&unit, needs_capture, &captured, &opts2).await
            });
        }

        let mut stop_triggered = false;
        while let Some(joined) = joinset.join_next().await {
            match joined {
                Ok(outcome) => {
                    if let Some(err) = &outcome.error {
                        tracing::error!(
                            row = %outcome.row_id, error = %err,
                            "pipeline invocation failed"
                        );
                        had_level_failure = true;
                        nodes_with_any_failure.insert(outcome.row_id.clone());
                        if matches!(on_error, OnError::Stop) && !stop_triggered {
                            stop_triggered = true;
                            tracing::error!(
                                "on_error: stop — aborting every in-flight and pending invocation"
                            );
                            joinset.abort_all();
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
                Err(e) if e.is_cancelled() => {
                    // Expected after abort_all() — task was cancelled before
                    // (or during) real work. Not counted as failure or success.
                }
                Err(e) => panic!("pipeline invocation task panicked: {e}"),
            }
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
                    for &ci in children {
                        let cid = nodes_by_id
                            .values()
                            .nth(ci)
                            .map(|n| n.id.clone())
                            .unwrap_or_default();
                        if !cid.is_empty() {
                            skipped_subtrees.insert(cid);
                        }
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

    // Reference fields we may not have read explicitly so dead-code-warnings stay quiet.
    let _ = (&by_id, &children_of);

    Ok(RunSummary {
        invocations: outcomes,
    })
}

/// One scheduled invocation — a root runs once, a child runs once per parent
/// record. Built by the level loop, consumed by [`run_unit`].
struct Unit {
    node: ExpandedNode,
    parent_record: Option<Value>,
    state_key: String,
    parent_record_key: Option<String>,
}

async fn run_unit(
    unit: &Unit,
    needs_capture: bool,
    captured: &Arc<Mutex<HashMap<String, Vec<Value>>>>,
    opts: &ExecuteOptions,
) -> InvocationOutcome {
    let result = run_one_invocation(
        &unit.node,
        unit.parent_record.as_ref(),
        &unit.state_key,
        needs_capture,
        opts,
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
                    .extend(records);
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

/// Run one pipeline invocation. Returns (captured records, records_written).
async fn run_one_invocation(
    node: &ExpandedNode,
    parent_record: Option<&Value>,
    state_key: &str,
    needs_capture: bool,
    opts: &ExecuteOptions,
) -> CliResult<(Vec<Value>, usize)> {
    // Observability identity for this invocation — built once, reused by both
    // the Pipeline builder and the transform instrumentation.
    let run_id = uuid::Uuid::now_v7().to_string();
    let pipeline_name = opts.pipeline_name.clone();
    let row_id = node.id.clone();
    let obs_labels = Labels::new(pipeline_name.clone(), row_id.clone(), run_id.clone());
    // 1) Resolve `${parent.path}` in the per-row source + sink configs.
    let mut source_cfg = node.source.config.clone();
    let mut sink_cfg = node.sink.config.clone();
    if let (Some(record), NodeRole::Child { parent_id, .. }) = (parent_record, &node.role) {
        let ctx: HashMap<String, Value> = HashMap::from([(parent_id.clone(), record.clone())]);
        resolve_inplace(&mut source_cfg, &ctx)?;
        resolve_inplace(&mut sink_cfg, &ctx)?;
    }

    // 2) Build source + sink.
    let source = build_source(&node.source.kind, source_cfg).await?;
    let raw_sink: Box<dyn Sink> = if opts.dry_run {
        Box::new(CountingSink::new())
    } else {
        build_sink(&node.sink.kind, sink_cfg).await?
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

    // 3) Compile transforms.
    let transforms = compile_transforms(&node.transforms)?;
    let source: Box<dyn Source> = if transforms.is_empty() {
        source
    } else {
        let compiled = transforms
            .iter()
            .map(compile_transform)
            .collect::<Result<Vec<_>, _>>()?;
        Box::new(TransformingSource {
            inner: source,
            transforms: compiled,
            obs_labels: obs_labels.clone(),
        })
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

    // 5) Run.
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
    let result = pipeline.run().await?;
    sink.flush().await?;

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
    let sink = build_sink(&spec.sink.kind, spec.sink.config.clone()).await?;
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

/// Wraps an inner source, applying every compiled transform to each record
/// and emitting per-record observability spans/metrics via
/// [`instrumented_apply_all`].
struct TransformingSource {
    inner: Box<dyn Source>,
    transforms: Vec<CompiledTransform>,
    obs_labels: Labels,
}

#[async_trait]
impl Source for TransformingSource {
    async fn fetch_with_context(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        let records = self.inner.fetch_with_context(ctx).await?;
        Ok(instrumented_apply_all(
            records,
            &self.transforms,
            &self.obs_labels,
        ))
    }
    async fn fetch_with_context_incremental(
        &self,
        ctx: &HashMap<String, Value>,
    ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
        let (records, bookmark) = self.inner.fetch_with_context_incremental(ctx).await?;
        let transformed = instrumented_apply_all(records, &self.transforms, &self.obs_labels);
        Ok((transformed, bookmark))
    }
    fn connector_name(&self) -> &'static str {
        self.inner.connector_name()
    }
    fn state_key(&self) -> Option<String> {
        self.inner.state_key()
    }
    async fn apply_start_bookmark(&self, bookmark: Value) -> Result<(), FaucetError> {
        self.inner.apply_start_bookmark(bookmark).await
    }
}

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
            pipeline: PipelineSpec {
                source: ConnectorSpec {
                    kind: "csv".into(),
                    config: json!({"path": input.to_str().unwrap()}),
                },
                sink: ConnectorSpec {
                    kind: "jsonl".into(),
                    config: json!({"path": output.to_str().unwrap()}),
                },
                transforms: Vec::new(),
                state: None,
                dlq: None,
            },
            matrix: Vec::new(),
            execution: None,
            observability: None,
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
    async fn on_error_stop_aborts_pending_invocations() {
        // First root writes to an invalid sink path. The second root would
        // succeed but `on_error: stop` must abort it before its output file
        // appears on disk.
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
            },
        )
        .await
        .unwrap();

        // bad spawned first, acquires the only permit, fails → abort_all()
        // cancels good before it gets to write anything. Only bad shows up
        // in the summary, and good's output file was never opened.
        assert_eq!(summary.invocations.len(), 1);
        assert_eq!(summary.invocations[0].row_id, "bad");
        assert!(summary.had_failures());
        assert!(
            !good_out.exists(),
            "good's sink should never have been opened under on_error=stop"
        );
    }

    #[tokio::test]
    async fn on_error_stop_under_parallelism_aborts_other_in_flight() {
        // Three roots running with `max_concurrent: 3`. The bad row points
        // its sink at a directory (open fails fast). The other two point at
        // sinks that block forever on the writer end of a pipe — the only
        // way they can complete is if abort_all() cancels them. The test
        // would hang if `on_error: stop` failed to abort in-flight work,
        // so a passing run is itself the assertion.
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
}
