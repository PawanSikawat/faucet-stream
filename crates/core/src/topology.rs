//! Multi-edge pipeline topology — fan-out (tee), fan-in (merge), and
//! hash-join over an explicit node graph (issues #71 and #72).
//!
//! The single-source→single-sink [`Pipeline`](crate::Pipeline) covers the
//! common case. A [`Topology`] generalizes it to a directed acyclic graph of
//! typed nodes connected by edges, so one run can *tee* a source's records to
//! several sinks, *merge* several sources into one sink, or *join* two
//! upstreams by key. It is the in-process primitive behind the CLI's
//! `pipeline.nodes` / `edges` topology mode.
//!
//! ## Node kinds
//!
//! | Kind | In | Out | Semantics |
//! |------|----|-----|-----------|
//! | [`NodeKind::Source`] | 0 | 1 | Drives [`Source::stream_pages`]. |
//! | [`NodeKind::Transform`] | 1 | 1 | Applies compiled transform stages per page. |
//! | [`NodeKind::Tee`] | 1 | N | Clones each page to every downstream edge. |
//! | [`NodeKind::Merge`] | N | 1 | Forwards pages from all inputs in arrival order. |
//! | [`NodeKind::Join`] | 2 | 1 | Hash-join: buffer the build edge, enrich the probe edge. |
//! | [`NodeKind::Sink`] | 1 | 0 | Drives [`run_stream`] (write → flush → persist). |
//!
//! ## Execution
//!
//! Each node runs as a cooperatively-scheduled future; edges are bounded
//! [`tokio::sync::mpsc`] channels so the slowest consumer paces its producer
//! (backpressure). No OS threads are spawned — the topology runs on whatever
//! runtime drives [`Topology::run`], overlapping the nodes' I/O. Sink nodes
//! reuse [`run_stream`], so DLQ routing, bookmark persistence, and the full
//! observability metric set come for free.
//!
//! ## State
//!
//! Each terminal sink owns its bookmark under `{pipeline}::{node_id}`. On
//! restart the source resumes from the **minimum** across every sink's stored
//! bookmark (so the slowest sink catches up), applied only when *every* sink
//! has a stored bookmark; otherwise the source replays in full. Sinks whose
//! bookmarks have diverged must therefore be idempotent — a faster sink will
//! re-see already-written pages.

use crate::dlq::DlqConfig;
use crate::error::FaucetError;
use crate::join::HashJoin;
use crate::observability::{Labels, RunStreamOptions, instrumented_apply_stages};
use crate::pipeline::{DEFAULT_BATCH_SIZE, StreamPage, run_stream};
use crate::replication::json_gt;
use crate::stage::CompiledStage;
use crate::state::StateStore;
use crate::traits::{Sink, Source};
use futures::StreamExt;
use metrics::{Label, SharedString, counter, histogram};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub use crate::join::{JoinConfig, JoinMode, KeyNormalize, OnCollision, OnDuplicate, Projection};

/// Default bounded-channel capacity for topology edges.
pub const DEFAULT_CHANNEL_CAPACITY: usize = 4;

/// A join node: the pure [`JoinConfig`] plus the labels of its two incoming
/// edges identifying which upstream is the build (right) side and which is the
/// probe (left) side.
#[derive(Debug, Clone)]
pub struct JoinNode {
    /// Pure join logic configuration.
    pub config: JoinConfig,
    /// Label of the incoming edge feeding the build (right) side.
    pub build_edge: String,
    /// Label of the incoming edge feeding the probe (left) side.
    pub probe_edge: String,
}

/// A typed topology node.
pub enum NodeKind {
    /// A data source (0 in, 1 out).
    Source(Box<dyn Source>),
    /// Transform stages applied per page (1 in, 1 out).
    Transform(Vec<CompiledStage>),
    /// Fan-out: clone each page to every downstream edge (1 in, N out).
    Tee {
        /// Bounded-channel capacity for each outgoing edge.
        capacity: usize,
        /// Optional expected fan-out (outgoing edge count) sanity check.
        fanout: Option<usize>,
    },
    /// Fan-in: forward pages from all inputs in arrival order (N in, 1 out).
    Merge,
    /// Hash-join two upstreams by key (2 in, 1 out).
    Join(JoinNode),
    /// A data sink (1 in, 0 out).
    Sink(Box<dyn Sink>),
}

impl std::fmt::Debug for NodeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.kind_str())
    }
}

impl NodeKind {
    /// Short name of this node kind, used in errors and metric labels.
    pub fn kind_str(&self) -> &'static str {
        match self {
            NodeKind::Source(_) => "source",
            NodeKind::Transform(_) => "transform",
            NodeKind::Tee { .. } => "tee",
            NodeKind::Merge => "merge",
            NodeKind::Join(_) => "join",
            NodeKind::Sink(_) => "sink",
        }
    }

    fn is_source(&self) -> bool {
        matches!(self, NodeKind::Source(_))
    }

    fn is_sink(&self) -> bool {
        matches!(self, NodeKind::Sink(_))
    }
}

/// A node in the topology: a stable id plus its typed kind.
#[derive(Debug)]
pub struct Node {
    /// Stable node id (used as the metric `node` label and state-key suffix).
    pub id: String,
    /// The node's kind.
    pub kind: NodeKind,
}

/// A directed edge from one node's output to another's input.
#[derive(Debug, Clone)]
pub struct Edge {
    /// Producer node id.
    pub from: String,
    /// Consumer node id.
    pub to: String,
    /// Optional edge label, used by [`NodeKind::Join`] to distinguish its
    /// build edge from its probe edge.
    pub label: Option<String>,
}

/// What to do when a node fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopologyOnError {
    /// Abort the whole topology on the first node failure (default).
    #[default]
    Propagate,
    /// Let every node run to completion; collect and report failures without
    /// aborting healthy branches.
    Continue,
}

/// Per-run options for [`Topology::run`].
#[derive(Clone)]
pub struct TopologyOptions {
    /// Pipeline name (metric `pipeline` label).
    pub pipeline_name: String,
    /// Run id (span attribute).
    pub run_id: String,
    /// Batch-size hint passed to source nodes' `stream_pages`.
    pub batch_size: usize,
    /// State store shared by every sink node (each under `{pipeline}::{node_id}`).
    pub state_store: Option<Arc<dyn StateStore>>,
    /// DLQ applied to every sink node.
    pub dlq: Option<DlqConfig>,
    /// Cooperative cancellation.
    pub cancel: Option<CancellationToken>,
    /// Failure policy.
    pub on_error: TopologyOnError,
    /// Default bounded-channel capacity for edges not fed by a tee.
    pub default_channel_capacity: usize,
}

impl Default for TopologyOptions {
    fn default() -> Self {
        Self {
            pipeline_name: "unnamed".into(),
            run_id: String::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            state_store: None,
            dlq: None,
            cancel: None,
            on_error: TopologyOnError::default(),
            default_channel_capacity: DEFAULT_CHANNEL_CAPACITY,
        }
    }
}

impl TopologyOptions {
    /// New options with the given pipeline name.
    pub fn new(pipeline_name: impl Into<String>) -> Self {
        Self {
            pipeline_name: pipeline_name.into(),
            ..Default::default()
        }
    }

    /// Attach a state store.
    pub fn with_state_store(mut self, store: Arc<dyn StateStore>) -> Self {
        self.state_store = Some(store);
        self
    }

    /// Attach a DLQ applied to every sink node.
    pub fn with_dlq(mut self, dlq: DlqConfig) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// Attach a cancellation token.
    pub fn with_cancel(mut self, cancel: CancellationToken) -> Self {
        self.cancel = Some(cancel);
        self
    }

    /// Set the failure policy.
    pub fn with_on_error(mut self, on_error: TopologyOnError) -> Self {
        self.on_error = on_error;
        self
    }

    /// Set the batch-size hint.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }
}

/// Outcome of a topology run.
#[derive(Debug, Clone, Default)]
pub struct TopologyResult {
    /// Total records written across all sink nodes.
    pub records_written: usize,
    /// Per-sink-node records written, keyed by node id.
    pub per_sink: HashMap<String, usize>,
    /// Per-sink-node final bookmark, keyed by node id.
    pub bookmarks: HashMap<String, Option<Value>>,
    /// Node failures observed under [`TopologyOnError::Continue`] (empty under
    /// `Propagate`, which returns `Err` on the first failure instead).
    pub errors: Vec<String>,
}

/// One incoming edge of a node: its optional label plus the receiving end of
/// the channel.
struct InEdge {
    label: Option<String>,
    rx: mpsc::Receiver<StreamPage>,
}

/// Pop the single input receiver from a one-input node's edge list.
fn take_single(mut ins: Vec<InEdge>) -> Option<mpsc::Receiver<StreamPage>> {
    ins.drain(..).next().map(|ie| ie.rx)
}

/// Remove and return the input receiver whose edge carries `label`.
fn take_by_label(ins: &mut Vec<InEdge>, label: &str) -> Option<mpsc::Receiver<StreamPage>> {
    ins.iter()
        .position(|ie| ie.label.as_deref() == Some(label))
        .map(|pos| ins.remove(pos).rx)
}

/// What a completed node future reports back.
enum NodeOutcome {
    Sink {
        node_id: String,
        records: usize,
        bookmark: Option<Value>,
    },
    Other,
}

/// A directed acyclic graph of typed nodes.
///
/// Build one with [`Topology::builder`], then drive it with [`Topology::run`].
#[derive(Debug)]
pub struct Topology {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl Topology {
    /// Start building a topology.
    pub fn builder() -> TopologyBuilder {
        TopologyBuilder::default()
    }

    /// The nodes, in insertion order.
    pub fn nodes(&self) -> &[Node] {
        &self.nodes
    }

    /// The edges, in insertion order.
    pub fn edges(&self) -> &[Edge] {
        &self.edges
    }

    /// Validate the graph: unique ids, existing endpoints, per-kind arity,
    /// tee fan-out, join edge labels, acyclicity, and source→sink
    /// reachability. Returns [`FaucetError::Config`] with a descriptive
    /// message on the first violation.
    pub fn validate(&self) -> Result<(), FaucetError> {
        if self.nodes.is_empty() {
            return Err(cfg("topology has no nodes"));
        }

        // Unique ids.
        let mut seen = HashSet::new();
        for n in &self.nodes {
            if !seen.insert(n.id.as_str()) {
                return Err(cfg(format!("duplicate node id '{}'", n.id)));
            }
        }
        let ids: HashSet<&str> = seen;

        // Edge endpoints exist.
        for e in &self.edges {
            if !ids.contains(e.from.as_str()) {
                return Err(cfg(format!(
                    "edge references unknown 'from' node '{}'",
                    e.from
                )));
            }
            if !ids.contains(e.to.as_str()) {
                return Err(cfg(format!("edge references unknown 'to' node '{}'", e.to)));
            }
        }

        // In/out degrees.
        let mut in_deg: HashMap<&str, usize> = HashMap::new();
        let mut out_deg: HashMap<&str, usize> = HashMap::new();
        for e in &self.edges {
            *out_deg.entry(e.from.as_str()).or_default() += 1;
            *in_deg.entry(e.to.as_str()).or_default() += 1;
        }

        let mut has_source = false;
        let mut has_sink = false;
        for n in &self.nodes {
            let i = in_deg.get(n.id.as_str()).copied().unwrap_or(0);
            let o = out_deg.get(n.id.as_str()).copied().unwrap_or(0);
            match &n.kind {
                NodeKind::Source(_) => {
                    has_source = true;
                    arity(&n.id, "source", i == 0, o == 1, "0 in, exactly 1 out")?;
                }
                NodeKind::Transform(_) => {
                    arity(&n.id, "transform", i == 1, o == 1, "exactly 1 in, 1 out")?;
                }
                NodeKind::Tee { fanout, .. } => {
                    arity(&n.id, "tee", i == 1, o >= 2, "exactly 1 in, 2+ out")?;
                    if let Some(f) = fanout
                        && *f != o
                    {
                        return Err(cfg(format!(
                            "tee '{}' declares fanout {f} but has {o} outgoing edges",
                            n.id
                        )));
                    }
                }
                NodeKind::Merge => {
                    arity(&n.id, "merge", i >= 2, o == 1, "2+ in, exactly 1 out")?;
                }
                NodeKind::Join(j) => {
                    arity(&n.id, "join", i == 2, o == 1, "exactly 2 in, 1 out")?;
                    self.validate_join_edges(&n.id, j)?;
                }
                NodeKind::Sink(_) => {
                    has_sink = true;
                    arity(&n.id, "sink", i == 1, o == 0, "exactly 1 in, 0 out")?;
                }
            }
        }

        if !has_source {
            return Err(cfg("topology has no source node"));
        }
        if !has_sink {
            return Err(cfg("topology has no sink node"));
        }

        self.detect_cycle()?;
        self.check_reachability()?;
        Ok(())
    }

    fn validate_join_edges(&self, node_id: &str, j: &JoinNode) -> Result<(), FaucetError> {
        let labels: Vec<&str> = self
            .edges
            .iter()
            .filter(|e| e.to == node_id)
            .filter_map(|e| e.label.as_deref())
            .collect();
        for want in [j.build_edge.as_str(), j.probe_edge.as_str()] {
            if !labels.contains(&want) {
                return Err(cfg(format!(
                    "join '{node_id}' has no incoming edge labelled '{want}' (known labels: {labels:?})"
                )));
            }
        }
        if j.build_edge == j.probe_edge {
            return Err(cfg(format!(
                "join '{node_id}' build_edge and probe_edge must differ"
            )));
        }
        Ok(())
    }

    /// DFS cycle detection (three-color).
    fn detect_cycle(&self) -> Result<(), FaucetError> {
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        for e in &self.edges {
            adj.entry(e.from.as_str()).or_default().push(e.to.as_str());
        }
        #[derive(Clone, Copy, PartialEq)]
        enum Color {
            White,
            Gray,
            Black,
        }
        let mut color: HashMap<&str, Color> = self
            .nodes
            .iter()
            .map(|n| (n.id.as_str(), Color::White))
            .collect();

        // Iterative DFS to avoid stack overflow on deep graphs.
        for start in self.nodes.iter().map(|n| n.id.as_str()) {
            if color[start] != Color::White {
                continue;
            }
            let mut stack: Vec<(&str, usize)> = vec![(start, 0)];
            *color.get_mut(start).unwrap() = Color::Gray;
            while let Some((node, idx)) = stack.last().copied() {
                let neighbours = adj.get(node).map(|v| v.as_slice()).unwrap_or(&[]);
                if idx < neighbours.len() {
                    stack.last_mut().unwrap().1 += 1;
                    let next = neighbours[idx];
                    match color[next] {
                        Color::Gray => {
                            return Err(cfg(format!("topology has a cycle through node '{next}'")));
                        }
                        Color::White => {
                            *color.get_mut(next).unwrap() = Color::Gray;
                            stack.push((next, 0));
                        }
                        Color::Black => {}
                    }
                } else {
                    *color.get_mut(node).unwrap() = Color::Black;
                    stack.pop();
                }
            }
        }
        Ok(())
    }

    /// Every source must reach at least one sink, and every sink must be
    /// reachable from at least one source.
    fn check_reachability(&self) -> Result<(), FaucetError> {
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        let mut radj: HashMap<&str, Vec<&str>> = HashMap::new();
        for e in &self.edges {
            adj.entry(e.from.as_str()).or_default().push(e.to.as_str());
            radj.entry(e.to.as_str()).or_default().push(e.from.as_str());
        }
        let sink_ids: HashSet<&str> = self
            .nodes
            .iter()
            .filter(|n| n.kind.is_sink())
            .map(|n| n.id.as_str())
            .collect();
        let source_ids: HashSet<&str> = self
            .nodes
            .iter()
            .filter(|n| n.kind.is_source())
            .map(|n| n.id.as_str())
            .collect();

        for src in &source_ids {
            if !reaches_any(src, &adj, &sink_ids) {
                return Err(cfg(format!("source '{src}' does not reach any sink node")));
            }
        }
        for sink in &sink_ids {
            if !reaches_any(sink, &radj, &source_ids) {
                return Err(cfg(format!(
                    "sink '{sink}' is not reachable from any source node"
                )));
            }
        }
        Ok(())
    }

    /// Run the topology to completion.
    pub async fn run(self, opts: TopologyOptions) -> Result<TopologyResult, FaucetError> {
        self.validate()?;
        let Topology { nodes, edges } = self;

        // Capacity per outgoing edge: a tee's edges use its configured
        // capacity; everything else uses the default.
        let tee_cap: HashMap<&str, usize> = nodes
            .iter()
            .filter_map(|n| match &n.kind {
                NodeKind::Tee { capacity, .. } => Some((n.id.as_str(), *capacity)),
                _ => None,
            })
            .collect();

        // Compute the source start bookmark = min across sink bookmarks.
        let sink_ids: Vec<String> = nodes
            .iter()
            .filter(|n| n.kind.is_sink())
            .map(|n| n.id.clone())
            .collect();
        let start_bookmark = compute_start_bookmark(&opts, &sink_ids).await;

        // Build channels.
        let mut outs: HashMap<String, Vec<mpsc::Sender<StreamPage>>> = HashMap::new();
        let mut ins: HashMap<String, Vec<InEdge>> = HashMap::new();
        for e in &edges {
            let cap = tee_cap
                .get(e.from.as_str())
                .copied()
                .unwrap_or(opts.default_channel_capacity)
                .max(1);
            let (tx, rx) = mpsc::channel(cap);
            outs.entry(e.from.clone()).or_default().push(tx);
            ins.entry(e.to.clone()).or_default().push(InEdge {
                label: e.label.clone(),
                rx,
            });
        }

        // Build one future per node.
        type NodeFut = Pin<Box<dyn Future<Output = Result<NodeOutcome, FaucetError>>>>;
        let mut futs: Vec<NodeFut> = Vec::with_capacity(nodes.len());

        for node in nodes {
            let node_outs = outs.remove(&node.id).unwrap_or_default();
            let mut node_ins = ins.remove(&node.id).unwrap_or_default();
            let pipeline = opts.pipeline_name.clone();
            let cancel = opts.cancel.clone();
            let Node { id, kind } = node;

            let fut: NodeFut = match kind {
                NodeKind::Source(source) => {
                    let sb = start_bookmark.clone();
                    let bs = opts.batch_size;
                    Box::pin(run_source_node(source, sb, bs, node_outs, cancel))
                }
                NodeKind::Transform(stages) => {
                    let rx = take_single(node_ins)
                        .ok_or_else(|| cfg(format!("transform '{id}' has no input edge")))?;
                    let labels = Labels::new(pipeline.clone(), id.clone(), opts.run_id.clone());
                    Box::pin(run_transform_node(stages, labels, rx, node_outs, cancel))
                }
                NodeKind::Tee { .. } => {
                    let rx = take_single(node_ins)
                        .ok_or_else(|| cfg(format!("tee '{id}' has no input edge")))?;
                    Box::pin(run_tee_node(id, pipeline, rx, node_outs, cancel))
                }
                NodeKind::Merge => {
                    let rxs: Vec<mpsc::Receiver<StreamPage>> =
                        node_ins.into_iter().map(|ie| ie.rx).collect();
                    Box::pin(run_merge_node(id, pipeline, rxs, node_outs, cancel))
                }
                NodeKind::Join(j) => {
                    let build_rx = take_by_label(&mut node_ins, &j.build_edge);
                    let probe_rx = take_by_label(&mut node_ins, &j.probe_edge);
                    match (build_rx, probe_rx) {
                        (Some(b), Some(p)) => {
                            Box::pin(run_join_node(id, pipeline, j, b, p, node_outs, cancel))
                        }
                        _ => {
                            return Err(cfg(format!(
                                "join '{id}' is missing its build/probe input edges"
                            )));
                        }
                    }
                }
                NodeKind::Sink(sink) => {
                    let rx = take_single(node_ins)
                        .ok_or_else(|| cfg(format!("sink '{id}' has no input edge")))?;
                    let sopts = SinkNodeOpts {
                        pipeline_name: pipeline,
                        run_id: opts.run_id.clone(),
                        state_store: opts.state_store.clone(),
                        dlq: opts.dlq.clone(),
                        cancel: cancel.clone(),
                    };
                    Box::pin(run_sink_node(id, sink, rx, sopts))
                }
            };
            futs.push(fut);
        }

        // Drop the leftover maps so no dangling senders keep channels open.
        drop(outs);
        drop(ins);

        match opts.on_error {
            TopologyOnError::Propagate => {
                let outcomes = futures::future::try_join_all(futs).await?;
                Ok(aggregate(outcomes))
            }
            TopologyOnError::Continue => {
                let results = futures::future::join_all(futs).await;
                let mut ok = Vec::new();
                let mut errs = Vec::new();
                for r in results {
                    match r {
                        Ok(o) => ok.push(o),
                        Err(e) => {
                            tracing::error!(error = %e, "topology node failed (on_error: continue)");
                            errs.push(e.to_string());
                        }
                    }
                }
                let mut result = aggregate(ok);
                result.errors = errs;
                Ok(result)
            }
        }
    }
}

/// Aggregate node outcomes into a [`TopologyResult`].
fn aggregate(outcomes: Vec<NodeOutcome>) -> TopologyResult {
    let mut result = TopologyResult::default();
    for o in outcomes {
        if let NodeOutcome::Sink {
            node_id,
            records,
            bookmark,
        } = o
        {
            result.records_written += records;
            result.per_sink.insert(node_id.clone(), records);
            result.bookmarks.insert(node_id, bookmark);
        }
    }
    result
}

fn cfg(msg: impl Into<String>) -> FaucetError {
    FaucetError::Config(format!("topology: {}", msg.into()))
}

fn arity(
    node_id: &str,
    kind: &str,
    in_ok: bool,
    out_ok: bool,
    expected: &str,
) -> Result<(), FaucetError> {
    if in_ok && out_ok {
        Ok(())
    } else {
        Err(cfg(format!(
            "{kind} '{node_id}' has the wrong edge arity (expected {expected})"
        )))
    }
}

fn reaches_any(start: &str, adj: &HashMap<&str, Vec<&str>>, targets: &HashSet<&str>) -> bool {
    let mut stack = vec![start];
    let mut seen = HashSet::new();
    while let Some(n) = stack.pop() {
        if targets.contains(n) {
            return true;
        }
        if !seen.insert(n) {
            continue;
        }
        if let Some(ns) = adj.get(n) {
            stack.extend(ns.iter().copied());
        }
    }
    false
}

/// Read every sink node's stored bookmark; return the minimum only when every
/// sink has one (so the slowest sink is not skipped past on restart).
async fn compute_start_bookmark(opts: &TopologyOptions, sink_ids: &[String]) -> Option<Value> {
    let store = opts.state_store.as_ref()?;
    if sink_ids.is_empty() {
        return None;
    }
    let mut values = Vec::with_capacity(sink_ids.len());
    for id in sink_ids {
        let key = format!("{}::{}", opts.pipeline_name, id);
        match store.get(&key).await {
            Ok(Some(v)) => values.push(v),
            _ => return None, // a sink with no bookmark → full replay.
        }
    }
    min_bookmark(&values)
}

/// The minimum of a set of bookmarks under the replication ordering.
fn min_bookmark(vals: &[Value]) -> Option<Value> {
    let mut min: Option<&Value> = None;
    for v in vals {
        match min {
            None => min = Some(v),
            Some(m) if json_gt(m, v) => min = Some(v),
            _ => {}
        }
    }
    min.cloned()
}

/// Send `page` to every live output, moving into the last and cloning for the
/// rest. Closed (dropped-receiver) outputs are removed. Returns `false` once
/// every output has closed.
async fn broadcast(page: StreamPage, outs: &mut Vec<mpsc::Sender<StreamPage>>) -> bool {
    if outs.is_empty() {
        return false;
    }
    let last = outs.len() - 1;
    let mut closed: Vec<usize> = Vec::new();
    for (i, tx) in outs.iter().enumerate().take(last) {
        if tx.send(page.clone()).await.is_err() {
            closed.push(i);
        }
    }
    if outs[last].send(page).await.is_err() {
        closed.push(last);
    }
    for &i in closed.iter().rev() {
        outs.remove(i);
    }
    !outs.is_empty()
}

fn cancelled(cancel: &Option<CancellationToken>) -> bool {
    cancel.as_ref().is_some_and(|c| c.is_cancelled())
}

async fn run_source_node(
    source: Box<dyn Source>,
    start_bookmark: Option<Value>,
    batch_size: usize,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    if let Some(bm) = start_bookmark {
        source.apply_start_bookmark(bm).await?;
    }
    let ctx = std::collections::HashMap::new();
    let mut pages = source.stream_pages(&ctx, batch_size);
    while let Some(item) = pages.next().await {
        if cancelled(&cancel) {
            break;
        }
        let page = item?;
        if !broadcast(page, &mut outs).await {
            break;
        }
    }
    Ok(NodeOutcome::Other)
}

async fn run_transform_node(
    stages: Vec<CompiledStage>,
    labels: Labels,
    mut rx: mpsc::Receiver<StreamPage>,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    while let Some(page) = rx.recv().await {
        if cancelled(&cancel) {
            break;
        }
        let records = instrumented_apply_stages(page.records, &stages, &labels)?;
        let out = StreamPage {
            records,
            bookmark: page.bookmark,
        };
        if !broadcast(out, &mut outs).await {
            break;
        }
    }
    Ok(NodeOutcome::Other)
}

fn node_labels(pipeline: &str, node: &str) -> Vec<Label> {
    vec![
        Label::new("pipeline", SharedString::from(pipeline.to_string())),
        Label::new("node", SharedString::from(node.to_string())),
    ]
}

async fn run_tee_node(
    node_id: String,
    pipeline: String,
    mut rx: mpsc::Receiver<StreamPage>,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    let labels = node_labels(&pipeline, &node_id);
    while let Some(page) = rx.recv().await {
        if cancelled(&cancel) {
            break;
        }
        counter!("faucet_tee_records_total", labels.clone()).increment(page.records.len() as u64);
        if !broadcast(page, &mut outs).await {
            break;
        }
    }
    Ok(NodeOutcome::Other)
}

async fn run_merge_node(
    node_id: String,
    pipeline: String,
    rxs: Vec<mpsc::Receiver<StreamPage>>,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    let labels = node_labels(&pipeline, &node_id);
    let streams = rxs.into_iter().map(|mut rx| {
        Box::pin(async_stream::stream! {
            while let Some(p) = rx.recv().await {
                yield p;
            }
        }) as Pin<Box<dyn futures::Stream<Item = StreamPage> + Send>>
    });
    let mut sel = futures::stream::select_all(streams);
    while let Some(page) = sel.next().await {
        if cancelled(&cancel) {
            break;
        }
        counter!("faucet_merge_records_total", labels.clone()).increment(page.records.len() as u64);
        if !broadcast(page, &mut outs).await {
            break;
        }
    }
    Ok(NodeOutcome::Other)
}

#[allow(clippy::too_many_arguments)]
async fn run_join_node(
    node_id: String,
    pipeline: String,
    j: JoinNode,
    mut build_rx: mpsc::Receiver<StreamPage>,
    mut probe_rx: mpsc::Receiver<StreamPage>,
    mut outs: Vec<mpsc::Sender<StreamPage>>,
    cancel: Option<CancellationToken>,
) -> Result<NodeOutcome, FaucetError> {
    let mode = j.config.mode;
    let mut join = HashJoin::new(j.config);

    // Build phase: fully drain the build side before probing.
    let build_start = std::time::Instant::now();
    while let Some(page) = build_rx.recv().await {
        if cancelled(&cancel) {
            return Ok(NodeOutcome::Other);
        }
        join.add_build_page(page.records)?;
    }
    let labels = node_labels(&pipeline, &node_id);
    histogram!("faucet_join_build_duration_seconds", labels.clone())
        .record(build_start.elapsed().as_secs_f64());

    // Probe phase.
    while let Some(page) = probe_rx.recv().await {
        if cancelled(&cancel) {
            break;
        }
        let enriched = join.probe_page(page.records)?;
        let out = StreamPage {
            records: enriched,
            bookmark: page.bookmark,
        };
        if !broadcast(out, &mut outs).await {
            break;
        }
    }

    emit_join_metrics(&labels, mode, join.stats());
    Ok(NodeOutcome::Other)
}

fn emit_join_metrics(labels: &[Label], mode: JoinMode, stats: &crate::join::JoinStats) {
    counter!("faucet_join_build_records_total", labels.to_vec()).increment(stats.build_records);
    counter!("faucet_join_build_nulls_total", labels.to_vec()).increment(stats.build_nulls);
    counter!("faucet_join_duplicates_total", labels.to_vec()).increment(stats.duplicates);
    counter!("faucet_join_probe_records_total", labels.to_vec()).increment(stats.probe_records);
    counter!("faucet_join_project_misses_total", labels.to_vec()).increment(stats.project_misses);
    let mut match_labels = labels.to_vec();
    match_labels.push(Label::new("kind", SharedString::from(mode.to_string())));
    counter!("faucet_join_matches_total", match_labels.clone()).increment(stats.matches);
    counter!("faucet_join_misses_total", match_labels).increment(stats.misses);
}

struct SinkNodeOpts {
    pipeline_name: String,
    run_id: String,
    state_store: Option<Arc<dyn StateStore>>,
    dlq: Option<DlqConfig>,
    cancel: Option<CancellationToken>,
}

async fn run_sink_node(
    node_id: String,
    sink: Box<dyn Sink>,
    mut rx: mpsc::Receiver<StreamPage>,
    opts: SinkNodeOpts,
) -> Result<NodeOutcome, FaucetError> {
    let pages = Box::pin(async_stream::stream! {
        while let Some(page) = rx.recv().await {
            yield Ok::<StreamPage, FaucetError>(page);
        }
    });

    let mut run_opts = RunStreamOptions::new()
        .with_name(opts.pipeline_name.clone())
        .with_row(node_id.clone())
        .with_run_id(opts.run_id.clone());
    if let Some(store) = opts.state_store {
        let key = format!("{}::{}", opts.pipeline_name, node_id);
        run_opts = run_opts.with_state(store, key);
    }
    if let Some(dlq) = opts.dlq {
        run_opts = run_opts.with_dlq(dlq);
    }
    if let Some(cancel) = opts.cancel {
        run_opts = run_opts.with_cancel(cancel);
    }

    let result = run_stream(pages, sink.as_ref(), run_opts).await?;
    Ok(NodeOutcome::Sink {
        node_id,
        records: result.records_written,
        bookmark: result.bookmark,
    })
}

// ── Builder ──────────────────────────────────────────────────────────────────

/// Fluent builder for a [`Topology`].
#[derive(Default)]
pub struct TopologyBuilder {
    nodes: Vec<Node>,
    edges: Vec<Edge>,
}

impl TopologyBuilder {
    /// Add a node of any kind.
    pub fn node(mut self, id: impl Into<String>, kind: NodeKind) -> Self {
        self.nodes.push(Node {
            id: id.into(),
            kind,
        });
        self
    }

    /// Add a source node.
    pub fn source(self, id: impl Into<String>, source: Box<dyn Source>) -> Self {
        self.node(id, NodeKind::Source(source))
    }

    /// Add a transform node.
    pub fn transform(self, id: impl Into<String>, stages: Vec<CompiledStage>) -> Self {
        self.node(id, NodeKind::Transform(stages))
    }

    /// Add a tee (fan-out) node.
    pub fn tee(self, id: impl Into<String>, capacity: usize, fanout: Option<usize>) -> Self {
        self.node(id, NodeKind::Tee { capacity, fanout })
    }

    /// Add a merge (fan-in) node.
    pub fn merge(self, id: impl Into<String>) -> Self {
        self.node(id, NodeKind::Merge)
    }

    /// Add a join node.
    pub fn join(self, id: impl Into<String>, join: JoinNode) -> Self {
        self.node(id, NodeKind::Join(join))
    }

    /// Add a sink node.
    pub fn sink(self, id: impl Into<String>, sink: Box<dyn Sink>) -> Self {
        self.node(id, NodeKind::Sink(sink))
    }

    /// Add an unlabelled edge.
    pub fn edge(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
            label: None,
        });
        self
    }

    /// Add a labelled edge (used by join build/probe wiring).
    pub fn labelled_edge(
        mut self,
        from: impl Into<String>,
        to: impl Into<String>,
        label: impl Into<String>,
    ) -> Self {
        self.edges.push(Edge {
            from: from.into(),
            to: to.into(),
            label: Some(label.into()),
        });
        self
    }

    /// Finalize and validate the topology.
    pub fn build(self) -> Result<Topology, FaucetError> {
        let t = Topology {
            nodes: self.nodes,
            edges: self.edges,
        };
        t.validate()?;
        Ok(t)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::join::{JoinConfig, JoinMode, Projection};
    use crate::state::MemoryStateStore;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Mutex;

    // ── Mock connectors ───────────────────────────────────────────────────────

    struct VecSource {
        records: Vec<Value>,
        bookmark: Option<Value>,
    }
    impl VecSource {
        fn boxed(records: Vec<Value>) -> Box<dyn Source> {
            Box::new(VecSource {
                records,
                bookmark: None,
            })
        }
        fn boxed_bm(records: Vec<Value>, bm: Value) -> Box<dyn Source> {
            Box::new(VecSource {
                records,
                bookmark: Some(bm),
            })
        }
    }
    #[async_trait]
    impl Source for VecSource {
        async fn fetch_with_context(
            &self,
            _c: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn fetch_with_context_incremental(
            &self,
            _c: &std::collections::HashMap<String, Value>,
        ) -> Result<(Vec<Value>, Option<Value>), FaucetError> {
            Ok((self.records.clone(), self.bookmark.clone()))
        }
    }

    struct FailingSource;
    #[async_trait]
    impl Source for FailingSource {
        async fn fetch_with_context(
            &self,
            _c: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Err(FaucetError::Source("boom".into()))
        }
    }

    /// Records the bookmark applied via `apply_start_bookmark`.
    struct RecordingSource {
        records: Vec<Value>,
        applied: Arc<Mutex<Option<Value>>>,
    }
    #[async_trait]
    impl Source for RecordingSource {
        async fn fetch_with_context(
            &self,
            _c: &std::collections::HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
        async fn apply_start_bookmark(&self, bm: Value) -> Result<(), FaucetError> {
            *self.applied.lock().unwrap() = Some(bm);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct CollectSink {
        store: Arc<Mutex<Vec<Value>>>,
    }
    impl CollectSink {
        fn new() -> (Self, Arc<Mutex<Vec<Value>>>) {
            let store = Arc::new(Mutex::new(Vec::new()));
            (
                Self {
                    store: store.clone(),
                },
                store,
            )
        }
    }
    #[async_trait]
    impl Sink for CollectSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.store.lock().unwrap().extend_from_slice(records);
            Ok(records.len())
        }
    }

    struct FailingSink;
    #[async_trait]
    impl Sink for FailingSink {
        async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
            Err(FaucetError::Sink("sink boom".into()))
        }
    }

    fn recs(n: usize) -> Vec<Value> {
        (0..n).map(|i| json!({ "i": i })).collect()
    }

    // ── Validation ────────────────────────────────────────────────────────────

    #[test]
    fn validate_rejects_empty() {
        let err = Topology {
            nodes: vec![],
            edges: vec![],
        }
        .validate()
        .unwrap_err();
        assert!(err.to_string().contains("no nodes"));
    }

    #[test]
    fn validate_rejects_duplicate_id() {
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .source("a", VecSource::boxed(recs(1)))
            .sink("a", Box::new(sink))
            .edge("a", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("duplicate node id"));
    }

    #[test]
    fn validate_rejects_unknown_endpoint() {
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .edge("s", "ghost")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("unknown 'to' node 'ghost'"));
    }

    #[test]
    fn validate_rejects_unknown_from_endpoint() {
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .sink("k", Box::new(sink))
            .edge("ghost", "k")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("unknown 'from' node 'ghost'"));
    }

    #[test]
    fn validate_rejects_source_with_incoming_edge() {
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .edge("k", "s") // sink→source: gives source in=1 and sink out=1
            .build()
            .unwrap_err();
        // Either arity or cycle is caught; both are correct rejections.
        assert!(err.to_string().contains("arity") || err.to_string().contains("cycle"));
    }

    #[test]
    fn validate_rejects_tee_fanout_mismatch() {
        let (s1, _) = CollectSink::new();
        let (s2, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .tee("t", 4, Some(3))
            .sink("a", Box::new(s1))
            .sink("b", Box::new(s2))
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("fanout 3 but has 2"));
    }

    #[test]
    fn validate_rejects_tee_with_one_output() {
        let (s1, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .tee("t", 4, None)
            .sink("a", Box::new(s1))
            .edge("s", "t")
            .edge("t", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("tee 't'"));
    }

    #[test]
    fn validate_rejects_merge_with_one_input() {
        let (s1, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .merge("m")
            .sink("a", Box::new(s1))
            .edge("s", "m")
            .edge("m", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("merge 'm'"));
    }

    #[test]
    fn validate_rejects_join_missing_label() {
        let (s1, _) = CollectSink::new();
        let jn = JoinNode {
            config: JoinConfig::default(),
            build_edge: "build".into(),
            probe_edge: "probe".into(),
        };
        let err = Topology::builder()
            .source("b", VecSource::boxed(recs(1)))
            .source("p", VecSource::boxed(recs(1)))
            .join("j", jn)
            .sink("a", Box::new(s1))
            .labelled_edge("b", "j", "build")
            .edge("p", "j") // unlabelled — probe label missing
            .edge("j", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("labelled 'probe'"));
    }

    #[test]
    fn validate_rejects_join_same_labels() {
        let (s1, _) = CollectSink::new();
        let jn = JoinNode {
            config: JoinConfig::default(),
            build_edge: "x".into(),
            probe_edge: "x".into(),
        };
        let err = Topology::builder()
            .source("b", VecSource::boxed(recs(1)))
            .source("p", VecSource::boxed(recs(1)))
            .join("j", jn)
            .sink("a", Box::new(s1))
            .labelled_edge("b", "j", "x")
            .labelled_edge("p", "j", "x")
            .edge("j", "a")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("must differ"));
    }

    #[test]
    fn validate_rejects_cycle() {
        // s → m(merge) → t(tee) → {m, k}. The m→t→m loop is a valid-arity
        // cycle (merge absorbs the back-edge, tee provides the second out).
        let (sink, _) = CollectSink::new();
        let err = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .merge("m")
            .tee("t", 4, None)
            .sink("k", Box::new(sink))
            .edge("s", "m")
            .edge("m", "t")
            .edge("t", "m")
            .edge("t", "k")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("cycle"), "{err}");
    }

    #[test]
    fn validate_rejects_no_source() {
        // Two transforms wired in a ring: valid arity, but no source node.
        let err = Topology::builder()
            .transform("t1", vec![])
            .transform("t2", vec![])
            .edge("t1", "t2")
            .edge("t2", "t1")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("no source"), "{err}");
    }

    #[test]
    fn validate_rejects_no_sink() {
        // Two sources into a self-looping merge: valid arity, but no sink.
        let err = Topology::builder()
            .source("s1", VecSource::boxed(recs(1)))
            .source("s2", VecSource::boxed(recs(1)))
            .merge("m")
            .edge("s1", "m")
            .edge("s2", "m")
            .edge("m", "m")
            .build()
            .unwrap_err();
        assert!(err.to_string().contains("no sink"), "{err}");
    }

    // ── Execution ─────────────────────────────────────────────────────────────

    #[tokio::test]
    async fn simple_source_to_sink() {
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(5)))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        assert_eq!(result.records_written, 5);
        assert_eq!(store.lock().unwrap().len(), 5);
        assert_eq!(result.per_sink.get("k"), Some(&5));
    }

    #[tokio::test]
    async fn source_transform_sink() {
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(3)))
            .transform("t", vec![]) // passthrough
            .sink("k", Box::new(sink))
            .edge("s", "t")
            .edge("t", "k")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        assert_eq!(result.records_written, 3);
        assert_eq!(store.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn tee_fans_out_to_three_sinks() {
        let (s1, st1) = CollectSink::new();
        let (s2, st2) = CollectSink::new();
        let (s3, st3) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(10)))
            .tee("t", 4, Some(3))
            .sink("a", Box::new(s1))
            .sink("b", Box::new(s2))
            .sink("c", Box::new(s3))
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .edge("t", "c")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        assert_eq!(st1.lock().unwrap().len(), 10);
        assert_eq!(st2.lock().unwrap().len(), 10);
        assert_eq!(st3.lock().unwrap().len(), 10);
        assert_eq!(result.records_written, 30);
    }

    #[tokio::test]
    async fn merge_fans_in_two_sources() {
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s1", VecSource::boxed(recs(4)))
            .source("s2", VecSource::boxed(recs(6)))
            .merge("m")
            .sink("k", Box::new(sink))
            .edge("s1", "m")
            .edge("s2", "m")
            .edge("m", "k")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        assert_eq!(result.records_written, 10);
        assert_eq!(store.lock().unwrap().len(), 10);
    }

    #[tokio::test]
    async fn join_enriches_end_to_end() {
        let (sink, store) = CollectSink::new();
        let customers = vec![
            json!({"id": 1, "tier": "gold"}),
            json!({"id": 2, "tier": "silver"}),
        ];
        let orders = vec![
            json!({"order": "A", "cust": 1}),
            json!({"order": "B", "cust": 2}),
            json!({"order": "C", "cust": 99}),
        ];
        let jn = JoinNode {
            config: JoinConfig {
                mode: JoinMode::Inner,
                build_key: "id".into(),
                probe_key: "cust".into(),
                projections: vec![Projection {
                    from: "tier".into(),
                    as_: "tier".into(),
                }],
                ..Default::default()
            },
            build_edge: "customers".into(),
            probe_edge: "orders".into(),
        };
        let topo = Topology::builder()
            .source("c", VecSource::boxed(customers))
            .source("o", VecSource::boxed(orders))
            .join("j", jn)
            .sink("k", Box::new(sink))
            .labelled_edge("c", "j", "customers")
            .labelled_edge("o", "j", "orders")
            .edge("j", "k")
            .build()
            .unwrap();
        let result = topo.run(TopologyOptions::new("p")).await.unwrap();
        // inner join: C (cust 99) drops → 2 enriched records.
        assert_eq!(result.records_written, 2);
        let written = store.lock().unwrap();
        assert!(
            written
                .iter()
                .any(|r| r["order"] == json!("A") && r["tier"] == json!("gold"))
        );
    }

    #[tokio::test]
    async fn propagate_aborts_on_sink_failure() {
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(3)))
            .sink("k", Box::new(FailingSink))
            .edge("s", "k")
            .build()
            .unwrap();
        let err = topo.run(TopologyOptions::new("p")).await.unwrap_err();
        assert!(matches!(err, FaucetError::Sink(_)));
    }

    #[tokio::test]
    async fn propagate_aborts_on_source_failure() {
        let (sink, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", Box::new(FailingSource))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        let err = topo.run(TopologyOptions::new("p")).await.unwrap_err();
        assert!(matches!(err, FaucetError::Source(_)));
    }

    #[tokio::test]
    async fn continue_lets_healthy_branch_finish() {
        // One branch fails, the other still receives every record.
        let (good, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(8)))
            .tee("t", 8, Some(2))
            .sink("bad", Box::new(FailingSink))
            .sink("good", Box::new(good))
            .edge("s", "t")
            .edge("t", "bad")
            .edge("t", "good")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_on_error(TopologyOnError::Continue);
        let result = topo.run(opts).await.unwrap();
        assert_eq!(store.lock().unwrap().len(), 8);
        assert!(!result.errors.is_empty(), "failing sink should be recorded");
    }

    #[tokio::test]
    async fn state_min_bookmark_applied_to_source() {
        let store = Arc::new(MemoryStateStore::new());
        // Two sinks with diverged bookmarks; source must resume from min.
        store.put("p::a", &json!(250)).await.unwrap();
        store.put("p::b", &json!(100)).await.unwrap();
        let applied = Arc::new(Mutex::new(None));
        let src = RecordingSource {
            records: recs(1),
            applied: applied.clone(),
        };
        let (s1, _) = CollectSink::new();
        let (s2, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", Box::new(src))
            .tee("t", 4, Some(2))
            .sink("a", Box::new(s1))
            .sink("b", Box::new(s2))
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        topo.run(opts).await.unwrap();
        assert_eq!(*applied.lock().unwrap(), Some(json!(100)));
    }

    #[tokio::test]
    async fn state_no_bookmark_when_a_sink_is_missing() {
        let store = Arc::new(MemoryStateStore::new());
        store.put("p::a", &json!(100)).await.unwrap();
        // sink b has no stored bookmark → full replay (no apply).
        let applied = Arc::new(Mutex::new(None));
        let src = RecordingSource {
            records: recs(1),
            applied: applied.clone(),
        };
        let (s1, _) = CollectSink::new();
        let (s2, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", Box::new(src))
            .tee("t", 4, Some(2))
            .sink("a", Box::new(s1))
            .sink("b", Box::new(s2))
            .edge("s", "t")
            .edge("t", "a")
            .edge("t", "b")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        topo.run(opts).await.unwrap();
        assert_eq!(*applied.lock().unwrap(), None);
    }

    #[tokio::test]
    async fn sink_persists_bookmark() {
        let store = Arc::new(MemoryStateStore::new());
        let (sink, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed_bm(recs(2), json!("v9")))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_state_store(store.clone());
        let result = topo.run(opts).await.unwrap();
        assert_eq!(result.bookmarks.get("k"), Some(&Some(json!("v9"))));
        assert_eq!(store.get("p::k").await.unwrap(), Some(json!("v9")));
    }

    #[tokio::test]
    async fn cancellation_stops_the_run() {
        let cancel = CancellationToken::new();
        cancel.cancel(); // pre-cancelled
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(1000)))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        let opts = TopologyOptions::new("p").with_cancel(cancel);
        let result = topo.run(opts).await.unwrap();
        // Cancelled before/early: far fewer than 1000 records written.
        assert!(store.lock().unwrap().len() < 1000);
        let _ = result;
    }

    #[test]
    fn min_bookmark_picks_smallest() {
        assert_eq!(
            min_bookmark(&[json!(250), json!(100), json!(300)]),
            Some(json!(100))
        );
        assert_eq!(min_bookmark(&[]), None);
    }

    #[test]
    fn kind_str_matches() {
        assert_eq!(NodeKind::Merge.kind_str(), "merge");
        assert_eq!(
            NodeKind::Tee {
                capacity: 1,
                fanout: None
            }
            .kind_str(),
            "tee"
        );
    }

    #[test]
    fn builder_exposes_nodes_and_edges() {
        let (sink, _) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(recs(1)))
            .sink("k", Box::new(sink))
            .edge("s", "k")
            .build()
            .unwrap();
        assert_eq!(topo.nodes().len(), 2);
        assert_eq!(topo.edges().len(), 1);
    }

    #[cfg(feature = "transform-keys-case")]
    #[tokio::test]
    async fn transform_node_applies_stage() {
        use crate::stage::{TransformStage, compile_stage};
        use crate::transform::{KeyCaseMode, RecordTransform};
        let stage = compile_stage(&TransformStage::Map(RecordTransform::KeysCase {
            mode: KeyCaseMode::Snake,
        }))
        .unwrap();
        let (sink, store) = CollectSink::new();
        let topo = Topology::builder()
            .source("s", VecSource::boxed(vec![json!({"FooBar": 1})]))
            .transform("t", vec![stage])
            .sink("k", Box::new(sink))
            .edge("s", "t")
            .edge("t", "k")
            .build()
            .unwrap();
        topo.run(TopologyOptions::new("p")).await.unwrap();
        let w = store.lock().unwrap();
        assert!(w[0].get("foo_bar").is_some());
    }
}
