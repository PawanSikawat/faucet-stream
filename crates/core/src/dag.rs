//! DAG-based orchestration for parent-child source pipelines.
//!
//! [`SourceDAG`] lets you wire multiple [`Source`]–[`Sink`] pairs into a
//! directed acyclic graph where child sources receive context extracted from
//! their parent's records.

use crate::error::FaucetError;
use crate::traits::{Sink, Source};
use crate::util::extract_context;
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::Semaphore;

/// A node in the source DAG.
pub struct DagNode {
    /// Human-readable name for this node (must be unique within the DAG).
    pub name: String,
    /// The source that fetches records.
    pub source: Box<dyn Source>,
    /// The sink that receives fetched records.
    pub sink: Box<dyn Sink>,
    /// Maps context key names to JSONPath expressions evaluated against each
    /// parent record.  For root nodes this is empty.
    pub context_mapping: HashMap<String, String>,
    /// Whether to inject the resolved context map into each record written to
    /// the sink (as top-level fields).
    pub inject_context: bool,
}

/// Result of a full DAG run.
#[derive(Debug)]
pub struct DagResult {
    /// Per-node results keyed by node name.
    pub node_results: HashMap<String, DagNodeResult>,
}

/// Result for a single node.
#[derive(Debug)]
pub struct DagNodeResult {
    /// Total records written to the sink.
    pub records_written: usize,
    /// Number of parent records that triggered this node (0 for root nodes).
    pub parent_records_processed: usize,
    /// Non-fatal errors encountered while processing individual parent records.
    pub errors: Vec<DagNodeError>,
}

/// Error from processing a single parent record.
#[derive(Debug)]
pub struct DagNodeError {
    /// The context values that were active when the error occurred.
    pub context: HashMap<String, Value>,
    /// The error that occurred.
    pub error: FaucetError,
}

/// Builder and executor for a source DAG.
///
/// Build up the graph with [`add_root`](Self::add_root) and
/// [`add_child`](Self::add_child), then call [`validate`](Self::validate) to
/// check for structural errors before running.
pub struct SourceDAG {
    nodes: HashMap<String, DagNode>,
    edges: HashMap<String, Vec<String>>, // parent -> children
    children: HashSet<String>,
    concurrency: usize,
}

impl SourceDAG {
    /// Create an empty DAG with default concurrency (10).
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            children: HashSet::new(),
            concurrency: 10,
        }
    }

    /// Add a root node (no parent).
    pub fn add_root(
        mut self,
        name: impl Into<String>,
        source: Box<dyn Source>,
        sink: Box<dyn Sink>,
    ) -> Self {
        let name = name.into();
        self.nodes.insert(
            name.clone(),
            DagNode {
                name: name.clone(),
                source,
                sink,
                context_mapping: HashMap::new(),
                inject_context: false,
            },
        );
        self
    }

    /// Add a child node that depends on `parent`.
    ///
    /// `context_mapping` maps context key names to JSONPath expressions that
    /// are evaluated against each parent record.  When `inject_context` is
    /// true, the resolved context values are merged into every record before
    /// writing to the sink.
    pub fn add_child(
        mut self,
        name: impl Into<String>,
        parent: impl Into<String>,
        source: Box<dyn Source>,
        sink: Box<dyn Sink>,
        context_mapping: HashMap<String, String>,
        inject_context: bool,
    ) -> Self {
        let name = name.into();
        let parent = parent.into();

        self.nodes.insert(
            name.clone(),
            DagNode {
                name: name.clone(),
                source,
                sink,
                context_mapping,
                inject_context,
            },
        );
        self.edges.entry(parent).or_default().push(name.clone());
        self.children.insert(name);
        self
    }

    /// Set the maximum number of concurrent child-source invocations.
    ///
    /// Values below 1 are clamped to 1 to prevent deadlocks.
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n.max(1);
        self
    }

    /// Validate the DAG structure.
    ///
    /// Returns `Err` if:
    /// 1. The DAG is empty (no nodes).
    /// 2. A parent referenced by an edge does not exist as a node.
    /// 3. There are no root nodes (every node is someone's child).
    /// 4. The graph contains a cycle (detected via Kahn's algorithm).
    pub fn validate(&self) -> Result<(), FaucetError> {
        // 1. Empty DAG
        if self.nodes.is_empty() {
            return Err(FaucetError::Config("DAG has no nodes".into()));
        }

        // 2. All parent references must exist as nodes, and all children must
        //    exist as nodes.
        for (parent, children) in &self.edges {
            if !self.nodes.contains_key(parent) {
                return Err(FaucetError::Config(format!(
                    "parent node '{parent}' referenced in edges does not exist"
                )));
            }
            for child in children {
                if !self.nodes.contains_key(child) {
                    return Err(FaucetError::Config(format!(
                        "child node '{child}' referenced in edges does not exist"
                    )));
                }
            }
        }

        // 3. At least one root node (a node that is not a child of any other).
        let roots: Vec<&String> = self
            .nodes
            .keys()
            .filter(|n| !self.children.contains(*n))
            .collect();
        if roots.is_empty() {
            return Err(FaucetError::Config(
                "DAG has no root nodes — every node is a child of another".into(),
            ));
        }

        // 4. Cycle detection via Kahn's algorithm (topological sort).
        //    Build in-degree map from edges.
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        for name in self.nodes.keys() {
            in_degree.insert(name.as_str(), 0);
        }
        for children in self.edges.values() {
            for child in children {
                *in_degree.entry(child.as_str()).or_default() += 1;
            }
        }

        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(name, _)| *name)
            .collect();

        let mut visited = 0usize;
        while let Some(node) = queue.pop_front() {
            visited += 1;
            if let Some(children) = self.edges.get(node) {
                for child in children {
                    let deg = in_degree.get_mut(child.as_str()).expect("node in edges");
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(child.as_str());
                    }
                }
            }
        }

        if visited != self.nodes.len() {
            return Err(FaucetError::Config("DAG contains a cycle".into()));
        }

        // 5. Each child node may have at most one parent.
        //    The current builder API (`add_child`) only wires one parent per call,
        //    but a caller could add the same child under two parents by calling
        //    `add_child` twice with different parents (the second call overwrites
        //    the node but both edges survive).  Catch that here.
        let mut child_in_degree: HashMap<&str, usize> = HashMap::new();
        for children in self.edges.values() {
            for child in children {
                *child_in_degree.entry(child.as_str()).or_insert(0) += 1;
            }
        }
        for (child, degree) in &child_in_degree {
            if *degree > 1 {
                return Err(FaucetError::Config(format!(
                    "child node '{child}' has multiple parents; diamond dependencies are not supported"
                )));
            }
        }

        Ok(())
    }

    /// Return a reference to the internal node map.
    pub fn nodes(&self) -> &HashMap<String, DagNode> {
        &self.nodes
    }

    /// Return a reference to the edge map (parent -> children).
    pub fn edges(&self) -> &HashMap<String, Vec<String>> {
        &self.edges
    }

    /// Return the set of node names that are children (non-root).
    pub fn children_set(&self) -> &HashSet<String> {
        &self.children
    }

    /// Return the configured concurrency limit.
    pub fn concurrency_limit(&self) -> usize {
        self.concurrency
    }

    /// Compute topological order using Kahn's algorithm.
    ///
    /// Assumes the DAG has already been validated (no cycles).
    fn topological_order(&self) -> Vec<String> {
        let mut in_degree: HashMap<&str, usize> = HashMap::new();
        for name in self.nodes.keys() {
            in_degree.insert(name.as_str(), 0);
        }
        for children in self.edges.values() {
            for child in children {
                *in_degree.entry(child.as_str()).or_default() += 1;
            }
        }

        let mut queue: VecDeque<&str> = in_degree
            .iter()
            .filter(|(_, deg)| **deg == 0)
            .map(|(name, _)| *name)
            .collect();

        let mut order = Vec::with_capacity(self.nodes.len());
        while let Some(node) = queue.pop_front() {
            order.push(node.to_string());
            if let Some(children) = self.edges.get(node) {
                for child in children {
                    let deg = in_degree.get_mut(child.as_str()).expect("node in edges");
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(child.as_str());
                    }
                }
            }
        }
        order
    }

    /// Find the parent of a child node.
    fn parent_of(&self, child: &str) -> Option<String> {
        for (parent, children) in &self.edges {
            if children.iter().any(|c| c == child) {
                return Some(parent.clone());
            }
        }
        None
    }

    /// Execute the DAG: fetch from all sources and write to all sinks.
    ///
    /// Root nodes are processed sequentially in topological order. For each
    /// child node, parent records are expanded concurrently (bounded by
    /// [`concurrency`](Self::concurrency)).
    pub async fn run(&self) -> Result<DagResult, FaucetError> {
        self.validate()?;

        let order = self.topological_order();
        let semaphore = Arc::new(Semaphore::new(self.concurrency.max(1)));

        // Stores output records per node for use by children.
        let mut node_records: HashMap<String, Vec<Value>> = HashMap::new();
        let mut node_results: HashMap<String, DagNodeResult> = HashMap::new();

        for name in &order {
            let node = self.nodes.get(name).expect("node exists after validation");
            let is_root = !self.children.contains(name);

            if is_root {
                // Root node: fetch with empty context
                let records = node.source.fetch_with_context(&HashMap::new()).await?;
                let written = node.sink.write_batch(&records).await?;
                node_records.insert(name.clone(), records);
                node_results.insert(
                    name.clone(),
                    DagNodeResult {
                        records_written: written,
                        parent_records_processed: 0,
                        errors: Vec::new(),
                    },
                );
            } else {
                // Child node: expand over parent records concurrently
                let parent_name = self
                    .parent_of(name)
                    .expect("child must have parent after validation");
                let parent_records = node_records.get(&parent_name).cloned().unwrap_or_default();

                let parent_count = parent_records.len();

                // Build a future for each parent record, bounded by semaphore.
                let source = &node.source;
                let sink = &node.sink;
                let mapping = &node.context_mapping;
                let inject = node.inject_context;

                let futs: Vec<_> = parent_records
                    .iter()
                    .map(|parent_record| {
                        let sem = Arc::clone(&semaphore);
                        async move {
                            let _permit = sem.acquire().await.expect("semaphore not closed");

                            let ctx = match extract_context(parent_record, mapping) {
                                Ok(c) => c,
                                Err(e) => {
                                    return (
                                        Vec::new(),
                                        Some(DagNodeError {
                                            context: HashMap::new(),
                                            error: e,
                                        }),
                                        0usize,
                                    );
                                }
                            };

                            let mut records = match source.fetch_with_context(&ctx).await {
                                Ok(r) => r,
                                Err(e) => {
                                    return (
                                        Vec::new(),
                                        Some(DagNodeError {
                                            context: ctx,
                                            error: e,
                                        }),
                                        0usize,
                                    );
                                }
                            };

                            // Inject context into records if configured
                            if inject {
                                for record in &mut records {
                                    if let Value::Object(map) = record {
                                        for (k, v) in &ctx {
                                            map.insert(k.clone(), v.clone());
                                        }
                                    }
                                }
                            }

                            // Write to sink
                            match sink.write_batch(&records).await {
                                Ok(n) => (records, None, n),
                                Err(e) => (
                                    Vec::new(),
                                    Some(DagNodeError {
                                        context: ctx,
                                        error: e,
                                    }),
                                    0usize,
                                ),
                            }
                        }
                    })
                    .collect();

                let results = futures::future::join_all(futs).await;

                let mut all_records: Vec<Value> = Vec::new();
                let mut errors: Vec<DagNodeError> = Vec::new();
                let mut total_written: usize = 0;

                for (records, maybe_err, written) in results {
                    total_written += written;
                    all_records.extend(records);
                    if let Some(err) = maybe_err {
                        errors.push(err);
                    }
                }

                node_records.insert(name.clone(), all_records);
                node_results.insert(
                    name.clone(),
                    DagNodeResult {
                        records_written: total_written,
                        parent_records_processed: parent_count,
                        errors,
                    },
                );
            }
        }

        // Flush all sinks
        for name in &order {
            let node = self.nodes.get(name).expect("node exists");
            node.sink.flush().await?;
        }

        Ok(DagResult { node_results })
    }
}

impl Default for SourceDAG {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::Mutex;

    // ── Mock Source ──────────────────────────────────────────────────────

    struct MockSource {
        records: Vec<Value>,
    }

    #[async_trait]
    impl Source for MockSource {
        async fn fetch_with_context(
            &self,
            _context: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Ok(self.records.clone())
        }
    }

    // ── Mock Sink ───────────────────────────────────────────────────────

    struct CollectingSink {
        written: Mutex<Vec<Value>>,
    }

    impl CollectingSink {
        fn new() -> Self {
            Self {
                written: Mutex::new(Vec::new()),
            }
        }

        fn records(&self) -> Vec<Value> {
            self.written.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl Sink for CollectingSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            let mut w = self.written.lock().unwrap();
            w.extend(records.iter().cloned());
            Ok(records.len())
        }
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    fn mock_source(records: Vec<Value>) -> Box<dyn Source> {
        Box::new(MockSource { records })
    }

    fn mock_sink() -> Box<dyn Sink> {
        Box::new(CollectingSink::new())
    }

    // ── Validation tests ────────────────────────────────────────────────

    #[test]
    fn validate_empty_dag_returns_error() {
        let dag = SourceDAG::new();
        let result = dag.validate();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("no nodes"), "unexpected error: {msg}");
    }

    #[test]
    fn validate_single_root_passes() {
        let dag =
            SourceDAG::new().add_root("root", mock_source(vec![json!({"id": 1})]), mock_sink());
        assert!(dag.validate().is_ok());
    }

    #[test]
    fn validate_missing_parent_returns_error() {
        let dag = SourceDAG::new().add_child(
            "child",
            "nonexistent_parent",
            mock_source(vec![]),
            mock_sink(),
            HashMap::from([("id".into(), "$.id".into())]),
            false,
        );
        let result = dag.validate();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(
            msg.contains("nonexistent_parent"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn validate_valid_chain_passes() {
        let dag = SourceDAG::new()
            .add_root("root", mock_source(vec![json!({"id": 1})]), mock_sink())
            .add_child(
                "child",
                "root",
                mock_source(vec![]),
                mock_sink(),
                HashMap::from([("parent_id".into(), "$.id".into())]),
                true,
            );
        assert!(dag.validate().is_ok());
    }

    #[test]
    fn validate_cycle_returns_error() {
        // root -> a -> b -> a  (cycle between a and b, root is still a root)
        let mut dag = SourceDAG::new()
            .add_root("root", mock_source(vec![]), mock_sink())
            .add_child(
                "a",
                "root",
                mock_source(vec![]),
                mock_sink(),
                HashMap::new(),
                false,
            )
            .add_child(
                "b",
                "a",
                mock_source(vec![]),
                mock_sink(),
                HashMap::new(),
                false,
            );
        // Force a back-edge from B -> A to create a cycle
        dag.edges.entry("b".into()).or_default().push("a".into());

        let result = dag.validate();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("cycle"), "unexpected error: {msg}");
    }

    #[test]
    fn validate_no_root_returns_error() {
        // Every node is a child — no root exists.
        let mut dag = SourceDAG::new();
        dag.nodes.insert(
            "a".into(),
            DagNode {
                name: "a".into(),
                source: mock_source(vec![]),
                sink: mock_sink(),
                context_mapping: HashMap::new(),
                inject_context: false,
            },
        );
        dag.children.insert("a".into());

        let result = dag.validate();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("no root"), "unexpected error: {msg}");
    }

    #[test]
    fn validate_multi_parent_returns_error() {
        // Build a diamond: A -> C and B -> C
        let mut dag = SourceDAG::new()
            .add_root("a", mock_source(vec![]), mock_sink())
            .add_root("b", mock_source(vec![]), mock_sink())
            .add_child(
                "c",
                "a",
                mock_source(vec![]),
                mock_sink(),
                HashMap::new(),
                false,
            );
        // Manually add a second parent edge: b -> c (the builder only wires one parent)
        dag.edges.entry("b".into()).or_default().push("c".into());

        let result = dag.validate();
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("multiple parents"), "unexpected error: {msg}");
    }

    #[test]
    fn default_concurrency_is_10() {
        let dag = SourceDAG::new();
        assert_eq!(dag.concurrency_limit(), 10);
    }

    #[test]
    fn concurrency_is_configurable() {
        let dag = SourceDAG::new().concurrency(5);
        assert_eq!(dag.concurrency_limit(), 5);
    }

    #[test]
    fn concurrency_zero_clamps_to_one() {
        let dag = SourceDAG::new().concurrency(0);
        assert_eq!(dag.concurrency_limit(), 1);
    }

    #[test]
    fn validate_multi_level_chain_passes() {
        let dag = SourceDAG::new()
            .add_root("root", mock_source(vec![]), mock_sink())
            .add_child(
                "mid",
                "root",
                mock_source(vec![]),
                mock_sink(),
                HashMap::from([("id".into(), "$.id".into())]),
                false,
            )
            .add_child(
                "leaf",
                "mid",
                mock_source(vec![]),
                mock_sink(),
                HashMap::from([("mid_id".into(), "$.mid_id".into())]),
                true,
            );
        assert!(dag.validate().is_ok());
    }

    // ── Additional test helpers for run() tests ─────────────────────────

    /// Arc wrapper so we can inspect sink contents after DAG run.
    struct ArcSink(std::sync::Arc<CollectingSink>);

    #[async_trait]
    impl Sink for ArcSink {
        async fn write_batch(&self, records: &[Value]) -> Result<usize, FaucetError> {
            self.0.write_batch(records).await
        }
    }

    /// Source that echoes its context as a single record.
    struct ContextEchoSource;

    #[async_trait]
    impl Source for ContextEchoSource {
        async fn fetch_with_context(
            &self,
            context: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            let record: serde_json::Map<String, Value> = context
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            Ok(vec![Value::Object(record)])
        }
    }

    /// Source that always fails.
    struct FailingSource;

    #[async_trait]
    impl Source for FailingSource {
        async fn fetch_with_context(
            &self,
            _context: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            Err(FaucetError::Source("boom".into()))
        }
    }

    fn arc_sink() -> (std::sync::Arc<CollectingSink>, Box<dyn Sink>) {
        let inner = std::sync::Arc::new(CollectingSink::new());
        (inner.clone(), Box::new(ArcSink(inner)))
    }

    // ── run() tests ────────────────────────────────────────────────────

    #[tokio::test]
    async fn run_single_root_writes_to_sink() {
        let (sink_inner, sink) = arc_sink();
        let dag = SourceDAG::new().add_root(
            "root",
            mock_source(vec![json!({"id": 1}), json!({"id": 2})]),
            sink,
        );

        let result = dag.run().await.unwrap();
        let root_result = &result.node_results["root"];
        assert_eq!(root_result.records_written, 2);
        assert_eq!(root_result.parent_records_processed, 0);
        assert!(root_result.errors.is_empty());

        let written = sink_inner.written.lock().unwrap();
        assert_eq!(written.len(), 2);
        assert_eq!(written[0]["id"], 1);
        assert_eq!(written[1]["id"], 2);
    }

    #[tokio::test]
    async fn run_parent_child_passes_context() {
        let (child_sink_inner, child_sink) = arc_sink();
        let dag = SourceDAG::new()
            .add_root(
                "orgs",
                mock_source(vec![
                    json!({"org_id": 10, "name": "acme"}),
                    json!({"org_id": 20, "name": "globex"}),
                ]),
                mock_sink(),
            )
            .add_child(
                "repos",
                "orgs",
                Box::new(ContextEchoSource),
                child_sink,
                HashMap::from([("org_id".into(), "$.org_id".into())]),
                false,
            );

        let result = dag.run().await.unwrap();
        let child_result = &result.node_results["repos"];
        assert_eq!(child_result.parent_records_processed, 2);
        assert_eq!(child_result.records_written, 2);
        assert!(child_result.errors.is_empty());

        let written = child_sink_inner.written.lock().unwrap();
        assert_eq!(written.len(), 2);
        // Each record should contain the org_id from the parent
        let org_ids: Vec<&Value> = written.iter().map(|r| &r["org_id"]).collect();
        assert!(org_ids.contains(&&json!(10)));
        assert!(org_ids.contains(&&json!(20)));
    }

    #[tokio::test]
    async fn run_inject_context_merges_parent_fields() {
        let (child_sink_inner, child_sink) = arc_sink();
        let dag = SourceDAG::new()
            .add_root(
                "orgs",
                mock_source(vec![json!({"org_id": 42})]),
                mock_sink(),
            )
            .add_child(
                "repos",
                "orgs",
                // Child source returns its own record
                mock_source(vec![json!({"repo": "faucet"})]),
                child_sink,
                HashMap::from([("org_id".into(), "$.org_id".into())]),
                true, // inject_context
            );

        let result = dag.run().await.unwrap();
        let child_result = &result.node_results["repos"];
        assert_eq!(child_result.records_written, 1);

        let written = child_sink_inner.written.lock().unwrap();
        assert_eq!(written.len(), 1);
        // The record should have both the original field and the injected context
        assert_eq!(written[0]["repo"], json!("faucet"));
        assert_eq!(written[0]["org_id"], json!(42));
    }

    #[tokio::test]
    async fn run_fan_out_two_children_of_same_parent() {
        let (users_sink_inner, users_sink) = arc_sink();
        let (repos_sink_inner, repos_sink) = arc_sink();

        let dag = SourceDAG::new()
            .add_root(
                "orgs",
                mock_source(vec![json!({"org_id": 1}), json!({"org_id": 2})]),
                mock_sink(),
            )
            .add_child(
                "users",
                "orgs",
                Box::new(ContextEchoSource),
                users_sink,
                HashMap::from([("org_id".into(), "$.org_id".into())]),
                false,
            )
            .add_child(
                "repos",
                "orgs",
                Box::new(ContextEchoSource),
                repos_sink,
                HashMap::from([("org_id".into(), "$.org_id".into())]),
                false,
            );

        let result = dag.run().await.unwrap();

        // Both children should have processed 2 parent records
        assert_eq!(result.node_results["users"].parent_records_processed, 2);
        assert_eq!(result.node_results["repos"].parent_records_processed, 2);
        assert_eq!(result.node_results["users"].records_written, 2);
        assert_eq!(result.node_results["repos"].records_written, 2);

        let users_written = users_sink_inner.written.lock().unwrap();
        let repos_written = repos_sink_inner.written.lock().unwrap();
        assert_eq!(users_written.len(), 2);
        assert_eq!(repos_written.len(), 2);
    }

    #[tokio::test]
    async fn run_linear_chain_three_levels() {
        let (c_sink_inner, c_sink) = arc_sink();

        let dag = SourceDAG::new()
            .add_root("A", mock_source(vec![json!({"a_id": 1})]), mock_sink())
            .add_child(
                "B",
                "A",
                // B echoes context (gets a_id), then adds b_id
                mock_source(vec![json!({"b_id": 100, "a_id": 1})]),
                mock_sink(),
                HashMap::from([("a_id".into(), "$.a_id".into())]),
                false,
            )
            .add_child(
                "C",
                "B",
                Box::new(ContextEchoSource),
                c_sink,
                HashMap::from([("b_id".into(), "$.b_id".into())]),
                false,
            );

        let result = dag.run().await.unwrap();

        // A -> 1 record, B -> 1 record (from A's 1 parent record), C -> 1 record
        assert_eq!(result.node_results["A"].records_written, 1);
        assert_eq!(result.node_results["B"].records_written, 1);
        assert_eq!(result.node_results["C"].records_written, 1);

        let c_written = c_sink_inner.written.lock().unwrap();
        assert_eq!(c_written.len(), 1);
        // C should have received b_id=100 from B's output
        assert_eq!(c_written[0]["b_id"], json!(100));
    }

    #[tokio::test]
    async fn run_child_error_is_non_fatal() {
        let dag = SourceDAG::new()
            .add_root(
                "orgs",
                mock_source(vec![json!({"org_id": 1}), json!({"org_id": 2})]),
                mock_sink(),
            )
            .add_child(
                "repos",
                "orgs",
                Box::new(FailingSource),
                mock_sink(),
                HashMap::from([("org_id".into(), "$.org_id".into())]),
                false,
            );

        let result = dag.run().await.unwrap();

        // Root should succeed
        assert_eq!(result.node_results["orgs"].records_written, 2);

        // Child should have 2 errors (one per parent record) but no panic/abort
        let child_result = &result.node_results["repos"];
        assert_eq!(child_result.parent_records_processed, 2);
        assert_eq!(child_result.records_written, 0);
        assert_eq!(child_result.errors.len(), 2);

        // Verify the errors contain the expected message
        for err in &child_result.errors {
            assert!(err.error.to_string().contains("boom"));
        }
    }

    // ── DynamicSource for integration test ─────────────────────────────

    /// Source that returns different records based on context values.
    struct DynamicSource {
        responses: HashMap<String, Vec<Value>>,
    }

    impl DynamicSource {
        fn new(responses: Vec<(String, Vec<Value>)>) -> Self {
            Self {
                responses: responses.into_iter().collect(),
            }
        }
    }

    #[async_trait]
    impl Source for DynamicSource {
        async fn fetch_with_context(
            &self,
            context: &HashMap<String, Value>,
        ) -> Result<Vec<Value>, FaucetError> {
            // Use the first context value as a lookup key
            for v in context.values() {
                let key = match v {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    _ => v.to_string(),
                };
                if let Some(records) = self.responses.get(&key) {
                    return Ok(records.clone());
                }
            }
            Ok(vec![])
        }
    }

    // ── Integration test ───────────────────────────────────────────────

    #[tokio::test]
    async fn integration_github_style_dag() {
        // Simulate: orgs -> users per org, with context injection
        let orgs_sink = std::sync::Arc::new(CollectingSink::new());
        let users_sink = std::sync::Arc::new(CollectingSink::new());

        let user_source = DynamicSource::new(vec![
            (
                "1".to_string(),
                vec![
                    json!({"user_id": 10, "name": "Alice"}),
                    json!({"user_id": 11, "name": "Bob"}),
                ],
            ),
            (
                "2".to_string(),
                vec![json!({"user_id": 20, "name": "Charlie"})],
            ),
        ]);

        let mut ctx_map = HashMap::new();
        ctx_map.insert("org_id".to_string(), "$.id".to_string());

        let dag = SourceDAG::new()
            .add_root(
                "orgs",
                Box::new(MockSource {
                    records: vec![
                        json!({"id": 1, "name": "Acme"}),
                        json!({"id": 2, "name": "Beta"}),
                    ],
                }),
                Box::new(ArcSink(orgs_sink.clone())),
            )
            .add_child(
                "users",
                "orgs",
                Box::new(user_source),
                Box::new(ArcSink(users_sink.clone())),
                ctx_map,
                true, // inject org_id into each user record
            )
            .concurrency(2);

        let result = dag.run().await.unwrap();

        // Orgs
        assert_eq!(result.node_results["orgs"].records_written, 2);
        assert_eq!(orgs_sink.records().len(), 2);

        // Users
        let users_result = &result.node_results["users"];
        assert_eq!(users_result.records_written, 3); // 2 from org 1 + 1 from org 2
        assert_eq!(users_result.parent_records_processed, 2);
        assert!(users_result.errors.is_empty());

        let user_records = users_sink.records();
        assert_eq!(user_records.len(), 3);

        // Every user record should have org_id injected
        for record in &user_records {
            assert!(record.get("org_id").is_some(), "missing org_id in {record}");
            assert!(record.get("name").is_some(), "missing name in {record}");
        }

        // Verify the right org_ids were injected
        let org1_users: Vec<_> = user_records
            .iter()
            .filter(|r| r["org_id"] == json!(1))
            .collect();
        let org2_users: Vec<_> = user_records
            .iter()
            .filter(|r| r["org_id"] == json!(2))
            .collect();
        assert_eq!(org1_users.len(), 2);
        assert_eq!(org2_users.len(), 1);
    }
}
