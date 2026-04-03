//! DAG-based orchestration for parent-child source pipelines.
//!
//! [`SourceDAG`] lets you wire multiple [`Source`]–[`Sink`] pairs into a
//! directed acyclic graph where child sources receive context extracted from
//! their parent's records.

use crate::error::FaucetError;
use crate::traits::{Sink, Source};
use serde_json::Value;
use std::collections::{HashMap, HashSet, VecDeque};

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
    pub fn concurrency(mut self, n: usize) -> Self {
        self.concurrency = n;
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
}
