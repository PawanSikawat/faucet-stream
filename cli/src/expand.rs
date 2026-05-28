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
pub const RESERVED_IDS: &[&str] = &["env", "file", "secret", "matrix", "pipeline"];

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
    /// Every `${id.path}` placeholder that survived load-time interpolation.
    /// Populated by `collect_deferred`; the executor uses this to know
    /// which parent record to feed which row.
    pub deferred_refs: Vec<DeferredRef>,
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
        }
        Ok(out)
    }
}

/// Expand `cfg` into a topologically valid list of nodes. Roots come first,
/// then children in BFS order.
pub fn expand(cfg: &PipelineConfig) -> CliResult<Vec<ExpandedNode>> {
    // Implicit single-row case: empty matrix → run pipeline once with no merge.
    let synthetic_row;
    let rows: &[MatrixRow] = if cfg.matrix.is_empty() {
        synthetic_row = [MatrixRow {
            id: None,
            parent: None,
            parent_key: "id".into(),
            source: None,
            sink: None,
            transforms: None,
            inherit_transforms: true,
            state: None,
            dlq: None,
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

        out.push(ExpandedNode {
            id: ids[i].clone(),
            row_index: i,
            role,
            source: merged_source,
            sink: merged_sink,
            transforms,
            state,
            dlq,
            deferred_refs: deferred,
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

/// Verify that every `${X.path}` token in `value` has `X` listed in `id_set`.
/// Load-time prefixes (`env`, `file`, `secret`) were already handled and are
/// ignored here.
fn check_refs(value: &Value, id_set: &HashSet<&str>, owner: &str) -> CliResult<()> {
    walk_strings(value, &mut |s| {
        for (token, dir) in iter_directives(s) {
            // Load-time / template directives (`${env:..}`, `${vars.X}`, …) are
            // resolved before expansion; only deferred `${id.path}` references
            // are validated here, against the known row ids.
            if let Directive::Deferred { id, .. } = dir
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

fn collect_deferred(value: &Value, out: &mut Vec<DeferredRef>) {
    let _ = walk_strings(value, &mut |s| {
        for (token, dir) in iter_directives(s) {
            if let Directive::Deferred { id, path } = dir {
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
}
