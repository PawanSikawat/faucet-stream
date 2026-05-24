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
    /// Every `${id.path}` placeholder that survived load-time interpolation.
    /// Populated by [`scan_deferred_refs`]; the executor uses this to know
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
    check_refs(&cfg.pipeline.source.config, &id_set, "pipeline.source")?;
    check_refs(&cfg.pipeline.sink.config, &id_set, "pipeline.sink")?;

    // 4) Build expanded nodes. Order: roots first (in declaration order),
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
        let merged = merge_pipeline(&cfg.pipeline, row);
        let role = match &row.parent {
            None => NodeRole::Root,
            Some(p) => NodeRole::Child {
                parent_id: p.clone(),
                parent_key: row.parent_key.clone(),
            },
        };
        let mut deferred = Vec::new();
        collect_deferred(&merged.source.config, &mut deferred);
        collect_deferred(&merged.sink.config, &mut deferred);
        out.push(ExpandedNode {
            id: ids[i].clone(),
            row_index: i,
            role,
            source: merged.source,
            sink: merged.sink,
            transforms: merged.transforms,
            state: merged.state,
            deferred_refs: deferred,
        });
    }
    Ok(out)
}

fn merge_pipeline(base: &PipelineSpec, row: &MatrixRow) -> PipelineSpec {
    let source = merge_connector(&base.source, row.source.as_ref());
    let sink = merge_connector(&base.sink, row.sink.as_ref());
    let transforms = row
        .transforms
        .clone()
        .unwrap_or_else(|| base.transforms.clone());
    let state = row.state.clone().or_else(|| base.state.clone());
    let dlq = row.dlq.clone().flatten().or_else(|| base.dlq.clone());
    PipelineSpec {
        source,
        sink,
        transforms,
        state,
        dlq,
    }
}

fn merge_connector(base: &ConnectorSpec, overlay: Option<&PartialConnector>) -> ConnectorSpec {
    let mut out = base.clone();
    if let Some(p) = overlay {
        if let Some(k) = &p.kind {
            out.kind = k.clone();
        }
        if let Some(c) = &p.config {
            merge_value(&mut out.config, c.clone());
        }
    }
    out
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
        for (token, prefix, _body) in iter_directives(s) {
            // Load-time prefixes already resolved by `interpolate::interpolate`.
            if matches!(prefix, "env" | "file" | "secret") {
                continue;
            }
            if !id_set.contains(prefix) {
                return Err(CliError::UnknownInterpolationId {
                    id: prefix.to_owned(),
                    token: format!("{token} (in {owner})"),
                });
            }
        }
        Ok(())
    })
}

fn collect_deferred(value: &Value, out: &mut Vec<DeferredRef>) {
    let _ = walk_strings(value, &mut |s| {
        for (token, prefix, body) in iter_directives(s) {
            if matches!(prefix, "env" | "file" | "secret") {
                continue;
            }
            out.push(DeferredRef {
                referenced_id: prefix.to_owned(),
                dotted_path: body.to_owned(),
                token: token.to_owned(),
            });
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

/// Iterate `${prefix:body}` and `${id.dotted.path}` tokens, returning
/// `(full_token_including_dollar_brace, prefix, body)`. The prefix is the
/// text before the first `:` (load-time) or `.` (deferred); body is the rest.
/// `${name}` alone (no `:` and no `.`) yields `(token, "name", "")`.
fn iter_directives(s: &str) -> impl Iterator<Item = (&str, &str, &str)> {
    let mut i = 0;
    let bytes = s.as_bytes();
    std::iter::from_fn(move || {
        while i + 1 < bytes.len() {
            // Skip `$${` escape — does not produce a directive.
            if bytes[i] == b'$'
                && bytes.get(i + 1) == Some(&b'$')
                && bytes.get(i + 2) == Some(&b'{')
            {
                i += 3;
                continue;
            }
            if bytes[i] == b'$' && bytes.get(i + 1) == Some(&b'{') {
                let start = i;
                let body_start = i + 2;
                let rel_end = s[body_start..].find('}')?;
                let end = body_start + rel_end;
                i = end + 1;
                let inner = &s[body_start..end];
                let (prefix, body) = if let Some(idx) = inner.find(':') {
                    (&inner[..idx], &inner[idx + 1..])
                } else if let Some(idx) = inner.find('.') {
                    (&inner[..idx], &inner[idx + 1..])
                } else {
                    (inner, "")
                };
                return Some((&s[start..i], prefix, body));
            }
            i += 1;
        }
        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_with_extension;

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
}
