//! Runtime matrix-row selection — the composable selection model spanning
//! four issues that resolve through **one** eligibility → narrowing → parents
//! → skip formula:
//!
//! ```text
//! 1. eligible  = status gate ({mandatory, active} ∪ --status)          # #371
//! 2. narrowed  = (eligible ∩ --tag) ∪ (--select / --only by id)        # #376 / #370
//! 3. parents   = apply include_parents policy to narrowed              # #377
//! 4. run set   = parents − (--skip)                                    # #370
//! ```
//!
//! - **#370 — identity.** `--select <id>` (exact) / `--only <glob>` force-include
//!   a row *by name*, bypassing the status gate; `--skip <id|glob>` removes last.
//! - **#371 — readiness (`status`).** Each row's source carries a
//!   [`SourceStatus`](crate::config::SourceStatus) ladder. The status gate
//!   decides *eligibility*; `--status <tier>` additively widens the eligible set.
//! - **#376 — classification (`tags`).** `--tag <t>` narrows *within* the
//!   eligible set. A tag can only shrink the eligible set, never resurrect a
//!   non-ready (`available`/`draft`/`archived`) row.
//! - **#377 — `include_parents`.** The single, explicit policy that decides
//!   whether a selected row's `parent:` / `depends_on:` ancestors are pulled in.
//!   Default `off` (strict): a required ancestor missing from the run set is a
//!   hard, fail-fast error.
//!
//! Selection runs on the **expanded node list** (after `expand()`), so it never
//! alters `{name}::{row_id}` state-key derivation — bookmarks stay identical
//! across a full run and any selected subset.

use crate::config::{IncludeParents, SelectionSpec, SourceStatus};
use crate::error::{CliError, CliResult};
use crate::expand::{ExpandedNode, NodeRole};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

/// A fully-resolved selection request, built from CLI flags + config.
#[derive(Debug, Clone, Default)]
pub struct RunSelection {
    /// Exact row ids to force-include (bypass status/tags).
    pub select: Vec<String>,
    /// Glob patterns to force-include by id (bypass status/tags).
    pub only: Vec<String>,
    /// Row ids / globs to remove from the run set (applied last).
    pub skip: Vec<String>,
    /// Status tiers to add to the default `{mandatory, active}` eligible set.
    pub status: Vec<SourceStatus>,
    /// Tags to narrow the eligible set by (union within the list).
    pub tags: Vec<String>,
    /// Parent/dependency inclusion policy.
    pub include_parents: IncludeParents,
}

impl RunSelection {
    /// Resolve raw CLI strings + the config's `selection:` block into a typed
    /// [`RunSelection`]. Parses `--status` tiers and `--include-parents`,
    /// surfacing typed errors on unknown values. Precedence for the policy:
    /// `--include-parents` flag/env > `selection.include_parents` in config >
    /// built-in default (`off`).
    #[allow(clippy::too_many_arguments)]
    pub fn resolve(
        select: &[String],
        only: &[String],
        skip: &[String],
        status: &[String],
        tags: &[String],
        include_parents_flag: Option<&str>,
        cfg_selection: Option<&SelectionSpec>,
    ) -> CliResult<Self> {
        let status = status
            .iter()
            .map(|s| {
                SourceStatus::parse(s).ok_or_else(|| CliError::UnknownStatus {
                    value: s.clone(),
                    available: SourceStatus::ALL
                        .iter()
                        .map(|v| v.as_str().to_owned())
                        .collect(),
                })
            })
            .collect::<CliResult<Vec<_>>>()?;

        let include_parents = match include_parents_flag {
            Some(s) => IncludeParents::parse(s).ok_or_else(|| CliError::UnknownIncludeParents {
                value: s.to_owned(),
            })?,
            None => cfg_selection.map(|s| s.include_parents).unwrap_or_default(),
        };

        Ok(Self {
            select: dedup(select),
            only: dedup(only),
            skip: dedup(skip),
            status,
            tags: dedup(tags),
            include_parents,
        })
    }

    /// Build from the shared CLI [`SelectionArgs`](crate::cli::SelectionArgs)
    /// plus the config's `selection:` block.
    pub fn from_args(
        args: &crate::cli::SelectionArgs,
        cfg_selection: Option<&SelectionSpec>,
    ) -> CliResult<Self> {
        Self::resolve(
            &args.select,
            &args.only,
            &args.skip,
            &args.status,
            &args.tags,
            args.include_parents.as_deref(),
            cfg_selection,
        )
    }

    /// Whether any selector actively narrows/widens the run set (so callers
    /// like `validate` know to print a selection report). `--include-parents`
    /// alone does not count — it only governs ancestor inclusion.
    pub fn narrows(&self) -> bool {
        self.has_matrix_only_selector() || !self.status.is_empty()
    }

    /// Any row-narrowing selector present (matrix-only flags). `--status` and
    /// `--include-parents` are excluded because they are meaningful even on a
    /// single anonymous row.
    fn has_matrix_only_selector(&self) -> bool {
        !self.select.is_empty()
            || !self.only.is_empty()
            || !self.skip.is_empty()
            || !self.tags.is_empty()
    }
}

/// Apply `sel` to `nodes` (expanded, in BFS order) and return the running
/// subset, order-preserved. Errors on unknown tokens, an empty run set, or a
/// dependency violation under the active `include_parents` policy.
///
/// `has_matrix` is `false` for the single anonymous invocation (no `matrix:`);
/// matrix-only selectors (`--select`/`--only`/`--skip`/`--tag`) are then a hard
/// error. The status gate still applies to the lone row.
pub fn select_nodes(
    nodes: Vec<ExpandedNode>,
    sel: &RunSelection,
    has_matrix: bool,
) -> CliResult<Vec<ExpandedNode>> {
    if !has_matrix && sel.has_matrix_only_selector() {
        let mut flags = Vec::new();
        if !sel.select.is_empty() {
            flags.push("--select");
        }
        if !sel.only.is_empty() {
            flags.push("--only");
        }
        if !sel.skip.is_empty() {
            flags.push("--skip");
        }
        if !sel.tags.is_empty() {
            flags.push("--tag");
        }
        return Err(CliError::SelectorsWithoutMatrix {
            flags: flags.join(", "),
        });
    }

    // Typo protection: every identity/skip token must match ≥1 row id, and
    // every requested tag must be present on some row. Checked against the
    // full node set (before any gating), so a typo is caught regardless of
    // status.
    for token in &sel.select {
        if !nodes.iter().any(|n| &n.id == token) {
            return Err(CliError::NoMatchForSelector {
                flag: "--select",
                token: token.clone(),
                available: all_ids(&nodes),
            });
        }
    }
    for token in sel.only.iter().chain(sel.skip.iter()) {
        let flag = if sel.only.contains(token) {
            "--only"
        } else {
            "--skip"
        };
        if !nodes.iter().any(|n| token_matches(token, &n.id)) {
            return Err(CliError::NoMatchForSelector {
                flag,
                token: token.clone(),
                available: all_ids(&nodes),
            });
        }
    }
    if !sel.tags.is_empty() {
        let present: BTreeSet<&str> = nodes
            .iter()
            .flat_map(|n| n.tags.iter().map(String::as_str))
            .collect();
        for tag in &sel.tags {
            if !present.contains(tag.as_str()) {
                return Err(CliError::UnknownTag {
                    tag: tag.clone(),
                    available: present.iter().map(|s| (*s).to_owned()).collect(),
                });
            }
        }
    }

    // Effective status set = {mandatory, active} ∪ --status.
    let mut active_status: HashSet<SourceStatus> =
        HashSet::from([SourceStatus::Mandatory, SourceStatus::Active]);
    active_status.extend(sel.status.iter().copied());

    let has_identity = !sel.select.is_empty() || !sel.only.is_empty();
    let has_tag = !sel.tags.is_empty();

    let is_eligible = |n: &ExpandedNode| active_status.contains(&n.status);
    let is_identity = |n: &ExpandedNode| {
        sel.select.iter().any(|id| id == &n.id) || sel.only.iter().any(|g| token_matches(g, &n.id))
    };
    let matches_tag = |n: &ExpandedNode| sel.tags.iter().any(|t| n.tags.iter().any(|nt| nt == t));

    // Stage 1 + 2: eligibility → narrowing.
    let mut run: HashSet<String> = HashSet::new();
    for n in &nodes {
        let included = if !has_identity && !has_tag {
            is_eligible(n)
        } else {
            let by_tag = has_tag && is_eligible(n) && matches_tag(n);
            let by_identity = has_identity && is_identity(n);
            by_tag || by_identity
        };
        if included {
            run.insert(n.id.clone());
        }
    }

    // Stage 3: parent / dependency closure under the include_parents policy.
    let node_by_id: HashMap<&str, &ExpandedNode> =
        nodes.iter().map(|n| (n.id.as_str(), n)).collect();
    apply_parent_policy(
        &nodes,
        &node_by_id,
        &active_status,
        sel.include_parents,
        &mut run,
    )?;

    // Stage 4: skip (applied last). A `mandatory` row is removable only by an
    // exact `--skip <id>`, never by a glob.
    for n in &nodes {
        if !run.contains(&n.id) {
            continue;
        }
        let mandatory = n.status == SourceStatus::Mandatory;
        let removed = sel
            .skip
            .iter()
            .any(|tok| skip_matches(tok, &n.id, mandatory));
        if removed {
            run.remove(&n.id);
        }
    }

    // Post-skip integrity: skipping a row that a surviving row structurally
    // depends on would orphan the dependent (a child can't fan out without its
    // parent). Fail fast rather than run a broken graph.
    let mut orphans: Vec<String> = Vec::new();
    for n in &nodes {
        if !run.contains(&n.id) {
            continue;
        }
        for (anc, kind) in required_ancestors(n) {
            if !run.contains(&anc) {
                orphans.push(format!("{} → {anc} ({kind})", n.id));
            }
        }
    }
    if !orphans.is_empty() {
        orphans.sort();
        orphans.dedup();
        return Err(CliError::RunSetMissingAncestors {
            pairs: orphans,
            policy: sel.include_parents.as_str(),
        });
    }

    if run.is_empty() {
        let rows = nodes
            .iter()
            .map(|n| format!("{} [{}]", n.id, n.status.as_str()))
            .collect();
        return Err(CliError::EmptyRunSet { rows });
    }

    Ok(nodes.into_iter().filter(|n| run.contains(&n.id)).collect())
}

/// Walk the `parent:` / `depends_on:` ancestor closure of the current run set,
/// adding or rejecting ancestors per the policy. Collects **every** offending
/// pair (transitively) before erroring.
fn apply_parent_policy(
    _nodes: &[ExpandedNode],
    node_by_id: &HashMap<&str, &ExpandedNode>,
    active_status: &HashSet<SourceStatus>,
    policy: IncludeParents,
    run: &mut HashSet<String>,
) -> CliResult<()> {
    let mut violations: Vec<String> = Vec::new();
    let mut queue: VecDeque<String> = run.iter().cloned().collect();
    while let Some(id) = queue.pop_front() {
        // `id` is always a real node (run set only ever holds known ids).
        let node = match node_by_id.get(id.as_str()) {
            Some(n) => *n,
            None => continue,
        };
        for (anc, kind) in required_ancestors(node) {
            if run.contains(&anc) {
                continue;
            }
            // Ancestor id validity was proven at expand time.
            let anc_status = node_by_id.get(anc.as_str()).map(|n| n.status);
            let eligible = anc_status
                .map(|s| active_status.contains(&s))
                .unwrap_or(false);
            match policy {
                IncludeParents::Off => {
                    violations.push(format!("{id} → {anc} ({kind})"));
                }
                IncludeParents::Eligible => {
                    if eligible {
                        if run.insert(anc.clone()) {
                            tracing::info!(
                                dependent = %id, ancestor = %anc, edge = kind,
                                "include_parents=eligible: auto-included required ancestor"
                            );
                            queue.push_back(anc);
                        }
                    } else {
                        violations.push(format!("{id} → {anc} ({kind}, parked)"));
                    }
                }
                IncludeParents::All => {
                    if run.insert(anc.clone()) {
                        if eligible {
                            tracing::info!(
                                dependent = %id, ancestor = %anc, edge = kind,
                                "include_parents=all: auto-included required ancestor"
                            );
                        } else {
                            tracing::warn!(
                                dependent = %id, ancestor = %anc, edge = kind,
                                "include_parents=all: pulling a parked ancestor into the run set"
                            );
                        }
                        queue.push_back(anc);
                    }
                }
            }
        }
    }
    if !violations.is_empty() {
        violations.sort();
        violations.dedup();
        return Err(CliError::RunSetMissingAncestors {
            pairs: violations,
            policy: policy.as_str(),
        });
    }
    Ok(())
}

/// The `parent:` + `depends_on:` edges of `node` — the "required ancestors" a
/// run-set row cannot execute correctly without.
fn required_ancestors(node: &ExpandedNode) -> Vec<(String, &'static str)> {
    let mut out = Vec::new();
    if let NodeRole::Child { parent_id, .. } = &node.role {
        out.push((parent_id.clone(), "parent"));
    }
    for d in &node.depends_on {
        out.push((d.clone(), "depends_on"));
    }
    out
}

fn all_ids(nodes: &[ExpandedNode]) -> Vec<String> {
    nodes.iter().map(|n| n.id.clone()).collect()
}

/// Dedup a token list, preserving first-seen order.
fn dedup(items: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for it in items {
        if seen.insert(it.clone()) {
            out.push(it.clone());
        }
    }
    out
}

/// Whether a `--skip` token removes `id`. A glob token never removes a
/// `mandatory` row; an exact-id token removes any row (including mandatory).
fn skip_matches(token: &str, id: &str, mandatory: bool) -> bool {
    if has_glob(token) {
        !mandatory && glob_match(token, id)
    } else {
        token == id
    }
}

/// Whether a token (exact id or glob) matches `id`.
fn token_matches(token: &str, id: &str) -> bool {
    if has_glob(token) {
        glob_match(token, id)
    } else {
        token == id
    }
}

fn has_glob(s: &str) -> bool {
    s.contains('*') || s.contains('?')
}

/// Minimal `*` (any run, incl. empty) / `?` (exactly one char) glob matcher.
/// Sufficient for row-id selection; no character classes or escaping.
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    // Iterative backtracking match.
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::parse_with_extension;
    use crate::expand::expand;

    /// Build expanded nodes from YAML for selection tests.
    fn nodes(yaml: &str) -> Vec<ExpandedNode> {
        expand(&parse_with_extension(yaml, "yaml").unwrap()).unwrap()
    }

    fn ids(nodes: &[ExpandedNode]) -> Vec<String> {
        let mut v: Vec<String> = nodes.iter().map(|n| n.id.clone()).collect();
        v.sort();
        v
    }

    fn sel() -> RunSelection {
        RunSelection::default()
    }

    /// A HiBob-style multi-endpoint matrix used by most tests.
    const HIBOB: &str = r#"
version: 1
pipeline:
  sources:
    hibob: { type: rest, config: { base_url: https://api.hibob.com } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: people
    source: { ref: hibob, status: active, config: { path: /people } }
    sink: { ref: wh }
    tags: [core, daily]
  - id: payroll
    source: { ref: hibob, status: mandatory, config: { path: /payroll } }
    sink: { ref: wh }
    tags: [finance]
  - id: audit
    source: { ref: hibob, status: available, config: { path: /audit } }
    sink: { ref: wh }
    tags: [finance]
  - id: beta
    source: { ref: hibob, status: draft, config: { path: /beta } }
    sink: { ref: wh }
"#;

    #[test]
    fn glob_matches_star_and_question() {
        assert!(glob_match("timeoff_*", "timeoff_requests"));
        assert!(glob_match("timeoff_*", "timeoff_"));
        assert!(!glob_match("timeoff_*", "people"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("p*e", "people"));
        assert!(!glob_match("p*e", "payroll"));
    }

    #[test]
    fn bare_run_includes_mandatory_and_active_only() {
        let out = select_nodes(nodes(HIBOB), &sel(), true).unwrap();
        assert_eq!(ids(&out), vec!["payroll", "people"]);
    }

    #[test]
    fn status_widens_eligible_set_additively() {
        let s = RunSelection {
            status: vec![SourceStatus::Available],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["audit", "payroll", "people"]);
    }

    #[test]
    fn select_by_id_bypasses_status_gate() {
        // `beta` is draft (parked) but explicitly selected → runs anyway.
        let s = RunSelection {
            select: vec!["beta".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["beta"]);
    }

    #[test]
    fn only_glob_selects_subset() {
        let s = RunSelection {
            only: vec!["p*".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        // p* matches people + payroll (both identity-selected, status bypassed).
        assert_eq!(ids(&out), vec!["payroll", "people"]);
    }

    #[test]
    fn tag_narrows_within_eligible_only() {
        // `finance` tags payroll (mandatory, eligible) + audit (available, NOT
        // eligible). Bare --tag finance keeps only the eligible one.
        let s = RunSelection {
            tags: vec!["finance".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["payroll"]);
    }

    #[test]
    fn tag_plus_status_resurrects_parked_row() {
        let s = RunSelection {
            tags: vec!["finance".into()],
            status: vec![SourceStatus::Available],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["audit", "payroll"]);
    }

    #[test]
    fn skip_removes_after_selection() {
        let s = RunSelection {
            status: vec![SourceStatus::Available],
            skip: vec!["audit".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["payroll", "people"]);
    }

    #[test]
    fn mandatory_survives_glob_skip_but_not_exact_skip() {
        // A glob skip cannot drop the mandatory `payroll` row…
        let s = RunSelection {
            skip: vec!["p*".into()],
            ..sel()
        };
        let out = select_nodes(nodes(HIBOB), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["payroll"]);
        // …but an exact-id skip can.
        let s = RunSelection {
            select: vec!["payroll".into()],
            skip: vec!["payroll".into()],
            ..sel()
        };
        let err = select_nodes(nodes(HIBOB), &s, true).unwrap_err();
        assert!(matches!(err, CliError::EmptyRunSet { .. }), "got {err:?}");
    }

    #[test]
    fn unknown_select_token_errors_with_available() {
        let s = RunSelection {
            select: vec!["peeple".into()],
            ..sel()
        };
        match select_nodes(nodes(HIBOB), &s, true).unwrap_err() {
            CliError::NoMatchForSelector {
                flag,
                token,
                available,
            } => {
                assert_eq!(flag, "--select");
                assert_eq!(token, "peeple");
                assert!(available.contains(&"people".to_string()));
            }
            other => panic!("expected NoMatchForSelector, got {other:?}"),
        }
    }

    #[test]
    fn unknown_tag_errors() {
        let s = RunSelection {
            tags: vec!["nope".into()],
            ..sel()
        };
        assert!(matches!(
            select_nodes(nodes(HIBOB), &s, true).unwrap_err(),
            CliError::UnknownTag { .. }
        ));
    }

    #[test]
    fn empty_run_set_errors_when_all_parked() {
        let yaml = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: a
    source: { ref: api, status: available }
    sink: { ref: wh }
  - id: b
    source: { ref: api, status: draft }
    sink: { ref: wh }
"#;
        assert!(matches!(
            select_nodes(nodes(yaml), &sel(), true).unwrap_err(),
            CliError::EmptyRunSet { .. }
        ));
    }

    const DEPS: &str = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: dims
    source: { ref: api, status: active }
    sink: { ref: wh }
    tags: [core]
  - id: facts
    source: { ref: api, status: active }
    sink: { ref: wh }
    tags: [finance]
    depends_on: [dims]
"#;

    #[test]
    fn include_parents_off_errors_on_missing_ancestor() {
        // Selecting only `facts` (via tag) drops its `depends_on: dims`.
        let s = RunSelection {
            tags: vec!["finance".into()],
            include_parents: IncludeParents::Off,
            ..sel()
        };
        match select_nodes(nodes(DEPS), &s, true).unwrap_err() {
            CliError::RunSetMissingAncestors { pairs, policy } => {
                assert_eq!(policy, "off");
                assert!(
                    pairs
                        .iter()
                        .any(|p| p.contains("facts") && p.contains("dims"))
                );
            }
            other => panic!("expected RunSetMissingAncestors, got {other:?}"),
        }
    }

    #[test]
    fn include_parents_eligible_pulls_in_active_ancestor() {
        let s = RunSelection {
            tags: vec!["finance".into()],
            include_parents: IncludeParents::Eligible,
            ..sel()
        };
        let out = select_nodes(nodes(DEPS), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["dims", "facts"]);
    }

    #[test]
    fn include_parents_eligible_errors_on_parked_ancestor() {
        let yaml = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: dims
    source: { ref: api, status: available }
    sink: { ref: wh }
  - id: facts
    source: { ref: api, status: active }
    sink: { ref: wh }
    depends_on: [dims]
"#;
        let s = RunSelection {
            select: vec!["facts".into()],
            include_parents: IncludeParents::Eligible,
            ..sel()
        };
        assert!(matches!(
            select_nodes(nodes(yaml), &s, true).unwrap_err(),
            CliError::RunSetMissingAncestors { .. }
        ));
    }

    #[test]
    fn include_parents_all_pulls_in_parked_ancestor() {
        let yaml = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: dims
    source: { ref: api, status: draft }
    sink: { ref: wh }
  - id: facts
    source: { ref: api, status: active }
    sink: { ref: wh }
    depends_on: [dims]
"#;
        let s = RunSelection {
            select: vec!["facts".into()],
            include_parents: IncludeParents::All,
            ..sel()
        };
        let out = select_nodes(nodes(yaml), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["dims", "facts"]);
    }

    #[test]
    fn select_ancestor_by_id_satisfies_dependency() {
        let s = RunSelection {
            select: vec!["facts".into(), "dims".into()],
            include_parents: IncludeParents::Off,
            ..sel()
        };
        let out = select_nodes(nodes(DEPS), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["dims", "facts"]);
    }

    #[test]
    fn parent_edge_closure_respected() {
        // `posts` is a per-record child of `users`; selecting only `posts` must
        // pull in `users` under `eligible`.
        let yaml = r#"
version: 1
pipeline:
  sources:
    api: { type: rest, config: { base_url: https://x } }
  sinks:
    wh: { type: jsonl, config: { path: ./o } }
matrix:
  - id: users
    source: { ref: api, status: active }
    sink: { ref: wh }
  - id: posts
    parent: users
    source: { ref: api, status: active, config: { path: "/u/${users.id}/posts" } }
    sink: { ref: wh }
"#;
        let s = RunSelection {
            select: vec!["posts".into()],
            include_parents: IncludeParents::Eligible,
            ..sel()
        };
        let out = select_nodes(nodes(yaml), &s, true).unwrap();
        assert_eq!(ids(&out), vec!["posts", "users"]);
    }

    #[test]
    fn matrix_only_selectors_rejected_without_matrix() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
"#;
        let s = RunSelection {
            select: vec!["row-0".into()],
            ..sel()
        };
        assert!(matches!(
            select_nodes(nodes(yaml), &s, false).unwrap_err(),
            CliError::SelectorsWithoutMatrix { .. }
        ));
    }

    #[test]
    fn no_selectors_keeps_plain_config_unchanged() {
        // A matrix with no status/tags anywhere must run every row (no
        // behaviour change for pre-selection configs).
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { base_url: https://x } }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - { id: a }
  - { id: b }
  - { id: c }
"#;
        let out = select_nodes(nodes(yaml), &sel(), true).unwrap();
        assert_eq!(ids(&out), vec!["a", "b", "c"]);
    }
}
