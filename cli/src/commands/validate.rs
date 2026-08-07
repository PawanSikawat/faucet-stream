//! `faucet validate` — parse + expand a pipeline config without running.
//!
//! Surfaces every per-row error with the row id, so a config with multiple
//! issues reports them together instead of failing at the first one.

use crate::cli::ValidateArgs;
use crate::config::{PipelineConfig, SourceStatus};
use crate::error::{CliError, CliResult};
use crate::expand::{NodeRole, expand};
use crate::registry::{sink_schema, source_schema};
use crate::select::RunSelection;
use crate::state::available_state_kinds;
use crate::transforms::available_transforms;
use std::collections::HashSet;

/// Execute the `validate` subcommand.
pub async fn run(args: ValidateArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };

    if args.show_composed {
        let composed = crate::compose::compose(&path, args.profile.as_deref())?;
        // Normalize to exactly one trailing newline: the YAML serializer appends
        // one but `serde_json::to_string_pretty` (JSON-format configs) does not,
        // and the fast path echoes the file verbatim. A single `\n` keeps
        // `faucet validate … --show-composed > out.{yaml,json}` well-formed.
        println!("{}", composed.trim_end_matches('\n'));
        return Ok(());
    }

    // Typed run params (#444). With no `--param`, required params bind to
    // type-shaped placeholders so a parameterized config still validates in CI;
    // supplying any `--param` opts into strict binding, which is how you check a
    // concrete invocation.
    let inputs = crate::config::RunInputs {
        params: crate::params::collect_cli_params(&args.param)?,
        env: crate::params::collect_env_overrides(&args.param_env)?
            .into_iter()
            .collect(),
        mode: if args.param.is_empty() {
            crate::params::BindMode::Placeholder
        } else {
            crate::params::BindMode::Strict
        },
    };

    let cfg = if args.no_secrets {
        // Grammar / structure only — never touch the network.
        PipelineConfig::from_path_tolerating_secrets_with(&path, args.profile.as_deref(), &inputs)?
    } else {
        // Real preflight: report each secret reference, then resolve.
        let refs = crate::secrets::scan_path_refs(&path, args.profile.as_deref())?;
        let cfg =
            PipelineConfig::from_path_async_with(&path, args.profile.as_deref(), &inputs).await?;
        for (scheme, reference) in &refs {
            println!("secret: {scheme}:{reference} → resolved");
        }
        cfg
    };
    if !cfg.params.is_empty() {
        let required: Vec<&str> = cfg
            .params
            .iter()
            .filter(|(_, p)| p.required)
            .map(|(n, _)| n.as_str())
            .collect();
        println!(
            "params: {} declared ({}){}",
            cfg.params.len(),
            if required.is_empty() {
                String::from("all optional")
            } else {
                format!("required: {}", required.join(", "))
            },
            if args.param.is_empty() && !required.is_empty() {
                " — validated against placeholders; pass --param NAME=VALUE to bind for real"
            } else {
                ""
            }
        );
    }
    // Topology mode (#71/#72): build + validate the node graph instead of the
    // matrix. `build_topology` runs the core structural validator (arity,
    // fan-out, join edges, cycle, reachability).
    if crate::topology::is_topology(&cfg) {
        let auth = crate::auth_catalog::build_auth_catalog(cfg.auth.as_ref())?;
        let topo = crate::topology::build_topology(&cfg, &auth).await?;
        println!(
            "topology '{}': {} node(s), {} edge(s) — valid",
            cfg.name.as_deref().unwrap_or("unnamed"),
            topo.nodes().len(),
            topo.edges().len()
        );
        for n in topo.nodes() {
            println!("  - {} ({})", n.id, n.kind.kind_str());
        }
        return Ok(());
    }

    let nodes = expand(&cfg)?;

    // Validate the replication block (snapshot source / CDC source / state) so
    // `faucet validate` catches misconfiguration without running.
    if let Some(spec) = &cfg.replication {
        crate::replication::compiled::CompiledReplication::compile(spec, &cfg)?;
        println!("replication: mode={:?} — valid", spec.mode);
    }

    // Validate the backfill defaults block (window / concurrency / timezone)
    // and the window-scoping requirement: a `backfill:` block on a pipeline
    // whose sources reference no `${backfill.*}` / `${now.*}` token would
    // replay identical data into every window (#282). Offline-safe.
    if let Some(spec) = &cfg.backfill {
        let source_configs: Vec<String> = nodes
            .iter()
            .filter(|n| matches!(n.role, crate::expand::NodeRole::Root))
            .map(|n| n.source.config.to_string())
            .collect();
        spec.validate(&source_configs)?;
        println!("backfill: defaults valid");
    }

    // Validate the schedule block (cron / timezone / bounds) so `faucet validate`
    // catches schedule misconfiguration in CI without running. Offline-safe.
    #[cfg(feature = "schedule")]
    if let Some(spec) = &cfg.schedule {
        crate::schedule::compiled::CompiledSchedule::compile(spec)?;
        println!(
            "schedule: cron '{}' tz '{}' — valid",
            spec.cron, spec.timezone
        );
    }

    // Validate the notifications block (unique names, non-empty channel fields)
    // so `faucet validate` catches misconfiguration without running. Offline.
    #[cfg(feature = "notify")]
    if !cfg.notifications.is_empty() {
        crate::notify::validate_all(&cfg.notifications)?;
        println!("notifications: {} rule(s) — valid", cfg.notifications.len());
    }

    // Lineage transport reachability — best-effort. A failure here is only a
    // warning: lineage emission never blocks a pipeline run.
    #[cfg(feature = "lineage")]
    if let Some(lc) = cfg.lineage.as_ref() {
        match crate::lineage_glue::check_transport(lc).await {
            Ok(msg) => println!("lineage: {msg}"),
            Err(msg) => println!("lineage: WARNING — {msg} (lineage never blocks a run)"),
        }
    }

    for node in &nodes {
        // Verifying the schema lookup also catches unknown connector kinds.
        source_schema(&node.source.kind)?;
        sink_schema(&node.sink.kind)?;
        for t in &node.transforms {
            if !available_transforms().contains(&t.kind.as_str()) {
                return Err(CliError::UnknownTransform {
                    name: format!("{} (row '{}')", t.kind, node.id),
                    available: available_transforms().join(", "),
                });
            }
        }
        if let Some(state) = &node.state
            && !available_state_kinds().contains(&state.kind.as_str())
        {
            return Err(CliError::UnknownStateStore {
                name: format!("{} (row '{}')", state.kind, node.id),
                available: available_state_kinds().join(", "),
            });
        }
    }

    let roots = nodes
        .iter()
        .filter(|n| matches!(n.role, NodeRole::Root))
        .count();
    let children = nodes.len() - roots;
    println!(
        "ok: '{}' rows={} (roots={}, children={}) execution={}",
        cfg.name.as_deref().unwrap_or("(unnamed)"),
        nodes.len(),
        roots,
        children,
        cfg.execution
            .as_ref()
            .map(|e| format!(
                "max_concurrent={:?} on_error={:?}",
                e.max_concurrent.unwrap_or(0),
                e.on_error
            ))
            .unwrap_or_else(|| "(defaults)".to_owned()),
    );
    for node in &nodes {
        println!("{}", row_line(node));
    }

    // Runtime row-selection report (#370/#371/#376/#377). Only printed when the
    // config actually uses the readiness ladder / tags, or a selector was
    // passed — so a plain config's `validate` output is unchanged. The
    // selection is computed the same way `faucet run` computes it, so the
    // run/skip decision here matches what a run would do; a selection error
    // (empty run set, missing ancestor, unknown token) is surfaced after the
    // report so `validate` catches it in CI without a run.
    let selection = RunSelection::from_args(&args.selection, cfg.selection.as_ref())?;
    let uses_selection_model = nodes
        .iter()
        .any(|n| n.status != SourceStatus::Active || !n.tags.is_empty());
    if selection.narrows() || uses_selection_model {
        let has_matrix = !cfg.matrix.is_empty();
        let selected = crate::select::select_nodes(nodes.clone(), &selection, has_matrix);
        let run_ids: HashSet<String> = match &selected {
            Ok(sel) => sel.iter().map(|n| n.id.clone()).collect(),
            Err(_) => HashSet::new(),
        };
        println!(
            "run selection (include_parents={}):",
            selection.include_parents.as_str()
        );
        for node in &nodes {
            let decision = if run_ids.contains(&node.id) {
                "RUN"
            } else {
                "skip"
            };
            let tags = if node.tags.is_empty() {
                String::new()
            } else {
                format!(" tags=[{}]", node.tags.join(", "))
            };
            println!(
                "  - {} status={}{} -> {}",
                node.id,
                node.status.as_str(),
                tags,
                decision
            );
        }
        // Propagate any selection error (empty run set / missing ancestor /
        // unknown token) now that the report has been printed.
        selected?;
    }
    Ok(())
}

/// Render one per-row report line for `faucet validate` output.
fn row_line(node: &crate::expand::ExpandedNode) -> String {
    let role = match &node.role {
        NodeRole::Root => "root".to_owned(),
        NodeRole::Child {
            parent_id,
            parent_key,
        } => {
            format!("child of '{parent_id}' (parent_key={parent_key})")
        }
    };
    let deps = if node.depends_on.is_empty() {
        String::new()
    } else {
        format!(" depends_on=[{}]", node.depends_on.join(", "))
    };
    format!(
        "  - {} [{}] source={} sink={}{} delivery={}",
        node.id, role, node.source.kind, node.sink.kind, deps, node.delivery_guarantee
    )
}

#[cfg(test)]
mod tests {
    use super::row_line;
    use crate::expand::expand;

    #[test]
    fn row_line_renders_role_and_depends_on() {
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o } }
matrix:
  - id: dims
  - id: posts
    parent: dims
    parent_key: id
  - id: facts
    depends_on: [dims]
"#,
            "yaml",
        )
        .unwrap();
        let nodes = expand(&cfg).unwrap();
        let line_for = |id: &str| row_line(nodes.iter().find(|n| n.id == id).unwrap());
        assert_eq!(
            line_for("dims"),
            "  - dims [root] source=rest sink=jsonl delivery=at-least-once"
        );
        assert_eq!(
            line_for("posts"),
            "  - posts [child of 'dims' (parent_key=id)] source=rest sink=jsonl \
             delivery=at-least-once"
        );
        assert_eq!(
            line_for("facts"),
            "  - facts [root] source=rest sink=jsonl depends_on=[dims] delivery=at-least-once"
        );
    }

    #[test]
    fn row_line_reports_derived_effectively_once_guarantees() {
        // Keyed upsert is reported even when the user did not request
        // `delivery: exactly_once` (truthful derived guarantee, #292)…
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
pipeline:
  source: { type: rest, config: {} }
  sink:
    type: postgres
    config:
      connection_url: "postgres://localhost/db"
      table_name: t
      column_mapping: auto_map
      write_mode: upsert
      key: [id]
"#,
            "yaml",
        )
        .unwrap();
        let nodes = expand(&cfg).unwrap();
        assert!(
            row_line(&nodes[0]).ends_with("delivery=effectively-once (keyed upsert)"),
            "got: {}",
            row_line(&nodes[0])
        );

        // …and the atomic-watermark mechanism is reported for a CDC → SQL
        // exactly_once topology.
        let cfg = crate::config::parse_with_extension(
            r#"
version: 1
delivery: exactly_once
pipeline:
  source:
    type: postgres-cdc
    config: { connection_url: "postgres://localhost/db", slot: s, publication: p }
  sink:
    type: postgres
    config:
      connection_url: "postgres://localhost/db"
      table_name: t
      column_mapping: auto_map
  state: { type: file, config: { path: ./state } }
"#,
            "yaml",
        )
        .unwrap();
        let nodes = expand(&cfg).unwrap();
        assert!(
            row_line(&nodes[0]).ends_with("delivery=effectively-once (atomic watermark)"),
            "got: {}",
            row_line(&nodes[0])
        );
    }
}
