//! `faucet validate` — parse + expand a pipeline config without running.
//!
//! Surfaces every per-row error with the row id, so a config with multiple
//! issues reports them together instead of failing at the first one.

use crate::cli::ValidateArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use crate::expand::{NodeRole, expand};
use crate::registry::{sink_schema, source_schema};
use crate::state::available_state_kinds;
use crate::transforms::available_transforms;

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

    let cfg = if args.no_secrets {
        // Grammar / structure only — never touch the network.
        PipelineConfig::from_path_tolerating_secrets(&path, args.profile.as_deref())?
    } else {
        // Real preflight: report each secret reference, then resolve.
        let refs = crate::secrets::scan_path_refs(&path, args.profile.as_deref())?;
        let cfg = PipelineConfig::from_path_async(&path, args.profile.as_deref()).await?;
        for (scheme, reference) in &refs {
            println!("secret: {scheme}:{reference} → resolved");
        }
        cfg
    };
    let nodes = expand(&cfg)?;

    // Validate the replication block (snapshot source / CDC source / state) so
    // `faucet validate` catches misconfiguration without running.
    if let Some(spec) = &cfg.replication {
        crate::replication::compiled::CompiledReplication::compile(spec, &cfg)?;
        println!("replication: mode={:?} — valid", spec.mode);
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
        "  - {} [{}] source={} sink={}{}",
        node.id, role, node.source.kind, node.sink.kind, deps
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
        assert_eq!(line_for("dims"), "  - dims [root] source=rest sink=jsonl");
        assert_eq!(
            line_for("posts"),
            "  - posts [child of 'dims' (parent_key=id)] source=rest sink=jsonl"
        );
        assert_eq!(
            line_for("facts"),
            "  - facts [root] source=rest sink=jsonl depends_on=[dims]"
        );
    }
}
