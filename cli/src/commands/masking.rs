//! `faucet masking` — validate a config's `masking:` block and print which
//! rules apply to each destination sink (the destination-scoping check).
//! Offline-safe: secrets are never fetched (compiling a policy needs no
//! credentials, and the key is not required to list rules).

use crate::cli::MaskingArgs;
use crate::config::PipelineConfig;
use crate::error::{CliError, CliResult};
use faucet_core::masking::{CompiledMasking, MaskAction, MaskRule, MaskingSpec};

/// Execute the `masking` subcommand.
pub async fn run(args: MaskingArgs) -> CliResult<()> {
    let cwd = std::env::current_dir()?;
    let env_path =
        crate::env_loader::resolve_env_file(args.env_file.as_deref(), args.no_env_file, &cwd)?;
    crate::env_loader::load_env_file_if_present(env_path.as_deref())?;

    let path = match args.config {
        Some(p) => p,
        None => crate::env_loader::discover_config_path(&cwd).ok_or(CliError::NoConfigOrFromEnv)?,
    };
    let cfg = PipelineConfig::from_path_tolerating_secrets(&path, args.profile.as_deref())?;
    let spec = cfg.pipeline.masking.as_ref().ok_or_else(|| {
        CliError::Config(
            "no `pipeline.masking:` block in this config — add one, or run \
             `faucet schema masking` to see the block's JSON Schema"
                .to_string(),
        )
    })?;
    // Compile first so a malformed policy fails before anything is printed.
    CompiledMasking::compile(spec).map_err(|e| CliError::Config(format!("masking: {e}")))?;

    let destinations = destinations(&cfg);
    print!("{}", render_summary(spec, &destinations));
    Ok(())
}

/// The destination sinks declared in the config: each named template under
/// `pipeline.sinks`, plus the legacy singular `pipeline.sink` as `default`.
/// Returns `(template_name, connector_kind)` pairs, sorted for stable output.
fn destinations(cfg: &PipelineConfig) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    if let Some(sink) = &cfg.pipeline.sink {
        out.push(("default".to_string(), sink.kind.clone()));
    }
    for (name, spec) in &cfg.pipeline.sinks {
        out.push((name.clone(), spec.kind.clone()));
    }
    out.sort();
    out.dedup();
    out
}

/// Labels of the rules that apply to a sink identified by `name` + `kind`.
fn applied_rules(spec: &MaskingSpec, name: &str, kind: &str) -> Vec<String> {
    spec.rules
        .iter()
        .enumerate()
        .filter(|(_, r)| rule_applies(r, name, kind))
        .map(|(i, r)| r.name.clone().unwrap_or_else(|| format!("rule_{i}")))
        .collect()
}

fn rule_applies(rule: &MaskRule, name: &str, kind: &str) -> bool {
    rule.applies_to.is_empty() || rule.applies_to.iter().any(|t| t == name || t == kind)
}

/// Render the human summary. Pure — returned as a string for testability.
fn render_summary(spec: &MaskingSpec, destinations: &[(String, String)]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    let n = spec.rules.len();
    let _ = writeln!(
        out,
        "masking — valid ({n} rule{})",
        if n == 1 { "" } else { "s" }
    );
    if let Some(d) = &spec.description {
        let _ = writeln!(out, "  description: {d}");
    }
    let _ = writeln!(
        out,
        "  key: {}",
        if spec.key.is_some() {
            "configured (keyed HMAC-SHA256 for hash/tokenize)"
        } else {
            "none (unkeyed SHA-256 for hash/tokenize)"
        }
    );
    let _ = writeln!(out, "  rules:");
    for (i, r) in spec.rules.iter().enumerate() {
        let label = r.name.clone().unwrap_or_else(|| format!("rule_{i}"));
        let scope = if r.applies_to.is_empty() {
            "all sinks".to_string()
        } else {
            format!("sinks[{}]", r.applies_to.join(", "))
        };
        let _ = writeln!(
            out,
            "    - {label}: {} → {} ({scope})",
            describe_match(r),
            describe_action(&r.action),
        );
    }

    if destinations.is_empty() {
        let _ = writeln!(
            out,
            "  destinations: (none declared — every unscoped rule applies)"
        );
    } else {
        let _ = writeln!(out, "  destinations:");
        for (name, kind) in destinations {
            let applied = applied_rules(spec, name, kind);
            let list = if applied.is_empty() {
                "(no rules apply)".to_string()
            } else {
                applied.join(", ")
            };
            let _ = writeln!(out, "    - {name} [{kind}]: {list}");
        }
    }
    out
}

fn describe_match(rule: &MaskRule) -> String {
    let m = &rule.matcher;
    let mut parts: Vec<String> = Vec::new();
    if let Some(p) = &m.field_pattern {
        parts.push(format!("field_pattern /{p}/"));
    }
    if let Some(d) = m.value_detector {
        parts.push(format!("detector {d}"));
    }
    if !m.fields.is_empty() {
        parts.push(format!("fields[{}]", m.fields.join(", ")));
    }
    parts.join(" | ")
}

fn describe_action(action: &MaskAction) -> String {
    match action {
        MaskAction::Redact { .. } => "redact".to_string(),
        MaskAction::Hash => "hash".to_string(),
        MaskAction::Tokenize { prefix } => match prefix {
            Some(p) => format!("tokenize (prefix '{p}')"),
            None => "tokenize".to_string(),
        },
        MaskAction::Partial { keep_last, .. } => format!("partial (keep_last {keep_last})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec() -> MaskingSpec {
        serde_json::from_value(json!({
            "description": "customer PII",
            "key": "k",
            "rules": [
                { "name": "emails", "match": { "value_detector": "email" },
                  "action": { "type": "redact" } },
                { "name": "ssn", "match": { "field_pattern": "(?i)ssn" },
                  "action": { "type": "hash" }, "applies_to": ["analytics"] },
                { "match": { "fields": ["card"] },
                  "action": { "type": "partial", "keep_last": 4 } }
            ]
        }))
        .unwrap()
    }

    #[test]
    fn summary_lists_rules_key_and_scope() {
        let dests = vec![
            ("default".to_string(), "postgres".to_string()),
            ("analytics".to_string(), "bigquery".to_string()),
        ];
        let out = render_summary(&spec(), &dests);
        assert!(out.contains("masking — valid (3 rules)"), "{out}");
        assert!(out.contains("description: customer PII"), "{out}");
        assert!(out.contains("keyed HMAC-SHA256"), "{out}");
        assert!(
            out.contains("emails: detector email → redact (all sinks)"),
            "{out}"
        );
        assert!(
            out.contains("ssn: field_pattern /(?i)ssn/ → hash (sinks[analytics])"),
            "{out}"
        );
        assert!(
            out.contains("rule_2: fields[card] → partial (keep_last 4)"),
            "{out}"
        );
    }

    #[test]
    fn summary_shows_applied_rules_per_destination() {
        let dests = vec![
            ("default".to_string(), "postgres".to_string()),
            ("analytics".to_string(), "bigquery".to_string()),
        ];
        let out = render_summary(&spec(), &dests);
        // default: unscoped rules (emails, rule_2) apply; ssn is analytics-only.
        assert!(
            out.contains("- default [postgres]: emails, rule_2"),
            "{out}"
        );
        // analytics: all three (ssn scoped to analytics + the two unscoped).
        assert!(
            out.contains("- analytics [bigquery]: emails, ssn, rule_2"),
            "{out}"
        );
    }

    #[test]
    fn scope_matches_connector_kind_too() {
        let s: MaskingSpec = serde_json::from_value(json!({
            "rules": [{ "match": { "fields": ["x"] }, "action": { "type": "redact" },
                        "applies_to": ["bigquery"] }]
        }))
        .unwrap();
        // A rule scoped to the `bigquery` KIND applies to a template of that kind.
        assert_eq!(applied_rules(&s, "warehouse", "bigquery"), vec!["rule_0"]);
        assert!(applied_rules(&s, "warehouse", "postgres").is_empty());
    }

    #[test]
    fn no_destinations_note() {
        let out = render_summary(&spec(), &[]);
        assert!(out.contains("none declared"), "{out}");
    }

    #[test]
    fn unkeyed_and_tokenize_without_prefix_render() {
        let s: MaskingSpec = serde_json::from_value(json!({
            "rules": [{ "name": "tok", "match": { "fields": ["id"] },
                        "action": { "type": "tokenize" } }]
        }))
        .unwrap();
        let out = render_summary(&s, &[("default".into(), "jsonl".into())]);
        assert!(out.contains("masking — valid (1 rule)"), "{out}");
        assert!(out.contains("none (unkeyed SHA-256"), "{out}");
        assert!(
            out.contains("tok: fields[id] → tokenize (all sinks)"),
            "{out}"
        );
        assert!(out.contains("- default [jsonl]: tok"), "{out}");
    }

    #[test]
    fn destinations_reads_singular_sink_and_named_sinks() {
        use crate::config::PipelineConfig;
        use std::path::Path;
        // Singular `sink:` → the `default` destination.
        let single = PipelineConfig::from_text(
            r#"version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  masking: { rules: [ { match: { fields: [x] }, action: { type: redact } } ] }
  sink: { type: jsonl, config: { path: ./out.jsonl } }
"#,
            Path::new("test.yaml"),
        )
        .unwrap();
        assert_eq!(
            destinations(&single),
            vec![("default".into(), "jsonl".into())]
        );

        // Named `sinks:` templates → one destination each, sorted.
        let named = PipelineConfig::from_text(
            r#"version: 1
pipeline:
  source: { type: csv, config: { path: ./in.csv } }
  masking: { rules: [ { match: { fields: [x] }, action: { type: redact } } ] }
  sinks:
    warehouse: { type: bigquery, config: {} }
    archive:   { type: jsonl, config: { path: ./a.jsonl } }
matrix:
  - id: a
    sink: { ref: archive }
  - id: w
    sink: { ref: warehouse }
"#,
            Path::new("test.yaml"),
        )
        .unwrap();
        assert_eq!(
            destinations(&named),
            vec![
                ("archive".into(), "jsonl".into()),
                ("warehouse".into(), "bigquery".into()),
            ]
        );
    }
}
