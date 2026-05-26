//! Two-stage interpolation for pipeline configs.
//!
//! **Load-time** ([`interpolate`]) resolves directives that are knowable
//! before any pipeline has run — environment variables, file contents,
//! secrets. Tokens that don't match one of these prefixes are left literal
//! so the matrix expander can later treat them as `${row_id.field.path}`
//! deferred references.
//!
//! **Record-time** ([`interpolate_record`]) resolves the remaining
//! `${row_id.dotted.path}` tokens against a context map of parent records,
//! producing a string ready to feed into a connector's `Deserialize` impl.
//!
//! Supported load-time directives:
//!
//! | Form               | Resolves to |
//! |--------------------|-------------|
//! | `${env:VAR}`       | the value of environment variable `VAR` |
//! | `${file:PATH}`     | the contents of the file at `PATH` (trimmed) |
//! | `${secret:VAR}`    | alias for `${env:VAR}` (reserved for a future secrets backend) |
//!
//! Anything else (including `${users.id}`, `${posts.author.name}`) is
//! deferred to record-time. A literal `${` is written `$${`.

use crate::error::{CliError, CliResult};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;

/// Resolve every load-time directive in `input`. Unknown prefixes (and
/// tokens with no `:` at all — i.e. `${row_id.field}` references) survive
/// verbatim for record-time resolution.
pub fn interpolate(input: &str) -> CliResult<String> {
    rewrite(input, |body| match split_directive(body) {
        Directive::LoadTime { prefix, body } => match prefix {
            "env" | "secret" => Ok(Some(std::env::var(body).map_err(|_| {
                CliError::MissingEnvVar {
                    var: body.to_owned(),
                    location: format!("${{{prefix}:{body}}}"),
                }
            })?)),
            "file" => Ok(Some(read_file_trimmed(body)?)),
            // Any other ${prefix:body} that isn't env/file/secret — leave it
            // literal so a downstream validator can flag truly bogus prefixes.
            _ => Ok(None),
        },
        Directive::Deferred => Ok(None),
    })
}

/// Resolve `${id.dotted.path}` tokens against `ctx`. Tokens that look like
/// load-time directives (`${env:...}`, `${file:...}`, `${secret:...}`) are
/// left untouched — they should already have been resolved by [`interpolate`].
///
/// Errors when an `id` is unknown or a dotted path does not resolve.
pub fn interpolate_record(input: &str, ctx: &HashMap<String, Value>) -> CliResult<String> {
    rewrite(input, |body| match split_directive(body) {
        Directive::LoadTime { .. } => Ok(None),
        Directive::Deferred => {
            // `body` looks like `<id>.<dotted.path>`; split on first '.'.
            let (id, path) = match body.split_once('.') {
                Some((i, p)) => (i, p),
                None => (body, ""), // `${id}` with no path → whole record stringified
            };
            let record = ctx
                .get(id)
                .ok_or_else(|| CliError::UnknownInterpolationId {
                    id: id.to_owned(),
                    token: format!("${{{body}}}"),
                })?;
            let resolved =
                resolve_dotted(record, path).ok_or_else(|| CliError::MissingRecordField {
                    id: id.to_owned(),
                    path: path.to_owned(),
                })?;
            Ok(Some(value_to_string(&resolved)))
        }
    })
}

/// Walk `input` byte-by-byte, calling `resolve` on each `${...}` body.
/// `resolve` returns `Some(s)` to substitute, or `None` to keep verbatim.
fn rewrite<F>(input: &str, mut resolve: F) -> CliResult<String>
where
    F: FnMut(&str) -> CliResult<Option<String>>,
{
    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        // Escape: `$${` → literal `${`.
        if bytes[i] == b'$' && i + 2 < bytes.len() && bytes[i + 1] == b'$' && bytes[i + 2] == b'{' {
            out.push('$');
            i += 2;
            continue;
        }
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            let start = i + 2;
            let Some(rel_end) = input[start..].find('}') else {
                // Unclosed directive — copy the rest verbatim and stop scanning.
                out.push_str(&input[i..]);
                break;
            };
            let end = start + rel_end;
            let body = &input[start..end];
            match resolve(body)? {
                Some(s) => out.push_str(&s),
                None => out.push_str(&input[i..=end]),
            }
            i = end + 1;
            continue;
        }
        let ch = input[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    Ok(out)
}

enum Directive<'a> {
    /// Prefixed directive like `${env:VAR}` or `${file:./p}` — split on `:`.
    LoadTime { prefix: &'a str, body: &'a str },
    /// No `:` in the body — must be `${id.dotted.path}`, deferred to runtime.
    Deferred,
}

fn split_directive(body: &str) -> Directive<'_> {
    match body.split_once(':') {
        Some((prefix, rest)) => Directive::LoadTime { prefix, body: rest },
        None => Directive::Deferred,
    }
}

fn read_file_trimmed(path_str: &str) -> CliResult<String> {
    let path = PathBuf::from(path_str);
    let bytes = std::fs::read(&path).map_err(|source| CliError::ReadInterpolatedFile {
        path: path.clone(),
        source,
    })?;
    Ok(String::from_utf8_lossy(&bytes).trim_end().to_owned())
}

/// Walk a dotted path through a JSON value. Returns `None` if any segment
/// is missing or addresses through a non-object/array node.
fn resolve_dotted(root: &Value, path: &str) -> Option<Value> {
    if path.is_empty() {
        return Some(root.clone());
    }
    let mut cur = root;
    for segment in path.split('.') {
        cur = match cur {
            Value::Object(map) => map.get(segment)?,
            Value::Array(arr) => {
                let idx: usize = segment.parse().ok()?;
                arr.get(idx)?
            }
            _ => return None,
        };
    }
    Some(cur.clone())
}

/// Render a JSON value as a plain string suitable for substitution into a
/// config field. Strings come through unquoted; everything else uses
/// `to_string()` (numbers / bools / null / nested JSON).
fn value_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

// ── Post-parse load-time ref resolution ─────────────────────────────────────

/// Resolve every `${vars.X}`, `${sources.X.PATH}`, `${sinks.X.PATH}` token
/// found in a parsed [`PipelineConfig`].
///
/// Order:
/// 1. Resolve `${vars.X}` references *inside the vars block itself* (with
///    cycle detection), so vars-referencing-vars works.
/// 2. Substitute `${vars.X}` inside `pipeline.sources.*` / `pipeline.sinks.*`
///    template bodies, plus the legacy singular `pipeline.source` /
///    `pipeline.sink` configs. This snapshot becomes the basis for
///    `${sources.X.PATH}` / `${sinks.X.PATH}` lookups.
/// 3. Walk every other string location in the config and resolve both
///    `${vars.X}` and `${sources/sinks.X.PATH}`. `${row_id.path}` tokens
///    are passed through verbatim for runtime resolution.
pub fn resolve_config_refs(cfg: &mut crate::config::PipelineConfig) -> CliResult<()> {
    // Phase 1: fully resolve the vars block (vars may reference other vars).
    if let Some(vars) = cfg.vars.clone() {
        let resolved = resolve_vars_block(&vars)?;
        cfg.vars = Some(resolved);
    }

    let empty_vars: HashMap<String, Value> = HashMap::new();
    let vars_ref: &HashMap<String, Value> = cfg.vars.as_ref().unwrap_or(&empty_vars);

    // Phase 2: substitute vars inside template bodies so the snapshot taken
    // next sees fully-resolved values.
    for (name, spec) in cfg.pipeline.sources.iter_mut() {
        resolve_value_with_vars(
            &mut spec.config,
            vars_ref,
            &format!("pipeline.sources.{name}.config"),
        )?;
    }
    for (name, spec) in cfg.pipeline.sinks.iter_mut() {
        resolve_value_with_vars(
            &mut spec.config,
            vars_ref,
            &format!("pipeline.sinks.{name}.config"),
        )?;
    }
    if let Some(spec) = cfg.pipeline.source.as_mut() {
        resolve_value_with_vars(&mut spec.config, vars_ref, "pipeline.source.config")?;
    }
    if let Some(spec) = cfg.pipeline.sink.as_mut() {
        resolve_value_with_vars(&mut spec.config, vars_ref, "pipeline.sink.config")?;
    }

    // Snapshot the templates *after* vars substitution.
    let snapshot = TemplateSnapshot::capture(&cfg.pipeline);

    // Phase 3: walk everything — including the template bodies again for
    // ${sources/sinks.X.PATH} refs (Phase 2 only resolved ${vars.X} there).
    for (name, spec) in cfg.pipeline.sources.iter_mut() {
        resolve_value_full(
            &mut spec.config,
            vars_ref,
            &snapshot,
            &format!("pipeline.sources.{name}.config"),
        )?;
    }
    for (name, spec) in cfg.pipeline.sinks.iter_mut() {
        resolve_value_full(
            &mut spec.config,
            vars_ref,
            &snapshot,
            &format!("pipeline.sinks.{name}.config"),
        )?;
    }
    if let Some(spec) = cfg.pipeline.source.as_mut() {
        resolve_value_full(
            &mut spec.config,
            vars_ref,
            &snapshot,
            "pipeline.source.config",
        )?;
    }
    if let Some(spec) = cfg.pipeline.sink.as_mut() {
        resolve_value_full(
            &mut spec.config,
            vars_ref,
            &snapshot,
            "pipeline.sink.config",
        )?;
    }
    for t in cfg.pipeline.transforms.iter_mut() {
        resolve_value_full(&mut t.config, vars_ref, &snapshot, "pipeline.transforms")?;
    }
    if let Some(s) = cfg.pipeline.state.as_mut() {
        resolve_value_full(&mut s.config, vars_ref, &snapshot, "pipeline.state.config")?;
    }
    if let Some(d) = cfg.pipeline.dlq.as_mut() {
        resolve_value_full(
            &mut d.sink.config,
            vars_ref,
            &snapshot,
            "pipeline.dlq.sink.config",
        )?;
    }
    for (i, row) in cfg.matrix.iter_mut().enumerate() {
        let row_owner = row.id.clone().unwrap_or_else(|| format!("row-{i}"));
        if let Some(p) = row.source.as_mut() {
            if let Some(c) = p.config.as_mut() {
                resolve_value_full(
                    c,
                    vars_ref,
                    &snapshot,
                    &format!("matrix[{row_owner}].source.config"),
                )?;
            }
        }
        if let Some(p) = row.sink.as_mut() {
            if let Some(c) = p.config.as_mut() {
                resolve_value_full(
                    c,
                    vars_ref,
                    &snapshot,
                    &format!("matrix[{row_owner}].sink.config"),
                )?;
            }
        }
        if let Some(ts) = row.transforms.as_mut() {
            for t in ts.iter_mut() {
                resolve_value_full(
                    &mut t.config,
                    vars_ref,
                    &snapshot,
                    &format!("matrix[{row_owner}].transforms"),
                )?;
            }
        }
        if let Some(s) = row.state.as_mut() {
            resolve_value_full(
                &mut s.config,
                vars_ref,
                &snapshot,
                &format!("matrix[{row_owner}].state.config"),
            )?;
        }
        if let Some(Some(d)) = row.dlq.as_mut() {
            resolve_value_full(
                &mut d.sink.config,
                vars_ref,
                &snapshot,
                &format!("matrix[{row_owner}].dlq.sink.config"),
            )?;
        }
    }
    Ok(())
}

/// Snapshot of the resolved templates (vars already substituted) so that
/// `${sources.X.PATH}` lookups can find values without re-walking the live config.
struct TemplateSnapshot {
    sources: HashMap<String, Value>,
    sinks: HashMap<String, Value>,
}

impl TemplateSnapshot {
    fn capture(spec: &crate::config::PipelineSpec) -> Self {
        let mut sources: HashMap<String, Value> = spec
            .sources
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::to_value(v)
                        .expect("ConnectorSpec derives Serialize and cannot fail"),
                )
            })
            .collect();
        if let Some(s) = &spec.source {
            sources.entry("default".into()).or_insert_with(|| {
                serde_json::to_value(s).expect("ConnectorSpec derives Serialize and cannot fail")
            });
        }
        let mut sinks: HashMap<String, Value> = spec
            .sinks
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::to_value(v)
                        .expect("ConnectorSpec derives Serialize and cannot fail"),
                )
            })
            .collect();
        if let Some(s) = &spec.sink {
            sinks.entry("default".into()).or_insert_with(|| {
                serde_json::to_value(s).expect("ConnectorSpec derives Serialize and cannot fail")
            });
        }
        Self { sources, sinks }
    }
}

/// Resolve the vars block in topological order, returning the fully-substituted
/// map. Cycles surface as [`CliError::InterpolationCycle`].
fn resolve_vars_block(input: &HashMap<String, Value>) -> CliResult<HashMap<String, Value>> {
    let mut resolved: HashMap<String, Value> = HashMap::new();
    let mut visiting: BTreeSet<String> = BTreeSet::new();
    for key in input.keys() {
        resolve_one_var(key, input, &mut resolved, &mut visiting)?;
    }
    Ok(resolved)
}

fn resolve_one_var(
    key: &str,
    input: &HashMap<String, Value>,
    resolved: &mut HashMap<String, Value>,
    visiting: &mut BTreeSet<String>,
) -> CliResult<()> {
    if resolved.contains_key(key) {
        return Ok(());
    }
    if !visiting.insert(key.to_string()) {
        // Already on the visiting stack — cycle detected.
        let chain: Vec<String> = visiting
            .iter()
            .map(|k| format!("vars.{k}"))
            .chain(std::iter::once(format!("vars.{key}")))
            .collect();
        return Err(CliError::InterpolationCycle { chain });
    }
    let mut value = input.get(key).expect("key was taken from input map").clone();
    resolve_value_with_vars_in_vars(&mut value, input, resolved, visiting, &format!("vars.{key}"))?;
    visiting.remove(key);
    resolved.insert(key.to_string(), value);
    Ok(())
}

/// Like [`resolve_value_with_vars`] but also recurses into other vars entries
/// for vars-referencing-vars resolution (used only during Phase 1).
fn resolve_value_with_vars_in_vars(
    v: &mut Value,
    input: &HashMap<String, Value>,
    resolved: &mut HashMap<String, Value>,
    visiting: &mut BTreeSet<String>,
    _owner: &str,
) -> CliResult<()> {
    match v {
        Value::String(s) => {
            let new_s = rewrite(s, |body| {
                let Some(name) = body.strip_prefix("vars.") else {
                    return Ok(None); // not a vars ref — leave verbatim
                };
                if !resolved.contains_key(name) {
                    if !input.contains_key(name) {
                        return Err(CliError::UnknownVarsRef {
                            name: name.to_string(),
                            token: format!("${{{body}}}"),
                        });
                    }
                    resolve_one_var(name, input, resolved, visiting)?;
                }
                Ok(Some(value_to_string(&resolved[name])))
            })?;
            *s = new_s;
        }
        Value::Array(a) => {
            for item in a.iter_mut() {
                resolve_value_with_vars_in_vars(item, input, resolved, visiting, _owner)?;
            }
        }
        Value::Object(m) => {
            for item in m.values_mut() {
                resolve_value_with_vars_in_vars(item, input, resolved, visiting, _owner)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Resolve only `${vars.X}` tokens in `v`. Used in Phase 2 to substitute
/// vars into source/sink template bodies before snapshotting.
fn resolve_value_with_vars(
    v: &mut Value,
    vars: &HashMap<String, Value>,
    _owner: &str,
) -> CliResult<()> {
    match v {
        Value::String(s) => {
            let new_s = rewrite(s, |body| {
                let Some(name) = body.strip_prefix("vars.") else {
                    return Ok(None);
                };
                let val = vars.get(name).ok_or_else(|| CliError::UnknownVarsRef {
                    name: name.to_string(),
                    token: format!("${{{body}}}"),
                })?;
                Ok(Some(value_to_string(val)))
            })?;
            *s = new_s;
        }
        Value::Array(a) => {
            for item in a.iter_mut() {
                resolve_value_with_vars(item, vars, _owner)?;
            }
        }
        Value::Object(m) => {
            for item in m.values_mut() {
                resolve_value_with_vars(item, vars, _owner)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Resolve both `${vars.X}` and `${sources/sinks.X.PATH}` tokens in `v`.
/// Used in Phase 3 for all locations outside the template bodies themselves.
fn resolve_value_full(
    v: &mut Value,
    vars: &HashMap<String, Value>,
    templates: &TemplateSnapshot,
    _owner: &str,
) -> CliResult<()> {
    match v {
        Value::String(s) => {
            let new_s = rewrite(s, |body| {
                if let Some(name) = body.strip_prefix("vars.") {
                    let val = vars.get(name).ok_or_else(|| CliError::UnknownVarsRef {
                        name: name.to_string(),
                        token: format!("${{{body}}}"),
                    })?;
                    return Ok(Some(value_to_string(val)));
                }
                if let Some(rest) = body.strip_prefix("sources.") {
                    return Ok(Some(lookup_template_path(&templates.sources, "sources", rest)?));
                }
                if let Some(rest) = body.strip_prefix("sinks.") {
                    return Ok(Some(lookup_template_path(&templates.sinks, "sinks", rest)?));
                }
                // Any other prefix (e.g. `${users.id}`) is a deferred row-id token —
                // leave verbatim for runtime resolution.
                Ok(None)
            })?;
            *s = new_s;
        }
        Value::Array(a) => {
            for item in a.iter_mut() {
                resolve_value_full(item, vars, templates, _owner)?;
            }
        }
        Value::Object(m) => {
            for item in m.values_mut() {
                resolve_value_full(item, vars, templates, _owner)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn lookup_template_path(
    catalog: &HashMap<String, Value>,
    kind: &str,
    rest: &str,
) -> CliResult<String> {
    // `rest` is `<name>` or `<name>.<dotted.path>`
    let (name, path) = rest.split_once('.').unwrap_or((rest, ""));
    let template = catalog.get(name).ok_or_else(|| CliError::UnknownTemplateRef {
        token: format!("${{{kind}.{rest}}}"),
        reason: format!("no {kind} template named '{name}'"),
    })?;
    let resolved = resolve_dotted(template, path).ok_or_else(|| CliError::UnknownTemplateRef {
        token: format!("${{{kind}.{rest}}}"),
        reason: format!("path '{path}' does not resolve inside {kind} template '{name}'"),
    })?;
    Ok(value_to_string(&resolved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn passes_through_text_with_no_directives() {
        let out = interpolate("just a string").unwrap();
        assert_eq!(out, "just a string");
    }

    #[test]
    fn substitutes_env_var() {
        unsafe { std::env::set_var("FAUCET_TEST_VAR", "hello") };
        let out = interpolate("token=${env:FAUCET_TEST_VAR}").unwrap();
        assert_eq!(out, "token=hello");
        unsafe { std::env::remove_var("FAUCET_TEST_VAR") };
    }

    #[test]
    fn missing_env_var_is_an_error() {
        unsafe { std::env::remove_var("FAUCET_TEST_MISSING") };
        let err = interpolate("token=${env:FAUCET_TEST_MISSING}").unwrap_err();
        match err {
            CliError::MissingEnvVar { var, .. } => assert_eq!(var, "FAUCET_TEST_MISSING"),
            other => panic!("expected MissingEnvVar, got {other:?}"),
        }
    }

    #[test]
    fn secret_prefix_is_env_alias_for_now() {
        unsafe { std::env::set_var("FAUCET_SECRET_VAR", "shh") };
        let out = interpolate("${secret:FAUCET_SECRET_VAR}").unwrap();
        assert_eq!(out, "shh");
        unsafe { std::env::remove_var("FAUCET_SECRET_VAR") };
    }

    #[test]
    fn reads_file_directive_and_trims_trailing_newline() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("token.txt");
        std::fs::write(&path, "abcdef\n").unwrap();
        let raw = format!("token=${{file:{}}}", path.display());
        let out = interpolate(&raw).unwrap();
        assert_eq!(out, "token=abcdef");
    }

    #[test]
    fn load_time_leaves_id_path_tokens_alone() {
        unsafe { std::env::set_var("FAUCET_T", "v") };
        let out = interpolate("a=${env:FAUCET_T} b=${users.id}").unwrap();
        assert_eq!(out, "a=v b=${users.id}");
        unsafe { std::env::remove_var("FAUCET_T") };
    }

    #[test]
    fn load_time_passes_unknown_prefix_through() {
        // Unknown prefixes are deferred — the matrix expander decides if
        // they reference a real row id later. (Pre-#54 behaviour was to error
        // here; that responsibility moves to expand.rs.)
        let out = interpolate("${weird:thing}").unwrap();
        assert_eq!(out, "${weird:thing}");
    }

    #[test]
    fn dollar_dollar_brace_is_escaped() {
        let out = interpolate("path=$${env:VAR}").unwrap();
        assert_eq!(out, "path=${env:VAR}");
    }

    #[test]
    fn unclosed_directive_is_left_literal() {
        let out = interpolate("hello ${env:NOPE").unwrap();
        assert_eq!(out, "hello ${env:NOPE");
    }

    #[test]
    fn multiple_directives_resolve_in_order() {
        unsafe { std::env::set_var("FAUCET_A", "one") };
        unsafe { std::env::set_var("FAUCET_B", "two") };
        let out = interpolate("${env:FAUCET_A}-${env:FAUCET_B}").unwrap();
        assert_eq!(out, "one-two");
        unsafe { std::env::remove_var("FAUCET_A") };
        unsafe { std::env::remove_var("FAUCET_B") };
    }

    // ── record-time tests ───────────────────────────────────────────────

    fn ctx_with(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).into(), v.clone()))
            .collect()
    }

    #[test]
    fn record_resolves_simple_dotted_path() {
        let ctx = ctx_with(&[("users", json!({"id": 42, "name": "alice"}))]);
        let out = interpolate_record("/v1/users/${users.id}", &ctx).unwrap();
        assert_eq!(out, "/v1/users/42");
    }

    #[test]
    fn record_resolves_nested_dotted_path() {
        let ctx = ctx_with(&[(
            "users",
            json!({"id": 1, "addr": {"city": "NYC", "zip": "10001"}}),
        )]);
        let out = interpolate_record("/${users.addr.city}/${users.addr.zip}", &ctx).unwrap();
        assert_eq!(out, "/NYC/10001");
    }

    #[test]
    fn record_resolves_array_index() {
        let ctx = ctx_with(&[("users", json!({"tags": ["a", "b", "c"]}))]);
        let out = interpolate_record("first=${users.tags.0}", &ctx).unwrap();
        assert_eq!(out, "first=a");
    }

    #[test]
    fn record_renders_numbers_and_booleans_as_strings() {
        let ctx = ctx_with(&[("users", json!({"id": 7, "active": true}))]);
        let out = interpolate_record("id=${users.id} active=${users.active}", &ctx).unwrap();
        assert_eq!(out, "id=7 active=true");
    }

    #[test]
    fn record_unknown_id_errors() {
        let ctx = ctx_with(&[("users", json!({"id": 1}))]);
        let err = interpolate_record("${nobody.x}", &ctx).unwrap_err();
        assert!(matches!(err, CliError::UnknownInterpolationId { .. }));
    }

    #[test]
    fn record_missing_field_errors() {
        let ctx = ctx_with(&[("users", json!({"id": 1}))]);
        let err = interpolate_record("${users.missing}", &ctx).unwrap_err();
        match err {
            CliError::MissingRecordField { id, path } => {
                assert_eq!(id, "users");
                assert_eq!(path, "missing");
            }
            other => panic!("expected MissingRecordField, got {other:?}"),
        }
    }

    #[test]
    fn record_leaves_load_time_directives_alone() {
        let ctx = HashMap::new();
        let out = interpolate_record("a=${env:NOPE} b=${file:./x}", &ctx).unwrap();
        assert_eq!(out, "a=${env:NOPE} b=${file:./x}");
    }

    // ── post-parse load-time tests ───────────────────────────────────────

    use crate::config::{parse_with_extension, PipelineConfig};
    use crate::interpolate::resolve_config_refs;

    fn load(yaml: &str) -> PipelineConfig {
        let mut cfg = parse_with_extension(yaml, "yaml").unwrap();
        resolve_config_refs(&mut cfg).unwrap();
        cfg
    }

    #[test]
    fn resolves_vars_in_source_config() {
        let cfg = load(r#"
version: 1
vars:
  base: https://api.example.com
pipeline:
  source: { type: rest, config: { base_url: "${vars.base}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#);
        assert_eq!(
            cfg.pipeline.source.as_ref().unwrap().config["base_url"],
            "https://api.example.com"
        );
    }

    #[test]
    fn resolves_vars_referencing_other_vars() {
        let cfg = load(r#"
version: 1
vars:
  base: https://api.example.com
  users_url: "${vars.base}/v1/users"
pipeline:
  source: { type: rest, config: { url: "${vars.users_url}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#);
        assert_eq!(
            cfg.pipeline.source.as_ref().unwrap().config["url"],
            "https://api.example.com/v1/users"
        );
    }

    #[test]
    fn resolves_template_ref_from_matrix_row() {
        let cfg = load(r#"
version: 1
pipeline:
  sources:
    users_api:
      type: rest
      config: { base_url: https://api.example.com }
  sinks:
    archive: { type: jsonl, config: { path: ./out.jsonl } }
matrix:
  - id: load_users
    source:
      ref: users_api
      config: { audit_url: "${sources.users_api.config.base_url}/audit" }
"#);
        let row_src = cfg.matrix[0].source.as_ref().unwrap();
        assert_eq!(
            row_src.config.as_ref().unwrap()["audit_url"],
            "https://api.example.com/audit"
        );
    }

    #[test]
    fn detects_vars_cycle() {
        let yaml = r#"
version: 1
vars:
  a: "${vars.b}"
  b: "${vars.a}"
pipeline:
  source: { type: rest, config: {} }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let mut cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = resolve_config_refs(&mut cfg).unwrap_err();
        match err {
            CliError::InterpolationCycle { chain } => {
                assert!(chain.iter().any(|s| s.contains("vars.a")));
                assert!(chain.iter().any(|s| s.contains("vars.b")));
            }
            other => panic!("expected InterpolationCycle, got {other:?}"),
        }
    }

    #[test]
    fn unknown_var_errors() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { url: "${vars.nope}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let mut cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = resolve_config_refs(&mut cfg).unwrap_err();
        match err {
            CliError::UnknownVarsRef { name, .. } => assert_eq!(name, "nope"),
            other => panic!("expected UnknownVarsRef, got {other:?}"),
        }
    }

    #[test]
    fn unknown_template_ref_errors() {
        let yaml = r#"
version: 1
pipeline:
  source: { type: rest, config: { x: "${sources.nope.config.foo}" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#;
        let mut cfg = parse_with_extension(yaml, "yaml").unwrap();
        let err = resolve_config_refs(&mut cfg).unwrap_err();
        match err {
            CliError::UnknownTemplateRef { reason, .. } => {
                assert!(reason.to_ascii_lowercase().contains("nope"));
            }
            other => panic!("expected UnknownTemplateRef, got {other:?}"),
        }
    }

    #[test]
    fn leaves_row_id_tokens_for_runtime() {
        let cfg = load(r#"
version: 1
pipeline:
  source: { type: rest, config: { path: "/v1/users/${users.id}/posts" } }
  sink:   { type: jsonl, config: { path: ./o.jsonl } }
"#);
        // ${users.id} must survive — it's a deferred row-id reference.
        assert_eq!(
            cfg.pipeline.source.as_ref().unwrap().config["path"],
            "/v1/users/${users.id}/posts"
        );
    }
}
