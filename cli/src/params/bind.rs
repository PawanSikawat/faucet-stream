//! Binding `${param.NAME}` references to supplied values (#444).
//!
//! Binding is a **pre-parse pass over the untyped config document**, run after
//! `${env:}` / `${file:}` / `${secret:}` interpolation and before the typed
//! `PipelineConfig` deserialise. Two consequences worth stating, because both
//! are load-bearing:
//!
//! 1. **Structure safety.** Substitution happens per JSON/YAML scalar, exactly
//!    like [`crate::interpolate::interpolate_value`] — a supplied value holding
//!    `:`, a newline, or `-` stays the single scalar it replaced and can never
//!    inject a key or an array element. Downstream, SQL-bound and JSON-safe
//!    substitution paths (`substitute_context_bind_params` /
//!    `substitute_context_json` in `faucet_core::util`) are untouched, so the
//!    existing SQL/JSON-injection guarantees still hold for param-derived text.
//! 2. **No re-interpolation of caller input.** Env/file/secret directives are
//!    resolved *before* binding, so a supplied value is never itself scanned for
//!    directives. Belt and braces, a supplied value containing `${` is rejected
//!    outright: params are data, not directives.
//!
//! When `${param.NAME}` is a scalar's *entire* text the declared type is
//! preserved (an `int` param lands as a JSON number, not `"5"`); embedded in a
//! longer string it is stringified, like every other interpolation namespace.

use super::spec::{self, ParamsSpec};
use crate::error::{CliError, CliResult};
use crate::interpolate::{
    Directive, classify_directive, iter_directives, rewrite, value_to_string,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// The config key holding the declaration block.
pub const PARAMS_KEY: &str = "params";

/// The interpolation namespace params live in (`${param.NAME}`).
pub const PARAM_ID: &str = "param";

/// What to do with a `required` param the caller did not supply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindMode {
    /// A missing required param is an error. Used on every path that actually
    /// runs a pipeline.
    Strict,
    /// A missing required param is filled with a type-shaped placeholder. Used
    /// to structurally validate a config whose values arrive later — template
    /// registration and `faucet validate` on a parameterized config.
    Placeholder,
}

/// Caller-supplied values, keyed by param name. Values arrive either as real
/// JSON (HTTP) or as strings (`--param k=v`); [`spec::coerce`] normalizes both.
pub type SuppliedParams = BTreeMap<String, Value>;

/// The result of a bind: every declared param's effective value, plus which of
/// them are sensitive.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoundParams {
    pub values: BTreeMap<String, Value>,
    pub secret_names: BTreeSet<String>,
}

impl BoundParams {
    /// The bound values with every `secret: true` entry replaced by `"***"` —
    /// the only form safe to echo into an API response, the audit log, or a run
    /// record.
    pub fn redacted(&self) -> BTreeMap<String, Value> {
        self.values
            .iter()
            .map(|(k, v)| {
                if self.secret_names.contains(k) {
                    (k.clone(), Value::String("***".into()))
                } else {
                    (k.clone(), v.clone())
                }
            })
            .collect()
    }

    /// Whether any bound param is marked secret (drives the cluster-persistence
    /// guard in the template-trigger path).
    pub fn has_secrets(&self) -> bool {
        !self.secret_names.is_empty()
    }
}

/// Read the `params:` block out of an untyped config document, validating it.
/// A document with no block yields an empty spec.
pub fn declared(doc: &Value) -> CliResult<ParamsSpec> {
    let Some(raw) = doc.get(PARAMS_KEY) else {
        return Ok(ParamsSpec::new());
    };
    if raw.is_null() {
        return Ok(ParamsSpec::new());
    }
    let parsed: ParamsSpec = serde_json::from_value(raw.clone())
        .map_err(|e| CliError::Config(format!("invalid `params:` block: {e}")))?;
    spec::validate(&parsed)?;
    Ok(parsed)
}

/// Resolve every declared param to a value, without touching the document.
///
/// Precedence: supplied → `default` → (`Placeholder` mode) type placeholder →
/// error. Supplied names that are not declared are rejected so a typo can never
/// be silently ignored.
pub fn resolve(
    spec: &ParamsSpec,
    supplied: &SuppliedParams,
    mode: BindMode,
) -> CliResult<BoundParams> {
    for name in supplied.keys() {
        if !spec.contains_key(name) {
            return Err(CliError::UnknownParam {
                name: name.clone(),
                known: spec.keys().cloned().collect(),
            });
        }
    }

    let mut bound = BoundParams::default();
    for (name, p) in spec {
        let value = match supplied.get(name) {
            Some(raw) => {
                reject_directives(name, raw)?;
                spec::coerce(name, p.kind, raw)?
            }
            None => match &p.default {
                // A default is authored in the config and already went through
                // env/file/secret resolution, so it is coerced but not
                // directive-checked.
                Some(d) => spec::coerce(name, p.kind, d)?,
                None => match mode {
                    BindMode::Placeholder => p.kind.placeholder(),
                    BindMode::Strict => {
                        return Err(CliError::MissingParam {
                            name: name.clone(),
                            description: p.description.clone(),
                        });
                    }
                },
            },
        };
        if p.secret {
            // Register before the value can reach any log line, error string, or
            // API body. `register` no-ops below the registry's minimum length.
            crate::secrets::registry::register(&value_to_string(&value));
            bound.secret_names.insert(name.clone());
        }
        bound.values.insert(name.clone(), value);
    }
    Ok(bound)
}

/// Bind params in an untyped config document, in place.
///
/// Reads and validates the document's own `params:` block, resolves each param
/// against `supplied`, then substitutes `${param.NAME}` everywhere **except**
/// inside the `params:` block itself (a default is a literal, not a target).
/// Returns the bound values so the caller can echo/audit them (redacted).
pub fn bind_document(
    doc: &mut Value,
    supplied: &SuppliedParams,
    mode: BindMode,
) -> CliResult<BoundParams> {
    let spec = declared(doc)?;
    let bound = resolve(&spec, supplied, mode)?;

    // Lift the declaration block out so defaults are never rewritten, then put
    // it back byte-identical — the block is part of the config and is persisted
    // with a registered template.
    let stashed = doc.get_mut(PARAMS_KEY).map(std::mem::take);
    let result = substitute(doc, &bound.values);
    if let (Some(block), Some(map)) = (stashed, doc.as_object_mut()) {
        map.insert(PARAMS_KEY.to_string(), block);
    }
    result?;
    Ok(bound)
}

/// Reject an interpolation directive inside a caller-supplied value. Supplied
/// params are data: allowing `${vault:…}` / `${env:…}` through would let a
/// caller read the *server's* secrets and environment by way of a param.
fn reject_directives(name: &str, raw: &Value) -> CliResult<()> {
    if let Value::String(s) = raw
        && s.contains("${")
    {
        return Err(CliError::Config(format!(
            "param '{name}': value contains an interpolation directive (`${{`). Param values are \
             literal data — put the directive in the config's `params:` default or in the config \
             body instead"
        )));
    }
    Ok(())
}

/// Substitute `${param.NAME}` throughout `v`.
fn substitute(v: &mut Value, bound: &BTreeMap<String, Value>) -> CliResult<()> {
    if let Value::String(s) = v {
        let replaced = match whole_token(s, bound)? {
            Some(typed) => typed,
            None => Value::String(rewrite_text(s, bound)?),
        };
        *v = replaced;
        return Ok(());
    }
    match v {
        Value::Array(items) => {
            for item in items.iter_mut() {
                substitute(item, bound)?;
            }
        }
        Value::Object(map) => {
            // Keys may carry tokens too (a param-named header, say). Rebuild the
            // map so a rewritten key is honoured — mirrors `interpolate_value`.
            let entries: Vec<(String, Value)> = std::mem::take(map).into_iter().collect();
            for (key, mut val) in entries {
                substitute(&mut val, bound)?;
                map.insert(rewrite_text(&key, bound)?, val);
            }
        }
        _ => {}
    }
    Ok(())
}

/// If `s` is *exactly* one `${param.NAME}` token, return that param's value with
/// its declared type intact. Anything else (extra text, several tokens, an
/// escaped `$${param.x}`) returns `None` for textual rewriting.
fn whole_token(s: &str, bound: &BTreeMap<String, Value>) -> CliResult<Option<Value>> {
    let mut tokens = iter_directives(s);
    let Some((token, dir)) = tokens.next() else {
        return Ok(None);
    };
    if tokens.next().is_some() || token != s {
        return Ok(None);
    }
    match dir {
        Directive::Deferred { id, path } if id == PARAM_ID => {
            Ok(Some(lookup(path, token, bound)?.clone()))
        }
        _ => Ok(None),
    }
}

/// Textual rewrite: every `${param.NAME}` becomes the stringified value; every
/// other directive survives verbatim for its own resolution stage.
fn rewrite_text(s: &str, bound: &BTreeMap<String, Value>) -> CliResult<String> {
    rewrite(s, |body| match classify_directive(body) {
        Directive::Deferred { id, path } if id == PARAM_ID => {
            let token = format!("${{{body}}}");
            Ok(Some(value_to_string(lookup(path, &token, bound)?)))
        }
        _ => Ok(None),
    })
}

/// Resolve the `NAME` in `${param.NAME}`. The path must be a bare name — nested
/// lookups (`${param.a.b}`) are not a thing, since params are scalars.
fn lookup<'a>(path: &str, token: &str, bound: &'a BTreeMap<String, Value>) -> CliResult<&'a Value> {
    if path.is_empty() {
        return Err(CliError::Config(format!(
            "interpolation '{token}' is missing a param name — write `${{param.NAME}}`"
        )));
    }
    if path.contains('.') {
        return Err(CliError::Config(format!(
            "interpolation '{token}' is not a valid param reference — params are scalars, so \
             `${{param.NAME}}` takes a bare name"
        )));
    }
    bound.get(path).ok_or_else(|| CliError::UnknownParamRef {
        name: path.to_string(),
        token: token.to_string(),
    })
}

/// Parse a `--param key=value` CLI argument. The value is kept as a JSON string;
/// [`spec::coerce`] converts it to the declared type at bind time.
pub fn parse_cli_param(arg: &str) -> CliResult<(String, Value)> {
    let (key, value) = arg.split_once('=').ok_or_else(|| {
        CliError::Config(format!("invalid --param '{arg}' — expected `name=value`"))
    })?;
    let key = key.trim();
    if key.is_empty() {
        return Err(CliError::Config(format!(
            "invalid --param '{arg}' — the name is empty"
        )));
    }
    Ok((key.to_string(), Value::String(value.to_string())))
}

/// Collect a `--param name=value` list into a [`SuppliedParams`] map, rejecting
/// a repeated name (silently keeping the last would be a footgun).
pub fn collect_cli_params(args: &[String]) -> CliResult<SuppliedParams> {
    let mut out = SuppliedParams::new();
    for arg in args {
        let (k, v) = parse_cli_param(arg)?;
        if out.insert(k.clone(), v).is_some() {
            return Err(CliError::Config(format!(
                "--param '{k}' was given more than once"
            )));
        }
    }
    Ok(out)
}

/// Collect a `--param-env NAME[=VALUE]` list into an env overlay. A bare `NAME`
/// takes the value from the caller's own environment (so a secret never appears
/// in the process arguments); `NAME=VALUE` sets it explicitly.
pub fn collect_env_overrides(args: &[String]) -> CliResult<BTreeMap<String, String>> {
    let mut out = BTreeMap::new();
    for arg in args {
        let (name, value) = match arg.split_once('=') {
            Some((n, v)) => (n.trim().to_string(), v.to_string()),
            None => {
                let n = arg.trim().to_string();
                let v = std::env::var(&n).map_err(|_| {
                    CliError::Config(format!(
                        "--param-env '{n}' has no value and '{n}' is not set in the environment"
                    ))
                })?;
                (n, v)
            }
        };
        if name.is_empty() {
            return Err(CliError::Config(format!(
                "invalid --param-env '{arg}' — the variable name is empty"
            )));
        }
        if out.insert(name.clone(), value).is_some() {
            return Err(CliError::Config(format!(
                "--param-env '{name}' was given more than once"
            )));
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec_of(yaml: &str) -> ParamsSpec {
        serde_yaml::from_str(yaml).unwrap()
    }

    fn supplied(pairs: &[(&str, Value)]) -> SuppliedParams {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn resolves_supplied_default_and_placeholder() {
        let spec = spec_of(
            "tenant: { required: true }\n\
             since: { default: \"1970-01-01\" }\n\
             page: { type: int, required: true }\n",
        );
        let bound = resolve(
            &spec,
            &supplied(&[("tenant", json!("acme")), ("page", json!("50"))]),
            BindMode::Strict,
        )
        .unwrap();
        assert_eq!(bound.values["tenant"], json!("acme"));
        assert_eq!(bound.values["since"], json!("1970-01-01"));
        // Coerced to the declared int type even though it arrived as a string.
        assert_eq!(bound.values["page"], json!(50));

        // Placeholder mode fills the required ones.
        let bound = resolve(&spec, &SuppliedParams::new(), BindMode::Placeholder).unwrap();
        assert_eq!(bound.values["tenant"], json!("<param>"));
        assert_eq!(bound.values["page"], json!(0));
        assert_eq!(bound.values["since"], json!("1970-01-01"));
    }

    #[test]
    fn missing_required_param_is_a_typed_error() {
        let spec = spec_of("tenant: { required: true, description: Tenant to sync }\n");
        match resolve(&spec, &SuppliedParams::new(), BindMode::Strict).unwrap_err() {
            CliError::MissingParam { name, description } => {
                assert_eq!(name, "tenant");
                assert_eq!(description.as_deref(), Some("Tenant to sync"));
            }
            other => panic!("expected MissingParam, got {other:?}"),
        }
    }

    #[test]
    fn unknown_supplied_param_is_rejected() {
        let spec = spec_of("tenant: { required: true }\n");
        match resolve(
            &spec,
            &supplied(&[("tenant", json!("a")), ("tenatn", json!("b"))]),
            BindMode::Strict,
        )
        .unwrap_err()
        {
            CliError::UnknownParam { name, known } => {
                assert_eq!(name, "tenatn");
                assert_eq!(known, vec!["tenant".to_string()]);
            }
            other => panic!("expected UnknownParam, got {other:?}"),
        }
    }

    #[test]
    fn supplied_value_may_not_carry_a_directive() {
        let spec = spec_of("t: { required: true }\n");
        let err = resolve(
            &spec,
            &supplied(&[("t", json!("${vault:secret/data/db#password}"))]),
            BindMode::Strict,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("literal data"), "{err}");
    }

    #[test]
    fn binds_document_typed_and_textual() {
        let mut doc = json!({
            "version": 1,
            "params": {
                "tenant": { "required": true },
                "page": { "type": "int", "default": 500 },
                "live": { "type": "bool", "default": true }
            },
            "pipeline": {
                "source": {
                    "type": "rest",
                    "config": {
                        "url": "https://api.example.com/${param.tenant}/events",
                        "page_size": "${param.page}",
                        "streaming": "${param.live}"
                    }
                }
            }
        });
        let bound = bind_document(
            &mut doc,
            &supplied(&[("tenant", json!("acme"))]),
            BindMode::Strict,
        )
        .unwrap();
        let cfg = &doc["pipeline"]["source"]["config"];
        assert_eq!(cfg["url"], "https://api.example.com/acme/events");
        // Whole-scalar tokens keep the declared type.
        assert_eq!(cfg["page_size"], json!(500));
        assert_eq!(cfg["streaming"], json!(true));
        // The declaration block survives untouched.
        assert_eq!(doc["params"]["page"]["default"], json!(500));
        assert_eq!(bound.values["tenant"], json!("acme"));
    }

    #[test]
    fn defaults_inside_the_params_block_are_not_substituted() {
        // A default that *looks* like a param reference stays literal — the block
        // is lifted out before substitution.
        let mut doc = json!({
            "params": { "a": { "default": "${param.a}" } },
            "pipeline": { "x": "ok" }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc["params"]["a"]["default"], "${param.a}");
    }

    #[test]
    fn undeclared_reference_is_rejected() {
        let mut doc = json!({
            "params": { "a": { "default": "1" } },
            "pipeline": { "url": "${param.b}" }
        });
        match bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap_err() {
            CliError::UnknownParamRef { name, token } => {
                assert_eq!(name, "b");
                assert_eq!(token, "${param.b}");
            }
            other => panic!("expected UnknownParamRef, got {other:?}"),
        }
    }

    #[test]
    fn reference_without_a_params_block_is_rejected() {
        // No `params:` at all — a `${param.x}` token must not silently survive
        // into a connector config.
        let mut doc = json!({ "pipeline": { "url": "${param.x}" } });
        let err = bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(matches!(err, CliError::UnknownParamRef { .. }), "{err:?}");
    }

    #[test]
    fn supplying_a_param_with_no_block_is_rejected() {
        let mut doc = json!({ "pipeline": {} });
        match bind_document(&mut doc, &supplied(&[("x", json!("1"))]), BindMode::Strict)
            .unwrap_err()
        {
            CliError::UnknownParam { known, .. } => assert!(known.is_empty()),
            other => panic!("expected UnknownParam, got {other:?}"),
        }
    }

    #[test]
    fn malformed_references_are_rejected() {
        for bad in ["${param}", "${param.a.b}"] {
            let mut doc = json!({
                "params": { "a": { "default": "1" } },
                "pipeline": { "url": bad }
            });
            let err = bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict)
                .unwrap_err()
                .to_string();
            assert!(err.contains("param"), "{bad}: {err}");
        }
    }

    #[test]
    fn escaped_token_stays_literal() {
        let mut doc = json!({
            "params": { "a": { "default": "v" } },
            "pipeline": { "note": "$${param.a}" }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc["pipeline"]["note"], "${param.a}");
    }

    #[test]
    fn other_namespaces_survive_binding() {
        let mut doc = json!({
            "params": { "a": { "default": "v" } },
            "pipeline": { "url": "${param.a}/${now.date}/${users.id}" }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc["pipeline"]["url"], "v/${now.date}/${users.id}");
    }

    #[test]
    fn substitutes_into_keys_and_arrays() {
        let mut doc = json!({
            "params": { "h": { "default": "X-Tenant" }, "n": { "type": "int", "default": 2 } },
            "pipeline": {
                "headers": { "${param.h}": "v" },
                "list": ["${param.n}", "n=${param.n}"]
            }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc["pipeline"]["headers"]["X-Tenant"], "v");
        assert_eq!(doc["pipeline"]["list"][0], json!(2));
        assert_eq!(doc["pipeline"]["list"][1], json!("n=2"));
    }

    #[test]
    fn secret_params_are_tracked_and_redacted() {
        let spec = spec_of("token: { required: true, secret: true }\nuser: { default: bob }\n");
        let bound = resolve(
            &spec,
            &supplied(&[("token", json!("s3cret-value-long-enough"))]),
            BindMode::Strict,
        )
        .unwrap();
        assert!(bound.has_secrets());
        let red = bound.redacted();
        assert_eq!(red["token"], json!("***"));
        assert_eq!(red["user"], json!("bob"));
        // Registered for redaction, so it can never reach a log line in clear.
        assert_eq!(
            crate::secrets::registry::redact("token=s3cret-value-long-enough"),
            "token=***"
        );
    }

    #[test]
    fn invalid_params_block_is_a_config_error() {
        let doc = json!({ "params": { "a": { "type": "date" } } });
        let err = declared(&doc).unwrap_err().to_string();
        assert!(err.contains("`params:` block"), "{err}");
        // A null block is simply absent.
        assert!(declared(&json!({ "params": null })).unwrap().is_empty());
        assert!(declared(&json!({})).unwrap().is_empty());
    }

    #[test]
    fn cli_param_parsing() {
        let (k, v) = parse_cli_param("tenant=acme").unwrap();
        assert_eq!(k, "tenant");
        assert_eq!(v, json!("acme"));
        // Values may contain '='.
        let (_, v) = parse_cli_param("q=a=b").unwrap();
        assert_eq!(v, json!("a=b"));
        // Empty value is allowed (an intentional blank).
        let (_, v) = parse_cli_param("q=").unwrap();
        assert_eq!(v, json!(""));
        assert!(parse_cli_param("noequals").is_err());
        assert!(parse_cli_param("=v").is_err());

        let map = collect_cli_params(&["a=1".into(), "b=2".into()]).unwrap();
        assert_eq!(map.len(), 2);
        let err = collect_cli_params(&["a=1".into(), "a=2".into()])
            .unwrap_err()
            .to_string();
        assert!(err.contains("more than once"), "{err}");
    }

    #[test]
    fn env_override_parsing() {
        let map = collect_env_overrides(&["A=1".into()]).unwrap();
        assert_eq!(map["A"], "1");
        // Bare NAME reads the caller's environment.
        unsafe { std::env::set_var("FAUCET_PARAM_ENV_TEST", "from-env") };
        let map = collect_env_overrides(&["FAUCET_PARAM_ENV_TEST".into()]).unwrap();
        assert_eq!(map["FAUCET_PARAM_ENV_TEST"], "from-env");
        unsafe { std::env::remove_var("FAUCET_PARAM_ENV_TEST") };
        assert!(collect_env_overrides(&["FAUCET_PARAM_ENV_TEST".into()]).is_err());
        assert!(collect_env_overrides(&["=1".into()]).is_err());
        assert!(collect_env_overrides(&["A=1".into(), "A=2".into()]).is_err());
    }

    #[test]
    fn placeholder_mode_leaves_a_bindable_document() {
        // The point of Placeholder mode: a config with required params still
        // parses + expands for structural validation.
        let mut doc = json!({
            "params": { "t": { "required": true }, "n": { "type": "int", "required": true } },
            "pipeline": { "source": { "config": { "url": "https://x/${param.t}", "n": "${param.n}" } } }
        });
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Placeholder).unwrap();
        let cfg = &doc["pipeline"]["source"]["config"];
        assert_eq!(cfg["url"], "https://x/<param>");
        assert_eq!(cfg["n"], json!(0));
    }

    #[test]
    fn bound_params_default_is_empty() {
        let b = BoundParams::default();
        assert!(!b.has_secrets());
        assert!(b.redacted().is_empty());
    }

    #[test]
    fn binding_a_non_object_document_is_a_no_op() {
        // A scalar / array document has no `params:` key; binding must not panic.
        let mut doc = json!(["${param.a}"]);
        let err = bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap_err();
        assert!(matches!(err, CliError::UnknownParamRef { .. }));
        let mut doc = json!(7);
        bind_document(&mut doc, &SuppliedParams::new(), BindMode::Strict).unwrap();
        assert_eq!(doc, json!(7));
    }
}
