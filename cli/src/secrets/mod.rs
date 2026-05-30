//! Secrets-manager interpolation for the config layer (#125).
//!
//! Resolution runs as the final config-load stage (after env/file and
//! vars/templates), over the parsed config tree. See the design spec.

pub mod registry;

#[cfg(feature = "secrets-aws-sm")]
mod aws_sm;
#[cfg(feature = "secrets-azure-kv")]
mod azure_kv;
#[cfg(feature = "secrets-gcp-sm")]
mod gcp_sm;
#[cfg(feature = "secrets-vault")]
mod vault;

use crate::error::{CliError, CliResult};
use crate::interpolate::{self, Directive};
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};

/// The four secret-manager schemes this layer recognises.
pub const SECRET_SCHEMES: &[&str] = &["vault", "aws-sm", "gcp-sm", "azure-kv"];

/// A `(scheme, reference)` pair, e.g. `("vault", "secret/data/app#token")`.
pub type SecretRef = (String, String);

#[async_trait]
pub trait SecretResolver: Send + Sync {
    /// Scheme handled, e.g. `"vault"`.
    fn scheme(&self) -> &'static str;
    /// Resolve a `path[#field]` reference to its string value.
    async fn resolve(&self, reference: &str) -> CliResult<String>;
}

/// Split a `path#field` reference into `(path, Some(field))` or `(path, None)`.
#[allow(dead_code)] // used by provider modules added in later tasks
pub(crate) fn split_field(reference: &str) -> (&str, Option<&str>) {
    match reference.split_once('#') {
        Some((path, field)) => (path, Some(field)),
        None => (reference, None),
    }
}

/// Extract `field` from a secret body that must parse as a JSON object.
/// Used by Vault and AWS resolvers for the `#field` selector.
#[allow(dead_code)] // used by provider modules added in later tasks
pub(crate) fn extract_field(
    scheme: &str,
    reference: &str,
    body: &str,
    field: &str,
) -> CliResult<String> {
    let json: Value = serde_json::from_str(body).map_err(|_| CliError::SecretNotJson {
        scheme: scheme.to_owned(),
        reference: reference.to_owned(),
    })?;
    let obj = json.as_object().ok_or_else(|| CliError::SecretNotJson {
        scheme: scheme.to_owned(),
        reference: reference.to_owned(),
    })?;
    match obj.get(field) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(other) => Ok(other.to_string()),
        None => Err(CliError::SecretFieldMissing {
            scheme: scheme.to_owned(),
            reference: reference.to_owned(),
            field: field.to_owned(),
            available: obj.keys().cloned().collect(),
        }),
    }
}

/// Apply `f` to every string leaf in `value`, recursively.
fn for_each_string<F: FnMut(&str)>(value: &Value, f: &mut F) {
    match value {
        Value::String(s) => f(s),
        Value::Array(a) => a.iter().for_each(|v| for_each_string(v, f)),
        Value::Object(m) => m.values().for_each(|v| for_each_string(v, f)),
        _ => {}
    }
}

/// Mutate every string leaf in `value`, recursively.
fn for_each_string_mut<F: FnMut(&mut String) -> CliResult<()>>(
    value: &mut Value,
    f: &mut F,
) -> CliResult<()> {
    match value {
        Value::String(s) => f(s),
        Value::Array(a) => a.iter_mut().try_for_each(|v| for_each_string_mut(v, f)),
        Value::Object(m) => m.values_mut().try_for_each(|v| for_each_string_mut(v, f)),
        _ => Ok(()),
    }
}

/// Collect every unique secret reference found in a single string.
fn collect_refs_in_str(s: &str, out: &mut BTreeSet<SecretRef>) {
    for (_token, dir) in interpolate::iter_directives(s) {
        if let Directive::LoadTime { prefix, body } = dir
            && SECRET_SCHEMES.contains(&prefix)
        {
            out.insert((prefix.to_owned(), body.to_owned()));
        }
    }
}

/// Collect all unique secret references reachable from a config `Value`.
pub fn collect_refs(value: &Value, out: &mut BTreeSet<SecretRef>) {
    for_each_string(value, &mut |s| collect_refs_in_str(s, out));
}

/// Substitute every secret directive in a `Value` from `cache`. Non-secret
/// directives (`${users.id}`) pass through verbatim.
pub fn substitute(value: &mut Value, cache: &HashMap<SecretRef, String>) -> CliResult<()> {
    for_each_string_mut(value, &mut |s| {
        let new = interpolate::rewrite(s, |body| match interpolate::classify_directive(body) {
            Directive::LoadTime { prefix, body: b } if SECRET_SCHEMES.contains(&prefix) => Ok(
                Some(
                    cache
                        .get(&(prefix.to_owned(), b.to_owned()))
                        .cloned()
                        .expect("scan collected every secret ref before fetch"),
                ),
            ),
            _ => Ok(None),
        })?;
        *s = new;
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn collects_unique_refs_and_ignores_other_directives() {
        let v = json!({
            "a": "${vault:secret/data/app#token}",
            "b": "${aws-sm:prod/db#password}",
            "c": "${vault:secret/data/app#token}",
            "d": "${users.id}",
            "e": "${env:HOME}",
            "nested": ["${gcp-sm:projects/p/secrets/s/versions/latest}"]
        });
        let mut refs = BTreeSet::new();
        collect_refs(&v, &mut refs);
        assert_eq!(refs.len(), 3);
        assert!(refs.contains(&("vault".into(), "secret/data/app#token".into())));
        assert!(refs.contains(&("aws-sm".into(), "prod/db#password".into())));
        assert!(refs.contains(&(
            "gcp-sm".into(),
            "projects/p/secrets/s/versions/latest".into()
        )));
    }

    #[test]
    fn substitutes_from_cache_and_preserves_runtime_refs() {
        let mut v = json!({
            "token": "Bearer ${vault:secret/data/app#token}",
            "path": "/v1/${users.id}"
        });
        let mut cache = HashMap::new();
        cache.insert(("vault".into(), "secret/data/app#token".into()), "abc123".into());
        substitute(&mut v, &cache).unwrap();
        assert_eq!(v["token"], "Bearer abc123");
        assert_eq!(v["path"], "/v1/${users.id}");
    }

    #[test]
    fn extract_field_picks_key_or_errors_with_available() {
        let body = r#"{"username":"u","password":"p"}"#;
        assert_eq!(extract_field("aws-sm", "ref", body, "password").unwrap(), "p");
        match extract_field("aws-sm", "ref", body, "missing").unwrap_err() {
            CliError::SecretFieldMissing { available, .. } => {
                assert!(available.contains(&"username".to_string()));
            }
            other => panic!("expected SecretFieldMissing, got {other:?}"),
        }
        match extract_field("aws-sm", "ref", "not json", "x").unwrap_err() {
            CliError::SecretNotJson { .. } => {}
            other => panic!("expected SecretNotJson, got {other:?}"),
        }
    }
}
