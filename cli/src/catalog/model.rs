//! Pure helpers for building catalog observations (#279): dataset-URI
//! canonicalization (fold `${now.*}`-derived segments back to their tokens,
//! redact credentials) and schema inference over the run's record samples.

use chrono::{DateTime, FixedOffset};
use serde_json::Value;

/// Canonicalize a concrete dataset URI for use as the catalog identity key.
///
/// 1. **`${now.*}` folding** — a dated path like `./out/dt=2026-07-06/x.jsonl`
///    produced by a `${now.date}` template would otherwise mint a new catalog
///    dataset every day (the cardinality trap called out in #279). For every
///    `${now.*}` token that appears in the connector's *raw* (pre-resolution)
///    config, the token's rendered value is replaced back with the token text
///    in the URI, so all runs of the template converge on one dataset.
/// 2. **Credential redaction** — via `faucet_core::redact_uri_credentials`,
///    so a `postgres://user:pass@host/db` URI never lands in the store.
pub fn canonicalize_uri(uri: &str, raw_config: &Value, clock: DateTime<FixedOffset>) -> String {
    let mut tokens: Vec<String> = Vec::new();
    collect_now_tokens(raw_config, &mut tokens);
    tokens.sort();
    tokens.dedup();

    // Render each token with the same clock the run used, then substitute the
    // rendered text back to the token — longest rendering first, so e.g.
    // `${now.datetime}` wins over the `${now.year}` embedded within it.
    let mut pairs: Vec<(String, String)> = tokens
        .into_iter()
        .filter_map(|t| {
            crate::interpolate::resolve_now(&t, clock)
                .ok()
                .filter(|rendered| !rendered.is_empty() && rendered != &t)
                .map(|rendered| (rendered, t))
        })
        .collect();
    pairs.sort_by_key(|(rendered, _)| std::cmp::Reverse(rendered.len()));

    let mut out = uri.to_string();
    for (rendered, token) in pairs {
        out = out.replace(&rendered, &token);
    }
    faucet_core::redact_uri_credentials(&out)
}

/// Collect every `${now.…}` token appearing in the string values of `v`.
fn collect_now_tokens(v: &Value, out: &mut Vec<String>) {
    match v {
        Value::String(s) => {
            let mut rest = s.as_str();
            while let Some(start) = rest.find("${now.") {
                let tail = &rest[start..];
                match tail.find('}') {
                    Some(end) => {
                        out.push(tail[..=end].to_string());
                        rest = &tail[end + 1..];
                    }
                    None => break,
                }
            }
        }
        Value::Array(items) => items.iter().for_each(|i| collect_now_tokens(i, out)),
        Value::Object(map) => map.values().for_each(|i| collect_now_tokens(i, out)),
        _ => {}
    }
}

/// Infer an `infer_schema`-shaped record schema from a run's sample, `None`
/// when nothing was sampled (empty run, or non-object records only).
pub fn schema_from_samples(samples: &[Value]) -> Option<Value> {
    if samples.is_empty() {
        return None;
    }
    let schema = faucet_core::schema::infer_schema(samples);
    // A sample of non-object records infers no properties — not a schema
    // worth a timeline entry.
    schema
        .get("properties")
        .and_then(Value::as_object)
        .is_some_and(|p| !p.is_empty())
        .then_some(schema)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn clock() -> DateTime<FixedOffset> {
        DateTime::parse_from_rfc3339("2026-07-06T10:30:00Z").unwrap()
    }

    #[test]
    fn folds_now_tokens_back_into_the_uri() {
        let raw = json!({"path": "./out/dt=${now.date}/part.jsonl"});
        let uri = canonicalize_uri("file://./out/dt=2026-07-06/part.jsonl", &raw, clock());
        assert_eq!(uri, "file://./out/dt=${now.date}/part.jsonl");
    }

    #[test]
    fn folds_strftime_tokens_and_ignores_unused_ones() {
        let raw = json!({"path": "./out/${now.strftime.%Y/%m}/x.jsonl", "other": "${now.unix}"});
        let uri = canonicalize_uri("file://./out/2026/07/x.jsonl", &raw, clock());
        assert_eq!(uri, "file://./out/${now.strftime.%Y/%m}/x.jsonl");
    }

    #[test]
    fn without_now_tokens_the_uri_is_untouched_except_redaction() {
        let raw = json!({"connection_url": "postgres://u:pw@h:5432/db"});
        let uri = canonicalize_uri("postgres://u:pw@h:5432/db/public.users", &raw, clock());
        assert!(!uri.contains("pw"), "credentials must be redacted: {uri}");
        assert!(uri.contains("h:5432"));
    }

    #[test]
    fn collect_finds_multiple_tokens_in_one_string() {
        let mut out = Vec::new();
        collect_now_tokens(&json!("a-${now.year}-${now.month}-b"), &mut out);
        assert_eq!(out, vec!["${now.year}", "${now.month}"]);
        // Unterminated token is ignored, not a panic.
        out.clear();
        collect_now_tokens(&json!("broken ${now.date"), &mut out);
        assert!(out.is_empty());
        // Tokens inside arrays (e.g. a list-valued config field) are found too.
        out.clear();
        collect_now_tokens(&json!({"paths": ["x", "${now.date}"]}), &mut out);
        assert_eq!(out, vec!["${now.date}"]);
    }

    #[test]
    fn schema_from_samples_requires_object_records() {
        assert!(schema_from_samples(&[]).is_none());
        assert!(schema_from_samples(&[json!("scalar")]).is_none());
        let schema = schema_from_samples(&[json!({"id": 1, "name": "a"})]).unwrap();
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["id"].is_object());
    }
}
