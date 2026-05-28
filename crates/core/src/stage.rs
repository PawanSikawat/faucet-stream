//! Pipeline-level transform stages. A [`TransformStage`] wraps one of four
//! shapes:
//!
//! - [`TransformStage::Map`] holds an unchanged 1→1 [`RecordTransform`].
//! - [`TransformStage::Filter`] is a predicate-based 1→0|1 stage (added in
//!   Task 4).
//! - [`TransformStage::Explode`] expands an array field into 1→0..N output
//!   records (added in Task 5).
//! - [`TransformStage::Custom`] is an `Fn(Value) -> Vec<Value>` closure
//!   escape hatch for library callers.
//!
//! [`apply_stages`] is the per-record runner: it flat-maps stages left to
//! right, so order matters (a `Filter` after an `Explode` filters children).
//! The observability wrapper [`crate::observability::instrumented_apply_stages`]
//! calls this per record and aggregates the page-level counters.

use crate::error::FaucetError;
use crate::transform::{CompiledTransform, RecordTransform, compile as compile_record};
use serde_json::Value;
use std::sync::Arc;

/// One stage in a transform pipeline.
pub enum TransformStage {
    /// Existing 1→1 record transform. Wraps unchanged.
    Map(RecordTransform),
    /// Arbitrary 0..N closure for library callers (not addressable from YAML).
    Custom(Arc<dyn Fn(Value) -> Vec<Value> + Send + Sync>),
}

impl std::fmt::Debug for TransformStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Map(t) => f.debug_tuple("Map").field(t).finish(),
            Self::Custom(_) => write!(f, "Custom(<fn>)"),
        }
    }
}

impl Clone for TransformStage {
    fn clone(&self) -> Self {
        match self {
            Self::Map(t) => Self::Map(t.clone()),
            Self::Custom(f) => Self::Custom(Arc::clone(f)),
        }
    }
}

/// Pre-compiled stage. Per-record work is just lookup + comparison + flat-map.
pub enum CompiledStage {
    Map(CompiledTransform),
    Custom(Arc<dyn Fn(Value) -> Vec<Value> + Send + Sync>),
}

impl Clone for CompiledStage {
    fn clone(&self) -> Self {
        match self {
            Self::Map(t) => Self::Map(t.clone()),
            Self::Custom(f) => Self::Custom(Arc::clone(f)),
        }
    }
}

// ── JSONPath subset: bare key | $.dot.path | $['bracketed key'] ─────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathSegment {
    /// `.key` or `['key']` — addresses a JSON object field by name.
    Key(String),
}

/// Pre-parsed JSONPath. Bad syntax surfaces at `compile_stage` time, not per
/// record. The implementation is a hand-rolled parser for the v1 subset
/// (bare key, dot path, bracketed string key) — sufficient because we need
/// to walk parent-container chains for explode mutation, which the
/// `jsonpath_rust::query()` API doesn't expose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPath {
    /// Normalised string form (always `$`-rooted) for error messages.
    pub normalised: String,
    /// Parsed segments. Empty = the record itself.
    segments: Vec<PathSegment>,
}

impl CompiledPath {
    /// Parse a v1-subset path. Rejects wildcards (`[*]`), recursive descent
    /// (`..`), and filter expressions (`[?(...)]`) at compile time.
    pub fn compile(raw: &str) -> Result<Self, FaucetError> {
        let normalised = if raw.starts_with('$') {
            raw.to_owned()
        } else {
            format!("$.{raw}")
        };
        // Cheap rejection scan for v1-disallowed syntax. We check on the raw
        // input so that `$..foo` and `[*]` are caught before tokenising.
        if normalised.contains("[*]")
            || normalised.contains("..")
            || normalised.contains("[?")
        {
            return Err(FaucetError::Transform(format!(
                "invalid path '{raw}': only single-node paths are supported \
                 ('[*]', '..', '[?]' not allowed in this layer)"
            )));
        }
        let mut segments: Vec<PathSegment> = Vec::new();
        // Strip the leading `$`. The remainder is a series of `.<ident>` or
        // `['key']` chunks.
        let mut rest = &normalised[1..];
        while !rest.is_empty() {
            if let Some(after_dot) = rest.strip_prefix('.') {
                // Read an identifier up to the next `.` or `[`.
                let end = after_dot
                    .find(['.', '['])
                    .unwrap_or(after_dot.len());
                let key = &after_dot[..end];
                if key.is_empty() {
                    return Err(FaucetError::Transform(format!(
                        "invalid path '{raw}': empty segment after '.'"
                    )));
                }
                segments.push(PathSegment::Key(key.to_owned()));
                rest = &after_dot[end..];
            } else if let Some(after_open) = rest.strip_prefix('[') {
                // Expect ['key'] or ["key"].
                let quote = after_open.chars().next().ok_or_else(|| {
                    FaucetError::Transform(format!(
                        "invalid path '{raw}': unterminated bracket"
                    ))
                })?;
                if quote != '\'' && quote != '"' {
                    return Err(FaucetError::Transform(format!(
                        "invalid path '{raw}': bracket form requires a quoted key"
                    )));
                }
                let after_quote = &after_open[quote.len_utf8()..];
                let close = after_quote.find(quote).ok_or_else(|| {
                    FaucetError::Transform(format!(
                        "invalid path '{raw}': unterminated quoted key"
                    ))
                })?;
                let key = &after_quote[..close];
                let after_close_quote = &after_quote[close + quote.len_utf8()..];
                let after_close_bracket =
                    after_close_quote.strip_prefix(']').ok_or_else(|| {
                        FaucetError::Transform(format!(
                            "invalid path '{raw}': expected ']' after quoted key"
                        ))
                    })?;
                segments.push(PathSegment::Key(key.to_owned()));
                rest = after_close_bracket;
            } else {
                return Err(FaucetError::Transform(format!(
                    "invalid path '{raw}': unexpected character at '{rest}'"
                )));
            }
        }
        if segments.is_empty() {
            return Err(FaucetError::Transform(format!(
                "invalid path '{raw}': empty (must address a key)"
            )));
        }
        Ok(Self { normalised, segments })
    }

    /// All segments of the path.
    pub fn segments(&self) -> &[PathSegment] {
        &self.segments
    }

    /// Last segment's key (used for explode's default prefix).
    pub fn last_segment(&self) -> &str {
        match self.segments.last() {
            Some(PathSegment::Key(k)) => k,
            None => "",
        }
    }

    /// Return `(parent_segments, leaf_key)` for mutation. Top-level paths
    /// return `(&[], leaf)` — the parent is the record itself.
    pub fn parent_and_leaf(&self) -> (&[PathSegment], &str) {
        let n = self.segments.len();
        let parent = &self.segments[..n - 1];
        let leaf = match &self.segments[n - 1] {
            PathSegment::Key(k) => k.as_str(),
        };
        (parent, leaf)
    }

    /// Resolve the path against a value. Returns `Ok(None)` for missing
    /// (key absent or walking through a non-object); `Ok(Some(&v))` if found.
    pub fn resolve<'a>(&self, value: &'a Value) -> Result<Option<&'a Value>, FaucetError> {
        Self::resolve_segments(value, &self.segments)
    }

    /// Resolve an explicit slice of segments — used to walk the parent
    /// container chain returned by `parent_and_leaf`.
    pub fn resolve_segments<'a>(
        value: &'a Value,
        segments: &[PathSegment],
    ) -> Result<Option<&'a Value>, FaucetError> {
        let mut cur = value;
        for seg in segments {
            let PathSegment::Key(k) = seg;
            match cur {
                Value::Object(map) => match map.get(k) {
                    Some(v) => cur = v,
                    None => return Ok(None),
                },
                _ => return Ok(None),
            }
        }
        Ok(Some(cur))
    }

    /// Resolve a path that must end at an `Object` container, returning a
    /// mutable reference. Used by explode to mutate the parent container.
    /// Returns `Ok(None)` if any intermediate is missing or non-object.
    pub fn resolve_segments_mut<'a>(
        value: &'a mut Value,
        segments: &[PathSegment],
    ) -> Result<Option<&'a mut serde_json::Map<String, Value>>, FaucetError> {
        let mut cur = value;
        for seg in segments {
            let PathSegment::Key(k) = seg;
            match cur {
                Value::Object(map) => match map.get_mut(k) {
                    Some(v) => cur = v,
                    None => return Ok(None),
                },
                _ => return Ok(None),
            }
        }
        match cur {
            Value::Object(map) => Ok(Some(map)),
            _ => Ok(None),
        }
    }
}

/// Compile a [`TransformStage`] into its [`CompiledStage`] form.
pub fn compile_stage(s: &TransformStage) -> Result<CompiledStage, FaucetError> {
    match s {
        TransformStage::Map(t) => Ok(CompiledStage::Map(compile_record(t)?)),
        TransformStage::Custom(f) => Ok(CompiledStage::Custom(Arc::clone(f))),
    }
}

/// Per-record stage runner. Returns 0..N output records. Pure; no metrics.
pub fn apply_stages(
    rec: Value,
    stages: &[CompiledStage],
) -> Result<Vec<Value>, FaucetError> {
    let mut acc = vec![rec];
    for stage in stages {
        let mut next: Vec<Value> = Vec::with_capacity(acc.len());
        for r in acc {
            next.extend(apply_one_stage(r, stage)?);
        }
        acc = next;
    }
    Ok(acc)
}

fn apply_one_stage(rec: Value, stage: &CompiledStage) -> Result<Vec<Value>, FaucetError> {
    match stage {
        CompiledStage::Map(t) => Ok(vec![crate::transform::apply_all(rec, std::slice::from_ref(t))?]),
        CompiledStage::Custom(f) => Ok(f(rec)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::KeyCaseMode;
    use serde_json::json;

    fn compile(stages: &[TransformStage]) -> Vec<CompiledStage> {
        stages.iter().map(compile_stage).collect::<Result<_, _>>().unwrap()
    }

    #[test]
    fn map_round_trip_with_keys_case() {
        let compiled = compile(&[TransformStage::Map(RecordTransform::KeysCase {
            mode: KeyCaseMode::Snake,
        })]);
        let out = apply_stages(json!({"FooBar": 1}), &compiled).unwrap();
        assert_eq!(out, vec![json!({"foo_bar": 1})]);
    }

    #[test]
    fn empty_stage_list_is_identity() {
        let out = apply_stages(json!({"a": 1}), &[]).unwrap();
        assert_eq!(out, vec![json!({"a": 1})]);
    }

    #[test]
    fn custom_closure_can_drop_and_multiply() {
        // 0-output closure
        let drop_all: Arc<dyn Fn(Value) -> Vec<Value> + Send + Sync> =
            Arc::new(|_| vec![]);
        let stages = vec![CompiledStage::Custom(drop_all)];
        assert_eq!(apply_stages(json!({"a": 1}), &stages).unwrap(), Vec::<Value>::new());

        // N-output closure
        let multiply: Arc<dyn Fn(Value) -> Vec<Value> + Send + Sync> =
            Arc::new(|v| vec![v.clone(), v.clone(), v]);
        let stages = vec![CompiledStage::Custom(multiply)];
        assert_eq!(apply_stages(json!({"a": 1}), &stages).unwrap().len(), 3);
    }

    // ── CompiledPath syntax ──

    #[test]
    fn path_bare_key_normalises_to_dollar_dot() {
        let p = CompiledPath::compile("status").expect("bare key compiles");
        assert_eq!(p.segments(), &[PathSegment::Key("status".to_owned())]);
    }

    #[test]
    fn path_dot_path() {
        let p = CompiledPath::compile("$.user.status").expect("dot path compiles");
        assert_eq!(
            p.segments(),
            &[
                PathSegment::Key("user".to_owned()),
                PathSegment::Key("status".to_owned()),
            ]
        );
    }

    #[test]
    fn path_bracketed_key_allows_dots_and_dashes() {
        let p = CompiledPath::compile("$['foo.bar']").expect("bracket form compiles");
        assert_eq!(p.segments(), &[PathSegment::Key("foo.bar".to_owned())]);

        let p2 = CompiledPath::compile("$['order-lines']").expect("dashes ok");
        assert_eq!(p2.segments(), &[PathSegment::Key("order-lines".to_owned())]);
    }

    #[test]
    fn path_rejects_wildcards() {
        for p in &["$.items[*]", "$..items", "$.items[?(@.x)]"] {
            let err = CompiledPath::compile(p).expect_err(p);
            let msg = format!("{err}");
            assert!(
                msg.contains("only single-node paths") || msg.contains("not allowed"),
                "expected v1-syntax error for {p}, got: {msg}"
            );
        }
    }

    #[test]
    fn path_resolve_value_top_level() {
        let p = CompiledPath::compile("status").unwrap();
        let rec = json!({"status": "active", "other": 1});
        assert_eq!(p.resolve(&rec).unwrap(), Some(&json!("active")));
    }

    #[test]
    fn path_resolve_value_nested() {
        let p = CompiledPath::compile("$.user.status").unwrap();
        let rec = json!({"user": {"status": "active"}});
        assert_eq!(p.resolve(&rec).unwrap(), Some(&json!("active")));
    }

    #[test]
    fn path_resolve_missing_returns_none() {
        let p = CompiledPath::compile("$.nope").unwrap();
        let rec = json!({"x": 1});
        assert_eq!(p.resolve(&rec).unwrap(), None);
    }

    #[test]
    fn path_resolve_through_non_object_returns_none() {
        // "$.user.status" against {"user": 1} — user isn't an object.
        let p = CompiledPath::compile("$.user.status").unwrap();
        assert_eq!(p.resolve(&json!({"user": 1})).unwrap(), None);
    }

    #[test]
    fn path_parent_container_top_level() {
        // For top-level paths, parent is the record itself.
        let p = CompiledPath::compile("items").unwrap();
        let rec = json!({"id": 1, "items": [1]});
        let (parent, leaf) = p.parent_and_leaf();
        assert_eq!(parent, &[] as &[PathSegment]);
        assert_eq!(leaf, "items");
        // Resolving the parent on the record should give the record back.
        assert_eq!(
            CompiledPath::resolve_segments(&rec, parent).unwrap(),
            Some(&rec)
        );
    }

    #[test]
    fn path_parent_container_nested() {
        let p = CompiledPath::compile("$.user.items").unwrap();
        let rec = json!({"user": {"items": [1]}});
        let (parent, leaf) = p.parent_and_leaf();
        assert_eq!(parent, &[PathSegment::Key("user".to_owned())]);
        assert_eq!(leaf, "items");
        assert_eq!(
            CompiledPath::resolve_segments(&rec, parent).unwrap(),
            Some(&json!({"items": [1]}))
        );
    }

    #[test]
    fn path_last_segment_for_prefix_defaulting() {
        assert_eq!(CompiledPath::compile("items").unwrap().last_segment(), "items");
        assert_eq!(
            CompiledPath::compile("$.user.items").unwrap().last_segment(),
            "items"
        );
        assert_eq!(
            CompiledPath::compile("$['order-lines']").unwrap().last_segment(),
            "order-lines"
        );
    }
}
