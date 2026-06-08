//! Pipeline-level transform stages. A [`TransformStage`] wraps one of five
//! shapes:
//!
//! - [`TransformStage::Map`] holds an unchanged 1→1 [`RecordTransform`].
//! - [`TransformStage::Filter`] is a predicate-based 1→0|1 stage (added in
//!   Task 4).
//! - [`TransformStage::Explode`] expands an array field into 1→0..N output
//!   records (added in Task 5).
//! - [`TransformStage::Custom`] is an `Fn(Value) -> Vec<Value>` closure
//!   escape hatch for library callers.
//! - [`TransformStage::PageFn`] is a `Fn(Vec<Value>) -> Result<Vec<Value>,
//!   FaucetError>` whole-batch closure for transforms that need the full page
//!   at once (SQL, sort, dedup, top-N). Not addressable from YAML.
//!
//! [`apply_stages_to_page`] is the page-granular runner: per-record stages
//! (`Map`/`Filter`/`Explode`/`Custom`) flat-map over each record in order,
//! while `PageFn` stages receive and return the whole page slice. Declared
//! order is preserved, so `filter → sql → select` works as expected.
//! The observability wrapper [`crate::observability::instrumented_apply_stages`]
//! wraps `apply_stages_to_page` and aggregates per-page metrics.

use crate::error::FaucetError;
use crate::transform::{CompiledTransform, RecordTransform, compile as compile_record};
use serde_json::Value;
use std::sync::Arc;

/// Type alias for the page-level transform closure stored in
/// [`TransformStage::PageFn`] and [`CompiledStage::PageFn`].
pub type PageFnBox =
    Arc<dyn Fn(Vec<Value>) -> Result<Vec<Value>, FaucetError> + Send + Sync>;

/// One stage in a transform pipeline.
pub enum TransformStage {
    /// Existing 1→1 record transform. Wraps unchanged.
    Map(RecordTransform),
    /// Predicate-based 1→0|1 stage. Keeps the record iff the predicate
    /// evaluates true.
    #[cfg(feature = "transform-filter")]
    Filter(FilterSpec),
    /// Array-expansion 1→0..N stage. Fans the record out once per element
    /// of the targeted array; object elements are merged into the parent
    /// container with a prefix, scalars/arrays replace the leaf in place.
    #[cfg(feature = "transform-explode")]
    Explode(ExplodeSpec),
    /// Arbitrary 0..N closure for library callers (not addressable from YAML).
    Custom(Arc<dyn Fn(Value) -> Vec<Value> + Send + Sync>),
    /// Page-level closure: sees the whole page, returns a new page; fallible.
    /// The escape hatch for transforms that need the full batch at once (SQL,
    /// sort, dedup, top-N). Dependency-free; built by connectors/CLI, not
    /// addressable as a per-record stage.
    PageFn(PageFnBox),
}

impl std::fmt::Debug for TransformStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Map(t) => f.debug_tuple("Map").field(t).finish(),
            #[cfg(feature = "transform-filter")]
            Self::Filter(s) => f.debug_tuple("Filter").field(s).finish(),
            #[cfg(feature = "transform-explode")]
            Self::Explode(s) => f.debug_tuple("Explode").field(s).finish(),
            Self::Custom(_) => write!(f, "Custom(<fn>)"),
            Self::PageFn(_) => write!(f, "PageFn(<fn>)"),
        }
    }
}

impl Clone for TransformStage {
    fn clone(&self) -> Self {
        match self {
            Self::Map(t) => Self::Map(t.clone()),
            #[cfg(feature = "transform-filter")]
            Self::Filter(s) => Self::Filter(s.clone()),
            #[cfg(feature = "transform-explode")]
            Self::Explode(s) => Self::Explode(s.clone()),
            Self::Custom(f) => Self::Custom(Arc::clone(f)),
            Self::PageFn(f) => Self::PageFn(Arc::clone(f)),
        }
    }
}

/// Pre-compiled stage. Per-record work is just lookup + comparison + flat-map.
pub enum CompiledStage {
    Map(CompiledTransform),
    #[cfg(feature = "transform-filter")]
    Filter(CompiledFilter),
    #[cfg(feature = "transform-explode")]
    Explode(CompiledExplode),
    Custom(Arc<dyn Fn(Value) -> Vec<Value> + Send + Sync>),
    PageFn(PageFnBox),
}

impl std::fmt::Debug for CompiledStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Map(_) => write!(f, "Map(<compiled>)"),
            #[cfg(feature = "transform-filter")]
            Self::Filter(cf) => f.debug_tuple("Filter").field(cf).finish(),
            #[cfg(feature = "transform-explode")]
            Self::Explode(e) => f.debug_tuple("Explode").field(e).finish(),
            Self::Custom(_) => write!(f, "Custom(<fn>)"),
            Self::PageFn(_) => write!(f, "PageFn(<fn>)"),
        }
    }
}

impl Clone for CompiledStage {
    fn clone(&self) -> Self {
        match self {
            Self::Map(t) => Self::Map(t.clone()),
            #[cfg(feature = "transform-filter")]
            Self::Filter(f) => Self::Filter(CompiledFilter {
                path: f.path.clone(),
                op: f.op,
                value: f.value.clone(),
            }),
            #[cfg(feature = "transform-explode")]
            Self::Explode(e) => Self::Explode(CompiledExplode {
                path: e.path.clone(),
                prefix: e.prefix.clone(),
                separator: e.separator.clone(),
                on_missing: e.on_missing,
            }),
            Self::Custom(f) => Self::Custom(Arc::clone(f)),
            Self::PageFn(f) => Self::PageFn(Arc::clone(f)),
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
        // Helper: the canonical "v1 doesn't support this syntax" error.
        // Wildcards (`[*]`), recursive descent (`..`), and filters (`[?...]`)
        // are rejected at the *syntax* level — but those substrings can
        // legitimately appear *inside* a quoted bracket key (e.g.
        // `$['a..b']`, `$['x[*]y']`), which is the documented escape hatch
        // for unusual keys. So we only reject them when they appear as
        // structural syntax during tokenisation, not as content.
        let v1_syntax_err = || {
            FaucetError::Transform(format!(
                "invalid path '{raw}': only single-node paths are supported \
                 ('[*]', '..', '[?]' not allowed in this layer)"
            ))
        };
        let mut segments: Vec<PathSegment> = Vec::new();
        // Strip the leading `$`. The remainder is a series of `.<ident>` or
        // `['key']` chunks.
        let mut rest = &normalised[1..];
        while !rest.is_empty() {
            if let Some(after_dot) = rest.strip_prefix('.') {
                // Recursive descent `..` shows up here as an empty identifier
                // (the next char is another `.`). Reject with the v1 message.
                if after_dot.starts_with('.') {
                    return Err(v1_syntax_err());
                }
                // Read an identifier up to the next `.` or `[`.
                let end = after_dot.find(['.', '[']).unwrap_or(after_dot.len());
                let key = &after_dot[..end];
                if key.is_empty() {
                    return Err(FaucetError::Transform(format!(
                        "invalid path '{raw}': empty segment after '.'"
                    )));
                }
                segments.push(PathSegment::Key(key.to_owned()));
                rest = &after_dot[end..];
            } else if let Some(after_open) = rest.strip_prefix('[') {
                // Expect ['key'] or ["key"]. A wildcard (`[*]`) or filter
                // (`[?...]`) shows up here as an unquoted next-char — catch
                // those with the v1 message before the generic "needs a
                // quoted key" error.
                let next = after_open.chars().next().ok_or_else(|| {
                    FaucetError::Transform(format!("invalid path '{raw}': unterminated bracket"))
                })?;
                if next == '*' || next == '?' {
                    return Err(v1_syntax_err());
                }
                if next != '\'' && next != '"' {
                    return Err(FaucetError::Transform(format!(
                        "invalid path '{raw}': bracket form requires a quoted key"
                    )));
                }
                let quote = next;
                let after_quote = &after_open[quote.len_utf8()..];
                let close = after_quote.find(quote).ok_or_else(|| {
                    FaucetError::Transform(format!("invalid path '{raw}': unterminated quoted key"))
                })?;
                let key = &after_quote[..close];
                let after_close_quote = &after_quote[close + quote.len_utf8()..];
                let after_close_bracket = after_close_quote.strip_prefix(']').ok_or_else(|| {
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
        Ok(Self {
            normalised,
            segments,
        })
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

// ── Filter spec ──

/// Predicate spec for [`TransformStage::Filter`]. Compiled into a
/// [`CompiledFilter`] at pipeline-build time; per-record work is a single
/// path resolve + comparison.
#[cfg(feature = "transform-filter")]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct FilterSpec {
    /// JSONPath subset (bare key, dot path, bracketed string key).
    pub path: String,
    /// Comparison operator.
    pub op: FilterOp,
    /// Required for `eq`, `ne`, `in`, `not_in`. Forbidden for `exists`.
    /// For `in` / `not_in`, must be a JSON array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<Value>,
}

/// Comparison operator for [`FilterSpec`].
#[cfg(feature = "transform-filter")]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum FilterOp {
    /// `path == value` (JSON equality; missing path → false).
    Eq,
    /// `path != value` (JSON inequality; missing path → true).
    Ne,
    /// `path` is present and non-null.
    Exists,
    /// `path`'s value is a member of `value` (must be an array).
    /// Missing path → false.
    In,
    /// `path`'s value is NOT a member of `value` (must be an array).
    /// Missing path → true.
    NotIn,
}

#[cfg(feature = "transform-filter")]
impl std::fmt::Display for FilterOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            FilterOp::Eq => "eq",
            FilterOp::Ne => "ne",
            FilterOp::Exists => "exists",
            FilterOp::In => "in",
            FilterOp::NotIn => "not_in",
        })
    }
}

/// Pre-compiled filter: path parsed once, value cloned once.
///
/// `pub` (rather than the spec's `pub(crate)`) so it can sit inside the
/// `pub` [`CompiledStage::Filter`] variant without tripping the
/// `private_interfaces` lint — same shape as the already-`pub`
/// [`CompiledTransform`] sibling that backs [`CompiledStage::Map`].
#[cfg(feature = "transform-filter")]
#[derive(Debug)]
pub struct CompiledFilter {
    pub path: CompiledPath,
    pub op: FilterOp,
    pub value: Option<Value>,
}

#[cfg(feature = "transform-filter")]
impl CompiledFilter {
    fn compile(spec: &FilterSpec) -> Result<Self, FaucetError> {
        // Validate op/value combo at compile time so bad configs fail fast.
        match spec.op {
            FilterOp::Exists => {
                if spec.value.is_some() {
                    return Err(FaucetError::Transform(
                        "filter: op 'exists' must not have a `value`".to_owned(),
                    ));
                }
            }
            FilterOp::Eq | FilterOp::Ne => {
                if spec.value.is_none() {
                    return Err(FaucetError::Transform(format!(
                        "filter: op '{}' requires a `value`",
                        spec.op
                    )));
                }
            }
            FilterOp::In | FilterOp::NotIn => {
                if !matches!(spec.value, Some(Value::Array(_))) {
                    return Err(FaucetError::Transform(format!(
                        "filter: op '{}' requires an array `value`",
                        spec.op
                    )));
                }
            }
        }
        let path = CompiledPath::compile(&spec.path).map_err(|e| match e {
            FaucetError::Transform(msg) => FaucetError::Transform(format!("filter: {msg}")),
            other => other,
        })?;
        Ok(Self {
            path,
            op: spec.op,
            value: spec.value.clone(),
        })
    }

    fn evaluate(&self, rec: &Value) -> Result<bool, FaucetError> {
        let resolved = self.path.resolve(rec)?;
        Ok(match self.op {
            FilterOp::Eq => resolved
                .map(|v| v == self.value.as_ref().unwrap())
                .unwrap_or(false),
            FilterOp::Ne => resolved
                .map(|v| v != self.value.as_ref().unwrap())
                .unwrap_or(true), // missing → keep
            FilterOp::Exists => matches!(resolved, Some(v) if !v.is_null()),
            FilterOp::In => match resolved {
                Some(v) => {
                    let arr = self
                        .value
                        .as_ref()
                        .unwrap()
                        .as_array()
                        .expect("compile validated");
                    arr.contains(v)
                }
                None => false,
            },
            FilterOp::NotIn => match resolved {
                Some(v) => {
                    let arr = self
                        .value
                        .as_ref()
                        .unwrap()
                        .as_array()
                        .expect("compile validated");
                    !arr.contains(v)
                }
                None => true, // missing → keep
            },
        })
    }
}

// ── Explode spec ──

/// Spec for [`TransformStage::Explode`]. Fans one record out into N records
/// based on the array at `path`. Compiled into a [`CompiledExplode`] at
/// pipeline-build time; per-record work is one path resolve + N clones +
/// N merges.
#[cfg(feature = "transform-explode")]
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ExplodeSpec {
    /// JSONPath subset (bare key, dot path, bracketed string key).
    pub path: String,
    /// Prefix prepended to each element field when the element is an object.
    /// Defaults (when `None`) to the last segment of `path`. Empty string =
    /// no prefix (pure LATERAL FLATTEN).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Separator between prefix and each element field. Default `"_"`.
    #[serde(default = "default_explode_separator")]
    pub separator: String,
    /// What to do when `path` doesn't yield a non-empty array.
    #[serde(default)]
    pub on_missing: OnMissing,
}

/// Behaviour when an [`ExplodeSpec`]'s `path` doesn't yield a non-empty
/// array. The default is `Passthrough` because silently dropping records
/// is the worst failure mode for ETL pipelines — surfacing the record
/// unchanged lets downstream stages decide.
#[cfg(feature = "transform-explode")]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Default,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum OnMissing {
    /// Emit the original record unchanged.
    #[default]
    Passthrough,
    /// Emit zero records.
    Drop,
    /// Return [`FaucetError::Transform`].
    Error,
}

#[cfg(feature = "transform-explode")]
fn default_explode_separator() -> String {
    "_".to_owned()
}

/// Pre-compiled explode: path parsed once, prefix resolved once.
///
/// `pub` (rather than the spec's `pub(crate)`) so it can sit inside the
/// `pub` [`CompiledStage::Explode`] variant without tripping the
/// `private_interfaces` lint — same shape as the already-`pub`
/// [`CompiledFilter`] sibling that backs [`CompiledStage::Filter`].
#[cfg(feature = "transform-explode")]
#[derive(Debug)]
pub struct CompiledExplode {
    pub path: CompiledPath,
    pub prefix: String,
    pub separator: String,
    pub on_missing: OnMissing,
}

#[cfg(feature = "transform-explode")]
impl CompiledExplode {
    fn compile(spec: &ExplodeSpec) -> Result<Self, FaucetError> {
        let path = CompiledPath::compile(&spec.path).map_err(|e| match e {
            FaucetError::Transform(msg) => FaucetError::Transform(format!("explode: {msg}")),
            other => other,
        })?;
        let prefix = match &spec.prefix {
            Some(p) => p.clone(),
            None => path.last_segment().to_owned(),
        };
        Ok(Self {
            path,
            prefix,
            separator: spec.separator.clone(),
            on_missing: spec.on_missing,
        })
    }

    fn apply(&self, rec: Value) -> Result<Vec<Value>, FaucetError> {
        // Resolve the value at `path`. If it's a non-empty array, fan out.
        // Otherwise, route through `on_missing`.
        let target = self.path.resolve(&rec)?;
        let Some(Value::Array(elements)) = target.cloned() else {
            return self.handle_missing(rec);
        };
        if elements.is_empty() {
            return self.handle_missing(rec);
        }
        let (parent_segments, leaf) = self.path.parent_and_leaf();
        let parent_segments = parent_segments.to_vec();
        let leaf = leaf.to_owned();
        let mut out: Vec<Value> = Vec::with_capacity(elements.len());
        for element in elements {
            let mut child = rec.clone();
            self.merge_one(&mut child, &parent_segments, &leaf, element)?;
            out.push(child);
        }
        Ok(out)
    }

    fn handle_missing(&self, rec: Value) -> Result<Vec<Value>, FaucetError> {
        match self.on_missing {
            OnMissing::Passthrough => Ok(vec![rec]),
            OnMissing::Drop => Ok(vec![]),
            OnMissing::Error => Err(FaucetError::Transform(format!(
                "explode: path '{}' did not yield a non-empty array",
                self.path.normalised
            ))),
        }
    }

    fn merge_one(
        &self,
        record: &mut Value,
        parent_segments: &[PathSegment],
        leaf: &str,
        element: Value,
    ) -> Result<(), FaucetError> {
        let Some(parent_map) = CompiledPath::resolve_segments_mut(record, parent_segments)? else {
            return Err(FaucetError::Transform(format!(
                "explode: parent container at '{}' unexpectedly missing during merge",
                self.path.normalised
            )));
        };
        match element {
            Value::Object(elem_map) => {
                parent_map.remove(leaf);
                for (k, v) in elem_map {
                    let new_key = if self.prefix.is_empty() {
                        k
                    } else {
                        format!("{}{}{}", self.prefix, self.separator, k)
                    };
                    if parent_map.contains_key(&new_key) {
                        return Err(FaucetError::Transform(format!(
                            "explode produced duplicate key '{new_key}'"
                        )));
                    }
                    parent_map.insert(new_key, v);
                }
            }
            other => {
                // Scalar / null / array — replace in place.
                parent_map.insert(leaf.to_owned(), other);
            }
        }
        Ok(())
    }
}

/// Compile a [`TransformStage`] into its [`CompiledStage`] form.
pub fn compile_stage(s: &TransformStage) -> Result<CompiledStage, FaucetError> {
    match s {
        TransformStage::Map(t) => Ok(CompiledStage::Map(compile_record(t)?)),
        #[cfg(feature = "transform-filter")]
        TransformStage::Filter(spec) => Ok(CompiledStage::Filter(CompiledFilter::compile(spec)?)),
        #[cfg(feature = "transform-explode")]
        TransformStage::Explode(spec) => {
            Ok(CompiledStage::Explode(CompiledExplode::compile(spec)?))
        }
        TransformStage::Custom(f) => Ok(CompiledStage::Custom(Arc::clone(f))),
        TransformStage::PageFn(f) => Ok(CompiledStage::PageFn(Arc::clone(f))),
    }
}

/// Per-record stage runner. Returns 0..N output records. Pure; no metrics.
pub fn apply_stages(rec: Value, stages: &[CompiledStage]) -> Result<Vec<Value>, FaucetError> {
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

/// Page-granular stage runner. Per-record stages (`Map`/`Filter`/`Explode`/
/// `Custom`) flat-map over the current page; `PageFn` stages transform the whole
/// page at once. Preserves declared order, so `filter → sql → select` works.
pub fn apply_stages_to_page(
    mut records: Vec<Value>,
    stages: &[CompiledStage],
) -> Result<Vec<Value>, FaucetError> {
    for stage in stages {
        match stage {
            CompiledStage::PageFn(f) => {
                records = f(records)?;
            }
            per_record => {
                let mut next = Vec::with_capacity(records.len());
                for r in records {
                    next.extend(apply_one_stage(r, per_record)?);
                }
                records = next;
            }
        }
    }
    Ok(records)
}

fn apply_one_stage(rec: Value, stage: &CompiledStage) -> Result<Vec<Value>, FaucetError> {
    match stage {
        CompiledStage::Map(t) => Ok(vec![crate::transform::apply_all(
            rec,
            std::slice::from_ref(t),
        )?]),
        #[cfg(feature = "transform-filter")]
        CompiledStage::Filter(f) => {
            if f.evaluate(&rec)? {
                Ok(vec![rec])
            } else {
                Ok(vec![])
            }
        }
        #[cfg(feature = "transform-explode")]
        CompiledStage::Explode(e) => e.apply(rec),
        CompiledStage::Custom(f) => Ok(f(rec)),
        CompiledStage::PageFn(_) => Err(FaucetError::Transform(
            "PageFn is a page-level stage and cannot run in a per-record context; \
             use apply_stages_to_page".to_owned(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::KeyCaseMode;
    use serde_json::json;

    fn compile(stages: &[TransformStage]) -> Vec<CompiledStage> {
        stages
            .iter()
            .map(compile_stage)
            .collect::<Result<_, _>>()
            .unwrap()
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
        let drop_all: Arc<dyn Fn(Value) -> Vec<Value> + Send + Sync> = Arc::new(|_| vec![]);
        let stages = vec![CompiledStage::Custom(drop_all)];
        assert_eq!(
            apply_stages(json!({"a": 1}), &stages).unwrap(),
            Vec::<Value>::new()
        );

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
    fn path_bracketed_key_may_contain_jsonpath_metacharacters() {
        // These substrings would be syntax errors at the top level, but inside
        // a quoted bracket they're just content — the bracket form is the
        // documented escape hatch for unusual keys.
        for p in &["$['a..b']", "$['x[*]y']", "$['q[?z']", "$['weird key']"] {
            let parsed = CompiledPath::compile(p)
                .unwrap_or_else(|e| panic!("bracket key with metachars should compile: {p} → {e}"));
            assert_eq!(
                parsed.segments().len(),
                1,
                "bracket form should produce exactly one segment: {p}"
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
        assert_eq!(
            CompiledPath::compile("items").unwrap().last_segment(),
            "items"
        );
        assert_eq!(
            CompiledPath::compile("$.user.items")
                .unwrap()
                .last_segment(),
            "items"
        );
        assert_eq!(
            CompiledPath::compile("$['order-lines']")
                .unwrap()
                .last_segment(),
            "order-lines"
        );
    }

    // ── Filter ──

    #[cfg(feature = "transform-filter")]
    fn filter(path: &str, op: FilterOp, value: Option<Value>) -> TransformStage {
        TransformStage::Filter(FilterSpec {
            path: path.to_owned(),
            op,
            value,
        })
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_eq_keeps_matching_drops_non_matching() {
        let stages = compile(&[filter("status", FilterOp::Eq, Some(json!("active")))]);
        assert_eq!(
            apply_stages(json!({"status": "active"}), &stages).unwrap(),
            vec![json!({"status": "active"})]
        );
        assert_eq!(
            apply_stages(json!({"status": "deleted"}), &stages).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_eq_missing_path_drops() {
        let stages = compile(&[filter("status", FilterOp::Eq, Some(json!("active")))]);
        assert_eq!(
            apply_stages(json!({"other": 1}), &stages).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_ne_keeps_missing_path() {
        let stages = compile(&[filter("deleted", FilterOp::Ne, Some(json!(true)))]);
        // "not equal to true" — record without the key is "satisfied by absence"
        assert_eq!(
            apply_stages(json!({"id": 1}), &stages).unwrap(),
            vec![json!({"id": 1})]
        );
        // explicit deleted=true → drop
        assert_eq!(
            apply_stages(json!({"id": 1, "deleted": true}), &stages).unwrap(),
            Vec::<Value>::new()
        );
        // explicit deleted=false → keep
        assert_eq!(
            apply_stages(json!({"id": 1, "deleted": false}), &stages).unwrap(),
            vec![json!({"id": 1, "deleted": false})]
        );
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_exists_requires_non_null_value() {
        let stages = compile(&[filter("status", FilterOp::Exists, None)]);
        assert_eq!(
            apply_stages(json!({"status": "active"}), &stages).unwrap(),
            vec![json!({"status": "active"})]
        );
        assert_eq!(
            apply_stages(json!({"status": null}), &stages).unwrap(),
            Vec::<Value>::new()
        );
        assert_eq!(
            apply_stages(json!({}), &stages).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_in_keeps_when_member() {
        let stages = compile(&[filter(
            "status",
            FilterOp::In,
            Some(json!(["active", "pending"])),
        )]);
        assert_eq!(
            apply_stages(json!({"status": "active"}), &stages).unwrap(),
            vec![json!({"status": "active"})]
        );
        assert_eq!(
            apply_stages(json!({"status": "closed"}), &stages).unwrap(),
            Vec::<Value>::new()
        );
        assert_eq!(
            apply_stages(json!({}), &stages).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_not_in_keeps_when_not_member_or_missing() {
        let stages = compile(&[filter(
            "status",
            FilterOp::NotIn,
            Some(json!(["banned", "deleted"])),
        )]);
        assert_eq!(
            apply_stages(json!({"status": "active"}), &stages).unwrap(),
            vec![json!({"status": "active"})]
        );
        assert_eq!(
            apply_stages(json!({"status": "banned"}), &stages).unwrap(),
            Vec::<Value>::new()
        );
        // missing path → keep
        assert_eq!(apply_stages(json!({}), &stages).unwrap(), vec![json!({})]);
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_type_strict_no_coercion() {
        // string "5" eq number 5 → false
        let stages = compile(&[filter("v", FilterOp::Eq, Some(json!(5)))]);
        assert_eq!(
            apply_stages(json!({"v": "5"}), &stages).unwrap(),
            Vec::<Value>::new()
        );
        assert_eq!(
            apply_stages(json!({"v": 5}), &stages).unwrap(),
            vec![json!({"v": 5})]
        );
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_compile_rejects_in_with_non_array_value() {
        let err = compile_stage(&filter("v", FilterOp::In, Some(json!("notarray"))))
            .expect_err("non-array value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("requires an array"));
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_compile_rejects_exists_with_value() {
        let err = compile_stage(&filter("v", FilterOp::Exists, Some(json!("x"))))
            .expect_err("exists with value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("'exists'"));
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_compile_rejects_eq_with_missing_value() {
        let err = compile_stage(&filter("v", FilterOp::Eq, None)).expect_err("eq requires value");
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("requires a"));
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn filter_compile_rejects_bad_path() {
        let err = compile_stage(&filter("$..nope", FilterOp::Exists, None)).expect_err("bad path");
        assert!(matches!(err, FaucetError::Transform(_)));
    }

    // ── Explode ──

    #[cfg(feature = "transform-explode")]
    fn explode(path: &str) -> TransformStage {
        TransformStage::Explode(ExplodeSpec {
            path: path.to_owned(),
            prefix: None,
            separator: "_".to_owned(),
            on_missing: OnMissing::Passthrough,
        })
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_object_default_prefix() {
        let stages = compile(&[explode("items")]);
        let out =
            apply_stages(json!({"id": 1, "items": [{"sku": "A", "qty": 2}]}), &stages).unwrap();
        assert_eq!(
            out,
            vec![json!({"id": 1, "items_sku": "A", "items_qty": 2})]
        );
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_object_custom_prefix_and_separator() {
        let stages = compile(&[TransformStage::Explode(ExplodeSpec {
            path: "items".to_owned(),
            prefix: Some("item".to_owned()),
            separator: "_".to_owned(),
            on_missing: OnMissing::Passthrough,
        })]);
        let out = apply_stages(
            json!({"id": 1, "items": [{"sku": "A"}, {"sku": "B"}]}),
            &stages,
        )
        .unwrap();
        assert_eq!(
            out,
            vec![
                json!({"id": 1, "item_sku": "A"}),
                json!({"id": 1, "item_sku": "B"}),
            ]
        );
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_object_empty_prefix_is_lateral_flatten() {
        let stages = compile(&[TransformStage::Explode(ExplodeSpec {
            path: "items".to_owned(),
            prefix: Some(String::new()),
            separator: "_".to_owned(),
            on_missing: OnMissing::Passthrough,
        })]);
        let out = apply_stages(json!({"id": 1, "items": [{"sku": "A"}]}), &stages).unwrap();
        assert_eq!(out, vec![json!({"id": 1, "sku": "A"})]);
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_scalar_replaces_in_place() {
        let stages = compile(&[explode("tags")]);
        let out = apply_stages(json!({"id": 1, "tags": ["rust", "etl"]}), &stages).unwrap();
        assert_eq!(
            out,
            vec![
                json!({"id": 1, "tags": "rust"}),
                json!({"id": 1, "tags": "etl"}),
            ]
        );
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_nested_object_path() {
        let stages = compile(&[explode("$.user.items")]);
        let out = apply_stages(
            json!({"id": 1, "user": {"name": "A", "items": [{"x": 1}]}}),
            &stages,
        )
        .unwrap();
        // items field at $.user is removed; items_x added as a sibling of name
        assert_eq!(
            out,
            vec![json!({"id": 1, "user": {"name": "A", "items_x": 1}})]
        );
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_collision_errors() {
        let stages = compile(&[explode("items")]);
        let err = apply_stages(
            json!({"id": 1, "items_sku": "X", "items": [{"sku": "A"}]}),
            &stages,
        )
        .expect_err("collision on items_sku");
        assert!(format!("{err}").contains("items_sku"));
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_on_missing_passthrough_default() {
        let stages = compile(&[explode("items")]);
        // missing
        assert_eq!(
            apply_stages(json!({"id": 1}), &stages).unwrap(),
            vec![json!({"id": 1})]
        );
        // null
        assert_eq!(
            apply_stages(json!({"id": 1, "items": null}), &stages).unwrap(),
            vec![json!({"id": 1, "items": null})]
        );
        // non-array
        assert_eq!(
            apply_stages(json!({"id": 1, "items": "scalar"}), &stages).unwrap(),
            vec![json!({"id": 1, "items": "scalar"})]
        );
        // empty array
        assert_eq!(
            apply_stages(json!({"id": 1, "items": []}), &stages).unwrap(),
            vec![json!({"id": 1, "items": []})]
        );
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_on_missing_drop() {
        let stages = compile(&[TransformStage::Explode(ExplodeSpec {
            path: "items".to_owned(),
            prefix: None,
            separator: "_".to_owned(),
            on_missing: OnMissing::Drop,
        })]);
        assert_eq!(
            apply_stages(json!({"id": 1}), &stages).unwrap(),
            Vec::<Value>::new()
        );
        assert_eq!(
            apply_stages(json!({"id": 1, "items": []}), &stages).unwrap(),
            Vec::<Value>::new()
        );
        assert_eq!(
            apply_stages(json!({"id": 1, "items": null}), &stages).unwrap(),
            Vec::<Value>::new()
        );
        assert_eq!(
            apply_stages(json!({"id": 1, "items": "scalar"}), &stages).unwrap(),
            Vec::<Value>::new()
        );
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_on_missing_error() {
        let stages = compile(&[TransformStage::Explode(ExplodeSpec {
            path: "items".to_owned(),
            prefix: None,
            separator: "_".to_owned(),
            on_missing: OnMissing::Error,
        })]);
        let err = apply_stages(json!({"id": 1}), &stages).expect_err("missing → error");
        assert!(format!("{err}").contains("items"));
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_compile_rejects_bad_path() {
        let err = compile_stage(&TransformStage::Explode(ExplodeSpec {
            path: "$..items".to_owned(),
            prefix: None,
            separator: "_".to_owned(),
            on_missing: OnMissing::Passthrough,
        }))
        .expect_err("recursive descent");
        assert!(matches!(err, FaucetError::Transform(_)));
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn explode_default_prefix_for_nested_path_is_last_segment() {
        let stages = compile(&[explode("$.user.items")]);
        let out = apply_stages(json!({"user": {"items": [{"id": 1}]}}), &stages).unwrap();
        assert_eq!(out, vec![json!({"user": {"items_id": 1}})]);
    }

    #[cfg(all(feature = "transform-filter", feature = "transform-explode"))]
    #[test]
    fn filter_then_explode_filters_parents() {
        // filter drops the deleted parent before explosion happens.
        let stages = compile(&[
            filter("deleted", FilterOp::Ne, Some(json!(true))),
            explode("items"),
        ]);
        let parent = json!({"id": 1, "deleted": true, "items": [{"sku": "A"}]});
        assert_eq!(apply_stages(parent, &stages).unwrap(), Vec::<Value>::new());

        let kept = json!({"id": 2, "deleted": false, "items": [{"sku": "B"}]});
        let out = apply_stages(kept, &stages).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["items_sku"], json!("B"));
    }

    #[cfg(all(feature = "transform-filter", feature = "transform-explode"))]
    #[test]
    fn explode_then_filter_filters_children() {
        // explode first, then filter dropping certain children.
        let stages = compile(&[
            explode("items"),
            filter("items_status", FilterOp::Eq, Some(json!("active"))),
        ]);
        let rec = json!({
            "id": 1,
            "items": [
                {"id": 10, "status": "active"},
                {"id": 11, "status": "archived"},
                {"id": 12, "status": "active"},
            ]
        });
        let out = apply_stages(rec, &stages).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["items_id"], json!(10));
        assert_eq!(out[1]["items_id"], json!(12));
    }

    #[cfg(feature = "transform-explode")]
    #[test]
    fn nested_explodes_multiply() {
        let stages = compile(&[explode("orders"), explode("orders_items")]);
        let rec = json!({
            "customer": "A",
            "orders": [
                {"id": 1, "items": [{"sku": "X"}, {"sku": "Y"}]},
                {"id": 2, "items": [{"sku": "Z"}]},
            ]
        });
        let out = apply_stages(rec, &stages).unwrap();
        assert_eq!(out.len(), 3);
    }

    // ── PageFn ──

    fn page_count_stage() -> CompiledStage {
        // Collapse the whole page into a single {"n": <count>} record.
        CompiledStage::PageFn(Arc::new(|recs: Vec<Value>| {
            Ok(vec![json!({ "n": recs.len() })])
        }))
    }

    #[test]
    fn page_fn_sees_whole_page() {
        let out = apply_stages_to_page(
            vec![json!({"a": 1}), json!({"a": 2}), json!({"a": 3})],
            &[page_count_stage()],
        )
        .unwrap();
        assert_eq!(out, vec![json!({"n": 3})]);
    }

    #[cfg(feature = "transform-filter")]
    #[test]
    fn page_fn_interleaves_with_per_record_stages() {
        // filter (drop a==2) → page-count → (map identity). Count must see 2 rows.
        let compiled = compile(&[TransformStage::Filter(FilterSpec {
            path: "a".into(),
            op: FilterOp::Ne,
            value: Some(json!(2)),
        })]);
        let mut stages = compiled;
        stages.push(page_count_stage());
        let out = apply_stages_to_page(
            vec![json!({"a": 1}), json!({"a": 2}), json!({"a": 3})],
            &stages,
        )
        .unwrap();
        assert_eq!(out, vec![json!({"n": 2})]);
    }

    #[test]
    fn page_fn_only_per_record_path_matches_flat_map() {
        // A page of per-record stages routes identically through both runners.
        let compiled = compile(&[TransformStage::Map(RecordTransform::KeysCase {
            mode: KeyCaseMode::Snake,
        })]);
        let page = vec![json!({"FooBar": 1}), json!({"BazQux": 2})];
        let via_page = apply_stages_to_page(page.clone(), &compiled).unwrap();
        let mut via_record = Vec::new();
        for r in page {
            via_record.extend(apply_stages(r, &compiled).unwrap());
        }
        assert_eq!(via_page, via_record);
    }

    #[test]
    fn page_fn_error_propagates() {
        let boom: CompiledStage = CompiledStage::PageFn(Arc::new(|_| {
            Err(FaucetError::Transform("boom".into()))
        }));
        let err = apply_stages_to_page(vec![json!({})], &[boom]).unwrap_err();
        assert!(matches!(err, FaucetError::Transform(m) if m == "boom"));
    }

    #[test]
    fn page_fn_in_per_record_context_errors() {
        let err = apply_stages(json!({"a": 1}), &[page_count_stage()]).unwrap_err();
        assert!(matches!(err, FaucetError::Transform(_)));
        assert!(format!("{err}").contains("per-record"));
    }

    #[test]
    fn page_fn_handles_empty_page() {
        // An empty page through page_count_stage yields vec![{"n": 0}] — no panic,
        // no error, and the PageFn closure correctly observes a zero-length slice.
        let out = apply_stages_to_page(vec![], &[page_count_stage()]).unwrap();
        assert_eq!(out, vec![json!({"n": 0})]);
    }
}
