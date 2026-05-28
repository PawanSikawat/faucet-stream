//! Pipeline-level transform stages. A [`TransformStage`] wraps one of four
//! shapes:
//!
//! - [`TransformStage::Map`] holds an unchanged 1→1 [`RecordTransform`].
//! - [`TransformStage::Filter`] is a predicate-based 1→0|1 stage (added in
//!   Task 4).
//! - [`TransformStage::Explode`] expands an array field into 1→0..N output
//!   records (added in Task 5).
//! - [`TransformStage::Custom`] is an `Fn(Value) -> Vec<Value>` closure
//!   escape hatch for library callers (added in Task 6).
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
}
