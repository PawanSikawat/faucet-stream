//! Config-shaped types for the data-quality layer. Pure declarations — no
//! evaluation logic (that lives in `record.rs` / `batch.rs`) and no
//! compilation (that lives in `compile.rs`).

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// What to do when a check fails. The allowed subset is validated per check
/// at compile time (see `compile.rs`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OnFailure {
    /// Route the specific offending row(s) to the DLQ; keep the rest.
    Quarantine,
    /// Route all survivors of the page to the DLQ; write nothing this page.
    QuarantineBatch,
    /// Surface `FaucetError::QualityFailure` and fail the run.
    Abort,
}

/// Ordering / equality operator for the `compare` check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CompareOp {
    Gt,
    Gte,
    Lt,
    Lte,
    Eq,
    Ne,
}

/// Expected JSON type for the `type_is` check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JsonType {
    Boolean,
    Number,
    String,
    Array,
    Object,
    Null,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn on_failure_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&OnFailure::QuarantineBatch).unwrap(),
            "\"quarantine_batch\""
        );
    }

    #[test]
    fn compare_op_round_trips() {
        let op: CompareOp = serde_json::from_str("\"gte\"").unwrap();
        assert_eq!(op, CompareOp::Gte);
    }

    #[test]
    fn json_type_round_trips() {
        let t: JsonType = serde_json::from_str("\"boolean\"").unwrap();
        assert_eq!(t, JsonType::Boolean);
    }
}
