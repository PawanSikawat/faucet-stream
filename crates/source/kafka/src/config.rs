//! Filled in by Task 10.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// TODO(Task 10): full KafkaSourceConfig
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct KafkaSourceConfig {}

// TODO(Task 10): expand
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OffsetReset {
    Earliest,
    #[default]
    Latest,
}
