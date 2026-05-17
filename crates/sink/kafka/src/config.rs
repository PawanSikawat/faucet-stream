//! Filled in by Task 17.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

// TODO(Task 17): full KafkaSinkConfig
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct KafkaSinkConfig {}

// TODO(Task 17): full KafkaSinkTopic
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum KafkaSinkTopic {
    Fixed { name: String },
    FromPath { path: String },
}

impl Default for KafkaSinkTopic {
    fn default() -> Self {
        Self::Fixed {
            name: String::new(),
        }
    }
}

// TODO(Task 17): full Acks
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Acks {
    None,
    Leader,
    #[default]
    All,
}
