//! Filled in by Task 13.

use crate::config::KafkaSourceConfig;
use async_trait::async_trait;
use faucet_core::{FaucetError, Source};
use serde_json::Value;
use std::collections::HashMap;

// TODO(Task 13): real KafkaSource implementation
pub struct KafkaSource {
    _config: KafkaSourceConfig,
}

impl KafkaSource {
    pub async fn new(_config: KafkaSourceConfig) -> Result<Self, FaucetError> {
        Err(FaucetError::Config(
            "KafkaSource is a placeholder; implementation lands in Task 13".into(),
        ))
    }
}

#[async_trait]
impl Source for KafkaSource {
    async fn fetch_with_context(
        &self,
        _context: &HashMap<String, Value>,
    ) -> Result<Vec<Value>, FaucetError> {
        Err(FaucetError::Config("placeholder".into()))
    }
}
