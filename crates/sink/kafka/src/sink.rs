//! Filled in by Task 20.

use crate::config::KafkaSinkConfig;
use async_trait::async_trait;
use faucet_core::{FaucetError, Sink};
use serde_json::Value;

// TODO(Task 20): real KafkaSink implementation
pub struct KafkaSink {
    _config: KafkaSinkConfig,
}

impl KafkaSink {
    pub async fn new(_config: KafkaSinkConfig) -> Result<Self, FaucetError> {
        Err(FaucetError::Config(
            "KafkaSink is a placeholder; implementation lands in Task 20".into(),
        ))
    }
}

#[async_trait]
impl Sink for KafkaSink {
    async fn write_batch(&self, _records: &[Value]) -> Result<usize, FaucetError> {
        Err(FaucetError::Config("placeholder".into()))
    }
}
