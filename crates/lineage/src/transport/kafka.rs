//! Kafka transport — produce each event as a JSON message. Gated on
//! `transport-kafka`.

use super::Transport;
use async_trait::async_trait;
use faucet_core::FaucetError;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use std::time::Duration;

pub struct KafkaTransport {
    producer: FutureProducer,
    topic: String,
}

impl KafkaTransport {
    pub fn new(brokers: &str, topic: String) -> Result<Self, FaucetError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .map_err(|e| FaucetError::Custom(Box::new(e)))?;
        Ok(Self { producer, topic })
    }
}

#[async_trait]
impl Transport for KafkaTransport {
    async fn send(&self, event_json: Vec<u8>) -> Result<(), FaucetError> {
        let record: FutureRecord<'_, (), [u8]> = FutureRecord::to(&self.topic).payload(&event_json);
        self.producer
            .send(record, Duration::from_secs(10))
            .await
            .map_err(|(e, _)| FaucetError::Custom(Box::new(e)))?;
        Ok(())
    }
}
