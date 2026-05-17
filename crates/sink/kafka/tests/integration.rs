//! Integration tests for KafkaSink using a real Apache Kafka broker via
//! testcontainers.
//!
//! These tests require Docker to be running. Set RUST_LOG=info,rdkafka=warn
//! while debugging.
//!
//! Import notes for testcontainers-modules 0.15:
//! - `Kafka` lives at `testcontainers_modules::kafka::apache::Kafka`
//! - The port constant is `testcontainers_modules::kafka::apache::KAFKA_PORT`
//! - `AsyncRunner` is at `testcontainers::runners::AsyncRunner` (not via modules re-export)

use faucet_core::Sink;
use faucet_kafka_common::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
use faucet_sink_kafka::{Acks, KafkaSink, KafkaSinkConfig, KafkaSinkTopic};
use rdkafka::ClientConfig;
use rdkafka::Message;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde_json::json;
use std::collections::BTreeMap;
use std::time::Duration;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::kafka::apache::{KAFKA_PORT, Kafka};

async fn start_kafka() -> (testcontainers::ContainerAsync<Kafka>, String) {
    let container = Kafka::default()
        .start()
        .await
        .expect("kafka container start");
    let port = container
        .get_host_port_ipv4(KAFKA_PORT)
        .await
        .expect("kafka port");
    (container, format!("127.0.0.1:{port}"))
}

fn sink_config(brokers: &str, topic: KafkaSinkTopic) -> KafkaSinkConfig {
    KafkaSinkConfig {
        brokers: brokers.into(),
        topic,
        auth: KafkaAuth::None,
        value_format: KafkaValueFormat::Json,
        key_format: None,
        key_path: None,
        partition_path: None,
        headers_path: None,
        on_key_error: OnKeyError::Fail,
        compression: CompressionType::None,
        acks: Acks::All,
        idempotent: true,
        linger: Duration::from_millis(5),
        batch_size: 16_384,
        message_timeout: Duration::from_secs(10),
        max_in_flight: 50,
        queue_full_backoff: Duration::from_millis(100),
        queue_full_max_retries: 3,
        extra_client_config: BTreeMap::new(),
    }
}

async fn drain_topic(brokers: &str, topic: &str, expect: usize) -> Vec<String> {
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", format!("test-consumer-{topic}"))
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .create()
        .expect("consumer init");
    consumer.subscribe(&[topic]).expect("subscribe");
    let mut out = Vec::new();
    while out.len() < expect {
        // 30s tolerates the initial JoinGroup/SyncGroup rebalance under CI load.
        let msg = tokio::time::timeout(Duration::from_secs(30), consumer.recv())
            .await
            .expect("recv timeout")
            .expect("recv");
        let payload = msg
            .payload()
            .map(|b| String::from_utf8_lossy(b).to_string())
            .unwrap_or_default();
        out.push(payload);
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn produce_and_consume_round_trip() {
    let (_c, brokers) = start_kafka().await;
    let topic = "rt";
    let sink = KafkaSink::new(sink_config(
        &brokers,
        KafkaSinkTopic::Fixed { name: topic.into() },
    ))
    .await
    .unwrap();
    let records = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 3);
    sink.flush().await.unwrap();
    let payloads = drain_topic(&brokers, topic, 3).await;
    assert_eq!(payloads.len(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn from_path_routes_to_per_record_topic() {
    let (_c, brokers) = start_kafka().await;
    let sink = KafkaSink::new(sink_config(
        &brokers,
        KafkaSinkTopic::FromPath {
            path: "$.dest".into(),
        },
    ))
    .await
    .unwrap();
    let records = vec![
        json!({"dest": "topic-a", "n": 1}),
        json!({"dest": "topic-b", "n": 2}),
        json!({"dest": "topic-a", "n": 3}),
    ];
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 3);
    sink.flush().await.unwrap();
    let a = drain_topic(&brokers, "topic-a", 2).await;
    let b = drain_topic(&brokers, "topic-b", 1).await;
    assert_eq!(a.len(), 2);
    assert_eq!(b.len(), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn on_key_error_skip_drops_records_without_key() {
    let (_c, brokers) = start_kafka().await;
    let topic = "skip";
    let mut cfg = sink_config(&brokers, KafkaSinkTopic::Fixed { name: topic.into() });
    cfg.key_path = Some("$.user_id".into());
    cfg.on_key_error = OnKeyError::Skip;
    let sink = KafkaSink::new(cfg).await.unwrap();
    let records = vec![
        json!({"user_id": "u1", "id": 1}),
        json!({"id": 2}), // no user_id — should be skipped
        json!({"user_id": "u3", "id": 3}),
    ];
    let n = sink.write_batch(&records).await.unwrap();
    assert_eq!(n, 2);
    sink.flush().await.unwrap();
    let payloads = drain_topic(&brokers, topic, 2).await;
    assert_eq!(payloads.len(), 2);
}
