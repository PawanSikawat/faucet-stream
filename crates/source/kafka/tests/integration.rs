//! Integration tests for KafkaSource using a real Kafka broker via
//! testcontainers (Apache Kafka image).
//!
//! These tests require Docker to be running. Set RUST_LOG=info,rdkafka=warn
//! while debugging.
//!
//! Import notes for testcontainers-modules 0.15:
//! - `Kafka` lives at `testcontainers_modules::kafka::apache::Kafka`
//! - The port constant is `testcontainers_modules::kafka::apache::KAFKA_PORT`
//! - `AsyncRunner` is at `testcontainers::runners::AsyncRunner` (not via modules re-export)

use faucet_core::{DEFAULT_BATCH_SIZE, Source};
use faucet_kafka_common::{KafkaAuth, KafkaValueFormat, OnDecodeError};
use faucet_source_kafka::{KafkaSource, KafkaSourceConfig, OffsetReset};
use rdkafka::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
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

async fn produce(brokers: &str, topic: &str, messages: &[(Option<&str>, &str)]) {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer init");

    for (key, value) in messages {
        let mut record: FutureRecord<'_, str, str> = FutureRecord::to(topic).payload(*value);
        if let Some(k) = key {
            record = record.key(*k);
        }
        producer
            .send(record, Duration::from_secs(5))
            .await
            .expect("producer send");
    }
    producer
        .flush(Duration::from_secs(5))
        .expect("producer flush");
}

fn source_config(
    brokers: &str,
    topic: &str,
    group: &str,
    max_messages: usize,
) -> KafkaSourceConfig {
    KafkaSourceConfig {
        brokers: brokers.into(),
        topics: vec![topic.into()],
        group_id: group.into(),
        auth: KafkaAuth::None,
        value_format: KafkaValueFormat::Json,
        key_format: None,
        auto_offset_reset: OffsetReset::Earliest,
        max_messages: Some(max_messages),
        // 30s gives the first JoinGroup/SyncGroup rebalance enough time to complete
        // on a fresh consumer group before idle_timeout fires. 5s was too tight under
        // CI load. Individual tests can override with a shorter value when they're
        // specifically exercising the timeout path.
        idle_timeout: Some(Duration::from_secs(30)),
        poll_timeout: Duration::from_secs(1),
        session_timeout: Duration::from_secs(30),
        on_decode_error: OnDecodeError::Fail,
        extra_client_config: BTreeMap::new(),
        batch_size: DEFAULT_BATCH_SIZE,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn round_trip_basic() {
    let (_container, brokers) = start_kafka().await;
    let topic = "round-trip";
    produce(
        &brokers,
        topic,
        &[
            (Some("k1"), r#"{"id":1}"#),
            (Some("k2"), r#"{"id":2}"#),
            (Some("k3"), r#"{"id":3}"#),
        ],
    )
    .await;

    let source = KafkaSource::new(source_config(&brokers, topic, "g-basic", 3))
        .await
        .unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records.len(), 3);
    assert_eq!(records[0]["value"]["id"], 1);
    assert_eq!(records[0]["key"], "k1");
    assert_eq!(records[0]["topic"], topic);
}

/// Verifies that `apply_start_bookmark` causes the consumer to seek past
/// previously-consumed offsets *before any message is delivered*, so the
/// restart boundary does not emit duplicates.
///
/// With 4 messages (id 1-4) and s1 draining the first 2 (ids 1 & 2):
///   bookmark = partition 0 → offset 2
/// s2 starts fresh (group g-resume-2, earliest) with the bookmark applied:
///   - rebalance callback fires on partition assignment → seeks to offset 2
///   - first message delivered is id=3 (offset 2)
///   - then id=4 (offset 3)
///   - idle_timeout fires → stops
/// So second = [id=3, id=4] (2 records). Crucially, id=1 (the message that
/// used to trigger the post-poll seek) is no longer in the output.
#[tokio::test(flavor = "multi_thread")]
async fn resume_with_bookmark_no_restart_duplicate() {
    let (_container, brokers) = start_kafka().await;
    let topic = "resume";
    produce(
        &brokers,
        topic,
        &[
            (None, r#"{"id":1}"#),
            (None, r#"{"id":2}"#),
            (None, r#"{"id":3}"#),
            (None, r#"{"id":4}"#),
        ],
    )
    .await;

    // First run: consume 2 messages and capture the bookmark.
    let s1 = KafkaSource::new(source_config(&brokers, topic, "g-resume", 2))
        .await
        .unwrap();
    let (first, bookmark) = s1.fetch_all_incremental().await.unwrap();
    assert_eq!(first.len(), 2);
    assert_eq!(first[0]["value"]["id"], 1);
    assert_eq!(first[1]["value"]["id"], 2);
    let bookmark = bookmark.expect("bookmark should be Some after consuming messages");

    // Second run: new group, apply bookmark → pre_rebalance seeks to offset 2
    // *before* any poll. id=1 and id=2 are skipped entirely.
    let s2 = KafkaSource::new(source_config(&brokers, topic, "g-resume-2", 10))
        .await
        .unwrap();
    s2.apply_start_bookmark(bookmark).await.unwrap();
    let (second, _) = s2.fetch_all_incremental().await.unwrap();

    assert_eq!(
        second.len(),
        2,
        "expected exactly 2 records with no restart duplicate, got {second:#?}"
    );
    assert_eq!(second[0]["value"]["id"], 3);
    assert_eq!(second[1]["value"]["id"], 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_timeout_returns_when_topic_empty() {
    let (_container, brokers) = start_kafka().await;
    let topic = "idle";
    produce(&brokers, topic, &[(None, r#"{"only": 1}"#)]).await;

    let mut cfg = source_config(&brokers, topic, "g-idle", 100);
    // Keep idle_timeout short enough to make the test specifically exercise the
    // idle-stop path (not the default 30s from source_config), but long enough to
    // tolerate the initial JoinGroup/SyncGroup rebalance plus the consume of 1
    // message before the idle wait starts.
    cfg.idle_timeout = Some(Duration::from_secs(10));
    let source = KafkaSource::new(cfg).await.unwrap();
    let start = std::time::Instant::now();
    let records = source.fetch_all().await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(records.len(), 1);
    assert!(
        elapsed < Duration::from_secs(60),
        "idle_timeout should bound the consume loop (took {elapsed:?})"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn raw_string_format_passes_bytes_as_strings() {
    let (_container, brokers) = start_kafka().await;
    let topic = "raw";
    produce(&brokers, topic, &[(None, "hello"), (None, "world")]).await;
    let mut cfg = source_config(&brokers, topic, "g-raw", 2);
    cfg.value_format = KafkaValueFormat::RawString;
    let source = KafkaSource::new(cfg).await.unwrap();
    let records = source.fetch_all().await.unwrap();
    assert_eq!(records[0]["value"], "hello");
    assert_eq!(records[1]["value"], "world");
}

#[tokio::test(flavor = "multi_thread")]
async fn state_key_is_deterministic_and_present() {
    let (_container, brokers) = start_kafka().await;
    let source = KafkaSource::new(source_config(&brokers, "topic-a", "group-x", 1))
        .await
        .unwrap();
    assert_eq!(source.state_key().as_deref(), Some("kafka:group-x:topic-a"));
}
