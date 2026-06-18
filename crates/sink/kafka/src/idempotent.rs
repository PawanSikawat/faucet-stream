//! Exactly-once delivery support for the Kafka sink.
//!
//! Implements the watermark mechanics behind the `Sink` idempotency hooks: a
//! transactional producer commits each page's records plus a commit-token
//! record into a compacted side-topic in one Kafka transaction, and the token
//! is read back on resume. See
//! `docs/superpowers/specs/2026-06-18-kafka-sink-exactly-once-design.md`.

use crate::config::KafkaSinkConfig;
use faucet_core::FaucetError;
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::RDKafkaErrorCode;
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use std::collections::HashMap;
use std::time::Duration;

/// Build the shared connection `ClientConfig` (brokers + auth) reused by the
/// producer, the transactional producer, the admin client, and the
/// token-reader consumer. Only keys valid for every client type live here.
///
/// Producer-only keys (compression, buffering, idempotence) and the
/// `extra_client_config` overrides are layered on by the producer builders —
/// applying them here would let a producer-only property reach a consumer or
/// admin client and be rejected at create time.
pub(crate) fn client_config_base(config: &KafkaSinkConfig) -> Result<ClientConfig, FaucetError> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", &config.brokers);
    config.auth.apply(&mut cfg)?;
    Ok(cfg)
}

/// Auto-create the compacted commit-token side-topic if it does not exist.
/// Idempotent: an "already exists" result is treated as success.
pub(crate) async fn ensure_commit_topic(
    config: &KafkaSinkConfig,
    base: &ClientConfig,
) -> Result<(), FaucetError> {
    let admin: AdminClient<DefaultClientContext> = base
        .create()
        .map_err(|e| FaucetError::Sink(format!("kafka admin client init: {e}")))?;
    let topic = NewTopic::new(
        &config.commit_token_topic,
        config.commit_token_topic_partitions,
        TopicReplication::Fixed(config.commit_token_topic_replication),
    )
    .set("cleanup.policy", "compact");
    let results = admin
        .create_topics([&topic], &AdminOptions::new())
        .await
        .map_err(|e| FaucetError::Sink(format!("kafka create_topics request: {e}")))?;
    for r in results {
        match r {
            Ok(_) => {}
            Err((_t, RDKafkaErrorCode::TopicAlreadyExists)) => {}
            Err((t, code)) => {
                return Err(FaucetError::Sink(format!(
                    "kafka create commit-token topic '{t}': {code:?}"
                )));
            }
        }
    }
    Ok(())
}

/// Read the latest committed token for `scope` from the compacted side-topic.
/// Returns `None` when the topic is empty or has no token for the scope.
///
/// Builds a short-lived, non-committing consumer, assigns every side-topic
/// partition from the beginning, and drains up to each partition's high
/// watermark. Called once per run (at startup), so a full read is cheap.
pub(crate) async fn read_last_token(
    config: &KafkaSinkConfig,
    base: &ClientConfig,
    scope: &str,
) -> Result<Option<String>, FaucetError> {
    let mut cfg = base.clone();
    cfg.set("group.id", "faucet-commit-token-reader");
    cfg.set("enable.auto.commit", "false");
    cfg.set("auto.offset.reset", "earliest");
    let topic = config.commit_token_topic.clone();
    let scope = scope.to_string();
    let timeout = config.message_timeout;

    tokio::task::spawn_blocking(move || read_last_token_blocking(&cfg, &topic, &scope, timeout))
        .await
        .map_err(|e| FaucetError::Sink(format!("kafka token read task: {e}")))?
}

fn read_last_token_blocking(
    cfg: &ClientConfig,
    topic: &str,
    scope: &str,
    timeout: Duration,
) -> Result<Option<String>, FaucetError> {
    let consumer: BaseConsumer = cfg
        .create()
        .map_err(|e| FaucetError::Sink(format!("kafka token reader init: {e}")))?;

    let metadata = consumer
        .fetch_metadata(Some(topic), timeout)
        .map_err(|e| FaucetError::Sink(format!("kafka token reader metadata: {e}")))?;
    let topic_meta = match metadata.topics().iter().find(|t| t.name() == topic) {
        Some(t) if !t.partitions().is_empty() => t,
        _ => return Ok(None),
    };

    let mut tpl = TopicPartitionList::new();
    // (partition_id, high_watermark)
    let mut ends: Vec<(i32, i64)> = Vec::new();
    for p in topic_meta.partitions() {
        let (_low, high) = consumer
            .fetch_watermarks(topic, p.id(), timeout)
            .map_err(|e| FaucetError::Sink(format!("kafka token reader watermarks: {e}")))?;
        ends.push((p.id(), high));
        tpl.add_partition_offset(topic, p.id(), Offset::Beginning)
            .map_err(|e| FaucetError::Sink(format!("kafka token reader tpl: {e}")))?;
    }
    if ends.iter().all(|(_, high)| *high == 0) {
        return Ok(None); // every partition empty
    }
    consumer
        .assign(&tpl)
        .map_err(|e| FaucetError::Sink(format!("kafka token reader assign: {e}")))?;

    let mut records: Vec<(Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    let mut consumed: HashMap<i32, i64> = HashMap::new();
    loop {
        let done = ends
            .iter()
            .all(|(pid, high)| *high == 0 || consumed.get(pid).copied().unwrap_or(0) >= *high);
        if done {
            break;
        }
        match consumer.poll(timeout) {
            Some(Ok(msg)) => {
                let key = msg.key().map(|k| k.to_vec()).unwrap_or_default();
                let val = msg.payload().map(|v| v.to_vec());
                *consumed.entry(msg.partition()).or_insert(0) += 1;
                records.push((key, val));
            }
            Some(Err(e)) => {
                return Err(FaucetError::Sink(format!("kafka token reader poll: {e}")));
            }
            None => break, // no message within the timeout — stop draining
        }
    }

    Ok(max_token_for_scope(&records, scope).map(faucet_core::idempotency::format_token))
}

/// Derive the producer `transactional.id` from a stable pipeline scope.
///
/// The result is `"{prefix}.{sanitized}"`, where `sanitized` replaces any
/// character outside `[A-Za-z0-9._-]` with `_`. This keeps the id stable across
/// restarts of the same pipeline-row (so a restart fences its own zombie) and
/// unique across rows/pipelines whose scopes differ after sanitization (so
/// distinct pipelines never fence each other). Sanitization is many-to-one, so
/// callers must keep their scopes distinct under it; faucet derives scopes from
/// the pipeline/row identity (`{name}::{row_id}`), which stay distinct. `prefix`
/// is interpolated verbatim — validating it as a legal `transactional.id`
/// fragment is the caller's responsibility.
pub(crate) fn derive_transactional_id(prefix: &str, scope: &str) -> String {
    let sanitized: String = scope
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{prefix}.{sanitized}")
}

/// The maximum commit-token value recorded for `scope` among consumed
/// side-topic records.
///
/// Records are `(key_bytes, value_bytes)`. Only records whose key equals
/// `scope` are considered; their values are parsed as commit tokens and the
/// maximum is returned (robust to pre-compaction duplicates / out-of-order
/// delivery). Returns `None` when no valid token exists for the scope.
pub(crate) fn max_token_for_scope(
    records: &[(Vec<u8>, Option<Vec<u8>>)],
    scope: &str,
) -> Option<u64> {
    records
        .iter()
        .filter(|(k, _)| k.as_slice() == scope.as_bytes())
        .filter_map(|(_, v)| v.as_ref())
        .filter_map(|v| std::str::from_utf8(v).ok())
        .filter_map(faucet_core::idempotency::parse_token)
        .max()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_config_sets_brokers_only() {
        use crate::config::{Acks, KafkaSinkConfig, KafkaSinkTopic};
        use faucet_common_kafka::{CompressionType, KafkaAuth, KafkaValueFormat, OnKeyError};
        use std::collections::BTreeMap;
        use std::time::Duration;

        let config = KafkaSinkConfig {
            brokers: "host:9092".into(),
            topic: KafkaSinkTopic::Fixed { name: "out".into() },
            auth: KafkaAuth::None,
            value_format: KafkaValueFormat::Json,
            key_format: None,
            value_schema: None,
            key_schema: None,
            key_path: None,
            partition_path: None,
            headers_path: None,
            on_key_error: OnKeyError::Fail,
            compression: CompressionType::None,
            acks: Acks::All,
            idempotent: true,
            linger: Duration::from_millis(5),
            batch_size: faucet_core::DEFAULT_BATCH_SIZE,
            message_timeout: Duration::from_secs(30),
            max_in_flight: 100,
            queue_full_backoff: Duration::from_millis(100),
            queue_full_max_retries: 3,
            transactional_id_prefix: None,
            commit_token_topic: "__faucet_commit_token".into(),
            commit_token_topic_partitions: 1,
            commit_token_topic_replication: -1,
            extra_client_config: BTreeMap::new(),
        };
        let cfg = client_config_base(&config).unwrap();
        assert_eq!(cfg.get("bootstrap.servers"), Some("host:9092"));
        // compression is producer-only — layered by new(), not by the base.
        assert_eq!(cfg.get("compression.type"), None);
    }

    #[test]
    fn derive_sanitizes_scope_separators() {
        assert_eq!(
            derive_transactional_id("faucet", "pipe::row0"),
            "faucet.pipe__row0"
        );
    }

    #[test]
    fn derive_keeps_allowed_chars_and_prefix() {
        assert_eq!(derive_transactional_id("acme", "a.b-c_1"), "acme.a.b-c_1");
        assert_eq!(derive_transactional_id("faucet", "x/y z"), "faucet.x_y_z");
    }

    #[test]
    fn max_token_picks_highest_for_scope_only() {
        let recs = vec![
            (
                b"s1".to_vec(),
                Some(faucet_core::idempotency::format_token(3).into_bytes()),
            ),
            (
                b"s1".to_vec(),
                Some(faucet_core::idempotency::format_token(7).into_bytes()),
            ),
            (
                b"s2".to_vec(),
                Some(faucet_core::idempotency::format_token(99).into_bytes()),
            ),
        ];
        assert_eq!(max_token_for_scope(&recs, "s1"), Some(7));
        assert_eq!(max_token_for_scope(&recs, "s2"), Some(99));
        assert_eq!(max_token_for_scope(&recs, "absent"), None);
    }

    #[test]
    fn max_token_ignores_garbage_and_tombstones() {
        let recs = vec![
            (b"s1".to_vec(), None),
            (b"s1".to_vec(), Some(b"not-a-token".to_vec())),
            (
                b"s1".to_vec(),
                Some(faucet_core::idempotency::format_token(4).into_bytes()),
            ),
        ];
        assert_eq!(max_token_for_scope(&recs, "s1"), Some(4));
    }
}
